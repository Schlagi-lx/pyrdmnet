"""Live-Test mit realer Hardware (z. B. LSS MasterPort 2 als RDMnet-Gateway).

Ablauf (nur lesend, kein RDM SET):
1. Netzwerkadapter wählen, eigenen Broker auf dessen IP starten
   und per mDNS/DNS-SD anmelden (Standard; macOS: Bonjour „dns-sd“)
2. Warten, bis sich RDMnet-Geräte (Gateways) verbinden; jede Verbindung und
   jede Ablehnung samt Grund wird sofort angezeigt
3. Geräte und RDM-Responder hinter den Gateway-Ports suchen (mit Wiederholung,
   weil die RDM-Discovery am Gateway Zeit braucht)
4. Jedes Gateway und jeden Scheinwerfer auslesen, alle Sensoren lesen
5. Optional: Abgleich gegen erwarteten Modus/Adresse/Universe (nur Anzeige)
6. Alles in einem Ergebnisordner ablegen: Bericht, JSON, SQLite,
   Broker-Ereignisse und Paketmitschnitt (für die Fehlersuche)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import platform
import re
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import __version__
from . import abgleich as A
from . import konstanten as K
from .auslesen import Auslesung, GeraeteAdresse
from .broker import BrokerKonfig, RdmnetBroker
from .controller import RdmFehler, RdmnetController
from .speicher import RdmSpeicher


# ------------------------------------------------------------------- Netzwerkadapter
def netzwerkadapter(text: str | None = None) -> list[tuple[str, str]]:
    """(Name, IPv4) aller Adapter mit Adresse, über ifconfig (macOS/Linux)."""
    if text is None:
        try:
            text = subprocess.run(["ifconfig"], capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.SubprocessError):
            return []
    adapter, name = [], None
    for zeile in text.splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+):? ", zeile)
        if m and not zeile.startswith((" ", "\t")):
            name = m.group(1).rstrip(":")
            continue
        m = re.search(r"\binet (?:addr:)?(\d+\.\d+\.\d+\.\d+)", zeile)
        if m and name:
            adapter.append((name, m.group(1)))
    return adapter


def ip_ist_lokal(ip: str) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((ip, 0))
        return True
    except OSError:
        return False


# ------------------------------------------------------------------- Konfiguration
@dataclass
class LiveKonfig:
    host: str
    port: int = 8888
    scope: str = K.E133_STANDARD_SCOPE
    ordner: Path = field(default_factory=lambda: Path.cwd())
    warte_auf_geraete_s: float = 180.0
    suche_responder_s: float = 60.0
    rdm_timeout_s: float = 5.0
    dnssd: bool = True
    erwartung: A.EinstellungsErwartung | None = None


def _jetzt() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class LiveTest:
    ordner_praefix = "livetest"
    berichtstitel = "Live-Test"
    berichtsmodus = "nur lesend (kein RDM SET)"

    def __init__(self, k: LiveKonfig, ausgabe=print):
        self.k = k
        self.p = ausgabe
        stempel = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.ordner = Path(k.ordner) / f"{self.ordner_praefix}_{stempel}"
        self.ergebnis: dict = {"version": __version__, "start_utc": _jetzt(), "host": k.host, "port": k.port,
                               "scope": k.scope, "system": platform.platform(), "python": platform.python_version(),
                               "gateways": [], "geraete": [], "fehler": [], "hinweise": []}

    # ----------------------------------------------------------------- Ablauf
    async def ausfuehren(self) -> dict:
        self.ordner.mkdir(parents=True, exist_ok=True)
        if not ip_ist_lokal(self.k.host):
            return self._abbruch(f"IP {self.k.host} gehört zu keinem Netzwerkadapter dieses Rechners")
        broker = RdmnetBroker(BrokerKonfig(host=self.k.host, port=self.k.port, scope=self.k.scope,
                                           ereignis_datei=self.ordner / "broker_ereignisse.jsonl",
                                           mitschnitt_datei=self.ordner / "paketmitschnitt.jsonl"))
        try:
            await broker.starten()
        except OSError as f:
            return self._abbruch(f"Broker kann nicht auf {self.k.host}:{self.k.port} starten: {f}")
        self.ergebnis["broker_uid"] = str(broker.konfig.uid)
        dn = None
        try:
            self.p(f"\nBroker läuft: {self.k.host}:{broker.port}, Scope „{self.k.scope}“, UID {broker.konfig.uid}")
            if self.k.dnssd:
                dn = await asyncio.to_thread(self._mdns_anmelden, broker)
            self.ergebnis["mdns"] = {"aktiv": dn is not None, "weg": dn.weg if dn else None,
                                     "name": dn.name if dn else None}
            if dn:
                self.p("Im MasterPort RDMnet aktivieren, Broker-Suche automatisch (mDNS/DNS-SD), "
                       f"Scope „{self.k.scope}“.")
            else:
                self.p("Im MasterPort RDMnet aktivieren und als Broker fest eintragen:")
                self.p(f"   Adresse {self.k.host}   Port {broker.port}   Scope {self.k.scope}")
            self.p(f"Warte bis zu {self.k.warte_auf_geraete_s:.0f} s auf RDMnet-Geräte …  (Strg+C bricht ab)\n")
            gateways = await self._warte_auf_geraete(broker)
            if not gateways:
                self._fehler("Kein RDMnet-Gerät hat sich verbunden")
                self._diagnose_hinweise(broker)
                return self._ende(broker)
            await self._auslesen(broker)
            return self._ende(broker)
        finally:
            if dn:
                dn.beenden()
            await broker.stoppen()

    def _mdns_anmelden(self, broker: RdmnetBroker):
        from .dnssd import DnssdAnmeldung
        try:
            dn = DnssdAnmeldung(self.k.scope, broker.konfig.cid, broker.konfig.uid, self.k.host, broker.port)
        except Exception as f:
            self._hinweis(f"mDNS-Anmeldung fehlgeschlagen: {f} – Broker muss im MasterPort fest eingetragen werden")
            return None
        self.p(f"mDNS: angemeldet als „{dn.name}“ ({dn.weg}), Typ _rdmnet._tcp mit Subtyp _{self.k.scope}")
        gefunden = dn.selbsttest()
        if dn.weg == "dns-sd":
            if gefunden:
                self.p("mDNS-Selbsttest: Broker über den Scope-Subtyp auffindbar – OK")
            else:
                self._hinweis("mDNS-Selbsttest: eigener Dienst über den Scope-Subtyp nicht gefunden")
            self.ergebnis["mdns_selbsttest"] = gefunden
        return dn

    async def _warte_auf_geraete(self, broker: RdmnetBroker) -> list[str]:
        ende = time.monotonic() + self.k.warte_auf_geraete_s
        gesehen = 0
        geraete: list[str] = []
        while time.monotonic() < ende:
            ereignisse = list(broker.ereignisse)
            for e in ereignisse[gesehen:]:
                art = e["ereignis"]
                if art == "tcp_verbindung":
                    self.p(f"  TCP-Verbindung von {e['gegenstelle']}")
                elif art == "connect_anfrage":
                    self.p(f"  Connect: UID {e['uid']}, {e['typ']}, Scope „{e['scope']}“, E1.33-Version {e['version']}")
                elif art == "connect_abgelehnt":
                    self.p(f"  ABGELEHNT ({e['gegenstelle']}): {e['grund']} – Scope „{e['scope']}“, UID {e['uid']}")
                elif art == "client_verbunden":
                    self.p(f"  VERBUNDEN: {e['typ']} {e['uid']} ({e['gegenstelle']})")
                elif art in ("protokollfehler", "interner_fehler"):
                    self.p(f"  PROTOKOLLFEHLER ({e['gegenstelle']}): {e['fehler']}")
            gesehen = len(ereignisse)
            geraete = [c["uid"] for c in broker.status()["clients"] if c["typ"] == "Gerät"]
            if geraete:
                await asyncio.sleep(3)  # weiteren Gateways kurz Zeit geben
                return [c["uid"] for c in broker.status()["clients"] if c["typ"] == "Gerät"]
            await asyncio.sleep(0.5)
        return geraete

    async def _auslesen(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.verbinden(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.clientliste()
        except Exception as f:
            self._fehler(f"Eigener Controller kann sich nicht verbinden: {f}")
            return
        aus = Auslesung(c, self.k.rdm_timeout_s)
        speicher = RdmSpeicher(self.ordner / "livetest.sqlite")
        try:
            # Gateways selbst (Default-Responder)
            for e in c.geraete():
                gp = await aus.profil(GeraeteAdresse(e.uid, 0, e.uid))
                self.ergebnis["gateways"].append(gp.als_dict())
                self.p(f"\nGateway {e.uid}: {gp.hersteller or '?'} – {gp.modell or '?'} – Firmware {gp.software_label or '?'}")
                if gp.nicht_verfuegbar:
                    self.p("  nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in gp.nicht_verfuegbar.items()))
                await self._ports_zeigen(aus, e.uid, gp.unterstuetzte_pids)
            adressen = await self._responder_suchen(c, aus)
            for adr in adressen:  # Endpoint 0 = natives RDMnet-Gerät ohne Ports
                await self._geraet(aus, speicher, adr)
        finally:
            speicher.schliessen()
            await c.trennen()

    async def _responder_suchen(self, c: RdmnetController, aus: Auslesung) -> list[GeraeteAdresse]:
        """Responder hinter den Ports suchen; die RDM-Discovery am Gateway braucht Zeit."""
        ende = time.monotonic() + self.k.suche_responder_s
        adressen: list[GeraeteAdresse] = []
        while True:
            try:
                adressen = await aus.geraete_finden()
            except RdmFehler as f:
                self._hinweis(f"Suche: {f}")
            if adressen or time.monotonic() >= ende:
                break
            self.p("  noch keine Scheinwerfer an den Gateway-Ports gemeldet – neuer Versuch in 5 s")
            await asyncio.sleep(5)
        if not adressen:
            self._fehler("Keine RDM-Responder gefunden – das Gateway meldet an keinem Port einen Scheinwerfer")
            for e in c.geraete():
                await self._ports_zeigen(aus, e.uid, None, nur_kurz=True)
            self._hinweis("Prüfen: Scheinwerfer am richtigen Port? Port im MasterPort als DMX-Ausgang mit "
                          "aktiviertem RDM eingestellt? RDM im Scheinwerfer aktiv? Ausgang sendet DMX "
                          "(sACN-Universe am Port aktiv)? Kabel/Terminierung?")
        return adressen

    async def _ports_zeigen(self, aus: Auslesung, rpt_uid, unterstuetzt, nur_kurz: bool = False) -> None:
        try:
            ports = await aus.gateway_ports(rpt_uid, unterstuetzt)
        except RdmFehler as f:
            self._hinweis(f"Portübersicht von {rpt_uid} nicht lesbar: {f}")
            return
        g = next((x for x in self.ergebnis["gateways"] if x["rdm_uid"] == str(rpt_uid)), None)
        if g is not None:
            g["ports" if not nur_kurz else "ports_nach_suche"] = ports
        self.p(f"  Ports von {rpt_uid}{' (nach der Suche)' if nur_kurz else ''}:")
        for p in ports:
            teile = [f"Port {p['endpoint']}", p["typ"]]
            for schl, txt in (("modus", "Modus"), ("universe", "Universe"), ("name", "Name"),
                              ("rdm_aktiv", "RDM"), ("discovery", "Discovery")):
                if p[schl] is not None:
                    teile.append(f"{txt} {p[schl]}")
            teile.append(f"{len(p['responder'])} Scheinwerfer")
            self.p("     " + ", ".join(str(t) for t in teile))

    async def _geraet(self, aus: Auslesung, speicher: RdmSpeicher, adr: GeraeteAdresse) -> None:
        self.p(f"\nScheinwerfer {adr}")
        p = await aus.profil(adr)
        gid = speicher.speichere_profil(p)
        eintrag = {"profil": p.als_dict()}
        if p.device_info is None:
            self._fehler(f"{adr}: DEVICE_INFO nicht lesbar ({p.nicht_verfuegbar})")
            self.ergebnis["geraete"].append(eintrag)
            return
        di, pa = p.device_info, p.personality_aktuell
        self.p(f"  {p.hersteller} – {p.modell} – Firmware {p.software_label} (Modell-ID {di.modell_id:#06x}, "
               f"Software-ID {di.software_version_id:#010x})")
        self.p(f"  Modus {di.personality_aktuell}/{di.personality_anzahl} „{pa.beschreibung if pa else '?'}“, "
               f"Footprint {di.footprint}, Startadresse {di.startadresse}, Universe {p.universe}")
        for x in p.personalities:
            self.p(f"     Modus {x.nummer}: {x.footprint:3d} Kanäle  „{x.beschreibung}“")
        self.p(f"  Betriebsstunden {p.geraete_stunden}, Lampenstunden {p.lampen_stunden}")
        if p.nicht_verfuegbar:
            self.p("  nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in p.nicht_verfuegbar.items()))
        s = await aus.sensor_schnappschuss(adr, p.sensoren, alle_sensoren=True)
        speicher.speichere_schnappschuss(gid, s, "LIVETEST", "einzeln")
        eintrag["sensoren"] = s.als_dict()
        for w in s.werte:
            rel = "" if w.messrelevant else "  (nicht messrelevant)"
            warn = "  ! außerhalb Normalbereich" if w.ausserhalb_normal else ""
            self.p(f"     Sensor {w.nummer} {w.beschreibung or w.typ_text}: {w.wert:g} {w.einheit_text}{rel}{warn}")
        for f in s.fehler:
            self.p(f"     Sensorfehler: {f}")
        if self.k.erwartung:
            e = A.pruefen(p, self.k.erwartung)
            speicher.speichere_abgleich(gid, e, self.k.erwartung.gdtf_modus, "LIVETEST")
            eintrag["abgleich"] = e.als_dict()
            self.p(f"  Abgleich: {e.status}, Messung erlaubt: {'Ja' if e.messung_erlaubt else 'Nein'}")
            for x in e.abweichungen + e.hinweise:
                self.p(f"     - {x}")
        self.ergebnis["geraete"].append(eintrag)

    # ----------------------------------------------------------------- Abschluss
    def _hinweis(self, text: str) -> None:
        self.ergebnis["hinweise"].append(text)
        self.p(f"  Hinweis: {text}")

    def _fehler(self, text: str) -> None:
        self.ergebnis["fehler"].append(text)
        self.p(f"  FEHLER: {text}")

    def _diagnose_hinweise(self, broker: RdmnetBroker) -> None:
        arten = [e["ereignis"] for e in broker.ereignisse]
        if "tcp_verbindung" not in arten:
            for t in ("Keine TCP-Verbindung angekommen. Prüfen: RDMnet im MasterPort aktiv, Broker-IP/Port richtig "
                      "eingetragen, gleiches Netz, macOS-Firewall erlaubt eingehende Verbindungen für Python.",
                      "mDNS: Scope im MasterPort muss exakt gleich sein (Groß-/Kleinschreibung); Mac und "
                      "MasterPort im selben Subnetz; mit „dns-sd -B _rdmnet._tcp“ im Terminal prüfen, ob der "
                      "Broker sichtbar ist. Notfalls --ohne-mdns und Adresse fest eintragen."):
                self._hinweis(t)
        elif "connect_abgelehnt" in arten:
            self._hinweis("Verbindung wurde abgelehnt – Grund siehe oben (meist Scope). Scope im MasterPort und "
                          "--scope angleichen.")
        else:
            self._hinweis("TCP-Verbindung ohne gültigen Connect – Paketmitschnitt bitte schicken.")

    def _abbruch(self, text: str) -> dict:
        self._fehler(text)
        self._dateien_schreiben(None)
        return self.ergebnis

    def _erfolgreich(self) -> bool:
        return bool(self.ergebnis["geraete"]) and not self.ergebnis["fehler"]

    def _ende(self, broker: RdmnetBroker) -> dict:
        self.ergebnis["ende_utc"] = _jetzt()
        self.ergebnis["broker_status"] = broker.status()
        self.ergebnis["erfolgreich"] = self._erfolgreich()
        self._dateien_schreiben(broker)
        self.p(f"\nErgebnis: {'ERFOLGREICH' if self.ergebnis['erfolgreich'] else 'NICHT ERFOLGREICH'}")
        self.p(f"Ergebnisordner: {self.ordner.resolve()}")
        self.p("Bitte den ganzen Ordner (oder bericht.md + paketmitschnitt.jsonl) zurückschicken.")
        return self.ergebnis

    def _dateien_schreiben(self, broker: RdmnetBroker | None) -> None:
        self.ordner.mkdir(parents=True, exist_ok=True)
        (self.ordner / "ergebnis.json").write_text(json.dumps(self.ergebnis, ensure_ascii=False, indent=2),
                                                   encoding="utf-8")
        z = [f"# {self.berichtstitel} pyrdmnet {__version__}", "",
             f"- Start: {self.ergebnis['start_utc']} (UTC)",
             f"- Broker: {self.k.host}:{self.k.port}, Scope „{self.k.scope}“",
             f"- System: {self.ergebnis['system']}, Python {self.ergebnis['python']}",
             f"- Ergebnis: {'erfolgreich' if self.ergebnis.get('erfolgreich') else 'nicht erfolgreich'}",
             f"- Modus: {self.berichtsmodus}", "", "## Gateways", ""]
        for g in self.ergebnis["gateways"]:
            z.append(f"- {g['rdm_uid']}: {g['hersteller']} – {g['modell']} – Firmware {g['software_label']}")
            for p in g.get("ports_nach_suche") or g.get("ports") or []:
                z.append(f"  - Port {p['endpoint']}: Modus {p['modus']}, Universe {p['universe']}, "
                         f"Name {p['name']}, RDM {p['rdm_aktiv']}, Scheinwerfer {len(p['responder'])}")
        z += ["", "## Scheinwerfer", ""]
        for e in self.ergebnis["geraete"]:
            p = e["profil"]
            di = p.get("device_info") or {}
            z.append(f"### {p['rdm_uid']} (Gateway {p['rpt_uid']}, Port {p['endpoint']})")
            z.append(f"- {p['hersteller']} – {p['modell']} – Firmware {p['software_label']}")
            z.append(f"- Modus {di.get('personality_aktuell')}, Footprint {di.get('footprint')}, "
                     f"Startadresse {di.get('startadresse')}, Universe {p.get('universe')}")
            for w in (e.get("sensoren") or {}).get("werte", []):
                z.append(f"- Sensor {w['nummer']} {w['beschreibung']}: {w['wert']:g} {w['einheit_text']}")
            if p.get("nicht_verfuegbar"):
                z.append("- Nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in p["nicht_verfuegbar"].items()))
            if e.get("abgleich"):
                z.append(f"- Abgleich: {e['abgleich']['status']}")
            z.append("")
        if self.ergebnis["fehler"] or self.ergebnis["hinweise"]:
            z += ["## Fehler und Hinweise", ""] + [f"- FEHLER: {x}" for x in self.ergebnis["fehler"]] \
                 + [f"- {x}" for x in self.ergebnis["hinweise"]]
        if broker:
            z += ["", "## Broker-Zähler", "", "```", json.dumps(broker.status()["zaehler"], indent=2), "```"]
        (self.ordner / "bericht.md").write_text("\n".join(z) + "\n", encoding="utf-8")


def adapter_waehlen(eingabe=input, ausgabe=print) -> str | None:
    liste = [(n, ip) for n, ip in netzwerkadapter() if not ip.startswith("127.")]
    if not liste:
        ausgabe("Keine Netzwerkadapter gefunden – bitte --host angeben.")
        return None
    ausgabe("Netzwerkadapter:")
    for i, (n, ip) in enumerate(liste, 1):
        ausgabe(f"  {i}) {n:8} {ip}")
    antwort = eingabe("Nummer des Adapters zum MasterPort: ").strip()
    if not antwort.isdigit() or not 1 <= int(antwort) <= len(liste):
        ausgabe("Ungültige Auswahl.")
        return None
    return liste[int(antwort) - 1][1]
