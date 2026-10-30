"""Schreibtest am realen Scheinwerfer: Modus und Startadresse setzen, zurücklesen,
Ausgangszustand wiederherstellen.

Ablauf:
1. Broker + mDNS starten, auf Gateway warten, Scheinwerfer suchen (wie Live-Test)
2. Ausgangszustand lesen und Plan anzeigen (Ziel und Rückweg)
3. Checkliste vor Ort: jeder Punkt muss mit „j“ bestätigt werden, sonst Abbruch
   ohne jede Änderung
4. Ziel setzen (erst Modus, dann Adresse), zurücklesen und prüfen
5. Rückweg setzen (auch wenn Schritt 4 fehlschlug, sobald etwas geändert
   wurde), zurücklesen und prüfen
6. Alles in Bericht, JSON und SQLite (Tabelle rdm_aenderung) ablegen

Die Modusnummern gibt der Nutzer ausdrücklich an; es wird keine
GDTF-Zuordnung angenommen.
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass

from . import konstanten as K
from .abgleich import AnwendungsErgebnis
from .auslesen import Auslesung, GeraeteAdresse, GeraeteProfil
from .broker import RdmnetBroker
from .controller import RdmFehler, RdmnetController
from .livetest import LiveKonfig, LiveTest
from .rdm import Uid
from .speicher import RdmSpeicher

CHECKLISTE = (
    "Der Scheinwerfer darf jetzt umgestellt werden (keine laufende Messung, keine Vorstellung)",
    "Kein Pult/keine andere Software steuert gerade diesen Scheinwerfer",
    "Niemand schaut direkt in den Scheinwerfer (Lichtzustand kann sich beim Moduswechsel ändern)",
    "Ich bleibe am Gerät, bis der Ausgangszustand wiederhergestellt ist",
)


@dataclass
class SchreibKonfig:
    ziel_modus: int
    ziel_adresse: int
    zurueck_modus: int | None = None  # None = gelesener Ausgangszustand
    zurueck_adresse: int | None = None
    uid: Uid | None = None  # bei mehreren Scheinwerfern Pflicht
    rueckleseversuche: int = 5
    pause_nach_set_s: float = 0.5


class SchreibTest(LiveTest):
    ordner_praefix = "schreibtest"
    berichtstitel = "Schreibtest"
    berichtsmodus = "RDM SET (DMX_PERSONALITY, DMX_START_ADDRESS) nach Checkliste, mit Zurücklesen und Rückweg"

    def __init__(self, k: LiveKonfig, sk: SchreibKonfig, ausgabe=print, eingabe=input):
        super().__init__(k, ausgabe)
        self.sk = sk
        self.eingabe = eingabe
        self.ergebnis["schreibtest"] = {"schritte": [], "erfolgreich": False}

    # ----------------------------------------------------------------- Ablauf
    async def _auslesen(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.verbinden(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.clientliste()
        except Exception as f:
            self._fehler(f"Eigener Controller kann sich nicht verbinden: {f}")
            return
        aus = Auslesung(c, self.k.rdm_timeout_s)
        speicher = RdmSpeicher(self.ordner / "schreibtest.sqlite")
        try:
            for e in c.geraete():
                gp = await aus.profil(GeraeteAdresse(e.uid, 0, e.uid))
                self.ergebnis["gateways"].append(gp.als_dict())
            adressen = await self._responder_suchen(c, aus)
            adr = self._ziel_waehlen(adressen)
            if adr is None:
                return
            vorher = await aus.profil(adr)
            gid = speicher.speichere_profil(vorher)
            self.ergebnis["geraete"].append({"profil": vorher.als_dict()})
            plan = self._plan(vorher)
            if plan is None:
                return
            if not await self._checkliste():
                self._fehler("Checkliste nicht vollständig bestätigt – nichts geändert")
                return
            c.schreibfreigabe = True
            try:
                await self._durchfuehren(c, aus, adr, plan, speicher, gid)
            finally:
                c.schreibfreigabe = False
            nachher = await aus.profil(adr)
            speicher.speichere_profil(nachher)
            self.ergebnis["schreibtest"]["profil_nachher"] = nachher.als_dict()
        finally:
            speicher.schliessen()
            await c.trennen()

    def _ziel_waehlen(self, adressen: list[GeraeteAdresse]) -> GeraeteAdresse | None:
        if self.sk.uid:
            treffer = [a for a in adressen if a.rdm_uid == self.sk.uid]
            if not treffer:
                self._fehler(f"Scheinwerfer {self.sk.uid} nicht gefunden")
                return None
            return treffer[0]
        if len(adressen) == 1:
            return adressen[0]
        if adressen:
            self._fehler("Mehrere Scheinwerfer gefunden – bitte mit --uid einen auswählen: "
                         + ", ".join(str(a.rdm_uid) for a in adressen))
        return None

    def _plan(self, p: GeraeteProfil) -> dict | None:
        di = p.device_info
        if di is None:
            self._fehler(f"Ausgangszustand nicht lesbar ({p.nicht_verfuegbar}) – nichts geändert")
            return None
        modi = {x.nummer: x for x in p.personalities}
        z_modus, z_adr = self.sk.ziel_modus, self.sk.ziel_adresse
        r_modus = self.sk.zurueck_modus if self.sk.zurueck_modus is not None else di.personality_aktuell
        r_adr = self.sk.zurueck_adresse if self.sk.zurueck_adresse is not None else di.startadresse
        for nr, adr, was in ((z_modus, z_adr, "Ziel"), (r_modus, r_adr, "Rückweg")):
            if nr not in modi:
                self._fehler(f"{was}: Modus {nr} gibt es an diesem Gerät nicht (vorhanden: {sorted(modi)})")
                return None
            if not 1 <= adr <= 512 or adr + modi[nr].footprint - 1 > 512:
                self._fehler(f"{was}: Startadresse {adr} mit {modi[nr].footprint} Kanälen passt nicht ins Universe")
                return None
        plan = {"vorher": (di.personality_aktuell, di.startadresse),
                "ziel": (z_modus, z_adr, modi[z_modus].footprint),
                "zurueck": (r_modus, r_adr, modi[r_modus].footprint)}
        self.ergebnis["schreibtest"]["plan"] = {
            "vorher": {"modus": di.personality_aktuell, "adresse": di.startadresse},
            "ziel": {"modus": z_modus, "adresse": z_adr}, "zurueck": {"modus": r_modus, "adresse": r_adr}}
        def txt(nr): return f"{nr} „{modi[nr].beschreibung}“ ({modi[nr].footprint} Kanäle)"
        self.p(f"\nSchreibtest an {p.hersteller} {p.modell} ({p.adresse.rdm_uid}):")
        self.p(f"  jetzt:      Modus {txt(di.personality_aktuell)}, Startadresse {di.startadresse}")
        self.p(f"  Schritt 1:  Modus {txt(z_modus)}, Startadresse {z_adr}  → zurücklesen")
        self.p(f"  Schritt 2:  Modus {txt(r_modus)}, Startadresse {r_adr}  → zurücklesen")
        if (r_modus, r_adr) != (di.personality_aktuell, di.startadresse):
            self.p("  ACHTUNG: Der Rückweg weicht vom jetzigen Zustand ab.")
        return plan

    async def _checkliste(self) -> bool:
        """Eingaben laufen in einem eigenen Thread, damit Broker-Heartbeats während der Abfrage weiterlaufen."""
        self.p("\nCheckliste vor Ort (je Punkt j = ja, sonst Abbruch):")
        bestaetigt = []
        for punkt in CHECKLISTE:
            antwort = (await asyncio.to_thread(self.eingabe, f"  {punkt}? [j/n] ")).strip().lower()
            ok = antwort in ("j", "ja", "y", "yes")
            bestaetigt.append({"punkt": punkt, "antwort": antwort})
            if not ok:
                self.ergebnis["schreibtest"]["checkliste"] = bestaetigt
                return False
        self.ergebnis["schreibtest"]["checkliste"] = bestaetigt
        return True

    async def _durchfuehren(self, c, aus, adr, plan, speicher: RdmSpeicher, gid: int) -> None:
        z_modus, z_adr, z_fp = plan["ziel"]
        r_modus, r_adr, r_fp = plan["zurueck"]
        st = self.ergebnis["schreibtest"]
        self.p("\nSchritt 1: Ziel setzen")
        s1 = await self._setzen_und_pruefen(c, aus, adr, z_modus, z_adr, z_fp, "Schritt 1", plan["vorher"])
        speicher.speichere_anwendung(gid, s1["anwendung"], "SCHREIBTEST Schritt 1")
        st["schritte"].append(s1["bericht"])
        if not s1["anwendung"].aenderungen and not s1["bericht"]["ok"]:
            self._fehler("Schritt 1 hat nichts geändert – Rückweg entfällt")
            return
        self.p("\nSchritt 2: zurückstellen")
        s2 = await self._setzen_und_pruefen(c, aus, adr, r_modus, r_adr, r_fp, "Schritt 2", (z_modus, z_adr))
        speicher.speichere_anwendung(gid, s2["anwendung"], "SCHREIBTEST Schritt 2")
        st["schritte"].append(s2["bericht"])
        st["erfolgreich"] = s1["bericht"]["ok"] and s2["bericht"]["ok"]
        if not s2["bericht"]["ok"]:
            self._fehler("RÜCKWEG NICHT BESTÄTIGT – Scheinwerfer bitte von Hand prüfen und einstellen: "
                         f"Modus {r_modus}, Startadresse {r_adr}")
        elif not s1["bericht"]["ok"]:
            self._fehler("Schritt 1 nicht wie erwartet zurückgelesen (Rückweg war erfolgreich)")

    async def _setzen_und_pruefen(self, c: RdmnetController, aus: Auslesung, adr: GeraeteAdresse, modus: int,
                                  adresse: int, footprint: int, name: str, alt: tuple[int, int]) -> dict:
        anw = AnwendungsErgebnis(False)
        bericht = {"schritt": name, "soll": {"modus": modus, "adresse": adresse, "footprint": footprint},
                   "gesetzt": [], "fehler": [], "ist": None, "ok": False}

        async def setzen(pid: int, daten: bytes, param: str, wert, alt_wert) -> bool:
            try:
                await c.rdm_set(adr.rpt_uid, pid, daten, endpoint=adr.endpoint, rdm_ziel=adr.rdm_uid,
                                timeout_s=self.k.rdm_timeout_s)
            except RdmFehler as f:
                bericht["fehler"].append(f"{param} setzen: {f}")
                anw.fehler.append(f"{param} setzen: {f}")
                self.p(f"  {param} = {wert}: FEHLER {f}")
                return False
            bericht["gesetzt"].append({param: wert})
            anw.aenderungen.append({"parameter": param, "alt": alt_wert, "neu": wert})
            self.p(f"  {param} = {wert}: bestätigt (ACK)")
            await asyncio.sleep(self.sk.pause_nach_set_s)
            return True

        if await setzen(K.PID_DMX_PERSONALITY, bytes((modus,)), "DMX_PERSONALITY", modus, alt[0]):
            await setzen(K.PID_DMX_START_ADDRESS, struct.pack(">H", adresse), "DMX_START_ADDRESS", adresse, alt[1])

        ist = None
        for versuch in range(self.sk.rueckleseversuche):
            try:
                ist = await aus.einstellung_lesen(adr)
            except RdmFehler as f:
                bericht["fehler"].append(f"Zurücklesen Versuch {versuch + 1}: {f}")
                await asyncio.sleep(1)
                continue
            if (ist.personality_aktuell, ist.startadresse, ist.footprint) == (modus, adresse, footprint):
                break
            await asyncio.sleep(1)  # Gerät braucht evtl. einen Moment nach dem Moduswechsel
        if ist:
            bericht["ist"] = {"modus": ist.personality_aktuell, "adresse": ist.startadresse,
                              "footprint": ist.footprint}
            bericht["ok"] = (ist.personality_aktuell, ist.startadresse, ist.footprint) == (modus, adresse, footprint)
            self.p(f"  zurückgelesen: Modus {ist.personality_aktuell}, Startadresse {ist.startadresse}, "
                   f"{ist.footprint} Kanäle → {'OK' if bericht['ok'] else 'ABWEICHUNG'}")
        else:
            self.p("  zurücklesen: FEHLGESCHLAGEN")
        anw.erfolgreich = bericht["ok"]
        return {"anwendung": anw, "bericht": bericht}

    # ----------------------------------------------------------------- Abschluss
    def _erfolgreich(self) -> bool:
        return bool(self.ergebnis["schreibtest"].get("erfolgreich")) and not self.ergebnis["fehler"]

    def _dateien_schreiben(self, broker) -> None:
        super()._dateien_schreiben(broker)
        st = self.ergebnis["schreibtest"]
        z = ["", "## Schreibtest", ""]
        if "plan" in st:
            p = st["plan"]
            z.append(f"- Vorher: Modus {p['vorher']['modus']}, Adresse {p['vorher']['adresse']}")
            z.append(f"- Ziel: Modus {p['ziel']['modus']}, Adresse {p['ziel']['adresse']}")
            z.append(f"- Rückweg: Modus {p['zurueck']['modus']}, Adresse {p['zurueck']['adresse']}")
        for s in st["schritte"]:
            z.append(f"- {s['schritt']}: Soll {s['soll']}, Ist {s['ist']}, {'OK' if s['ok'] else 'NICHT OK'}"
                     + (f", Fehler: {'; '.join(s['fehler'])}" if s["fehler"] else ""))
        z.append(f"- Ergebnis: {'erfolgreich' if st.get('erfolgreich') else 'nicht erfolgreich'}")
        with open(self.ordner / "bericht.md", "a", encoding="utf-8") as f:
            f.write("\n".join(z) + "\n")
