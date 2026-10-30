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
from . import settings_check as A
from . import constants as C
from .readout import DeviceReader, DeviceAddress
from .broker import BrokerConfig, RdmnetBroker
from .controller import RdmError, RdmnetController
from .storage import RdmStore


# ------------------------------------------------------------------- Netzwerkadapter
def network_adapters(text: str | None = None) -> list[tuple[str, str]]:
    """(Name, IPv4) aller Adapter mit Adresse, über ifconfig (macOS/Linux)."""
    if text is None:
        try:
            text = subprocess.run(["ifconfig"], capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.SubprocessError):
            return []
    adapter, name = [], None
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+):? ", line)
        if m and not line.startswith((" ", "\t")):
            name = m.group(1).rstrip(":")
            continue
        m = re.search(r"\binet (?:addr:)?(\d+\.\d+\.\d+\.\d+)", line)
        if m and name:
            adapter.append((name, m.group(1)))
    return adapter


def ip_is_local(ip: str) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((ip, 0))
        return True
    except OSError:
        return False


# ------------------------------------------------------------------- Konfiguration
@dataclass
class LiveConfig:
    host: str
    port: int = 8888
    scope: str = C.E133_DEFAULT_SCOPE
    folder: Path = field(default_factory=lambda: Path.cwd())
    wait_for_devices_s: float = 180.0
    find_responders_s: float = 60.0
    rdm_timeout_s: float = 5.0
    dnssd: bool = True
    expected: A.ExpectedSetting | None = None


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class LiveTest:
    folder_prefix = "livetest"
    report_title = "Live-Test"
    report_mode = "nur lesend (kein RDM SET)"

    def __init__(self, k: LiveConfig, output=print):
        self.k = k
        self.p = output
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.folder = Path(k.folder) / f"{self.folder_prefix}_{stamp}"
        self.result: dict = {"version": __version__, "start_utc": _now(), "host": k.host, "port": k.port,
                               "scope": k.scope, "system": platform.platform(), "python": platform.python_version(),
                               "gateways": [], "devices": [], "error": [], "hints": []}

    # ----------------------------------------------------------------- Ablauf
    async def run(self) -> dict:
        self.folder.mkdir(parents=True, exist_ok=True)
        if not ip_is_local(self.k.host):
            return self._abort(f"IP {self.k.host} gehört zu keinem Netzwerkadapter dieses Rechners")
        broker = RdmnetBroker(BrokerConfig(host=self.k.host, port=self.k.port, scope=self.k.scope,
                                           event_file=self.folder / "broker_ereignisse.jsonl",
                                           capture_file=self.folder / "paketmitschnitt.jsonl"))
        try:
            await broker.start()
        except OSError as f:
            return self._abort(f"Broker kann nicht auf {self.k.host}:{self.k.port} starten: {f}")
        self.result["broker_uid"] = str(broker.config.uid)
        reg = None
        try:
            self.p(f"\nBroker läuft: {self.k.host}:{broker.port}, Scope „{self.k.scope}“, UID {broker.config.uid}")
            if self.k.dnssd:
                reg = await asyncio.to_thread(self._register_mdns, broker)
            self.result["mdns"] = {"active": reg is not None, "method": reg.method if reg else None,
                                     "name": reg.name if reg else None}
            if reg:
                self.p("Im MasterPort RDMnet aktivieren, Broker-Suche automatisch (mDNS/DNS-SD), "
                       f"Scope „{self.k.scope}“.")
            else:
                self.p("Im MasterPort RDMnet aktivieren und als Broker fest eintragen:")
                self.p(f"   Adresse {self.k.host}   Port {broker.port}   Scope {self.k.scope}")
            self.p(f"Warte bis zu {self.k.wait_for_devices_s:.0f} s auf RDMnet-Geräte …  (Strg+C bricht ab)\n")
            gateways = await self._wait_for_devices(broker)
            if not gateways:
                self._error("Kein RDMnet-Gerät hat sich verbunden")
                self._diagnostic_hints(broker)
                return self._finish(broker)
            await self._read_devices(broker)
            return self._finish(broker)
        finally:
            if reg:
                reg.stop()
            await broker.stop()

    def _register_mdns(self, broker: RdmnetBroker):
        from .dnssd import DnssdRegistration
        try:
            reg = DnssdRegistration(self.k.scope, broker.config.cid, broker.config.uid, self.k.host, broker.port)
        except Exception as f:
            self._hint(f"mDNS-Anmeldung fehlgeschlagen: {f} – Broker muss im MasterPort fest eingetragen werden")
            return None
        self.p(f"mDNS: angemeldet als „{reg.name}“ ({reg.method}), Typ _rdmnet._tcp mit Subtyp _{self.k.scope}")
        found = reg.self_test()
        if reg.method == "dns-sd":
            if found:
                self.p("mDNS-Selbsttest: Broker über den Scope-Subtyp auffindbar – OK")
            else:
                self._hint("mDNS-Selbsttest: eigener Dienst über den Scope-Subtyp nicht gefunden")
            self.result["mdns_self_test"] = found
        return reg

    async def _wait_for_devices(self, broker: RdmnetBroker) -> list[str]:
        end = time.monotonic() + self.k.wait_for_devices_s
        seen = 0
        devices: list[str] = []
        while time.monotonic() < end:
            events = list(broker.events)
            for e in events[seen:]:
                kind = e["event"]
                if kind == "tcp_connection":
                    self.p(f"  TCP-Verbindung von {e['peer']}")
                elif kind == "connect_request":
                    self.p(f"  Connect: UID {e['uid']}, {e['type']}, Scope „{e['scope']}“, E1.33-Version {e['version']}")
                elif kind == "connect_rejected":
                    self.p(f"  ABGELEHNT ({e['peer']}): {e['reason']} – Scope „{e['scope']}“, UID {e['uid']}")
                elif kind == "client_connected":
                    self.p(f"  VERBUNDEN: {e['type']} {e['uid']} ({e['peer']})")
                elif kind in ("protocol_error", "internal_error"):
                    self.p(f"  PROTOKOLLFEHLER ({e['peer']}): {e['error']}")
            seen = len(events)
            devices = [c["uid"] for c in broker.status()["clients"] if c["type"] == "Gerät"]
            if devices:
                await asyncio.sleep(3)  # weiteren Gateways kurz Zeit geben
                return [c["uid"] for c in broker.status()["clients"] if c["type"] == "Gerät"]
            await asyncio.sleep(0.5)
        return devices

    async def _read_devices(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.connect(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.client_list()
        except Exception as f:
            self._error(f"Eigener Controller kann sich nicht verbinden: {f}")
            return
        reader = DeviceReader(c, self.k.rdm_timeout_s)
        store = RdmStore(self.folder / "livetest.sqlite")
        try:
            # Gateways selbst (Default-Responder)
            for e in c.devices():
                gp = await reader.profile(DeviceAddress(e.uid, 0, e.uid))
                self.result["gateways"].append(gp.to_dict())
                self.p(f"\nGateway {e.uid}: {gp.manufacturer or '?'} – {gp.model or '?'} – Firmware {gp.software_label or '?'}")
                if gp.unavailable:
                    self.p("  nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in gp.unavailable.items()))
                await self._show_ports(reader, e.uid, gp.supported_pids)
            addresses = await self._find_responders(c, reader)
            for dmx_addr in addresses:  # Endpoint 0 = natives RDMnet-Gerät ohne Ports
                await self._device(reader, store, dmx_addr)
        finally:
            store.close()
            await c.disconnect()

    async def _find_responders(self, c: RdmnetController, reader: DeviceReader) -> list[DeviceAddress]:
        """Responder hinter den Ports suchen; die RDM-Discovery am Gateway braucht Zeit."""
        end = time.monotonic() + self.k.find_responders_s
        addresses: list[DeviceAddress] = []
        while True:
            try:
                addresses = await reader.find_devices()
            except RdmError as f:
                self._hint(f"Suche: {f}")
            if addresses or time.monotonic() >= end:
                break
            self.p("  noch keine Scheinwerfer an den Gateway-Ports gemeldet – neuer Versuch in 5 s")
            await asyncio.sleep(5)
        if not addresses:
            self._error("Keine RDM-Responder gefunden – das Gateway meldet an keinem Port einen Scheinwerfer")
            for e in c.devices():
                await self._show_ports(reader, e.uid, None, brief=True)
            self._hint("Prüfen: Scheinwerfer am richtigen Port? Port im MasterPort als DMX-Ausgang mit "
                          "aktiviertem RDM eingestellt? RDM im Scheinwerfer aktiv? Ausgang sendet DMX "
                          "(sACN-Universe am Port aktiv)? Kabel/Terminierung?")
        return addresses

    async def _show_ports(self, reader: DeviceReader, rpt_uid, supported, brief: bool = False) -> None:
        try:
            ports = await reader.gateway_ports(rpt_uid, supported)
        except RdmError as f:
            self._hint(f"Portübersicht von {rpt_uid} nicht lesbar: {f}")
            return
        g = next((x for x in self.result["gateways"] if x["rdm_uid"] == str(rpt_uid)), None)
        if g is not None:
            g["ports" if not brief else "ports_after_search"] = ports
        self.p(f"  Ports von {rpt_uid}{' (nach der Suche)' if brief else ''}:")
        for p in ports:
            parts = [f"Port {p['endpoint']}", p["type"]]
            for key, txt in (("mode", "Modus"), ("universe", "Universe"), ("name", "Name"),
                              ("rdm_active", "RDM"), ("discovery", "Discovery")):
                if p[key] is not None:
                    parts.append(f"{txt} {p[key]}")
            parts.append(f"{len(p['responder'])} Scheinwerfer")
            self.p("     " + ", ".join(str(t) for t in parts))

    async def _device(self, reader: DeviceReader, store: RdmStore, dmx_addr: DeviceAddress) -> None:
        self.p(f"\nScheinwerfer {dmx_addr}")
        p = await reader.profile(dmx_addr)
        gid = store.save_profile(p)
        entry = {"profile": p.to_dict()}
        if p.device_info is None:
            self._error(f"{dmx_addr}: DEVICE_INFO nicht lesbar ({p.unavailable})")
            self.result["devices"].append(entry)
            return
        di, pa = p.device_info, p.personality_current
        self.p(f"  {p.manufacturer} – {p.model} – Firmware {p.software_label} (Modell-ID {di.model_id:#06x}, "
               f"Software-ID {di.software_version_id:#010x})")
        self.p(f"  Modus {di.personality_current}/{di.personality_count} „{pa.description if pa else '?'}“, "
               f"Footprint {di.footprint}, Startadresse {di.start_address}, Universe {p.universe}")
        for x in p.personalities:
            self.p(f"     Modus {x.number}: {x.footprint:3d} Kanäle  „{x.description}“")
        self.p(f"  Betriebsstunden {p.device_hours}, Lampenstunden {p.lamp_hours}")
        if p.unavailable:
            self.p("  nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in p.unavailable.items()))
        s = await reader.sensor_snapshot(dmx_addr, p.sensors, all_sensors=True)
        store.save_snapshot(gid, s, "LIVETEST", "single")
        entry["sensors"] = s.to_dict()
        for w in s.values:
            rel = "" if w.measurement_relevant else "  (nicht messrelevant)"
            warn = "  ! außerhalb Normalbereich" if w.outside_normal else ""
            self.p(f"     Sensor {w.number} {w.description or w.type_text}: {w.value:g} {w.unit_text}{rel}{warn}")
        for f in s.error:
            self.p(f"     Sensorfehler: {f}")
        if self.k.expected:
            e = A.check(p, self.k.expected)
            store.save_check(gid, e, self.k.expected.gdtf_mode, "LIVETEST")
            entry["check"] = e.to_dict()
            self.p(f"  Abgleich: {e.status}, Messung erlaubt: {'Ja' if e.measurement_allowed else 'Nein'}")
            for x in e.deviations + e.hints:
                self.p(f"     - {x}")
        self.result["devices"].append(entry)

    # ----------------------------------------------------------------- Abschluss
    def _hint(self, text: str) -> None:
        self.result["hints"].append(text)
        self.p(f"  Hinweis: {text}")

    def _error(self, text: str) -> None:
        self.result["error"].append(text)
        self.p(f"  FEHLER: {text}")

    def _diagnostic_hints(self, broker: RdmnetBroker) -> None:
        kinds = [e["event"] for e in broker.events]
        if "tcp_connection" not in kinds:
            for t in ("Keine TCP-Verbindung angekommen. Prüfen: RDMnet im MasterPort aktiv, Broker-IP/Port richtig "
                      "eingetragen, gleiches Netz, macOS-Firewall erlaubt eingehende Verbindungen für Python.",
                      "mDNS: Scope im MasterPort muss exakt gleich sein (Groß-/Kleinschreibung); Mac und "
                      "MasterPort im selben Subnetz; mit „dns-sd -B _rdmnet._tcp“ im Terminal prüfen, ob der "
                      "Broker sichtbar ist. Notfalls --ohne-mdns und Adresse fest eintragen."):
                self._hint(t)
        elif "connect_rejected" in kinds:
            self._hint("Verbindung wurde abgelehnt – Grund siehe oben (meist Scope). Scope im MasterPort und "
                          "--scope angleichen.")
        else:
            self._hint("TCP-Verbindung ohne gültigen Connect – Paketmitschnitt bitte schicken.")

    def _abort(self, text: str) -> dict:
        self._error(text)
        self._write_files(None)
        return self.result

    def _succeeded(self) -> bool:
        return bool(self.result["devices"]) and not self.result["error"]

    def _finish(self, broker: RdmnetBroker) -> dict:
        self.result["end_utc"] = _now()
        self.result["broker_status"] = broker.status()
        self.result["success"] = self._succeeded()
        self._write_files(broker)
        self.p(f"\nErgebnis: {'ERFOLGREICH' if self.result['success'] else 'NICHT ERFOLGREICH'}")
        self.p(f"Ergebnisordner: {self.folder.resolve()}")
        self.p("Bitte den ganzen Ordner (oder bericht.md + paketmitschnitt.jsonl) zurückschicken.")
        return self.result

    def _write_files(self, broker: RdmnetBroker | None) -> None:
        self.folder.mkdir(parents=True, exist_ok=True)
        (self.folder / "ergebnis.json").write_text(json.dumps(self.result, ensure_ascii=False, indent=2),
                                                   encoding="utf-8")
        z = [f"# {self.report_title} pyrdmnet {__version__}", "",
             f"- Start: {self.result['start_utc']} (UTC)",
             f"- Broker: {self.k.host}:{self.k.port}, Scope „{self.k.scope}“",
             f"- System: {self.result['system']}, Python {self.result['python']}",
             f"- Ergebnis: {'success' if self.result.get('success') else 'nicht erfolgreich'}",
             f"- Modus: {self.report_mode}", "", "## Gateways", ""]
        for g in self.result["gateways"]:
            z.append(f"- {g['rdm_uid']}: {g['manufacturer']} – {g['model']} – Firmware {g['software_label']}")
            for p in g.get("ports_after_search") or g.get("ports") or []:
                z.append(f"  - Port {p['endpoint']}: Modus {p['mode']}, Universe {p['universe']}, "
                         f"Name {p['name']}, RDM {p['rdm_active']}, Scheinwerfer {len(p['responder'])}")
        z += ["", "## Scheinwerfer", ""]
        for e in self.result["devices"]:
            p = e["profile"]
            di = p.get("device_info") or {}
            z.append(f"### {p['rdm_uid']} (Gateway {p['rpt_uid']}, Port {p['endpoint']})")
            z.append(f"- {p['manufacturer']} – {p['model']} – Firmware {p['software_label']}")
            z.append(f"- Modus {di.get('personality_current')}, Footprint {di.get('footprint')}, "
                     f"Startadresse {di.get('start_address')}, Universe {p.get('universe')}")
            for w in (e.get("sensors") or {}).get("values", []):
                z.append(f"- Sensor {w['number']} {w['description']}: {w['value']:g} {w['unit_text']}")
            if p.get("unavailable"):
                z.append("- Nicht verfügbar: " + ", ".join(f"{k} ({v})" for k, v in p["unavailable"].items()))
            if e.get("check"):
                z.append(f"- Abgleich: {e['check']['status']}")
            z.append("")
        if self.result["error"] or self.result["hints"]:
            z += ["## Fehler und Hinweise", ""] + [f"- FEHLER: {x}" for x in self.result["error"]] \
                 + [f"- {x}" for x in self.result["hints"]]
        if broker:
            z += ["", "## Broker-Zähler", "", "```", json.dumps(broker.status()["counters"], indent=2), "```"]
        (self.folder / "bericht.md").write_text("\n".join(z) + "\n", encoding="utf-8")


def choose_adapter(input_fn=input, output=print) -> str | None:
    group = [(n, ip) for n, ip in network_adapters() if not ip.startswith("127.")]
    if not group:
        output("Keine Netzwerkadapter gefunden – bitte --host angeben.")
        return None
    output("Netzwerkadapter:")
    for i, (n, ip) in enumerate(group, 1):
        output(f"  {i}) {n:8} {ip}")
    response = input_fn("Nummer des Adapters zum MasterPort: ").strip()
    if not response.isdigit() or not 1 <= int(response) <= len(group):
        output("Ungültige Auswahl.")
        return None
    return group[int(response) - 1][1]
