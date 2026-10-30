"""Live test with real hardware (e.g. LSS MasterPort 2 as RDMnet gateway).

Flow (read-only, no RDM SET):
1. Choose a network adapter, start our own broker on its IP
   and register it via mDNS/DNS-SD (default; macOS: Bonjour "dns-sd")
2. Wait until RDMnet devices (gateways) connect; every connection and
   every rejection, including its reason, is shown immediately
3. Find devices and RDM responders behind the gateway ports (with retries,
   because RDM discovery on the gateway takes time)
4. Read out every gateway and every fixture, read all sensors
5. Optional: settings check against expected mode/address/universe (display only)
6. Store everything in a result folder: report, JSON, SQLite,
   broker events and packet capture (for troubleshooting)
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


# ------------------------------------------------------------------- Network adapters
def network_adapters(text: str | None = None) -> list[tuple[str, str]]:
    """(name, IPv4) of all adapters that have an address, via ifconfig (macOS/Linux)."""
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


# ------------------------------------------------------------------- Configuration
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
    folder_prefix = "live_test"
    report_title = "Live-Test"
    report_mode = "read-only (no RDM SET)"

    def __init__(self, k: LiveConfig, output=print):
        self.k = k
        self.p = output
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.folder = Path(k.folder) / f"{self.folder_prefix}_{stamp}"
        self.result: dict = {"version": __version__, "start_utc": _now(), "host": k.host, "port": k.port,
                               "scope": k.scope, "system": platform.platform(), "python": platform.python_version(),
                               "gateways": [], "devices": [], "error": [], "hints": []}

    # ----------------------------------------------------------------- Flow
    async def run(self) -> dict:
        self.folder.mkdir(parents=True, exist_ok=True)
        if not ip_is_local(self.k.host):
            return self._abort(f"IP {self.k.host} does not belong to any network adapter of this computer")
        broker = RdmnetBroker(BrokerConfig(host=self.k.host, port=self.k.port, scope=self.k.scope,
                                           event_file=self.folder / "broker_events.jsonl",
                                           capture_file=self.folder / "packet_capture.jsonl"))
        try:
            await broker.start()
        except OSError as f:
            return self._abort(f"Broker cannot start on {self.k.host}:{self.k.port}: {f}")
        self.result["broker_uid"] = str(broker.config.uid)
        reg = None
        try:
            self.p(f"\nBroker running: {self.k.host}:{broker.port}, scope \"{self.k.scope}\", UID {broker.config.uid}")
            if self.k.dnssd:
                reg = await asyncio.to_thread(self._register_mdns, broker)
            self.result["mdns"] = {"active": reg is not None, "method": reg.method if reg else None,
                                     "name": reg.name if reg else None}
            if reg:
                self.p("In the MasterPort, enable RDMnet with automatic broker discovery (mDNS/DNS-SD), "
                       f"scope \"{self.k.scope}\".")
            else:
                self.p("In the MasterPort, enable RDMnet and enter this broker statically:")
                self.p(f"   Address {self.k.host}   Port {broker.port}   Scope {self.k.scope}")
            self.p(f"Waiting up to {self.k.wait_for_devices_s:.0f} s for RDMnet devices ...  (Ctrl+C aborts)\n")
            gateways = await self._wait_for_devices(broker)
            if not gateways:
                self._error("No RDMnet device has connected")
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
            self._hint(f"mDNS registration failed: {f} - the broker must be entered statically in the MasterPort")
            return None
        self.p(f"mDNS: registered as \"{reg.name}\" ({reg.method}), type _rdmnet._tcp with subtype _{self.k.scope}")
        found = reg.self_test()
        if reg.method == "dns-sd":
            if found:
                self.p("mDNS self-test: broker discoverable via the scope subtype - OK")
            else:
                self._hint("mDNS self-test: own service not found via the scope subtype")
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
                    self.p(f"  TCP connection from {e['peer']}")
                elif kind == "connect_request":
                    self.p(f"  Connect: UID {e['uid']}, {e['type']}, scope \"{e['scope']}\", "
                           f"E1.33 version {e['version']}")
                elif kind == "connect_rejected":
                    self.p(f"  REJECTED ({e['peer']}): {e['reason']} - scope \"{e['scope']}\", UID {e['uid']}")
                elif kind == "client_connected":
                    self.p(f"  CONNECTED: {e['type']} {e['uid']} ({e['peer']})")
                elif kind in ("protocol_error", "internal_error"):
                    self.p(f"  PROTOCOL ERROR ({e['peer']}): {e['error']}")
            seen = len(events)
            devices = [c["uid"] for c in broker.status()["clients"] if c["type"] == "Device"]
            if devices:
                await asyncio.sleep(3)  # give further gateways a moment to connect
                return [c["uid"] for c in broker.status()["clients"] if c["type"] == "Device"]
            await asyncio.sleep(0.5)
        return devices

    async def _read_devices(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.connect(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.client_list()
        except Exception as f:
            self._error(f"Own controller cannot connect: {f}")
            return
        reader = DeviceReader(c, self.k.rdm_timeout_s)
        store = RdmStore(self.folder / "live_test.sqlite")
        try:
            # the gateways themselves (default responder)
            for e in c.devices():
                gp = await reader.profile(DeviceAddress(e.uid, 0, e.uid))
                self.result["gateways"].append(gp.to_dict())
                self.p(f"\nGateway {e.uid}: {gp.manufacturer or '?'} - {gp.model or '?'} - Firmware {gp.software_label or '?'}")
                if gp.unavailable:
                    self.p("  unavailable: " + ", ".join(f"{k} ({v})" for k, v in gp.unavailable.items()))
                await self._show_ports(reader, e.uid, gp.supported_pids)
            addresses = await self._find_responders(c, reader)
            for dmx_addr in addresses:  # endpoint 0 = native RDMnet device without ports
                await self._device(reader, store, dmx_addr)
        finally:
            store.close()
            await c.disconnect()

    async def _find_responders(self, c: RdmnetController, reader: DeviceReader) -> list[DeviceAddress]:
        """Find responders behind the ports; RDM discovery on the gateway takes time."""
        end = time.monotonic() + self.k.find_responders_s
        addresses: list[DeviceAddress] = []
        while True:
            try:
                addresses = await reader.find_devices()
            except RdmError as f:
                self._hint(f"Search: {f}")
            if addresses or time.monotonic() >= end:
                break
            self.p("  no fixtures reported on the gateway ports yet - retrying in 5 s")
            await asyncio.sleep(5)
        if not addresses:
            self._error("No RDM responders found - the gateway reports no fixture on any port")
            for e in c.devices():
                await self._show_ports(reader, e.uid, None, brief=True)
            self._hint("Check: fixture on the correct port? Port in the MasterPort configured as DMX output with "
                          "RDM enabled? RDM enabled in the fixture? Output sending DMX "
                          "(sACN universe active on the port)? Cable/termination?")
        return addresses

    async def _show_ports(self, reader: DeviceReader, rpt_uid, supported, brief: bool = False) -> None:
        try:
            ports = await reader.gateway_ports(rpt_uid, supported)
        except RdmError as f:
            self._hint(f"Port overview of {rpt_uid} not readable: {f}")
            return
        g = next((x for x in self.result["gateways"] if x["rdm_uid"] == str(rpt_uid)), None)
        if g is not None:
            g["ports" if not brief else "ports_after_search"] = ports
        self.p(f"  Ports of {rpt_uid}{' (after the search)' if brief else ''}:")
        for p in ports:
            parts = [f"Port {p['endpoint']}", p["type"]]
            for key, txt in (("mode", "Mode"), ("universe", "Universe"), ("name", "Name"),
                              ("rdm_active", "RDM"), ("discovery", "Discovery")):
                if p[key] is not None:
                    parts.append(f"{txt} {p[key]}")
            parts.append(f"{len(p['responder'])} fixture(s)")
            self.p("     " + ", ".join(str(t) for t in parts))

    async def _device(self, reader: DeviceReader, store: RdmStore, dmx_addr: DeviceAddress) -> None:
        self.p(f"\nFixture {dmx_addr}")
        p = await reader.profile(dmx_addr)
        gid = store.save_profile(p)
        entry = {"profile": p.to_dict()}
        if p.device_info is None:
            self._error(f"{dmx_addr}: DEVICE_INFO not readable ({p.unavailable})")
            self.result["devices"].append(entry)
            return
        di, pa = p.device_info, p.personality_current
        self.p(f"  {p.manufacturer} - {p.model} - Firmware {p.software_label} (model ID {di.model_id:#06x}, "
               f"software ID {di.software_version_id:#010x})")
        self.p(f"  Mode {di.personality_current}/{di.personality_count} \"{pa.description if pa else '?'}\", "
               f"footprint {di.footprint}, start address {di.start_address}, universe {p.universe}")
        for x in p.personalities:
            self.p(f"     Mode {x.number}: {x.footprint:3d} channels  \"{x.description}\"")
        self.p(f"  Device hours {p.device_hours}, lamp hours {p.lamp_hours}")
        if p.unavailable:
            self.p("  unavailable: " + ", ".join(f"{k} ({v})" for k, v in p.unavailable.items()))
        s = await reader.sensor_snapshot(dmx_addr, p.sensors, all_sensors=True)
        store.save_snapshot(gid, s, "LIVETEST", "single")
        entry["sensors"] = s.to_dict()
        for w in s.values:
            rel = "" if w.measurement_relevant else "  (not relevant for measurement)"
            warn = "  ! outside normal range" if w.outside_normal else ""
            self.p(f"     Sensor {w.number} {w.description or w.type_text}: {w.value:g} {w.unit_text}{rel}{warn}")
        for f in s.error:
            self.p(f"     Sensor error: {f}")
        if self.k.expected:
            e = A.check(p, self.k.expected)
            store.save_check(gid, e, self.k.expected.gdtf_mode, "LIVETEST")
            entry["check"] = e.to_dict()
            self.p(f"  Settings check: {e.status}, measurement allowed: {'yes' if e.measurement_allowed else 'no'}")
            for x in e.deviations + e.hints:
                self.p(f"     - {x}")
        self.result["devices"].append(entry)

    # ----------------------------------------------------------------- Completion
    def _hint(self, text: str) -> None:
        self.result["hints"].append(text)
        self.p(f"  Hint: {text}")

    def _error(self, text: str) -> None:
        self.result["error"].append(text)
        self.p(f"  ERROR: {text}")

    def _diagnostic_hints(self, broker: RdmnetBroker) -> None:
        kinds = [e["event"] for e in broker.events]
        if "tcp_connection" not in kinds:
            for t in ("No TCP connection received. Check: RDMnet enabled in the MasterPort, broker IP/port entered "
                      "correctly, same network, macOS firewall allows incoming connections for Python.",
                      "mDNS: the scope in the MasterPort must match exactly (case-sensitive); Mac and "
                      "MasterPort in the same subnet; run \"dns-sd -B _rdmnet._tcp\" in a terminal to check whether "
                      "the broker is visible. If necessary use --no-mdns and enter the address statically."):
                self._hint(t)
        elif "connect_rejected" in kinds:
            self._hint("Connection was rejected - see reason above (usually the scope). Scope in the MasterPort "
                          "and --scope must match.")
        else:
            self._hint("TCP connection without a valid connect - please send the packet capture.")

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
        self.p(f"\nResult: {'SUCCESSFUL' if self.result['success'] else 'NOT SUCCESSFUL'}")
        self.p(f"Result folder: {self.folder.resolve()}")
        self.p("Please send back the whole folder (or report.md + packet_capture.jsonl).")
        return self.result

    def _write_files(self, broker: RdmnetBroker | None) -> None:
        self.folder.mkdir(parents=True, exist_ok=True)
        (self.folder / "result.json").write_text(json.dumps(self.result, ensure_ascii=False, indent=2),
                                                   encoding="utf-8")
        z = [f"# {self.report_title} pyrdmnet {__version__}", "",
             f"- Start: {self.result['start_utc']} (UTC)",
             f"- Broker: {self.k.host}:{self.k.port}, scope \"{self.k.scope}\"",
             f"- System: {self.result['system']}, Python {self.result['python']}",
             f"- Result: {'successful' if self.result.get('success') else 'not successful'}",
             f"- Mode: {self.report_mode}", "", "## Gateways", ""]
        for g in self.result["gateways"]:
            z.append(f"- {g['rdm_uid']}: {g['manufacturer']} - {g['model']} - Firmware {g['software_label']}")
            for p in g.get("ports_after_search") or g.get("ports") or []:
                z.append(f"  - Port {p['endpoint']}: mode {p['mode']}, universe {p['universe']}, "
                         f"name {p['name']}, RDM {p['rdm_active']}, fixtures {len(p['responder'])}")
        z += ["", "## Fixtures", ""]
        for e in self.result["devices"]:
            p = e["profile"]
            di = p.get("device_info") or {}
            z.append(f"### {p['rdm_uid']} (Gateway {p['rpt_uid']}, Port {p['endpoint']})")
            z.append(f"- {p['manufacturer']} - {p['model']} - Firmware {p['software_label']}")
            z.append(f"- Mode {di.get('personality_current')}, footprint {di.get('footprint')}, "
                     f"start address {di.get('start_address')}, universe {p.get('universe')}")
            for w in (e.get("sensors") or {}).get("values", []):
                z.append(f"- Sensor {w['number']} {w['description']}: {w['value']:g} {w['unit_text']}")
            if p.get("unavailable"):
                z.append("- Unavailable: " + ", ".join(f"{k} ({v})" for k, v in p["unavailable"].items()))
            if e.get("check"):
                z.append(f"- Settings check: {e['check']['status']}")
            z.append("")
        if self.result["error"] or self.result["hints"]:
            z += ["## Errors and hints", ""] + [f"- ERROR: {x}" for x in self.result["error"]] \
                 + [f"- {x}" for x in self.result["hints"]]
        if broker:
            z += ["", "## Broker counters", "", "```", json.dumps(broker.status()["counters"], indent=2), "```"]
        (self.folder / "report.md").write_text("\n".join(z) + "\n", encoding="utf-8")


def choose_adapter(input_fn=input, output=print) -> str | None:
    group = [(n, ip) for n, ip in network_adapters() if not ip.startswith("127.")]
    if not group:
        output("No network adapters found - please specify --host.")
        return None
    output("Network adapters:")
    for i, (n, ip) in enumerate(group, 1):
        output(f"  {i}) {n:8} {ip}")
    response = input_fn("Number of the adapter facing the MasterPort: ").strip()
    if not response.isdigit() or not 1 <= int(response) <= len(group):
        output("Invalid selection.")
        return None
    return group[int(response) - 1][1]
