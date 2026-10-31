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
6. Optional additional checks (``LiveConfig.checks``, CLI ``--checks``), all without RDM SET:
   * ``comms``: TCP_COMMS_STATUS of every gateway (E1.33 7.6.3)
   * ``controllers``: two controllers at once on one fixture (fair queuing, 9.2.10)
   * ``llrp``: find the gateways via LLRP and read scope / search domain (section 5)
   * ``restart``: restart the broker and measure how fast the gateways reconnect (6.2.5)
7. Store everything in a result folder: report, JSON, SQLite,
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
import sys
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
_IPV4 = r"(\d+\.\d+\.\d+\.\d+)"


def _run_text(command: list[str]) -> str | None:
    try:
        p = subprocess.run(command, capture_output=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    # ipconfig writes in the console code page; only names may suffer, addresses are ASCII
    return p.stdout.decode("utf-8", errors="replace") if p.returncode == 0 else None


def parse_ifconfig(text: str) -> list[tuple[str, str]]:
    """``ifconfig`` output (macOS, older Linux)."""
    adapters, name = [], None
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+):? ", line)
        if m and not line.startswith((" ", "\t")):
            name = m.group(1).rstrip(":")
            continue
        m = re.search(r"\binet (?:addr:)?" + _IPV4, line)
        if m and name:
            adapters.append((name, m.group(1)))
    return adapters


def parse_ip_addr(text: str) -> list[tuple[str, str]]:
    """``ip -4 -o addr show`` output (Linux without ifconfig)."""
    adapters = []
    for line in text.splitlines():
        m = re.match(r"^\d+:\s+(\S+)\s+inet\s+" + _IPV4, line)
        if m:
            adapters.append((m.group(1), m.group(2)))
    return adapters


def parse_ipconfig(text: str) -> list[tuple[str, str]]:
    """``ipconfig`` output (Windows, any display language: matches the "IPv4" line of each adapter)."""
    adapters, name = [], None
    for line in text.splitlines():
        if line and not line[0].isspace() and line.rstrip().endswith(":"):
            name = line.rstrip()[:-1].strip()
            continue
        m = re.search(r"IPv4[^:]*:\s*" + _IPV4, line)
        if m and name:
            adapters.append((name, m.group(1)))
    return adapters


def network_adapters(text: str | None = None) -> list[tuple[str, str]]:
    """(name, IPv4) of all adapters that have an address.

    Windows: ``ipconfig``; macOS/Linux: ``ifconfig``, falling back to ``ip -4 -o addr show``.
    ``text`` (ifconfig format) is only used by tests.
    """
    if text is not None:
        return parse_ifconfig(text)
    if sys.platform == "win32":
        out = _run_text(["ipconfig"])
        return parse_ipconfig(out) if out else []
    out = _run_text(["ifconfig"])
    if out and (adapters := parse_ifconfig(out)):
        return adapters
    out = _run_text(["ip", "-4", "-o", "addr", "show"])
    return parse_ip_addr(out) if out else []


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
    # pauses of the lab flow (tests use short values)
    settle_s: float = 3.0  # after the first device connects: time for further gateways
    poll_s: float = 0.5  # interval while waiting for devices
    search_retry_s: float = 5.0  # between searches for responders behind the gateway ports
    mdns_self_test_s: float = 4.0
    mdns_check_s: float = 3.0  # search for other brokers on the scope before registering (E1.33 9.1.4)
    dnssd: bool = True
    expected: A.ExpectedSetting | None = None
    checks: tuple[str, ...] = ()  # additional checks, see ALL_CHECKS
    restart_wait_s: float = 60.0  # restart check: how long to wait for the gateways to come back
    restart_pause_s: float = 2.0  # restart check: broker down for this long
    llrp_network: object | None = None  # llrp check: LlrpNetwork (default: multicast on the adapter ``host``)


ALL_CHECKS = ("comms", "controllers", "llrp", "restart")
DEFAULT_CHECKS = ("comms", "controllers")  # read-only and without interruption


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
                               "gateways": [], "devices": [], "checks": {}, "error": [], "hints": []}
        self.broker: RdmnetBroker | None = None
        self._announcer = None
        self._addresses: list[DeviceAddress] = []

    # ----------------------------------------------------------------- Flow
    async def run(self) -> dict:
        self.folder.mkdir(parents=True, exist_ok=True)
        if not ip_is_local(self.k.host):
            return self._abort(f"IP {self.k.host} does not belong to any network adapter of this computer")
        broker = self.broker = RdmnetBroker(self._broker_config(self.k.port))
        try:
            await broker.start()
        except OSError as f:
            return self._abort(f"Broker cannot start on {self.k.host}:{self.k.port}: {f}")
        self.result["broker_uid"] = str(broker.config.uid)
        announcer = None
        try:
            self.p(f"\nBroker running: {self.k.host}:{broker.port}, scope \"{self.k.scope}\", UID {broker.config.uid}")
            if self.k.dnssd:
                announcer = self._announcer = await self._announce(broker)
            reg = announcer.registration if announcer else None
            self.result["mdns"] = {"active": reg is not None, "method": reg.method if reg else None,
                                   "name": reg.name if reg else None,
                                   "standby": bool(announcer and announcer.standby),
                                   "other_brokers": [r.describe() for r in announcer.conflicts] if announcer else []}
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
            if self.k.checks:
                await self._additional_checks()
            return self._finish(self.broker)
        finally:
            if announcer:
                await announcer.stop()
            await self.broker.stop()

    def _broker_config(self, port: int, previous: RdmnetBroker | None = None) -> BrokerConfig:
        same = {"cid": previous.config.cid, "uid": previous.config.uid} if previous else {}  # restart: same identity
        return BrokerConfig(host=self.k.host, port=port, scope=self.k.scope, **same,
                            event_file=self.folder / "broker_events.jsonl",
                            capture_file=self.folder / "packet_capture.jsonl")

    async def _announce(self, broker: RdmnetBroker):
        """Check the scope for other brokers, then register via mDNS (E1.33 9.1.4)."""
        from .dnssd import BrokerAnnouncer
        announcer = BrokerAnnouncer(broker, self.k.host, check_s=self.k.mdns_check_s, on_message=self._hint)
        try:
            await announcer.start()
        except Exception as f:
            self._hint(f"mDNS registration failed: {f} - the broker must be entered statically in the MasterPort")
            return None
        if announcer.standby:
            self._hint("Another broker on this scope may take the MasterPort's connection instead of this one. "
                       "Stop the other broker or use a different scope; until then enter this broker statically.")
            return announcer
        reg = announcer.registration
        self.p(f"mDNS: registered as \"{reg.name}\" ({reg.method}), type _rdmnet._tcp with subtype _{self.k.scope}")
        found = await asyncio.to_thread(reg.self_test, self.k.mdns_self_test_s)
        if reg.method == "dns-sd":
            if found:
                self.p("mDNS self-test: broker discoverable via the scope subtype - OK")
            else:
                self._hint("mDNS self-test: own service not found via the scope subtype")
            self.result["mdns_self_test"] = found
        return announcer

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
                await asyncio.sleep(self.k.settle_s)  # give further gateways a moment to connect
                return [c["uid"] for c in broker.status()["clients"] if c["type"] == "Device"]
            await asyncio.sleep(self.k.poll_s)
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
            addresses = self._addresses = await self._find_responders(c, reader)
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
            self.p(f"  no fixtures reported on the gateway ports yet - retrying in {self.k.search_retry_s:g} s")
            await asyncio.sleep(self.k.search_retry_s)
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

    # ----------------------------------------------------------------- Additional checks
    async def _additional_checks(self) -> None:
        self.p("\nAdditional checks: " + ", ".join(self.k.checks))
        for name in ALL_CHECKS:  # fixed order: the disruptive restart comes last
            if name not in self.k.checks:
                continue
            try:
                result = await getattr(self, f"_check_{name}")()
            except Exception as f:  # a check must not end the live test
                result = {"status": "failed", "error": repr(f)}
            self.result["checks"][name] = result
            self.p(f"  -> {name}: {result['status'].upper()}")
            if result["status"] == "failed":
                self._hint(f"Check {name} failed: {result.get('error') or result}")

    async def _controller(self) -> RdmnetController:
        c = RdmnetController()
        await c.connect(self.k.host, self.broker.port, timeout_s=self.k.rdm_timeout_s)
        await c.client_list()
        return c

    async def _check_comms(self) -> dict:
        """TCP_COMMS_STATUS (E1.33 7.6.3) of every gateway: required for devices."""
        c = await self._controller()
        gateways = {}
        try:
            for e in c.devices():
                try:
                    entries = await c.read_tcp_comms_status(e.uid, timeout_s=self.k.rdm_timeout_s)
                    gateways[str(e.uid)] = [{"scope": x.scope, "broker": x.broker_address,
                                             "unhealthy_events": x.unhealthy_events} for x in entries]
                    for x in entries:
                        self.p(f"  {e.uid}: scope \"{x.scope}\", broker {x.broker_address}, "
                               f"unhealthy TCP events {x.unhealthy_events}")
                except RdmError as f:
                    gateways[str(e.uid)] = {"error": str(f)}
                    self.p(f"  {e.uid}: TCP_COMMS_STATUS not readable: {f}")
        finally:
            await c.disconnect()
        ok = gateways and all(isinstance(v, list) for v in gateways.values())
        return {"status": "ok" if ok else "not supported" if gateways else "failed", "gateways": gateways}

    async def _check_controllers(self, busy: int = 20, quiet: int = 5) -> dict:
        """Two controllers at once (9.2.10): A sends many requests, B a few afterwards; B must not wait for all
        of A's. The target is the first fixture (RDM on the DMX line is the bottleneck), else a gateway."""
        if self._addresses:
            a = self._addresses[0]
            target = dict(rpt_dest=a.rpt_uid, endpoint=a.endpoint, rdm_dest=a.rdm_uid)
        else:
            gateways = [g["rdm_uid"] for g in self.result["gateways"]]
            if not gateways:
                return {"status": "skipped", "reason": "no gateway"}
            from .rdm import Uid
            target = dict(rpt_dest=Uid.from_text(gateways[0]))
        ca, cb = await self._controller(), await self._controller()
        done_a = 0
        errors: list[str] = []
        timeout = max(self.k.rdm_timeout_s, 2.0) * 4

        async def get(c):
            nonlocal done_a
            try:
                await c.rdm_get(pid=C.PID_DEVICE_INFO, timeout_s=timeout, **target)
                if c is ca:
                    done_a += 1
            except RdmError as f:
                errors.append(f"{'A' if c is ca else 'B'}: {f}")
        try:
            start = time.monotonic()
            busy_tasks = [asyncio.create_task(get(ca)) for _ in range(busy)]
            await asyncio.sleep(0)  # A's requests go out first, B follows at once
            answered_at_start = done_a
            await asyncio.gather(*(get(cb) for _ in range(quiet)))
            quiet_s, answered_a = time.monotonic() - start, done_a
            await asyncio.gather(*busy_tasks)
            total_s = time.monotonic() - start
        finally:
            await ca.disconnect()
            await cb.disconnect()
        self.p(f"  target {target['rpt_dest']} endpoint {target.get('endpoint', 0)}: A {busy} requests, "
               f"B {quiet} requests; B done after {quiet_s:.2f} s with {answered_a} of A's answered, "
               f"all done after {total_s:.2f} s, {len(errors)} error(s)")
        if errors:
            status = "failed"
        elif answered_at_start >= busy or total_s < 0.2:  # too fast to see any queuing: no statement possible
            status = "inconclusive"
        else:
            status = "ok" if answered_a < busy else "failed"  # B must not wait for all of A's requests
        return {"status": status, "target": str(target["rpt_dest"]),
                "endpoint": target.get("endpoint", 0), "busy_requests": busy, "quiet_requests": quiet,
                "quiet_done_s": round(quiet_s, 3), "busy_answered_when_quiet_done": answered_a,
                "total_s": round(total_s, 3), "errors": errors[:10],
                "requests_per_s": round((busy + quiet - len(errors)) / total_s, 1) if total_s else None}

    async def _check_llrp(self) -> dict:
        """Find the gateways via LLRP (also connected ones) and read scope and search domain (read only)."""
        from .llrp import LlrpManager, LlrpNetwork
        rdmnet = {g["rdm_uid"] for g in self.result["gateways"]}
        found = []
        async with LlrpManager(self.k.llrp_network or LlrpNetwork(adapter=self.k.host)) as m:
            self.p(f"  LLRP via {' + '.join(m.families)}")
            for z in await m.discover(filter=0):
                entry = {"uid": str(z.uid), "cid": str(z.cid), "type": z.type_text, "address": z.address,
                         "hardware": z.hardware_text, "rdmnet_gateway": str(z.uid) in rdmnet}
                try:
                    cs = await m.read_scope(z)
                    entry.update(scope=cs.scope, static_broker=cs.static_address,
                                 search_domain=await m.read_search_domain(z))
                except RdmError as f:
                    entry["error"] = str(f)
                found.append(entry)
                self.p(f"  {z.uid} {z.type_text} {z.address}: scope \"{entry.get('scope', '?')}\", "
                       f"broker {entry.get('static_broker') or 'via mDNS'}, domain {entry.get('search_domain', '?')}"
                       + (f", ERROR {entry['error']}" if "error" in entry else ""))
            families = m.families
        missing = sorted(rdmnet - {e["uid"] for e in found})
        for uid in missing:
            self.p(f"  {uid}: not found via LLRP")
        status = "ok" if found and not missing and not any("error" in e for e in found) else \
            "not supported" if not found else "failed"
        return {"status": status, "families": families, "targets": found, "missing": missing}

    async def _check_restart(self) -> dict:
        """Restart the broker and measure how fast every gateway reconnects (E1.33 6.2.5)."""
        before = {c["cid"]: c["uid"] for c in self.broker.status()["clients"] if c["type"] == "Device"}
        port = self.broker.port
        self.p(f"  stopping the broker for {self.k.restart_pause_s:g} s ({len(before)} gateway(s) connected)")
        old = self.broker
        await old.stop()
        await asyncio.sleep(self.k.restart_pause_s)
        self.broker = RdmnetBroker(self._broker_config(port, old))
        await self.broker.start()
        if self._announcer is not None:
            self._announcer.broker = self.broker
        start = time.monotonic()
        back: dict[str, float] = {}
        while time.monotonic() - start < self.k.restart_wait_s and len(back) < len(before):
            for c in self.broker.status()["clients"]:
                if c["cid"] in before and c["cid"] not in back:
                    back[c["cid"]] = round(time.monotonic() - start, 2)
                    self.p(f"  {c['uid']} back after {back[c['cid']]:.1f} s")
            await asyncio.sleep(self.k.poll_s)
        gateways = [{"cid": cid, "uid_before": uid, "reconnect_s": back.get(cid)} for cid, uid in before.items()]
        for g in gateways:
            if g["reconnect_s"] is None:
                self.p(f"  {g['uid_before']} NOT back within {self.k.restart_wait_s:g} s")
        return {"status": "ok" if before and len(back) == len(before) else "failed", "gateways": gateways,
                "pause_s": self.k.restart_pause_s}

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
            firewall = {"win32": "Windows Defender Firewall allows Python on this network (the network profile "
                                 "must not be 'Public', or allow TCP port " + str(self.k.port) + " inbound)",
                        "darwin": "macOS firewall allows incoming connections for Python"}.get(
                sys.platform, "the firewall allows incoming TCP connections on port " + str(self.k.port))
            for t in ("No TCP connection received. Check: RDMnet enabled in the MasterPort, broker IP/port entered "
                      f"correctly, same network, {firewall}.",
                      "mDNS: the scope in the MasterPort must match exactly (case-sensitive); this computer and "
                      "the MasterPort in the same subnet; run \"dns-sd -B _rdmnet._tcp\" in a terminal to check whether "
                      "the broker is visible (Windows: requires Bonjour, otherwise pip install zeroconf). If necessary "
                      "use --no-mdns and enter the address statically."):
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
        if self.result["checks"]:
            z += ["## Additional checks", ""]
            for name, r in self.result["checks"].items():
                z.append(f"### {name}: {r['status']}")
                z += ["```", json.dumps({k: v for k, v in r.items() if k != "status"}, ensure_ascii=False,
                                        indent=2), "```", ""]
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
