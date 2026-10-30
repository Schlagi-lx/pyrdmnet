"""DNS-SD/mDNS registration of the broker according to E1.33.

The service ``_rdmnet._tcp`` is registered with the scope subtype
``_<scope>._sub._rdmnet._tcp`` - RDMnet devices use it to find the broker
for their scope. TXT records as in the ETC reference implementation:
TxtVers, E133Scope, E133Vers, CID (without hyphens), UID (without
colon), Model, Manuf.

Methods (in this order):
1. ``dns-sd`` (Bonjour) - always present on macOS; on Windows when Apple's
   Bonjour service is installed (e.g. "Bonjour Print Services for Windows" or
   iTunes). Uses the system mDNS service, registers on all adapters and
   supports subtypes. Recommended.
2. Python package ``zeroconf`` (Linux, Windows without Bonjour). Subtype
   registration untested here.
"""

from __future__ import annotations

import re
import shutil
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass

from . import constants as C
from .rdm import Uid

SERVICE_TYPE = "_rdmnet._tcp"


def txt_entries(scope: str, cid: uuid.UUID, uid: Uid, model: str = "pyrdmnet Broker",
                  manufacturer: str = "pyrdmnet") -> dict[str, str]:
    return {
        "TxtVers": "1",
        "E133Scope": scope,
        "E133Vers": str(C.E133_VERSION),
        "CID": cid.hex.upper(),
        "UID": f"{uid.manufacturer:04X}{uid.device:08X}",
        "Model": model,
        "Manuf": manufacturer,
    }


def service_name(scope: str, cid: uuid.UUID) -> str:
    return f"pyrdmnet Broker {scope} {cid.hex[:8]}"


def dns_sd_command(scope: str, cid: uuid.UUID, uid: Uid, port: int) -> list[str]:
    """Command line for ``dns-sd -R`` including scope subtype and TXT records."""
    txt = [f"{k}={v}" for k, v in txt_entries(scope, cid, uid).items()]
    return ["dns-sd", "-R", service_name(scope, cid), f"{SERVICE_TYPE},_{scope}", "local.", str(port), *txt]


class DnssdRegistration:
    """Registers the broker via mDNS. Raises RuntimeError if no method is available."""

    def __init__(self, scope: str, cid: uuid.UUID, uid: Uid, ip: str, port: int, method: str | None = None):
        self.scope, self.cid, self.uid, self.ip, self.port = scope, cid, uid, ip, port
        self.name = service_name(scope, cid)
        self._proc: subprocess.Popen | None = None
        self._zc = None
        self.output: list[str] = []
        method = method or ("dns-sd" if shutil.which("dns-sd") else "zeroconf")
        self.method = method
        if method == "dns-sd":
            self._with_dns_sd()
        elif method == "zeroconf":
            self._with_zeroconf()
        else:
            raise ValueError(f"Unknown DNS-SD method {method!r}")

    # --- Bonjour
    def _with_dns_sd(self) -> None:
        path = shutil.which("dns-sd")
        if not path:
            raise RuntimeError("Command 'dns-sd' not found (Bonjour)")
        # start via the resolved path: on Windows this also covers dns-sd.exe / .cmd outside the default search
        command = [path, *dns_sd_command(self.scope, self.cid, self.uid, self.port)[1:]]
        self._proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                      encoding="utf-8", errors="replace")
        threading.Thread(target=self._read_output, daemon=True).start()
        # wait for confirmation ("Name now registered and active")
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if any("registered" in z.lower() for z in self.output):
                return
            if self._proc.poll() is not None:
                raise RuntimeError("dns-sd exited: " + " | ".join(self.output))
            time.sleep(0.1)
        raise RuntimeError("dns-sd: no registration confirmation within 5 s: " + " | ".join(self.output))

    def _read_output(self) -> None:
        for line in self._proc.stdout:
            self.output.append(line.rstrip())
            del self.output[:-50]

    # --- zeroconf
    def _with_zeroconf(self) -> None:
        try:
            from zeroconf import ServiceInfo, Zeroconf  # type: ignore
        except ImportError as error:
            raise RuntimeError("Neither 'dns-sd' (Bonjour) nor the package 'zeroconf' is available "
                               "(pip install zeroconf)") from error
        type = f"{SERVICE_TYPE}.local."
        self._zc_info = ServiceInfo(
            type, f"{self.name}.{type}", addresses=[socket.inet_aton(self.ip)], port=self.port,
            properties=txt_entries(self.scope, self.cid, self.uid), server=f"pyrdmnet-{self.cid.hex[:8]}.local.")
        self._zc = Zeroconf(interfaces=[self.ip])
        self._zc.register_service(self._zc_info)
        self.output.append("zeroconf: registered (without scope subtype)")

    # --- Check and shutdown
    def self_test(self, duration_s: float = 4.0) -> list[str]:
        """Looks up our own service via the scope subtype (dns-sd only). Returns the matching lines."""
        if self.method != "dns-sd":
            return []
        command = [shutil.which("dns-sd") or "dns-sd", "-B", f"{SERVICE_TYPE},_{self.scope}", "local."]
        try:
            p = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace",
                               timeout=duration_s)
            text = p.stdout
        except subprocess.TimeoutExpired as t:  # dns-sd -B runs forever; a timeout is the normal case
            text = t.stdout.decode() if isinstance(t.stdout, bytes) else (t.stdout or "")
        return [z for z in text.splitlines() if self.name in z and " Add " in z]

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=3)
        if self._proc and self._proc.stdout:
            self._proc.stdout.close()
        if self._zc:
            try:
                self._zc.unregister_service(self._zc_info)
            finally:
                self._zc.close()


# ------------------------------------------------------------------- broker discovery (E1.33 6.2.3, 9.1.4)
@dataclass(frozen=True)
class BrokerRecord:
    """A broker found via DNS-SD for a scope."""

    name: str  # service instance name
    host: str = ""
    port: int = 0
    txt: tuple[tuple[str, str], ...] = ()

    @property
    def properties(self) -> dict[str, str]:
        return dict(self.txt)

    @property
    def cid(self) -> uuid.UUID | None:
        try:
            return uuid.UUID(hex=self.properties.get("CID", ""))
        except ValueError:
            return None

    @property
    def scope(self) -> str:
        return self.properties.get("E133Scope", "")

    def describe(self) -> str:
        where = f"{self.host}:{self.port}" if self.host else "address not resolved"
        return f"\"{self.name}\" at {where}" + (f", CID {self.cid}" if self.cid else "")


_BROWSE_LINE = re.compile(r"\s(Add|Rmv)\s+\d+\s+\d+\s+\S+\s+_rdmnet\._tcp\.\s+(.+?)\s*$")
_REACHED = re.compile(r"can be reached at (\S+?)\.?:(\d+)")


def parse_dns_sd_browse(text: str) -> list[str]:
    """Instance names from ``dns-sd -B`` output (Add/Rmv lines, duplicates per interface merged)."""
    names: dict[str, None] = {}
    for line in text.splitlines():
        m = _BROWSE_LINE.search(line)
        if not m:
            continue
        if m.group(1) == "Add":
            names[m.group(2)] = None
        else:
            names.pop(m.group(2), None)
    return list(names)


def parse_dns_sd_lookup(name: str, text: str) -> BrokerRecord:
    """Host, port and TXT records from ``dns-sd -L`` output."""
    host, port, txt = "", 0, []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = _REACHED.search(line)
        if m:
            host, port = m.group(1), int(m.group(2))
            if i + 1 < len(lines):  # TXT records follow on the next line: key=value separated by spaces
                for token in re.split(r"(?<!\\) ", lines[i + 1].strip()):
                    if "=" in token:
                        key, value = token.split("=", 1)
                        txt.append((key, value.replace("\\ ", " ")))
            break
    return BrokerRecord(name, host, port, tuple(txt))


def _run_until_timeout(command: list[str], duration_s: float) -> str:
    """dns-sd -B/-L run until stopped; a timeout is the normal end."""
    try:
        p = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=duration_s)
        return p.stdout
    except subprocess.TimeoutExpired as t:
        out = t.stdout
        return out.decode("utf-8", errors="replace") if isinstance(out, bytes) else (out or "")


def _find_with_dns_sd(scope: str, duration_s: float) -> list[BrokerRecord]:
    path = shutil.which("dns-sd")
    names = parse_dns_sd_browse(_run_until_timeout([path, "-B", f"{SERVICE_TYPE},_{scope}", "local."], duration_s))
    return [parse_dns_sd_lookup(n, _run_until_timeout([path, "-L", n, SERVICE_TYPE, "local."], min(duration_s, 2.0)))
            for n in names]


def _find_with_zeroconf(scope: str, duration_s: float) -> list[BrokerRecord]:
    from zeroconf import ServiceBrowser, Zeroconf  # type: ignore
    subtype = f"_{scope}._sub.{SERVICE_TYPE}.local."
    names: list[str] = []

    class Listener:
        def add_service(self, zc, type_, name):
            names.append(name)

        def remove_service(self, zc, type_, name):
            if name in names:
                names.remove(name)

        def update_service(self, zc, type_, name):
            pass

    zc = Zeroconf()
    try:
        ServiceBrowser(zc, subtype, Listener())
        time.sleep(duration_s)
        records = []
        for full in list(dict.fromkeys(names)):
            info = zc.get_service_info(f"{SERVICE_TYPE}.local.", full.replace(subtype, f"{SERVICE_TYPE}.local."), 2000)
            instance = full.split(".")[0]
            if info is None:
                records.append(BrokerRecord(instance))
                continue
            props = tuple((k.decode(errors="replace"), (v or b"").decode(errors="replace"))
                          for k, v in (info.properties or {}).items())
            addresses = info.parsed_addresses()
            records.append(BrokerRecord(instance, addresses[0] if addresses else (info.server or ""), info.port, props))
        return records
    finally:
        zc.close()


def find_brokers(scope: str, duration_s: float = 3.0, method: str | None = None) -> list[BrokerRecord]:
    """All brokers advertising the scope subtype via mDNS. Raises RuntimeError if no method is available."""
    method = method or ("dns-sd" if shutil.which("dns-sd") else "zeroconf")
    if method == "dns-sd":
        if not shutil.which("dns-sd"):
            raise RuntimeError("Command 'dns-sd' not found (Bonjour)")
        return _find_with_dns_sd(scope, duration_s)
    try:
        return _find_with_zeroconf(scope, duration_s)
    except ImportError as error:
        raise RuntimeError("Neither 'dns-sd' (Bonjour) nor the package 'zeroconf' is available "
                           "(pip install zeroconf)") from error


def is_own_record(record: BrokerRecord, scope: str, cid: uuid.UUID, port: int) -> bool:
    """9.1.4: records of the querying broker itself (same CID, port and scope) are ignored."""
    return record.cid == cid and (record.port in (0, port)) and record.scope in ("", scope)


def conflicting_brokers(scope: str, cid: uuid.UUID, port: int, duration_s: float = 3.0,
                        method: str | None = None) -> list[BrokerRecord]:
    return [r for r in find_brokers(scope, duration_s, method) if not is_own_record(r, scope, cid, port)]


# ------------------------------------------------------------------- start-up and monitoring (E1.33 9.1.4)
class BrokerAnnouncer:
    """DNS-SD registration of a broker following E1.33 9.1.4.

    1. Before publishing, look for other brokers on the scope. If there are any: alert,
       set the broker to STANDBY, do not publish, re-check every ``retry_s``.
    2. When no other broker is found (any more): register, set ACTIVE.
    3. While registered: re-check every ``monitor_s``; another broker only raises an alert.

    ``broker`` needs ``config`` (scope, cid, uid), ``port``, ``set_state()`` and ``_event()``
    (an :class:`~pyrdmnet.broker.RdmnetBroker`). ``finder`` and ``registrar`` can be replaced
    in tests. ``on_message`` receives human-readable alerts.
    """

    def __init__(self, broker, ip: str, *, method: str | None = None, check_s: float = 3.0,
                 retry_s: float = 15.0, monitor_s: float = 60.0, finder=None, registrar=None, on_message=None):
        self.broker, self.ip, self.method = broker, ip, method
        self.check_s, self.retry_s, self.monitor_s = check_s, retry_s, monitor_s
        self.finder = finder or conflicting_brokers
        self.registrar = registrar or DnssdRegistration
        self.on_message = on_message or (lambda text: None)
        self.registration = None
        self.conflicts: list[BrokerRecord] = []
        self._reported: set[str] = set()
        self._task = None

    @property
    def standby(self) -> bool:
        return self.registration is None

    async def start(self) -> None:
        """First check and, if possible, registration; then monitoring in the background.

        Raises RuntimeError if neither dns-sd nor zeroconf is available.
        """
        import asyncio
        conflicts = await self._check()
        if conflicts:
            self._enter_standby(conflicts)
        else:
            await self._register()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        import asyncio
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self.registration is not None:
            await asyncio.to_thread(self.registration.stop)
            self.registration = None

    async def _check(self) -> list[BrokerRecord]:
        import asyncio
        k = self.broker.config
        self.conflicts = await asyncio.to_thread(self.finder, k.scope, k.cid, self.broker.port, self.check_s,
                                                 self.method)
        return self.conflicts

    async def _register(self) -> None:
        import asyncio
        k = self.broker.config
        self.registration = await asyncio.to_thread(self.registrar, k.scope, k.cid, k.uid, self.ip, self.broker.port,
                                                    self.method)
        self.broker.set_state(C.E133_BROKER_ACTIVE, reason="DNS-SD registered")
        self.broker._event("dnssd_registered", name=getattr(self.registration, "name", ""),
                           method=getattr(self.registration, "method", ""))

    def _enter_standby(self, conflicts: list[BrokerRecord]) -> None:
        names = [r.describe() for r in conflicts]
        self.broker.set_state(C.E133_BROKER_STANDBY, reason="another broker on the scope", brokers=names)
        self.broker._event("scope_conflict", phase="start-up", brokers=names)
        self._reported = set(names)
        self.on_message(f"Configuration error: another broker already uses scope \"{self.broker.config.scope}\": "
                        + "; ".join(names) + ". This broker stays in STANDBY (not announced via mDNS) and "
                        f"re-checks every {self.retry_s:g} s.")

    async def _loop(self) -> None:
        import asyncio
        while True:
            await asyncio.sleep(self.retry_s if self.standby else self.monitor_s)
            try:
                conflicts = await self._check()
            except RuntimeError as error:
                self.broker._event("dnssd_error", error=str(error))
                continue
            names = {r.describe() for r in conflicts}
            if self.standby:
                if not conflicts:
                    await self._register()
                    self._reported = set()
                    self.on_message("The other broker on the scope is gone - registered via mDNS, broker ACTIVE.")
            elif names - self._reported:
                self.broker._event("scope_conflict", phase="running", brokers=sorted(names))
                self.on_message(f"Configuration error: another broker appeared on scope "
                                f"\"{self.broker.config.scope}\": " + "; ".join(sorted(names - self._reported)))
                self._reported |= names
            else:
                self._reported &= names
