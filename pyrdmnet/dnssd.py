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

import shutil
import socket
import subprocess
import threading
import time
import uuid

from . import constants as C
from .rdm import Uid

SERVICE_TYPE = "_rdmnet._tcp"


def txt_entries(scope: str, cid: uuid.UUID, uid: Uid, model: str = "pyrdmnet Broker",
                  manufacturer: str = "GelMatch") -> dict[str, str]:
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
