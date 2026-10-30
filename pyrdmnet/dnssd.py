"""DNS-SD/mDNS-Anmeldung des Brokers nach E1.33.

Angemeldet wird der Dienst ``_rdmnet._tcp`` mit dem Scope-Subtyp
``_<scope>._sub._rdmnet._tcp`` – darüber suchen RDMnet-Geräte den Broker
ihres Scopes. TXT-Einträge wie in der ETC-Referenzimplementierung:
TxtVers, E133Scope, E133Vers, CID (ohne Bindestriche), UID (ohne
Doppelpunkt), Model, Manuf.

Wege (in dieser Reihenfolge):
1. ``dns-sd`` (Bonjour) – auf macOS immer vorhanden, unter Windows mit
   Bonjour. Läuft über den System-mDNS-Dienst, meldet auf allen Adaptern an
   und unterstützt Subtypen. Empfohlen.
2. Python-Paket ``zeroconf`` (Linux/Windows ohne Bonjour). Subtyp-Anmeldung
   hier ungetestet.
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
    """Befehlszeile für ``dns-sd -R`` inkl. Scope-Subtyp und TXT-Einträgen."""
    txt = [f"{k}={v}" for k, v in txt_entries(scope, cid, uid).items()]
    return ["dns-sd", "-R", service_name(scope, cid), f"{SERVICE_TYPE},_{scope}", "local.", str(port), *txt]


class DnssdRegistration:
    """Meldet den Broker per mDNS an. Wirft RuntimeError, wenn kein Weg verfügbar ist."""

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
            raise ValueError(f"Unbekannter DNS-SD-Weg {method!r}")

    # --- Bonjour
    def _with_dns_sd(self) -> None:
        if not shutil.which("dns-sd"):
            raise RuntimeError("Befehl 'dns-sd' nicht gefunden (Bonjour)")
        self._proc = subprocess.Popen(dns_sd_command(self.scope, self.cid, self.uid, self.port),
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._read_output, daemon=True).start()
        # auf Bestätigung warten ("Name now registered and active")
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if any("registered" in z.lower() for z in self.output):
                return
            if self._proc.poll() is not None:
                raise RuntimeError("dns-sd beendet: " + " | ".join(self.output))
            time.sleep(0.1)
        raise RuntimeError("dns-sd: keine Bestätigung der Anmeldung binnen 5 s: " + " | ".join(self.output))

    def _read_output(self) -> None:
        for line in self._proc.stdout:
            self.output.append(line.rstrip())
            del self.output[:-50]

    # --- zeroconf
    def _with_zeroconf(self) -> None:
        try:
            from zeroconf import ServiceInfo, Zeroconf  # type: ignore
        except ImportError as error:
            raise RuntimeError("Weder 'dns-sd' (Bonjour) noch das Paket 'zeroconf' verfügbar "
                               "(pip install zeroconf)") from error
        type = f"{SERVICE_TYPE}.local."
        self._zc_info = ServiceInfo(
            type, f"{self.name}.{type}", addresses=[socket.inet_aton(self.ip)], port=self.port,
            properties=txt_entries(self.scope, self.cid, self.uid), server=f"pyrdmnet-{self.cid.hex[:8]}.local.")
        self._zc = Zeroconf(interfaces=[self.ip])
        self._zc.register_service(self._zc_info)
        self.output.append("zeroconf: angemeldet (ohne Scope-Subtyp)")

    # --- Prüfung und Ende
    def self_test(self, duration_s: float = 4.0) -> list[str]:
        """Sucht den eigenen Dienst über den Scope-Subtyp (nur mit dns-sd). Liefert die gefundenen Zeilen."""
        if self.method != "dns-sd":
            return []
        command = ["dns-sd", "-B", f"{SERVICE_TYPE},_{self.scope}", "local."]
        try:
            p = subprocess.run(command, capture_output=True, text=True, timeout=duration_s)
            text = p.stdout
        except subprocess.TimeoutExpired as t:  # dns-sd -B läuft endlos; Timeout ist der Normalfall
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
