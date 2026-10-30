"""RDM-Responder des Brokers (ANSI E1.33, Abschnitt 6.2).

Ein Controller erreicht den Broker über eine RPT-Request-PDU mit der
Broker-UID als Ziel und dem NULL-Endpoint. Der Broker antwortet wie ein
RDMnet-Gerät mit einer RPT-Notification (Anfrage + Antwort).

Unterstützt (nur GET, außer IDENTIFY_DEVICE):
* E1.20: SUPPORTED_PARAMETERS, DEVICE_INFO, MANUFACTURER_LABEL,
  DEVICE_MODEL_DESCRIPTION, SOFTWARE_VERSION_LABEL, DEVICE_LABEL,
  IDENTIFY_DEVICE (GET/SET, ohne physische Wirkung)
* E1.33: COMPONENT_SCOPE, SEARCH_DOMAIN, BROKER_STATUS

Scope, Suchdomäne und Broker-Zustand lassen sich bewusst nicht per RDM
ändern (NACK UNSUPPORTED_COMMAND_CLASS, BROKER_STATUS meldet „Set Allowed“ = 0):
Ein Scopewechsel würde alle Clients trennen und gehört in die Konfiguration.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Callable

from . import __version__
from . import konstanten as K
from . import rdm_parameter as R
from .rdm import RdmNachricht, Uid

# E1.20 Produktkategorie „Data Distribution“ (Datenverteilung)
PRODUKTKATEGORIE_DATENVERTEILUNG = 0x0700

UNTERSTUETZTE_PIDS = (
    K.PID_SUPPORTED_PARAMETERS,
    K.PID_DEVICE_INFO,
    K.PID_MANUFACTURER_LABEL,
    K.PID_DEVICE_MODEL_DESCRIPTION,
    K.PID_SOFTWARE_VERSION_LABEL,
    K.PID_DEVICE_LABEL,
    K.PID_IDENTIFY_DEVICE,
    K.PID_COMPONENT_SCOPE,
    K.PID_SEARCH_DOMAIN,
    K.PID_BROKER_STATUS,
)


def _version_als_zahl(text: str) -> int:
    """'0.4.1' -> 0x00000401 (je Teil ein Byte, für DEVICE_INFO)."""
    teile = [int(t) if t.isdigit() else 0 for t in text.split(".")[:4]]
    zahl = 0
    for t in teile:
        zahl = (zahl << 8) | (t & 0xFF)
    return zahl


def baue_component_scope(slot: int, scope: str) -> bytes:
    """COMPONENT_SCOPE-Antwort: Slot, Scope (63 Byte, nullterminiert), keine statische Konfiguration."""
    roh = scope.encode("utf-8")[:K.E133_SCOPE_LAENGE - 1]
    return (struct.pack(">H", slot) + roh.ljust(K.E133_SCOPE_LAENGE, b"\x00")
            + bytes((K.E133_KEINE_STATISCHE_KONFIG,)) + bytes(4 + 16) + struct.pack(">H", 0))


@dataclass
class BrokerResponderDaten:
    uid: Uid
    scope: str
    suchdomain: str = K.E133_STANDARD_DOMAIN
    hersteller_label: str = "pyrdmnet"
    modell: str = "pyrdmnet Broker"
    geraete_label: str = "pyrdmnet Broker"
    modell_id: int = 0x0001
    software_label: str = f"pyrdmnet {__version__}"


class BrokerResponder:
    """Beantwortet RDM-Befehle, die an die Broker-UID gerichtet sind."""

    def __init__(self, daten: BrokerResponderDaten, ereignis: Callable[..., None] | None = None):
        self.daten = daten
        self.identifizieren = False
        self._ereignis = ereignis or (lambda art, **kw: None)

    def beantworten(self, a: RdmNachricht) -> RdmNachricht | None:
        """Antwort auf eine RDM-Anfrage; None bei Broadcast (E1.20: keine Antwort)."""
        if a.ziel.ist_broadcast:
            return None
        nack = lambda grund: a.antwort(K.ANTWORT_NACK, struct.pack(">H", grund))  # noqa: E731
        if a.subdevice != K.RDM_ROOT_DEVICE:
            return nack(K.NR_SUB_DEVICE_OUT_OF_RANGE)
        if a.command_class == K.CC_GET:
            ergebnis = self._get(a.pid, a.daten)
            if isinstance(ergebnis, int):
                return nack(ergebnis)
            return a.antwort(K.ANTWORT_ACK, ergebnis)
        if a.command_class == K.CC_SET:
            if a.pid == K.PID_IDENTIFY_DEVICE:
                if len(a.daten) != 1 or a.daten[0] > 1:
                    return nack(K.NR_FORMAT_ERROR)
                self.identifizieren = bool(a.daten[0])
                self._ereignis("broker_identify", aktiv=self.identifizieren, von=str(a.quelle))
                return a.antwort(K.ANTWORT_ACK)
            return nack(K.NR_UNSUPPORTED_COMMAND_CLASS if a.pid in UNTERSTUETZTE_PIDS else K.NR_UNKNOWN_PID)
        return nack(K.NR_UNSUPPORTED_COMMAND_CLASS)

    def _get(self, pid: int, d: bytes) -> bytes | int:
        dd = self.daten
        if pid == K.PID_SUPPORTED_PARAMETERS:
            # E1.20: die Pflicht-PIDs (SUPPORTED_PARAMETERS, DEVICE_INFO, SOFTWARE_VERSION_LABEL,
            # IDENTIFY_DEVICE) werden nicht aufgeführt
            pflicht = {K.PID_SUPPORTED_PARAMETERS, K.PID_DEVICE_INFO, K.PID_SOFTWARE_VERSION_LABEL,
                       K.PID_IDENTIFY_DEVICE}
            return b"".join(struct.pack(">H", p) for p in UNTERSTUETZTE_PIDS if p not in pflicht)
        if pid == K.PID_DEVICE_INFO:
            return R.baue_device_info(R.DeviceInfo(0x0100, dd.modell_id, PRODUKTKATEGORIE_DATENVERTEILUNG,
                                                   _version_als_zahl(__version__), 0, 0, 0, 0xFFFF, 0, 0))
        if pid == K.PID_MANUFACTURER_LABEL:
            return R.baue_text(dd.hersteller_label)
        if pid == K.PID_DEVICE_MODEL_DESCRIPTION:
            return R.baue_text(dd.modell)
        if pid == K.PID_SOFTWARE_VERSION_LABEL:
            return R.baue_text(dd.software_label)
        if pid == K.PID_DEVICE_LABEL:
            return R.baue_text(dd.geraete_label)
        if pid == K.PID_IDENTIFY_DEVICE:
            return bytes((int(self.identifizieren),))
        if pid == K.PID_COMPONENT_SCOPE:
            if len(d) != 2:
                return K.NR_FORMAT_ERROR
            (slot,) = struct.unpack(">H", d)
            if slot != 1:  # der Broker bedient genau einen Scope
                return K.NR_DATA_OUT_OF_RANGE
            return baue_component_scope(1, dd.scope)
        if pid == K.PID_SEARCH_DOMAIN:
            return dd.suchdomain.encode("utf-8")[:K.E133_DOMAIN_LAENGE]
        if pid == K.PID_BROKER_STATUS:
            return bytes((0, K.E133_BROKER_AKTIV))  # Set Allowed = 0, Zustand aktiv
        return K.NR_UNKNOWN_PID
