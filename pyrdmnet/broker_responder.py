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
from . import constants as C
from . import rdm_params as R
from .rdm import RdmMessage, Uid

# E1.20 Produktkategorie „Data Distribution“ (Datenverteilung)
PRODUCT_CATEGORY_DATA_DISTRIBUTION = 0x0700

SUPPORTED_PIDS = (
    C.PID_SUPPORTED_PARAMETERS,
    C.PID_DEVICE_INFO,
    C.PID_MANUFACTURER_LABEL,
    C.PID_DEVICE_MODEL_DESCRIPTION,
    C.PID_SOFTWARE_VERSION_LABEL,
    C.PID_DEVICE_LABEL,
    C.PID_IDENTIFY_DEVICE,
    C.PID_COMPONENT_SCOPE,
    C.PID_SEARCH_DOMAIN,
    C.PID_BROKER_STATUS,
)


def _version_as_number(text: str) -> int:
    """'0.4.1' -> 0x00000401 (je Teil ein Byte, für DEVICE_INFO)."""
    parts = [int(t) if t.isdigit() else 0 for t in text.split(".")[:4]]
    number = 0
    for t in parts:
        number = (number << 8) | (t & 0xFF)
    return number


def build_component_scope(slot: int, scope: str) -> bytes:
    """COMPONENT_SCOPE-Antwort des Brokers: immer ohne statische Konfiguration (E1.33 7.6.2)."""
    return R.build_component_scope(R.ComponentScope(slot, scope))


@dataclass
class BrokerResponderInfo:
    uid: Uid
    scope: str
    search_domain: str = C.E133_DEFAULT_DOMAIN
    manufacturer_label: str = "pyrdmnet"
    model: str = "pyrdmnet Broker"
    device_label: str = "pyrdmnet Broker"
    model_id: int = 0x0001
    software_label: str = f"pyrdmnet {__version__}"


class BrokerResponder:
    """Beantwortet RDM-Befehle, die an die Broker-UID gerichtet sind."""

    def __init__(self, data: BrokerResponderInfo, event: Callable[..., None] | None = None):
        self.data = data
        self.identifying = False
        self._event = event or (lambda kind, **kw: None)

    def respond(self, a: RdmMessage) -> RdmMessage | None:
        """Antwort auf eine RDM-Anfrage; None bei Broadcast (E1.20: keine Antwort)."""
        if a.dest.is_broadcast:
            return None
        nack = lambda reason: a.response(C.RESPONSE_NACK, struct.pack(">H", reason))  # noqa: E731
        if a.subdevice != C.RDM_ROOT_DEVICE:
            return nack(C.NR_SUB_DEVICE_OUT_OF_RANGE)
        if a.command_class == C.CC_GET:
            result = self._get(a.pid, a.data)
            if isinstance(result, int):
                return nack(result)
            return a.response(C.RESPONSE_ACK, result)
        if a.command_class == C.CC_SET:
            if a.pid == C.PID_IDENTIFY_DEVICE:
                if len(a.data) != 1 or a.data[0] > 1:
                    return nack(C.NR_FORMAT_ERROR)
                self.identifying = bool(a.data[0])
                self._event("broker_identify", active=self.identifying, by=str(a.source))
                return a.response(C.RESPONSE_ACK)
            return nack(C.NR_UNSUPPORTED_COMMAND_CLASS if a.pid in SUPPORTED_PIDS else C.NR_UNKNOWN_PID)
        return nack(C.NR_UNSUPPORTED_COMMAND_CLASS)

    def _get(self, pid: int, d: bytes) -> bytes | int:
        dd = self.data
        if pid == C.PID_SUPPORTED_PARAMETERS:
            # E1.20: die Pflicht-PIDs (SUPPORTED_PARAMETERS, DEVICE_INFO, SOFTWARE_VERSION_LABEL,
            # IDENTIFY_DEVICE) werden nicht aufgeführt
            required_pids = {C.PID_SUPPORTED_PARAMETERS, C.PID_DEVICE_INFO, C.PID_SOFTWARE_VERSION_LABEL,
                       C.PID_IDENTIFY_DEVICE}
            return b"".join(struct.pack(">H", p) for p in SUPPORTED_PIDS if p not in required_pids)
        if pid == C.PID_DEVICE_INFO:
            return R.build_device_info(R.DeviceInfo(0x0100, dd.model_id, PRODUCT_CATEGORY_DATA_DISTRIBUTION,
                                                   _version_as_number(__version__), 0, 0, 0, 0xFFFF, 0, 0))
        if pid == C.PID_MANUFACTURER_LABEL:
            return R.build_text(dd.manufacturer_label)
        if pid == C.PID_DEVICE_MODEL_DESCRIPTION:
            return R.build_text(dd.model)
        if pid == C.PID_SOFTWARE_VERSION_LABEL:
            return R.build_text(dd.software_label)
        if pid == C.PID_DEVICE_LABEL:
            return R.build_text(dd.device_label)
        if pid == C.PID_IDENTIFY_DEVICE:
            return bytes((int(self.identifying),))
        if pid == C.PID_COMPONENT_SCOPE:
            if len(d) != 2:
                return C.NR_FORMAT_ERROR
            (slot,) = struct.unpack(">H", d)
            if slot != 1:  # der Broker bedient genau einen Scope
                return C.NR_DATA_OUT_OF_RANGE
            return build_component_scope(1, dd.scope)
        if pid == C.PID_SEARCH_DOMAIN:
            return dd.search_domain.encode("utf-8")[:C.E133_DOMAIN_LENGTH]
        if pid == C.PID_BROKER_STATUS:
            return bytes((0, C.E133_BROKER_ACTIVE))  # Set Allowed = 0, Zustand aktiv
        return C.NR_UNKNOWN_PID
