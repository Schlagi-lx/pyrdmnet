"""Default responder of RPT clients - controllers and devices (ANSI E1.33 7.2.1, 7.4.8).

Every RPT component answers RDM requests addressed to its own UID and the NULL
endpoint. Required (E1.33 table 7-1 and table A-15 for controllers and devices):

* E1.20: IDENTIFY_DEVICE, SUPPORTED_PARAMETERS, PARAMETER_DESCRIPTION,
  MANUFACTURER_LABEL, DEVICE_MODEL_DESCRIPTION, SOFTWARE_VERSION_LABEL,
  DEVICE_LABEL (plus DEVICE_INFO, required by E1.20 itself)
* E1.33: COMPONENT_SCOPE, SEARCH_DOMAIN, TCP_COMMS_STATUS

:class:`ClientResponder` answers all of them for a :class:`~pyrdmnet.client.RptClient`.
Devices with their own responder (the simulator) use :meth:`ClientResponder.respond_e133`
for the three E1.33 PIDs only.

SET COMPONENT_SCOPE changes the client's scope (and optionally its static broker); the
client then closes its connection and - with automatic reconnection - connects again in
the new scope. SET TCP_COMMS_STATUS resets the unhealthy-event counter of a scope.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

from . import __version__
from . import constants as C
from . import rdm_params as R
from .rdm import RdmFormatError, RdmMessage

if TYPE_CHECKING:
    from .client import RptClient

E133_CLIENT_PIDS = (C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_TCP_COMMS_STATUS)

SUPPORTED_PIDS = (
    C.PID_SUPPORTED_PARAMETERS,
    C.PID_PARAMETER_DESCRIPTION,
    C.PID_DEVICE_INFO,
    C.PID_MANUFACTURER_LABEL,
    C.PID_DEVICE_MODEL_DESCRIPTION,
    C.PID_SOFTWARE_VERSION_LABEL,
    C.PID_DEVICE_LABEL,
    C.PID_IDENTIFY_DEVICE,
    *E133_CLIENT_PIDS,
)

PRODUCT_CATEGORY_CONTROL_CONTROLLER = 0x7001  # E1.20 table A-5


def version_as_number(text: str) -> int:
    """'0.5.0' -> 0x00000500 (one byte per part, for DEVICE_INFO)."""
    number = 0
    for t in [int(t) if t.isdigit() else 0 for t in text.split(".")[:4]]:
        number = (number << 8) | (t & 0xFF)
    return number


class ClientResponder:
    """Default responder of an RPT client. ``respond`` returns the RDM response (None for broadcasts)."""

    def __init__(self, client: RptClient, manufacturer_label: str = "pyrdmnet", model: str = "pyrdmnet Controller",
                 device_label: str | None = None, product_category: int = PRODUCT_CATEGORY_CONTROL_CONTROLLER,
                 model_id: int = 0x0002):
        self.client = client
        self.manufacturer_label = manufacturer_label
        self.model = model
        self.device_label = device_label if device_label is not None else model
        self.product_category = product_category
        self.model_id = model_id
        self.identifying = False

    # ----------------------------------------------------------------- complete default responder
    def respond(self, a: RdmMessage) -> RdmMessage | None:
        if a.dest.is_broadcast:
            return None  # E1.20: broadcasts are not answered
        nack = lambda reason: a.response(C.RESPONSE_NACK, struct.pack(">H", reason))  # noqa: E731
        if a.subdevice != C.RDM_ROOT_DEVICE:
            return nack(C.NR_SUB_DEVICE_OUT_OF_RANGE)
        if a.pid in E133_CLIENT_PIDS:
            return self.respond_e133(a)
        if a.command_class == C.CC_GET:
            result = self._get(a.pid, a.data)
            return nack(result) if isinstance(result, int) else a.response(C.RESPONSE_ACK, result)
        if a.command_class != C.CC_SET:
            return nack(C.NR_UNSUPPORTED_COMMAND_CLASS)
        if a.pid == C.PID_IDENTIFY_DEVICE:
            if len(a.data) != 1 or a.data[0] > 1:
                return nack(C.NR_FORMAT_ERROR)
            self.identifying = bool(a.data[0])
            return a.response(C.RESPONSE_ACK)
        if a.pid == C.PID_DEVICE_LABEL:
            if len(a.data) > 32:
                return nack(C.NR_FORMAT_ERROR)
            self.device_label = a.data.decode("ascii", errors="replace")
            return a.response(C.RESPONSE_ACK)
        return nack(C.NR_UNSUPPORTED_COMMAND_CLASS if a.pid in SUPPORTED_PIDS else C.NR_UNKNOWN_PID)

    def _get(self, pid: int, d: bytes) -> bytes | int:
        if pid == C.PID_SUPPORTED_PARAMETERS:  # E1.33 7.2.1: report all PIDs, including the required ones
            return b"".join(struct.pack(">H", p) for p in SUPPORTED_PIDS)
        if pid == C.PID_PARAMETER_DESCRIPTION:  # only for manufacturer-specific PIDs - there are none
            return C.NR_DATA_OUT_OF_RANGE
        if pid == C.PID_DEVICE_INFO:
            return R.build_device_info(R.DeviceInfo(0x0100, self.model_id, self.product_category,
                                                    version_as_number(__version__), 0, 0, 0, 0xFFFF, 0, 0))
        if pid == C.PID_MANUFACTURER_LABEL:
            return R.build_text(self.manufacturer_label)
        if pid == C.PID_DEVICE_MODEL_DESCRIPTION:
            return R.build_text(self.model)
        if pid == C.PID_SOFTWARE_VERSION_LABEL:
            return R.build_text(f"pyrdmnet {__version__}")
        if pid == C.PID_DEVICE_LABEL:
            return R.build_text(self.device_label)
        if pid == C.PID_IDENTIFY_DEVICE:
            return bytes((int(self.identifying),))
        return C.NR_UNKNOWN_PID

    # ----------------------------------------------------------------- E1.33 management PIDs
    def respond_e133(self, a: RdmMessage) -> RdmMessage:
        """COMPONENT_SCOPE, SEARCH_DOMAIN and TCP_COMMS_STATUS (E1.33 7.6.1-7.6.3)."""
        nack = lambda reason: a.response(C.RESPONSE_NACK, struct.pack(">H", reason))  # noqa: E731
        c, pid, d = self.client, a.pid, a.data
        if a.command_class == C.CC_GET:
            if pid == C.PID_COMPONENT_SCOPE:
                if len(d) != 2:
                    return nack(C.NR_FORMAT_ERROR)
                if d != b"\x00\x01":  # one scope: slot 1
                    return nack(C.NR_DATA_OUT_OF_RANGE)
                return a.response(C.RESPONSE_ACK, R.build_component_scope(c.component_scope()))
            if pid == C.PID_SEARCH_DOMAIN:
                return a.response(C.RESPONSE_ACK, c.search_domain.encode("utf-8")[:C.E133_DOMAIN_LENGTH])
            if pid == C.PID_TCP_COMMS_STATUS:
                return a.response(C.RESPONSE_ACK, b"".join(R.build_tcp_comms_entry(e)
                                                           for e in c.tcp_comms_entries()))
            return nack(C.NR_UNKNOWN_PID)
        if a.command_class != C.CC_SET:
            return nack(C.NR_UNSUPPORTED_COMMAND_CLASS)
        if pid == C.PID_TCP_COMMS_STATUS:
            if len(d) != C.E133_SCOPE_LENGTH:
                return nack(C.NR_FORMAT_ERROR)
            if not c.reset_unhealthy_events(R.read_scope_string(d)):
                return nack(C.NR_UNKNOWN_SCOPE)
            return a.response(C.RESPONSE_ACK)
        if pid == C.PID_SEARCH_DOMAIN:
            if len(d) > C.E133_DOMAIN_LENGTH:
                return nack(C.NR_FORMAT_ERROR)
            c.search_domain = d.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
            return a.response(C.RESPONSE_ACK)
        if pid == C.PID_COMPONENT_SCOPE:
            try:
                new = R.read_component_scope(d)
            except RdmFormatError:
                return nack(C.NR_FORMAT_ERROR)
            if new.slot != 1 or not new.scope:  # one scope; the empty string is not a scope
                return nack(C.NR_DATA_OUT_OF_RANGE)
            if new.static_type not in (C.E133_NO_STATIC_CONFIG, C.E133_STATIC_CONFIG_IPV4,
                                       C.E133_STATIC_CONFIG_IPV6):
                return nack(C.NR_INVALID_STATIC_CONFIG_TYPE)
            if new.static_type != C.E133_NO_STATIC_CONFIG and new.static_port == 0:
                return nack(C.NR_INVALID_PORT)
            if new.static_type == C.E133_STATIC_CONFIG_IPV4 and new.static_ipv4 in ("0.0.0.0", "255.255.255.255"):
                return nack(C.NR_INVALID_IPV4_ADDRESS)
            if new.static_type == C.E133_STATIC_CONFIG_IPV6 and new.static_ipv6 == bytes(16):
                return nack(C.NR_INVALID_IPV6_ADDRESS)
            c.set_component_scope(new)
            return a.response(C.RESPONSE_ACK)
        return nack(C.NR_UNKNOWN_PID)
