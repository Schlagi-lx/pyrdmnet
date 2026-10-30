"""RDM messages per ANSI E1.20: UID, encoding, decoding, checksum."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

from . import constants as C


class RdmFormatError(ValueError):
    """An RDM message is malformed (length, start code, checksum)."""


# ------------------------------------------------------------------- UID
@dataclass(frozen=True, order=True)
class Uid:
    """RDM UID: 16-bit manufacturer ID + 32-bit device ID."""

    manufacturer: int
    device: int

    def __post_init__(self) -> None:
        if not 0 <= self.manufacturer <= 0xFFFF or not 0 <= self.device <= 0xFFFFFFFF:
            raise ValueError(f"UID out of range: {self.manufacturer:#x}:{self.device:#x}")

    @classmethod
    def from_int(cls, value: int) -> "Uid":
        return cls((value >> 32) & 0xFFFF, value & 0xFFFFFFFF)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Uid":
        if len(data) != 6:
            raise RdmFormatError("UID requires exactly 6 bytes")
        h, g = struct.unpack(">HI", data)
        return cls(h, g)

    @classmethod
    def from_text(cls, text: str) -> "Uid":
        """Accepts ``7ff0:00000001`` or ``7ff000000001``."""
        t = text.strip().replace(":", "")
        if len(t) != 12:
            raise ValueError(f"Invalid UID text: {text!r}")
        return cls.from_int(int(t, 16))

    def to_int(self) -> int:
        return (self.manufacturer << 32) | self.device

    def to_bytes(self) -> bytes:
        return struct.pack(">HI", self.manufacturer, self.device)

    # --- properties per E1.20 / E1.33
    @property
    def is_broadcast(self) -> bool:
        return self.device == 0xFFFFFFFF

    @property
    def is_dynamic_request(self) -> bool:
        """E1.33: a dynamic UID is requested with the MSB set and device ID 0."""
        return bool(self.manufacturer & 0x8000) and self.device == 0

    @property
    def is_dynamic(self) -> bool:
        return bool(self.manufacturer & 0x8000)

    @property
    def is_valid_for_client(self) -> bool:
        return self.to_int() != 0 and not self.is_broadcast

    def __str__(self) -> str:
        return f"{self.manufacturer:04x}:{self.device:08x}"


ALL_CONTROLLERS = Uid.from_int(C.E133_RPT_ALL_CONTROLLERS)
ALL_DEVICES = Uid.from_int(C.E133_RPT_ALL_DEVICES)
NULL_UID = Uid(0, 0)


def is_device_broadcast(uid: Uid) -> bool:
    """RPT_ALL_DEVICES (0xFFFD FFFFFFFF) or RPT_ALL_MID_DEVICES (0xFFFD mmmm FFFF)."""
    w = uid.to_int()
    if w == C.E133_RPT_ALL_DEVICES:
        return True
    return (w >> 32) == 0xFFFD and (w & 0xFFFF) == 0xFFFF


def manufacturer_from_mid_broadcast(uid: Uid) -> int | None:
    w = uid.to_int()
    if (w >> 32) == 0xFFFD and (w & 0xFFFF) == 0xFFFF and w != C.E133_RPT_ALL_DEVICES:
        return (w >> 16) & 0xFFFF
    return None


# ------------------------------------------------------------------- Message
def checksum(data: bytes) -> int:
    return sum(data) & 0xFFFF


@dataclass
class RdmMessage:
    """An RDM message (request or response)."""

    dest: Uid
    source: Uid
    transaction: int
    port_or_response_type: int
    command_class: int
    pid: int
    data: bytes = b""
    message_count: int = 0
    subdevice: int = C.RDM_ROOT_DEVICE

    # --- derived properties
    @property
    def is_response(self) -> bool:
        return self.command_class in (C.CC_GET_RESPONSE, C.CC_SET_RESPONSE, C.CC_DISCOVERY_RESPONSE)

    @property
    def response_type(self) -> int:
        return self.port_or_response_type

    def encode(self) -> bytes:
        if len(self.data) > C.RDM_MAX_PDL:
            raise RdmFormatError(f"Parameter data too long ({len(self.data)} > {C.RDM_MAX_PDL})")
        length = C.RDM_HEADER_LENGTH + len(self.data)
        header = struct.pack(
            ">BBB6s6sBBBHBHB",
            C.RDM_STARTCODE,
            C.RDM_SUB_STARTCODE,
            length,
            self.dest.to_bytes(),
            self.source.to_bytes(),
            self.transaction & 0xFF,
            self.port_or_response_type & 0xFF,
            self.message_count & 0xFF,
            self.subdevice & 0xFFFF,
            self.command_class & 0xFF,
            self.pid & 0xFFFF,
            len(self.data),
        )
        raw = header + self.data
        return raw + struct.pack(">H", checksum(raw))

    @classmethod
    def decode(cls, raw: bytes) -> "RdmMessage":
        if len(raw) < C.RDM_HEADER_LENGTH + 2:
            raise RdmFormatError(f"RDM message too short ({len(raw)} bytes)")
        if raw[0] != C.RDM_STARTCODE or raw[1] != C.RDM_SUB_STARTCODE:
            raise RdmFormatError("Wrong RDM start code or sub-start code")
        length = raw[2]
        if length < C.RDM_HEADER_LENGTH or len(raw) < length + 2:
            raise RdmFormatError(f"Length field {length} does not match {len(raw)} bytes")
        pdl = raw[23]
        if length != C.RDM_HEADER_LENGTH + pdl:
            raise RdmFormatError(f"PDL {pdl} does not match length field {length}")
        (expected,) = struct.unpack(">H", raw[length:length + 2])
        actual = checksum(raw[:length])
        if expected != actual:
            raise RdmFormatError(f"Checksum mismatch (read {expected:#06x}, calculated {actual:#06x})")
        (_, _, _, dest, source, tn, port, mc, sub, cc, pid, _pdl) = struct.unpack(
            ">BBB6s6sBBBHBHB", raw[:C.RDM_HEADER_LENGTH]
        )
        return cls(
            dest=Uid.from_bytes(dest),
            source=Uid.from_bytes(source),
            transaction=tn,
            port_or_response_type=port,
            message_count=mc,
            subdevice=sub,
            command_class=cc,
            pid=pid,
            data=bytes(raw[C.RDM_HEADER_LENGTH:length]),
        )

    def response(self, response_type: int, data: bytes = b"", message_count: int = 0) -> "RdmMessage":
        """Creates the matching response to this request (for responder/simulator)."""
        cc_response = {C.CC_GET: C.CC_GET_RESPONSE, C.CC_SET: C.CC_SET_RESPONSE,
                      C.CC_DISCOVERY: C.CC_DISCOVERY_RESPONSE}.get(self.command_class, self.command_class)
        return RdmMessage(
            dest=self.source,
            source=self.dest,
            transaction=self.transaction,
            port_or_response_type=response_type,
            message_count=message_count,
            subdevice=self.subdevice,
            command_class=cc_response,
            pid=self.pid,
            data=data,
        )


@dataclass
class TransactionCounter:
    """Transaction number per RDM destination (E1.20: 8 bits, sequential)."""

    _state: dict = field(default_factory=dict)

    def next_tn(self, dest: Uid) -> int:
        n = (self._state.get(dest, -1) + 1) & 0xFF
        self._state[dest] = n
        return n
