"""Parameterdaten ausgewählter RDM-PIDs lesen (Controller) und erzeugen (Simulator).

Jede ``lese_*``-Funktion prüft die Länge und wirft :class:`RdmFormatFehler`
bei abweichendem Format. Es wird nichts geraten oder aufgefüllt.
"""

from __future__ import annotations

import struct
from dataclasses import asdict, dataclass

from . import constants as C
from .rdm import RdmFormatError, Uid


def _text(data: bytes) -> str:
    """RDM-Texte sind ASCII bis 32 Zeichen, evtl. nullterminiert."""
    return data.split(b"\x00", 1)[0].decode("ascii", errors="replace").strip()


def _check_length(data: bytes, expected: int, pid: str) -> None:
    if len(data) != expected:
        raise RdmFormatError(f"{pid}: {len(data)} Byte statt {expected}")


# ------------------------------------------------------------------- DEVICE_INFO
@dataclass
class DeviceInfo:
    rdm_protocol: int
    model_id: int
    product_category: int
    software_version_id: int
    footprint: int
    personality_current: int
    personality_count: int
    start_address: int  # 0xFFFF = kein Footprint
    subdevice_count: int
    sensor_count: int

    @property
    def product_category_text(self) -> str:
        return C.PRODUCT_CATEGORY_GROUP.get(self.product_category >> 8, "unknown")

    def to_dict(self) -> dict:
        d = asdict(self)
        d["product_category_text"] = self.product_category_text
        return d


_DI = struct.Struct(">HHHIHBBHHB")  # 19 Byte


def read_device_info(data: bytes) -> DeviceInfo:
    _check_length(data, _DI.size, "DEVICE_INFO")
    return DeviceInfo(*_DI.unpack(data))


def build_device_info(di: DeviceInfo) -> bytes:
    return _DI.pack(di.rdm_protocol, di.model_id, di.product_category, di.software_version_id,
                    di.footprint, di.personality_current, di.personality_count, di.start_address,
                    di.subdevice_count, di.sensor_count)


# ------------------------------------------------------------------- Texte
def read_text_field(data: bytes) -> str:
    if len(data) > 32:
        raise RdmFormatError(f"Text länger als 32 Byte ({len(data)})")
    return _text(data)


def build_text(text: str) -> bytes:
    return text.encode("ascii", errors="replace")[:32]


# ------------------------------------------------------------------- Personality
@dataclass
class Personality:
    number: int
    footprint: int
    description: str


def read_personality(data: bytes) -> tuple[int, int]:
    _check_length(data, 2, "DMX_PERSONALITY")
    return data[0], data[1]


def read_personality_description(data: bytes) -> Personality:
    if not 3 <= len(data) <= 35:
        raise RdmFormatError(f"DMX_PERSONALITY_DESCRIPTION: {len(data)} Byte")
    num, fp = struct.unpack(">BH", data[:3])
    return Personality(num, fp, _text(data[3:]))


def build_personality_description(p: Personality) -> bytes:
    return struct.pack(">BH", p.number, p.footprint) + build_text(p.description)


def read_u16(data: bytes, pid: str) -> int:
    _check_length(data, 2, pid)
    return struct.unpack(">H", data)[0]


def read_u32(data: bytes, pid: str) -> int:
    _check_length(data, 4, pid)
    return struct.unpack(">I", data)[0]


def read_pid_list(data: bytes) -> list[int]:
    if len(data) % 2:
        raise RdmFormatError("SUPPORTED_PARAMETERS: ungerade Länge")
    return [v for (v,) in struct.iter_unpack(">H", data)]


# ------------------------------------------------------------------- Sensoren
@dataclass
class SensorDefinition:
    number: int
    type: int
    unit: int
    prefix: int
    range_min: int
    range_max: int
    normal_min: int
    normal_max: int
    recorded_support: int  # Bit0: recorded value, Bit1: lowest/highest
    description: str

    @property
    def factor(self) -> float:
        return 10.0 ** C.PREFIX_EXPONENT.get(self.prefix, 0)

    @property
    def prefix_known(self) -> bool:
        return self.prefix in C.PREFIX_EXPONENT

    @property
    def type_text(self) -> str:
        return C.SENSOR_TYPE_TEXT.get(self.type, f"herstellerspezifisch ({self.type:#04x})")

    @property
    def unit_text(self) -> str:
        return C.UNIT_TEXT.get(self.unit, f"Einheit {self.unit:#04x}")

    @property
    def measurement_relevant(self) -> bool:
        return self.type in C.MEASUREMENT_RELEVANT_SENSOR_TYPES

    @property
    def has_recorded(self) -> bool:
        return bool(self.recorded_support & 0x01)

    @property
    def has_min_max(self) -> bool:
        return bool(self.recorded_support & 0x02)

    def scaled(self, raw: int) -> float:
        return raw * self.factor

    def to_dict(self) -> dict:
        d = asdict(self)
        d.update(type_text=self.type_text, unit_text=self.unit_text, measurement_relevant=self.measurement_relevant,
                 factor=self.factor)
        return d


_SD = struct.Struct(">BBBBhhhhB")  # 13 Byte + Beschreibung


def read_sensor_definition(data: bytes) -> SensorDefinition:
    if not _SD.size <= len(data) <= _SD.size + 32:
        raise RdmFormatError(f"SENSOR_DEFINITION: {len(data)} Byte")
    values = _SD.unpack(data[:_SD.size])
    return SensorDefinition(*values, description=_text(data[_SD.size:]))


def build_sensor_definition(sd: SensorDefinition) -> bytes:
    return _SD.pack(sd.number, sd.type, sd.unit, sd.prefix, sd.range_min, sd.range_max,
                    sd.normal_min, sd.normal_max, sd.recorded_support) + build_text(sd.description)


@dataclass
class SensorRawValue:
    number: int
    current: int
    lowest: int
    highest: int
    recorded: int


_SV = struct.Struct(">Bhhhh")  # 9 Byte


def read_sensor_value(data: bytes) -> SensorRawValue:
    _check_length(data, _SV.size, "SENSOR_VALUE")
    return SensorRawValue(*_SV.unpack(data))


def build_sensor_value(w: SensorRawValue) -> bytes:
    return _SV.pack(w.number, w.current, w.lowest, w.highest, w.recorded)


# ------------------------------------------------------------------- Endpoints (E1.37-7)
@dataclass
class Endpoint:
    number: int
    type: int


def read_endpoint_list(data: bytes) -> tuple[int, list[Endpoint]]:
    if len(data) < 4 or (len(data) - 4) % 3:
        raise RdmFormatError(f"ENDPOINT_LIST: {len(data)} Byte")
    (change,) = struct.unpack(">I", data[:4])
    eps = [Endpoint(n, t) for n, t in struct.iter_unpack(">HB", data[4:])]
    return change, eps


def build_endpoint_list(change: int, eps: list[Endpoint]) -> bytes:
    return struct.pack(">I", change) + b"".join(struct.pack(">HB", e.number, e.type) for e in eps)


def read_endpoint_responders(data: bytes) -> tuple[int, int, list[Uid]]:
    if len(data) < 6 or (len(data) - 6) % 6:
        raise RdmFormatError(f"ENDPOINT_RESPONDERS: {len(data)} Byte")
    ep, change = struct.unpack(">HI", data[:6])
    uids = [Uid.from_bytes(data[i:i + 6]) for i in range(6, len(data), 6)]
    return ep, change, uids


def build_endpoint_responders(ep: int, change: int, uids: list[Uid]) -> bytes:
    return struct.pack(">HI", ep, change) + b"".join(u.to_bytes() for u in uids)


def read_endpoint_universe(data: bytes) -> tuple[int, int]:
    _check_length(data, 4, "ENDPOINT_TO_UNIVERSE")
    return struct.unpack(">HH", data)


# ------------------------------------------------------------------- E1.33 COMPONENT_SCOPE / SEARCH_DOMAIN
# Hinweis: E1.33-2019 nennt als PDL 0x54, die Felder ergeben aber 88 Byte (0x58);
# die ETC-Referenzimplementierung sendet ebenfalls 88 Byte. Gesendet wird 88.
COMPONENT_SCOPE_LENGTH = 2 + C.E133_SCOPE_LENGTH + 1 + 4 + 16 + 2


@dataclass
class ComponentScope:
    slot: int
    scope: str
    static_type: int = C.E133_NO_STATIC_CONFIG
    static_ipv4: str = "0.0.0.0"
    static_ipv6: bytes = bytes(16)
    static_port: int = 0

    @property
    def static_address(self) -> str | None:
        if self.static_type == C.E133_STATIC_CONFIG_IPV4:
            return f"{self.static_ipv4}:{self.static_port}"
        if self.static_type == C.E133_STATIC_CONFIG_IPV6:
            import ipaddress
            return f"[{ipaddress.IPv6Address(self.static_ipv6)}]:{self.static_port}"
        return None


def build_component_scope(cs: ComponentScope) -> bytes:
    raw = cs.scope.encode("utf-8")[:C.E133_SCOPE_LENGTH - 1]
    ipv4 = bytes(int(t) for t in cs.static_ipv4.split("."))
    if len(ipv4) != 4 or len(cs.static_ipv6) != 16:
        raise RdmFormatError("COMPONENT_SCOPE: statische Adresse ungültig")
    return (struct.pack(">H", cs.slot) + raw.ljust(C.E133_SCOPE_LENGTH, b"\x00") + bytes((cs.static_type,))
            + ipv4 + cs.static_ipv6 + struct.pack(">H", cs.static_port))


def read_component_scope(data: bytes) -> ComponentScope:
    _check_length(data, COMPONENT_SCOPE_LENGTH, "COMPONENT_SCOPE")
    (slot,) = struct.unpack(">H", data[:2])
    scope = data[2:2 + C.E133_SCOPE_LENGTH].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
    p = 2 + C.E133_SCOPE_LENGTH
    ipv4 = ".".join(str(b) for b in data[p + 1:p + 5])
    (port,) = struct.unpack(">H", data[p + 21:p + 23])
    return ComponentScope(slot, scope, data[p], ipv4, bytes(data[p + 5:p + 21]), port)


def read_search_domain(data: bytes) -> str:
    if len(data) > C.E133_DOMAIN_LENGTH:
        raise RdmFormatError(f"SEARCH_DOMAIN länger als {C.E133_DOMAIN_LENGTH} Byte")
    return data.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
