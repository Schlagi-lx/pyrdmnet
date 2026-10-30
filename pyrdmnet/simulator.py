"""Simuliertes RDMnet-Gerät für Offline-Tests.

Zwei Betriebsarten:
* Gateway (Standard): RDMnet-Gerät mit Default-Responder (Endpoint 0) und
  einem physischen Port (Endpoint 1), an dem ein simulierter RDM-Scheinwerfer hängt.
* Nativ: Der Scheinwerfer ist selbst das RDMnet-Gerät (Endpoint 0).

Alle Werte sind SIMULATION. Hersteller/Modell sind bewusst generisch und
stellen kein reales Gerät dar.
"""

from __future__ import annotations

import asyncio
import struct
import time
import uuid
from dataclasses import dataclass, field

from . import constants as C
from . import pdu as P
from . import rdm_params as R
from .client import RptClient
from .rdm import RdmFormatError, RdmMessage, Uid, is_device_broadcast


@dataclass
class SimSensor:
    definition: R.SensorDefinition
    start_value: float  # in physikalischer Einheit
    rise_per_s: float = 0.0  # einfache lineare Drift (nur Simulation)
    end_value: float | None = None

    def raw_value(self, seconds: float) -> int:
        value = self.start_value + self.rise_per_s * seconds
        if self.end_value is not None:
            value = min(value, self.end_value) if self.rise_per_s >= 0 else max(value, self.end_value)
        raw = round(value / self.definition.factor)
        return max(-32768, min(32767, raw))


@dataclass
class SimFixture:
    """Simulierter RDM-Responder (Scheinwerfer)."""

    uid: Uid
    manufacturer_label: str = "pyrdmnet Simulator"
    model: str = "SIM-PAR RGBL"
    software_label: str = "SIM 1.0.0"
    device_label: str = "Test bench 1"
    model_id: int = 0x0101
    software_version_id: int = 0x00010000
    product_category: int = 0x0101  # Scheinwerfer, fest
    personalities: list[R.Personality] = field(default_factory=lambda: [
        R.Personality(1, 4, "Direct 4ch"),
        R.Personality(2, 6, "Direct 6ch"),
        R.Personality(3, 5, "HSIC 5ch"),
    ])
    personality: int = 2
    start_address: int = 100
    device_hours: int = 1234
    lamp_hours: int | None = None  # LED: nicht unterstützt -> NACK
    sensors: list[SimSensor] = field(default_factory=list)
    write_protect: bool = False
    ack_timer_once: set = field(default_factory=set)  # PIDs, die einmal ACK_TIMER liefern
    overflow_pids: set = field(default_factory=set)  # PIDs, deren Antwort geteilt wird
    start: float = field(default_factory=time.monotonic)
    set_log: list[tuple[int, bytes]] = field(default_factory=list)
    extra_pids: list[int] = field(default_factory=list)  # z. B. Endpoint-PIDs beim Gateway

    def __post_init__(self) -> None:
        if not self.sensors:
            self.sensors = [
                SimSensor(R.SensorDefinition(0, 0x00, 0x01, 0x00, -40, 150, 10, 85, 0x03, "LED temperature"), 25.0, 0.5, 68.0),
                SimSensor(R.SensorDefinition(1, 0x00, 0x01, 0x00, -40, 120, 0, 60, 0x03, "Housing"), 23.0, 0.1, 41.0),
                SimSensor(R.SensorDefinition(2, 0x01, 0x04, 0x01, 900, 2650, 2000, 2500, 0x01, "Mains voltage"), 230.4),
                SimSensor(R.SensorDefinition(3, 0x1D, 0x1C, 0x13, 0, 4096, 0, 4096, 0x00, "Memory"), 512000.0),
            ]

    # --- Hilfen
    def _p(self, num: int) -> R.Personality | None:
        return next((p for p in self.personalities if p.number == num), None)

    @property
    def footprint(self) -> int:
        p = self._p(self.personality)
        return p.footprint if p else 0

    def pids(self) -> list[int]:
        base = [C.PID_DEVICE_INFO, C.PID_DEVICE_MODEL_DESCRIPTION, C.PID_MANUFACTURER_LABEL, C.PID_DEVICE_LABEL,
                 C.PID_SOFTWARE_VERSION_LABEL, C.PID_DMX_PERSONALITY, C.PID_DMX_PERSONALITY_DESCRIPTION,
                 C.PID_DMX_START_ADDRESS, C.PID_SENSOR_DEFINITION, C.PID_SENSOR_VALUE, C.PID_DEVICE_HOURS,
                 C.PID_IDENTIFY_DEVICE]
        if self.lamp_hours is not None:
            base.append(C.PID_LAMP_HOURS)
        return base + list(self.extra_pids)

    # --- Antwortlogik
    def respond(self, a: RdmMessage) -> RdmMessage | None:
        if a.dest != self.uid and not a.dest.is_broadcast:
            return None
        if a.command_class == C.CC_SET:
            return self._set(a)
        if a.command_class != C.CC_GET:
            return a.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_UNSUPPORTED_COMMAND_CLASS))
        if a.pid in self.ack_timer_once:
            self.ack_timer_once.discard(a.pid)
            return a.response(C.RESPONSE_ACK_TIMER, struct.pack(">H", 1))  # 100 ms
        data = self._get(a)
        if isinstance(data, int):
            return a.response(C.RESPONSE_NACK, struct.pack(">H", data))
        if a.pid in self.overflow_pids and len(data) > 4:
            # Teil 1 als ACK_OVERFLOW, beim nächsten GET der Rest
            self.overflow_pids.discard(a.pid)
            self._overflow_rest = (a.pid, data[4:])
            return a.response(C.RESPONSE_ACK_OVERFLOW, data[:4])
        rest = getattr(self, "_overflow_rest", None)
        if rest and rest[0] == a.pid:
            self._overflow_rest = None
            return a.response(C.RESPONSE_ACK, rest[1])
        return a.response(C.RESPONSE_ACK, data)

    def _get(self, a: RdmMessage) -> bytes | int:
        pid, d = a.pid, a.data
        t = time.monotonic() - self.start
        if pid == C.PID_SUPPORTED_PARAMETERS:
            return b"".join(struct.pack(">H", p) for p in self.pids())
        if pid == C.PID_DEVICE_INFO:
            return R.build_device_info(R.DeviceInfo(0x0100, self.model_id, self.product_category,
                                                   self.software_version_id, self.footprint, self.personality,
                                                   len(self.personalities), self.start_address, 0,
                                                   len(self.sensors)))
        if pid == C.PID_DEVICE_MODEL_DESCRIPTION:
            return R.build_text(self.model)
        if pid == C.PID_MANUFACTURER_LABEL:
            return R.build_text(self.manufacturer_label)
        if pid == C.PID_DEVICE_LABEL:
            return R.build_text(self.device_label)
        if pid == C.PID_SOFTWARE_VERSION_LABEL:
            return R.build_text(self.software_label)
        if pid == C.PID_DMX_PERSONALITY:
            return bytes((self.personality, len(self.personalities)))
        if pid == C.PID_DMX_PERSONALITY_DESCRIPTION:
            if len(d) != 1:
                return C.NR_FORMAT_ERROR
            p = self._p(d[0])
            return R.build_personality_description(p) if p else C.NR_DATA_OUT_OF_RANGE
        if pid == C.PID_DMX_START_ADDRESS:
            return struct.pack(">H", self.start_address)
        if pid == C.PID_SENSOR_DEFINITION:
            if len(d) != 1 or d[0] >= len(self.sensors):
                return C.NR_DATA_OUT_OF_RANGE
            return R.build_sensor_definition(self.sensors[d[0]].definition)
        if pid == C.PID_SENSOR_VALUE:
            if len(d) != 1 or d[0] >= len(self.sensors):
                return C.NR_DATA_OUT_OF_RANGE
            s = self.sensors[d[0]]
            current = s.raw_value(t)
            return R.build_sensor_value(R.SensorRawValue(d[0], current, s.raw_value(0), current, current))
        if pid == C.PID_DEVICE_HOURS:
            return struct.pack(">I", self.device_hours)
        if pid == C.PID_LAMP_HOURS and self.lamp_hours is not None:
            return struct.pack(">I", self.lamp_hours)
        if pid == C.PID_IDENTIFY_DEVICE:
            return b"\x00"
        return C.NR_UNKNOWN_PID

    def _set(self, a: RdmMessage) -> RdmMessage:
        pid, d = a.pid, a.data
        nack = lambda g: a.response(C.RESPONSE_NACK, struct.pack(">H", g))  # noqa: E731
        if self.write_protect:
            return nack(C.NR_WRITE_PROTECT)
        if pid == C.PID_DMX_PERSONALITY:
            if len(d) != 1:
                return nack(C.NR_FORMAT_ERROR)
            if not self._p(d[0]):
                return nack(C.NR_DATA_OUT_OF_RANGE)
            self.personality = d[0]
        elif pid == C.PID_DMX_START_ADDRESS:
            if len(d) != 2:
                return nack(C.NR_FORMAT_ERROR)
            (dmx_addr,) = struct.unpack(">H", d)
            if not 1 <= dmx_addr <= 512 or dmx_addr + self.footprint - 1 > 512:
                return nack(C.NR_DATA_OUT_OF_RANGE)
            self.start_address = dmx_addr
        elif pid == C.PID_IDENTIFY_DEVICE:
            if len(d) != 1 or d[0] > 1:
                return nack(C.NR_FORMAT_ERROR)
        else:
            return nack(C.NR_UNSUPPORTED_COMMAND_CLASS if pid in self.pids() else C.NR_UNKNOWN_PID)
        self.set_log.append((pid, bytes(d)))
        return a.response(C.RESPONSE_ACK)


class SimulatedGateway(RptClient):
    """RDMnet-Gerät (RPT Device) mit simuliertem Scheinwerfer."""

    def __init__(self, uid: Uid | None = None, scope: str = C.E133_DEFAULT_SCOPE, native: bool = False,
                 fixture: SimFixture | None = None, universe: int = 1, response_delay_s: float = 0.0,
                 **kwargs):
        uid = uid or Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x00000100)
        super().__init__(C.RPT_CLIENT_TYPE_DEVICE, uid, scope, uuid.uuid4(), **kwargs)
        self.native = native
        self.universe = universe
        self.delay = response_delay_s
        self.mute = False  # Test: antwortet nicht (Zeitüberschreitung beim Controller)
        if native:
            self.fixture = fixture or SimFixture(uid=uid)
        else:
            self.fixture = fixture or SimFixture(uid=Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x00000201))
        self.gateway_responder = SimFixture(uid=uid, manufacturer_label="pyrdmnet Simulator",
                                                 model="SIM Gateway 1 Port", product_category=0x0801,
                                                 personalities=[], personality=0, start_address=0xFFFF,
                                                 extra_pids=[C.PID_ENDPOINT_LIST, C.PID_ENDPOINT_TO_UNIVERSE,
                                                              C.PID_ENDPOINT_MODE, C.PID_ENDPOINT_LABEL,
                                                              C.PID_ENDPOINT_RESPONDERS],
                                                 sensors=[SimSensor(R.SensorDefinition(
                                                     0, 0x00, 0x01, 0x00, -20, 90, 0, 60, 0x00, "Gateway"), 31.0)])

    async def _on_rpt(self, rpt: P.RptPdu) -> None:
        k = rpt.header
        if k.vector != C.VECTOR_RPT_REQUEST or self.mute:
            return
        if self.delay:
            await asyncio.sleep(self.delay)
        try:
            (raw,) = P.read_request_or_notification(rpt.data, C.VECTOR_REQUEST_RDM_CMD)
            request = RdmMessage.decode(raw)
        except (P.ProtocolError, RdmFormatError, ValueError):
            await self._status(k, C.RPT_STATUS_INVALID_MESSAGE)
            return
        if is_device_broadcast(k.dest) or request.dest.is_broadcast:
            return  # E1.20: auf Broadcasts wird nicht geantwortet
        response = self._responder_reply(k.dest_endpoint, request)
        if isinstance(response, int):
            await self._status(k, response)
            return
        rheader = P.RptHeader(C.VECTOR_RPT_NOTIFICATION, self.uid, k.dest_endpoint, k.source, C.E133_NULL_ENDPOINT,
                          k.sequence)
        await self.send_rpt(rheader, P.build_notification([raw, response.encode()]))

    def _responder_reply(self, endpoint: int, a: RdmMessage) -> RdmMessage | int:
        if endpoint == C.E133_NULL_ENDPOINT:
            if self.native:
                return self.fixture.respond(a) or C.RPT_STATUS_UNKNOWN_RDM_UID
            if a.dest != self.uid:
                return C.RPT_STATUS_UNKNOWN_RDM_UID
            if a.command_class == C.CC_GET and a.pid == C.PID_ENDPOINT_LIST:
                return a.response(C.RESPONSE_ACK, R.build_endpoint_list(1, [R.Endpoint(1, C.ENDPOINT_TYPE_PHYSICAL)]))
            if a.command_class == C.CC_GET and a.pid == C.PID_ENDPOINT_RESPONDERS:
                if a.data != b"\x00\x01":
                    return a.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_DATA_OUT_OF_RANGE))
                return a.response(C.RESPONSE_ACK, R.build_endpoint_responders(1, 1, [self.fixture.uid]))
            if a.command_class == C.CC_GET and a.pid in (C.PID_ENDPOINT_MODE, C.PID_ENDPOINT_LABEL):
                if a.data != b"\x00\x01":
                    return a.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_DATA_OUT_OF_RANGE))
                content = b"\x02" if a.pid == C.PID_ENDPOINT_MODE else R.build_text("DMX Port 1")
                return a.response(C.RESPONSE_ACK, b"\x00\x01" + content)
            if a.command_class == C.CC_GET and a.pid == C.PID_ENDPOINT_TO_UNIVERSE:
                if a.data != b"\x00\x01":
                    return a.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_DATA_OUT_OF_RANGE))
                return a.response(C.RESPONSE_ACK, struct.pack(">HH", 1, self.universe))
            return self.gateway_responder.respond(a) or C.RPT_STATUS_UNKNOWN_RDM_UID
        if endpoint == 1 and not self.native:
            if a.dest != self.fixture.uid:
                return C.RPT_STATUS_UNKNOWN_RDM_UID
            return self.fixture.respond(a) or C.RPT_STATUS_RDM_TIMEOUT
        return C.RPT_STATUS_UNKNOWN_ENDPOINT

    async def _status(self, k: P.RptHeader, code: int) -> None:
        rheader = P.RptHeader(C.VECTOR_RPT_STATUS, self.uid, k.dest_endpoint, k.source, C.E133_NULL_ENDPOINT, k.sequence)
        await self.send_rpt(rheader, P.build_status(code))
