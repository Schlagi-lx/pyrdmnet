"""Geräte finden, vollständig auslesen und Sensor-Schnappschüsse erstellen."""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import asdict, dataclass, field

from . import constants as C
from . import rdm_params as R
from .controller import RdmError, RdmNackError, RdmnetController
from .rdm import Uid


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class DeviceAddress:
    """Wie ein RDM-Responder über RDMnet erreicht wird."""

    rpt_uid: Uid  # RDMnet-Gerät (Gateway oder natives Gerät)
    endpoint: int  # 0 = Default-Responder
    rdm_uid: Uid  # RDM-Responder (Scheinwerfer)

    def __str__(self) -> str:
        return f"{self.rdm_uid} (über {self.rpt_uid}, Endpoint {self.endpoint})"


@dataclass
class DeviceProfile:
    address: DeviceAddress
    read_utc: str
    device_info: R.DeviceInfo | None = None
    manufacturer: str = ""
    model: str = ""
    device_label: str = ""
    software_label: str = ""
    supported_pids: list[int] = field(default_factory=list)
    personalities: list[R.Personality] = field(default_factory=list)
    sensors: list[R.SensorDefinition] = field(default_factory=list)
    device_hours: int | None = None
    lamp_hours: int | None = None
    universe: int | None = None  # nur wenn das Gateway ENDPOINT_TO_UNIVERSE liefert
    unavailable: dict[str, str] = field(default_factory=dict)  # PID -> Grund

    @property
    def personality_current(self) -> R.Personality | None:
        if not self.device_info:
            return None
        return next((p for p in self.personalities if p.number == self.device_info.personality_current), None)

    @property
    def start_address(self) -> int | None:
        return self.device_info.start_address if self.device_info else None

    @property
    def footprint(self) -> int | None:
        return self.device_info.footprint if self.device_info else None

    def to_dict(self) -> dict:
        return {
            "rpt_uid": str(self.address.rpt_uid),
            "endpoint": self.address.endpoint,
            "rdm_uid": str(self.address.rdm_uid),
            "read_utc": self.read_utc,
            "manufacturer": self.manufacturer,
            "model": self.model,
            "device_label": self.device_label,
            "software_label": self.software_label,
            "device_info": self.device_info.to_dict() if self.device_info else None,
            "supported_pids": [C.PID_NAMES.get(p, f"0x{p:04x}") for p in self.supported_pids],
            "personalities": [asdict(p) for p in self.personalities],
            "personality_current": asdict(self.personality_current) if self.personality_current else None,
            "sensors": [s.to_dict() for s in self.sensors],
            "device_hours": self.device_hours,
            "lamp_hours": self.lamp_hours,
            "universe": self.universe,
            "unavailable": self.unavailable,
        }


@dataclass
class SensorReading:
    number: int
    description: str
    type: int
    type_text: str
    unit_text: str
    raw: int
    value: float
    lowest: float | None
    highest: float | None
    recorded: float | None
    outside_normal: bool
    measurement_relevant: bool


@dataclass
class SensorSnapshot:
    rdm_uid: str
    time_utc: str
    duration_s: float
    values: list[SensorReading] = field(default_factory=list)
    error: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.error and not self.values:
            return "failed"
        return "partial" if self.error else "ok"

    def to_dict(self) -> dict:
        return {"rdm_uid": self.rdm_uid, "time_utc": self.time_utc, "duration_s": self.duration_s,
                "status": self.status, "values": [asdict(w) for w in self.values], "error": self.error}


class DeviceReader:
    """Lese-Operationen auf einem verbundenen Controller."""

    def __init__(self, controller: RdmnetController, timeout_s: float = 3.0):
        self.c = controller
        self.timeout_s = timeout_s

    async def _get(self, a: DeviceAddress, pid: int, data: bytes = b"") -> bytes:
        return await self.c.rdm_get(a.rpt_uid, pid, data, endpoint=a.endpoint, rdm_dest=a.rdm_uid,
                                    timeout_s=self.timeout_s)

    # ----------------------------------------------------------------- Finden
    async def find_devices(self) -> list[DeviceAddress]:
        """Alle RDM-Responder hinter allen RDMnet-Geräten (Default-Responder + Endpoints)."""
        await self.c.client_list(refresh=True)
        addresses: list[DeviceAddress] = []
        for e in self.c.devices():
            try:
                _changed, eps = R.read_endpoint_list(await self.c.rdm_get(e.uid, C.PID_ENDPOINT_LIST,
                                                                           timeout_s=self.timeout_s))
            except RdmNackError:
                eps = []  # natives Gerät ohne Endpoints
            except RdmError:
                continue
            physical = [ep for ep in eps if ep.number != 0]
            if not physical:
                addresses.append(DeviceAddress(e.uid, 0, e.uid))
                continue
            for ep in physical:
                try:
                    _ep, _changed, uids = R.read_endpoint_responders(await self.c.rdm_get(
                        e.uid, C.PID_ENDPOINT_RESPONDERS, ep.number.to_bytes(2, "big"), timeout_s=self.timeout_s))
                except RdmError:
                    continue
                addresses.extend(DeviceAddress(e.uid, ep.number, u) for u in uids)
        return addresses

    # ----------------------------------------------------------------- Auslesen
    async def profile(self, a: DeviceAddress) -> DeviceProfile:
        p = DeviceProfile(a, now_utc())

        async def attempt(pid: int, data: bytes = b""):
            try:
                return await self._get(a, pid, data)
            except RdmNackError as f:
                p.unavailable[C.PID_NAMES.get(pid, hex(pid))] = C.NACK_TEXT.get(f.reason, hex(f.reason))
            except RdmError as f:
                p.unavailable[C.PID_NAMES.get(pid, hex(pid))] = str(f)
            return None

        d = await attempt(C.PID_DEVICE_INFO)
        if d is None:
            return p  # Gerät antwortet nicht sinnvoll – Profil bleibt leer, Gründe stehen drin
        p.device_info = R.read_device_info(d)
        if (d := await attempt(C.PID_SUPPORTED_PARAMETERS)) is not None:
            p.supported_pids = R.read_pid_list(d)
        for pid, dest in ((C.PID_MANUFACTURER_LABEL, "manufacturer"), (C.PID_DEVICE_MODEL_DESCRIPTION, "model"),
                          (C.PID_DEVICE_LABEL, "device_label"), (C.PID_SOFTWARE_VERSION_LABEL, "software_label")):
            if (d := await attempt(pid)) is not None:
                setattr(p, dest, R.read_text_field(d))
        for num in range(1, p.device_info.personality_count + 1):
            if (d := await attempt(C.PID_DMX_PERSONALITY_DESCRIPTION, bytes((num,)))) is not None:
                p.personalities.append(R.read_personality_description(d))
        for num in range(p.device_info.sensor_count):
            if (d := await attempt(C.PID_SENSOR_DEFINITION, bytes((num,)))) is not None:
                p.sensors.append(R.read_sensor_definition(d))
        if (d := await attempt(C.PID_DEVICE_HOURS)) is not None:
            p.device_hours = R.read_u32(d, "DEVICE_HOURS")
        if not p.supported_pids or C.PID_LAMP_HOURS in p.supported_pids:
            if (d := await attempt(C.PID_LAMP_HOURS)) is not None:
                p.lamp_hours = R.read_u32(d, "LAMP_HOURS")
        if a.endpoint:
            try:
                _ep, p.universe = R.read_endpoint_universe(await self.c.rdm_get(
                    a.rpt_uid, C.PID_ENDPOINT_TO_UNIVERSE, a.endpoint.to_bytes(2, "big"), timeout_s=self.timeout_s))
            except RdmError as f:
                p.unavailable["ENDPOINT_TO_UNIVERSE"] = str(f)
        return p

    async def gateway_ports(self, rpt_uid: Uid, supported: list[int] | None = None) -> list[dict]:
        """Übersicht aller Ports eines Gateways (E1.37-7): Typ, Modus, Universe, Name, Responder."""
        def supports(pid: int) -> bool:
            return not supported or pid in supported

        async def get(pid: int, ep: int) -> bytes | None:
            try:
                return await self.c.rdm_get(rpt_uid, pid, ep.to_bytes(2, "big"), timeout_s=self.timeout_s)
            except RdmError:
                return None

        _changed, eps = R.read_endpoint_list(await self.c.rdm_get(rpt_uid, C.PID_ENDPOINT_LIST, timeout_s=self.timeout_s))
        ports = []
        for ep in eps:
            port = {"endpoint": ep.number, "type": "physical" if ep.type == C.ENDPOINT_TYPE_PHYSICAL else "virtual",
                    "mode": None, "universe": None, "name": None, "rdm_active": None, "discovery": None,
                    "responder": []}
            if supports(C.PID_ENDPOINT_MODE) and (d := await get(C.PID_ENDPOINT_MODE, ep.number)) and len(d) >= 3:
                port["mode"] = C.ENDPOINT_MODE_TEXT.get(d[2], f"Modus {d[2]}")
            if supports(C.PID_ENDPOINT_TO_UNIVERSE) and (d := await get(C.PID_ENDPOINT_TO_UNIVERSE, ep.number)) \
                    and len(d) == 4:
                port["universe"] = R.read_endpoint_universe(d)[1]
            if supports(C.PID_ENDPOINT_LABEL) and (d := await get(C.PID_ENDPOINT_LABEL, ep.number)) and len(d) >= 2:
                port["name"] = R.read_text_field(d[2:34])
            if supports(C.PID_RDM_TRAFFIC_ENABLE) and (d := await get(C.PID_RDM_TRAFFIC_ENABLE, ep.number)) \
                    and len(d) >= 3:
                port["rdm_active"] = bool(d[2])
            if supports(C.PID_DISCOVERY_STATE) and (d := await get(C.PID_DISCOVERY_STATE, ep.number)) and len(d) >= 5:
                count = int.from_bytes(d[2:4], "big")
                port["discovery"] = f"{C.DISCOVERY_STATE_TEXT.get(d[4], d[4])}, {count} Geräte"
            if (d := await get(C.PID_ENDPOINT_RESPONDERS, ep.number)) is not None:
                port["responder"] = [str(u) for u in R.read_endpoint_responders(d)[2]]
            ports.append(port)
        return ports

    async def read_setting(self, a: DeviceAddress) -> R.DeviceInfo:
        return R.read_device_info(await self._get(a, C.PID_DEVICE_INFO))

    # ----------------------------------------------------------------- Sensoren
    async def sensor_snapshot(self, a: DeviceAddress, sensors: list[R.SensorDefinition],
                                   all_sensors: bool = False) -> SensorSnapshot:
        t0 = time.monotonic()
        s = SensorSnapshot(str(a.rdm_uid), now_utc(), 0.0)
        for sd in sensors:
            if not (all_sensors or sd.measurement_relevant):
                continue
            try:
                w = R.read_sensor_value(await self._get(a, C.PID_SENSOR_VALUE, bytes((sd.number,))))
            except RdmError as f:
                s.error.append(f"Sensor {sd.number} ({sd.description}): {f}")
                continue
            if w.number != sd.number:
                s.error.append(f"Sensor {sd.number}: Antwort für Sensor {w.number}")
                continue
            s.values.append(SensorReading(
                number=sd.number, description=sd.description, type=sd.type, type_text=sd.type_text,
                unit_text=sd.unit_text, raw=w.current, value=sd.scaled(w.current),
                lowest=sd.scaled(w.lowest) if sd.has_min_max else None,
                highest=sd.scaled(w.highest) if sd.has_min_max else None,
                recorded=sd.scaled(w.recorded) if sd.has_recorded else None,
                outside_normal=not (sd.normal_min <= w.current <= sd.normal_max),
                measurement_relevant=sd.measurement_relevant))
        s.duration_s = round(time.monotonic() - t0, 4)
        return s
