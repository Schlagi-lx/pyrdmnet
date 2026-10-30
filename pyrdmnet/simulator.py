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

from . import konstanten as K
from . import pdu as P
from . import rdm_parameter as R
from .klient import RptKlient
from .rdm import RdmFormatFehler, RdmNachricht, Uid, ist_geraete_broadcast


@dataclass
class SimSensor:
    definition: R.SensorDefinition
    startwert: float  # in physikalischer Einheit
    anstieg_je_s: float = 0.0  # einfache lineare Drift (nur Simulation)
    endwert: float | None = None

    def rohwert(self, sekunden: float) -> int:
        wert = self.startwert + self.anstieg_je_s * sekunden
        if self.endwert is not None:
            wert = min(wert, self.endwert) if self.anstieg_je_s >= 0 else max(wert, self.endwert)
        roh = round(wert / self.definition.faktor)
        return max(-32768, min(32767, roh))


@dataclass
class SimScheinwerfer:
    """Simulierter RDM-Responder (Scheinwerfer)."""

    uid: Uid
    hersteller_label: str = "pyrdmnet Simulator"
    modell: str = "SIM-PAR RGBL"
    software_label: str = "SIM 1.0.0"
    geraete_label: str = "Messplatz 1"
    modell_id: int = 0x0101
    software_version_id: int = 0x00010000
    produktkategorie: int = 0x0101  # Scheinwerfer, fest
    personalities: list[R.Personality] = field(default_factory=lambda: [
        R.Personality(1, 4, "Direkt 4ch"),
        R.Personality(2, 6, "Direkt 6ch"),
        R.Personality(3, 5, "HSIC 5ch"),
    ])
    personality: int = 2
    startadresse: int = 100
    geraete_stunden: int = 1234
    lampen_stunden: int | None = None  # LED: nicht unterstützt -> NACK
    sensoren: list[SimSensor] = field(default_factory=list)
    schreibschutz: bool = False
    ack_timer_einmal: set = field(default_factory=set)  # PIDs, die einmal ACK_TIMER liefern
    overflow_pids: set = field(default_factory=set)  # PIDs, deren Antwort geteilt wird
    start: float = field(default_factory=time.monotonic)
    set_protokoll: list[tuple[int, bytes]] = field(default_factory=list)
    zusatz_pids: list[int] = field(default_factory=list)  # z. B. Endpoint-PIDs beim Gateway

    def __post_init__(self) -> None:
        if not self.sensoren:
            self.sensoren = [
                SimSensor(R.SensorDefinition(0, 0x00, 0x01, 0x00, -40, 150, 10, 85, 0x03, "LED Temperatur"), 25.0, 0.5, 68.0),
                SimSensor(R.SensorDefinition(1, 0x00, 0x01, 0x00, -40, 120, 0, 60, 0x03, "Gehaeuse"), 23.0, 0.1, 41.0),
                SimSensor(R.SensorDefinition(2, 0x01, 0x04, 0x01, 900, 2650, 2000, 2500, 0x01, "Netzspannung"), 230.4),
                SimSensor(R.SensorDefinition(3, 0x1D, 0x1C, 0x13, 0, 4096, 0, 4096, 0x00, "Speicher"), 512000.0),
            ]

    # --- Hilfen
    def _p(self, nr: int) -> R.Personality | None:
        return next((p for p in self.personalities if p.nummer == nr), None)

    @property
    def footprint(self) -> int:
        p = self._p(self.personality)
        return p.footprint if p else 0

    def pids(self) -> list[int]:
        basis = [K.PID_DEVICE_INFO, K.PID_DEVICE_MODEL_DESCRIPTION, K.PID_MANUFACTURER_LABEL, K.PID_DEVICE_LABEL,
                 K.PID_SOFTWARE_VERSION_LABEL, K.PID_DMX_PERSONALITY, K.PID_DMX_PERSONALITY_DESCRIPTION,
                 K.PID_DMX_START_ADDRESS, K.PID_SENSOR_DEFINITION, K.PID_SENSOR_VALUE, K.PID_DEVICE_HOURS,
                 K.PID_IDENTIFY_DEVICE]
        if self.lampen_stunden is not None:
            basis.append(K.PID_LAMP_HOURS)
        return basis + list(self.zusatz_pids)

    # --- Antwortlogik
    def beantworten(self, a: RdmNachricht) -> RdmNachricht | None:
        if a.ziel != self.uid and not a.ziel.ist_broadcast:
            return None
        if a.command_class == K.CC_SET:
            return self._set(a)
        if a.command_class != K.CC_GET:
            return a.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_UNSUPPORTED_COMMAND_CLASS))
        if a.pid in self.ack_timer_einmal:
            self.ack_timer_einmal.discard(a.pid)
            return a.antwort(K.ANTWORT_ACK_TIMER, struct.pack(">H", 1))  # 100 ms
        daten = self._get(a)
        if isinstance(daten, int):
            return a.antwort(K.ANTWORT_NACK, struct.pack(">H", daten))
        if a.pid in self.overflow_pids and len(daten) > 4:
            # Teil 1 als ACK_OVERFLOW, beim nächsten GET der Rest
            self.overflow_pids.discard(a.pid)
            self._overflow_rest = (a.pid, daten[4:])
            return a.antwort(K.ANTWORT_ACK_OVERFLOW, daten[:4])
        rest = getattr(self, "_overflow_rest", None)
        if rest and rest[0] == a.pid:
            self._overflow_rest = None
            return a.antwort(K.ANTWORT_ACK, rest[1])
        return a.antwort(K.ANTWORT_ACK, daten)

    def _get(self, a: RdmNachricht) -> bytes | int:
        pid, d = a.pid, a.daten
        t = time.monotonic() - self.start
        if pid == K.PID_SUPPORTED_PARAMETERS:
            return b"".join(struct.pack(">H", p) for p in self.pids())
        if pid == K.PID_DEVICE_INFO:
            return R.baue_device_info(R.DeviceInfo(0x0100, self.modell_id, self.produktkategorie,
                                                   self.software_version_id, self.footprint, self.personality,
                                                   len(self.personalities), self.startadresse, 0,
                                                   len(self.sensoren)))
        if pid == K.PID_DEVICE_MODEL_DESCRIPTION:
            return R.baue_text(self.modell)
        if pid == K.PID_MANUFACTURER_LABEL:
            return R.baue_text(self.hersteller_label)
        if pid == K.PID_DEVICE_LABEL:
            return R.baue_text(self.geraete_label)
        if pid == K.PID_SOFTWARE_VERSION_LABEL:
            return R.baue_text(self.software_label)
        if pid == K.PID_DMX_PERSONALITY:
            return bytes((self.personality, len(self.personalities)))
        if pid == K.PID_DMX_PERSONALITY_DESCRIPTION:
            if len(d) != 1:
                return K.NR_FORMAT_ERROR
            p = self._p(d[0])
            return R.baue_personality_beschreibung(p) if p else K.NR_DATA_OUT_OF_RANGE
        if pid == K.PID_DMX_START_ADDRESS:
            return struct.pack(">H", self.startadresse)
        if pid == K.PID_SENSOR_DEFINITION:
            if len(d) != 1 or d[0] >= len(self.sensoren):
                return K.NR_DATA_OUT_OF_RANGE
            return R.baue_sensor_definition(self.sensoren[d[0]].definition)
        if pid == K.PID_SENSOR_VALUE:
            if len(d) != 1 or d[0] >= len(self.sensoren):
                return K.NR_DATA_OUT_OF_RANGE
            s = self.sensoren[d[0]]
            akt = s.rohwert(t)
            return R.baue_sensor_wert(R.SensorRohwert(d[0], akt, s.rohwert(0), akt, akt))
        if pid == K.PID_DEVICE_HOURS:
            return struct.pack(">I", self.geraete_stunden)
        if pid == K.PID_LAMP_HOURS and self.lampen_stunden is not None:
            return struct.pack(">I", self.lampen_stunden)
        if pid == K.PID_IDENTIFY_DEVICE:
            return b"\x00"
        return K.NR_UNKNOWN_PID

    def _set(self, a: RdmNachricht) -> RdmNachricht:
        pid, d = a.pid, a.daten
        nack = lambda g: a.antwort(K.ANTWORT_NACK, struct.pack(">H", g))  # noqa: E731
        if self.schreibschutz:
            return nack(K.NR_WRITE_PROTECT)
        if pid == K.PID_DMX_PERSONALITY:
            if len(d) != 1:
                return nack(K.NR_FORMAT_ERROR)
            if not self._p(d[0]):
                return nack(K.NR_DATA_OUT_OF_RANGE)
            self.personality = d[0]
        elif pid == K.PID_DMX_START_ADDRESS:
            if len(d) != 2:
                return nack(K.NR_FORMAT_ERROR)
            (adr,) = struct.unpack(">H", d)
            if not 1 <= adr <= 512 or adr + self.footprint - 1 > 512:
                return nack(K.NR_DATA_OUT_OF_RANGE)
            self.startadresse = adr
        elif pid == K.PID_IDENTIFY_DEVICE:
            if len(d) != 1 or d[0] > 1:
                return nack(K.NR_FORMAT_ERROR)
        else:
            return nack(K.NR_UNSUPPORTED_COMMAND_CLASS if pid in self.pids() else K.NR_UNKNOWN_PID)
        self.set_protokoll.append((pid, bytes(d)))
        return a.antwort(K.ANTWORT_ACK)


class SimulierterGateway(RptKlient):
    """RDMnet-Gerät (RPT Device) mit simuliertem Scheinwerfer."""

    def __init__(self, uid: Uid | None = None, scope: str = K.E133_STANDARD_SCOPE, nativ: bool = False,
                 scheinwerfer: SimScheinwerfer | None = None, universe: int = 1, antwort_verzoegerung_s: float = 0.0,
                 **kwargs):
        uid = uid or Uid(K.ESTA_PROTOTYP_HERSTELLER, 0x00000100)
        super().__init__(K.RPT_CLIENT_TYP_GERAET, uid, scope, uuid.uuid4(), **kwargs)
        self.nativ = nativ
        self.universe = universe
        self.verzoegerung = antwort_verzoegerung_s
        self.stumm = False  # Test: antwortet nicht (Zeitüberschreitung beim Controller)
        if nativ:
            self.scheinwerfer = scheinwerfer or SimScheinwerfer(uid=uid)
        else:
            self.scheinwerfer = scheinwerfer or SimScheinwerfer(uid=Uid(K.ESTA_PROTOTYP_HERSTELLER, 0x00000201))
        self.gateway_responder = SimScheinwerfer(uid=uid, hersteller_label="pyrdmnet Simulator",
                                                 modell="SIM Gateway 1 Port", produktkategorie=0x0801,
                                                 personalities=[], personality=0, startadresse=0xFFFF,
                                                 zusatz_pids=[K.PID_ENDPOINT_LIST, K.PID_ENDPOINT_TO_UNIVERSE,
                                                              K.PID_ENDPOINT_MODE, K.PID_ENDPOINT_LABEL,
                                                              K.PID_ENDPOINT_RESPONDERS],
                                                 sensoren=[SimSensor(R.SensorDefinition(
                                                     0, 0x00, 0x01, 0x00, -20, 90, 0, 60, 0x00, "Gateway"), 31.0)])

    async def _bei_rpt(self, rpt: P.RptPdu) -> None:
        k = rpt.kopf
        if k.vektor != K.VECTOR_RPT_REQUEST or self.stumm:
            return
        if self.verzoegerung:
            await asyncio.sleep(self.verzoegerung)
        try:
            (roh,) = P.lese_request_oder_notification(rpt.daten, K.VECTOR_REQUEST_RDM_CMD)
            anfrage = RdmNachricht.dekodieren(roh)
        except (P.ProtokollFehler, RdmFormatFehler, ValueError):
            await self._status(k, K.RPT_STATUS_INVALID_MESSAGE)
            return
        if ist_geraete_broadcast(k.ziel) or anfrage.ziel.ist_broadcast:
            return  # E1.20: auf Broadcasts wird nicht geantwortet
        antwort = self._responder_antwort(k.ziel_endpoint, anfrage)
        if isinstance(antwort, int):
            await self._status(k, antwort)
            return
        rkopf = P.RptKopf(K.VECTOR_RPT_NOTIFICATION, self.uid, k.ziel_endpoint, k.quelle, K.E133_NULL_ENDPOINT,
                          k.sequenz)
        await self.sende_rpt(rkopf, P.baue_notification([roh, antwort.kodieren()]))

    def _responder_antwort(self, endpoint: int, a: RdmNachricht) -> RdmNachricht | int:
        if endpoint == K.E133_NULL_ENDPOINT:
            if self.nativ:
                return self.scheinwerfer.beantworten(a) or K.RPT_STATUS_UNKNOWN_RDM_UID
            if a.ziel != self.uid:
                return K.RPT_STATUS_UNKNOWN_RDM_UID
            if a.command_class == K.CC_GET and a.pid == K.PID_ENDPOINT_LIST:
                return a.antwort(K.ANTWORT_ACK, R.baue_endpoint_liste(1, [R.Endpoint(1, K.ENDPOINT_TYP_PHYSISCH)]))
            if a.command_class == K.CC_GET and a.pid == K.PID_ENDPOINT_RESPONDERS:
                if a.daten != b"\x00\x01":
                    return a.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_DATA_OUT_OF_RANGE))
                return a.antwort(K.ANTWORT_ACK, R.baue_endpoint_responder(1, 1, [self.scheinwerfer.uid]))
            if a.command_class == K.CC_GET and a.pid in (K.PID_ENDPOINT_MODE, K.PID_ENDPOINT_LABEL):
                if a.daten != b"\x00\x01":
                    return a.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_DATA_OUT_OF_RANGE))
                inhalt = b"\x02" if a.pid == K.PID_ENDPOINT_MODE else R.baue_text("DMX Port 1")
                return a.antwort(K.ANTWORT_ACK, b"\x00\x01" + inhalt)
            if a.command_class == K.CC_GET and a.pid == K.PID_ENDPOINT_TO_UNIVERSE:
                if a.daten != b"\x00\x01":
                    return a.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_DATA_OUT_OF_RANGE))
                return a.antwort(K.ANTWORT_ACK, struct.pack(">HH", 1, self.universe))
            return self.gateway_responder.beantworten(a) or K.RPT_STATUS_UNKNOWN_RDM_UID
        if endpoint == 1 and not self.nativ:
            if a.ziel != self.scheinwerfer.uid:
                return K.RPT_STATUS_UNKNOWN_RDM_UID
            return self.scheinwerfer.beantworten(a) or K.RPT_STATUS_RDM_TIMEOUT
        return K.RPT_STATUS_UNKNOWN_ENDPOINT

    async def _status(self, k: P.RptKopf, code: int) -> None:
        rkopf = P.RptKopf(K.VECTOR_RPT_STATUS, self.uid, k.ziel_endpoint, k.quelle, K.E133_NULL_ENDPOINT, k.sequenz)
        await self.sende_rpt(rkopf, P.baue_status(code))
