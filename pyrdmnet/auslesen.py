"""Geräte finden, vollständig auslesen und Sensor-Schnappschüsse erstellen."""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import asdict, dataclass, field

from . import konstanten as K
from . import rdm_parameter as R
from .controller import RdmFehler, RdmNackFehler, RdmnetController
from .rdm import Uid


def jetzt_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class GeraeteAdresse:
    """Wie ein RDM-Responder über RDMnet erreicht wird."""

    rpt_uid: Uid  # RDMnet-Gerät (Gateway oder natives Gerät)
    endpoint: int  # 0 = Default-Responder
    rdm_uid: Uid  # RDM-Responder (Scheinwerfer)

    def __str__(self) -> str:
        return f"{self.rdm_uid} (über {self.rpt_uid}, Endpoint {self.endpoint})"


@dataclass
class GeraeteProfil:
    adresse: GeraeteAdresse
    ausgelesen_utc: str
    device_info: R.DeviceInfo | None = None
    hersteller: str = ""
    modell: str = ""
    geraete_label: str = ""
    software_label: str = ""
    unterstuetzte_pids: list[int] = field(default_factory=list)
    personalities: list[R.Personality] = field(default_factory=list)
    sensoren: list[R.SensorDefinition] = field(default_factory=list)
    geraete_stunden: int | None = None
    lampen_stunden: int | None = None
    universe: int | None = None  # nur wenn das Gateway ENDPOINT_TO_UNIVERSE liefert
    nicht_verfuegbar: dict[str, str] = field(default_factory=dict)  # PID -> Grund

    @property
    def personality_aktuell(self) -> R.Personality | None:
        if not self.device_info:
            return None
        return next((p for p in self.personalities if p.nummer == self.device_info.personality_aktuell), None)

    @property
    def startadresse(self) -> int | None:
        return self.device_info.startadresse if self.device_info else None

    @property
    def footprint(self) -> int | None:
        return self.device_info.footprint if self.device_info else None

    def als_dict(self) -> dict:
        return {
            "rpt_uid": str(self.adresse.rpt_uid),
            "endpoint": self.adresse.endpoint,
            "rdm_uid": str(self.adresse.rdm_uid),
            "ausgelesen_utc": self.ausgelesen_utc,
            "hersteller": self.hersteller,
            "modell": self.modell,
            "geraete_label": self.geraete_label,
            "software_label": self.software_label,
            "device_info": self.device_info.als_dict() if self.device_info else None,
            "unterstuetzte_pids": [K.PID_NAMEN.get(p, f"0x{p:04x}") for p in self.unterstuetzte_pids],
            "personalities": [asdict(p) for p in self.personalities],
            "personality_aktuell": asdict(self.personality_aktuell) if self.personality_aktuell else None,
            "sensoren": [s.als_dict() for s in self.sensoren],
            "geraete_stunden": self.geraete_stunden,
            "lampen_stunden": self.lampen_stunden,
            "universe": self.universe,
            "nicht_verfuegbar": self.nicht_verfuegbar,
        }


@dataclass
class SensorMesswert:
    nummer: int
    beschreibung: str
    typ: int
    typ_text: str
    einheit_text: str
    roh: int
    wert: float
    niedrigster: float | None
    hoechster: float | None
    aufgezeichnet: float | None
    ausserhalb_normal: bool
    messrelevant: bool


@dataclass
class SensorSchnappschuss:
    rdm_uid: str
    zeit_utc: str
    dauer_s: float
    werte: list[SensorMesswert] = field(default_factory=list)
    fehler: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.fehler and not self.werte:
            return "fehlgeschlagen"
        return "teilweise" if self.fehler else "ok"

    def als_dict(self) -> dict:
        return {"rdm_uid": self.rdm_uid, "zeit_utc": self.zeit_utc, "dauer_s": self.dauer_s,
                "status": self.status, "werte": [asdict(w) for w in self.werte], "fehler": self.fehler}


class Auslesung:
    """Lese-Operationen auf einem verbundenen Controller."""

    def __init__(self, controller: RdmnetController, timeout_s: float = 3.0):
        self.c = controller
        self.timeout_s = timeout_s

    async def _get(self, a: GeraeteAdresse, pid: int, daten: bytes = b"") -> bytes:
        return await self.c.rdm_get(a.rpt_uid, pid, daten, endpoint=a.endpoint, rdm_ziel=a.rdm_uid,
                                    timeout_s=self.timeout_s)

    # ----------------------------------------------------------------- Finden
    async def geraete_finden(self) -> list[GeraeteAdresse]:
        """Alle RDM-Responder hinter allen RDMnet-Geräten (Default-Responder + Endpoints)."""
        await self.c.clientliste(neu_abfragen=True)
        adressen: list[GeraeteAdresse] = []
        for e in self.c.geraete():
            try:
                _aend, eps = R.lese_endpoint_liste(await self.c.rdm_get(e.uid, K.PID_ENDPOINT_LIST,
                                                                           timeout_s=self.timeout_s))
            except RdmNackFehler:
                eps = []  # natives Gerät ohne Endpoints
            except RdmFehler:
                continue
            physisch = [ep for ep in eps if ep.nummer != 0]
            if not physisch:
                adressen.append(GeraeteAdresse(e.uid, 0, e.uid))
                continue
            for ep in physisch:
                try:
                    _ep, _aend, uids = R.lese_endpoint_responder(await self.c.rdm_get(
                        e.uid, K.PID_ENDPOINT_RESPONDERS, ep.nummer.to_bytes(2, "big"), timeout_s=self.timeout_s))
                except RdmFehler:
                    continue
                adressen.extend(GeraeteAdresse(e.uid, ep.nummer, u) for u in uids)
        return adressen

    # ----------------------------------------------------------------- Auslesen
    async def profil(self, a: GeraeteAdresse) -> GeraeteProfil:
        p = GeraeteProfil(a, jetzt_utc())

        async def versuch(pid: int, daten: bytes = b""):
            try:
                return await self._get(a, pid, daten)
            except RdmNackFehler as f:
                p.nicht_verfuegbar[K.PID_NAMEN.get(pid, hex(pid))] = K.NACK_TEXT.get(f.grund, hex(f.grund))
            except RdmFehler as f:
                p.nicht_verfuegbar[K.PID_NAMEN.get(pid, hex(pid))] = str(f)
            return None

        d = await versuch(K.PID_DEVICE_INFO)
        if d is None:
            return p  # Gerät antwortet nicht sinnvoll – Profil bleibt leer, Gründe stehen drin
        p.device_info = R.lese_device_info(d)
        if (d := await versuch(K.PID_SUPPORTED_PARAMETERS)) is not None:
            p.unterstuetzte_pids = R.lese_pid_liste(d)
        for pid, ziel in ((K.PID_MANUFACTURER_LABEL, "hersteller"), (K.PID_DEVICE_MODEL_DESCRIPTION, "modell"),
                          (K.PID_DEVICE_LABEL, "geraete_label"), (K.PID_SOFTWARE_VERSION_LABEL, "software_label")):
            if (d := await versuch(pid)) is not None:
                setattr(p, ziel, R.lese_text(d))
        for nr in range(1, p.device_info.personality_anzahl + 1):
            if (d := await versuch(K.PID_DMX_PERSONALITY_DESCRIPTION, bytes((nr,)))) is not None:
                p.personalities.append(R.lese_personality_beschreibung(d))
        for nr in range(p.device_info.sensor_anzahl):
            if (d := await versuch(K.PID_SENSOR_DEFINITION, bytes((nr,)))) is not None:
                p.sensoren.append(R.lese_sensor_definition(d))
        if (d := await versuch(K.PID_DEVICE_HOURS)) is not None:
            p.geraete_stunden = R.lese_u32(d, "DEVICE_HOURS")
        if not p.unterstuetzte_pids or K.PID_LAMP_HOURS in p.unterstuetzte_pids:
            if (d := await versuch(K.PID_LAMP_HOURS)) is not None:
                p.lampen_stunden = R.lese_u32(d, "LAMP_HOURS")
        if a.endpoint:
            try:
                _ep, p.universe = R.lese_endpoint_universe(await self.c.rdm_get(
                    a.rpt_uid, K.PID_ENDPOINT_TO_UNIVERSE, a.endpoint.to_bytes(2, "big"), timeout_s=self.timeout_s))
            except RdmFehler as f:
                p.nicht_verfuegbar["ENDPOINT_TO_UNIVERSE"] = str(f)
        return p

    async def gateway_ports(self, rpt_uid: Uid, unterstuetzt: list[int] | None = None) -> list[dict]:
        """Übersicht aller Ports eines Gateways (E1.37-7): Typ, Modus, Universe, Name, Responder."""
        def kann(pid: int) -> bool:
            return not unterstuetzt or pid in unterstuetzt

        async def get(pid: int, ep: int) -> bytes | None:
            try:
                return await self.c.rdm_get(rpt_uid, pid, ep.to_bytes(2, "big"), timeout_s=self.timeout_s)
            except RdmFehler:
                return None

        _aend, eps = R.lese_endpoint_liste(await self.c.rdm_get(rpt_uid, K.PID_ENDPOINT_LIST, timeout_s=self.timeout_s))
        ports = []
        for ep in eps:
            port = {"endpoint": ep.nummer, "typ": "physisch" if ep.typ == K.ENDPOINT_TYP_PHYSISCH else "virtuell",
                    "modus": None, "universe": None, "name": None, "rdm_aktiv": None, "discovery": None,
                    "responder": []}
            if kann(K.PID_ENDPOINT_MODE) and (d := await get(K.PID_ENDPOINT_MODE, ep.nummer)) and len(d) >= 3:
                port["modus"] = K.ENDPOINT_MODUS_TEXT.get(d[2], f"Modus {d[2]}")
            if kann(K.PID_ENDPOINT_TO_UNIVERSE) and (d := await get(K.PID_ENDPOINT_TO_UNIVERSE, ep.nummer)) \
                    and len(d) == 4:
                port["universe"] = R.lese_endpoint_universe(d)[1]
            if kann(K.PID_ENDPOINT_LABEL) and (d := await get(K.PID_ENDPOINT_LABEL, ep.nummer)) and len(d) >= 2:
                port["name"] = R.lese_text(d[2:34])
            if kann(K.PID_RDM_TRAFFIC_ENABLE) and (d := await get(K.PID_RDM_TRAFFIC_ENABLE, ep.nummer)) \
                    and len(d) >= 3:
                port["rdm_aktiv"] = bool(d[2])
            if kann(K.PID_DISCOVERY_STATE) and (d := await get(K.PID_DISCOVERY_STATE, ep.nummer)) and len(d) >= 5:
                anzahl = int.from_bytes(d[2:4], "big")
                port["discovery"] = f"{K.DISCOVERY_STATE_TEXT.get(d[4], d[4])}, {anzahl} Geräte"
            if (d := await get(K.PID_ENDPOINT_RESPONDERS, ep.nummer)) is not None:
                port["responder"] = [str(u) for u in R.lese_endpoint_responder(d)[2]]
            ports.append(port)
        return ports

    async def einstellung_lesen(self, a: GeraeteAdresse) -> R.DeviceInfo:
        return R.lese_device_info(await self._get(a, K.PID_DEVICE_INFO))

    # ----------------------------------------------------------------- Sensoren
    async def sensor_schnappschuss(self, a: GeraeteAdresse, sensoren: list[R.SensorDefinition],
                                   alle_sensoren: bool = False) -> SensorSchnappschuss:
        t0 = time.monotonic()
        s = SensorSchnappschuss(str(a.rdm_uid), jetzt_utc(), 0.0)
        for sd in sensoren:
            if not (alle_sensoren or sd.messrelevant):
                continue
            try:
                w = R.lese_sensor_wert(await self._get(a, K.PID_SENSOR_VALUE, bytes((sd.nummer,))))
            except RdmFehler as f:
                s.fehler.append(f"Sensor {sd.nummer} ({sd.beschreibung}): {f}")
                continue
            if w.nummer != sd.nummer:
                s.fehler.append(f"Sensor {sd.nummer}: Antwort für Sensor {w.nummer}")
                continue
            s.werte.append(SensorMesswert(
                nummer=sd.nummer, beschreibung=sd.beschreibung, typ=sd.typ, typ_text=sd.typ_text,
                einheit_text=sd.einheit_text, roh=w.aktuell, wert=sd.skaliert(w.aktuell),
                niedrigster=sd.skaliert(w.niedrigster) if sd.hat_min_max else None,
                hoechster=sd.skaliert(w.hoechster) if sd.hat_min_max else None,
                aufgezeichnet=sd.skaliert(w.aufgezeichnet) if sd.hat_recorded else None,
                ausserhalb_normal=not (sd.normal_min <= w.aktuell <= sd.normal_max),
                messrelevant=sd.messrelevant))
        s.dauer_s = round(time.monotonic() - t0, 4)
        return s
