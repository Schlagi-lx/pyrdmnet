"""Parameterdaten ausgewählter RDM-PIDs lesen (Controller) und erzeugen (Simulator).

Jede ``lese_*``-Funktion prüft die Länge und wirft :class:`RdmFormatFehler`
bei abweichendem Format. Es wird nichts geraten oder aufgefüllt.
"""

from __future__ import annotations

import struct
from dataclasses import asdict, dataclass

from . import konstanten as K
from .rdm import RdmFormatFehler, Uid


def _text(daten: bytes) -> str:
    """RDM-Texte sind ASCII bis 32 Zeichen, evtl. nullterminiert."""
    return daten.split(b"\x00", 1)[0].decode("ascii", errors="replace").strip()


def _laenge(daten: bytes, soll: int, pid: str) -> None:
    if len(daten) != soll:
        raise RdmFormatFehler(f"{pid}: {len(daten)} Byte statt {soll}")


# ------------------------------------------------------------------- DEVICE_INFO
@dataclass
class DeviceInfo:
    rdm_protokoll: int
    modell_id: int
    produktkategorie: int
    software_version_id: int
    footprint: int
    personality_aktuell: int
    personality_anzahl: int
    startadresse: int  # 0xFFFF = kein Footprint
    subdevice_anzahl: int
    sensor_anzahl: int

    @property
    def produktkategorie_text(self) -> str:
        return K.PRODUKTKATEGORIE_GRUPPE.get(self.produktkategorie >> 8, "unbekannt")

    def als_dict(self) -> dict:
        d = asdict(self)
        d["produktkategorie_text"] = self.produktkategorie_text
        return d


_DI = struct.Struct(">HHHIHBBHHB")  # 19 Byte


def lese_device_info(daten: bytes) -> DeviceInfo:
    _laenge(daten, _DI.size, "DEVICE_INFO")
    return DeviceInfo(*_DI.unpack(daten))


def baue_device_info(di: DeviceInfo) -> bytes:
    return _DI.pack(di.rdm_protokoll, di.modell_id, di.produktkategorie, di.software_version_id,
                    di.footprint, di.personality_aktuell, di.personality_anzahl, di.startadresse,
                    di.subdevice_anzahl, di.sensor_anzahl)


# ------------------------------------------------------------------- Texte
def lese_text(daten: bytes) -> str:
    if len(daten) > 32:
        raise RdmFormatFehler(f"Text länger als 32 Byte ({len(daten)})")
    return _text(daten)


def baue_text(text: str) -> bytes:
    return text.encode("ascii", errors="replace")[:32]


# ------------------------------------------------------------------- Personality
@dataclass
class Personality:
    nummer: int
    footprint: int
    beschreibung: str


def lese_personality(daten: bytes) -> tuple[int, int]:
    _laenge(daten, 2, "DMX_PERSONALITY")
    return daten[0], daten[1]


def lese_personality_beschreibung(daten: bytes) -> Personality:
    if not 3 <= len(daten) <= 35:
        raise RdmFormatFehler(f"DMX_PERSONALITY_DESCRIPTION: {len(daten)} Byte")
    nr, fp = struct.unpack(">BH", daten[:3])
    return Personality(nr, fp, _text(daten[3:]))


def baue_personality_beschreibung(p: Personality) -> bytes:
    return struct.pack(">BH", p.nummer, p.footprint) + baue_text(p.beschreibung)


def lese_u16(daten: bytes, pid: str) -> int:
    _laenge(daten, 2, pid)
    return struct.unpack(">H", daten)[0]


def lese_u32(daten: bytes, pid: str) -> int:
    _laenge(daten, 4, pid)
    return struct.unpack(">I", daten)[0]


def lese_pid_liste(daten: bytes) -> list[int]:
    if len(daten) % 2:
        raise RdmFormatFehler("SUPPORTED_PARAMETERS: ungerade Länge")
    return [v for (v,) in struct.iter_unpack(">H", daten)]


# ------------------------------------------------------------------- Sensoren
@dataclass
class SensorDefinition:
    nummer: int
    typ: int
    einheit: int
    praefix: int
    bereich_min: int
    bereich_max: int
    normal_min: int
    normal_max: int
    recorded_unterstuetzung: int  # Bit0: recorded value, Bit1: lowest/highest
    beschreibung: str

    @property
    def faktor(self) -> float:
        return 10.0 ** K.PRAEFIX_EXPONENT.get(self.praefix, 0)

    @property
    def praefix_bekannt(self) -> bool:
        return self.praefix in K.PRAEFIX_EXPONENT

    @property
    def typ_text(self) -> str:
        return K.SENSORTYP_TEXT.get(self.typ, f"herstellerspezifisch ({self.typ:#04x})")

    @property
    def einheit_text(self) -> str:
        return K.EINHEIT_TEXT.get(self.einheit, f"Einheit {self.einheit:#04x}")

    @property
    def messrelevant(self) -> bool:
        return self.typ in K.SENSORTYPEN_MESSRELEVANT

    @property
    def hat_recorded(self) -> bool:
        return bool(self.recorded_unterstuetzung & 0x01)

    @property
    def hat_min_max(self) -> bool:
        return bool(self.recorded_unterstuetzung & 0x02)

    def skaliert(self, roh: int) -> float:
        return roh * self.faktor

    def als_dict(self) -> dict:
        d = asdict(self)
        d.update(typ_text=self.typ_text, einheit_text=self.einheit_text, messrelevant=self.messrelevant,
                 faktor=self.faktor)
        return d


_SD = struct.Struct(">BBBBhhhhB")  # 13 Byte + Beschreibung


def lese_sensor_definition(daten: bytes) -> SensorDefinition:
    if not _SD.size <= len(daten) <= _SD.size + 32:
        raise RdmFormatFehler(f"SENSOR_DEFINITION: {len(daten)} Byte")
    werte = _SD.unpack(daten[:_SD.size])
    return SensorDefinition(*werte, beschreibung=_text(daten[_SD.size:]))


def baue_sensor_definition(sd: SensorDefinition) -> bytes:
    return _SD.pack(sd.nummer, sd.typ, sd.einheit, sd.praefix, sd.bereich_min, sd.bereich_max,
                    sd.normal_min, sd.normal_max, sd.recorded_unterstuetzung) + baue_text(sd.beschreibung)


@dataclass
class SensorRohwert:
    nummer: int
    aktuell: int
    niedrigster: int
    hoechster: int
    aufgezeichnet: int


_SV = struct.Struct(">Bhhhh")  # 9 Byte


def lese_sensor_wert(daten: bytes) -> SensorRohwert:
    _laenge(daten, _SV.size, "SENSOR_VALUE")
    return SensorRohwert(*_SV.unpack(daten))


def baue_sensor_wert(w: SensorRohwert) -> bytes:
    return _SV.pack(w.nummer, w.aktuell, w.niedrigster, w.hoechster, w.aufgezeichnet)


# ------------------------------------------------------------------- Endpoints (E1.37-7)
@dataclass
class Endpoint:
    nummer: int
    typ: int


def lese_endpoint_liste(daten: bytes) -> tuple[int, list[Endpoint]]:
    if len(daten) < 4 or (len(daten) - 4) % 3:
        raise RdmFormatFehler(f"ENDPOINT_LIST: {len(daten)} Byte")
    (aenderung,) = struct.unpack(">I", daten[:4])
    eps = [Endpoint(n, t) for n, t in struct.iter_unpack(">HB", daten[4:])]
    return aenderung, eps


def baue_endpoint_liste(aenderung: int, eps: list[Endpoint]) -> bytes:
    return struct.pack(">I", aenderung) + b"".join(struct.pack(">HB", e.nummer, e.typ) for e in eps)


def lese_endpoint_responder(daten: bytes) -> tuple[int, int, list[Uid]]:
    if len(daten) < 6 or (len(daten) - 6) % 6:
        raise RdmFormatFehler(f"ENDPOINT_RESPONDERS: {len(daten)} Byte")
    ep, aenderung = struct.unpack(">HI", daten[:6])
    uids = [Uid.aus_bytes(daten[i:i + 6]) for i in range(6, len(daten), 6)]
    return ep, aenderung, uids


def baue_endpoint_responder(ep: int, aenderung: int, uids: list[Uid]) -> bytes:
    return struct.pack(">HI", ep, aenderung) + b"".join(u.als_bytes() for u in uids)


def lese_endpoint_universe(daten: bytes) -> tuple[int, int]:
    _laenge(daten, 4, "ENDPOINT_TO_UNIVERSE")
    return struct.unpack(">HH", daten)


# ------------------------------------------------------------------- E1.33 COMPONENT_SCOPE / SEARCH_DOMAIN
# Hinweis: E1.33-2019 nennt als PDL 0x54, die Felder ergeben aber 88 Byte (0x58);
# die ETC-Referenzimplementierung sendet ebenfalls 88 Byte. Gesendet wird 88.
COMPONENT_SCOPE_LAENGE = 2 + K.E133_SCOPE_LAENGE + 1 + 4 + 16 + 2


@dataclass
class ComponentScope:
    slot: int
    scope: str
    statisch_typ: int = K.E133_KEINE_STATISCHE_KONFIG
    statisch_ipv4: str = "0.0.0.0"
    statisch_ipv6: bytes = bytes(16)
    statisch_port: int = 0

    @property
    def statische_adresse(self) -> str | None:
        if self.statisch_typ == K.E133_STATISCHE_KONFIG_IPV4:
            return f"{self.statisch_ipv4}:{self.statisch_port}"
        if self.statisch_typ == K.E133_STATISCHE_KONFIG_IPV6:
            import ipaddress
            return f"[{ipaddress.IPv6Address(self.statisch_ipv6)}]:{self.statisch_port}"
        return None


def baue_component_scope(cs: ComponentScope) -> bytes:
    roh = cs.scope.encode("utf-8")[:K.E133_SCOPE_LAENGE - 1]
    ipv4 = bytes(int(t) for t in cs.statisch_ipv4.split("."))
    if len(ipv4) != 4 or len(cs.statisch_ipv6) != 16:
        raise RdmFormatFehler("COMPONENT_SCOPE: statische Adresse ungültig")
    return (struct.pack(">H", cs.slot) + roh.ljust(K.E133_SCOPE_LAENGE, b"\x00") + bytes((cs.statisch_typ,))
            + ipv4 + cs.statisch_ipv6 + struct.pack(">H", cs.statisch_port))


def lese_component_scope(daten: bytes) -> ComponentScope:
    _laenge(daten, COMPONENT_SCOPE_LAENGE, "COMPONENT_SCOPE")
    (slot,) = struct.unpack(">H", daten[:2])
    scope = daten[2:2 + K.E133_SCOPE_LAENGE].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
    p = 2 + K.E133_SCOPE_LAENGE
    ipv4 = ".".join(str(b) for b in daten[p + 1:p + 5])
    (port,) = struct.unpack(">H", daten[p + 21:p + 23])
    return ComponentScope(slot, scope, daten[p], ipv4, bytes(daten[p + 5:p + 21]), port)


def lese_suchdomain(daten: bytes) -> str:
    if len(daten) > K.E133_DOMAIN_LAENGE:
        raise RdmFormatFehler(f"SEARCH_DOMAIN länger als {K.E133_DOMAIN_LAENGE} Byte")
    return daten.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
