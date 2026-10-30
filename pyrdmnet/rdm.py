"""RDM-Nachrichten nach ANSI E1.20: UID, Kodierung, Dekodierung, Prüfsumme."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

from . import konstanten as K


class RdmFormatFehler(ValueError):
    """Eine RDM-Nachricht ist formal ungültig (Länge, Startcode, Prüfsumme)."""


# ------------------------------------------------------------------- UID
@dataclass(frozen=True, order=True)
class Uid:
    """RDM-UID: 16 Bit Herstellerkennung + 32 Bit Gerätekennung."""

    hersteller: int
    geraet: int

    def __post_init__(self) -> None:
        if not 0 <= self.hersteller <= 0xFFFF or not 0 <= self.geraet <= 0xFFFFFFFF:
            raise ValueError(f"UID außerhalb des Wertebereichs: {self.hersteller:#x}:{self.geraet:#x}")

    @classmethod
    def aus_int(cls, wert: int) -> "Uid":
        return cls((wert >> 32) & 0xFFFF, wert & 0xFFFFFFFF)

    @classmethod
    def aus_bytes(cls, daten: bytes) -> "Uid":
        if len(daten) != 6:
            raise RdmFormatFehler("UID braucht genau 6 Byte")
        h, g = struct.unpack(">HI", daten)
        return cls(h, g)

    @classmethod
    def aus_text(cls, text: str) -> "Uid":
        """Akzeptiert ``7ff0:00000001`` oder ``7ff000000001``."""
        t = text.strip().replace(":", "")
        if len(t) != 12:
            raise ValueError(f"UID-Text ungültig: {text!r}")
        return cls.aus_int(int(t, 16))

    def als_int(self) -> int:
        return (self.hersteller << 32) | self.geraet

    def als_bytes(self) -> bytes:
        return struct.pack(">HI", self.hersteller, self.geraet)

    # --- Eigenschaften nach E1.20 / E1.33
    @property
    def ist_broadcast(self) -> bool:
        return self.geraet == 0xFFFFFFFF

    @property
    def ist_dynamisch_angefragt(self) -> bool:
        """E1.33: Dynamische UID wird angefragt mit gesetztem MSB und Geräte-ID 0."""
        return bool(self.hersteller & 0x8000) and self.geraet == 0

    @property
    def ist_dynamisch(self) -> bool:
        return bool(self.hersteller & 0x8000)

    @property
    def ist_gueltig_fuer_client(self) -> bool:
        return self.als_int() != 0 and not self.ist_broadcast

    def __str__(self) -> str:
        return f"{self.hersteller:04x}:{self.geraet:08x}"


ALLE_CONTROLLER = Uid.aus_int(K.E133_RPT_ALLE_CONTROLLER)
ALLE_GERAETE = Uid.aus_int(K.E133_RPT_ALLE_GERAETE)
NULL_UID = Uid(0, 0)


def ist_geraete_broadcast(uid: Uid) -> bool:
    """RPT_ALL_DEVICES (0xFFFD FFFFFFFF) oder RPT_ALL_MID_DEVICES (0xFFFD mmmm FFFF)."""
    w = uid.als_int()
    if w == K.E133_RPT_ALLE_GERAETE:
        return True
    return (w >> 32) == 0xFFFD and (w & 0xFFFF) == 0xFFFF


def hersteller_aus_mid_broadcast(uid: Uid) -> int | None:
    w = uid.als_int()
    if (w >> 32) == 0xFFFD and (w & 0xFFFF) == 0xFFFF and w != K.E133_RPT_ALLE_GERAETE:
        return (w >> 16) & 0xFFFF
    return None


# ------------------------------------------------------------------- Nachricht
def pruefsumme(daten: bytes) -> int:
    return sum(daten) & 0xFFFF


@dataclass
class RdmNachricht:
    """Eine RDM-Nachricht (Anfrage oder Antwort)."""

    ziel: Uid
    quelle: Uid
    transaktion: int
    port_oder_antworttyp: int
    command_class: int
    pid: int
    daten: bytes = b""
    nachrichtenzaehler: int = 0
    subdevice: int = K.RDM_ROOT_DEVICE

    # --- abgeleitete Eigenschaften
    @property
    def ist_antwort(self) -> bool:
        return self.command_class in (K.CC_GET_RESPONSE, K.CC_SET_RESPONSE, K.CC_DISCOVERY_RESPONSE)

    @property
    def antworttyp(self) -> int:
        return self.port_oder_antworttyp

    def kodieren(self) -> bytes:
        if len(self.daten) > K.RDM_MAX_PDL:
            raise RdmFormatFehler(f"Parameterdaten zu lang ({len(self.daten)} > {K.RDM_MAX_PDL})")
        laenge = K.RDM_KOPF_LAENGE + len(self.daten)
        kopf = struct.pack(
            ">BBB6s6sBBBHBHB",
            K.RDM_STARTCODE,
            K.RDM_SUB_STARTCODE,
            laenge,
            self.ziel.als_bytes(),
            self.quelle.als_bytes(),
            self.transaktion & 0xFF,
            self.port_oder_antworttyp & 0xFF,
            self.nachrichtenzaehler & 0xFF,
            self.subdevice & 0xFFFF,
            self.command_class & 0xFF,
            self.pid & 0xFFFF,
            len(self.daten),
        )
        roh = kopf + self.daten
        return roh + struct.pack(">H", pruefsumme(roh))

    @classmethod
    def dekodieren(cls, roh: bytes) -> "RdmNachricht":
        if len(roh) < K.RDM_KOPF_LAENGE + 2:
            raise RdmFormatFehler(f"RDM-Nachricht zu kurz ({len(roh)} Byte)")
        if roh[0] != K.RDM_STARTCODE or roh[1] != K.RDM_SUB_STARTCODE:
            raise RdmFormatFehler("Falscher RDM-Start- oder Sub-Startcode")
        laenge = roh[2]
        if laenge < K.RDM_KOPF_LAENGE or len(roh) < laenge + 2:
            raise RdmFormatFehler(f"Längenfeld {laenge} passt nicht zu {len(roh)} Byte")
        pdl = roh[23]
        if laenge != K.RDM_KOPF_LAENGE + pdl:
            raise RdmFormatFehler(f"PDL {pdl} passt nicht zum Längenfeld {laenge}")
        (soll,) = struct.unpack(">H", roh[laenge:laenge + 2])
        ist = pruefsumme(roh[:laenge])
        if soll != ist:
            raise RdmFormatFehler(f"Prüfsumme falsch (gelesen {soll:#06x}, berechnet {ist:#06x})")
        (_, _, _, ziel, quelle, tn, port, mc, sub, cc, pid, _pdl) = struct.unpack(
            ">BBB6s6sBBBHBHB", roh[:K.RDM_KOPF_LAENGE]
        )
        return cls(
            ziel=Uid.aus_bytes(ziel),
            quelle=Uid.aus_bytes(quelle),
            transaktion=tn,
            port_oder_antworttyp=port,
            nachrichtenzaehler=mc,
            subdevice=sub,
            command_class=cc,
            pid=pid,
            daten=bytes(roh[K.RDM_KOPF_LAENGE:laenge]),
        )

    def antwort(self, antworttyp: int, daten: bytes = b"", nachrichtenzaehler: int = 0) -> "RdmNachricht":
        """Erzeugt die passende Antwort auf diese Anfrage (für Responder/Simulator)."""
        cc_antwort = {K.CC_GET: K.CC_GET_RESPONSE, K.CC_SET: K.CC_SET_RESPONSE,
                      K.CC_DISCOVERY: K.CC_DISCOVERY_RESPONSE}.get(self.command_class, self.command_class)
        return RdmNachricht(
            ziel=self.quelle,
            quelle=self.ziel,
            transaktion=self.transaktion,
            port_oder_antworttyp=antworttyp,
            nachrichtenzaehler=nachrichtenzaehler,
            subdevice=self.subdevice,
            command_class=cc_antwort,
            pid=self.pid,
            daten=daten,
        )


@dataclass
class TransaktionsZaehler:
    """Transaktionsnummer je RDM-Ziel (E1.20: 8 Bit, fortlaufend)."""

    _stand: dict = field(default_factory=dict)

    def naechste(self, ziel: Uid) -> int:
        n = (self._stand.get(ziel, -1) + 1) & 0xFF
        self._stand[ziel] = n
        return n
