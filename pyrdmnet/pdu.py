"""ACN/E1.33-Rahmen: TCP-Präambel, Root-Layer, Broker-, RPT-, Request-,
Notification-, Status- und RDM-Command-PDUs.

Alle PDUs werden mit gesetzten Flags L|V|H|D (0xF0) und 20-Bit-Länge
geschrieben, wie in E1.33 vorgesehen. Beim Lesen wird geprüft, dass V/H/D
gesetzt sind (keine Vererbung); andernfalls wird die Nachricht abgelehnt.
"""

from __future__ import annotations

import asyncio
import struct
import uuid
from dataclasses import dataclass, field

from . import konstanten as K
from .rdm import Uid

MAX_BLOCK = 64 * 1024  # Schutz gegen übergroße Blöcke von fehlerhaften Clients


class ProtokollFehler(ValueError):
    """Eine RDMnet-Nachricht ist formal ungültig."""


# ------------------------------------------------------------------- Grundbausteine
def flags_laenge(n: int) -> bytes:
    if not 0 < n < (1 << 20):
        raise ProtokollFehler(f"PDU-Länge {n} nicht darstellbar")
    return bytes((K.ACN_PDU_FLAGS | ((n >> 16) & 0x0F), (n >> 8) & 0xFF, n & 0xFF))


def lese_flags_laenge(buf: bytes, pos: int) -> tuple[int, int]:
    """Gibt (PDU-Länge, Kopfgröße 2 oder 3) zurück."""
    if pos + 2 > len(buf):
        raise ProtokollFehler("PDU-Kopf abgeschnitten")
    flags = buf[pos]
    if flags & 0x70 != 0x70:
        raise ProtokollFehler(f"PDU-Vererbung (Flags {flags:#04x}) wird nicht unterstützt")
    if flags & 0x80:
        if pos + 3 > len(buf):
            raise ProtokollFehler("PDU-Kopf abgeschnitten")
        n = ((flags & 0x0F) << 16) | (buf[pos + 1] << 8) | buf[pos + 2]
        kopf = 3
    else:
        n = ((flags & 0x0F) << 8) | buf[pos + 1]
        kopf = 2
    if n < kopf or pos + n > len(buf):
        raise ProtokollFehler(f"PDU-Länge {n} passt nicht zum Puffer")
    return n, kopf


def pdus_aufteilen(buf: bytes) -> list[tuple[bytes, int]]:
    """Teilt einen Puffer in aufeinanderfolgende PDUs; liefert (PDU, Kopfgröße)."""
    pos, teile = 0, []
    while pos < len(buf):
        n, kopf = lese_flags_laenge(buf, pos)
        teile.append((buf[pos:pos + n], kopf))
        pos += n
    return teile


# ------------------------------------------------------------------- Root Layer
@dataclass
class RootPdu:
    vektor: int
    cid: uuid.UUID
    daten: bytes


def baue_paket(root_vektor: int, cid: uuid.UUID, inhalt: bytes) -> bytes:
    root = flags_laenge(3 + 4 + 16 + len(inhalt)) + struct.pack(">I", root_vektor) + cid.bytes + inhalt
    return K.ACN_TCP_PREAMBEL + struct.pack(">I", len(root)) + root


def lese_root_pdus(block: bytes) -> list[RootPdu]:
    ergebnis = []
    for pdu, kopf in pdus_aufteilen(block):
        if len(pdu) < kopf + 20:
            raise ProtokollFehler("Root-PDU zu kurz")
        (vektor,) = struct.unpack(">I", pdu[kopf:kopf + 4])
        cid = uuid.UUID(bytes=bytes(pdu[kopf + 4:kopf + 20]))
        ergebnis.append(RootPdu(vektor, cid, bytes(pdu[kopf + 20:])))
    return ergebnis


async def lese_tcp_block(reader: asyncio.StreamReader, max_block: int = MAX_BLOCK) -> bytes:
    """Liest Präambel + Blockgröße + Block. Wirft ProtokollFehler bei falscher Präambel."""
    kopf = await reader.readexactly(16)
    if kopf[:12] != K.ACN_TCP_PREAMBEL:
        raise ProtokollFehler("Falsche ACN-TCP-Präambel")
    (groesse,) = struct.unpack(">I", kopf[12:])
    if groesse == 0 or groesse > max_block:
        raise ProtokollFehler(f"PDU-Blockgröße {groesse} unzulässig")
    return await reader.readexactly(groesse)


# ------------------------------------------------------------------- Broker-PDUs
@dataclass
class BrokerPdu:
    vektor: int
    daten: bytes


def baue_broker(vektor: int, daten: bytes = b"") -> bytes:
    return flags_laenge(5 + len(daten)) + struct.pack(">H", vektor) + daten


def lese_broker(daten: bytes) -> BrokerPdu:
    teile = pdus_aufteilen(daten)
    if len(teile) != 1:
        raise ProtokollFehler("Genau eine Broker-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) < kopf + 2:
        raise ProtokollFehler("Broker-PDU zu kurz")
    (vektor,) = struct.unpack(">H", pdu[kopf:kopf + 2])
    return BrokerPdu(vektor, bytes(pdu[kopf + 2:]))


@dataclass
class ClientEintrag:
    """RPT-Client-Eintrag (E1.33 6.3.2)."""

    cid: uuid.UUID
    uid: Uid
    typ: int  # RPT_CLIENT_TYP_GERAET / _CONTROLLER
    binding_cid: uuid.UUID = field(default_factory=lambda: uuid.UUID(int=0))
    protokoll: int = K.E133_CLIENT_PROTOKOLL_RPT

    def kodieren(self) -> bytes:
        nutz = self.uid.als_bytes() + bytes((self.typ,)) + self.binding_cid.bytes
        return flags_laenge(3 + 4 + 16 + len(nutz)) + struct.pack(">I", self.protokoll) + self.cid.bytes + nutz

    @property
    def typ_text(self) -> str:
        return {0: "Gerät", 1: "Controller"}.get(self.typ, f"Typ {self.typ}")


def lese_client_eintraege(daten: bytes) -> list[ClientEintrag]:
    eintraege = []
    for pdu, kopf in pdus_aufteilen(daten):
        if len(pdu) < kopf + 20:
            raise ProtokollFehler("Client-Eintrag zu kurz")
        (prot,) = struct.unpack(">I", pdu[kopf:kopf + 4])
        cid = uuid.UUID(bytes=bytes(pdu[kopf + 4:kopf + 20]))
        nutz = pdu[kopf + 20:]
        if prot != K.E133_CLIENT_PROTOKOLL_RPT:
            # EPT-Clients werden erkannt, aber von diesem Modul nicht unterstützt.
            eintraege.append(ClientEintrag(cid, Uid(0, 0), 0xFF, protokoll=prot))
            continue
        if len(nutz) != 23:
            raise ProtokollFehler(f"RPT-Client-Eintrag mit {len(nutz)} statt 23 Byte")
        eintraege.append(ClientEintrag(cid, Uid.aus_bytes(bytes(nutz[:6])), nutz[6],
                                       uuid.UUID(bytes=bytes(nutz[7:23]))))
    return eintraege


@dataclass
class ConnectDaten:
    scope: str
    version: int
    domain: str
    flags: int
    client: ClientEintrag

    def kodieren(self) -> bytes:
        scope = self.scope.encode("utf-8")[:K.E133_SCOPE_LAENGE - 1]
        domain = self.domain.encode("utf-8")[:K.E133_DOMAIN_LAENGE - 1]
        return (scope.ljust(K.E133_SCOPE_LAENGE, b"\x00") + struct.pack(">H", self.version)
                + domain.ljust(K.E133_DOMAIN_LAENGE, b"\x00") + bytes((self.flags,)) + self.client.kodieren())


def _nullstring(b: bytes) -> str:
    return b.split(b"\x00", 1)[0].decode("utf-8", errors="replace")


def lese_connect(daten: bytes) -> ConnectDaten:
    fest = K.E133_SCOPE_LAENGE + 2 + K.E133_DOMAIN_LAENGE + 1
    if len(daten) < fest + 23:
        raise ProtokollFehler("Connect-Nachricht zu kurz")
    scope = _nullstring(daten[:K.E133_SCOPE_LAENGE])
    (version,) = struct.unpack(">H", daten[K.E133_SCOPE_LAENGE:K.E133_SCOPE_LAENGE + 2])
    dpos = K.E133_SCOPE_LAENGE + 2
    domain = _nullstring(daten[dpos:dpos + K.E133_DOMAIN_LAENGE])
    flags = daten[fest - 1]
    eintraege = lese_client_eintraege(daten[fest:])
    if len(eintraege) != 1:
        raise ProtokollFehler("Connect muss genau einen Client-Eintrag enthalten")
    return ConnectDaten(scope, version, domain, flags, eintraege[0])


@dataclass
class ConnectReply:
    code: int
    version: int
    broker_uid: Uid
    client_uid: Uid

    def kodieren(self) -> bytes:
        return struct.pack(">HH", self.code, self.version) + self.broker_uid.als_bytes() + self.client_uid.als_bytes()


def lese_connect_reply(daten: bytes) -> ConnectReply:
    if len(daten) != 16:
        raise ProtokollFehler(f"Connect-Reply mit {len(daten)} statt 16 Byte")
    code, version = struct.unpack(">HH", daten[:4])
    return ConnectReply(code, version, Uid.aus_bytes(daten[4:10]), Uid.aus_bytes(daten[10:16]))


@dataclass
class DynUidAnfrage:
    uid: Uid  # Herstellerkennung mit gesetztem MSB, Geräte-ID 0
    rid: uuid.UUID


def lese_dyn_uid_anfragen(daten: bytes) -> list[DynUidAnfrage]:
    if len(daten) % 22:
        raise ProtokollFehler("Dynamic-UID-Anfrage: Länge kein Vielfaches von 22")
    return [DynUidAnfrage(Uid.aus_bytes(daten[i:i + 6]), uuid.UUID(bytes=bytes(daten[i + 6:i + 22])))
            for i in range(0, len(daten), 22)]


def baue_dyn_uid_zuordnungen(zuordnungen: list[tuple[Uid, uuid.UUID, int]]) -> bytes:
    return b"".join(u.als_bytes() + r.bytes + struct.pack(">H", s) for u, r, s in zuordnungen)


def lese_dyn_uid_zuordnungen(daten: bytes) -> list[tuple[Uid, uuid.UUID, int]]:
    if len(daten) % 24:
        raise ProtokollFehler("Dynamic-UID-Zuordnung: Länge kein Vielfaches von 24")
    return [(Uid.aus_bytes(daten[i:i + 6]), uuid.UUID(bytes=bytes(daten[i + 6:i + 22])),
             struct.unpack(">H", daten[i + 22:i + 24])[0]) for i in range(0, len(daten), 24)]


def lese_uid_liste(daten: bytes) -> list[Uid]:
    if len(daten) % 6:
        raise ProtokollFehler("UID-Liste: Länge kein Vielfaches von 6")
    return [Uid.aus_bytes(daten[i:i + 6]) for i in range(0, len(daten), 6)]


# ------------------------------------------------------------------- RPT
@dataclass
class RptKopf:
    vektor: int
    quelle: Uid
    quelle_endpoint: int
    ziel: Uid
    ziel_endpoint: int
    sequenz: int


@dataclass
class RptPdu:
    kopf: RptKopf
    daten: bytes  # Request-, Status- oder Notification-PDU


def baue_rpt(kopf: RptKopf, daten: bytes) -> bytes:
    return (flags_laenge(28 + len(daten)) + struct.pack(">I", kopf.vektor) + kopf.quelle.als_bytes()
            + struct.pack(">H", kopf.quelle_endpoint) + kopf.ziel.als_bytes()
            + struct.pack(">HIB", kopf.ziel_endpoint, kopf.sequenz & 0xFFFFFFFF, 0) + daten)


def lese_rpt(daten: bytes) -> RptPdu:
    teile = pdus_aufteilen(daten)
    if len(teile) != 1:
        raise ProtokollFehler("Genau eine RPT-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) < kopf + 25:
        raise ProtokollFehler("RPT-PDU zu kurz")
    p = kopf
    (vektor,) = struct.unpack(">I", pdu[p:p + 4])
    quelle = Uid.aus_bytes(bytes(pdu[p + 4:p + 10]))
    (q_ep,) = struct.unpack(">H", pdu[p + 10:p + 12])
    ziel = Uid.aus_bytes(bytes(pdu[p + 12:p + 18]))
    z_ep, seq, _res = struct.unpack(">HIB", pdu[p + 18:p + 25])
    return RptPdu(RptKopf(vektor, quelle, q_ep, ziel, z_ep, seq), bytes(pdu[p + 25:]))


def baue_rdm_cmd_pdu(rdm_roh: bytes) -> bytes:
    """RDM-Command-PDU: Flags+Länge, Vektor 0xCC (= Startcode), Rest der RDM-Nachricht."""
    if not rdm_roh or rdm_roh[0] != K.VECTOR_RDM_CMD_RDM_DATA:
        raise ProtokollFehler("RDM-Nachricht muss mit Startcode 0xCC beginnen")
    return flags_laenge(3 + len(rdm_roh)) + rdm_roh


def _lese_rdm_cmd_pdus(daten: bytes) -> list[bytes]:
    nachrichten = []
    for pdu, kopf in pdus_aufteilen(daten):
        if len(pdu) <= kopf or pdu[kopf] != K.VECTOR_RDM_CMD_RDM_DATA:
            raise ProtokollFehler("RDM-Command-PDU mit falschem Vektor")
        nachrichten.append(bytes(pdu[kopf:]))  # inkl. Startcode
    return nachrichten


def baue_request(rdm_roh: bytes) -> bytes:
    cmd = baue_rdm_cmd_pdu(rdm_roh)
    return flags_laenge(7 + len(cmd)) + struct.pack(">I", K.VECTOR_REQUEST_RDM_CMD) + cmd


def baue_notification(rdm_nachrichten: list[bytes]) -> bytes:
    cmds = b"".join(baue_rdm_cmd_pdu(r) for r in rdm_nachrichten)
    return flags_laenge(7 + len(cmds)) + struct.pack(">I", K.VECTOR_NOTIFICATION_RDM_CMD) + cmds


def lese_request_oder_notification(daten: bytes, erwarteter_vektor: int) -> list[bytes]:
    teile = pdus_aufteilen(daten)
    if len(teile) != 1:
        raise ProtokollFehler("Genau eine Request-/Notification-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) < kopf + 4:
        raise ProtokollFehler("Request-/Notification-PDU zu kurz")
    (vektor,) = struct.unpack(">I", pdu[kopf:kopf + 4])
    if vektor != erwarteter_vektor:
        raise ProtokollFehler(f"Unerwarteter Vektor {vektor:#x}")
    return _lese_rdm_cmd_pdus(bytes(pdu[kopf + 4:]))


def baue_status(code: int, text: str = "") -> bytes:
    t = text.encode("utf-8")[:1024]
    return flags_laenge(5 + len(t)) + struct.pack(">H", code) + t


def lese_status(daten: bytes) -> tuple[int, str]:
    teile = pdus_aufteilen(daten)
    if len(teile) != 1:
        raise ProtokollFehler("Genau eine Status-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) < kopf + 2:
        raise ProtokollFehler("Status-PDU zu kurz")
    (code,) = struct.unpack(">H", pdu[kopf:kopf + 2])
    return code, bytes(pdu[kopf + 2:]).decode("utf-8", errors="replace")
