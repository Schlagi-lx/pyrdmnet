"""Low Level Recovery Protocol (LLRP, ANSI E1.33 Abschnitt 5).

LLRP läuft per UDP-Multicast und braucht keinen Broker. Damit lassen sich
RDMnet-Komponenten (z. B. ein Gateway mit falschem Scope) finden und ihr
Scope, ihre Suchdomäne oder eine statische Broker-Adresse lesen und setzen.

* :class:`LlrpManager` – Discovery nach E1.33 5.6.3 und RDM GET/SET an Targets.
  SET nur für ``ERLAUBTE_SET_PIDS`` und nur mit Schreibfreigabe; jede Änderung
  wird zurückgelesen.
* :class:`LlrpTarget` – antwortet auf Probe Requests und RDM-Befehle; wird vom
  Broker (``BrokerKonfig.llrp``) und in Tests genutzt.

Einschränkungen: nur IPv4 (E1.33 verlangt vom Manager zusätzlich IPv6).
Netzwerkeinstellungen nach E1.37-2 (IP-Adresse, DHCP …) werden bewusst nicht
gesetzt. Als Hardware-Adresse meldet das Target ``uuid.getnode()``.
"""

from __future__ import annotations

import asyncio
import logging
import random
import socket
import struct
import sys
import uuid
from dataclasses import dataclass, field
from typing import Callable

from . import konstanten as K
from . import pdu as P
from . import rdm_parameter as R
from .controller import RdmAntwortFehler, RdmNackFehler, RdmZeitFehler, SchreibSperreFehler
from .rdm import RdmFormatFehler, RdmNachricht, TransaktionsZaehler, Uid

log = logging.getLogger("pyrdmnet.llrp")

BROADCAST_CID = uuid.UUID(K.LLRP_BROADCAST_CID)
UID_MIN, UID_MAX = 0, (1 << 48) - 1
ERLAUBTE_SET_PIDS = {K.PID_COMPONENT_SCOPE, K.PID_SEARCH_DOMAIN, K.PID_IDENTIFY_DEVICE}


# ------------------------------------------------------------------- Kodierung
@dataclass
class LlrpPaket:
    sender_cid: uuid.UUID
    vektor: int
    ziel_cid: uuid.UUID
    transaktion: int
    daten: bytes


def baue_llrp(sender_cid: uuid.UUID, vektor: int, ziel_cid: uuid.UUID, transaktion: int, daten: bytes) -> bytes:
    llrp = P.flags_laenge(3 + 4 + 16 + 4 + len(daten)) + struct.pack(">I", vektor) + ziel_cid.bytes \
        + struct.pack(">I", transaktion & 0xFFFFFFFF) + daten
    root = P.flags_laenge(3 + 4 + 16 + len(llrp)) + struct.pack(">I", K.VECTOR_ROOT_LLRP) + sender_cid.bytes + llrp
    return K.ACN_UDP_PREAMBEL + root


def lese_llrp(datagramm: bytes) -> LlrpPaket:
    if datagramm[:16] != K.ACN_UDP_PREAMBEL:
        raise P.ProtokollFehler("Keine ACN-UDP-Präambel")
    roots = P.lese_root_pdus(datagramm[16:])
    if len(roots) != 1 or roots[0].vektor != K.VECTOR_ROOT_LLRP:
        raise P.ProtokollFehler("Keine LLRP-Root-PDU")
    teile = P.pdus_aufteilen(roots[0].daten)
    if len(teile) != 1:
        raise P.ProtokollFehler("Genau eine LLRP-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) < kopf + 24:
        raise P.ProtokollFehler("LLRP-PDU zu kurz")
    vektor, = struct.unpack(">I", pdu[kopf:kopf + 4])
    ziel = uuid.UUID(bytes=bytes(pdu[kopf + 4:kopf + 20]))
    tn, = struct.unpack(">I", pdu[kopf + 20:kopf + 24])
    return LlrpPaket(roots[0].cid, vektor, ziel, tn, bytes(pdu[kopf + 24:]))


@dataclass
class ProbeAnfrage:
    untere: Uid
    obere: Uid
    filter: int = K.LLRP_FILTER_NUR_UNVERBUNDEN
    bekannte: list[Uid] = field(default_factory=list)


def baue_probe_anfrage(a: ProbeAnfrage) -> bytes:
    if len(a.bekannte) > K.LLRP_KNOWN_UID_SIZE:
        raise P.ProtokollFehler(f"Mehr als {K.LLRP_KNOWN_UID_SIZE} bekannte UIDs")
    nutz = a.untere.als_bytes() + a.obere.als_bytes() + struct.pack(">H", a.filter) \
        + b"".join(u.als_bytes() for u in a.bekannte)
    return P.flags_laenge(4 + len(nutz)) + bytes((K.VECTOR_PROBE_REQUEST_DATA,)) + nutz


def lese_probe_anfrage(daten: bytes) -> ProbeAnfrage:
    teile = P.pdus_aufteilen(daten)
    if len(teile) != 1:
        raise P.ProtokollFehler("Genau eine Probe-Request-PDU erwartet")
    pdu, kopf = teile[0]
    nutz = pdu[kopf + 1:]
    if pdu[kopf] != K.VECTOR_PROBE_REQUEST_DATA or len(nutz) < 14 or (len(nutz) - 14) % 6:
        raise P.ProtokollFehler("Probe Request ungültig")
    bekannte = [Uid.aus_bytes(bytes(nutz[i:i + 6])) for i in range(14, len(nutz), 6)]
    return ProbeAnfrage(Uid.aus_bytes(bytes(nutz[:6])), Uid.aus_bytes(bytes(nutz[6:12])),
                        struct.unpack(">H", nutz[12:14])[0], bekannte)


@dataclass
class ProbeAntwort:
    uid: Uid
    hardware: bytes  # 6 Byte, meist MAC-Adresse
    typ: int


def baue_probe_antwort(a: ProbeAntwort) -> bytes:
    nutz = a.uid.als_bytes() + a.hardware[:6].rjust(6, b"\x00") + bytes((a.typ,))
    return P.flags_laenge(4 + len(nutz)) + bytes((K.VECTOR_PROBE_REPLY_DATA,)) + nutz


def lese_probe_antwort(daten: bytes) -> ProbeAntwort:
    teile = P.pdus_aufteilen(daten)
    if len(teile) != 1:
        raise P.ProtokollFehler("Genau eine Probe-Reply-PDU erwartet")
    pdu, kopf = teile[0]
    nutz = pdu[kopf + 1:]
    if pdu[kopf] != K.VECTOR_PROBE_REPLY_DATA or len(nutz) != 13:
        raise P.ProtokollFehler("Probe Reply ungültig")
    return ProbeAntwort(Uid.aus_bytes(bytes(nutz[:6])), bytes(nutz[6:12]), nutz[12])


def lese_rdm_cmd(daten: bytes) -> bytes:
    """RDM Command PDU (5.4.2.3) -> rohe RDM-Nachricht inkl. Startcode."""
    teile = P.pdus_aufteilen(daten)
    if len(teile) != 1:
        raise P.ProtokollFehler("Genau eine RDM-Command-PDU erwartet")
    pdu, kopf = teile[0]
    if len(pdu) <= kopf or pdu[kopf] != K.VECTOR_RDM_CMD_RDM_DATA:
        raise P.ProtokollFehler("RDM-Command-PDU mit falschem Vektor")
    return bytes(pdu[kopf:])


def hardware_adresse() -> bytes:
    return uuid.getnode().to_bytes(6, "big")


# ------------------------------------------------------------------- Transport
@dataclass
class LlrpNetz:
    """Adressen für LLRP. Standard: Multicast nach E1.33; Tests nutzen Unicast auf 127.0.0.1."""

    adapter: str = "0.0.0.0"  # IP des Netzwerkadapters (0.0.0.0 = Standard des Systems)
    anfrage_ziel: str = K.LLRP_IPV4_ANFRAGE
    antwort_ziel: str = K.LLRP_IPV4_ANTWORT
    anfrage_port: int = K.LLRP_PORT
    antwort_port: int = K.LLRP_PORT
    multicast: bool = True
    ttl: int = 1  # >1: Antworten können aus anderen Netzsegmenten kommen (E1.33 5.3.3)


def _udp_socket(netz: LlrpNetz, port: int, gruppe: str) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass
    try:
        if netz.multicast:
            s.bind(("", port))
            mreq = socket.inet_aton(gruppe) + socket.inet_aton(netz.adapter)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, netz.ttl)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            if netz.adapter != "0.0.0.0":
                s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(netz.adapter))
            multicast_all = getattr(socket, "IP_MULTICAST_ALL", 49 if sys.platform.startswith("linux") else None)
            if multicast_all is not None:
                try:  # Linux: nur Gruppen empfangen, die dieser Socket abonniert hat
                    s.setsockopt(socket.IPPROTO_IP, multicast_all, 0)
                except OSError:
                    pass
        else:
            s.bind((netz.adapter, port))
    except OSError:
        s.close()
        raise
    s.setblocking(False)
    return s


class _Protokoll(asyncio.DatagramProtocol):
    def __init__(self, empfang: Callable[[bytes, tuple], None]):
        self.empfang = empfang

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        try:
            self.empfang(data, addr)
        except Exception:  # ein fehlerhaftes Paket darf den Empfang nicht beenden
            log.exception("LLRP-Paket konnte nicht verarbeitet werden")


async def _endpunkt(sock: socket.socket, empfang) -> asyncio.DatagramTransport:
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(lambda: _Protokoll(empfang), sock=sock)
    return transport


# ------------------------------------------------------------------- Manager
@dataclass(frozen=True)
class LlrpZiel:
    """Ein per LLRP gefundenes Target. UID allein ist nicht eindeutig (dynamische UIDs), daher mit CID."""

    cid: uuid.UUID
    uid: Uid
    hardware: bytes
    typ: int
    adresse: str  # IP, von der die Antwort kam

    @property
    def typ_text(self) -> str:
        return K.LLRP_TYP_TEXT.get(self.typ, f"Typ {self.typ:#04x}")

    @property
    def hardware_text(self) -> str:
        return ":".join(f"{b:02x}" for b in self.hardware)


class LlrpManager:
    """LLRP-Manager (E1.33 5.6). Verwendung::

        async with LlrpManager(LlrpNetz(adapter="192.168.1.10")) as m:
            ziele = await m.entdecken()
            print(await m.scope_lesen(ziele[0]))
    """

    def __init__(self, netz: LlrpNetz | None = None, uid: Uid | None = None, cid: uuid.UUID | None = None,
                 zeitlimit_s: float = K.LLRP_TIMEOUT_S, wiederholungen: int = 1):
        self.netz = netz or LlrpNetz()
        # E1.33 5.6.1: Manager-UID darf dynamisch sein; sie wird nur in RDM-Befehlen verwendet
        self.uid = uid or Uid(0x8000 | K.ESTA_PROTOTYP_HERSTELLER, 0x4C4C5250)  # „LLRP“
        self.cid = cid or uuid.uuid4()
        self.zeitlimit_s = zeitlimit_s
        self.wiederholungen = wiederholungen
        self.schreibfreigabe = False
        self.protokoll: list[dict] = []  # Änderungen alt -> neu
        self._tn = 0
        self._rdm_tn = TransaktionsZaehler()
        self._transport: asyncio.DatagramTransport | None = None
        self._probe: tuple[int, dict] | None = None  # (Transaktion, {(cid, uid): LlrpZiel})
        self._offen: dict[int, asyncio.Future] = {}

    async def starten(self) -> None:
        sock = _udp_socket(self.netz, self.netz.antwort_port, self.netz.antwort_ziel)
        self._transport = await _endpunkt(sock, self._empfangen)

    async def stoppen(self) -> None:
        if self._transport:
            self._transport.close()
            self._transport = None

    async def __aenter__(self) -> "LlrpManager":
        await self.starten()
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.stoppen()

    # --- Senden/Empfangen
    def _naechste_tn(self) -> int:
        self._tn = (self._tn + 1) & 0xFFFFFFFF
        return self._tn

    def _senden(self, vektor: int, ziel_cid: uuid.UUID, daten: bytes) -> int:
        if self._transport is None:
            raise RuntimeError("LLRP-Manager nicht gestartet")
        tn = self._naechste_tn()
        self._transport.sendto(baue_llrp(self.cid, vektor, ziel_cid, tn, daten),
                               (self.netz.anfrage_ziel, self.netz.anfrage_port))
        return tn

    def _empfangen(self, data: bytes, addr: tuple) -> None:
        try:
            p = lese_llrp(data)
        except P.ProtokollFehler:
            return
        if p.ziel_cid != self.cid:  # E1.33 5.6.2
            return
        if p.vektor == K.VECTOR_LLRP_PROBE_REPLY and self._probe and self._probe[0] == p.transaktion:
            try:
                a = lese_probe_antwort(p.daten)
            except P.ProtokollFehler:
                return
            self._probe[1][(p.sender_cid, a.uid)] = LlrpZiel(p.sender_cid, a.uid, a.hardware, a.typ, addr[0])
        elif p.vektor == K.VECTOR_LLRP_RDM_CMD:
            f = self._offen.get(p.transaktion)
            if f and not f.done():
                try:
                    f.set_result((p.sender_cid, lese_rdm_cmd(p.daten)))
                except P.ProtokollFehler as fehler:
                    f.set_exception(RdmAntwortFehler(f"LLRP-Antwort ungültig: {fehler}"))

    # --- Discovery (E1.33 5.6.3)
    async def entdecken(self, filter: int = K.LLRP_FILTER_NUR_UNVERBUNDEN) -> list[LlrpZiel]:
        """Sucht Targets. Standard-Filter nach Norm: nur Clients ohne Broker-Verbindung
        (Broker antworten immer). ``filter=0`` findet auch verbundene Clients."""
        gefunden: dict[tuple[uuid.UUID, Uid], LlrpZiel] = {}
        await self._bereich(UID_MIN, UID_MAX, filter, gefunden)
        return sorted(gefunden.values(), key=lambda z: (z.uid, str(z.cid)))

    async def _bereich(self, unten: int, oben: int, filter: int, gefunden: dict) -> None:
        stille = 0
        while True:
            bekannt = sorted({z.uid for z in gefunden.values() if unten <= z.uid.als_int() <= oben})
            if len(bekannt) > K.LLRP_KNOWN_UID_SIZE:  # Schritt 1: Bereich teilen
                mitte = (unten + oben) // 2
                await self._bereich(unten, mitte, filter, gefunden)
                await self._bereich(mitte + 1, oben, filter, gefunden)
                return
            neu: dict = {}
            anfrage = ProbeAnfrage(Uid.aus_int(unten), Uid.aus_int(oben), filter, bekannt)
            self._probe = (self._senden(K.VECTOR_LLRP_PROBE_REQUEST, BROADCAST_CID, baue_probe_anfrage(anfrage)), neu)
            await asyncio.sleep(self.zeitlimit_s)  # Schritt 3: LLRP_TIMEOUT abwarten
            self._probe = None
            frisch = {k: v for k, v in neu.items() if k not in gefunden}
            gefunden.update(neu)
            if frisch:
                stille = 0  # Schritt 4
                continue
            stille += 1  # Schritt 5: zwei weitere Versuche ohne Antwort
            if stille >= 3:
                return

    # --- RDM über LLRP
    async def rdm_get(self, ziel: LlrpZiel, pid: int, daten: bytes = b"") -> bytes:
        return await self._rdm(ziel, K.CC_GET, pid, daten)

    async def rdm_set(self, ziel: LlrpZiel, pid: int, daten: bytes) -> bytes:
        if pid not in ERLAUBTE_SET_PIDS:
            raise SchreibSperreFehler(f"SET für {K.PID_NAMEN.get(pid, hex(pid))} über LLRP nicht freigegeben")
        if not self.schreibfreigabe:
            raise SchreibSperreFehler("Keine Schreibfreigabe für diese Sitzung")
        return await self._rdm(ziel, K.CC_SET, pid, daten)

    async def _rdm(self, ziel: LlrpZiel, cc: int, pid: int, daten: bytes) -> bytes:
        anfrage = RdmNachricht(ziel=ziel.uid, quelle=self.uid, transaktion=self._rdm_tn.naechste(ziel.uid),
                               port_oder_antworttyp=1, command_class=cc, pid=pid, daten=daten)
        roh = P.baue_rdm_cmd_pdu(anfrage.kodieren())
        for versuch in range(self.wiederholungen + 1):
            fut = asyncio.get_running_loop().create_future()
            tn = self._senden(K.VECTOR_LLRP_RDM_CMD, ziel.cid, roh)
            self._offen[tn] = fut
            try:
                sender, antwort_roh = await asyncio.wait_for(fut, self.zeitlimit_s)
                break
            except asyncio.TimeoutError:
                if versuch == self.wiederholungen:
                    raise RdmZeitFehler(f"Keine LLRP-Antwort auf {K.PID_NAMEN.get(pid, hex(pid))} "
                                        f"von {ziel.uid}") from None
            finally:
                self._offen.pop(tn, None)
        try:
            n = RdmNachricht.dekodieren(antwort_roh)
        except RdmFormatFehler as fehler:
            raise RdmAntwortFehler(f"RDM-Antwort ungültig: {fehler}") from fehler
        soll_cc = K.CC_GET_RESPONSE if cc == K.CC_GET else K.CC_SET_RESPONSE
        if sender != ziel.cid or n.quelle != ziel.uid or n.pid != pid or n.command_class != soll_cc \
                or n.transaktion != anfrage.transaktion:
            raise RdmAntwortFehler(f"LLRP-Antwort passt nicht zur Anfrage (PID {n.pid:#06x})")
        if n.antworttyp == K.ANTWORT_ACK:
            return n.daten
        if n.antworttyp == K.ANTWORT_NACK:
            grund = int.from_bytes(n.daten[:2], "big") if len(n.daten) >= 2 else 0xFFFF
            raise RdmNackFehler(pid, grund)
        raise RdmAntwortFehler(f"Antworttyp {n.antworttyp} ist über LLRP unzulässig")

    # --- Komfort: Scope und Suchdomäne
    async def scope_lesen(self, ziel: LlrpZiel) -> R.ComponentScope:
        return R.lese_component_scope(await self.rdm_get(ziel, K.PID_COMPONENT_SCOPE, struct.pack(">H", 1)))

    async def suchdomain_lesen(self, ziel: LlrpZiel) -> str:
        return R.lese_suchdomain(await self.rdm_get(ziel, K.PID_SEARCH_DOMAIN))

    async def scope_setzen(self, ziel: LlrpZiel, scope: str,
                           statischer_broker: tuple[str, int] | None = None) -> tuple[R.ComponentScope, R.ComponentScope]:
        """Setzt Scope (Slot 1) und optional eine statische IPv4-Broker-Adresse; liest zurück.

        Ohne ``statischer_broker`` sucht das Gerät den Broker per mDNS. Achtung: Ein
        verbundenes Gerät trennt sich bei Scopewechsel vom Broker.
        """
        raw = scope.encode("utf-8")
        if not 1 <= len(raw) <= K.E133_SCOPE_LAENGE - 1:
            raise ValueError(f"Scope muss 1 bis {K.E133_SCOPE_LAENGE - 1} Byte lang sein")
        alt = await self.scope_lesen(ziel)
        neu = R.ComponentScope(1, scope)
        if statischer_broker:
            neu.statisch_typ = K.E133_STATISCHE_KONFIG_IPV4
            neu.statisch_ipv4, neu.statisch_port = statischer_broker
        await self.rdm_set(ziel, K.PID_COMPONENT_SCOPE, R.baue_component_scope(neu))
        gelesen = await self.scope_lesen(ziel)
        self._protokollieren(ziel, "COMPONENT_SCOPE", (alt.scope, alt.statische_adresse),
                             (gelesen.scope, gelesen.statische_adresse))
        if (gelesen.scope, gelesen.statische_adresse) != (neu.scope, neu.statische_adresse):
            raise RdmAntwortFehler(f"Rücklesen weicht ab: {gelesen.scope!r} / {gelesen.statische_adresse}")
        return alt, gelesen

    async def suchdomain_setzen(self, ziel: LlrpZiel, domain: str) -> tuple[str, str]:
        alt = await self.suchdomain_lesen(ziel)
        await self.rdm_set(ziel, K.PID_SEARCH_DOMAIN, domain.encode("utf-8")[:K.E133_DOMAIN_LAENGE])
        gelesen = await self.suchdomain_lesen(ziel)
        self._protokollieren(ziel, "SEARCH_DOMAIN", alt, gelesen)
        if gelesen != domain:
            raise RdmAntwortFehler(f"Rücklesen weicht ab: {gelesen!r}")
        return alt, gelesen

    def _protokollieren(self, ziel: LlrpZiel, parameter: str, alt, neu) -> None:
        eintrag = {"uid": str(ziel.uid), "cid": str(ziel.cid), "parameter": parameter, "alt": alt, "neu": neu}
        self.protokoll.append(eintrag)
        log.info("LLRP SET %s", eintrag)


# ------------------------------------------------------------------- Target
class LlrpTarget:
    """LLRP-Target (E1.33 5.7). ``responder`` beantwortet RDM-Anfragen (z. B. BrokerResponder)."""

    def __init__(self, uid: Callable[[], Uid] | Uid, cid: uuid.UUID, typ: int,
                 responder: Callable[[RdmNachricht], RdmNachricht | None],
                 netz: LlrpNetz | None = None, verbunden: Callable[[], bool] | None = None,
                 max_backoff_s: float = K.LLRP_MAX_BACKOFF_S):
        self._uid = uid if callable(uid) else (lambda: uid)
        self.cid = cid
        self.typ = typ
        self.responder = responder
        self.netz = netz or LlrpNetz()
        self.verbunden = verbunden or (lambda: False)
        self.max_backoff_s = max_backoff_s
        self.zaehler = {"probe": 0, "probe_antwort": 0, "rdm": 0}
        self._transport: asyncio.DatagramTransport | None = None
        self._tasks: set[asyncio.Task] = set()

    @property
    def uid(self) -> Uid:
        return self._uid()

    async def starten(self) -> None:
        sock = _udp_socket(self.netz, self.netz.anfrage_port, self.netz.anfrage_ziel)
        self._transport = await _endpunkt(sock, self._empfangen)

    async def stoppen(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._transport:
            self._transport.close()
            self._transport = None

    def _antworten(self, ziel_cid: uuid.UUID, vektor: int, tn: int, daten: bytes) -> None:
        if self._transport:
            self._transport.sendto(baue_llrp(self.cid, vektor, ziel_cid, tn, daten),
                                   (self.netz.antwort_ziel, self.netz.antwort_port))

    def _empfangen(self, data: bytes, _addr: tuple) -> None:
        try:
            p = lese_llrp(data)
        except P.ProtokollFehler:
            return
        if p.ziel_cid not in (BROADCAST_CID, self.cid):  # E1.33 5.7.2
            return
        if p.vektor == K.VECTOR_LLRP_PROBE_REQUEST:
            self._probe(p)
        elif p.vektor == K.VECTOR_LLRP_RDM_CMD:
            self._rdm(p)

    def _probe(self, p: LlrpPaket) -> None:
        try:
            a = lese_probe_anfrage(p.daten)
        except P.ProtokollFehler:
            return
        self.zaehler["probe"] += 1
        uid = self.uid
        if not a.untere <= uid <= a.obere or uid in a.bekannte:
            return
        ist_broker = self.typ == K.LLRP_TYP_BROKER
        if a.filter & K.LLRP_FILTER_NUR_BROKER and not ist_broker:
            return
        if (a.filter & K.LLRP_FILTER_NUR_UNVERBUNDEN and not ist_broker
                and self.typ != K.LLRP_TYP_KEIN_RDMNET and self.verbunden()):
            return
        antwort = baue_probe_antwort(ProbeAntwort(uid, hardware_adresse(), self.typ))

        async def verzoegert():  # E1.33 5.7.3: zufällige Wartezeit 0 … LLRP_MAX_BACKOFF
            await asyncio.sleep(random.uniform(0, self.max_backoff_s))
            self.zaehler["probe_antwort"] += 1
            self._antworten(p.sender_cid, K.VECTOR_LLRP_PROBE_REPLY, p.transaktion, antwort)

        t = asyncio.ensure_future(verzoegert())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _rdm(self, p: LlrpPaket) -> None:
        try:
            anfrage = RdmNachricht.dekodieren(lese_rdm_cmd(p.daten))
        except (P.ProtokollFehler, RdmFormatFehler):
            return
        if anfrage.ziel != self.uid and not anfrage.ziel.ist_broadcast:  # E1.33 5.7.4
            return
        if anfrage.command_class not in (K.CC_GET, K.CC_SET):
            return
        self.zaehler["rdm"] += 1
        if anfrage.subdevice not in (K.RDM_ROOT_DEVICE, 0xFFFF):
            antwort = anfrage.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_SUB_DEVICE_OUT_OF_RANGE))
        else:
            antwort = self.responder(anfrage)
        if antwort is None or anfrage.ziel.ist_broadcast:
            return
        if antwort.antworttyp in (K.ANTWORT_ACK_TIMER, K.ANTWORT_ACK_OVERFLOW):  # E1.33 5.7.2
            antwort = anfrage.antwort(K.ANTWORT_NACK, struct.pack(">H", K.NR_ACTION_NOT_SUPPORTED))
        self._antworten(p.sender_cid, K.VECTOR_LLRP_RDM_CMD, p.transaktion, P.baue_rdm_cmd_pdu(antwort.kodieren()))


# ------------------------------------------------------------------- einfacher Komponenten-Responder
class KomponentenResponder:
    """RDM-Responder für LLRP-Targets von Geräten/Controllern (Simulator, Tests).

    Unterstützt DEVICE_INFO, Labels, IDENTIFY_DEVICE sowie COMPONENT_SCOPE und
    SEARCH_DOMAIN (GET/SET). ``bei_aenderung(pid)`` wird nach einem SET aufgerufen.
    """

    def __init__(self, uid: Uid, scope: str = K.E133_STANDARD_SCOPE, domain: str = K.E133_STANDARD_DOMAIN,
                 modell: str = "pyrdmnet LLRP", produktkategorie: int = 0x0100,
                 bei_aenderung: Callable[[int], None] | None = None):
        self.uid = uid
        self.scope = R.ComponentScope(1, scope)
        self.domain = domain
        self.modell = modell
        self.produktkategorie = produktkategorie
        self.identifizieren = False
        self.bei_aenderung = bei_aenderung or (lambda pid: None)

    def __call__(self, a: RdmNachricht) -> RdmNachricht | None:
        nack = lambda g: a.antwort(K.ANTWORT_NACK, struct.pack(">H", g))  # noqa: E731
        pid, d = a.pid, a.daten
        if a.command_class == K.CC_GET:
            if pid == K.PID_DEVICE_INFO:
                return a.antwort(K.ANTWORT_ACK, R.baue_device_info(R.DeviceInfo(
                    0x0100, 1, self.produktkategorie, 1, 0, 0, 0, 0xFFFF, 0, 0)))
            if pid in (K.PID_DEVICE_MODEL_DESCRIPTION, K.PID_DEVICE_LABEL):
                return a.antwort(K.ANTWORT_ACK, R.baue_text(self.modell))
            if pid == K.PID_MANUFACTURER_LABEL:
                return a.antwort(K.ANTWORT_ACK, R.baue_text("pyrdmnet Simulator"))
            if pid == K.PID_IDENTIFY_DEVICE:
                return a.antwort(K.ANTWORT_ACK, bytes((int(self.identifizieren),)))
            if pid == K.PID_COMPONENT_SCOPE:
                if len(d) != 2:
                    return nack(K.NR_FORMAT_ERROR)
                if d != b"\x00\x01":
                    return nack(K.NR_DATA_OUT_OF_RANGE)
                return a.antwort(K.ANTWORT_ACK, R.baue_component_scope(self.scope))
            if pid == K.PID_SEARCH_DOMAIN:
                return a.antwort(K.ANTWORT_ACK, self.domain.encode("utf-8"))
            return nack(K.NR_UNKNOWN_PID)
        if pid == K.PID_IDENTIFY_DEVICE:
            if len(d) != 1 or d[0] > 1:
                return nack(K.NR_FORMAT_ERROR)
            self.identifizieren = bool(d[0])
        elif pid == K.PID_COMPONENT_SCOPE:
            try:
                neu = R.lese_component_scope(d)
            except RdmFormatFehler:
                return nack(K.NR_FORMAT_ERROR)
            if neu.slot != 1:
                return nack(K.NR_DATA_OUT_OF_RANGE)
            if not neu.scope:
                return nack(K.NR_DATA_OUT_OF_RANGE)  # Geräte haben genau einen Scope
            self.scope = neu
        elif pid == K.PID_SEARCH_DOMAIN:
            if len(d) > K.E133_DOMAIN_LAENGE:
                return nack(K.NR_DATA_OUT_OF_RANGE)
            self.domain = d.decode("utf-8", errors="replace")
        else:
            return nack(K.NR_UNKNOWN_PID)
        self.bei_aenderung(pid)
        return a.antwort(K.ANTWORT_ACK)
