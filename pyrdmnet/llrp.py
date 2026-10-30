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

from . import constants as C
from . import pdu as P
from . import rdm_params as R
from .controller import RdmResponseError, RdmNackError, RdmTimeoutError, WriteLockError
from .rdm import RdmFormatError, RdmMessage, TransactionCounter, Uid

log = logging.getLogger("pyrdmnet.llrp")

BROADCAST_CID = uuid.UUID(C.LLRP_BROADCAST_CID)
UID_MIN, UID_MAX = 0, (1 << 48) - 1
ALLOWED_SET_PIDS = {C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_IDENTIFY_DEVICE}


# ------------------------------------------------------------------- Kodierung
@dataclass
class LlrpPacket:
    sender_cid: uuid.UUID
    vector: int
    dest_cid: uuid.UUID
    transaction: int
    data: bytes


def build_llrp(sender_cid: uuid.UUID, vector: int, dest_cid: uuid.UUID, transaction: int, data: bytes) -> bytes:
    llrp = P.flags_length(3 + 4 + 16 + 4 + len(data)) + struct.pack(">I", vector) + dest_cid.bytes \
        + struct.pack(">I", transaction & 0xFFFFFFFF) + data
    root = P.flags_length(3 + 4 + 16 + len(llrp)) + struct.pack(">I", C.VECTOR_ROOT_LLRP) + sender_cid.bytes + llrp
    return C.ACN_UDP_PREAMBLE + root


def read_llrp(datagram: bytes) -> LlrpPacket:
    if datagram[:16] != C.ACN_UDP_PREAMBLE:
        raise P.ProtocolError("Keine ACN-UDP-Präambel")
    roots = P.read_root_pdus(datagram[16:])
    if len(roots) != 1 or roots[0].vector != C.VECTOR_ROOT_LLRP:
        raise P.ProtocolError("Keine LLRP-Root-PDU")
    parts = P.split_pdus(roots[0].data)
    if len(parts) != 1:
        raise P.ProtocolError("Genau eine LLRP-PDU erwartet")
    pdu, header = parts[0]
    if len(pdu) < header + 24:
        raise P.ProtocolError("LLRP-PDU zu kurz")
    vector, = struct.unpack(">I", pdu[header:header + 4])
    target = uuid.UUID(bytes=bytes(pdu[header + 4:header + 20]))
    tn, = struct.unpack(">I", pdu[header + 20:header + 24])
    return LlrpPacket(roots[0].cid, vector, target, tn, bytes(pdu[header + 24:]))


@dataclass
class ProbeRequest:
    lower: Uid
    upper: Uid
    filter: int = C.LLRP_FILTER_CLIENT_CONN_INACTIVE
    known: list[Uid] = field(default_factory=list)


def build_probe_request(a: ProbeRequest) -> bytes:
    if len(a.known) > C.LLRP_KNOWN_UID_SIZE:
        raise P.ProtocolError(f"Mehr als {C.LLRP_KNOWN_UID_SIZE} bekannte UIDs")
    payload = a.lower.to_bytes() + a.upper.to_bytes() + struct.pack(">H", a.filter) \
        + b"".join(u.to_bytes() for u in a.known)
    return P.flags_length(4 + len(payload)) + bytes((C.VECTOR_PROBE_REQUEST_DATA,)) + payload


def read_probe_request(data: bytes) -> ProbeRequest:
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Genau eine Probe-Request-PDU erwartet")
    pdu, header = parts[0]
    payload = pdu[header + 1:]
    if pdu[header] != C.VECTOR_PROBE_REQUEST_DATA or len(payload) < 14 or (len(payload) - 14) % 6:
        raise P.ProtocolError("Probe Request ungültig")
    known = [Uid.from_bytes(bytes(payload[i:i + 6])) for i in range(14, len(payload), 6)]
    return ProbeRequest(Uid.from_bytes(bytes(payload[:6])), Uid.from_bytes(bytes(payload[6:12])),
                        struct.unpack(">H", payload[12:14])[0], known)


@dataclass
class ProbeReply:
    uid: Uid
    hardware: bytes  # 6 Byte, meist MAC-Adresse
    type: int


def build_probe_reply(a: ProbeReply) -> bytes:
    payload = a.uid.to_bytes() + a.hardware[:6].rjust(6, b"\x00") + bytes((a.type,))
    return P.flags_length(4 + len(payload)) + bytes((C.VECTOR_PROBE_REPLY_DATA,)) + payload


def read_probe_reply(data: bytes) -> ProbeReply:
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Genau eine Probe-Reply-PDU erwartet")
    pdu, header = parts[0]
    payload = pdu[header + 1:]
    if pdu[header] != C.VECTOR_PROBE_REPLY_DATA or len(payload) != 13:
        raise P.ProtocolError("Probe Reply ungültig")
    return ProbeReply(Uid.from_bytes(bytes(payload[:6])), bytes(payload[6:12]), payload[12])


def read_rdm_cmd(data: bytes) -> bytes:
    """RDM Command PDU (5.4.2.3) -> rohe RDM-Nachricht inkl. Startcode."""
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Genau eine RDM-Command-PDU erwartet")
    pdu, header = parts[0]
    if len(pdu) <= header or pdu[header] != C.VECTOR_RDM_CMD_RDM_DATA:
        raise P.ProtocolError("RDM-Command-PDU mit falschem Vektor")
    return bytes(pdu[header:])


def hardware_address() -> bytes:
    return uuid.getnode().to_bytes(6, "big")


# ------------------------------------------------------------------- Transport
@dataclass
class LlrpNetwork:
    """Adressen für LLRP. Standard: Multicast nach E1.33; Tests nutzen Unicast auf 127.0.0.1."""

    adapter: str = "0.0.0.0"  # IP des Netzwerkadapters (0.0.0.0 = Standard des Systems)
    request_addr: str = C.LLRP_IPV4_REQUEST
    response_addr: str = C.LLRP_IPV4_RESPONSE
    request_port: int = C.LLRP_PORT
    response_port: int = C.LLRP_PORT
    multicast: bool = True
    ttl: int = 1  # >1: Antworten können aus anderen Netzsegmenten kommen (E1.33 5.3.3)


def _udp_socket(network: LlrpNetwork, port: int, group: str) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass
    try:
        if network.multicast:
            s.bind(("", port))
            mreq = socket.inet_aton(group) + socket.inet_aton(network.adapter)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, network.ttl)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            if network.adapter != "0.0.0.0":
                s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(network.adapter))
            multicast_all = getattr(socket, "IP_MULTICAST_ALL", 49 if sys.platform.startswith("linux") else None)
            if multicast_all is not None:
                try:  # Linux: nur Gruppen empfangen, die dieser Socket abonniert hat
                    s.setsockopt(socket.IPPROTO_IP, multicast_all, 0)
                except OSError:
                    pass
        else:
            s.bind((network.adapter, port))
    except OSError:
        s.close()
        raise
    s.setblocking(False)
    return s


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, on_receive: Callable[[bytes, tuple], None]):
        self.on_receive = on_receive

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        try:
            self.on_receive(data, addr)
        except Exception:  # ein fehlerhaftes Paket darf den Empfang nicht beenden
            log.exception("LLRP-Paket konnte nicht verarbeitet werden")


async def _endpoint(sock: socket.socket, on_receive) -> asyncio.DatagramTransport:
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(lambda: _Protocol(on_receive), sock=sock)
    return transport


# ------------------------------------------------------------------- Manager
@dataclass(frozen=True)
class DiscoveredTarget:
    """Ein per LLRP gefundenes Target. UID allein ist nicht eindeutig (dynamische UIDs), daher mit CID."""

    cid: uuid.UUID
    uid: Uid
    hardware: bytes
    type: int
    address: str  # IP, von der die Antwort kam

    @property
    def type_text(self) -> str:
        return C.LLRP_TYPE_TEXT.get(self.type, f"Typ {self.type:#04x}")

    @property
    def hardware_text(self) -> str:
        return ":".join(f"{b:02x}" for b in self.hardware)


class LlrpManager:
    """LLRP-Manager (E1.33 5.6). Verwendung::

        async with LlrpManager(LlrpNetz(adapter="192.168.1.10")) as m:
            ziele = await m.entdecken()
            print(await m.scope_lesen(ziele[0]))
    """

    def __init__(self, network: LlrpNetwork | None = None, uid: Uid | None = None, cid: uuid.UUID | None = None,
                 timeout_window_s: float = C.LLRP_TIMEOUT_S, retries: int = 1):
        self.network = network or LlrpNetwork()
        # E1.33 5.6.1: Manager-UID darf dynamisch sein; sie wird nur in RDM-Befehlen verwendet
        self.uid = uid or Uid(0x8000 | C.ESTA_PROTOTYPE_MANUFACTURER, 0x4C4C5250)  # „LLRP“
        self.cid = cid or uuid.uuid4()
        self.timeout_window_s = timeout_window_s
        self.retries = retries
        self.write_enabled = False
        self.protocol: list[dict] = []  # Änderungen alt -> neu
        self._tn = 0
        self._rdm_tn = TransactionCounter()
        self._transport: asyncio.DatagramTransport | None = None
        self._probe: tuple[int, dict] | None = None  # (Transaktion, {(cid, uid): LlrpZiel})
        self._pending: dict[int, asyncio.Future] = {}

    async def start(self) -> None:
        sock = _udp_socket(self.network, self.network.response_port, self.network.response_addr)
        self._transport = await _endpoint(sock, self._on_datagram)

    async def stop(self) -> None:
        if self._transport:
            self._transport.close()
            self._transport = None

    async def __aenter__(self) -> "LlrpManager":
        await self.start()
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.stop()

    # --- Senden/Empfangen
    def _next_tn(self) -> int:
        self._tn = (self._tn + 1) & 0xFFFFFFFF
        return self._tn

    def _send(self, vector: int, dest_cid: uuid.UUID, data: bytes) -> int:
        if self._transport is None:
            raise RuntimeError("LLRP-Manager nicht gestartet")
        tn = self._next_tn()
        self._transport.sendto(build_llrp(self.cid, vector, dest_cid, tn, data),
                               (self.network.request_addr, self.network.request_port))
        return tn

    def _on_datagram(self, data: bytes, addr: tuple) -> None:
        try:
            p = read_llrp(data)
        except P.ProtocolError:
            return
        if p.dest_cid != self.cid:  # E1.33 5.6.2
            return
        if p.vector == C.VECTOR_LLRP_PROBE_REPLY and self._probe and self._probe[0] == p.transaction:
            try:
                a = read_probe_reply(p.data)
            except P.ProtocolError:
                return
            self._probe[1][(p.sender_cid, a.uid)] = DiscoveredTarget(p.sender_cid, a.uid, a.hardware, a.type, addr[0])
        elif p.vector == C.VECTOR_LLRP_RDM_CMD:
            f = self._pending.get(p.transaction)
            if f and not f.done():
                try:
                    f.set_result((p.sender_cid, read_rdm_cmd(p.data)))
                except P.ProtocolError as error:
                    f.set_exception(RdmResponseError(f"LLRP-Antwort ungültig: {error}"))

    # --- Discovery (E1.33 5.6.3)
    async def discover(self, filter: int = C.LLRP_FILTER_CLIENT_CONN_INACTIVE) -> list[DiscoveredTarget]:
        """Sucht Targets. Standard-Filter nach Norm: nur Clients ohne Broker-Verbindung
        (Broker antworten immer). ``filter=0`` findet auch verbundene Clients."""
        found: dict[tuple[uuid.UUID, Uid], DiscoveredTarget] = {}
        await self._probe_range(UID_MIN, UID_MAX, filter, found)
        return sorted(found.values(), key=lambda z: (z.uid, str(z.cid)))

    async def _probe_range(self, lo: int, hi: int, filter: int, found: dict) -> None:
        silent_rounds = 0
        while True:
            known = sorted({z.uid for z in found.values() if lo <= z.uid.to_int() <= hi})
            if len(known) > C.LLRP_KNOWN_UID_SIZE:  # Schritt 1: Bereich teilen
                middle = (lo + hi) // 2
                await self._probe_range(lo, middle, filter, found)
                await self._probe_range(middle + 1, hi, filter, found)
                return
            new: dict = {}
            request = ProbeRequest(Uid.from_int(lo), Uid.from_int(hi), filter, known)
            self._probe = (self._send(C.VECTOR_LLRP_PROBE_REQUEST, BROADCAST_CID, build_probe_request(request)), new)
            await asyncio.sleep(self.timeout_window_s)  # Schritt 3: LLRP_TIMEOUT abwarten
            self._probe = None
            fresh = {k: v for k, v in new.items() if k not in found}
            found.update(new)
            if fresh:
                silent_rounds = 0  # Schritt 4
                continue
            silent_rounds += 1  # Schritt 5: zwei weitere Versuche ohne Antwort
            if silent_rounds >= 3:
                return

    # --- RDM über LLRP
    async def rdm_get(self, target: DiscoveredTarget, pid: int, data: bytes = b"") -> bytes:
        return await self._rdm(target, C.CC_GET, pid, data)

    async def rdm_set(self, target: DiscoveredTarget, pid: int, data: bytes) -> bytes:
        if pid not in ALLOWED_SET_PIDS:
            raise WriteLockError(f"SET für {C.PID_NAMES.get(pid, hex(pid))} über LLRP nicht freigegeben")
        if not self.write_enabled:
            raise WriteLockError("Keine Schreibfreigabe für diese Sitzung")
        return await self._rdm(target, C.CC_SET, pid, data)

    async def _rdm(self, target: DiscoveredTarget, cc: int, pid: int, data: bytes) -> bytes:
        request = RdmMessage(dest=target.uid, source=self.uid, transaction=self._rdm_tn.next_tn(target.uid),
                               port_or_response_type=1, command_class=cc, pid=pid, data=data)
        raw = P.build_rdm_cmd_pdu(request.encode())
        for attempt in range(self.retries + 1):
            fut = asyncio.get_running_loop().create_future()
            tn = self._send(C.VECTOR_LLRP_RDM_CMD, target.cid, raw)
            self._pending[tn] = fut
            try:
                sender, response_raw = await asyncio.wait_for(fut, self.timeout_window_s)
                break
            except asyncio.TimeoutError:
                if attempt == self.retries:
                    raise RdmTimeoutError(f"Keine LLRP-Antwort auf {C.PID_NAMES.get(pid, hex(pid))} "
                                        f"von {target.uid}") from None
            finally:
                self._pending.pop(tn, None)
        try:
            n = RdmMessage.decode(response_raw)
        except RdmFormatError as error:
            raise RdmResponseError(f"RDM-Antwort ungültig: {error}") from error
        expected_cc = C.CC_GET_RESPONSE if cc == C.CC_GET else C.CC_SET_RESPONSE
        if sender != target.cid or n.source != target.uid or n.pid != pid or n.command_class != expected_cc \
                or n.transaction != request.transaction:
            raise RdmResponseError(f"LLRP-Antwort passt nicht zur Anfrage (PID {n.pid:#06x})")
        if n.response_type == C.RESPONSE_ACK:
            return n.data
        if n.response_type == C.RESPONSE_NACK:
            reason = int.from_bytes(n.data[:2], "big") if len(n.data) >= 2 else 0xFFFF
            raise RdmNackError(pid, reason)
        raise RdmResponseError(f"Antworttyp {n.response_type} ist über LLRP unzulässig")

    # --- Komfort: Scope und Suchdomäne
    async def read_scope(self, target: DiscoveredTarget) -> R.ComponentScope:
        return R.read_component_scope(await self.rdm_get(target, C.PID_COMPONENT_SCOPE, struct.pack(">H", 1)))

    async def read_search_domain(self, target: DiscoveredTarget) -> str:
        return R.read_search_domain(await self.rdm_get(target, C.PID_SEARCH_DOMAIN))

    async def set_scope(self, target: DiscoveredTarget, scope: str,
                           static_broker: tuple[str, int] | None = None) -> tuple[R.ComponentScope, R.ComponentScope]:
        """Setzt Scope (Slot 1) und optional eine statische IPv4-Broker-Adresse; liest zurück.

        Ohne ``statischer_broker`` sucht das Gerät den Broker per mDNS. Achtung: Ein
        verbundenes Gerät trennt sich bei Scopewechsel vom Broker.
        """
        raw = scope.encode("utf-8")
        if not 1 <= len(raw) <= C.E133_SCOPE_LENGTH - 1:
            raise ValueError(f"Scope muss 1 bis {C.E133_SCOPE_LENGTH - 1} Byte lang sein")
        old = await self.read_scope(target)
        new = R.ComponentScope(1, scope)
        if static_broker:
            new.static_type = C.E133_STATIC_CONFIG_IPV4
            new.static_ipv4, new.static_port = static_broker
        await self.rdm_set(target, C.PID_COMPONENT_SCOPE, R.build_component_scope(new))
        read_back = await self.read_scope(target)
        self._log_change(target, "COMPONENT_SCOPE", (old.scope, old.static_address),
                             (read_back.scope, read_back.static_address))
        if (read_back.scope, read_back.static_address) != (new.scope, new.static_address):
            raise RdmResponseError(f"Rücklesen weicht ab: {read_back.scope!r} / {read_back.static_address}")
        return old, read_back

    async def set_search_domain(self, target: DiscoveredTarget, domain: str) -> tuple[str, str]:
        old = await self.read_search_domain(target)
        await self.rdm_set(target, C.PID_SEARCH_DOMAIN, domain.encode("utf-8")[:C.E133_DOMAIN_LENGTH])
        read_back = await self.read_search_domain(target)
        self._log_change(target, "SEARCH_DOMAIN", old, read_back)
        if read_back != domain:
            raise RdmResponseError(f"Rücklesen weicht ab: {read_back!r}")
        return old, read_back

    def _log_change(self, target: DiscoveredTarget, parameter: str, old, new) -> None:
        entry = {"uid": str(target.uid), "cid": str(target.cid), "parameter": parameter, "old": old, "new": new}
        self.protocol.append(entry)
        log.info("LLRP SET %s", entry)


# ------------------------------------------------------------------- Target
class LlrpTarget:
    """LLRP-Target (E1.33 5.7). ``responder`` beantwortet RDM-Anfragen (z. B. BrokerResponder)."""

    def __init__(self, uid: Callable[[], Uid] | Uid, cid: uuid.UUID, type: int,
                 responder: Callable[[RdmMessage], RdmMessage | None],
                 network: LlrpNetwork | None = None, connected: Callable[[], bool] | None = None,
                 max_backoff_s: float = C.LLRP_MAX_BACKOFF_S):
        self._uid = uid if callable(uid) else (lambda: uid)
        self.cid = cid
        self.type = type
        self.responder = responder
        self.network = network or LlrpNetwork()
        self.connected = connected or (lambda: False)
        self.max_backoff_s = max_backoff_s
        self.counters = {"probe": 0, "probe_reply": 0, "rdm": 0}
        self._transport: asyncio.DatagramTransport | None = None
        self._tasks: set[asyncio.Task] = set()

    @property
    def uid(self) -> Uid:
        return self._uid()

    async def start(self) -> None:
        sock = _udp_socket(self.network, self.network.request_port, self.network.request_addr)
        self._transport = await _endpoint(sock, self._on_datagram)

    async def stop(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._transport:
            self._transport.close()
            self._transport = None

    def _reply(self, dest_cid: uuid.UUID, vector: int, tn: int, data: bytes) -> None:
        if self._transport:
            self._transport.sendto(build_llrp(self.cid, vector, dest_cid, tn, data),
                                   (self.network.response_addr, self.network.response_port))

    def _on_datagram(self, data: bytes, _addr: tuple) -> None:
        try:
            p = read_llrp(data)
        except P.ProtocolError:
            return
        if p.dest_cid not in (BROADCAST_CID, self.cid):  # E1.33 5.7.2
            return
        if p.vector == C.VECTOR_LLRP_PROBE_REQUEST:
            self._probe(p)
        elif p.vector == C.VECTOR_LLRP_RDM_CMD:
            self._rdm(p)

    def _probe(self, p: LlrpPacket) -> None:
        try:
            a = read_probe_request(p.data)
        except P.ProtocolError:
            return
        self.counters["probe"] += 1
        uid = self.uid
        if not a.lower <= uid <= a.upper or uid in a.known:
            return
        is_broker = self.type == C.LLRP_TYPE_BROKER
        if a.filter & C.LLRP_FILTER_BROKERS_ONLY and not is_broker:
            return
        if (a.filter & C.LLRP_FILTER_CLIENT_CONN_INACTIVE and not is_broker
                and self.type != C.LLRP_TYPE_NON_RDMNET and self.connected()):
            return
        response = build_probe_reply(ProbeReply(uid, hardware_address(), self.type))

        async def delayed():  # E1.33 5.7.3: zufällige Wartezeit 0 … LLRP_MAX_BACKOFF
            await asyncio.sleep(random.uniform(0, self.max_backoff_s))
            self.counters["probe_reply"] += 1
            self._reply(p.sender_cid, C.VECTOR_LLRP_PROBE_REPLY, p.transaction, response)

        t = asyncio.ensure_future(delayed())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _rdm(self, p: LlrpPacket) -> None:
        try:
            request = RdmMessage.decode(read_rdm_cmd(p.data))
        except (P.ProtocolError, RdmFormatError):
            return
        if request.dest != self.uid and not request.dest.is_broadcast:  # E1.33 5.7.4
            return
        if request.command_class not in (C.CC_GET, C.CC_SET):
            return
        self.counters["rdm"] += 1
        if request.subdevice not in (C.RDM_ROOT_DEVICE, 0xFFFF):
            response = request.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_SUB_DEVICE_OUT_OF_RANGE))
        else:
            response = self.responder(request)
        if response is None or request.dest.is_broadcast:
            return
        if response.response_type in (C.RESPONSE_ACK_TIMER, C.RESPONSE_ACK_OVERFLOW):  # E1.33 5.7.2
            response = request.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_ACTION_NOT_SUPPORTED))
        self._reply(p.sender_cid, C.VECTOR_LLRP_RDM_CMD, p.transaction, P.build_rdm_cmd_pdu(response.encode()))


# ------------------------------------------------------------------- einfacher Komponenten-Responder
class ComponentResponder:
    """RDM-Responder für LLRP-Targets von Geräten/Controllern (Simulator, Tests).

    Unterstützt DEVICE_INFO, Labels, IDENTIFY_DEVICE sowie COMPONENT_SCOPE und
    SEARCH_DOMAIN (GET/SET). ``bei_aenderung(pid)`` wird nach einem SET aufgerufen.
    """

    def __init__(self, uid: Uid, scope: str = C.E133_DEFAULT_SCOPE, domain: str = C.E133_DEFAULT_DOMAIN,
                 model: str = "pyrdmnet LLRP", product_category: int = 0x0100,
                 on_change: Callable[[int], None] | None = None):
        self.uid = uid
        self.scope = R.ComponentScope(1, scope)
        self.domain = domain
        self.model = model
        self.product_category = product_category
        self.identifying = False
        self.on_change = on_change or (lambda pid: None)

    def __call__(self, a: RdmMessage) -> RdmMessage | None:
        nack = lambda g: a.response(C.RESPONSE_NACK, struct.pack(">H", g))  # noqa: E731
        pid, d = a.pid, a.data
        if a.command_class == C.CC_GET:
            if pid == C.PID_DEVICE_INFO:
                return a.response(C.RESPONSE_ACK, R.build_device_info(R.DeviceInfo(
                    0x0100, 1, self.product_category, 1, 0, 0, 0, 0xFFFF, 0, 0)))
            if pid in (C.PID_DEVICE_MODEL_DESCRIPTION, C.PID_DEVICE_LABEL):
                return a.response(C.RESPONSE_ACK, R.build_text(self.model))
            if pid == C.PID_MANUFACTURER_LABEL:
                return a.response(C.RESPONSE_ACK, R.build_text("pyrdmnet Simulator"))
            if pid == C.PID_IDENTIFY_DEVICE:
                return a.response(C.RESPONSE_ACK, bytes((int(self.identifying),)))
            if pid == C.PID_COMPONENT_SCOPE:
                if len(d) != 2:
                    return nack(C.NR_FORMAT_ERROR)
                if d != b"\x00\x01":
                    return nack(C.NR_DATA_OUT_OF_RANGE)
                return a.response(C.RESPONSE_ACK, R.build_component_scope(self.scope))
            if pid == C.PID_SEARCH_DOMAIN:
                return a.response(C.RESPONSE_ACK, self.domain.encode("utf-8"))
            return nack(C.NR_UNKNOWN_PID)
        if pid == C.PID_IDENTIFY_DEVICE:
            if len(d) != 1 or d[0] > 1:
                return nack(C.NR_FORMAT_ERROR)
            self.identifying = bool(d[0])
        elif pid == C.PID_COMPONENT_SCOPE:
            try:
                new = R.read_component_scope(d)
            except RdmFormatError:
                return nack(C.NR_FORMAT_ERROR)
            if new.slot != 1:
                return nack(C.NR_DATA_OUT_OF_RANGE)
            if not new.scope:
                return nack(C.NR_DATA_OUT_OF_RANGE)  # Geräte haben genau einen Scope
            self.scope = new
        elif pid == C.PID_SEARCH_DOMAIN:
            if len(d) > C.E133_DOMAIN_LENGTH:
                return nack(C.NR_DATA_OUT_OF_RANGE)
            self.domain = d.decode("utf-8", errors="replace")
        else:
            return nack(C.NR_UNKNOWN_PID)
        self.on_change(pid)
        return a.response(C.RESPONSE_ACK)
