"""Low Level Recovery Protocol (LLRP, ANSI E1.33 section 5).

LLRP runs over UDP multicast and needs no broker. It can be used to find
RDMnet components (e.g. a gateway with the wrong scope) and to read and set
their scope, search domain or a static broker address.

* :class:`LlrpManager` - discovery per E1.33 5.6.3 and RDM GET/SET to targets.
  SET only for ``ALLOWED_SET_PIDS`` and only with write permission; every change
  is read back.
* :class:`LlrpTarget` - answers probe requests and RDM commands; used by the
  broker (``BrokerConfig.llrp``) and in tests.

Limitations: IPv4 only (E1.33 additionally requires IPv6 for managers).
Network settings per E1.37-2 (IP address, DHCP ...) are deliberately not
set. The target reports ``uuid.getnode()`` as its hardware address.
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


# ------------------------------------------------------------------- Encoding
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
        raise P.ProtocolError("No ACN UDP preamble")
    roots = P.read_root_pdus(datagram[16:])
    if len(roots) != 1 or roots[0].vector != C.VECTOR_ROOT_LLRP:
        raise P.ProtocolError("No LLRP root PDU")
    parts = P.split_pdus(roots[0].data)
    if len(parts) != 1:
        raise P.ProtocolError("Expected exactly one LLRP PDU")
    pdu, header = parts[0]
    if len(pdu) < header + 24:
        raise P.ProtocolError("LLRP PDU too short")
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
        raise P.ProtocolError(f"More than {C.LLRP_KNOWN_UID_SIZE} known UIDs")
    payload = a.lower.to_bytes() + a.upper.to_bytes() + struct.pack(">H", a.filter) \
        + b"".join(u.to_bytes() for u in a.known)
    return P.flags_length(4 + len(payload)) + bytes((C.VECTOR_PROBE_REQUEST_DATA,)) + payload


def read_probe_request(data: bytes) -> ProbeRequest:
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Expected exactly one probe request PDU")
    pdu, header = parts[0]
    payload = pdu[header + 1:]
    if pdu[header] != C.VECTOR_PROBE_REQUEST_DATA or len(payload) < 14 or (len(payload) - 14) % 6:
        raise P.ProtocolError("Invalid probe request")
    known = [Uid.from_bytes(bytes(payload[i:i + 6])) for i in range(14, len(payload), 6)]
    return ProbeRequest(Uid.from_bytes(bytes(payload[:6])), Uid.from_bytes(bytes(payload[6:12])),
                        struct.unpack(">H", payload[12:14])[0], known)


@dataclass
class ProbeReply:
    uid: Uid
    hardware: bytes  # 6 bytes, usually the MAC address
    type: int


def build_probe_reply(a: ProbeReply) -> bytes:
    payload = a.uid.to_bytes() + a.hardware[:6].rjust(6, b"\x00") + bytes((a.type,))
    return P.flags_length(4 + len(payload)) + bytes((C.VECTOR_PROBE_REPLY_DATA,)) + payload


def read_probe_reply(data: bytes) -> ProbeReply:
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Expected exactly one probe reply PDU")
    pdu, header = parts[0]
    payload = pdu[header + 1:]
    if pdu[header] != C.VECTOR_PROBE_REPLY_DATA or len(payload) != 13:
        raise P.ProtocolError("Invalid probe reply")
    return ProbeReply(Uid.from_bytes(bytes(payload[:6])), bytes(payload[6:12]), payload[12])


def read_rdm_cmd(data: bytes) -> bytes:
    """RDM Command PDU (5.4.2.3) -> raw RDM message including start code."""
    parts = P.split_pdus(data)
    if len(parts) != 1:
        raise P.ProtocolError("Expected exactly one RDM command PDU")
    pdu, header = parts[0]
    if len(pdu) <= header or pdu[header] != C.VECTOR_RDM_CMD_RDM_DATA:
        raise P.ProtocolError("RDM command PDU with wrong vector")
    return bytes(pdu[header:])


def hardware_address() -> bytes:
    return uuid.getnode().to_bytes(6, "big")


# ------------------------------------------------------------------- Transport
@dataclass
class LlrpNetwork:
    """Addresses for LLRP. Default: multicast per E1.33; tests use unicast on 127.0.0.1."""

    adapter: str = "0.0.0.0"  # IP of the network adapter (0.0.0.0 = system default)
    request_addr: str = C.LLRP_IPV4_REQUEST
    response_addr: str = C.LLRP_IPV4_RESPONSE
    request_port: int = C.LLRP_PORT
    response_port: int = C.LLRP_PORT
    multicast: bool = True
    ttl: int = 1  # >1: replies may come from other network segments (E1.33 5.3.3)


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
                try:  # Linux: only receive groups this socket has joined
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
        except Exception:  # a malformed packet must not stop reception
            log.exception("Failed to process LLRP packet")


async def _endpoint(sock: socket.socket, on_receive) -> asyncio.DatagramTransport:
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(lambda: _Protocol(on_receive), sock=sock)
    return transport


# ------------------------------------------------------------------- Manager
@dataclass(frozen=True)
class DiscoveredTarget:
    """A target found via LLRP. The UID alone is not unique (dynamic UIDs), hence the CID."""

    cid: uuid.UUID
    uid: Uid
    hardware: bytes
    type: int
    address: str  # IP the reply came from

    @property
    def type_text(self) -> str:
        return C.LLRP_TYPE_TEXT.get(self.type, f"Type {self.type:#04x}")

    @property
    def hardware_text(self) -> str:
        return ":".join(f"{b:02x}" for b in self.hardware)


class LlrpManager:
    """LLRP manager (E1.33 5.6). Usage::

        async with LlrpManager(LlrpNetwork(adapter="192.168.1.10")) as m:
            targets = await m.discover()
            print(await m.read_scope(targets[0]))
    """

    def __init__(self, network: LlrpNetwork | None = None, uid: Uid | None = None, cid: uuid.UUID | None = None,
                 timeout_window_s: float = C.LLRP_TIMEOUT_S, retries: int = 1):
        self.network = network or LlrpNetwork()
        # E1.33 5.6.1: the manager UID may be dynamic; it is only used in RDM commands
        self.uid = uid or Uid(0x8000 | C.ESTA_PROTOTYPE_MANUFACTURER, 0x4C4C5250)  # "LLRP"
        self.cid = cid or uuid.uuid4()
        self.timeout_window_s = timeout_window_s
        self.retries = retries
        self.write_enabled = False
        self.protocol: list[dict] = []  # changes old -> new
        self._tn = 0
        self._rdm_tn = TransactionCounter()
        self._transport: asyncio.DatagramTransport | None = None
        self._probe: tuple[int, dict] | None = None  # (transaction, {(cid, uid): DiscoveredTarget})
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

    # --- Send/receive
    def _next_tn(self) -> int:
        self._tn = (self._tn + 1) & 0xFFFFFFFF
        return self._tn

    def _send(self, vector: int, dest_cid: uuid.UUID, data: bytes) -> int:
        if self._transport is None:
            raise RuntimeError("LLRP manager not started")
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
                    f.set_exception(RdmResponseError(f"Invalid LLRP response: {error}"))

    # --- Discovery (E1.33 5.6.3)
    async def discover(self, filter: int = C.LLRP_FILTER_CLIENT_CONN_INACTIVE) -> list[DiscoveredTarget]:
        """Searches for targets. Default filter per the standard: only clients without a broker
        connection (brokers always reply). ``filter=0`` also finds connected clients."""
        found: dict[tuple[uuid.UUID, Uid], DiscoveredTarget] = {}
        await self._probe_range(UID_MIN, UID_MAX, filter, found)
        return sorted(found.values(), key=lambda z: (z.uid, str(z.cid)))

    async def _probe_range(self, lo: int, hi: int, filter: int, found: dict) -> None:
        silent_rounds = 0
        while True:
            known = sorted({z.uid for z in found.values() if lo <= z.uid.to_int() <= hi})
            if len(known) > C.LLRP_KNOWN_UID_SIZE:  # step 1: split the range
                middle = (lo + hi) // 2
                await self._probe_range(lo, middle, filter, found)
                await self._probe_range(middle + 1, hi, filter, found)
                return
            new: dict = {}
            request = ProbeRequest(Uid.from_int(lo), Uid.from_int(hi), filter, known)
            self._probe = (self._send(C.VECTOR_LLRP_PROBE_REQUEST, BROADCAST_CID, build_probe_request(request)), new)
            await asyncio.sleep(self.timeout_window_s)  # step 3: wait for LLRP_TIMEOUT
            self._probe = None
            fresh = {k: v for k, v in new.items() if k not in found}
            found.update(new)
            if fresh:
                silent_rounds = 0  # step 4
                continue
            silent_rounds += 1  # step 5: two more attempts without reply
            if silent_rounds >= 3:
                return

    # --- RDM over LLRP
    async def rdm_get(self, target: DiscoveredTarget, pid: int, data: bytes = b"") -> bytes:
        return await self._rdm(target, C.CC_GET, pid, data)

    async def rdm_set(self, target: DiscoveredTarget, pid: int, data: bytes) -> bytes:
        if pid not in ALLOWED_SET_PIDS:
            raise WriteLockError(f"SET of {C.PID_NAMES.get(pid, hex(pid))} via LLRP is not allowed")
        if not self.write_enabled:
            raise WriteLockError("No write permission for this session")
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
                    raise RdmTimeoutError(f"No LLRP response to {C.PID_NAMES.get(pid, hex(pid))} "
                                        f"from {target.uid}") from None
            finally:
                self._pending.pop(tn, None)
        try:
            n = RdmMessage.decode(response_raw)
        except RdmFormatError as error:
            raise RdmResponseError(f"Invalid RDM response: {error}") from error
        expected_cc = C.CC_GET_RESPONSE if cc == C.CC_GET else C.CC_SET_RESPONSE
        if sender != target.cid or n.source != target.uid or n.pid != pid or n.command_class != expected_cc \
                or n.transaction != request.transaction:
            raise RdmResponseError(f"LLRP response does not match the request (PID {n.pid:#06x})")
        if n.response_type == C.RESPONSE_ACK:
            return n.data
        if n.response_type == C.RESPONSE_NACK:
            reason = int.from_bytes(n.data[:2], "big") if len(n.data) >= 2 else 0xFFFF
            raise RdmNackError(pid, reason)
        raise RdmResponseError(f"Response type {n.response_type} is not allowed via LLRP")

    # --- Convenience: scope and search domain
    async def read_scope(self, target: DiscoveredTarget) -> R.ComponentScope:
        return R.read_component_scope(await self.rdm_get(target, C.PID_COMPONENT_SCOPE, struct.pack(">H", 1)))

    async def read_search_domain(self, target: DiscoveredTarget) -> str:
        return R.read_search_domain(await self.rdm_get(target, C.PID_SEARCH_DOMAIN))

    async def set_scope(self, target: DiscoveredTarget, scope: str,
                           static_broker: tuple[str, int] | None = None) -> tuple[R.ComponentScope, R.ComponentScope]:
        """Sets the scope (slot 1) and optionally a static IPv4 broker address; reads back.

        Without ``static_broker`` the device locates the broker via mDNS. Note: a
        connected device disconnects from the broker when its scope changes.
        """
        raw = scope.encode("utf-8")
        if not 1 <= len(raw) <= C.E133_SCOPE_LENGTH - 1:
            raise ValueError(f"Scope must be 1 to {C.E133_SCOPE_LENGTH - 1} bytes long")
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
            raise RdmResponseError(f"Read-back mismatch: {read_back.scope!r} / {read_back.static_address}")
        return old, read_back

    async def set_search_domain(self, target: DiscoveredTarget, domain: str) -> tuple[str, str]:
        old = await self.read_search_domain(target)
        await self.rdm_set(target, C.PID_SEARCH_DOMAIN, domain.encode("utf-8")[:C.E133_DOMAIN_LENGTH])
        read_back = await self.read_search_domain(target)
        self._log_change(target, "SEARCH_DOMAIN", old, read_back)
        if read_back != domain:
            raise RdmResponseError(f"Read-back mismatch: {read_back!r}")
        return old, read_back

    def _log_change(self, target: DiscoveredTarget, parameter: str, old, new) -> None:
        entry = {"uid": str(target.uid), "cid": str(target.cid), "parameter": parameter, "old": old, "new": new}
        self.protocol.append(entry)
        log.info("LLRP SET %s", entry)


# ------------------------------------------------------------------- Target
class LlrpTarget:
    """LLRP target (E1.33 5.7). ``responder`` answers RDM requests (e.g. BrokerResponder)."""

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

        async def delayed():  # E1.33 5.7.3: random backoff 0 ... LLRP_MAX_BACKOFF
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


# ------------------------------------------------------------------- simple component responder
class ComponentResponder:
    """RDM responder for LLRP targets of devices/controllers (simulator, tests).

    Supports DEVICE_INFO, labels, IDENTIFY_DEVICE as well as COMPONENT_SCOPE and
    SEARCH_DOMAIN (GET/SET). ``on_change(pid)`` is called after a SET.
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
                return nack(C.NR_DATA_OUT_OF_RANGE)  # devices have exactly one scope
            self.scope = new
        elif pid == C.PID_SEARCH_DOMAIN:
            if len(d) > C.E133_DOMAIN_LENGTH:
                return nack(C.NR_DATA_OUT_OF_RANGE)
            self.domain = d.decode("utf-8", errors="replace")
        else:
            return nack(C.NR_UNKNOWN_PID)
        self.on_change(pid)
        return a.response(C.RESPONSE_ACK)
