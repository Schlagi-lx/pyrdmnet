"""Client base (connection setup, receiving, heartbeat, reconnection) for controllers, devices and EPT clients.

Automatic reconnection (``connect(..., auto_reconnect=True)``) follows E1.33 6.2.5: after a
lost connection the client reconnects; the first two attempts are immediate, then a back-off
applies that grows by a random 1-5 s whenever the TCP connection was established but the
connect failed (capped at 30 s). Failed TCP connects do not grow the back-off; they are
retried every ``reconnect_min_s`` (an implementation choice to avoid a busy loop).
Client Redirect IPv4 (6.2.5.3.2) is followed during connect and while connected.

RPT clients count unhealthy TCP connections (heartbeat timeout, 6.2.4.1.3) per scope for
TCP_COMMS_STATUS (7.6.3) and keep the scope configuration that their default responder
reports and changes (COMPONENT_SCOPE, SEARCH_DOMAIN, see :mod:`pyrdmnet.client_responder`).
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import random
import time
import uuid
from typing import Callable

from . import constants as C
from . import pdu as P
from . import rdm_params as R
from .connection import TcpConnection
from .rdm import RdmMessage, Uid

log = logging.getLogger("pyrdmnet.client")


class RdmnetConnectionError(ConnectionError):
    def __init__(self, text: str, code: int | None = None, established: bool | None = None):
        super().__init__(text)
        self.code = code
        self.established = established  # True: TCP connection was established, the connect itself failed


class _Redirect(Exception):
    def __init__(self, host: str, port: int):
        super().__init__(f"redirect to {host}:{port}")
        self.host, self.port = host, port


MAX_REDIRECTS = 3


class RptClient:
    """Shared logic for RPT controllers and RPT devices."""

    def __init__(self, type: int, uid: Uid, scope: str = C.E133_DEFAULT_SCOPE, cid: uuid.UUID | None = None,
                 incremental: bool = False, heartbeat_interval_s: float = C.E133_HEARTBEAT_INTERVAL_S,
                 heartbeat_timeout_s: float = C.E133_HEARTBEAT_TIMEOUT_S):
        self.type = type
        self.requested_uid = uid
        self.scope = scope
        self.cid = cid or uuid.uuid4()
        self.incremental = incremental
        self.binding_cid = uuid.UUID(int=0)  # devices only: CID of a component on the same hardware
        self.heartbeat_interval_s = heartbeat_interval_s
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.uid: Uid | None = None
        self.broker_uid: Uid | None = None
        self._v: TcpConnection | None = None
        self._tasks: list[asyncio.Task] = []
        self.disconnected = asyncio.Event()
        self.disconnect_reason: str = ""
        self._update_reply: asyncio.Future | None = None
        # reconnection (E1.33 6.2.5)
        self.reconnect_min_s = 1.0  # retry interval while the TCP connect itself fails
        self.backoff_step_s = (1.0, 5.0)  # random increment after an established but failed connect
        self.backoff_max_s = 30.0
        self.reconnect_count = 0
        self.reconnecting = False
        self.last_connect_error = ""
        self.on_connection_change: Callable[[bool, str], None] | None = None  # (connected, text)
        self.target: tuple[str, int] | None = None  # current broker address (may change by redirect)
        self._connect_timeout_s = 5.0
        self._closing = False
        # scope configuration and statistics (E1.33 7.6.1-7.6.3)
        self.search_domain = C.E133_DEFAULT_DOMAIN
        self.static_broker: tuple[str, int] | None = None  # static broker address of the scope, if configured
        self.unhealthy_events: dict[str, int] = {}  # scope -> unhealthy TCP connections (16 bit, saturating)
        self._background: set[asyncio.Task] = set()  # tasks that outlive one connection (scope change)
        self._supervisor: asyncio.Task | None = None
        self._is_connected = asyncio.Event()
        self._sleep = asyncio.sleep  # replaceable in tests (back-off timing)

    @property
    def connected(self) -> bool:
        return self._v is not None and not self._v.closed

    async def connect(self, host: str, port: int, timeout_s: float = 5.0, auto_reconnect: bool = False) -> Uid:
        """Connect to the broker. Raises RdmnetConnectionError if the first connect fails.

        With ``auto_reconnect`` the client reconnects by itself after a lost connection
        (E1.33 6.2.5) until :meth:`disconnect` is called.
        """
        if self._supervisor is not None and not self._supervisor.done():
            raise RdmnetConnectionError("Automatic reconnection is already running")
        self._closing = False
        self.target, self._connect_timeout_s = (host, port), timeout_s
        uid = await self._connect_following_redirects()
        if auto_reconnect:
            self._supervisor = asyncio.create_task(self._supervise())
        return uid

    @property
    def auto_reconnecting(self) -> bool:
        """True while automatic reconnection is active (between connect(auto_reconnect=True) and disconnect())."""
        return self._supervisor is not None and not self._supervisor.done()

    async def wait_connected(self, timeout_s: float) -> None:
        """Wait until the (automatically re-)established connection is up."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        try:
            while not self.connected:
                await asyncio.wait_for(self._is_connected.wait(), max(deadline - loop.time(), 0))
                if not self.connected:  # event of a connection that is already closing again
                    self._is_connected.clear()
        except asyncio.TimeoutError as error:
            raise RdmnetConnectionError(f"Not connected to broker within {timeout_s:g} s"
                                        + (f" (last error: {self.last_connect_error})" if self.last_connect_error
                                           else "")) from error

    async def _connect_following_redirects(self) -> Uid:
        for _ in range(MAX_REDIRECTS + 1):
            try:
                return await self._connect_once(*self.target, self._connect_timeout_s)
            except _Redirect as r:
                log.info("Broker redirects %s:%s to %s:%s", *self.target, r.host, r.port)
                self.target = (r.host, r.port)
        raise RdmnetConnectionError(f"More than {MAX_REDIRECTS} redirects", established=True)

    async def _supervise(self) -> None:
        """Reconnect after a lost connection with the back-off of E1.33 6.2.5.3.1."""
        while not self._closing:
            await self.disconnected.wait()
            if self._closing:
                return
            self.reconnecting = True
            backoff, attempt = 0.0, 0
            try:
                while not self._closing:
                    attempt += 1
                    if attempt > 2:  # the first two attempts are immediate
                        await self._sleep(max(backoff, self.reconnect_min_s))
                    target = self.target
                    try:
                        await self._connect_following_redirects()
                    except RdmnetConnectionError as error:
                        self.last_connect_error = str(error)
                        if self.target != target:
                            backoff = 0.0  # address changed (redirect): reset the back-off
                        elif error.established:
                            backoff = min(backoff + random.uniform(*self.backoff_step_s), self.backoff_max_s)
                        continue
                    self.reconnect_count += 1
                    log.info("Reconnected to broker %s:%s (attempt %d)", *self.target, attempt)
                    break
            finally:
                self.reconnecting = False

    async def _connect_once(self, host: str, port: int, timeout_s: float) -> Uid:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout_s)
        except (OSError, asyncio.TimeoutError) as error:
            raise RdmnetConnectionError(f"Broker {host}:{port} not reachable: {error}", established=False) from error
        v = TcpConnection(reader, writer, self.cid)
        entry = self._connect_entry()
        flags = C.BROKER_CONNECT_FLAG_INCREMENTAL if self.incremental else 0
        cd = P.ConnectData(self.scope, C.E133_VERSION, self.search_domain, flags, entry)
        try:
            await v.send_broker(C.VECTOR_BROKER_CONNECT, cd.encode())
            roots = await asyncio.wait_for(v.read(), timeout_s)
            bp = P.read_broker(roots[0].data)
            if roots[0].vector == C.VECTOR_ROOT_E133_BROKER and bp.vector == C.VECTOR_BROKER_REDIRECT_V4:
                raise _Redirect(*P.read_redirect_v4(bp.data))
            if roots[0].vector != C.VECTOR_ROOT_E133_BROKER or bp.vector != C.VECTOR_BROKER_CONNECT_REPLY:
                raise RdmnetConnectionError("Broker did not answer with Connect Reply", established=True)
            rep = P.read_connect_reply(bp.data)
        except _Redirect:
            await v.close()
            raise
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, P.ProtocolError, ConnectionError) as error:
            await v.close()
            if isinstance(error, RdmnetConnectionError):
                raise
            raise RdmnetConnectionError(f"Connect failed: {error!r}", established=True) from error
        if rep.code != C.E133_CONNECT_OK:
            await v.close()
            raise RdmnetConnectionError(f"Broker rejected: {C.CONNECT_CODE_TEXT.get(rep.code, rep.code)}", rep.code,
                                        established=True)
        self._v, self.uid, self.broker_uid = v, rep.client_uid, rep.broker_uid
        self.disconnected.clear()
        self.last_connect_error = ""
        for t in self._tasks:  # leftovers of a previous connection (e.g. a heartbeat still sleeping)
            if not t.done() and t is not asyncio.current_task():
                t.cancel()
        self._tasks = [asyncio.create_task(self._receive_loop()),
                       asyncio.create_task(self._heartbeat_loop())]
        self._is_connected.set()
        await self._after_connect()
        self._notify(True, f"connected to {host}:{port}")
        return rep.client_uid

    def _notify(self, connected: bool, text: str) -> None:
        if self.on_connection_change:
            try:
                self.on_connection_change(connected, text)
            except Exception:  # a listener must not break the connection handling
                log.exception("on_connection_change failed")

    def _connect_entry(self) -> P.ClientEntry:
        """Client entry sent with Client Connect (overridden by EPT clients)."""
        return P.ClientEntry(self.cid, self.requested_uid, self.type, self.binding_cid)

    async def update_entry(self, uid: Uid | None = None, type: int | None = None,
                                    binding_cid: uuid.UUID | None = None, incremental: bool | None = None,
                                    timeout_s: float = 5.0) -> Uid:
        """Change the own client entry without reconnecting (E1.33 Client Entry Update).

        Values not given stay the same. With a dynamic UID, a dynamic UID is requested
        again; the broker assigns the same one (assignment per CID).
        If the broker rejects, it closes the connection (RdmnetConnectionError with code).
        """
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        if self._update_reply is not None and not self._update_reply.done():
            raise RdmnetConnectionError("An entry update is already in progress")
        if uid is None:
            uid = self.requested_uid if self.requested_uid.is_dynamic_request else self.uid
        type = self.type if type is None else type
        binding = self.binding_cid if binding_cid is None else binding_cid
        incr = self.incremental if incremental is None else incremental
        flags = C.BROKER_CONNECT_FLAG_INCREMENTAL if incr else 0
        self._update_reply = asyncio.get_running_loop().create_future()
        await self.send_broker(C.VECTOR_BROKER_CLIENT_ENTRY_UPDATE,
                                P.build_client_entry_update(flags, P.ClientEntry(self.cid, uid, type, binding)))
        try:
            rep = await asyncio.wait_for(asyncio.shield(self._update_reply), timeout_s)
        except asyncio.TimeoutError as error:
            raise RdmnetConnectionError("No response from broker to Client Entry Update") from error
        if rep.code != C.E133_CONNECT_OK:
            raise RdmnetConnectionError(f"Broker rejected entry update: "
                                    f"{C.CONNECT_CODE_TEXT.get(rep.code, rep.code)}", rep.code)
        self.requested_uid = uid
        self.uid, self.type, self.binding_cid, self.incremental = rep.client_uid, type, binding, incr
        return rep.client_uid

    async def disconnect(self) -> None:
        self._closing = True
        if self._supervisor is not None:
            self._supervisor.cancel()
            await asyncio.gather(self._supervisor, return_exceptions=True)
            self._supervisor = None
        if self._v and not self._v.closed:
            try:
                await self._v.send_broker(C.VECTOR_BROKER_DISCONNECT,
                                           C.E133_DISCONNECT_SHUTDOWN.to_bytes(2, "big"))
            except (ConnectionError, OSError):
                pass
        await self._shutdown("disconnected by program")
        for t in self._tasks:
            if t is not asyncio.current_task():
                t.cancel()
        await asyncio.gather(*[t for t in self._tasks if t is not asyncio.current_task()],
                             return_exceptions=True)

    async def _shutdown(self, reason: str) -> None:
        self._is_connected.clear()  # before awaiting the close: wait_connected() must not see a stale "connected"
        if self._v:
            await self._v.close()
        f = self._update_reply
        if f is not None and not f.done():
            f.set_exception(RdmnetConnectionError(f"Connection closed: {reason}"))
        if not self.disconnected.is_set():
            self.disconnect_reason = reason
            self.disconnected.set()
            await self._after_disconnect(reason)
            self._notify(False, reason)

    async def send_broker(self, vector: int, data: bytes = b"") -> None:
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        await self._v.send_broker(vector, data)

    async def send_rpt(self, header: P.RptHeader, data: bytes) -> None:
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        await self._v.send_rpt(header, data)

    async def _receive_loop(self) -> None:
        reason = "Broker closed the connection"
        try:
            while True:
                for root in await self._v.read():
                    try:
                        if root.vector == C.VECTOR_ROOT_E133_BROKER:
                            bp = P.read_broker(root.data)
                            if bp.vector == C.VECTOR_BROKER_DISCONNECT:
                                reason = "Broker sent Disconnect"
                                return
                            if bp.vector == C.VECTOR_BROKER_REDIRECT_V4:  # 6.2.5.3.2, also while connected
                                self.target = P.read_redirect_v4(bp.data)
                                reason = f"Broker redirect to {self.target[0]}:{self.target[1]}"
                                return
                            if bp.vector == C.VECTOR_BROKER_CONNECT_REPLY:
                                f = self._update_reply
                                if f is not None and not f.done():
                                    f.set_result(P.read_connect_reply(bp.data))
                                continue
                            await self._on_broker(bp)
                        elif root.vector == C.VECTOR_ROOT_E133_RPT:
                            await self._on_rpt(P.read_rpt(root.data))
                        elif root.vector == C.VECTOR_ROOT_E133_EPT:
                            await self._on_ept(root)
                    except P.ProtocolError as error:
                        log.warning("Invalid message discarded: %s", error)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except P.ProtocolError as error:
            reason = f"Protocol error: {error}"
        except asyncio.CancelledError:
            reason = "terminated"
            raise
        finally:
            await self._shutdown(reason)

    async def _heartbeat_loop(self) -> None:
        tick = min(1.0, self.heartbeat_interval_s / 3)
        while self.connected:
            await asyncio.sleep(tick)
            now = time.monotonic()
            if now - self._v.last_received > self.heartbeat_timeout_s:  # 6.2.4.1.3: connection unhealthy
                self.unhealthy_events[self.scope] = min(self.unhealthy_events.get(self.scope, 0) + 1, 0xFFFF)
                await self._shutdown("Heartbeat timeout (broker not responding)")
                return
            if now - self._v.last_sent >= self.heartbeat_interval_s:
                try:
                    await self._v.send_broker(C.VECTOR_BROKER_NULL)
                except (ConnectionError, OSError):
                    await self._shutdown("Send failed")
                    return

    # --- Scope configuration and TCP_COMMS_STATUS (E1.33 7.6)
    def component_scope(self) -> R.ComponentScope:
        cs = R.ComponentScope(1, self.scope)
        if self.static_broker:
            host, cs.static_port = self.static_broker
            address = ipaddress.ip_address(host)
            if address.version == 6:
                cs.static_type, cs.static_ipv6 = C.E133_STATIC_CONFIG_IPV6, address.packed
            else:
                cs.static_type, cs.static_ipv4 = C.E133_STATIC_CONFIG_IPV4, str(address)
        return cs

    def set_component_scope(self, cs: R.ComponentScope) -> None:
        """New scope / static broker (SET COMPONENT_SCOPE). If it changes, the client leaves its broker;
        with automatic reconnection it connects again in the new scope (to the static broker, if given)."""
        static = None
        if cs.static_type == C.E133_STATIC_CONFIG_IPV4:
            static = (cs.static_ipv4, cs.static_port)
        elif cs.static_type == C.E133_STATIC_CONFIG_IPV6:
            static = (str(ipaddress.IPv6Address(cs.static_ipv6)), cs.static_port)
        changed = (cs.scope, static) != (self.scope, self.static_broker)
        self.scope, self.static_broker = cs.scope, static
        if static:
            self.target = static
        if changed and self.connected:
            log.info("Scope changed to %r (static broker %s): reconnecting", cs.scope, static)
            # after the response to the SET has been sent
            t = asyncio.get_running_loop().create_task(self._leave_broker(f"Scope changed to {cs.scope!r}"))
            self._background.add(t)
            t.add_done_callback(self._background.discard)

    async def _leave_broker(self, reason: str) -> None:
        await asyncio.sleep(0)
        if self._v and not self._v.closed:
            try:
                await self._v.send_broker(C.VECTOR_BROKER_DISCONNECT,  # 7.6: reconfigured via RPT
                                          C.E133_DISCONNECT_RPT_RECONFIGURE.to_bytes(2, "big"))
            except (ConnectionError, OSError):
                pass
        await self._shutdown(reason)

    def tcp_comms_entries(self) -> list[R.TcpCommsEntry]:
        """TCP_COMMS_STATUS: one entry per scope (an RPT client of pyrdmnet has one)."""
        e = R.TcpCommsEntry(self.scope, unhealthy_events=self.unhealthy_events.get(self.scope, 0))
        if self.connected and self._v.peer_address:
            host, e.broker_port = self._v.peer_address[0], self._v.peer_address[1]
            address = ipaddress.ip_address(host.split("%", 1)[0])
            if address.version == 6 and address.ipv4_mapped:
                address = address.ipv4_mapped
            if address.version == 6:
                e.broker_ipv6 = address.packed
            else:
                e.broker_ipv4 = str(address)
        return [e]

    def reset_unhealthy_events(self, scope: str) -> bool:
        """SET TCP_COMMS_STATUS; False if the client is not in this scope."""
        if scope != self.scope:
            return False
        self.unhealthy_events[scope] = 0
        return True

    # --- Answering RPT requests (devices and controllers)
    async def _send_rpt_status(self, k: P.RptHeader, code: int) -> None:
        header = P.RptHeader(C.VECTOR_RPT_STATUS, self.uid, k.dest_endpoint, k.source, C.E133_NULL_ENDPOINT,
                             k.sequence)
        await self.send_rpt(header, P.build_status(code))

    async def _send_rdm_response(self, k: P.RptHeader, raw_request: bytes, response: RdmMessage) -> None:
        header = P.RptHeader(C.VECTOR_RPT_NOTIFICATION, self.uid, k.dest_endpoint, k.source, C.E133_NULL_ENDPOINT,
                             k.sequence)
        await self.send_rpt(header, P.build_notification([raw_request, response.encode()]))

    # --- Extension points
    async def _after_connect(self) -> None:
        pass

    async def _after_disconnect(self, reason: str) -> None:
        pass

    async def _on_broker(self, bp: P.BrokerPdu) -> None:
        pass

    async def _on_rpt(self, rpt: P.RptPdu) -> None:
        pass

    async def _on_ept(self, root: P.RootPdu) -> None:
        pass
