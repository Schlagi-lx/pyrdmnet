"""RPT client base (connection setup, receiving, heartbeat) for controllers and devices."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

from . import constants as C
from . import pdu as P
from .rdm import Uid
from .connection import TcpConnection

log = logging.getLogger("pyrdmnet.klient")


class RdmnetConnectionError(ConnectionError):
    def __init__(self, text: str, code: int | None = None):
        super().__init__(text)
        self.code = code


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

    @property
    def connected(self) -> bool:
        return self._v is not None and not self._v.closed

    async def connect(self, host: str, port: int, timeout_s: float = 5.0) -> Uid:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout_s)
        except (OSError, asyncio.TimeoutError) as error:
            raise RdmnetConnectionError(f"Broker {host}:{port} not reachable: {error}") from error
        v = TcpConnection(reader, writer, self.cid)
        entry = P.ClientEntry(self.cid, self.requested_uid, self.type, self.binding_cid)
        flags = C.BROKER_CONNECT_FLAG_INCREMENTAL if self.incremental else 0
        cd = P.ConnectData(self.scope, C.E133_VERSION, C.E133_DEFAULT_DOMAIN, flags, entry)
        try:
            await v.send_broker(C.VECTOR_BROKER_CONNECT, cd.encode())
            roots = await asyncio.wait_for(v.read(), timeout_s)
            bp = P.read_broker(roots[0].data)
            if roots[0].vector != C.VECTOR_ROOT_E133_BROKER or bp.vector != C.VECTOR_BROKER_CONNECT_REPLY:
                raise RdmnetConnectionError("Broker did not answer with Connect Reply")
            rep = P.read_connect_reply(bp.data)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, P.ProtocolError, ConnectionError) as error:
            await v.close()
            if isinstance(error, RdmnetConnectionError):
                raise
            raise RdmnetConnectionError(f"Connect failed: {error!r}") from error
        if rep.code != C.E133_CONNECT_OK:
            await v.close()
            raise RdmnetConnectionError(f"Broker rejected: {C.CONNECT_CODE_TEXT.get(rep.code, rep.code)}", rep.code)
        self._v, self.uid, self.broker_uid = v, rep.client_uid, rep.broker_uid
        self.disconnected.clear()
        self._tasks = [asyncio.create_task(self._receive_loop()),
                       asyncio.create_task(self._heartbeat_loop())]
        await self._after_connect()
        return rep.client_uid

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
        if self._v:
            await self._v.close()
        f = self._update_reply
        if f is not None and not f.done():
            f.set_exception(RdmnetConnectionError(f"Connection closed: {reason}"))
        if not self.disconnected.is_set():
            self.disconnect_reason = reason
            self.disconnected.set()
            await self._after_disconnect(reason)

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
                            if bp.vector == C.VECTOR_BROKER_CONNECT_REPLY:
                                f = self._update_reply
                                if f is not None and not f.done():
                                    f.set_result(P.read_connect_reply(bp.data))
                                continue
                            await self._on_broker(bp)
                        elif root.vector == C.VECTOR_ROOT_E133_RPT:
                            await self._on_rpt(P.read_rpt(root.data))
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
            if now - self._v.last_received > self.heartbeat_timeout_s:
                await self._shutdown("Heartbeat timeout (broker not responding)")
                return
            if now - self._v.last_sent >= self.heartbeat_interval_s:
                try:
                    await self._v.send_broker(C.VECTOR_BROKER_NULL)
                except (ConnectionError, OSError):
                    await self._shutdown("Send failed")
                    return

    # --- Extension points
    async def _after_connect(self) -> None:
        pass

    async def _after_disconnect(self, reason: str) -> None:
        pass

    async def _on_broker(self, bp: P.BrokerPdu) -> None:
        pass

    async def _on_rpt(self, rpt: P.RptPdu) -> None:
        pass
