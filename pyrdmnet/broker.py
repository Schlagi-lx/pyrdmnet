"""Standalone RDMnet broker according to ANSI E1.33 (RPT clients, TCP, asyncio).

Scope of V0.1:
* Client Connect with checks of scope, E1.33 version, client entry, UID
* Static and dynamic UIDs (assignment per CID stays the same across reconnects)
* Connected Client List, Client Add/Remove/Entry Change to clients with the incremental flag
* Client Entry Update (change the entry without reconnecting)
* Request Dynamic UIDs / Fetch Dynamic UID List
* RPT forwarding controller → device (also broadcast to all devices or
  all devices of one manufacturer), device → controller (targeted or to all)
* Status UNKNOWN_RPT_UID and BROADCAST_COMPLETE from the broker
* Broker's own RDM responder (see :mod:`pyrdmnet.broker_responder`)
* Heartbeat (broker NULL) and disconnect on timeout
* Event log in memory and optionally as a JSONL file

* LLRP target (optional, ``BrokerConfig.llrp``)

Not included: EPT clients, redirect. DNS-SD registration via :mod:`pyrdmnet.dnssd`.
"""

from __future__ import annotations

import asyncio
import collections
import datetime as dt
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import constants as C
from . import pdu as P
from .broker_responder import BrokerResponder, BrokerResponderInfo
from .llrp import LlrpNetwork, LlrpTarget
from .rdm import ALL_CONTROLLERS, RdmFormatError, RdmMessage, Uid, manufacturer_from_mid_broadcast, is_device_broadcast
from .connection import TcpConnection

log = logging.getLogger("pyrdmnet.broker")


@dataclass
class BrokerConfig:
    host: str = "127.0.0.1"
    port: int = 8888  # 0 = pick a free port
    scope: str = C.E133_DEFAULT_SCOPE
    search_domain: str = C.E133_DEFAULT_DOMAIN  # for RDM SEARCH_DOMAIN
    device_label: str = "pyrdmnet Broker"  # for RDM DEVICE_LABEL
    uid: Uid = field(default_factory=lambda: Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x00000001))
    cid: uuid.UUID = field(default_factory=uuid.uuid4)
    max_clients: int = 64
    heartbeat_interval_s: float = C.E133_HEARTBEAT_INTERVAL_S
    heartbeat_timeout_s: float = C.E133_HEARTBEAT_TIMEOUT_S
    connect_timeout_s: float = 10.0
    event_file: Path | None = None
    capture_file: Path | None = None  # raw data of all packets as JSONL (live test diagnostics)
    capture_max_lines: int = 50000
    llrp: bool = False  # respond as LLRP target (E1.33 5.2: mandatory for brokers in production)
    llrp_network: LlrpNetwork | None = None  # default: multicast on the adapter of ``host``


@dataclass
class _Client:
    connection: TcpConnection
    entry: P.ClientEntry
    incremental: bool
    connected_since: str

    @property
    def uid(self) -> Uid:
        return self.entry.uid

    @property
    def is_controller(self) -> bool:
        return self.entry.type == C.RPT_CLIENT_TYPE_CONTROLLER


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


class RdmnetBroker:
    def __init__(self, config: BrokerConfig | None = None):
        self.config = config or BrokerConfig()
        self._server: asyncio.base_events.Server | None = None
        self._clients: dict[Uid, _Client] = {}
        self._dyn_uid_by_rid: dict[tuple[int, uuid.UUID], Uid] = {}
        self._dyn_next: dict[int, int] = collections.defaultdict(lambda: 1)
        self._heartbeat_task: asyncio.Task | None = None
        self._connection_tasks: set[asyncio.Task] = set()
        self.events: collections.deque = collections.deque(maxlen=1000)
        self.counters = collections.Counter()
        self.port: int | None = None
        self._capture_lines = 0
        self.llrp_target: LlrpTarget | None = None
        k = self.config
        self.responder = BrokerResponder(
            BrokerResponderInfo(uid=k.uid, scope=k.scope, search_domain=k.search_domain, device_label=k.device_label),
            event=self._event)

    # ----------------------------------------------------------------- Lifecycle
    async def start(self) -> None:
        self._server = await asyncio.start_server(self._new_connection, self.config.host, self.config.port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self.config.llrp:
            network = self.config.llrp_network or LlrpNetwork(
                adapter=self.config.host if self.config.host not in ("", "0.0.0.0", "127.0.0.1") else "0.0.0.0")
            self.llrp_target = LlrpTarget(self.config.uid, self.config.cid, C.LLRP_TYPE_BROKER,
                                          self.responder.respond, network)
            try:
                await self.llrp_target.start()
            except OSError as error:  # LLRP is an add-on: the broker keeps running anyway
                self.llrp_target = None
                self._event("llrp_unavailable", error=str(error))
        self._event("broker_started", host=self.config.host, port=self.port, scope=self.config.scope,
                       uid=str(self.config.uid), cid=str(self.config.cid))

    async def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self.llrp_target:
            await self.llrp_target.stop()
            self.llrp_target = None
        for c in list(self._clients.values()):
            try:
                await c.connection.send_broker(C.VECTOR_BROKER_DISCONNECT, C.E133_DISCONNECT_SHUTDOWN.to_bytes(2, "big"))
            except Exception:
                pass
            await c.connection.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for t in list(self._connection_tasks):
            t.cancel()
        if self._connection_tasks:
            await asyncio.gather(*self._connection_tasks, return_exceptions=True)
        self._clients.clear()
        self._event("broker_stopped")

    def status(self) -> dict:
        return {
            "time_utc": _now(),
            "host": self.config.host,
            "port": self.port,
            "scope": self.config.scope,
            "broker_uid": str(self.config.uid),
            "clients": [
                {"uid": str(c.uid), "type": c.entry.type_text, "cid": str(c.entry.cid),
                 "peer": c.connection.peer, "connected_since": c.connected_since}
                for c in self._clients.values()
            ],
            "counters": dict(self.counters),
        }

    # ----------------------------------------------------------------- Events
    def _event(self, kind: str, **data) -> None:
        e = {"time_utc": _now(), "event": kind, **data}
        self.events.append(e)
        log.info("%s %s", kind, data)
        if self.config.event_file:
            try:
                with open(self.config.event_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
            except OSError as error:  # logging must never stop the broker
                log.warning("Event file not writable: %s", error)

    def _capture(self, v: TcpConnection, direction: str, data: bytes) -> None:
        if self._capture_lines >= self.config.capture_max_lines:
            return
        self._capture_lines += 1
        e = {"time_utc": _now(), "peer": v.peer, "richtung": direction, "laenge": len(data),
             "hex": data[:4096].hex()}
        try:
            with open(self.config.capture_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(e) + "\n")
        except OSError:
            pass

    # ----------------------------------------------------------------- Connection
    async def _new_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        self._connection_tasks.add(task)
        v = TcpConnection(reader, writer, self.config.cid)
        if self.config.capture_file:
            v.capture = lambda direction, data: self._capture(v, direction, data)
        self._event("tcp_connection", peer=v.peer)
        client: _Client | None = None
        try:
            client = await asyncio.wait_for(self._handle_connect(v), timeout=self.config.connect_timeout_s)
            if client is None:
                return
            while True:
                for root in await v.read():
                    await self._handle_root(client, root)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass
        except P.ProtocolError as error:
            self.counters["protocol_error"] += 1
            self._event("protocol_error", peer=v.peer, error=str(error))
        except asyncio.CancelledError:
            raise
        except Exception as error:  # unexpected: log it, close the connection
            self.counters["internal_error"] += 1
            self._event("internal_error", peer=v.peer, error=repr(error))
        finally:
            await v.close()
            if client is not None:
                await self._remove_client(client)
            self._connection_tasks.discard(task)

    async def _handle_connect(self, v: TcpConnection) -> _Client | None:
        roots = await v.read()
        root = roots[0]
        if root.vector != C.VECTOR_ROOT_E133_BROKER:
            raise P.ProtocolError("First message is not a broker PDU")
        bp = P.read_broker(root.data)
        if bp.vector != C.VECTOR_BROKER_CONNECT:
            raise P.ProtocolError("First message is not a Client Connect")
        try:
            cd = P.read_connect(bp.data)
        except P.ProtocolError:
            await self._connect_reply(v, C.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0))
            raise
        self._event("connect_request", peer=v.peer, scope=cd.scope, version=cd.version,
                       uid=str(cd.client.uid), type=cd.client.type_text, protocol=f"{cd.client.protocol:#x}",
                       incremental=bool(cd.flags & C.BROKER_CONNECT_FLAG_INCREMENTAL))
        code, uid = self._check_connect(cd, root.cid)
        await self._connect_reply(v, code, uid)
        if code != C.E133_CONNECT_OK:
            self.counters["connect_rejected"] += 1
            self._event("connect_rejected", peer=v.peer, reason=C.CONNECT_CODE_TEXT[code],
                           scope=cd.scope, uid=str(cd.client.uid))
            return None
        entry = P.ClientEntry(cd.client.cid, uid, cd.client.type, cd.client.binding_cid)
        client = _Client(v, entry, bool(cd.flags & C.BROKER_CONNECT_FLAG_INCREMENTAL), _now())
        self._clients[uid] = client
        self.counters["connect_ok"] += 1
        self._event("client_connected", uid=str(uid), type=entry.type_text, peer=v.peer)
        await self._send_to_incremental(C.VECTOR_BROKER_CLIENT_ADD, entry.encode(), except_uid=uid)
        return client

    def _check_connect(self, cd: P.ConnectData, root_cid: uuid.UUID) -> tuple[int, Uid]:
        if cd.scope != self.config.scope:
            return C.E133_CONNECT_SCOPE_MISMATCH, Uid(0, 0)
        if cd.version != C.E133_VERSION or cd.client.protocol != C.E133_CLIENT_PROTOCOL_RPT:
            return C.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if cd.client.type not in (C.RPT_CLIENT_TYPE_DEVICE, C.RPT_CLIENT_TYPE_CONTROLLER):
            return C.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if cd.client.cid != root_cid:
            return C.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if len(self._clients) >= self.config.max_clients:
            return C.E133_CONNECT_CAPACITY_EXCEEDED, Uid(0, 0)
        return self._check_uid(cd.client.uid, cd.client.cid)

    def _check_uid(self, uid: Uid, cid: uuid.UUID, own: _Client | None = None) -> tuple[int, Uid]:
        """UID check according to E1.33 9.1.5; ``own`` = client that is changing its own entry."""
        if uid.is_dynamic_request:
            uid = self._assign_dyn_uid(uid.manufacturer & 0x7FFF, cid)
        elif not uid.is_valid_for_client or uid.is_dynamic:
            return C.E133_CONNECT_INVALID_UID, Uid(0, 0)
        owner = self._clients.get(uid)
        if (owner is not None and owner is not own) or uid == self.config.uid:
            return C.E133_CONNECT_DUPLICATE_UID, Uid(0, 0)
        return C.E133_CONNECT_OK, uid

    def _assign_dyn_uid(self, manufacturer: int, rid: uuid.UUID) -> Uid:
        key = (manufacturer, rid)
        if key not in self._dyn_uid_by_rid:
            n = self._dyn_next[manufacturer]
            self._dyn_next[manufacturer] = n + 1
            self._dyn_uid_by_rid[key] = Uid(0x8000 | manufacturer, n)
        return self._dyn_uid_by_rid[key]

    async def _connect_reply(self, v: TcpConnection, code: int, uid: Uid) -> None:
        rep = P.ConnectReply(code, C.E133_VERSION, self.config.uid, uid)
        await v.send_broker(C.VECTOR_BROKER_CONNECT_REPLY, rep.encode())

    async def _remove_client(self, client: _Client) -> None:
        if self._clients.get(client.uid) is client:
            del self._clients[client.uid]
            self._event("client_disconnected", uid=str(client.uid), type=client.entry.type_text)
            await self._send_to_incremental(C.VECTOR_BROKER_CLIENT_REMOVE, client.entry.encode())

    async def _send_to_incremental(self, vector: int, data: bytes, except_uid: Uid | None = None) -> None:
        for c in list(self._clients.values()):
            if c.incremental and c.uid != except_uid:
                await self._safe_send(c, c.connection.send_broker(vector, data))

    async def _safe_send(self, client: _Client, coro) -> bool:
        try:
            await coro
            return True
        except (ConnectionError, OSError) as error:
            self._event("send_failed", uid=str(client.uid), error=str(error))
            await client.connection.close()
            return False

    # ----------------------------------------------------------------- Messages
    async def _handle_root(self, client: _Client, root: P.RootPdu) -> None:
        if root.vector == C.VECTOR_ROOT_E133_BROKER:
            await self._handle_broker(client, P.read_broker(root.data))
        elif root.vector == C.VECTOR_ROOT_E133_RPT:
            await self._handle_rpt(client, P.read_rpt(root.data))
        else:
            self.counters["unknown_root_vector"] += 1

    async def _handle_broker(self, client: _Client, bp: P.BrokerPdu) -> None:
        v = client.connection
        if bp.vector == C.VECTOR_BROKER_NULL:
            return
        if bp.vector == C.VECTOR_BROKER_FETCH_CLIENT_LIST:
            data = b"".join(c.entry.encode() for c in self._clients.values())
            await v.send_broker(C.VECTOR_BROKER_CONNECTED_CLIENT_LIST, data)
        elif bp.vector == C.VECTOR_BROKER_REQUEST_DYNAMIC_UIDS:
            responses = []
            for a in P.read_dyn_uid_requests(bp.data):
                if not a.uid.is_dynamic_request:
                    responses.append((a.uid, a.rid, C.E133_DYN_UID_INVALID))
                else:
                    responses.append((self._assign_dyn_uid(a.uid.manufacturer & 0x7FFF, a.rid), a.rid,
                                      C.E133_DYN_UID_OK))
            await v.send_broker(C.VECTOR_BROKER_ASSIGNED_DYNAMIC_UIDS, P.build_dyn_uid_assignments(responses))
        elif bp.vector == C.VECTOR_BROKER_FETCH_DYNAMIC_UID_LIST:
            reverse = {u: rid for (_h, rid), u in self._dyn_uid_by_rid.items()}
            responses = [(u, reverse[u], C.E133_DYN_UID_OK) if u in reverse
                         else (u, uuid.UUID(int=0), C.E133_DYN_UID_NOT_FOUND)
                         for u in P.read_uid_list(bp.data)]
            await v.send_broker(C.VECTOR_BROKER_ASSIGNED_DYNAMIC_UIDS, P.build_dyn_uid_assignments(responses))
        elif bp.vector == C.VECTOR_BROKER_CLIENT_ENTRY_UPDATE:
            await self._update_client_entry(client, bp.data)
        elif bp.vector == C.VECTOR_BROKER_DISCONNECT:
            self._event("client_disconnect", uid=str(client.uid))
            await v.close()
        else:
            self.counters["broker_vector_ignored"] += 1

    async def _update_client_entry(self, client: _Client, data: bytes) -> None:
        """Client Entry Update (E1.33 6.3.1.4, 9.1.6): check like a Connect, without scope.

        Protocol (vector) and CID (header) must stay the same; UID, client type and
        binding CID (data) may change. On error: Connect Reply with error code,
        remove the client, close the connection.
        """
        v, old = client.connection, client.entry
        try:
            flags, new = P.read_client_entry_update(data)
        except P.ProtocolError:
            new = None
        if (new is None or new.protocol != old.protocol or new.cid != old.cid
                or new.type not in (C.RPT_CLIENT_TYPE_DEVICE, C.RPT_CLIENT_TYPE_CONTROLLER)):
            code, uid = C.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        else:
            code, uid = self._check_uid(new.uid, new.cid, own=client)
        if code != C.E133_CONNECT_OK:
            self.counters["entry_update_rejected"] += 1
            self._event("entry_update_rejected", uid=str(old.uid), reason=C.CONNECT_CODE_TEXT[code])
            await self._safe_send(client, self._connect_reply(v, code, uid))
            await self._remove_client(client)
            await v.close()
            return
        entry = P.ClientEntry(new.cid, uid, new.type, new.binding_cid)
        if uid != old.uid:
            self._clients.pop(old.uid, None)
            self._clients[uid] = client
        client.entry = entry
        client.incremental = bool(flags & C.BROKER_CONNECT_FLAG_INCREMENTAL)
        self.counters["entry_update_ok"] += 1
        self._event("entry_changed", uid_old=str(old.uid), uid=str(uid), type=entry.type_text,
                       incremental=client.incremental)
        await self._safe_send(client, self._connect_reply(v, C.E133_CONNECT_OK, uid))
        await self._send_to_incremental(C.VECTOR_BROKER_CLIENT_ENTRY_CHANGE, entry.encode(), except_uid=uid)

    async def _handle_rpt(self, client: _Client, rpt: P.RptPdu) -> None:
        k = rpt.header
        if k.source != client.uid:
            self.counters["rpt_wrong_source"] += 1
            self._event("rpt_dropped", reason="Source UID does not match the connection", uid=str(client.uid))
            return
        packet = P.build_packet(C.VECTOR_ROOT_E133_RPT, self.config.cid, P.build_rpt(k, rpt.data))

        if k.vector == C.VECTOR_RPT_REQUEST:
            if not client.is_controller:
                self.counters["request_from_device_dropped"] += 1
                return
            if is_device_broadcast(k.dest):
                mid = manufacturer_from_mid_broadcast(k.dest)
                for c in list(self._clients.values()):
                    if not c.is_controller and (mid is None or c.uid.manufacturer & 0x7FFF == mid):
                        await self._safe_send(c, c.connection.send_raw(packet))
                self.counters["rpt_broadcast"] += 1
                await self._status_to(client, k, C.RPT_STATUS_BROADCAST_COMPLETE)
                return
            if k.dest == self.config.uid:
                await self._to_broker_responder(client, rpt)
                return
            dest = self._clients.get(k.dest)
            if dest is None or dest.is_controller:
                self.counters["rpt_unknown_uid"] += 1
                await self._status_to(client, k, C.RPT_STATUS_UNKNOWN_RPT_UID)
                return
            if await self._safe_send(dest, dest.connection.send_raw(packet)):
                self.counters["rpt_request_forwarded"] += 1

        elif k.vector in (C.VECTOR_RPT_NOTIFICATION, C.VECTOR_RPT_STATUS):
            if k.dest == ALL_CONTROLLERS:
                for c in list(self._clients.values()):
                    if c.is_controller:
                        await self._safe_send(c, c.connection.send_raw(packet))
            else:
                dest = self._clients.get(k.dest)
                if dest is None:
                    self.counters["rpt_response_without_dest"] += 1
                    return
                await self._safe_send(dest, dest.connection.send_raw(packet))
            self.counters["rpt_response_forwarded"] += 1
        else:
            self.counters["rpt_unknown_vector"] += 1
            await self._status_to(client, k, C.RPT_STATUS_UNKNOWN_VECTOR)

    async def _to_broker_responder(self, client: _Client, rpt: P.RptPdu) -> None:
        """RPT request to the broker UID: answered by the broker's own RDM responder (E1.33 6.2)."""
        k = rpt.header
        if k.dest_endpoint != C.E133_NULL_ENDPOINT:
            await self._status_to(client, k, C.RPT_STATUS_UNKNOWN_ENDPOINT)
            return
        try:
            messages = P.read_request_or_notification(rpt.data, C.VECTOR_REQUEST_RDM_CMD)
            if len(messages) != 1:
                raise P.ProtocolError("RPT request must contain exactly one RDM command")
            raw = messages[0]
            request = RdmMessage.decode(raw)
        except (P.ProtocolError, RdmFormatError, ValueError):
            await self._status_to(client, k, C.RPT_STATUS_INVALID_MESSAGE)
            return
        if request.dest != self.config.uid and not request.dest.is_broadcast:
            await self._status_to(client, k, C.RPT_STATUS_UNKNOWN_RDM_UID)
            return
        response = self.responder.respond(request)
        self.counters["broker_rdm_request"] += 1
        if response is None:
            return
        rheader = P.RptHeader(C.VECTOR_RPT_NOTIFICATION, self.config.uid, C.E133_NULL_ENDPOINT, k.source,
                          k.source_endpoint, k.sequence)
        await self._safe_send(client, client.connection.send_rpt(
            rheader, P.build_notification([raw, response.encode()])))

    async def _status_to(self, client: _Client, k: P.RptHeader, code: int) -> None:
        response = P.RptHeader(C.VECTOR_RPT_STATUS, k.dest, k.dest_endpoint, k.source, k.source_endpoint, k.sequence)
        await self._safe_send(client, client.connection.send_rpt(response, P.build_status(code)))

    # ----------------------------------------------------------------- Heartbeat
    async def _heartbeat_loop(self) -> None:
        tick = min(1.0, self.config.heartbeat_interval_s / 3)
        while True:
            await asyncio.sleep(tick)
            now = time.monotonic()
            for c in list(self._clients.values()):
                v = c.connection
                if now - v.last_received > self.config.heartbeat_timeout_s:
                    self.counters["heartbeat_timeout"] += 1
                    self._event("heartbeat_timeout", uid=str(c.uid))
                    await v.close()  # read loop ends, client gets removed
                elif now - v.last_sent >= self.config.heartbeat_interval_s:
                    await self._safe_send(c, v.send_broker(C.VECTOR_BROKER_NULL))
