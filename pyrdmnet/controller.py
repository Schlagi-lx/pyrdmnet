"""RDMnet controller: RDM GET unrestricted, RDM SET only restricted.

Decision 2026-10-26 (option b): the software may set DMX mode and
start address on the device. Safeguards:
* SET only for the PIDs in :data:`ALLOWED_SET_PIDS`
* SET only with active write permission (``write_enabled = True``, per session)
* Discovery commands are never sent
Any other write request is rejected before sending (:class:`WriteLockError`).
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from . import constants as C
from . import pdu as P
from .client import RptClient, RdmnetConnectionError
from .rdm import RdmFormatError, RdmMessage, TransactionCounter, Uid

log = logging.getLogger("pyrdmnet.controller")


class RdmError(Exception):
    """Base class of all errors of an RDM request."""


ALLOWED_SET_PIDS = frozenset({C.PID_DMX_PERSONALITY, C.PID_DMX_START_ADDRESS, C.PID_IDENTIFY_DEVICE})


class WriteLockError(RdmError):
    """A writing RDM command is not allowed (PID not permitted or no write permission)."""


class RdmTimeoutError(RdmError):
    pass


class RdmNackError(RdmError):
    def __init__(self, pid: int, reason: int):
        self.pid, self.reason = pid, reason
        super().__init__(f"NACK for {C.PID_NAMES.get(pid, hex(pid))}: {C.NACK_TEXT.get(reason, hex(reason))}")


class RptStatusError(RdmError):
    def __init__(self, code: int, text: str = ""):
        self.code = code
        super().__init__(f"RPT-Status: {C.RPT_STATUS_TEXT.get(code, hex(code))} {text}".strip())


class RdmResponseError(RdmError):
    """Response formally invalid or does not match the request."""


class RdmnetController(RptClient):
    def __init__(self, uid: Uid | None = None, scope: str = C.E133_DEFAULT_SCOPE, cid: uuid.UUID | None = None,
                 **kwargs):
        # default: request a dynamic UID in the ESTA prototype range
        uid = uid or Uid(0x8000 | C.ESTA_PROTOTYPE_MANUFACTURER, 0)
        super().__init__(C.RPT_CLIENT_TYPE_CONTROLLER, uid, scope, cid, incremental=True, **kwargs)
        self.clients: dict[Uid, P.ClientEntry] = {}
        self._client_list_ready = asyncio.Event()
        self._pending: dict[int, asyncio.Future] = {}
        self._sequence = 0
        self._tn = TransactionCounter()
        self.unsolicited_messages: list[tuple[Uid, int, bytes]] = []
        self.ack_timer_max_s = 10.0
        self.ack_overflow_max = 32
        self.write_enabled = False  # enable explicitly per session (on-site checklist)

    # ----------------------------------------------------------------- Client list
    async def _after_connect(self) -> None:
        self._client_list_ready.clear()
        await self.send_broker(C.VECTOR_BROKER_FETCH_CLIENT_LIST)

    async def _after_disconnect(self, reason: str) -> None:
        for f in self._pending.values():
            if not f.done():
                f.set_exception(RdmnetConnectionError(f"Connection closed: {reason}"))
        self._pending.clear()

    async def client_list(self, timeout_s: float = 5.0, refresh: bool = False) -> dict[Uid, P.ClientEntry]:
        if refresh:
            self._client_list_ready.clear()
            await self.send_broker(C.VECTOR_BROKER_FETCH_CLIENT_LIST)
        await asyncio.wait_for(self._client_list_ready.wait(), timeout_s)
        return dict(self.clients)

    def devices(self) -> list[P.ClientEntry]:
        return [e for e in self.clients.values() if e.type == C.RPT_CLIENT_TYPE_DEVICE]

    async def _on_broker(self, bp: P.BrokerPdu) -> None:
        if bp.vector == C.VECTOR_BROKER_CONNECTED_CLIENT_LIST:
            self.clients = {e.uid: e for e in P.read_client_entries(bp.data)
                            if e.protocol == C.E133_CLIENT_PROTOCOL_RPT}
            self._client_list_ready.set()
        elif bp.vector in (C.VECTOR_BROKER_CLIENT_ADD, C.VECTOR_BROKER_CLIENT_ENTRY_CHANGE):
            for e in P.read_client_entries(bp.data):
                if e.protocol == C.E133_CLIENT_PROTOCOL_RPT:
                    # Entry Change may change the UID: replace the old entry with the same CID
                    for old in [u for u, a in self.clients.items() if a.cid == e.cid]:
                        del self.clients[old]
                    self.clients[e.uid] = e
        elif bp.vector == C.VECTOR_BROKER_CLIENT_REMOVE:
            for e in P.read_client_entries(bp.data):
                self.clients.pop(e.uid, None)

    # ----------------------------------------------------------------- RPT receive
    async def _on_rpt(self, rpt: P.RptPdu) -> None:
        k = rpt.header
        f = self._pending.get(k.sequence)
        if k.vector == C.VECTOR_RPT_STATUS:
            code, text = P.read_status(rpt.data)
            if f and not f.done():
                f.set_exception(RptStatusError(code, text))
            return
        if k.vector != C.VECTOR_RPT_NOTIFICATION:
            return
        messages = P.read_request_or_notification(rpt.data, C.VECTOR_NOTIFICATION_RDM_CMD)
        if f and not f.done():
            f.set_result(messages)
        else:
            for n in messages:
                self.unsolicited_messages.append((k.source, k.source_endpoint, n))
            del self.unsolicited_messages[:-200]

    # ----------------------------------------------------------------- RDM GET
    def _next_sequence(self) -> int:
        self._sequence = (self._sequence % 0xFFFFFFFF) + 1
        return self._sequence

    async def _send_and_wait(self, rpt_dest: Uid, endpoint: int, request: RdmMessage,
                               timeout_s: float) -> RdmMessage:
        self._check_write_permission(request.command_class, request.pid)
        seq = self._next_sequence()
        fut = asyncio.get_running_loop().create_future()
        self._pending[seq] = fut
        header = P.RptHeader(C.VECTOR_RPT_REQUEST, self.uid, C.E133_NULL_ENDPOINT, rpt_dest, endpoint, seq)
        try:
            await self.send_rpt(header, P.build_request(request.encode()))
            messages = await asyncio.wait_for(fut, timeout_s)
        except asyncio.TimeoutError as error:
            raise RdmTimeoutError(f"No response to {C.PID_NAMES.get(request.pid, hex(request.pid))} "
                                f"from {request.dest} within {timeout_s:.1f} s") from error
        finally:
            self._pending.pop(seq, None)
        return self._select_response(request, messages)

    def _check_write_permission(self, cc: int, pid: int) -> None:
        if cc == C.CC_GET:
            return
        if cc != C.CC_SET:
            raise WriteLockError("The measurement lab does not send discovery commands")
        if pid not in ALLOWED_SET_PIDS:
            raise WriteLockError(f"SET for {C.PID_NAMES.get(pid, hex(pid))} not permitted")
        if not self.write_enabled:
            raise WriteLockError("No write permission for this session")

    @staticmethod
    def _select_response(request: RdmMessage, messages: list[bytes]) -> RdmMessage:
        """Notification contains request and response; pick out the matching response."""
        for raw in messages:
            try:
                n = RdmMessage.decode(raw)
            except RdmFormatError as error:
                raise RdmResponseError(f"RDM response invalid: {error}") from error
            if not n.is_response:
                continue
            expected_cc = C.CC_GET_RESPONSE if request.command_class == C.CC_GET else C.CC_SET_RESPONSE
            if (n.pid != request.pid or n.transaction != request.transaction or n.source != request.dest
                    or n.command_class != expected_cc):
                raise RdmResponseError(f"Response does not match the request (PID {n.pid:#06x}, TN {n.transaction})")
            return n
        raise RdmResponseError("Notification without RDM response")

    async def rdm_get(self, rpt_dest: Uid, pid: int, data: bytes = b"", endpoint: int = C.E133_NULL_ENDPOINT,
                      rdm_dest: Uid | None = None, subdevice: int = C.RDM_ROOT_DEVICE,
                      timeout_s: float = 3.0) -> bytes:
        return await self._rdm_command(C.CC_GET, rpt_dest, pid, data, endpoint, rdm_dest, subdevice, timeout_s)

    async def rdm_set(self, rpt_dest: Uid, pid: int, data: bytes, endpoint: int = C.E133_NULL_ENDPOINT,
                      rdm_dest: Uid | None = None, subdevice: int = C.RDM_ROOT_DEVICE,
                      timeout_s: float = 3.0) -> bytes:
        """RDM SET - only for ALLOWED_SET_PIDS and with active write permission."""
        self._check_write_permission(C.CC_SET, pid)
        return await self._rdm_command(C.CC_SET, rpt_dest, pid, data, endpoint, rdm_dest, subdevice, timeout_s)

    async def _rdm_command(self, cc: int, rpt_dest: Uid, pid: int, data: bytes, endpoint: int,
                          rdm_dest: Uid | None, subdevice: int, timeout_s: float) -> bytes:
        """RDM GET/SET over RDMnet. Handles ACK_TIMER and ACK_OVERFLOW.

        rpt_dest: UID of the RDMnet device (gateway or native device)
        rdm_dest: UID of the RDM responder (default: rpt_dest, i.e. the default responder)
        endpoint: 0 = default responder, >0 = gateway port
        """
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        rdm_dest = rdm_dest or rpt_dest
        total = b""
        overflow = 0
        waited = 0.0
        while True:
            request = RdmMessage(dest=rdm_dest, source=self.uid, transaction=self._tn.next_tn(rdm_dest),
                                   port_or_response_type=1, command_class=cc, pid=pid, data=data,
                                   subdevice=subdevice)
            response = await self._send_and_wait(rpt_dest, endpoint, request, timeout_s)
            type = response.response_type
            if type == C.RESPONSE_ACK:
                return total + response.data
            if type == C.RESPONSE_NACK:
                reason = int.from_bytes(response.data[:2], "big") if len(response.data) >= 2 else 0xFFFF
                raise RdmNackError(pid, reason)
            if type == C.RESPONSE_ACK_TIMER:
                tenths = int.from_bytes(response.data[:2], "big") if len(response.data) >= 2 else 1
                pause = max(tenths, 1) / 10.0
                waited += pause
                if waited > self.ack_timer_max_s:
                    raise RdmTimeoutError(f"ACK_TIMER total exceeds {self.ack_timer_max_s} s")
                await asyncio.sleep(pause)
                if cc == C.CC_SET:
                    # SET is not repeated (would be executed twice); verify the result by reading back
                    return b""
                continue  # E1.20 provides QUEUED_MESSAGE; here: repeat the same GET request
            if type == C.RESPONSE_ACK_OVERFLOW:
                total += response.data
                overflow += 1
                if overflow > self.ack_overflow_max:
                    raise RdmResponseError("Too many ACK_OVERFLOW parts")
                continue
            raise RdmResponseError(f"Unknown response type {type}")
