"""Extensible Packet Transport (EPT, ANSI E1.33 section 8).

EPT carries manufacturer-defined protocols through the broker. EPT components are
addressed by CID only (no UIDs). This module provides

* encoding and decoding of EPT PDUs, EPT Data PDUs and EPT Status PDUs, including
  PDU inheritance (E1.33 4.1.1): a block may contain several EPT PDUs whose vector
  and data are inherited from the previous PDU, e.g. one message to several CIDs
  (Appendix D.9). Inheritance is only allowed for EPT; RPT, LLRP and broker PDUs
  always carry all fields.
* :class:`EptClient`, a client that connects with CLIENT_PROTOCOL_EPT, keeps the EPT
  client list and sends/receives EPT data.

EPT PDUs always use a 24-bit length field (8.3.2.1). The text requires the L bit to
be set, but the example in Appendix D.9 shows inherited PDUs with flags 0x20 (L bit
clear); both forms are accepted on receive, and PDUs are always sent with the L bit set.
"""

from __future__ import annotations

import asyncio
import logging
import struct
import uuid
from dataclasses import dataclass, field

from . import constants as C
from . import pdu as P
from .client import RdmnetConnectionError, RptClient
from .rdm import Uid

log = logging.getLogger("pyrdmnet.ept")

_FLAG_V, _FLAG_H, _FLAG_D = 0x40, 0x20, 0x10


# ------------------------------------------------------------------- PDU blocks with inheritance
def read_inheriting_pdus(block: bytes, vector_len: int, header_len: int) -> list[tuple[bytes, bytes, bytes]]:
    """Split an EPT PDU block into (vector, header, data), resolving inheritance (E1.33 4.1.1)."""
    out: list[tuple[bytes, bytes, bytes]] = []
    pos, prev = 0, None
    while pos < len(block):
        if pos + 3 > len(block):
            raise P.ProtocolError("EPT PDU header truncated")
        flags = block[pos]
        n = ((flags & 0x0F) << 16) | (block[pos + 1] << 8) | block[pos + 2]  # always 24 bits (8.3.2.1)
        if n < 3 or pos + n > len(block):
            raise P.ProtocolError(f"EPT PDU length {n} does not fit the block")
        body, i = block[pos + 3:pos + n], 0
        if flags & _FLAG_V:
            vector, i = body[i:i + vector_len], i + vector_len
            if len(vector) != vector_len:
                raise P.ProtocolError("EPT PDU vector truncated")
        elif prev is None:
            raise P.ProtocolError("First EPT PDU in a block cannot inherit its vector")
        else:
            vector = prev[0]
        if header_len == 0:
            header = b""
        elif flags & _FLAG_H:
            header, i = body[i:i + header_len], i + header_len
            if len(header) != header_len:
                raise P.ProtocolError("EPT PDU header segment truncated")
        elif prev is None:
            raise P.ProtocolError("First EPT PDU in a block cannot inherit its header")
        else:
            header = prev[1]
        if flags & _FLAG_D:
            data = bytes(body[i:])
        elif prev is None:
            raise P.ProtocolError("First EPT PDU in a block cannot inherit its data")
        elif i != len(body):
            raise P.ProtocolError("EPT PDU without data flag has trailing bytes")
        else:
            data = prev[2]
        prev = (bytes(vector), bytes(header), data)
        out.append(prev)
        pos += n
    return out


def _single(block: bytes, vector_len: int, what: str) -> tuple[bytes, bytes]:
    parts = read_inheriting_pdus(block, vector_len, 0)
    if len(parts) != 1:
        raise P.ProtocolError(f"Expected exactly one {what}")
    return parts[0][0], parts[0][2]


# ------------------------------------------------------------------- EPT PDU (8.3.2)
@dataclass
class EptPdu:
    vector: int  # VECTOR_EPT_DATA / VECTOR_EPT_STATUS (others are answered with Unknown Vector)
    dest_cid: uuid.UUID
    data: bytes  # encoded EPT Data PDU or EPT Status PDU


def read_ept_pdus(block: bytes) -> list[EptPdu]:
    return [EptPdu(struct.unpack(">I", v)[0], uuid.UUID(bytes=h), d)
            for v, h, d in read_inheriting_pdus(block, 4, 16)]


def build_ept_pdu(vector: int, dest_cid: uuid.UUID, data: bytes) -> bytes:
    return P.flags_length(3 + 4 + 16 + len(data)) + struct.pack(">I", vector) + dest_cid.bytes + data


def build_ept_pdus_shared(vector: int, dest_cids, data: bytes) -> bytes:
    """One message to several CIDs: the first PDU is complete, the others inherit vector and data."""
    cids = list(dest_cids)
    if not cids:
        raise ValueError("At least one destination CID needed")
    first = build_ept_pdu(vector, cids[0], data)
    rest = b"".join(bytes((0x80 | _FLAG_H, 0, 19)) + c.bytes for c in cids[1:])  # L|H, length 3 + 16
    return first + rest


def build_ept_packet(sender_cid: uuid.UUID, block: bytes) -> bytes:
    return P.build_packet(C.VECTOR_ROOT_E133_EPT, sender_cid, block)


# ------------------------------------------------------------------- EPT Data PDU (8.3.3)
@dataclass
class EptData:
    protocol_vector: int  # ESTA manufacturer ID (high 16 bits) + protocol ID (low 16 bits)
    payload: bytes


def build_ept_data(protocol_vector: int, payload: bytes) -> bytes:
    return P.flags_length(3 + 4 + len(payload)) + struct.pack(">I", protocol_vector) + payload


def read_ept_data(data: bytes) -> EptData:
    vector, payload = _single(data, 4, "EPT Data PDU")
    return EptData(struct.unpack(">I", vector)[0], payload)


# ------------------------------------------------------------------- EPT Status PDU (8.3.4)
@dataclass
class EptStatus:
    code: int  # VECTOR_EPT_STATUS_UNKNOWN_CID / _UNKNOWN_VECTOR
    cid: uuid.UUID | None = None  # Unknown CID: the CID that was not found
    vector: int | None = None  # Unknown Vector: the vector that was not recognised
    text: str = ""

    @property
    def code_text(self) -> str:
        return {C.VECTOR_EPT_STATUS_UNKNOWN_CID: "UNKNOWN_CID",
                C.VECTOR_EPT_STATUS_UNKNOWN_VECTOR: "UNKNOWN_VECTOR"}.get(self.code, f"{self.code:#06x}")


def _status_text(text: str) -> bytes:
    return text.encode("utf-8")[:C.EPT_STATUS_STRING_MAX]


def build_ept_status_unknown_cid(cid: uuid.UUID, text: str = "") -> bytes:
    payload = cid.bytes + _status_text(text)
    return P.flags_length(3 + 2 + len(payload)) + struct.pack(">H", C.VECTOR_EPT_STATUS_UNKNOWN_CID) + payload


def build_ept_status_unknown_vector(vector: int, text: str = "") -> bytes:
    payload = struct.pack(">I", vector) + _status_text(text)
    return P.flags_length(3 + 2 + len(payload)) + struct.pack(">H", C.VECTOR_EPT_STATUS_UNKNOWN_VECTOR) + payload


def read_ept_status(data: bytes) -> EptStatus:
    vector, payload = _single(data, 2, "EPT Status PDU")
    code = struct.unpack(">H", vector)[0]
    text = lambda raw: raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")  # noqa: E731
    if code == C.VECTOR_EPT_STATUS_UNKNOWN_CID:
        if len(payload) < 16:
            raise P.ProtocolError("EPT Unknown CID status too short")
        return EptStatus(code, cid=uuid.UUID(bytes=payload[:16]), text=text(payload[16:]))
    if code == C.VECTOR_EPT_STATUS_UNKNOWN_VECTOR:
        if len(payload) < 4:
            raise P.ProtocolError("EPT Unknown Vector status too short")
        return EptStatus(code, vector=struct.unpack(">I", payload[:4])[0], text=text(payload[4:]))
    return EptStatus(code, text=text(payload))


# ------------------------------------------------------------------- client
@dataclass
class EptMessage:
    sender_cid: uuid.UUID
    protocol_vector: int
    payload: bytes


class EptClient(RptClient):
    """EPT client (E1.33 8.2): connects with an EPT client entry, keeps the EPT client list.

    Received data arrives in :attr:`received` (asyncio queue of :class:`EptMessage`), status
    messages in :attr:`statuses` as (sender CID, :class:`EptStatus`).
    """

    def __init__(self, protocols, scope: str = C.E133_DEFAULT_SCOPE, cid: uuid.UUID | None = None,
                 incremental: bool = True, **kwargs):
        super().__init__(0xFF, Uid(0, 0), scope, cid, incremental=incremental, **kwargs)
        self.protocols = tuple(protocols)
        self.clients: dict[uuid.UUID, P.ClientEntry] = {}
        self.received: asyncio.Queue[EptMessage] = asyncio.Queue()
        self.statuses: list[tuple[uuid.UUID, EptStatus]] = []
        self.status_event = asyncio.Event()
        self._client_list_ready = asyncio.Event()

    def _connect_entry(self) -> P.ClientEntry:
        return P.ClientEntry.ept(self.cid, self.protocols)

    async def _after_connect(self) -> None:
        self._client_list_ready.clear()
        await self.send_broker(C.VECTOR_BROKER_FETCH_CLIENT_LIST)

    async def client_list(self, timeout_s: float = 5.0, refresh: bool = False) -> dict[uuid.UUID, P.ClientEntry]:
        if refresh:
            self._client_list_ready.clear()
            await self.send_broker(C.VECTOR_BROKER_FETCH_CLIENT_LIST)
        await asyncio.wait_for(self._client_list_ready.wait(), timeout_s)
        return dict(self.clients)

    async def _on_broker(self, bp: P.BrokerPdu) -> None:
        if bp.vector == C.VECTOR_BROKER_CONNECTED_CLIENT_LIST:
            self.clients = {e.cid: e for e in P.read_client_entries(bp.data) if e.is_ept}
            self._client_list_ready.set()
        elif bp.vector in (C.VECTOR_BROKER_CLIENT_ADD, C.VECTOR_BROKER_CLIENT_ENTRY_CHANGE):
            self.clients.update({e.cid: e for e in P.read_client_entries(bp.data) if e.is_ept})
        elif bp.vector == C.VECTOR_BROKER_CLIENT_REMOVE:
            for e in P.read_client_entries(bp.data):
                self.clients.pop(e.cid, None)

    async def send_data(self, dest_cids, protocol_vector: int, payload: bytes) -> None:
        """Send EPT data to one CID or a list of CIDs (packed with inheritance, Appendix D.9)."""
        cids = [dest_cids] if isinstance(dest_cids, uuid.UUID) else list(dest_cids)
        block = build_ept_pdus_shared(C.VECTOR_EPT_DATA, cids, build_ept_data(protocol_vector, payload))
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        await self._v.send_raw(build_ept_packet(self.cid, block))

    async def update_protocols(self, protocols, timeout_s: float = 5.0) -> None:
        """Change the sub-protocol list without reconnecting (Client Entry Update, 8.2.2)."""
        if not self.connected:
            raise RdmnetConnectionError("Not connected to broker")
        flags = C.BROKER_CONNECT_FLAG_INCREMENTAL if self.incremental else 0
        self._update_reply = asyncio.get_running_loop().create_future()
        await self.send_broker(C.VECTOR_BROKER_CLIENT_ENTRY_UPDATE,
                               P.build_client_entry_update(flags, P.ClientEntry.ept(self.cid, protocols)))
        try:
            rep = await asyncio.wait_for(asyncio.shield(self._update_reply), timeout_s)
        except asyncio.TimeoutError as error:
            raise RdmnetConnectionError("No response from broker to Client Entry Update") from error
        if rep.code != C.E133_CONNECT_OK:
            raise RdmnetConnectionError(f"Broker rejected entry update: "
                                        f"{C.CONNECT_CODE_TEXT.get(rep.code, rep.code)}", rep.code)
        self.protocols = tuple(protocols)

    async def _on_ept(self, root: P.RootPdu) -> None:
        for pdu in read_ept_pdus(root.data):
            if pdu.dest_cid != self.cid:
                continue
            if pdu.vector == C.VECTOR_EPT_DATA:
                d = read_ept_data(pdu.data)
                await self.received.put(EptMessage(root.cid, d.protocol_vector, d.payload))
            elif pdu.vector == C.VECTOR_EPT_STATUS:
                self.statuses.append((root.cid, read_ept_status(pdu.data)))
                self.status_event.set()
            else:  # 8.3.4.3: answer an unknown EPT PDU vector with Unknown Vector
                status = build_ept_status_unknown_vector(pdu.vector, "vector not supported")
                await self._v.send_raw(build_ept_packet(
                    self.cid, build_ept_pdu(C.VECTOR_EPT_STATUS, root.cid, status)))
