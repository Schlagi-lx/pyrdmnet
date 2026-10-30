"""ACN/E1.33 framing: TCP preamble, root layer, broker, RPT, request,
notification, status and RDM command PDUs.

All PDUs are written with flags L|V|H|D (0xF0) set and a 20-bit length,
as specified in E1.33. On reading, V/H/D must be set (no inheritance);
otherwise the message is rejected.
"""

from __future__ import annotations

import asyncio
import struct
import uuid
from dataclasses import dataclass, field

from . import constants as C
from .rdm import Uid

MAX_BLOCK = 64 * 1024  # protection against oversized blocks from faulty clients


class ProtocolError(ValueError):
    """An RDMnet message is malformed."""


# ------------------------------------------------------------------- Building blocks
def flags_length(n: int) -> bytes:
    if not 0 < n < (1 << 20):
        raise ProtocolError(f"PDU length {n} cannot be represented")
    return bytes((C.ACN_PDU_FLAGS | ((n >> 16) & 0x0F), (n >> 8) & 0xFF, n & 0xFF))


def read_flags_length(buf: bytes, pos: int) -> tuple[int, int]:
    """Returns (PDU length, header size 2 or 3)."""
    if pos + 2 > len(buf):
        raise ProtocolError("PDU-Kopf abgeschnitten")
    flags = buf[pos]
    if flags & 0x70 != 0x70:
        raise ProtocolError(f"PDU inheritance (flags {flags:#04x}) is not supported")
    if flags & 0x80:
        if pos + 3 > len(buf):
            raise ProtocolError("PDU-Kopf abgeschnitten")
        n = ((flags & 0x0F) << 16) | (buf[pos + 1] << 8) | buf[pos + 2]
        header = 3
    else:
        n = ((flags & 0x0F) << 8) | buf[pos + 1]
        header = 2
    if n < header or pos + n > len(buf):
        raise ProtocolError(f"PDU length {n} does not fit the buffer")
    return n, header


def split_pdus(buf: bytes) -> list[tuple[bytes, int]]:
    """Splits a buffer into consecutive PDUs; returns (PDU, header size)."""
    pos, parts = 0, []
    while pos < len(buf):
        n, header = read_flags_length(buf, pos)
        parts.append((buf[pos:pos + n], header))
        pos += n
    return parts


# ------------------------------------------------------------------- Root Layer
@dataclass
class RootPdu:
    vector: int
    cid: uuid.UUID
    data: bytes


def build_packet(root_vector: int, cid: uuid.UUID, content: bytes) -> bytes:
    root = flags_length(3 + 4 + 16 + len(content)) + struct.pack(">I", root_vector) + cid.bytes + content
    return C.ACN_TCP_PREAMBLE + struct.pack(">I", len(root)) + root


def read_root_pdus(block: bytes) -> list[RootPdu]:
    result = []
    for pdu, header in split_pdus(block):
        if len(pdu) < header + 20:
            raise ProtocolError("Root PDU too short")
        (vector,) = struct.unpack(">I", pdu[header:header + 4])
        cid = uuid.UUID(bytes=bytes(pdu[header + 4:header + 20]))
        result.append(RootPdu(vector, cid, bytes(pdu[header + 20:])))
    return result


async def read_tcp_block(reader: asyncio.StreamReader, max_block: int = MAX_BLOCK) -> bytes:
    """Reads preamble + block size + block. Raises ProtocolError on a wrong preamble."""
    header = await reader.readexactly(16)
    if header[:12] != C.ACN_TCP_PREAMBLE:
        raise ProtocolError("Wrong ACN TCP preamble")
    (size,) = struct.unpack(">I", header[12:])
    if size == 0 or size > max_block:
        raise ProtocolError(f"PDU block size {size} not allowed")
    return await reader.readexactly(size)


# ------------------------------------------------------------------- Broker PDUs
@dataclass
class BrokerPdu:
    vector: int
    data: bytes


def build_broker(vector: int, data: bytes = b"") -> bytes:
    return flags_length(5 + len(data)) + struct.pack(">H", vector) + data


def read_broker(data: bytes) -> BrokerPdu:
    parts = split_pdus(data)
    if len(parts) != 1:
        raise ProtocolError("Expected exactly one broker PDU")
    pdu, header = parts[0]
    if len(pdu) < header + 2:
        raise ProtocolError("Broker PDU too short")
    (vector,) = struct.unpack(">H", pdu[header:header + 2])
    return BrokerPdu(vector, bytes(pdu[header + 2:]))


@dataclass
class ClientEntry:
    """RPT client entry (E1.33 6.3.2)."""

    cid: uuid.UUID
    uid: Uid
    type: int  # RPT_CLIENT_TYPE_DEVICE / _CONTROLLER
    binding_cid: uuid.UUID = field(default_factory=lambda: uuid.UUID(int=0))
    protocol: int = C.E133_CLIENT_PROTOCOL_RPT

    def encode(self) -> bytes:
        payload = self.uid.to_bytes() + bytes((self.type,)) + self.binding_cid.bytes
        return flags_length(3 + 4 + 16 + len(payload)) + struct.pack(">I", self.protocol) + self.cid.bytes + payload

    @property
    def type_text(self) -> str:
        return {0: "Device", 1: "Controller"}.get(self.type, f"Type {self.type}")


def read_client_entries(data: bytes) -> list[ClientEntry]:
    entries = []
    for pdu, header in split_pdus(data):
        if len(pdu) < header + 20:
            raise ProtocolError("Client entry too short")
        (prot,) = struct.unpack(">I", pdu[header:header + 4])
        cid = uuid.UUID(bytes=bytes(pdu[header + 4:header + 20]))
        payload = pdu[header + 20:]
        if prot != C.E133_CLIENT_PROTOCOL_RPT:
            # EPT clients are recognised but not supported by this module.
            entries.append(ClientEntry(cid, Uid(0, 0), 0xFF, protocol=prot))
            continue
        if len(payload) != 23:
            raise ProtocolError(f"RPT client entry with {len(payload)} bytes instead of 23")
        entries.append(ClientEntry(cid, Uid.from_bytes(bytes(payload[:6])), payload[6],
                                       uuid.UUID(bytes=bytes(payload[7:23]))))
    return entries


def build_client_entry_update(flags: int, entry: ClientEntry) -> bytes:
    """Data of a client entry update message (E1.33 6.3.1.4): flags + client entry."""
    return bytes((flags,)) + entry.encode()


def read_client_entry_update(data: bytes) -> tuple[int, ClientEntry]:
    if len(data) < 1:
        raise ProtocolError("Client entry update without data")
    entries = read_client_entries(data[1:])
    if len(entries) != 1:
        raise ProtocolError("Client entry update must contain exactly one client entry")
    return data[0], entries[0]


@dataclass
class ConnectData:
    scope: str
    version: int
    domain: str
    flags: int
    client: ClientEntry

    def encode(self) -> bytes:
        scope = self.scope.encode("utf-8")[:C.E133_SCOPE_LENGTH - 1]
        domain = self.domain.encode("utf-8")[:C.E133_DOMAIN_LENGTH - 1]
        return (scope.ljust(C.E133_SCOPE_LENGTH, b"\x00") + struct.pack(">H", self.version)
                + domain.ljust(C.E133_DOMAIN_LENGTH, b"\x00") + bytes((self.flags,)) + self.client.encode())


def _null_string(b: bytes) -> str:
    return b.split(b"\x00", 1)[0].decode("utf-8", errors="replace")


def read_connect(data: bytes) -> ConnectData:
    fixed = C.E133_SCOPE_LENGTH + 2 + C.E133_DOMAIN_LENGTH + 1
    if len(data) < fixed + 23:
        raise ProtocolError("Connect message too short")
    scope = _null_string(data[:C.E133_SCOPE_LENGTH])
    (version,) = struct.unpack(">H", data[C.E133_SCOPE_LENGTH:C.E133_SCOPE_LENGTH + 2])
    dpos = C.E133_SCOPE_LENGTH + 2
    domain = _null_string(data[dpos:dpos + C.E133_DOMAIN_LENGTH])
    flags = data[fixed - 1]
    entries = read_client_entries(data[fixed:])
    if len(entries) != 1:
        raise ProtocolError("Connect must contain exactly one client entry")
    return ConnectData(scope, version, domain, flags, entries[0])


@dataclass
class ConnectReply:
    code: int
    version: int
    broker_uid: Uid
    client_uid: Uid

    def encode(self) -> bytes:
        return struct.pack(">HH", self.code, self.version) + self.broker_uid.to_bytes() + self.client_uid.to_bytes()


def read_connect_reply(data: bytes) -> ConnectReply:
    if len(data) != 16:
        raise ProtocolError(f"Connect reply with {len(data)} bytes instead of 16")
    code, version = struct.unpack(">HH", data[:4])
    return ConnectReply(code, version, Uid.from_bytes(data[4:10]), Uid.from_bytes(data[10:16]))


@dataclass
class DynUidRequest:
    uid: Uid  # manufacturer ID with MSB set, device ID 0
    rid: uuid.UUID


def read_dyn_uid_requests(data: bytes) -> list[DynUidRequest]:
    if len(data) % 22:
        raise ProtocolError("Dynamic UID request: length not a multiple of 22")
    return [DynUidRequest(Uid.from_bytes(data[i:i + 6]), uuid.UUID(bytes=bytes(data[i + 6:i + 22])))
            for i in range(0, len(data), 22)]


def build_dyn_uid_assignments(mappings: list[tuple[Uid, uuid.UUID, int]]) -> bytes:
    return b"".join(u.to_bytes() + r.bytes + struct.pack(">H", s) for u, r, s in mappings)


def read_dyn_uid_assignments(data: bytes) -> list[tuple[Uid, uuid.UUID, int]]:
    if len(data) % 24:
        raise ProtocolError("Dynamic UID mapping: length not a multiple of 24")
    return [(Uid.from_bytes(data[i:i + 6]), uuid.UUID(bytes=bytes(data[i + 6:i + 22])),
             struct.unpack(">H", data[i + 22:i + 24])[0]) for i in range(0, len(data), 24)]


def read_uid_list(data: bytes) -> list[Uid]:
    if len(data) % 6:
        raise ProtocolError("UID list: length not a multiple of 6")
    return [Uid.from_bytes(data[i:i + 6]) for i in range(0, len(data), 6)]


# ------------------------------------------------------------------- RPT
@dataclass
class RptHeader:
    vector: int
    source: Uid
    source_endpoint: int
    dest: Uid
    dest_endpoint: int
    sequence: int


@dataclass
class RptPdu:
    header: RptHeader
    data: bytes  # request, status or notification PDU


def build_rpt(header: RptHeader, data: bytes) -> bytes:
    return (flags_length(28 + len(data)) + struct.pack(">I", header.vector) + header.source.to_bytes()
            + struct.pack(">H", header.source_endpoint) + header.dest.to_bytes()
            + struct.pack(">HIB", header.dest_endpoint, header.sequence & 0xFFFFFFFF, 0) + data)


def read_rpt(data: bytes) -> RptPdu:
    parts = split_pdus(data)
    if len(parts) != 1:
        raise ProtocolError("Expected exactly one RPT PDU")
    pdu, header = parts[0]
    if len(pdu) < header + 25:
        raise ProtocolError("RPT PDU too short")
    p = header
    (vector,) = struct.unpack(">I", pdu[p:p + 4])
    source = Uid.from_bytes(bytes(pdu[p + 4:p + 10]))
    (src_ep,) = struct.unpack(">H", pdu[p + 10:p + 12])
    dest = Uid.from_bytes(bytes(pdu[p + 12:p + 18]))
    dst_ep, seq, _res = struct.unpack(">HIB", pdu[p + 18:p + 25])
    return RptPdu(RptHeader(vector, source, src_ep, dest, dst_ep, seq), bytes(pdu[p + 25:]))


def build_rdm_cmd_pdu(rdm_raw: bytes) -> bytes:
    """RDM command PDU: flags+length, vector 0xCC (= start code), rest of the RDM message."""
    if not rdm_raw or rdm_raw[0] != C.VECTOR_RDM_CMD_RDM_DATA:
        raise ProtocolError("RDM message must begin with start code 0xCC")
    return flags_length(3 + len(rdm_raw)) + rdm_raw


def _read_rdm_cmd_pdus(data: bytes) -> list[bytes]:
    messages = []
    for pdu, header in split_pdus(data):
        if len(pdu) <= header or pdu[header] != C.VECTOR_RDM_CMD_RDM_DATA:
            raise ProtocolError("RDM command PDU with wrong vector")
        messages.append(bytes(pdu[header:]))  # incl. start code
    return messages


def build_request(rdm_raw: bytes) -> bytes:
    cmd = build_rdm_cmd_pdu(rdm_raw)
    return flags_length(7 + len(cmd)) + struct.pack(">I", C.VECTOR_REQUEST_RDM_CMD) + cmd


def build_notification(rdm_messages: list[bytes]) -> bytes:
    cmds = b"".join(build_rdm_cmd_pdu(r) for r in rdm_messages)
    return flags_length(7 + len(cmds)) + struct.pack(">I", C.VECTOR_NOTIFICATION_RDM_CMD) + cmds


def read_request_or_notification(data: bytes, expected_vector: int) -> list[bytes]:
    parts = split_pdus(data)
    if len(parts) != 1:
        raise ProtocolError("Expected exactly one request/notification PDU")
    pdu, header = parts[0]
    if len(pdu) < header + 4:
        raise ProtocolError("Request/notification PDU too short")
    (vector,) = struct.unpack(">I", pdu[header:header + 4])
    if vector != expected_vector:
        raise ProtocolError(f"Unexpected vector {vector:#x}")
    return _read_rdm_cmd_pdus(bytes(pdu[header + 4:]))


def build_status(code: int, text: str = "") -> bytes:
    t = text.encode("utf-8")[:1024]
    return flags_length(5 + len(t)) + struct.pack(">H", code) + t


def read_status(data: bytes) -> tuple[int, str]:
    parts = split_pdus(data)
    if len(parts) != 1:
        raise ProtocolError("Expected exactly one status PDU")
    pdu, header = parts[0]
    if len(pdu) < header + 2:
        raise ProtocolError("Status PDU too short")
    (code,) = struct.unpack(">H", pdu[header:header + 2])
    return code, bytes(pdu[header + 2:]).decode("utf-8", errors="replace")
