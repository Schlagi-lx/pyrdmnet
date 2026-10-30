"""EPT (E1.33 section 8, 9.3): encoding incl. inheritance, broker routing, client lists, status."""

import asyncio
import struct
import unittest
import uuid

from pyrdmnet import constants as C
from pyrdmnet import ept as E
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.client import RdmnetConnectionError, RptClient
from pyrdmnet.controller import RdmnetController
from pyrdmnet.rdm import Uid

PROTO = 0x7FF00001  # ESTA prototype manufacturer ID + protocol ID 1
SUB = [P.EptSubProtocol(PROTO, "pyrdmnet test")]

# Appendix D.9.2, table D-8 (the third CID in the standard contains a typo "55al", corrected to "55a1")
SENDER = uuid.UUID("deadbeef-baad-f00d-face-b00cd15eea5e")
RECEIVERS = [uuid.UUID("beeffeed-fabc-0c0a-b0bc-0ded0b0ecafe"), uuid.UUID("d0d0face-ca55-e77e-ca5c-adedba5eba11"),
             uuid.UUID("decea5ed-0b57-ac1e-c010-55a1c0a1e5ce")]
D9_BLOCK = (bytes.fromhex("f0002b") + struct.pack(">I", 1) + RECEIVERS[0].bytes
            + bytes.fromhex("f00014") + struct.pack(">I", 1) + b"Hello worlds!"
            + bytes.fromhex("200013") + RECEIVERS[1].bytes
            + bytes.fromhex("200013") + RECEIVERS[2].bytes)


async def wait_until(condition, timeout_s=2.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class TestEncoding(unittest.TestCase):
    def test_appendix_d9_inheritance(self):
        packet = E.build_ept_packet(SENDER, D9_BLOCK)
        self.assertEqual(packet[12:16], bytes.fromhex("00000068"))  # PDU block size as in table D-8
        pdus = E.read_ept_pdus(D9_BLOCK)
        self.assertEqual([p.dest_cid for p in pdus], RECEIVERS)
        for p in pdus:
            self.assertEqual(p.vector, C.VECTOR_EPT_DATA)
            self.assertEqual(E.read_ept_data(p.data), E.EptData(1, b"Hello worlds!"))

    def test_shared_build_matches_layout(self):
        block = E.build_ept_pdus_shared(C.VECTOR_EPT_DATA, RECEIVERS, E.build_ept_data(1, b"Hello worlds!"))
        self.assertEqual(len(block), len(D9_BLOCK))
        self.assertEqual(block[82 - 39], 0xA0)  # octet 82 of table D-8 (packet offset); sent with L bit set
        self.assertEqual(E.read_ept_pdus(block), E.read_ept_pdus(D9_BLOCK))

    def test_inheritance_errors(self):
        with self.assertRaises(P.ProtocolError):
            E.read_ept_pdus(bytes.fromhex("200013") + RECEIVERS[0].bytes)  # first PDU cannot inherit
        with self.assertRaises(P.ProtocolError):
            E.read_ept_pdus(D9_BLOCK[:-3])  # truncated
        with self.assertRaises(P.ProtocolError):
            E.read_ept_pdus(D9_BLOCK + bytes.fromhex("200014") + RECEIVERS[0].bytes + b"x")  # trailing data

    def test_status(self):
        cid = uuid.uuid4()
        s = E.read_ept_status(E.build_ept_status_unknown_cid(cid, "not here"))
        self.assertEqual((s.code, s.cid, s.text, s.code_text), (C.VECTOR_EPT_STATUS_UNKNOWN_CID, cid, "not here",
                                                                "UNKNOWN_CID"))
        s = E.read_ept_status(E.build_ept_status_unknown_vector(0x1234))
        self.assertEqual((s.code, s.vector, s.text), (C.VECTOR_EPT_STATUS_UNKNOWN_VECTOR, 0x1234, ""))
        self.assertEqual(len(E.build_ept_status_unknown_cid(cid, "x" * 2000)), 3 + 2 + 16 + 1024)

    def test_client_entry(self):
        e = P.ClientEntry.ept(uuid.uuid4(), [P.EptSubProtocol(PROTO, "a"), P.EptSubProtocol(2, "b" * 40)])
        raw = e.encode()
        self.assertEqual(len(raw), 23 + 2 * 36)
        (read,) = P.read_client_entries(raw)
        self.assertTrue(read.is_ept)
        self.assertEqual([(p.vector, p.name) for p in read.ept_protocols], [(PROTO, "a"), (2, "b" * 31)])
        with self.assertRaises(P.ProtocolError):
            P.read_client_entries(raw[:-1].replace(raw[:3], P.flags_length(len(raw) - 1), 1))


class TestBrokerEpt(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0, max_clients=6))
        await self.b.start()
        self.clients: list[RptClient] = []

    async def asyncTearDown(self):
        for c in self.clients:
            await c.disconnect()
        await self.b.stop()

    async def ept(self, **kw) -> E.EptClient:
        c = E.EptClient(SUB, **kw)
        await c.connect("127.0.0.1", self.b.port)
        self.clients.append(c)
        await c.client_list()
        return c

    async def test_connect_and_separate_client_lists(self):
        a = await self.ept()
        self.assertEqual((a.uid, a.broker_uid), (Uid(0, 0), Uid(0, 0)))  # 9.1.5: zero UIDs for EPT
        ctrl = RdmnetController()
        await ctrl.connect("127.0.0.1", self.b.port)
        self.clients.append(ctrl)
        await ctrl.client_list()
        b = await self.ept()
        await wait_until(lambda: b.cid in a.clients)
        self.assertEqual(set(b.clients), {a.cid, b.cid})
        self.assertTrue(all(e.protocol == C.E133_CLIENT_PROTOCOL_RPT for e in ctrl.clients.values()))
        self.assertEqual(len(ctrl.clients), 1)  # only itself, no EPT entries
        self.assertEqual({c["type"] for c in self.b.status()["clients"]}, {"EPT", "Controller"})
        await b.disconnect()
        await wait_until(lambda: b.cid not in a.clients)

    async def test_forward_data(self):
        a, b = await self.ept(), await self.ept()
        await a.send_data(b.cid, PROTO, b"hello")
        msg = await asyncio.wait_for(b.received.get(), 2)
        self.assertEqual((msg.sender_cid, msg.protocol_vector, msg.payload), (a.cid, PROTO, b"hello"))
        self.assertEqual(self.b.counters["ept_forwarded"], 1)

    async def test_one_to_many_with_inheritance(self):
        a, b, c = await self.ept(), await self.ept(), await self.ept()
        await a.send_data([b.cid, c.cid, a.cid], PROTO, b"to all")
        for r in (b, c, a):
            msg = await asyncio.wait_for(r.received.get(), 2)
            self.assertEqual((msg.sender_cid, msg.payload), (a.cid, b"to all"))
        self.assertEqual(self.b.counters["ept_forwarded"], 3)

    async def test_unknown_cid_and_broker_cid(self):
        a = await self.ept()
        missing = uuid.uuid4()
        await a.send_data([missing, self.b.config.cid], PROTO, b"x")
        await wait_until(lambda: len(a.statuses) == 2)
        senders = {s for s, _ in a.statuses}
        self.assertEqual(senders, {self.b.config.cid})
        self.assertEqual([st.cid for _, st in a.statuses], [missing, self.b.config.cid])
        self.assertTrue(all(st.code == C.VECTOR_EPT_STATUS_UNKNOWN_CID for _, st in a.statuses))

    async def test_unknown_vector(self):
        a, b = await self.ept(), await self.ept()
        await a._v.send_raw(E.build_ept_packet(a.cid, E.build_ept_pdu(0x99, b.cid, b"")))
        await wait_until(lambda: a.statuses)
        _, st = a.statuses[0]
        self.assertEqual((st.code, st.vector), (C.VECTOR_EPT_STATUS_UNKNOWN_VECTOR, 0x99))
        self.assertTrue(b.received.empty())

    async def test_spoofed_sender_and_malformed_dropped(self):
        a, b = await self.ept(), await self.ept()
        await a._v.send_raw(E.build_ept_packet(uuid.uuid4(), E.build_ept_pdu(
            C.VECTOR_EPT_DATA, b.cid, E.build_ept_data(PROTO, b"fake"))))
        await a._v.send_raw(E.build_ept_packet(a.cid, bytes.fromhex("200013") + b.cid.bytes))
        await wait_until(lambda: self.b.counters["ept_malformed"] == 1)
        self.assertEqual(self.b.counters["ept_wrong_source"], 1)
        self.assertTrue(b.received.empty())
        self.assertTrue(a.connected)

    async def test_protocol_mismatch_dropped(self):
        a = await self.ept()
        ctrl = RdmnetController()
        await ctrl.connect("127.0.0.1", self.b.port)
        self.clients.append(ctrl)
        await ctrl._v.send_raw(E.build_ept_packet(ctrl.cid, E.build_ept_pdu(
            C.VECTOR_EPT_DATA, a.cid, E.build_ept_data(PROTO, b"x"))))
        await wait_until(lambda: self.b.counters["root_protocol_mismatch"] == 1)
        self.assertTrue(a.received.empty())

    async def test_invalid_entries_rejected(self):
        c = E.EptClient([])  # no sub-protocol
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await c.connect("127.0.0.1", self.b.port)
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_INVALID_CLIENT_ENTRY)
        a = await self.ept()
        twin = E.EptClient(SUB, cid=a.cid)  # same CID twice
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await twin.connect("127.0.0.1", self.b.port)
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_INVALID_CLIENT_ENTRY)

    async def test_same_cid_for_rpt_and_ept(self):
        a = await self.ept()
        dev = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x7FF0, 0x55), cid=a.cid)  # 9.1.6: CID + protocol
        await dev.connect("127.0.0.1", self.b.port)
        self.clients.append(dev)
        self.assertIn(Uid(0x7FF0, 0x55), self.b._clients)

    async def test_capacity_counts_ept(self):
        for _ in range(6):
            await self.ept()
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await self.ept()
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_CAPACITY_EXCEEDED)

    async def test_entry_update_changes_protocols(self):
        a, b = await self.ept(), await self.ept()
        new = [P.EptSubProtocol(PROTO, "v2"), P.EptSubProtocol(0x7FF00002, "extra")]
        await b.update_protocols(new)
        await wait_until(lambda: len(a.clients[b.cid].ept_protocols) == 2)
        self.assertEqual(self.b._ept_clients[b.cid].entry.ept_protocols, tuple(new))
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await b.update_protocols([])
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_INVALID_CLIENT_ENTRY)
        await wait_until(lambda: b.cid not in a.clients)


if __name__ == "__main__":
    unittest.main()
