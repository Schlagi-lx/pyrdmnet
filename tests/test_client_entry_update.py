"""Client Entry Update (E1.33 6.3.1.4, 9.1.6): Client-Eintrag ohne Neuverbindung ändern."""

import asyncio
import unittest
import uuid

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController
from pyrdmnet.client import RptClient, RdmnetConnectionError
from pyrdmnet.rdm import Uid

DEVICE = Uid(0x7FF0, 0x10)


async def wait_until(condition, timeout_s=2.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class TestClientEntryUpdate(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0))
        await self.b.start()
        self.c = RdmnetController()
        await self.c.connect("127.0.0.1", self.b.port)
        self.g = RptClient(C.RPT_CLIENT_TYPE_DEVICE, DEVICE)
        await self.g.connect("127.0.0.1", self.b.port)
        await wait_until(lambda: DEVICE in self.c.clients)

    async def asyncTearDown(self):
        for k in (self.c, self.g):
            await k.disconnect()
        await self.b.stop()

    def test_encoding_round_trip(self):
        e = P.ClientEntry(uuid.uuid4(), DEVICE, C.RPT_CLIENT_TYPE_DEVICE, uuid.uuid4())
        flags, read_back = P.read_client_entry_update(P.build_client_entry_update(1, e))
        self.assertEqual((flags, read_back), (1, e))
        with self.assertRaises(P.ProtocolError):
            P.read_client_entry_update(b"")

    async def test_change_binding_cid(self):
        bind = uuid.uuid4()
        self.assertEqual(await self.g.update_entry(binding_cid=bind), DEVICE)
        await wait_until(lambda: self.c.clients[DEVICE].binding_cid == bind)
        self.assertEqual(self.b._clients[DEVICE].entry.binding_cid, bind)
        self.assertEqual(self.b.counters["entry_update_ok"], 1)
        self.assertTrue(self.g.connected)

    async def test_uid_change_is_rekeyed(self):
        new = Uid(0x7FF0, 0x11)
        self.assertEqual(await self.g.update_entry(uid=new), new)
        self.assertEqual(self.g.uid, new)
        self.assertIn(new, self.b._clients)
        self.assertNotIn(DEVICE, self.b._clients)
        await wait_until(lambda: new in self.c.clients)
        self.assertNotIn(DEVICE, self.c.clients)  # alter Eintrag derselben CID ersetzt
        # alte UID ist wieder frei
        k = RptClient(C.RPT_CLIENT_TYPE_DEVICE, DEVICE)
        await k.connect("127.0.0.1", self.b.port)
        await k.disconnect()

    async def test_duplicate_uid_disconnects(self):
        other = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x7FF0, 0x30))
        await other.connect("127.0.0.1", self.b.port)
        self.addAsyncCleanup(other.disconnect)
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await self.g.update_entry(uid=other.uid)
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_DUPLICATE_UID)
        await asyncio.wait_for(self.g.disconnected.wait(), 2)
        await wait_until(lambda: DEVICE not in self.c.clients)
        self.assertNotIn(DEVICE, self.b._clients)
        self.assertEqual(self.b.counters["entry_update_rejected"], 1)
        self.assertIn(other.uid, self.b._clients)  # der andere bleibt unberührt

    async def test_invalid_uid_disconnects(self):
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await self.g.update_entry(uid=Uid(0x7FF0, 0xFFFFFFFF))
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_INVALID_UID)
        with self.assertRaises(RdmnetConnectionError):  # dynamische UID als feste angegeben
            await self.c.update_entry(uid=self.c.uid)

    async def test_change_cid_not_allowed(self):
        foreign = P.ClientEntry(uuid.uuid4(), DEVICE, C.RPT_CLIENT_TYPE_DEVICE)
        fut = self.g._update_reply = asyncio.get_running_loop().create_future()
        await self.g.send_broker(C.VECTOR_BROKER_CLIENT_ENTRY_UPDATE, P.build_client_entry_update(0, foreign))
        rep = await asyncio.wait_for(fut, 2)
        self.assertEqual(rep.code, C.E133_CONNECT_INVALID_CLIENT_ENTRY)
        await asyncio.wait_for(self.g.disconnected.wait(), 2)
        await wait_until(lambda: DEVICE not in self.c.clients)

    async def test_dynamic_uid_kept(self):
        d = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x8000 | 0x7FF0, 0))
        first = await d.connect("127.0.0.1", self.b.port)
        self.assertEqual(await d.update_entry(binding_cid=uuid.uuid4()), first)
        await d.disconnect()

    async def test_disable_incremental(self):
        await self.c.update_entry(incremental=False)
        self.assertFalse(self.b._clients[self.c.uid].incremental)
        k = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x7FF0, 0x20))
        await k.connect("127.0.0.1", self.b.port)
        await asyncio.sleep(0.1)
        self.assertNotIn(k.uid, self.c.clients)  # keine Client-Add-Meldung mehr
        await k.disconnect()


if __name__ == "__main__":
    unittest.main()
