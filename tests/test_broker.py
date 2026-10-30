"""Broker: Connect-Prüfungen, Clientlisten, Routing, Heartbeat, Robustheit."""

import asyncio
import os
import struct
import unittest
import uuid

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController, RptStatusError
from pyrdmnet.client import RptClient, RdmnetConnectionError
from pyrdmnet.rdm import ALL_DEVICES, Uid
from pyrdmnet.simulator import SimulatedGateway

from helpers import NetworkTest


class TestConnect(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0, scope="lab", max_clients=3))
        await self.b.start()

    async def asyncTearDown(self):
        await self.b.stop()

    async def _rpt_client(self, uid, scope="lab", type=C.RPT_CLIENT_TYPE_DEVICE, cid=None):
        k = RptClient(type, uid, scope, cid=cid)
        await k.connect("127.0.0.1", self.b.port)
        return k

    async def test_ok_and_wrong_scope(self):
        k = await self._rpt_client(Uid(0x7FF0, 5))
        self.assertEqual(k.uid, Uid(0x7FF0, 5))
        self.assertEqual(k.broker_uid, self.b.config.uid)
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await self._rpt_client(Uid(0x7FF0, 6), scope="default")
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_SCOPE_MISMATCH)
        await k.disconnect()

    async def test_duplicate_and_invalid_uid(self):
        k = await self._rpt_client(Uid(0x7FF0, 5))
        for u, code in ((Uid(0x7FF0, 5), C.E133_CONNECT_DUPLICATE_UID),
                        (Uid(0, 0), C.E133_CONNECT_INVALID_UID),
                        (Uid(0x7FF0, 0xFFFFFFFF), C.E133_CONNECT_INVALID_UID),
                        (self.b.config.uid, C.E133_CONNECT_DUPLICATE_UID)):
            with self.assertRaises(RdmnetConnectionError) as ctx:
                await self._rpt_client(u)
            self.assertEqual(ctx.exception.code, code, u)
        await k.disconnect()

    async def test_dynamic_uid_kept_per_cid(self):
        cid = uuid.uuid4()
        k = await self._rpt_client(Uid(0xFFF0, 0), cid=cid)
        first = k.uid
        self.assertTrue(first.is_dynamic)
        await k.disconnect()
        await asyncio.sleep(0.05)
        k2 = await self._rpt_client(Uid(0xFFF0, 0), cid=cid)
        self.assertEqual(k2.uid, first)
        k3 = await self._rpt_client(Uid(0xFFF0, 0))
        self.assertNotEqual(k3.uid, first)
        await k2.disconnect()
        await k3.disconnect()

    async def test_capacity(self):
        ks = [await self._rpt_client(Uid(0x7FF0, i)) for i in range(10, 13)]  # 0x7ff0:00000001 ist die Broker-UID
        with self.assertRaises(RdmnetConnectionError) as ctx:
            await self._rpt_client(Uid(0x7FF0, 9))
        self.assertEqual(ctx.exception.code, C.E133_CONNECT_CAPACITY_EXCEEDED)
        for k in ks:
            await k.disconnect()

    async def test_wrong_preamble_and_garbage(self):
        for data in (b"GET / HTTP/1.0\r\n\r\n", os.urandom(200), C.ACN_TCP_PREAMBLE + struct.pack(">I", 10**7)):
            r, w = await asyncio.open_connection("127.0.0.1", self.b.port)
            w.write(data)
            await w.drain()
            self.assertEqual(await asyncio.wait_for(r.read(), 3), b"")  # Broker schließt
            w.close()
        k = await self._rpt_client(Uid(0x7FF0, 5))  # Broker läuft weiter
        await k.disconnect()
        self.assertGreaterEqual(self.b.counters["protocol_error"], 1)

    async def test_invalid_connect_content(self):
        r, w = await asyncio.open_connection("127.0.0.1", self.b.port)
        cid = uuid.uuid4()
        w.write(P.build_packet(C.VECTOR_ROOT_E133_BROKER, cid, P.build_broker(C.VECTOR_BROKER_CONNECT, b"\x00" * 20)))
        await w.drain()
        block = await asyncio.wait_for(P.read_tcp_block(r), 3)
        rep = P.read_connect_reply(P.read_broker(P.read_root_pdus(block)[0].data).data)
        self.assertEqual(rep.code, C.E133_CONNECT_INVALID_CLIENT_ENTRY)
        w.close()


class TestRouting(NetworkTest):
    async def test_client_list_and_add_remove(self):
        group = await self.c.client_list(refresh=True)
        self.assertIn(self.gateway.uid, group)
        self.assertIn(self.c.uid, group)
        second = SimulatedGateway(uid=Uid(0x7FF0, 0x777))
        await second.connect("127.0.0.1", self.port)
        for _ in range(50):
            if second.uid in self.c.clients:
                break
            await asyncio.sleep(0.02)
        self.assertIn(second.uid, self.c.clients)
        await second.disconnect()
        for _ in range(50):
            if second.uid not in self.c.clients:
                break
            await asyncio.sleep(0.02)
        self.assertNotIn(second.uid, self.c.clients)

    async def test_unknown_rpt_uid(self):
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(Uid(0x7FF0, 0xABCDEF), C.PID_DEVICE_INFO, timeout_s=2)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_RPT_UID)

    async def test_broadcast_complete(self):
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(ALL_DEVICES, C.PID_DEVICE_INFO, rdm_dest=Uid(0xFFFF, 0xFFFFFFFF), timeout_s=2)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_BROADCAST_COMPLETE)
        self.assertEqual(self.broker.counters["rpt_broadcast"], 1)

    async def test_spoofed_source_uid_dropped(self):
        header = P.RptHeader(C.VECTOR_RPT_REQUEST, Uid(0x7FF0, 0x999), 0, self.gateway.uid, 0, 1)
        await self.c._v.send_rpt(header, P.build_status(1))
        await asyncio.sleep(0.1)
        self.assertEqual(self.broker.counters["rpt_wrong_source"], 1)

    async def test_status_endpoint_unknown(self):
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(self.gateway.uid, C.PID_DEVICE_INFO, endpoint=7, rdm_dest=self.fx.uid)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_ENDPOINT)

    async def test_status_output(self):
        s = self.broker.status()
        self.assertEqual(len(s["clients"]), 2)
        self.assertEqual({c["type"] for c in s["clients"]}, {"Gerät", "Controller"})


class TestHeartbeat(unittest.IsolatedAsyncioTestCase):
    async def test_broker_drops_silent_client(self):
        b = RdmnetBroker(BrokerConfig(port=0, heartbeat_interval_s=0.2, heartbeat_timeout_s=0.6))
        await b.start()
        # Client ohne eigenen Heartbeat (sehr langes Intervall)
        k = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x7FF0, 3), heartbeat_interval_s=100, heartbeat_timeout_s=100)
        await k.connect("127.0.0.1", b.port)
        await asyncio.wait_for(k.disconnected.wait(), 5)
        self.assertEqual(b.counters["heartbeat_timeout"], 1)
        self.assertEqual(b.status()["clients"], [])
        await b.stop()

    async def test_heartbeat_keeps_connection(self):
        b = RdmnetBroker(BrokerConfig(port=0, heartbeat_interval_s=0.2, heartbeat_timeout_s=0.6))
        await b.start()
        c = RdmnetController(heartbeat_interval_s=0.2, heartbeat_timeout_s=0.6)
        await c.connect("127.0.0.1", b.port)
        await asyncio.sleep(1.5)
        self.assertTrue(c.connected)
        self.assertEqual(b.counters["heartbeat_timeout"], 0)
        await c.disconnect()
        await b.stop()

    async def test_client_detects_broker_failure(self):
        b = RdmnetBroker(BrokerConfig(port=0))
        await b.start()
        c = RdmnetController()
        await c.connect("127.0.0.1", b.port)
        await b.stop()
        await asyncio.wait_for(c.disconnected.wait(), 3)
        self.assertFalse(c.connected)


if __name__ == "__main__":
    unittest.main()
