"""Simulated gateway as LLRP target (TODO 3b): find and reconfigure a device that cannot reach its broker."""

import asyncio
import time
import unittest

from pyrdmnet import constants as C
from pyrdmnet import rdm_params as R
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.client import RdmnetConnectionError
from pyrdmnet.llrp import LlrpManager
from pyrdmnet.simulator import SimulatedGateway

from test_llrp import network


async def wait_until(condition, timeout_s=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class SimulatorLlrpTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.network = network(False)
        self.b = RdmnetBroker(BrokerConfig(port=0))
        await self.b.start()
        self.m = LlrpManager(self.network, timeout_window_s=0.15, retries=0)
        await self.m.start()
        self.m.write_enabled = True
        self.cleanup: list = [self.m.stop, self.b.stop]

    async def asyncTearDown(self):
        for stop in reversed(self.cleanup):
            await stop()

    async def gateway(self, scope="default") -> SimulatedGateway:
        g = SimulatedGateway(scope=scope)
        g.reconnect_min_s = 0.05
        target = await g.start_llrp(self.network)
        target.max_backoff_s = 0.03
        self.cleanup.append(g.disconnect)
        return g


class TestWrongScope(SimulatorLlrpTest):
    async def test_fix_scope_via_llrp(self):
        g = await self.gateway(scope="wrong")
        self.assertIsNone(await g.connect("127.0.0.1", self.b.port, keep_trying=True))  # broker rejects the scope
        self.assertFalse(g.connected)
        self.assertIn("SCOPE", g.last_connect_error.upper())
        (z,) = await self.m.discover()  # not connected: found with the default filter
        self.assertEqual((z.uid, z.cid, z.type), (g.requested_uid, g.cid, C.LLRP_TYPE_DEVICE))
        self.assertEqual((await self.m.read_scope(z)).scope, "wrong")
        await asyncio.sleep(0.3)  # the client is now in a back-off pause of 1 s or more
        start = time.monotonic()
        await self.m.set_scope(z, "default")
        await wait_until(lambda: g.connected)
        self.assertLess(time.monotonic() - start, 1.0)  # woken up instead of waiting for the back-off
        self.assertIn(g.uid, self.b._clients)

    async def test_static_broker_via_llrp(self):
        other = RdmnetBroker(BrokerConfig(port=0, scope="stage"))
        await other.start()
        self.cleanup.append(other.stop)
        g = await self.gateway(scope="stage")
        self.assertIsNone(await g.connect("127.0.0.1", self.b.port, keep_trying=True))  # broker of another scope
        (z,) = await self.m.discover()
        await self.m.set_scope(z, "stage", static_broker=("127.0.0.1", other.port))
        await wait_until(lambda: g.connected and g.uid in other._clients)
        self.assertEqual(g.target, ("127.0.0.1", other.port))


class TestConnectedDevice(SimulatorLlrpTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.g = await self.gateway()
        await self.g.connect("127.0.0.1", self.b.port, auto_reconnect=True)

    async def test_filter_and_reading(self):
        self.assertEqual(await self.m.discover(), [])  # connected clients do not answer the default probe
        (z,) = await self.m.discover(filter=0)
        self.assertEqual(z.uid, self.g.uid)
        pids = await self.m.rdm_get(z, C.PID_SUPPORTED_PARAMETERS)
        found = {int.from_bytes(pids[i:i + 2], "big") for i in range(0, len(pids), 2)}
        self.assertLessEqual({C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_TCP_COMMS_STATUS}, found)
        (e,) = R.read_tcp_comms_entries(await self.m.rdm_get(z, C.PID_TCP_COMMS_STATUS))
        self.assertEqual(e.broker_address, f"127.0.0.1:{self.b.port}")
        self.assertEqual(await self.m.set_search_domain(z, "studio."), ("local.", "studio."))
        self.assertEqual(self.g.search_domain, "studio.")

    async def test_scope_change_disconnects_with_llrp_reason(self):
        (z,) = await self.m.discover(filter=0)
        await self.m.set_scope(z, "stage")  # no broker for "stage" here: the device keeps trying
        await wait_until(lambda: any(e["event"] == "client_disconnect" for e in self.b.events))
        event = next(e for e in self.b.events if e["event"] == "client_disconnect")
        self.assertEqual(event["reason"], "LLRP_RECONFIGURE")
        await wait_until(lambda: not self.g.connected)
        self.assertEqual(self.g.scope, "stage")
        self.assertTrue(self.g.auto_reconnecting)


class TestKeepTrying(unittest.IsolatedAsyncioTestCase):
    async def test_first_connect_fails_then_broker_appears(self):
        b = RdmnetBroker(BrokerConfig(port=0))
        await b.start()
        port = b.port
        await b.stop()
        g = SimulatedGateway()
        g.reconnect_min_s = 0.05
        try:
            self.assertIsNone(await g.connect("127.0.0.1", port, keep_trying=True, timeout_s=2))
            self.assertTrue(g.auto_reconnecting)
            b = RdmnetBroker(BrokerConfig(port=port))
            await b.start()
            await g.wait_connected(5)
            self.assertIn(g.uid, b._clients)
        finally:
            await g.disconnect()
            await b.stop()

    async def test_without_keep_trying_first_connect_raises(self):
        with self.assertRaises(RdmnetConnectionError):
            await SimulatedGateway().connect("127.0.0.1", 1, auto_reconnect=True, timeout_s=2)


if __name__ == "__main__":
    unittest.main()
