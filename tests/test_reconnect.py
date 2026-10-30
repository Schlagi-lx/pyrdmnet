"""Automatic reconnection (E1.33 6.2.5): broker restart, back-off, redirect, facade."""

import asyncio
import socket
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.client import RdmnetConnectionError, RptClient
from pyrdmnet.controller import RdmnetController
from pyrdmnet.facade import RdmConfig, RdmModule
from pyrdmnet.rdm import Uid
from pyrdmnet.simulator import SimulatedGateway

FAST = 0.05


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_until(condition, timeout_s=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


def fast(client):
    client.reconnect_min_s = FAST
    return client


class TestBackoff(unittest.IsolatedAsyncioTestCase):
    """The back-off algorithm of 6.2.5.3.1, with a scripted connect and recorded sleeps."""

    async def run_script(self, results, step):
        c = RptClient(C.RPT_CLIENT_TYPE_DEVICE, Uid(0x7FF0, 1))
        c.backoff_step_s = (step, step)
        sleeps: list[float] = []
        script = list(results)

        async def sleep(t):
            sleeps.append(t)

        async def connect():
            r = script.pop(0)
            if r == "ok":
                c.disconnected.clear()
                return Uid(0x7FF0, 1)
            raise RdmnetConnectionError("failed", established=(r == "established"))
        c._sleep, c._connect_following_redirects = sleep, connect
        c.target = ("127.0.0.1", 1)
        c.disconnected.set()
        task = asyncio.create_task(c._supervise())
        await wait_until(lambda: not script)
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return c, sleeps

    async def test_growth_and_tcp_failures(self):
        c, sleeps = await self.run_script(["established", "established", "established", "tcp", "ok"], step=2)
        # attempts 1 and 2 immediate; back-off 2, 4, 6; a TCP failure does not grow it
        self.assertEqual(sleeps, [4, 6, 6])
        self.assertEqual(c.reconnect_count, 1)
        self.assertFalse(c.reconnecting)

    async def test_cap_at_30_s(self):
        _, sleeps = await self.run_script(["established"] * 4 + ["ok"], step=20)
        self.assertEqual(sleeps, [30, 30, 30])

    async def test_tcp_failures_use_minimum_interval(self):
        _, sleeps = await self.run_script(["tcp"] * 4 + ["ok"], step=3)
        self.assertEqual(sleeps, [1.0, 1.0, 1.0])  # reconnect_min_s default


class TestBrokerRestart(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = free_port()
        self.b = RdmnetBroker(BrokerConfig(port=self.port))
        await self.b.start()
        self.c = fast(RdmnetController())
        self.changes: list[bool] = []
        self.c.on_connection_change = lambda connected, text: self.changes.append(connected)
        self.g = fast(SimulatedGateway())
        await self.g.connect("127.0.0.1", self.port, auto_reconnect=True)
        await self.c.connect("127.0.0.1", self.port, auto_reconnect=True)
        await self.c.client_list()

    async def asyncTearDown(self):
        await self.c.disconnect()
        await self.g.disconnect()
        await self.b.stop()

    async def restart_broker(self, down_s=0.0):
        await self.b.stop()
        await wait_until(lambda: not self.c.connected and not self.g.connected)
        await asyncio.sleep(down_s)
        self.b = RdmnetBroker(BrokerConfig(port=self.port))
        await self.b.start()

    async def test_controller_and_gateway_reconnect(self):
        await self.restart_broker()
        await self.c.wait_connected(5)
        await wait_until(lambda: self.g.connected)
        await wait_until(lambda: self.g.uid in self.c.clients)  # client list fetched again / incremental add
        data = await self.c.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=2)
        self.assertEqual(len(data), 19)
        self.assertEqual((self.c.reconnect_count, self.g.reconnect_count), (1, 1))
        self.assertEqual(self.changes, [True, False, True])

    async def test_broker_down_for_a_while(self):
        await self.restart_broker(down_s=0.4)  # several refused TCP connects meanwhile
        await self.c.wait_connected(5)
        self.assertTrue(self.c.connected)
        self.assertEqual(self.c.last_connect_error, "")

    async def test_requests_during_outage_fail_fast(self):
        await self.b.stop()
        await wait_until(lambda: not self.c.connected)
        with self.assertRaises(RdmnetConnectionError):
            await self.c.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=2)
        with self.assertRaises(RdmnetConnectionError):
            await self.c.wait_connected(0.2)
        self.b = RdmnetBroker(BrokerConfig(port=self.port))
        await self.b.start()

    async def test_disconnect_stops_reconnection(self):
        await self.c.disconnect()
        self.assertFalse(self.c.auto_reconnecting)
        await self.restart_broker()
        await asyncio.sleep(0.3)
        self.assertFalse(self.c.connected)

    async def test_redirect_while_connected(self):
        second = RdmnetBroker(BrokerConfig(port=0))
        await second.start()
        try:
            client = self.b._clients[self.c.uid]
            await client.connection.send_broker(C.VECTOR_BROKER_REDIRECT_V4, P.build_redirect_v4("127.0.0.1",
                                                                                                 second.port))
            await wait_until(lambda: self.c.connected and self.c.target == ("127.0.0.1", second.port))
            self.assertIn(self.c.uid, second._clients)
        finally:
            await self.c.disconnect()
            await second.stop()


class TestRedirectOnConnect(unittest.IsolatedAsyncioTestCase):
    async def test_follows_redirect(self):
        b = RdmnetBroker(BrokerConfig(port=0))
        await b.start()

        async def redirector(reader, writer):
            await P.read_tcp_block(reader)  # Client Connect
            writer.write(P.build_packet(C.VECTOR_ROOT_E133_BROKER, b.config.cid,
                                        P.build_broker(C.VECTOR_BROKER_REDIRECT_V4,
                                                       P.build_redirect_v4("127.0.0.1", b.port))))
            await writer.drain()
            writer.close()
        server = await asyncio.start_server(redirector, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        c = RdmnetController()
        try:
            await c.connect("127.0.0.1", port)
            self.assertEqual(c.target, ("127.0.0.1", b.port))
            self.assertIn(c.uid, b._clients)
        finally:
            await c.disconnect()
            server.close()
            await server.wait_closed()
            await b.stop()

    async def test_redirect_loop_gives_up(self):
        async def loop(reader, writer):
            await P.read_tcp_block(reader)
            writer.write(P.build_packet(C.VECTOR_ROOT_E133_BROKER, uuid.uuid4(),
                                        P.build_broker(C.VECTOR_BROKER_REDIRECT_V4,
                                                       P.build_redirect_v4("127.0.0.1", port))))
            await writer.drain()
            writer.close()
        server = await asyncio.start_server(loop, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            with self.assertRaises(RdmnetConnectionError):
                await RdmnetController().connect("127.0.0.1", port)
        finally:
            server.close()
            await server.wait_closed()


class TestFacadeExternalBroker(unittest.TestCase):
    """Broker and gateway run in their own thread (like an external broker); the facade must wait."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.port = free_port()
        self.broker = self.on_loop(self.start_broker())
        self.gateway = SimulatedGateway()
        self.gateway.reconnect_min_s = 0.01  # gateway back before the controller fetches the client list
        self.on_loop(self.gateway.connect("127.0.0.1", self.port, auto_reconnect=True))
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.on_loop(self.gateway.disconnect())
        self.on_loop(self.broker.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()
        self.tmp.cleanup()

    def on_loop(self, coro, timeout=10):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    async def start_broker(self):
        b = RdmnetBroker(BrokerConfig(port=self.port))
        await b.start()
        return b

    def test_facade_waits_for_reconnection(self):
        module = RdmModule(RdmConfig(internal_broker=False, broker_port=self.port,
                                     db=Path(self.tmp.name) / "m.sqlite", timeout_s=1.0))
        module.start()
        try:
            module.controller.reconnect_min_s = 0.2
            self.assertEqual(len(module.find_devices()), 1)
            module.set_write_enabled(True)
            self.on_loop(self.broker.stop())

            async def restart_later():
                await asyncio.sleep(0.5)
                return await self.start_broker()
            pending = asyncio.run_coroutine_threadsafe(restart_later(), self.loop)
            devices = module.find_devices()  # waits for the automatic reconnection
            self.broker = pending.result(5)
            self.assertEqual(len(devices), 1)
            self.assertTrue(module.controller.write_enabled)  # same controller object, permission kept
            self.assertGreaterEqual(module.controller.reconnect_count, 1)
        finally:
            module.stop()


if __name__ == "__main__":
    unittest.main()
