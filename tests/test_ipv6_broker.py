"""IPv6 for broker connections: listening, connecting, Client Redirect IPv6 (E1.33 6.3.1.6), static IPv6 broker."""

import asyncio
import socket
import unittest

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet import rdm_params as R
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.client import RdmnetConnectionError
from pyrdmnet.controller import RdmnetController
from pyrdmnet.simulator import SimulatedGateway


def ipv6_available() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.bind(("::1", 0))
            return True
    except (OSError, AttributeError):
        return False


IPV6 = ipv6_available()


async def wait_until(condition, timeout_s=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class TestEncoding(unittest.TestCase):
    def test_redirect_v6(self):
        raw = P.build_redirect_v6("fd00::5", 8888)
        self.assertEqual(len(raw), 18)
        self.assertEqual(raw[:2], b"\xfd\x00")
        self.assertEqual(P.read_redirect_v6(raw), ("fd00::5", 8888))
        with self.assertRaises(P.ProtocolError):
            P.read_redirect_v6(raw[:-1])
        self.assertEqual(P.build_redirect("10.0.0.1", 1)[0], C.VECTOR_BROKER_REDIRECT_V4)
        vector, data = P.build_redirect("::1", 2)
        self.assertEqual((vector, P.read_redirect(vector, data)), (C.VECTOR_BROKER_REDIRECT_V6, ("::1", 2)))

    def test_format_address(self):
        self.assertEqual(P.format_address("10.0.0.1", 8888), "10.0.0.1:8888")
        self.assertEqual(P.format_address("fd00::5", 8888), "[fd00::5]:8888")

    def test_static_ipv6_component_scope(self):
        g = SimulatedGateway()
        g.set_component_scope(R.ComponentScope(1, "stage", C.E133_STATIC_CONFIG_IPV6,
                                               static_ipv6=bytes(15) + b"\x01", static_port=8888))
        self.assertEqual((g.scope, g.static_broker, g.target), ("stage", ("::1", 8888), ("::1", 8888)))
        cs = g.component_scope()
        self.assertEqual((cs.static_type, cs.static_address), (C.E133_STATIC_CONFIG_IPV6, "[::1]:8888"))


class TestRedirectV4(unittest.IsolatedAsyncioTestCase):
    async def test_broker_redirects_client(self):
        first, second = RdmnetBroker(BrokerConfig(port=0)), RdmnetBroker(BrokerConfig(port=0))
        await first.start()
        await second.start()
        c = RdmnetController()
        c.reconnect_min_s = 0.05
        try:
            await c.connect("127.0.0.1", first.port, auto_reconnect=True)
            await first.redirect_client(c.uid, "127.0.0.1", second.port)
            await wait_until(lambda: c.connected and c.target == ("127.0.0.1", second.port))
            self.assertIn(c.uid, second._clients)
            self.assertEqual(first.counters["redirect_sent"], 1)
            with self.assertRaises(KeyError):
                await first.redirect_client(c.uid, "127.0.0.1", 1)  # no longer connected there
            with self.assertRaises(ValueError):  # 6.3.1.6: IPv6 redirect only over IPv6
                await second.redirect_client(c.uid, "::1", first.port)
        finally:
            await c.disconnect()
            await first.stop()
            await second.stop()


@unittest.skipUnless(IPV6, "IPv6 loopback (::1) not available")
class TestIpv6Broker(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(host="127.0.0.1", port=0, extra_hosts=("::1",)))
        await self.b.start()

    async def asyncTearDown(self):
        await self.b.stop()

    async def test_dual_stack(self):
        self.assertEqual(self.b.addresses, [("127.0.0.1", self.b.port), ("::1", self.b.port)])
        self.assertEqual(self.b.status()["addresses"], [f"127.0.0.1:{self.b.port}", f"[::1]:{self.b.port}"])
        g = SimulatedGateway()
        c = RdmnetController()
        try:
            await g.connect("::1", self.b.port)  # device over IPv6
            await c.connect("127.0.0.1", self.b.port)  # controller over IPv4
            await c.client_list()
            self.assertEqual(len(await c.rdm_get(g.uid, C.PID_DEVICE_INFO)), 19)
            (e,) = await c.read_tcp_comms_status(g.uid)
            self.assertEqual((e.broker_ipv4, e.broker_address), ("0.0.0.0", f"[::1]:{self.b.port}"))
            peers = {x["uid"]: x["peer"] for x in self.b.status()["clients"]}
            self.assertTrue(peers[str(g.uid)].startswith("[::1]:"))
        finally:
            await c.disconnect()
            await g.disconnect()

    async def test_ipv6_only_broker(self):
        b6 = RdmnetBroker(BrokerConfig(host="::1", port=0))
        await b6.start()
        c = RdmnetController()
        try:
            await c.connect("::1", b6.port)
            self.assertTrue(c.connected)
            with self.assertRaises(RdmnetConnectionError):  # nothing listens on IPv4
                await RdmnetController().connect("127.0.0.1", b6.port, timeout_s=2)
        finally:
            await c.disconnect()
            await b6.stop()

    async def test_redirect_v6_while_connected(self):
        second = RdmnetBroker(BrokerConfig(host="::1", port=0))
        await second.start()
        c = RdmnetController()
        c.reconnect_min_s = 0.05
        try:
            await c.connect("::1", self.b.port, auto_reconnect=True)
            await self.b.redirect_client(c.uid, "::1", second.port)
            await wait_until(lambda: c.connected and c.target == ("::1", second.port))
            self.assertIn(c.uid, second._clients)
        finally:
            await c.disconnect()
            await second.stop()

    async def test_redirect_v6_during_connect(self):
        async def redirector(reader, writer):
            await P.read_tcp_block(reader)  # Client Connect
            writer.write(P.build_packet(C.VECTOR_ROOT_E133_BROKER, self.b.config.cid,
                                        P.build_broker(C.VECTOR_BROKER_REDIRECT_V6,
                                                       P.build_redirect_v6("::1", self.b.port))))
            await writer.drain()
            writer.close()
        server = await asyncio.start_server(redirector, "::1", 0)
        c = RdmnetController()
        try:
            await c.connect("::1", server.sockets[0].getsockname()[1])
            self.assertEqual(c.target, ("::1", self.b.port))
            self.assertIn(c.uid, self.b._clients)
        finally:
            await c.disconnect()
            server.close()
            await server.wait_closed()

    async def test_scope_change_to_static_ipv6_broker(self):
        stage = RdmnetBroker(BrokerConfig(host="::1", port=0, scope="stage"))
        await stage.start()
        g = SimulatedGateway()
        g.reconnect_min_s = 0.05
        c = RdmnetController()
        try:
            await g.connect("127.0.0.1", self.b.port, auto_reconnect=True)
            await c.connect("127.0.0.1", self.b.port)
            await c.client_list()
            c.write_enabled = True
            new = R.ComponentScope(1, "stage", C.E133_STATIC_CONFIG_IPV6, static_ipv6=bytes(15) + b"\x01",
                                   static_port=stage.port)
            await c.rdm_set(g.uid, C.PID_COMPONENT_SCOPE, R.build_component_scope(new))
            await wait_until(lambda: g.uid in stage._clients)
            self.assertEqual(g.target, ("::1", stage.port))
        finally:
            await c.disconnect()
            await g.disconnect()
            await stage.stop()


class TestListenErrors(unittest.IsolatedAsyncioTestCase):
    async def test_failing_extra_host_closes_first_server(self):
        b = RdmnetBroker(BrokerConfig(port=0, extra_hosts=("192.0.2.1",)))  # TEST-NET address: not on this host
        with self.assertRaises(OSError):
            await b.start()
        self.assertEqual(b._servers, [])


if __name__ == "__main__":
    unittest.main()
