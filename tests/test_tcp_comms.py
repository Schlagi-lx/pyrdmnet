"""TCP_COMMS_STATUS (E1.33 7.6.3) and the default responders of controllers and devices (7.2.1, 7.4.8)."""

import asyncio
import struct
import time
import unittest

from pyrdmnet import constants as C
from pyrdmnet import rdm_params as R
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.broker_responder import BrokerResponder, BrokerResponderInfo
from pyrdmnet.client_responder import PRODUCT_CATEGORY_CONTROL_CONTROLLER
from pyrdmnet.controller import RdmNackError, RdmnetController, RptStatusError
from pyrdmnet.rdm import RdmMessage, Uid
from pyrdmnet.simulator import SimulatedGateway

from helpers import NetworkTest

REQUIRED_DEFAULT_RESPONDER_PIDS = {  # E1.33 table 7-1 plus table A-15 for controllers and devices
    C.PID_IDENTIFY_DEVICE, C.PID_SUPPORTED_PARAMETERS, C.PID_PARAMETER_DESCRIPTION, C.PID_MANUFACTURER_LABEL,
    C.PID_DEVICE_MODEL_DESCRIPTION, C.PID_SOFTWARE_VERSION_LABEL, C.PID_DEVICE_LABEL,
    C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_TCP_COMMS_STATUS,
}


def pid_list(data: bytes) -> set[int]:
    return {struct.unpack(">H", data[i:i + 2])[0] for i in range(0, len(data), 2)}


async def wait_until(condition, timeout_s=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class TestEncoding(unittest.TestCase):
    def test_entry(self):
        e = R.TcpCommsEntry("lab", "192.168.1.10", bytes(16), 8888, 3)
        raw = R.build_tcp_comms_entry(e)
        self.assertEqual(len(raw), 0x57)
        self.assertEqual(raw[:4], b"lab\x00")
        self.assertEqual(raw[63:67], bytes((192, 168, 1, 10)))
        self.assertEqual(raw[83:], struct.pack(">HH", 8888, 3))
        self.assertEqual(R.read_tcp_comms_entry(raw), e)
        self.assertEqual(e.broker_address, "192.168.1.10:8888")
        v6 = R.TcpCommsEntry("lab", broker_ipv6=bytes(15) + b"\x01", broker_port=8888)
        self.assertEqual(R.read_tcp_comms_entry(R.build_tcp_comms_entry(v6)).broker_address, "[::1]:8888")
        self.assertIsNone(R.TcpCommsEntry("lab").broker_address)
        self.assertEqual(R.read_tcp_comms_entries(raw + raw), [e, e])
        self.assertEqual(R.read_tcp_comms_entry(R.build_tcp_comms_entry(
            R.TcpCommsEntry("lab", unhealthy_events=70000))).unhealthy_events, 0xFFFF)  # 16 bit counter
        with self.assertRaises(R.RdmFormatError):
            R.read_tcp_comms_entries(raw[:-1])
        with self.assertRaises(R.RdmFormatError):
            R.build_scope_string("")


class TestDeviceTcpComms(NetworkTest):
    async def test_read_and_reset(self):
        (e,) = await self.c.read_tcp_comms_status(self.gateway.uid)
        self.assertEqual((e.scope, e.broker_address, e.unhealthy_events),
                         ("default", f"127.0.0.1:{self.port}", 0))
        self.gateway.unhealthy_events["default"] = 4
        self.assertEqual((await self.c.read_tcp_comms_status(self.gateway.uid))[0].unhealthy_events, 4)
        self.c.write_enabled = True
        with self.assertRaises(RdmNackError) as ctx:
            await self.c.reset_tcp_comms_status(self.gateway.uid, "other")
        self.assertEqual(ctx.exception.reason, C.NR_UNKNOWN_SCOPE)
        await self.c.reset_tcp_comms_status(self.gateway.uid)
        self.assertEqual((await self.c.read_tcp_comms_status(self.gateway.uid))[0].unhealthy_events, 0)

    async def test_default_responder_pids(self):
        pids = pid_list(await self.c.rdm_get(self.gateway.uid, C.PID_SUPPORTED_PARAMETERS))
        self.assertLessEqual(REQUIRED_DEFAULT_RESPONDER_PIDS, pids)
        self.assertIn(C.PID_ENDPOINT_LIST, pids)  # gateway PIDs stay
        with self.assertRaises(RdmNackError) as ctx:
            await self.c.rdm_get(self.gateway.uid, C.PID_PARAMETER_DESCRIPTION, b"\x80\x00")
        self.assertEqual(ctx.exception.reason, C.NR_DATA_OUT_OF_RANGE)
        cs = R.read_component_scope(await self.c.rdm_get(self.gateway.uid, C.PID_COMPONENT_SCOPE, b"\x00\x01"))
        self.assertEqual((cs.scope, cs.static_address), ("default", None))
        self.assertEqual(R.read_search_domain(await self.c.rdm_get(self.gateway.uid, C.PID_SEARCH_DOMAIN)), "local.")


class TestNativeDeviceTcpComms(NetworkTest):
    native = True

    async def test_read(self):
        (e,) = await self.c.read_tcp_comms_status(self.gateway.uid)
        self.assertEqual(e.broker_address, f"127.0.0.1:{self.port}")
        pids = pid_list(await self.c.rdm_get(self.gateway.uid, C.PID_SUPPORTED_PARAMETERS))
        self.assertLessEqual(REQUIRED_DEFAULT_RESPONDER_PIDS, pids)
        self.assertIn(C.PID_DMX_START_ADDRESS, pids)


class TestUnhealthyEvent(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_timeout_counts(self):
        b = RdmnetBroker(BrokerConfig(port=0))
        await b.start()
        g = SimulatedGateway(heartbeat_interval_s=0.3)
        g.reconnect_min_s = 0.05
        c = RdmnetController()
        try:
            await g.connect("127.0.0.1", b.port, auto_reconnect=True)
            await c.connect("127.0.0.1", b.port)
            g._v.last_received = time.monotonic() - 3600  # the broker seems silent: connection unhealthy
            await wait_until(lambda: g.reconnect_count == 1 and g.connected)
            self.assertEqual(g.unhealthy_events, {"default": 1})
            await c.client_list()
            (e,) = await c.read_tcp_comms_status(g.uid)
            self.assertEqual(e.unhealthy_events, 1)
        finally:
            await c.disconnect()
            await g.disconnect()
            await b.stop()


class TestControllerDefaultResponder(unittest.IsolatedAsyncioTestCase):
    """A second controller addresses the first one (E1.33 7.4.8/7.4.9); the broker forwards it (9.2.7)."""

    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0, scope="lab"))
        await self.b.start()
        self.target = RdmnetController(scope="lab")
        await self.target.connect("127.0.0.1", self.b.port)
        self.c = RdmnetController(scope="lab")
        await self.c.connect("127.0.0.1", self.b.port)

    async def asyncTearDown(self):
        await self.c.disconnect()
        await self.target.disconnect()
        await self.b.stop()

    async def test_required_pids(self):
        t = self.target.uid
        self.assertLessEqual(REQUIRED_DEFAULT_RESPONDER_PIDS,
                             pid_list(await self.c.rdm_get(t, C.PID_SUPPORTED_PARAMETERS)))
        di = R.read_device_info(await self.c.rdm_get(t, C.PID_DEVICE_INFO))
        self.assertEqual(di.product_category, PRODUCT_CATEGORY_CONTROL_CONTROLLER)
        self.assertEqual(await self.c.rdm_get(t, C.PID_MANUFACTURER_LABEL), b"pyrdmnet")
        self.assertTrue((await self.c.rdm_get(t, C.PID_SOFTWARE_VERSION_LABEL)).startswith(b"pyrdmnet "))
        self.assertEqual(R.read_component_scope(await self.c.rdm_get(t, C.PID_COMPONENT_SCOPE, b"\x00\x01")).scope,
                         "lab")
        (e,) = await self.c.read_tcp_comms_status(t)
        self.assertEqual((e.scope, e.broker_address), ("lab", f"127.0.0.1:{self.b.port}"))
        with self.assertRaises(RdmNackError):
            await self.c.rdm_get(t, C.PID_DMX_START_ADDRESS)

    async def test_set_label_and_identify(self):
        t = self.target.uid
        self.c.write_enabled = True
        await self.c.rdm_set(t, C.PID_DEVICE_LABEL, b"Desk 2")
        self.assertEqual(await self.c.rdm_get(t, C.PID_DEVICE_LABEL), b"Desk 2")
        await self.c.rdm_set(t, C.PID_IDENTIFY_DEVICE, b"\x01")
        self.assertTrue(self.target.responder.identifying)

    async def test_wrong_endpoint_and_rdm_uid(self):
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(self.target.uid, C.PID_DEVICE_INFO, endpoint=1)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_ENDPOINT)
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(self.target.uid, C.PID_DEVICE_INFO, rdm_dest=Uid(0x7FF0, 0x55))
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_RDM_UID)


class TestScopeChangeViaRdm(unittest.IsolatedAsyncioTestCase):
    """SET COMPONENT_SCOPE over RDMnet (7.6.1): the device leaves its broker and joins the new scope."""

    async def test_move_device_to_other_broker(self):
        first = RdmnetBroker(BrokerConfig(port=0, scope="default"))
        second = RdmnetBroker(BrokerConfig(port=0, scope="stage"))
        await first.start()
        await second.start()
        g = SimulatedGateway()
        g.reconnect_min_s = 0.05
        c = RdmnetController()
        try:
            await g.connect("127.0.0.1", first.port, auto_reconnect=True)
            await c.connect("127.0.0.1", first.port)
            await c.client_list()
            c.write_enabled = True
            new = R.ComponentScope(1, "stage", C.E133_STATIC_CONFIG_IPV4, "127.0.0.1", bytes(16), second.port)
            await c.rdm_set(g.uid, C.PID_COMPONENT_SCOPE, R.build_component_scope(new))
            await wait_until(lambda: g.uid in second._clients)
            self.assertEqual((g.scope, g.target, g.static_broker),
                             ("stage", ("127.0.0.1", second.port), ("127.0.0.1", second.port)))
            self.assertTrue(any(e["event"] == "client_disconnected" for e in first.events)
                            or g.uid not in first._clients)
            cs = g.component_scope()
            self.assertEqual(cs.static_address, f"127.0.0.1:{second.port}")
            # invalid static configurations are rejected
            for bad, reason in (
                    (R.ComponentScope(1, "x", 7), C.NR_INVALID_STATIC_CONFIG_TYPE),
                    (R.ComponentScope(1, "x", C.E133_STATIC_CONFIG_IPV4, "0.0.0.0", bytes(16), 1),
                     C.NR_INVALID_IPV4_ADDRESS),
                    (R.ComponentScope(1, "x", C.E133_STATIC_CONFIG_IPV6, static_port=1), C.NR_INVALID_IPV6_ADDRESS),
                    (R.ComponentScope(1, "x", C.E133_STATIC_CONFIG_IPV4, "10.0.0.1", bytes(16), 0),
                     C.NR_INVALID_PORT),
                    (R.ComponentScope(2, "x"), C.NR_DATA_OUT_OF_RANGE)):
                n = g.client_responder.respond_e133(RdmMessage(g.uid, c.uid, 1, 1, C.CC_SET, C.PID_COMPONENT_SCOPE,
                                                               R.build_component_scope(bad)))
                self.assertEqual((n.response_type, struct.unpack(">H", n.data)[0]), (C.RESPONSE_NACK, reason))
            self.assertEqual(g.scope, "stage")
        finally:
            await c.disconnect()
            await g.disconnect()
            await first.stop()
            await second.stop()

    async def test_search_domain(self):
        g = SimulatedGateway()
        n = g.client_responder.respond_e133(RdmMessage(g.requested_uid, Uid(1, 2), 1, 1, C.CC_SET,
                                                       C.PID_SEARCH_DOMAIN, b"studio."))
        self.assertEqual(n.response_type, C.RESPONSE_ACK)
        self.assertEqual(g.search_domain, "studio.")


class TestBrokerDefaultResponder(unittest.TestCase):
    def test_lists_required_pids_and_category(self):
        r = BrokerResponder(BrokerResponderInfo(uid=Uid(0x7FF0, 1), scope="lab"))

        def get(pid, data=b""):
            return r.respond(RdmMessage(Uid(0x7FF0, 1), Uid(0x7FF0, 9), 1, 1, C.CC_GET, pid, data))
        pids = pid_list(get(C.PID_SUPPORTED_PARAMETERS).data)
        self.assertLessEqual({C.PID_SUPPORTED_PARAMETERS, C.PID_PARAMETER_DESCRIPTION, C.PID_DEVICE_INFO,
                              C.PID_SOFTWARE_VERSION_LABEL, C.PID_IDENTIFY_DEVICE, C.PID_BROKER_STATUS}, pids)
        n = get(C.PID_PARAMETER_DESCRIPTION, b"\x80\x00")
        self.assertEqual(struct.unpack(">H", n.data)[0], C.NR_DATA_OUT_OF_RANGE)
        self.assertEqual(R.read_device_info(get(C.PID_DEVICE_INFO).data).product_category, 0x0801)


if __name__ == "__main__":
    unittest.main()
