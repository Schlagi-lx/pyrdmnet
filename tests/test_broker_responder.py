"""Broker-eigener RDM-Responder (E1.33 6.2): Anfragen an die Broker-UID."""

import asyncio
import struct
import unittest

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet import rdm_params as R
from pyrdmnet.broker_responder import BrokerResponder, BrokerResponderInfo, build_component_scope
from pyrdmnet.controller import RdmNackError, RptStatusError, WriteLockError
from pyrdmnet.rdm import RdmMessage, Uid

from helpers import NetworkTest

BROKER = Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 1)
CONTROLLER = Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x99)


def request(pid, data=b"", cc=C.CC_GET, dest=BROKER, subdevice=C.RDM_ROOT_DEVICE):
    return RdmMessage(dest=dest, source=CONTROLLER, transaction=7, port_or_response_type=1,
                        command_class=cc, pid=pid, data=data, subdevice=subdevice)


class TestResponderDirect(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.r = BrokerResponder(BrokerResponderInfo(uid=BROKER, scope="lab", search_domain="local."),
                                 event=lambda kind, **kw: self.events.append((kind, kw)))

    def _ack(self, a):
        n = self.r.respond(a)
        self.assertEqual(n.response_type, C.RESPONSE_ACK, n)
        self.assertEqual((n.dest, n.source, n.transaction), (CONTROLLER, BROKER, 7))
        return n.data

    def _nack(self, a):
        n = self.r.respond(a)
        self.assertEqual(n.response_type, C.RESPONSE_NACK)
        return struct.unpack(">H", n.data)[0]

    def test_component_scope(self):
        d = self._ack(request(C.PID_COMPONENT_SCOPE, b"\x00\x01"))
        self.assertEqual(len(d), 2 + 63 + 1 + 4 + 16 + 2)
        self.assertEqual(d[:2], b"\x00\x01")
        self.assertEqual(d[2:65].rstrip(b"\x00"), b"lab")
        self.assertEqual(d[64], 0)  # nullterminiert
        self.assertEqual(d[65], C.E133_NO_STATIC_CONFIG)
        self.assertEqual(self._nack(request(C.PID_COMPONENT_SCOPE, b"\x00\x02")), C.NR_DATA_OUT_OF_RANGE)
        self.assertEqual(self._nack(request(C.PID_COMPONENT_SCOPE)), C.NR_FORMAT_ERROR)

    def test_long_scope_truncated(self):
        d = build_component_scope(1, "x" * 80)
        self.assertEqual(d[2:65], b"x" * 62 + b"\x00")

    def test_search_domain_and_broker_status(self):
        self.assertEqual(self._ack(request(C.PID_SEARCH_DOMAIN)), b"local.")
        self.assertEqual(self._ack(request(C.PID_BROKER_STATUS)), bytes((0, C.E133_BROKER_ACTIVE)))

    def test_base_pids(self):
        pids = self._ack(request(C.PID_SUPPORTED_PARAMETERS))
        group = [struct.unpack(">H", pids[i:i + 2])[0] for i in range(0, len(pids), 2)]
        for p in (C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_BROKER_STATUS, C.PID_MANUFACTURER_LABEL):
            self.assertIn(p, group)
        di = R.read_device_info(self._ack(request(C.PID_DEVICE_INFO)))
        self.assertEqual((di.footprint, di.start_address, di.sensor_count), (0, 0xFFFF, 0))
        self.assertTrue(self._ack(request(C.PID_SOFTWARE_VERSION_LABEL)).startswith(b"pyrdmnet"))
        self.assertEqual(self._nack(request(C.PID_DMX_START_ADDRESS)), C.NR_UNKNOWN_PID)

    def test_set_identify_only(self):
        self.assertEqual(self._nack(request(C.PID_COMPONENT_SCOPE, b"x", cc=C.CC_SET)),
                         C.NR_UNSUPPORTED_COMMAND_CLASS)
        self.assertEqual(self._nack(request(C.PID_DMX_START_ADDRESS, b"\x00\x01", cc=C.CC_SET)), C.NR_UNKNOWN_PID)
        self._ack(request(C.PID_IDENTIFY_DEVICE, b"\x01", cc=C.CC_SET))
        self.assertEqual(self._ack(request(C.PID_IDENTIFY_DEVICE)), b"\x01")
        self.assertEqual(self.events[-1][0], "broker_identify")
        self.assertEqual(self._nack(request(C.PID_IDENTIFY_DEVICE, b"\x02", cc=C.CC_SET)), C.NR_FORMAT_ERROR)

    def test_subdevice_and_broadcast(self):
        self.assertEqual(self._nack(request(C.PID_DEVICE_INFO, subdevice=1)), C.NR_SUB_DEVICE_OUT_OF_RANGE)
        self.assertIsNone(self.r.respond(request(C.PID_DEVICE_INFO, dest=Uid(0xFFFF, 0xFFFFFFFF))))


class TestResponderOverNetwork(NetworkTest):
    broker_config = {"scope": "default", "search_domain": "local."}

    async def test_get_over_rdmnet(self):
        b = self.broker.config.uid
        self.assertEqual(self.c.broker_uid, b)
        d = await self.c.rdm_get(b, C.PID_COMPONENT_SCOPE, b"\x00\x01", timeout_s=2)
        self.assertEqual(d[2:65].rstrip(b"\x00"), b"default")
        self.assertEqual(await self.c.rdm_get(b, C.PID_SEARCH_DOMAIN, timeout_s=2), b"local.")
        self.assertEqual(await self.c.rdm_get(b, C.PID_BROKER_STATUS, timeout_s=2), b"\x00\x01")
        self.assertEqual(self.broker.counters["broker_rdm_request"], 3)

    async def test_nack_and_status(self):
        b = self.broker.config.uid
        with self.assertRaises(RdmNackError) as ctx:
            await self.c.rdm_get(b, C.PID_DMX_PERSONALITY, timeout_s=2)
        self.assertEqual(ctx.exception.reason, C.NR_UNKNOWN_PID)
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(b, C.PID_DEVICE_INFO, endpoint=1, timeout_s=2)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_ENDPOINT)
        with self.assertRaises(RptStatusError) as ctx:
            await self.c.rdm_get(b, C.PID_DEVICE_INFO, rdm_dest=Uid(0x7FF0, 0x1234), timeout_s=2)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_UNKNOWN_RDM_UID)

    async def test_identify_needs_write_permission(self):
        b = self.broker.config.uid
        with self.assertRaises(WriteLockError):
            await self.c.rdm_set(b, C.PID_IDENTIFY_DEVICE, b"\x01", timeout_s=2)
        self.c.write_enabled = True
        await self.c.rdm_set(b, C.PID_IDENTIFY_DEVICE, b"\x01", timeout_s=2)
        self.assertTrue(self.broker.responder.identifying)

    async def test_multiple_rdm_commands_invalid(self):
        b = self.broker.config.uid
        a = request(C.PID_DEVICE_INFO, dest=b).encode()
        header = P.RptHeader(C.VECTOR_RPT_REQUEST, self.c.uid, 0, b, 0, 4711)
        fut = self.c._pending[4711] = asyncio.get_running_loop().create_future()
        cmds = P.build_rdm_cmd_pdu(a) * 2  # E1.33: genau ein RDM-Befehl je Request erlaubt
        data = P.flags_length(7 + len(cmds)) + struct.pack(">I", C.VECTOR_REQUEST_RDM_CMD) + cmds
        await self.c.send_rpt(header, data)
        with self.assertRaises(RptStatusError) as ctx:
            await asyncio.wait_for(fut, 2)
        self.assertEqual(ctx.exception.code, C.RPT_STATUS_INVALID_MESSAGE)


if __name__ == "__main__":
    unittest.main()
