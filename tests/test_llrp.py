"""LLRP (E1.33 section 5): encoding, discovery, RDM over LLRP, broker as target."""

import asyncio
import itertools
import os
import socket
import struct
import unittest
import uuid
from unittest import mock

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet import rdm_params as R
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.controller import RdmNackError, RdmTimeoutError, WriteLockError
from pyrdmnet.llrp import (BROADCAST_CID, ComponentResponder, LlrpManager, LlrpNetwork, LlrpTarget, DiscoveredTarget,
                           ProbeRequest, ProbeReply, build_llrp, build_probe_request, build_probe_reply,
                           read_llrp, read_probe_request, read_probe_reply, read_rdm_cmd)
from pyrdmnet.rdm import RdmMessage, Uid

_ports = itertools.count(20000 + (os.getpid() % 2000) * 10, 2)


def network(multicast: bool) -> LlrpNetwork:
    p = next(_ports)
    if multicast:
        return LlrpNetwork(adapter="127.0.0.1", request_port=p, response_port=p + 1)
    return LlrpNetwork(adapter="127.0.0.1", request_addr="127.0.0.1", response_addr="127.0.0.1",
                    request_port=p, response_port=p + 1, multicast=False)


def multicast_available() -> bool:
    try:
        n = network(True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", n.request_port))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                         socket.inet_aton(n.request_addr) + socket.inet_aton("127.0.0.1"))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))
            s.settimeout(0.5)
            s.sendto(b"x", (n.request_addr, n.request_port))
            return s.recv(10) == b"x"
    except OSError:
        return False


MULTICAST = multicast_available()


class TestEncoding(unittest.TestCase):
    def test_packet_round_trip(self):
        cid, target = uuid.uuid4(), uuid.uuid4()
        raw = build_llrp(cid, C.VECTOR_LLRP_PROBE_REPLY, target, 0xFFFFFFFF, b"\xf0\x00\x04\x01")
        self.assertEqual(raw[:16], C.ACN_UDP_PREAMBLE)
        p = read_llrp(raw)
        self.assertEqual((p.sender_cid, p.vector, p.dest_cid, p.transaction, p.data),
                         (cid, C.VECTOR_LLRP_PROBE_REPLY, target, 0xFFFFFFFF, b"\xf0\x00\x04\x01"))
        # lengths per table 5-1: root from octet 16, LLRP PDU from 39, data from 66
        self.assertEqual(len(build_llrp(cid, 1, target, 1, b"")), 66)
        with self.assertRaises(P.ProtocolError):
            read_llrp(b"\x00" * 70)

    def test_probe(self):
        a = ProbeRequest(Uid(0, 0), Uid(0xFFFF, 0xFFFFFFFF), C.LLRP_FILTER_BROKERS_ONLY, [Uid(1, 2), Uid(3, 4)])
        raw = build_probe_request(a)
        self.assertEqual(len(raw), 4 + 14 + 12)
        self.assertEqual(read_probe_request(raw), a)
        with self.assertRaises(P.ProtocolError):
            build_probe_request(ProbeRequest(Uid(0, 0), Uid(0, 1), 0, [Uid(1, i) for i in range(201)]))
        r = ProbeReply(Uid(0x7FF0, 9), b"\x01\x02\x03\x04\x05\x06", C.LLRP_TYPE_BROKER)
        self.assertEqual(len(build_probe_reply(r)), 17)
        self.assertEqual(read_probe_reply(build_probe_reply(r)), r)

    def test_rdm_cmd_and_component_scope(self):
        n = RdmMessage(dest=Uid(1, 2), source=Uid(3, 4), transaction=5, port_or_response_type=1,
                         command_class=C.CC_GET, pid=C.PID_COMPONENT_SCOPE, data=b"\x00\x01")
        self.assertEqual(read_rdm_cmd(P.build_rdm_cmd_pdu(n.encode())), n.encode())
        cs = R.ComponentScope(1, "lab", C.E133_STATIC_CONFIG_IPV4, "10.0.0.5", bytes(16), 8888)
        raw = R.build_component_scope(cs)
        self.assertEqual(len(raw), 88)
        self.assertEqual(R.read_component_scope(raw), cs)
        self.assertEqual(cs.static_address, "10.0.0.5:8888")


class LlrpTest(unittest.IsolatedAsyncioTestCase):
    multicast = False

    async def asyncSetUp(self):
        self.network = network(self.multicast)
        self._targets: list[LlrpTarget] = []
        self.m = LlrpManager(self.network, timeout_window_s=0.15, retries=0)
        await self.m.start()

    async def asyncTearDown(self):
        await self.m.stop()
        for t in self._targets:
            await t.stop()

    async def add_target(self, uid, type=C.LLRP_TYPE_DEVICE, connected=False, **kw):
        r = ComponentResponder(uid, **kw)
        t = LlrpTarget(uid, uuid.uuid4(), type, r, self.network, connected=lambda: connected, max_backoff_s=0.03)
        await t.start()
        self._targets.append(t)
        return t, r


class TestSingleTarget(LlrpTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.t, self.r = await self.add_target(Uid(0x7FF0, 0x42), scope="wrong")
        (self.target,) = await self.m.discover()

    async def test_found_and_read(self):
        self.assertEqual((self.target.uid, self.target.cid, self.target.type), (self.t.uid, self.t.cid, C.LLRP_TYPE_DEVICE))
        self.assertEqual(self.target.address, "127.0.0.1")
        self.assertEqual(len(self.target.hardware), 6)
        self.assertEqual((await self.m.read_scope(self.target)).scope, "wrong")
        self.assertEqual(await self.m.read_search_domain(self.target), "local.")
        with self.assertRaises(RdmNackError) as ctx:
            await self.m.rdm_get(self.target, C.PID_DMX_START_ADDRESS)
        self.assertEqual(ctx.exception.reason, C.NR_UNKNOWN_PID)

    async def test_set_scope_with_permission_and_readback(self):
        with self.assertRaises(WriteLockError):
            await self.m.set_scope(self.target, "lab")
        self.assertEqual(self.r.scope.scope, "wrong")
        self.m.write_enabled = True
        old, new = await self.m.set_scope(self.target, "lab", static_broker=("192.168.1.10", 8888))
        self.assertEqual((old.scope, new.scope, new.static_address), ("wrong", "lab", "192.168.1.10:8888"))
        self.assertEqual(self.m.protocol[-1]["old"], ("wrong", None))
        self.assertEqual(await self.m.set_search_domain(self.target, "studio."), ("local.", "studio."))
        with self.assertRaises(ValueError):
            await self.m.set_scope(self.target, "")

    async def test_set_allowed_pids_only(self):
        self.m.write_enabled = True
        with self.assertRaises(WriteLockError):
            await self.m.rdm_set(self.target, C.PID_DEVICE_LABEL, b"x")

    async def test_timeout(self):
        await self.t.stop()
        with self.assertRaises(RdmTimeoutError):
            await self.m.read_scope(self.target)

    async def test_foreign_cid_ignored(self):
        other_target = DiscoveredTarget(uuid.uuid4(), self.target.uid, self.target.hardware, self.target.type, "127.0.0.1")
        with self.assertRaises(RdmTimeoutError):
            await self.m.read_scope(other_target)

    async def test_subdevice_nack(self):
        n = RdmMessage(dest=self.t.uid, source=self.m.uid, transaction=1, port_or_response_type=1,
                         command_class=C.CC_GET, pid=C.PID_DEVICE_INFO, subdevice=3)
        fut = asyncio.get_running_loop().create_future()
        tn = self.m._send(C.VECTOR_LLRP_RDM_CMD, self.t.cid, P.build_rdm_cmd_pdu(n.encode()))
        self.m._pending[tn] = fut
        _, raw = await asyncio.wait_for(fut, 1)
        a = RdmMessage.decode(raw)
        self.assertEqual((a.response_type, struct.unpack(">H", a.data)[0]),
                         (C.RESPONSE_NACK, C.NR_SUB_DEVICE_OUT_OF_RANGE))


@unittest.skipUnless(MULTICAST, "Multicast on 127.0.0.1 not available")
class TestMultipleTargets(LlrpTest):
    multicast = True

    async def test_discovery_and_filter(self):
        await self.add_target(Uid(0x7FF0, 1))
        await self.add_target(Uid(0x7FF0, 2), connected=True)  # connected to a broker
        await self.add_target(Uid(0x7FF0, 3), type=C.LLRP_TYPE_BROKER, connected=True)
        default = {z.uid.device for z in await self.m.discover()}
        self.assertEqual(default, {1, 3})  # brokers always reply
        self.assertEqual({z.uid.device for z in await self.m.discover(filter=0)}, {1, 2, 3})
        self.assertEqual({z.uid.device for z in await self.m.discover(filter=C.LLRP_FILTER_BROKERS_ONLY)}, {3})

    async def test_same_uid_different_cid(self):
        dyn = Uid(0x8000 | 0x7FF0, 0)  # two targets without an assigned dynamic UID
        a, _ = await self.add_target(dyn)
        b, _ = await self.add_target(dyn)
        targets = await self.m.discover()
        self.assertEqual({z.cid for z in targets}, {a.cid, b.cid})

    async def test_range_split(self):
        # 3 targets each in the lower and upper UID half; with a limit of 3, one split suffices
        for h in (0x0001, 0xFFF0):
            for i in range(3):
                await self.add_target(Uid(h, 0x1000 + i))
        sent = []
        original = self.m._send
        self.m._send = lambda v, c, d: (sent.append(read_probe_request(d)) if v == 1 else None,
                                         original(v, c, d))[1]
        with mock.patch.object(C, "LLRP_KNOWN_UID_SIZE", 3):
            targets = await self.m.discover()
        self.assertEqual(len(targets), 6)
        subranges = {(a.lower, a.upper) for a in sent}
        self.assertIn((Uid(0, 0), Uid(0x7FFF, 0xFFFFFFFF)), subranges)
        self.assertIn((Uid(0x8000, 0), Uid(0xFFFF, 0xFFFFFFFF)), subranges)
        self.assertTrue(all(len(a.known) <= 3 for a in sent))


class TestBrokerAsTarget(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.network = network(MULTICAST)
        self.b = RdmnetBroker(BrokerConfig(port=0, scope="lab", llrp=True, llrp_network=self.network))
        await self.b.start()
        self.m = LlrpManager(self.network, timeout_window_s=0.15, retries=0)
        await self.m.start()
        self.b.llrp_target.max_backoff_s = 0.03

    async def asyncTearDown(self):
        await self.m.stop()
        await self.b.stop()

    async def test_broker_discoverable_scope_read_only(self):
        self.assertIsNotNone(self.b.llrp_target)
        (z,) = await self.m.discover(filter=C.LLRP_FILTER_BROKERS_ONLY)
        self.assertEqual((z.uid, z.cid, z.type), (self.b.config.uid, self.b.config.cid, C.LLRP_TYPE_BROKER))
        self.assertEqual((await self.m.read_scope(z)).scope, "lab")
        self.m.write_enabled = True
        with self.assertRaises(RdmNackError) as ctx:
            await self.m.set_scope(z, "other")
        self.assertEqual(ctx.exception.reason, C.NR_UNSUPPORTED_COMMAND_CLASS)
        await self.b.stop()
        self.assertIsNone(self.b.llrp_target)


if __name__ == "__main__":
    unittest.main()
