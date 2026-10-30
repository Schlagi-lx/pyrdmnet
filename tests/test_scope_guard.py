"""One broker per scope (E1.33 9.1.4, 7.4.3): discovery, STANDBY at start-up, monitoring, CLI."""

import asyncio
import io
import os
import struct
import tempfile
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from pyrdmnet import constants as C
from pyrdmnet import dnssd
from pyrdmnet.__main__ import main as cli_main
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.broker_responder import BrokerResponder, BrokerResponderInfo
from pyrdmnet.controller import RdmnetController
from pyrdmnet.live_test import LiveConfig, LiveTest
from pyrdmnet.rdm import RdmMessage, Uid

from helpers import FAST_LIVE
from test_mdns import install_fake

OTHER_CID = uuid.UUID("0123456789abcdef0123456789abcdef")
OTHER_TXT = f"TxtVers=1 E133Scope=default E133Vers=1 CID={OTHER_CID.hex.upper()} UID=6574000000AB Model=ETC\\ Broker"
OTHER = dnssd.BrokerRecord("ETC Broker", "etc-pc.local", 8888, (("E133Scope", "default"),
                                                                 ("CID", OTHER_CID.hex.upper())))

BROWSE = """Browsing for _rdmnet._tcp,_default
DATE: ---Fri 30 Oct 2026---
17:00:00.000  ...STARTING...
Timestamp     A/R    Flags  if Domain               Service Type         Instance Name
17:00:00.100  Add        3   4 local.               _rdmnet._tcp.        pyrdmnet Broker default 12345678
17:00:00.100  Add        3   6 local.               _rdmnet._tcp.        pyrdmnet Broker default 12345678
17:00:00.200  Add        2   4 local.               _rdmnet._tcp.        ETC Broker
17:00:00.300  Add        2   4 local.               _rdmnet._tcp.        Gone Broker
17:00:01.300  Rmv        0   4 local.               _rdmnet._tcp.        Gone Broker
"""
LOOKUP = ("Lookup ETC Broker._rdmnet._tcp.local\n"
          "17:00:01.000  ETC\\032Broker._rdmnet._tcp.local. can be reached at etc-pc.local.:8888 (interface 4)\n"
          f" {OTHER_TXT}\n")


async def wait_until(condition, timeout_s=3.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


class TestParsing(unittest.TestCase):
    def test_browse_and_lookup(self):
        self.assertEqual(dnssd.parse_dns_sd_browse(BROWSE), ["pyrdmnet Broker default 12345678", "ETC Broker"])
        r = dnssd.parse_dns_sd_lookup("ETC Broker", LOOKUP)
        self.assertEqual((r.host, r.port, r.cid, r.scope), ("etc-pc.local", 8888, OTHER_CID, "default"))
        self.assertEqual(r.properties["Model"], "ETC Broker")  # escaped space
        self.assertIn("etc-pc.local:8888", r.describe())
        self.assertEqual(dnssd.parse_dns_sd_lookup("x", "nothing"), dnssd.BrokerRecord("x"))

    def test_own_record(self):
        own = uuid.uuid4()
        mine = dnssd.BrokerRecord("me", "h", 9000, (("E133Scope", "default"), ("CID", own.hex)))
        self.assertTrue(dnssd.is_own_record(mine, "default", own, 9000))
        self.assertFalse(dnssd.is_own_record(mine, "default", own, 9001))  # same CID, other port: not us
        self.assertFalse(dnssd.is_own_record(OTHER, "default", own, 8888))


class TestResponderState(unittest.TestCase):
    def test_broker_status_reports_state_and_rejects_set(self):
        state = [C.E133_BROKER_STANDBY]
        r = BrokerResponder(BrokerResponderInfo(uid=Uid(0x7FF0, 1), scope="default"), state=lambda: state[0])

        def request(cc, data=b""):
            return r.respond(RdmMessage(dest=Uid(0x7FF0, 1), source=Uid(0x7FF0, 9), transaction=1,
                                        port_or_response_type=1, command_class=cc, pid=C.PID_BROKER_STATUS,
                                        data=data))
        self.assertEqual(request(C.CC_GET).data, bytes((0, C.E133_BROKER_STANDBY)))
        state[0] = C.E133_BROKER_ACTIVE
        self.assertEqual(request(C.CC_GET).data, bytes((0, C.E133_BROKER_ACTIVE)))
        n = request(C.CC_SET, b"\x00")
        self.assertEqual((n.response_type, struct.unpack(">H", n.data)[0]),
                         (C.RESPONSE_NACK, C.NR_ACTION_NOT_SUPPORTED))


class FakeRegistration:
    def __init__(self, scope, cid, uid, ip, port, method=None):
        self.name, self.method, self.stopped = f"pyrdmnet Broker {scope}", "fake", False

    def stop(self):
        self.stopped = True


class TestAnnouncer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0))
        await self.b.start()
        self.others: list = []
        self.messages: list[str] = []
        self.a = dnssd.BrokerAnnouncer(self.b, "127.0.0.1", retry_s=0.05, monitor_s=0.05,
                                       finder=lambda *args: list(self.others), registrar=FakeRegistration,
                                       on_message=self.messages.append)

    async def asyncTearDown(self):
        await self.a.stop()
        await self.b.stop()

    async def test_standby_until_other_broker_is_gone(self):
        self.others = [OTHER]
        await self.a.start()
        self.assertTrue(self.a.standby)
        self.assertEqual(self.b.state, C.E133_BROKER_STANDBY)
        self.assertEqual(self.b.status()["state"], "standby")
        self.assertIn("STANDBY", self.messages[0])
        # a controller sees the state via RDM (E1.33 7.6.4)
        c = RdmnetController()
        await c.connect("127.0.0.1", self.b.port)
        try:
            self.assertEqual(await c.rdm_get(self.b.config.uid, C.PID_BROKER_STATUS, timeout_s=2),
                             bytes((0, C.E133_BROKER_STANDBY)))
        finally:
            await c.disconnect()
        self.others = []
        await wait_until(lambda: not self.a.standby)
        self.assertEqual(self.b.state, C.E133_BROKER_ACTIVE)
        kinds = [e["event"] for e in self.b.events]
        self.assertIn("scope_conflict", kinds)
        self.assertIn("dnssd_registered", kinds)
        self.assertIn("gone", self.messages[-1])

    async def test_monitoring_only_alerts(self):
        await self.a.start()
        self.assertFalse(self.a.standby)
        self.others = [OTHER]
        await wait_until(lambda: any(e["event"] == "scope_conflict" for e in self.b.events))
        await asyncio.sleep(0.2)  # several monitoring rounds: alert only once, stay ACTIVE
        self.assertEqual(sum(e["event"] == "scope_conflict" for e in self.b.events), 1)
        self.assertEqual(self.b.state, C.E133_BROKER_ACTIVE)
        self.assertFalse(self.a.standby)

    async def test_stop_unregisters(self):
        await self.a.start()
        reg = self.a.registration
        await self.a.stop()
        self.assertTrue(reg.stopped)


class TestWithFakeDnsSd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        install_fake(Path(self.tmp.name))
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.tmp.name}{os.pathsep}{os.environ['PATH']}"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def others(self, *names):
        lines = [f"{n}|{n.lower().replace(' ', '-')}.local.|8888|{OTHER_TXT}" for n in names]
        (Path(self.tmp.name) / "others.txt").write_text("\n".join(lines))

    def test_find_brokers_and_conflicts(self):
        self.others("ETC Broker")
        found = dnssd.find_brokers("default", duration_s=0.5)
        self.assertEqual([(r.name, r.host, r.port, r.cid) for r in found],
                         [("ETC Broker", "etc-broker.local", 8888, OTHER_CID)])
        own = uuid.uuid4()
        reg = dnssd.DnssdRegistration("default", own, Uid(0x7FF0, 1), "127.0.0.1", 9000)
        try:
            self.assertEqual(len(dnssd.find_brokers("default", duration_s=0.5)), 2)
            conflicts = dnssd.conflicting_brokers("default", own, 9000, duration_s=0.5)
            self.assertEqual([r.name for r in conflicts], ["ETC Broker"])  # own registration ignored
        finally:
            reg.stop()

    def run_cli(self, *args):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = cli_main(["brokers", "--time", "0.5", *args])
        return rc, out.getvalue()

    def test_cli_brokers(self):
        self.assertEqual(self.run_cli()[0], 1)  # none found
        self.others("ETC Broker")
        rc, text = self.run_cli()
        self.assertEqual(rc, 0)
        self.assertIn("ETC Broker", text)
        self.others("ETC Broker", "Second Broker")
        rc, text = self.run_cli()
        self.assertEqual(rc, 1)
        self.assertIn("WARNING: 2 brokers", text)

    def test_live_test_reports_other_broker(self):
        self.others("ETC Broker")
        output: list[str] = []
        k = LiveConfig(host="127.0.0.1", port=0, folder=Path(self.tmp.name), wait_for_devices_s=0.3, dnssd=True,
                       **FAST_LIVE)
        res = asyncio.run(LiveTest(k, output=output.append).run())
        self.assertTrue(res["mdns"]["standby"])
        self.assertFalse(res["mdns"]["active"])
        self.assertEqual(len(res["mdns"]["other_brokers"]), 1)
        self.assertTrue(any("another broker already uses scope" in line for line in output))
        self.assertTrue(any("enter this broker statically" in line for line in output))


if __name__ == "__main__":
    unittest.main()
