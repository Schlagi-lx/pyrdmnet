"""Additional live test checks (hardware preparation) against two simulated gateways."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from pyrdmnet.live_test import ALL_CHECKS, LiveConfig, LiveTest
from pyrdmnet.rdm import Uid
from pyrdmnet.simulator import SimFixture, SimulatedGateway

from helpers import FAST_LIVE
from test_live_test import free_port
from test_llrp import MULTICAST, network


class TestAdditionalChecks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output: list[str] = []
        self.llrp = network(MULTICAST)
        self.gateways = [
            SimulatedGateway(response_delay_s=0.01),
            SimulatedGateway(uid=Uid(0x7FF0, 0x300), fixture=SimFixture(uid=Uid(0x7FF0, 0x301)),
                             response_delay_s=0.01),
        ]
        for i, g in enumerate(self.gateways):
            g.reconnect_min_s = 0.05
            if MULTICAST or i == 0:  # unicast: only one LLRP target can listen on the port
                (await g.start_llrp(self.llrp)).max_backoff_s = 0.03

    async def asyncTearDown(self):
        for g in self.gateways:
            await g.disconnect()
        self.tmp.cleanup()

    async def run_live_test(self, checks):
        port = free_port()
        k = LiveConfig(host="127.0.0.1", port=port, folder=Path(self.tmp.name), wait_for_devices_s=3,
                       find_responders_s=0.6, rdm_timeout_s=1, dnssd=False, checks=checks, restart_pause_s=0.2,
                       restart_wait_s=5, llrp_network=self.llrp, **FAST_LIVE)
        lt = LiveTest(k, output=self.output.append)

        async def connect_later():
            await asyncio.sleep(0.5)
            for g in self.gateways:
                await g.connect("127.0.0.1", port, auto_reconnect=True)
        t = asyncio.create_task(connect_later())
        res = await lt.run()
        await t
        return lt, res

    async def test_all_checks(self):
        lt, res = await self.run_live_test(ALL_CHECKS)
        self.assertTrue(res["success"], res["error"])
        self.assertEqual(len(res["gateways"]), 2)
        self.assertEqual(len(res["devices"]), 2)
        checks = res["checks"]
        self.assertEqual(list(checks), list(ALL_CHECKS))
        # comms: both gateways report this broker
        self.assertEqual(checks["comms"]["status"], "ok")
        for entries in checks["comms"]["gateways"].values():
            self.assertEqual(entries[0]["broker"], f"127.0.0.1:{lt.k.port}")
        # controllers: B is done long before all of A's requests
        c = checks["controllers"]
        self.assertEqual(c["status"], "ok", c)
        self.assertLess(c["busy_answered_when_quiet_done"], c["busy_requests"])
        self.assertEqual(c["endpoint"], 1)  # a fixture behind a gateway port
        # llrp: both gateways found (only one without multicast)
        ll = checks["llrp"]
        self.assertEqual({t["scope"] for t in ll["targets"]}, {"default"})
        if MULTICAST:
            self.assertEqual(ll["status"], "ok", ll)
            self.assertEqual(len(ll["targets"]), 2)
        else:
            self.assertEqual(ll["missing"], [str(self.gateways[1].uid)])
        # restart: both gateways back
        r = checks["restart"]
        self.assertEqual(r["status"], "ok", r)
        self.assertTrue(all(g["reconnect_s"] is not None for g in r["gateways"]))
        self.assertEqual(len(r["gateways"]), 2)
        # files
        report = (lt.folder / "report.md").read_text()
        self.assertIn("## Additional checks", report)
        self.assertIn("### restart: ok", report)
        self.assertEqual(json.loads((lt.folder / "result.json").read_text())["checks"]["comms"]["status"], "ok")

    async def test_controllers_inconclusive_with_fast_device(self):
        for g in self.gateways:
            g.delay = 0.0  # answers at once: no queuing to observe
        lt, res = await self.run_live_test(("controllers",))
        self.assertEqual(res["checks"]["controllers"]["status"], "inconclusive")
        self.assertTrue(res["success"])

    async def test_no_checks(self):
        lt, res = await self.run_live_test(())
        self.assertTrue(res["success"], res["error"])
        self.assertEqual(res["checks"], {})

    async def test_comms_not_supported(self):
        for g in self.gateways:  # a gateway that does not know TCP_COMMS_STATUS
            orig = g._responder_reply

            def without(endpoint, a, orig=orig):
                from pyrdmnet import constants as C
                if a.pid == C.PID_TCP_COMMS_STATUS:
                    import struct
                    return a.response(C.RESPONSE_NACK, struct.pack(">H", C.NR_UNKNOWN_PID))
                return orig(endpoint, a)
            g._responder_reply = without
        lt, res = await self.run_live_test(("comms",))
        self.assertTrue(res["success"])  # the main live test still succeeds
        self.assertEqual(res["checks"]["comms"]["status"], "not supported")


class TestCli(unittest.TestCase):
    def test_parse_checks(self):
        from pyrdmnet.__main__ import _parse_checks
        self.assertEqual(_parse_checks("all"), ALL_CHECKS)
        self.assertEqual(_parse_checks("none"), ())
        self.assertEqual(_parse_checks("llrp, restart"), ("llrp", "restart"))
        self.assertIsNone(_parse_checks("comms,typo"))


if __name__ == "__main__":
    unittest.main()
