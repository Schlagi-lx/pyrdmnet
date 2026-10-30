"""Write test flow offline against the simulator (does not replace the real test)."""

import asyncio
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from pyrdmnet import constants as C
from pyrdmnet.live_test import LiveConfig
from pyrdmnet.write_test import CHECKLIST, WriteConfig, WriteTest
from pyrdmnet.simulator import SimFixture, SimulatedGateway
from pyrdmnet import rdm_params as R
from pyrdmnet.rdm import Uid

from test_live_test import free_port


def etc_par_sim() -> SimFixture:
    """Simulator with the modes of the real ETC ColorSource PAR from live test 2."""
    return SimFixture(uid=Uid(0x7FF0, 0x0201), personalities=[
        R.Personality(1, 5, "5 Channel"), R.Personality(2, 6, "Direct"),
        R.Personality(3, 1, "1 Channel"), R.Personality(4, 3, "RGB")], personality=2, start_address=1)


class TestWriteTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output: list[str] = []

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _run_test(self, wc: WriteConfig, responses=None, gateway=None):
        port = free_port()
        gateway = gateway or SimulatedGateway(fixture=etc_par_sim())
        responses = list(responses if responses is not None else ["y"] * len(CHECKLIST))
        k = LiveConfig(host="127.0.0.1", port=port, folder=Path(self.tmp.name), wait_for_devices_s=3,
                       find_responders_s=2, rdm_timeout_s=1, dnssd=False)
        st = WriteTest(k, wc, output=self.output.append, input_fn=lambda _: responses.pop(0) if responses else "n")

        async def connect():
            await asyncio.sleep(0.5)
            await gateway.connect("127.0.0.1", port)
        t = asyncio.create_task(connect())
        res = await st.run()
        await t
        await gateway.disconnect()
        return st, res, gateway.fixture

    async def test_requested_flow_100_mode4_and_back(self):
        st, res, fx = await self._run_test(WriteConfig(target_mode=4, target_address=100))
        self.assertTrue(res["success"], res["error"])
        self.assertEqual(fx.set_log, [(C.PID_DMX_PERSONALITY, b"\x04"), (C.PID_DMX_START_ADDRESS, b"\x00\x64"),
                                            (C.PID_DMX_PERSONALITY, b"\x02"), (C.PID_DMX_START_ADDRESS, b"\x00\x01")])
        self.assertEqual((fx.personality, fx.start_address), (2, 1))  # initial state
        s1, s2 = res["write_test"]["steps"]
        self.assertEqual(s1["actual"], {"mode": 4, "address": 100, "footprint": 3})
        self.assertEqual(s2["actual"], {"mode": 2, "address": 1, "footprint": 6})
        db = sqlite3.connect(st.folder / "write_test.sqlite")
        changed = db.execute("SELECT parameter, old, new, success FROM rdm_change ORDER BY id").fetchall()
        db.close()
        self.assertEqual(changed, [("DMX_PERSONALITY", "2", "4", 1), ("DMX_START_ADDRESS", "1", "100", 1),
                                ("DMX_PERSONALITY", "4", "2", 1), ("DMX_START_ADDRESS", "100", "1", 1)])
        report = (st.folder / "report.md").read_text()
        self.assertIn("## Write test", report)
        self.assertEqual(len(res["gateways"]), 1)
        self.assertEqual(report.count("## Write test"), 1)
        self.assertTrue(report.startswith("# Write test"))
        self.assertNotIn("read-only", report)
        self.assertIn("SIM Gateway", report)
        self.assertTrue(json.loads((st.folder / "result.json").read_text())["success"])

    async def test_explicit_return_mode1(self):
        st, res, fx = await self._run_test(WriteConfig(4, 100, return_mode=1, return_address=1))
        self.assertTrue(res["success"], res["error"])
        self.assertEqual((fx.personality, fx.start_address), (1, 1))
        self.assertTrue(any("return step differs from the current state" in z for z in self.output))

    async def test_checklist_no_changes_nothing(self):
        st, res, fx = await self._run_test(WriteConfig(4, 100), responses=["y", "n"])
        self.assertFalse(res["success"])
        self.assertEqual(fx.set_log, [])
        self.assertIn("Checklist not fully confirmed - nothing changed", res["error"])

    async def test_unknown_mode(self):
        st, res, fx = await self._run_test(WriteConfig(9, 100))
        self.assertFalse(res["success"])
        self.assertEqual(fx.set_log, [])

    async def test_address_out_of_range(self):
        st, res, fx = await self._run_test(WriteConfig(2, 510))  # 510 + 6 - 1 > 512
        self.assertFalse(res["success"])
        self.assertEqual(fx.set_log, [])

    async def test_write_protect(self):
        g = SimulatedGateway(fixture=etc_par_sim())
        g.fixture.write_protect = True
        st, res, fx = await self._run_test(WriteConfig(4, 100), gateway=g)
        self.assertFalse(res["success"])
        self.assertTrue(any("Step 1 changed nothing" in f for f in res["error"]))

    async def test_address_fails_return_runs(self):
        g = SimulatedGateway(fixture=etc_par_sim())
        fx = g.fixture
        orig = fx._set

        def mode_only(a):  # device accepts the mode change but refuses addresses != 1
            if a.pid == C.PID_DMX_START_ADDRESS and a.data != b"\x00\x01":
                return a.response(C.RESPONSE_NACK, b"\x00\x06")
            return orig(a)
        fx._set = mode_only
        st, res, fx = await self._run_test(WriteConfig(4, 100), gateway=g)
        self.assertFalse(res["success"])
        self.assertEqual((fx.personality, fx.start_address), (2, 1))  # return step took effect
        self.assertFalse(res["write_test"]["steps"][0]["ok"])
        self.assertTrue(res["write_test"]["steps"][1]["ok"])

    async def test_slow_checklist_keeps_connection(self):
        """Input takes longer than the heartbeat interval - broker keeps running (input in a thread)."""
        responses = ["y"] * len(CHECKLIST)

        def slow(_):
            time.sleep(0.3)
            return responses.pop(0)
        port = free_port()
        g = SimulatedGateway(fixture=etc_par_sim())
        k = LiveConfig(host="127.0.0.1", port=port, folder=Path(self.tmp.name), wait_for_devices_s=3,
                       find_responders_s=2, rdm_timeout_s=1, dnssd=False)
        st = WriteTest(k, WriteConfig(4, 100), output=self.output.append, input_fn=slow)

        async def connect():
            await asyncio.sleep(0.5)
            await g.connect("127.0.0.1", port)
        t = asyncio.create_task(connect())
        res = await st.run()
        await t
        await g.disconnect()
        self.assertTrue(res["success"], res["error"])


if __name__ == "__main__":
    unittest.main()
