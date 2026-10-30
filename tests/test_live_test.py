"""Live test flow offline against the simulator (does not replace the real test)."""

import asyncio
import json
import socket
import tempfile
import unittest
from pathlib import Path

from pyrdmnet import settings_check as A
from pyrdmnet.live_test import LiveConfig, LiveTest, choose_adapter, ip_is_local, network_adapters
from pyrdmnet.simulator import SimulatedGateway


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestLiveTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output: list[str] = []

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _run_test(self, gateway: SimulatedGateway | None, device_scope="default", **kw):
        port = free_port()
        kw.setdefault("dnssd", False)
        k = LiveConfig(host="127.0.0.1", port=port, folder=Path(self.tmp.name), wait_for_devices_s=3,
                       find_responders_s=2, rdm_timeout_s=1, **kw)
        lt = LiveTest(k, output=self.output.append)

        async def connect_later():
            await asyncio.sleep(0.5)
            if gateway:
                try:
                    await gateway.connect("127.0.0.1", port)
                except Exception:
                    pass
        t = asyncio.create_task(connect_later())
        res = await lt.run()
        await t
        if gateway:
            await gateway.disconnect()
        return lt, res

    async def test_successful_run(self):
        lt, res = await self._run_test(SimulatedGateway(),
                                   expected=A.ExpectedSetting("Direct 6ch", 6, 100, 1))
        self.assertTrue(res["success"], res["error"])
        self.assertEqual(len(res["gateways"]), 1)
        self.assertEqual(len(res["devices"]), 1)
        self.assertEqual(res["devices"][0]["check"]["status"], A.OK_UNCONFIRMED)
        self.assertEqual(len(res["devices"][0]["sensors"]["values"]), 4)
        for file in ("report.md", "result.json", "live_test.sqlite", "broker_events.jsonl",
                      "packet_capture.jsonl"):
            self.assertTrue((lt.folder / file).exists(), file)
        lines = (lt.folder / "packet_capture.jsonl").read_text().splitlines()
        self.assertGreater(len(lines), 10)
        self.assertEqual(json.loads(lines[0])["direction"], "in")
        self.assertTrue(any("CONNECTED" in z for z in self.output))
        self.assertIn("SIM-PAR RGBL", (lt.folder / "report.md").read_text())
        self.assertIn("read-only", (lt.folder / "report.md").read_text())
        ports = res["gateways"][0]["ports"]
        self.assertEqual(ports[0]["mode"], "Output")
        self.assertEqual(ports[0]["name"], "DMX Port 1")
        self.assertEqual(ports[0]["universe"], 1)
        self.assertEqual(len(ports[0]["responder"]), 1)
        self.assertTrue(any(f"Mode {ports[0]['mode']}" in z for z in self.output))

    async def test_nobody_connects(self):
        lt, res = await self._run_test(None)
        self.assertFalse(res["success"])
        self.assertIn("No RDMnet device has connected", res["error"])
        self.assertTrue(any("No TCP connection" in h for h in res["hints"]))

    async def test_wrong_scope_reported(self):
        lt, res = await self._run_test(SimulatedGateway(scope="stage"))
        self.assertFalse(res["success"])
        self.assertTrue(any("REJECTED" in z and "stage" in z for z in self.output))
        self.assertTrue(any("Scope" in h for h in res["hints"]))

    async def test_gateway_without_fixture(self):
        g = SimulatedGateway()
        orig = g._responder_reply

        def without(endpoint, a):  # port 1 reports no responders
            from pyrdmnet import constants as C
            from pyrdmnet import rdm_params as R
            if endpoint == 0 and a.pid == C.PID_ENDPOINT_RESPONDERS:
                return a.response(C.RESPONSE_ACK, R.build_endpoint_responders(1, 1, []))
            return orig(endpoint, a)
        g._responder_reply = without
        lt, res = await self._run_test(g)
        self.assertFalse(res["success"])
        self.assertEqual(len(res["gateways"]), 1)
        self.assertTrue(any("No RDM responders" in f for f in res["error"]))
        self.assertEqual(res["gateways"][0]["ports_after_search"][0]["responder"], [])
        self.assertTrue(any("DMX output" in h for h in res["hints"]))

    async def test_foreign_ip(self):
        k = LiveConfig(host="203.0.113.7", folder=Path(self.tmp.name))
        res = await LiveTest(k, output=self.output.append).run()
        self.assertFalse(res.get("success"))
        self.assertIn("does not belong to any network adapter", res["error"][0])


class TestAdapter(unittest.TestCase):
    def test_local(self):
        self.assertTrue(ip_is_local("127.0.0.1"))
        self.assertFalse(ip_is_local("203.0.113.7"))

    def test_ifconfig_macos(self):
        text = ("lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384\n"
                "\tinet 127.0.0.1 netmask 0xff000000\n"
                "en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
                "\tether aa:bb:cc:dd:ee:ff\n"
                "\tinet6 fe80::1%en0 prefixlen 64 scopeid 0x4\n"
                "\tinet 192.168.178.20 netmask 0xffffff00 broadcast 192.168.178.255\n"
                "en7: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
                "\tinet 2.0.0.10 netmask 0xff000000 broadcast 2.255.255.255\n")
        self.assertEqual(network_adapters(text), [("lo0", "127.0.0.1"), ("en0", "192.168.178.20"),
                                                 ("en7", "2.0.0.10")])

    def test_selection(self):
        group = [(n, ip) for n, ip in network_adapters() if not ip.startswith("127.")]
        reader: list[str] = []
        if group:
            self.assertEqual(choose_adapter(lambda _: "1", reader.append), group[0][1])
        self.assertIsNone(choose_adapter(lambda _: "x", reader.append))


if __name__ == "__main__":
    unittest.main()


class TestMasterPortCapture(unittest.TestCase):
    """Responses from the real live test on 2026-10-26 (LSS MasterPort 2, firmware v2.13.0028)."""

    def test_decode_responses(self):
        from pyrdmnet import rdm_params as R
        di = R.read_device_info(bytes.fromhex("010008510802020d001c00000101ffff000004"))
        self.assertEqual((di.model_id, di.footprint, di.start_address, di.sensor_count), (0x0851, 0, 0xFFFF, 4))
        self.assertEqual(di.product_category_text, "Data distribution")
        changed, eps = R.read_endpoint_list(bytes.fromhex("00000000000101000201000301000401"))
        self.assertEqual([(e.number, e.type) for e in eps], [(1, 1), (2, 1), (3, 1), (4, 1)])
        self.assertEqual(R.read_endpoint_responders(bytes.fromhex("000100000004")), (1, 4, []))
        sd = R.read_sensor_definition(bytes.fromhex("0300010100000320000002ee024350552054656d706572617475726500"))
        self.assertEqual((sd.description, sd.unit_text, sd.factor, sd.normal_max), ("CPU Temperature", "°C", 0.1, 750))
        p = R.read_personality_description(bytes.fromhex("01000044656661756c7400"))
        self.assertEqual((p.number, p.footprint, p.description), (1, 0, "Default"))
