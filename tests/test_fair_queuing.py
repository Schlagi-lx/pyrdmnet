"""Fair queuing of controller requests in the broker (E1.33 9.2.10)."""

import asyncio
import time
import unittest

from pyrdmnet import constants as C
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController, RdmTimeoutError, RptStatusError
from pyrdmnet.simulator import SimulatedGateway


async def wait_until(condition, timeout_s=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(poll(), timeout_s)


class FairQueuingTest(unittest.IsolatedAsyncioTestCase):
    config: dict = {}
    delay_s = 0.03  # the device answers one request after the other, each after this delay

    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerConfig(port=0, **self.config))
        await self.b.start()
        self.g = SimulatedGateway(response_delay_s=self.delay_s)
        await self.g.connect("127.0.0.1", self.b.port)
        self.a, self.c = RdmnetController(), RdmnetController()
        for k in (self.a, self.c):
            await k.connect("127.0.0.1", self.b.port)
            await k.client_list()

    async def asyncTearDown(self):
        for k in (self.a, self.c, self.g):
            await k.disconnect()
        await self.b.stop()

    def queue(self) -> dict:
        return self.b.status()["queued_requests"].get(str(self.g.uid), {"waiting": 0, "in_flight": 0})

    async def busy_and_quiet(self, busy_requests=40, quiet_requests=4):
        """Controller A floods the device, B asks a few questions afterwards. Returns how many of A's
        requests were answered when B was done, and B's time."""
        done_a = 0

        async def get_a():
            nonlocal done_a
            await self.a.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=20)
            done_a += 1
        busy = [asyncio.create_task(get_a()) for _ in range(busy_requests)]
        await wait_until(lambda: self.b.counters["rpt_request_forwarded"] >= 1)
        start = time.monotonic()
        await asyncio.gather(*(self.c.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=20)
                               for _ in range(quiet_requests)))
        quiet_s = time.monotonic() - start
        answered_a = done_a
        await asyncio.gather(*busy)
        return answered_a, quiet_s


class TestFairShare(FairQueuingTest):
    async def test_quiet_controller_not_starved(self):
        answered_a, _ = await self.busy_and_quiet()
        # round robin: B's 4 requests interleave with A's instead of waiting behind all 40
        self.assertLessEqual(answered_a, 4 + 4 + 2, f"A answered {answered_a} before B was done")
        self.assertEqual(self.queue(), {"waiting": 0, "in_flight": 0})

    async def test_busy_controller_uses_whole_bandwidth_alone(self):
        await asyncio.gather(*(self.a.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=20) for _ in range(20)))
        self.assertEqual(self.b.counters["rpt_request_forwarded"], 20)
        self.assertEqual(self.b.counters["request_timeout"], 0)


class TestWithoutFairQueuing(FairQueuingTest):
    config = {"device_window": 0}

    async def test_quiet_controller_waits_behind_busy_one(self):
        answered_a, _ = await self.busy_and_quiet()
        self.assertGreaterEqual(answered_a, 25)  # shows that the fair share test above measures something


class TestWindow(FairQueuingTest):
    config = {"device_window": 4, "request_timeout_s": 0.2, "max_queued_per_controller": 5}

    async def test_window_timeout_and_queue_limit(self):
        self.g.mute = True  # the device never answers
        tasks = [asyncio.create_task(self.a.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=0.1))
                 for _ in range(12)]
        await wait_until(lambda: self.queue()["in_flight"] == 4 and self.b.counters["request_queue_full"] == 3)
        self.assertEqual(self.queue(), {"waiting": 5, "in_flight": 4})  # 4 + 5, three dropped
        self.assertEqual(self.b.counters["request_queue_full"], 3)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertTrue(all(isinstance(r, RdmTimeoutError) for r in results))
        # unanswered slots expire, the waiting requests are sent afterwards
        await wait_until(lambda: self.b.counters["request_timeout"] >= 9, 3)
        self.assertEqual(self.b.counters["rpt_request_forwarded"], 9)
        self.assertEqual(self.queue(), {"waiting": 0, "in_flight": 0})

    async def test_device_leaves_with_waiting_requests(self):
        self.g.mute = True
        tasks = [asyncio.create_task(self.a.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=5)) for _ in range(6)]
        await wait_until(lambda: self.queue() == {"waiting": 2, "in_flight": 4})
        await self.g.disconnect()
        results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 1)  # no 5 s timeouts
        unknown = [r for r in results if isinstance(r, RptStatusError) and r.code == C.RPT_STATUS_UNKNOWN_RPT_UID]
        self.assertEqual(len(unknown), 6)  # waiting and in flight: no answer will come from the device

    async def test_controller_leaves_with_waiting_requests(self):
        self.g.mute = True
        tasks = [asyncio.create_task(self.a.rdm_get(self.g.uid, C.PID_DEVICE_INFO, timeout_s=5)) for _ in range(5)]
        await wait_until(lambda: self.queue() == {"waiting": 1, "in_flight": 4})
        await self.a.disconnect()
        await asyncio.gather(*tasks, return_exceptions=True)  # connection closed
        await wait_until(lambda: self.queue() == {"waiting": 0, "in_flight": 0}, 1)
        self.g.mute = False
        self.assertEqual(len(await self.c.rdm_get(self.g.uid, C.PID_DEVICE_INFO)), 19)  # B is not blocked


if __name__ == "__main__":
    unittest.main()
