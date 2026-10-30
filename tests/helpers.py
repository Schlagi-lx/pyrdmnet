"""Shared test environment: broker + simulated device + controller on 127.0.0.1."""

import unittest

from pyrdmnet.readout import DeviceReader, DeviceAddress
from pyrdmnet.broker import BrokerConfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController
from pyrdmnet.rdm import Uid
from pyrdmnet.simulator import SimulatedGateway


class NetworkTest(unittest.IsolatedAsyncioTestCase):
    native = False
    broker_config: dict = {}

    async def asyncSetUp(self):
        self.broker = RdmnetBroker(BrokerConfig(host="127.0.0.1", port=0, **self.broker_config))
        await self.broker.start()
        self.port = self.broker.port
        self.gateway = SimulatedGateway(native=self.native)
        await self.gateway.connect("127.0.0.1", self.port)
        self.c = RdmnetController()
        await self.c.connect("127.0.0.1", self.port)
        await self.c.client_list()
        self.reader = DeviceReader(self.c, timeout_s=2.0)
        fx = self.gateway.fixture
        self.fx = fx
        self.address = (DeviceAddress(self.gateway.uid, 0, self.gateway.uid) if self.native
                        else DeviceAddress(self.gateway.uid, 1, fx.uid))

    async def asyncTearDown(self):
        for k in (self.c, self.gateway):
            await k.disconnect()
        await self.broker.stop()


def uid(h, g):
    return Uid(h, g)


# Short pauses for LiveConfig in tests (lab defaults: settle 3 s, poll 0.5 s, search retry 5 s, mDNS check 4 s)
FAST_LIVE = dict(settle_s=0.1, poll_s=0.05, search_retry_s=0.2, mdns_self_test_s=1.0)
# Short pauses for WriteConfig in tests (lab defaults: 0.5 s after SET, 1 s between read-back attempts)
FAST_WRITE = dict(pause_after_set_s=0.02, readback_pause_s=0.05)
