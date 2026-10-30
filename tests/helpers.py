"""Gemeinsame Testumgebung: Broker + simuliertes Gerät + Controller auf 127.0.0.1."""

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
