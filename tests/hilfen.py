"""Gemeinsame Testumgebung: Broker + simuliertes Gerät + Controller auf 127.0.0.1."""

import unittest

from pyrdmnet.auslesen import Auslesung, GeraeteAdresse
from pyrdmnet.broker import BrokerKonfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController
from pyrdmnet.rdm import Uid
from pyrdmnet.simulator import SimulierterGateway


class NetzTest(unittest.IsolatedAsyncioTestCase):
    nativ = False
    broker_konfig: dict = {}

    async def asyncSetUp(self):
        self.broker = RdmnetBroker(BrokerKonfig(host="127.0.0.1", port=0, **self.broker_konfig))
        await self.broker.starten()
        self.port = self.broker.port
        self.gateway = SimulierterGateway(nativ=self.nativ)
        await self.gateway.verbinden("127.0.0.1", self.port)
        self.c = RdmnetController()
        await self.c.verbinden("127.0.0.1", self.port)
        await self.c.clientliste()
        self.aus = Auslesung(self.c, timeout_s=2.0)
        sw = self.gateway.scheinwerfer
        self.sw = sw
        self.adresse = (GeraeteAdresse(self.gateway.uid, 0, self.gateway.uid) if self.nativ
                        else GeraeteAdresse(self.gateway.uid, 1, sw.uid))

    async def asyncTearDown(self):
        for k in (self.c, self.gateway):
            await k.trennen()
        await self.broker.stoppen()


def uid(h, g):
    return Uid(h, g)
