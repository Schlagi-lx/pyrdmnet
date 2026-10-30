"""Client Entry Update (E1.33 6.3.1.4, 9.1.6): Client-Eintrag ohne Neuverbindung ändern."""

import asyncio
import unittest
import uuid

from pyrdmnet import konstanten as K
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerKonfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController
from pyrdmnet.klient import RptKlient, VerbindungsFehler
from pyrdmnet.rdm import Uid

GERAET = Uid(0x7FF0, 0x10)


async def warte_bis(bedingung, timeout_s=2.0):
    async def schleife():
        while not bedingung():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(schleife(), timeout_s)


class TestClientEntryUpdate(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerKonfig(port=0))
        await self.b.starten()
        self.c = RdmnetController()
        await self.c.verbinden("127.0.0.1", self.b.port)
        self.g = RptKlient(K.RPT_CLIENT_TYP_GERAET, GERAET)
        await self.g.verbinden("127.0.0.1", self.b.port)
        await warte_bis(lambda: GERAET in self.c.clients)

    async def asyncTearDown(self):
        for k in (self.c, self.g):
            await k.trennen()
        await self.b.stoppen()

    def test_kodierung_rundlauf(self):
        e = P.ClientEintrag(uuid.uuid4(), GERAET, K.RPT_CLIENT_TYP_GERAET, uuid.uuid4())
        flags, gelesen = P.lese_client_entry_update(P.baue_client_entry_update(1, e))
        self.assertEqual((flags, gelesen), (1, e))
        with self.assertRaises(P.ProtokollFehler):
            P.lese_client_entry_update(b"")

    async def test_binding_cid_aendern(self):
        bind = uuid.uuid4()
        self.assertEqual(await self.g.eintrag_aktualisieren(binding_cid=bind), GERAET)
        await warte_bis(lambda: self.c.clients[GERAET].binding_cid == bind)
        self.assertEqual(self.b._clients[GERAET].eintrag.binding_cid, bind)
        self.assertEqual(self.b.zaehler["eintrag_update_ok"], 1)
        self.assertTrue(self.g.verbunden)

    async def test_uid_aendern_wird_umgetragen(self):
        neu = Uid(0x7FF0, 0x11)
        self.assertEqual(await self.g.eintrag_aktualisieren(uid=neu), neu)
        self.assertEqual(self.g.uid, neu)
        self.assertIn(neu, self.b._clients)
        self.assertNotIn(GERAET, self.b._clients)
        await warte_bis(lambda: neu in self.c.clients)
        self.assertNotIn(GERAET, self.c.clients)  # alter Eintrag derselben CID ersetzt
        # alte UID ist wieder frei
        k = RptKlient(K.RPT_CLIENT_TYP_GERAET, GERAET)
        await k.verbinden("127.0.0.1", self.b.port)
        await k.trennen()

    async def test_doppelte_uid_trennt(self):
        andere = RptKlient(K.RPT_CLIENT_TYP_GERAET, Uid(0x7FF0, 0x30))
        await andere.verbinden("127.0.0.1", self.b.port)
        self.addAsyncCleanup(andere.trennen)
        with self.assertRaises(VerbindungsFehler) as ctx:
            await self.g.eintrag_aktualisieren(uid=andere.uid)
        self.assertEqual(ctx.exception.code, K.E133_CONNECT_DUPLICATE_UID)
        await asyncio.wait_for(self.g.getrennt.wait(), 2)
        await warte_bis(lambda: GERAET not in self.c.clients)
        self.assertNotIn(GERAET, self.b._clients)
        self.assertEqual(self.b.zaehler["eintrag_update_abgelehnt"], 1)
        self.assertIn(andere.uid, self.b._clients)  # der andere bleibt unberührt

    async def test_ungueltige_uid_trennt(self):
        with self.assertRaises(VerbindungsFehler) as ctx:
            await self.g.eintrag_aktualisieren(uid=Uid(0x7FF0, 0xFFFFFFFF))
        self.assertEqual(ctx.exception.code, K.E133_CONNECT_INVALID_UID)
        with self.assertRaises(VerbindungsFehler):  # dynamische UID als feste angegeben
            await self.c.eintrag_aktualisieren(uid=self.c.uid)

    async def test_cid_aendern_unzulaessig(self):
        fremd = P.ClientEintrag(uuid.uuid4(), GERAET, K.RPT_CLIENT_TYP_GERAET)
        fut = self.g._update_antwort = asyncio.get_running_loop().create_future()
        await self.g.sende_broker(K.VECTOR_BROKER_CLIENT_ENTRY_UPDATE, P.baue_client_entry_update(0, fremd))
        rep = await asyncio.wait_for(fut, 2)
        self.assertEqual(rep.code, K.E133_CONNECT_INVALID_CLIENT_ENTRY)
        await asyncio.wait_for(self.g.getrennt.wait(), 2)
        await warte_bis(lambda: GERAET not in self.c.clients)

    async def test_dynamische_uid_bleibt(self):
        d = RptKlient(K.RPT_CLIENT_TYP_GERAET, Uid(0x8000 | 0x7FF0, 0))
        erste = await d.verbinden("127.0.0.1", self.b.port)
        self.assertEqual(await d.eintrag_aktualisieren(binding_cid=uuid.uuid4()), erste)
        await d.trennen()

    async def test_inkrementell_abschalten(self):
        await self.c.eintrag_aktualisieren(inkrementell=False)
        self.assertFalse(self.b._clients[self.c.uid].inkrementell)
        k = RptKlient(K.RPT_CLIENT_TYP_GERAET, Uid(0x7FF0, 0x20))
        await k.verbinden("127.0.0.1", self.b.port)
        await asyncio.sleep(0.1)
        self.assertNotIn(k.uid, self.c.clients)  # keine Client-Add-Meldung mehr
        await k.trennen()


if __name__ == "__main__":
    unittest.main()
