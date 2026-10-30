"""Broker: Connect-Prüfungen, Clientlisten, Routing, Heartbeat, Robustheit."""

import asyncio
import os
import struct
import unittest
import uuid

from pyrdmnet import konstanten as K
from pyrdmnet import pdu as P
from pyrdmnet.broker import BrokerKonfig, RdmnetBroker
from pyrdmnet.controller import RdmnetController, RptStatusFehler
from pyrdmnet.klient import RptKlient, VerbindungsFehler
from pyrdmnet.rdm import ALLE_GERAETE, Uid
from pyrdmnet.simulator import SimulierterGateway

from hilfen import NetzTest


class TestConnect(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.b = RdmnetBroker(BrokerKonfig(port=0, scope="labor", max_clients=3))
        await self.b.starten()

    async def asyncTearDown(self):
        await self.b.stoppen()

    async def _klient(self, uid, scope="labor", typ=K.RPT_CLIENT_TYP_GERAET, cid=None):
        k = RptKlient(typ, uid, scope, cid=cid)
        await k.verbinden("127.0.0.1", self.b.port)
        return k

    async def test_ok_und_scope_falsch(self):
        k = await self._klient(Uid(0x7FF0, 5))
        self.assertEqual(k.uid, Uid(0x7FF0, 5))
        self.assertEqual(k.broker_uid, self.b.konfig.uid)
        with self.assertRaises(VerbindungsFehler) as ctx:
            await self._klient(Uid(0x7FF0, 6), scope="default")
        self.assertEqual(ctx.exception.code, K.E133_CONNECT_SCOPE_MISMATCH)
        await k.trennen()

    async def test_doppelte_und_ungueltige_uid(self):
        k = await self._klient(Uid(0x7FF0, 5))
        for u, code in ((Uid(0x7FF0, 5), K.E133_CONNECT_DUPLICATE_UID),
                        (Uid(0, 0), K.E133_CONNECT_INVALID_UID),
                        (Uid(0x7FF0, 0xFFFFFFFF), K.E133_CONNECT_INVALID_UID),
                        (self.b.konfig.uid, K.E133_CONNECT_DUPLICATE_UID)):
            with self.assertRaises(VerbindungsFehler) as ctx:
                await self._klient(u)
            self.assertEqual(ctx.exception.code, code, u)
        await k.trennen()

    async def test_dynamische_uid_bleibt_je_cid(self):
        cid = uuid.uuid4()
        k = await self._klient(Uid(0xFFF0, 0), cid=cid)
        erste = k.uid
        self.assertTrue(erste.ist_dynamisch)
        await k.trennen()
        await asyncio.sleep(0.05)
        k2 = await self._klient(Uid(0xFFF0, 0), cid=cid)
        self.assertEqual(k2.uid, erste)
        k3 = await self._klient(Uid(0xFFF0, 0))
        self.assertNotEqual(k3.uid, erste)
        await k2.trennen()
        await k3.trennen()

    async def test_kapazitaet(self):
        ks = [await self._klient(Uid(0x7FF0, i)) for i in range(10, 13)]  # 0x7ff0:00000001 ist die Broker-UID
        with self.assertRaises(VerbindungsFehler) as ctx:
            await self._klient(Uid(0x7FF0, 9))
        self.assertEqual(ctx.exception.code, K.E133_CONNECT_CAPACITY_EXCEEDED)
        for k in ks:
            await k.trennen()

    async def test_falsche_praeambel_und_muell(self):
        for daten in (b"GET / HTTP/1.0\r\n\r\n", os.urandom(200), K.ACN_TCP_PREAMBEL + struct.pack(">I", 10**7)):
            r, w = await asyncio.open_connection("127.0.0.1", self.b.port)
            w.write(daten)
            await w.drain()
            self.assertEqual(await asyncio.wait_for(r.read(), 3), b"")  # Broker schließt
            w.close()
        k = await self._klient(Uid(0x7FF0, 5))  # Broker läuft weiter
        await k.trennen()
        self.assertGreaterEqual(self.b.zaehler["protokollfehler"], 1)

    async def test_ungueltiger_connect_inhalt(self):
        r, w = await asyncio.open_connection("127.0.0.1", self.b.port)
        cid = uuid.uuid4()
        w.write(P.baue_paket(K.VECTOR_ROOT_E133_BROKER, cid, P.baue_broker(K.VECTOR_BROKER_CONNECT, b"\x00" * 20)))
        await w.drain()
        block = await asyncio.wait_for(P.lese_tcp_block(r), 3)
        rep = P.lese_connect_reply(P.lese_broker(P.lese_root_pdus(block)[0].daten).daten)
        self.assertEqual(rep.code, K.E133_CONNECT_INVALID_CLIENT_ENTRY)
        w.close()


class TestRouting(NetzTest):
    async def test_clientliste_und_add_remove(self):
        liste = await self.c.clientliste(neu_abfragen=True)
        self.assertIn(self.gateway.uid, liste)
        self.assertIn(self.c.uid, liste)
        zweites = SimulierterGateway(uid=Uid(0x7FF0, 0x777))
        await zweites.verbinden("127.0.0.1", self.port)
        for _ in range(50):
            if zweites.uid in self.c.clients:
                break
            await asyncio.sleep(0.02)
        self.assertIn(zweites.uid, self.c.clients)
        await zweites.trennen()
        for _ in range(50):
            if zweites.uid not in self.c.clients:
                break
            await asyncio.sleep(0.02)
        self.assertNotIn(zweites.uid, self.c.clients)

    async def test_unbekannte_rpt_uid(self):
        with self.assertRaises(RptStatusFehler) as ctx:
            await self.c.rdm_get(Uid(0x7FF0, 0xABCDEF), K.PID_DEVICE_INFO, timeout_s=2)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_UNKNOWN_RPT_UID)

    async def test_broadcast_complete(self):
        with self.assertRaises(RptStatusFehler) as ctx:
            await self.c.rdm_get(ALLE_GERAETE, K.PID_DEVICE_INFO, rdm_ziel=Uid(0xFFFF, 0xFFFFFFFF), timeout_s=2)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_BROADCAST_COMPLETE)
        self.assertEqual(self.broker.zaehler["rpt_broadcast"], 1)

    async def test_gefaelschte_quell_uid_wird_verworfen(self):
        kopf = P.RptKopf(K.VECTOR_RPT_REQUEST, Uid(0x7FF0, 0x999), 0, self.gateway.uid, 0, 1)
        await self.c._v.sende_rpt(kopf, P.baue_status(1))
        await asyncio.sleep(0.1)
        self.assertEqual(self.broker.zaehler["rpt_falsche_quelle"], 1)

    async def test_status_endpoint_unbekannt(self):
        with self.assertRaises(RptStatusFehler) as ctx:
            await self.c.rdm_get(self.gateway.uid, K.PID_DEVICE_INFO, endpoint=7, rdm_ziel=self.sw.uid)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_UNKNOWN_ENDPOINT)

    async def test_status_ausgabe(self):
        s = self.broker.status()
        self.assertEqual(len(s["clients"]), 2)
        self.assertEqual({c["typ"] for c in s["clients"]}, {"Gerät", "Controller"})


class TestHeartbeat(unittest.IsolatedAsyncioTestCase):
    async def test_broker_trennt_stummen_client(self):
        b = RdmnetBroker(BrokerKonfig(port=0, heartbeat_intervall_s=0.2, heartbeat_timeout_s=0.6))
        await b.starten()
        # Client ohne eigenen Heartbeat (sehr langes Intervall)
        k = RptKlient(K.RPT_CLIENT_TYP_GERAET, Uid(0x7FF0, 3), heartbeat_intervall_s=100, heartbeat_timeout_s=100)
        await k.verbinden("127.0.0.1", b.port)
        await asyncio.wait_for(k.getrennt.wait(), 5)
        self.assertEqual(b.zaehler["heartbeat_timeout"], 1)
        self.assertEqual(b.status()["clients"], [])
        await b.stoppen()

    async def test_heartbeat_haelt_verbindung(self):
        b = RdmnetBroker(BrokerKonfig(port=0, heartbeat_intervall_s=0.2, heartbeat_timeout_s=0.6))
        await b.starten()
        c = RdmnetController(heartbeat_intervall_s=0.2, heartbeat_timeout_s=0.6)
        await c.verbinden("127.0.0.1", b.port)
        await asyncio.sleep(1.5)
        self.assertTrue(c.verbunden)
        self.assertEqual(b.zaehler["heartbeat_timeout"], 0)
        await c.trennen()
        await b.stoppen()

    async def test_client_erkennt_broker_ausfall(self):
        b = RdmnetBroker(BrokerKonfig(port=0))
        await b.starten()
        c = RdmnetController()
        await c.verbinden("127.0.0.1", b.port)
        await b.stoppen()
        await asyncio.wait_for(c.getrennt.wait(), 3)
        self.assertFalse(c.verbunden)


if __name__ == "__main__":
    unittest.main()
