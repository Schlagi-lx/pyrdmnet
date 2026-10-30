"""LLRP (E1.33 Abschnitt 5): Kodierung, Discovery, RDM über LLRP, Broker als Target."""

import asyncio
import itertools
import os
import socket
import struct
import unittest
import uuid
from unittest import mock

from pyrdmnet import konstanten as K
from pyrdmnet import pdu as P
from pyrdmnet import rdm_parameter as R
from pyrdmnet.broker import BrokerKonfig, RdmnetBroker
from pyrdmnet.controller import RdmNackFehler, RdmZeitFehler, SchreibSperreFehler
from pyrdmnet.llrp import (BROADCAST_CID, KomponentenResponder, LlrpManager, LlrpNetz, LlrpTarget, LlrpZiel,
                           ProbeAnfrage, ProbeAntwort, baue_llrp, baue_probe_anfrage, baue_probe_antwort,
                           lese_llrp, lese_probe_anfrage, lese_probe_antwort, lese_rdm_cmd)
from pyrdmnet.rdm import RdmNachricht, Uid

_ports = itertools.count(20000 + (os.getpid() % 2000) * 10, 2)


def netz(multicast: bool) -> LlrpNetz:
    p = next(_ports)
    if multicast:
        return LlrpNetz(adapter="127.0.0.1", anfrage_port=p, antwort_port=p + 1)
    return LlrpNetz(adapter="127.0.0.1", anfrage_ziel="127.0.0.1", antwort_ziel="127.0.0.1",
                    anfrage_port=p, antwort_port=p + 1, multicast=False)


def multicast_verfuegbar() -> bool:
    try:
        n = netz(True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", n.anfrage_port))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                         socket.inet_aton(n.anfrage_ziel) + socket.inet_aton("127.0.0.1"))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))
            s.settimeout(0.5)
            s.sendto(b"x", (n.anfrage_ziel, n.anfrage_port))
            return s.recv(10) == b"x"
    except OSError:
        return False


MULTICAST = multicast_verfuegbar()


class TestKodierung(unittest.TestCase):
    def test_paket_rundlauf(self):
        cid, ziel = uuid.uuid4(), uuid.uuid4()
        roh = baue_llrp(cid, K.VECTOR_LLRP_PROBE_REPLY, ziel, 0xFFFFFFFF, b"\xf0\x00\x04\x01")
        self.assertEqual(roh[:16], K.ACN_UDP_PREAMBEL)
        p = lese_llrp(roh)
        self.assertEqual((p.sender_cid, p.vektor, p.ziel_cid, p.transaktion, p.daten),
                         (cid, K.VECTOR_LLRP_PROBE_REPLY, ziel, 0xFFFFFFFF, b"\xf0\x00\x04\x01"))
        # Längen nach Tabelle 5-1: Root ab Oktett 16, LLRP-PDU ab 39, Daten ab 66
        self.assertEqual(len(baue_llrp(cid, 1, ziel, 1, b"")), 66)
        with self.assertRaises(P.ProtokollFehler):
            lese_llrp(b"\x00" * 70)

    def test_probe(self):
        a = ProbeAnfrage(Uid(0, 0), Uid(0xFFFF, 0xFFFFFFFF), K.LLRP_FILTER_NUR_BROKER, [Uid(1, 2), Uid(3, 4)])
        roh = baue_probe_anfrage(a)
        self.assertEqual(len(roh), 4 + 14 + 12)
        self.assertEqual(lese_probe_anfrage(roh), a)
        with self.assertRaises(P.ProtokollFehler):
            baue_probe_anfrage(ProbeAnfrage(Uid(0, 0), Uid(0, 1), 0, [Uid(1, i) for i in range(201)]))
        r = ProbeAntwort(Uid(0x7FF0, 9), b"\x01\x02\x03\x04\x05\x06", K.LLRP_TYP_BROKER)
        self.assertEqual(len(baue_probe_antwort(r)), 17)
        self.assertEqual(lese_probe_antwort(baue_probe_antwort(r)), r)

    def test_rdm_cmd_und_component_scope(self):
        n = RdmNachricht(ziel=Uid(1, 2), quelle=Uid(3, 4), transaktion=5, port_oder_antworttyp=1,
                         command_class=K.CC_GET, pid=K.PID_COMPONENT_SCOPE, daten=b"\x00\x01")
        self.assertEqual(lese_rdm_cmd(P.baue_rdm_cmd_pdu(n.kodieren())), n.kodieren())
        cs = R.ComponentScope(1, "labor", K.E133_STATISCHE_KONFIG_IPV4, "10.0.0.5", bytes(16), 8888)
        roh = R.baue_component_scope(cs)
        self.assertEqual(len(roh), 88)
        self.assertEqual(R.lese_component_scope(roh), cs)
        self.assertEqual(cs.statische_adresse, "10.0.0.5:8888")


class LlrpTest(unittest.IsolatedAsyncioTestCase):
    multicast = False

    async def asyncSetUp(self):
        self.netz = netz(self.multicast)
        self.targets: list[LlrpTarget] = []
        self.m = LlrpManager(self.netz, zeitlimit_s=0.15, wiederholungen=0)
        await self.m.starten()

    async def asyncTearDown(self):
        await self.m.stoppen()
        for t in self.targets:
            await t.stoppen()

    async def target(self, uid, typ=K.LLRP_TYP_GERAET, verbunden=False, **kw):
        r = KomponentenResponder(uid, **kw)
        t = LlrpTarget(uid, uuid.uuid4(), typ, r, self.netz, verbunden=lambda: verbunden, max_backoff_s=0.03)
        await t.starten()
        self.targets.append(t)
        return t, r


class TestEinTarget(LlrpTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.t, self.r = await self.target(Uid(0x7FF0, 0x42), scope="falsch")
        (self.ziel,) = await self.m.entdecken()

    async def test_gefunden_und_lesen(self):
        self.assertEqual((self.ziel.uid, self.ziel.cid, self.ziel.typ), (self.t.uid, self.t.cid, K.LLRP_TYP_GERAET))
        self.assertEqual(self.ziel.adresse, "127.0.0.1")
        self.assertEqual(len(self.ziel.hardware), 6)
        self.assertEqual((await self.m.scope_lesen(self.ziel)).scope, "falsch")
        self.assertEqual(await self.m.suchdomain_lesen(self.ziel), "local.")
        with self.assertRaises(RdmNackFehler) as ctx:
            await self.m.rdm_get(self.ziel, K.PID_DMX_START_ADDRESS)
        self.assertEqual(ctx.exception.grund, K.NR_UNKNOWN_PID)

    async def test_scope_setzen_mit_freigabe_und_ruecklesen(self):
        with self.assertRaises(SchreibSperreFehler):
            await self.m.scope_setzen(self.ziel, "labor")
        self.assertEqual(self.r.scope.scope, "falsch")
        self.m.schreibfreigabe = True
        alt, neu = await self.m.scope_setzen(self.ziel, "labor", statischer_broker=("192.168.1.10", 8888))
        self.assertEqual((alt.scope, neu.scope, neu.statische_adresse), ("falsch", "labor", "192.168.1.10:8888"))
        self.assertEqual(self.m.protokoll[-1]["alt"], ("falsch", None))
        self.assertEqual(await self.m.suchdomain_setzen(self.ziel, "studio."), ("local.", "studio."))
        with self.assertRaises(ValueError):
            await self.m.scope_setzen(self.ziel, "")

    async def test_set_nur_erlaubte_pids(self):
        self.m.schreibfreigabe = True
        with self.assertRaises(SchreibSperreFehler):
            await self.m.rdm_set(self.ziel, K.PID_DEVICE_LABEL, b"x")

    async def test_zeitueberschreitung(self):
        await self.t.stoppen()
        with self.assertRaises(RdmZeitFehler):
            await self.m.scope_lesen(self.ziel)

    async def test_fremde_cid_wird_ignoriert(self):
        anderes = LlrpZiel(uuid.uuid4(), self.ziel.uid, self.ziel.hardware, self.ziel.typ, "127.0.0.1")
        with self.assertRaises(RdmZeitFehler):
            await self.m.scope_lesen(anderes)

    async def test_subdevice_nack(self):
        n = RdmNachricht(ziel=self.t.uid, quelle=self.m.uid, transaktion=1, port_oder_antworttyp=1,
                         command_class=K.CC_GET, pid=K.PID_DEVICE_INFO, subdevice=3)
        fut = asyncio.get_running_loop().create_future()
        tn = self.m._senden(K.VECTOR_LLRP_RDM_CMD, self.t.cid, P.baue_rdm_cmd_pdu(n.kodieren()))
        self.m._offen[tn] = fut
        _, roh = await asyncio.wait_for(fut, 1)
        a = RdmNachricht.dekodieren(roh)
        self.assertEqual((a.antworttyp, struct.unpack(">H", a.daten)[0]),
                         (K.ANTWORT_NACK, K.NR_SUB_DEVICE_OUT_OF_RANGE))


@unittest.skipUnless(MULTICAST, "Multicast auf 127.0.0.1 nicht verfügbar")
class TestMehrereTargets(LlrpTest):
    multicast = True

    async def test_discovery_und_filter(self):
        await self.target(Uid(0x7FF0, 1))
        await self.target(Uid(0x7FF0, 2), verbunden=True)  # mit Broker verbunden
        await self.target(Uid(0x7FF0, 3), typ=K.LLRP_TYP_BROKER, verbunden=True)
        standard = {z.uid.geraet for z in await self.m.entdecken()}
        self.assertEqual(standard, {1, 3})  # Broker antworten immer
        self.assertEqual({z.uid.geraet for z in await self.m.entdecken(filter=0)}, {1, 2, 3})
        self.assertEqual({z.uid.geraet for z in await self.m.entdecken(filter=K.LLRP_FILTER_NUR_BROKER)}, {3})

    async def test_gleiche_uid_verschiedene_cid(self):
        dyn = Uid(0x8000 | 0x7FF0, 0)  # zwei Targets ohne zugewiesene dynamische UID
        a, _ = await self.target(dyn)
        b, _ = await self.target(dyn)
        ziele = await self.m.entdecken()
        self.assertEqual({z.cid for z in ziele}, {a.cid, b.cid})

    async def test_bereich_teilen(self):
        # je 3 Targets in unterer und oberer UID-Hälfte; bei Grenze 3 genügt eine Teilung
        for h in (0x0001, 0xFFF0):
            for i in range(3):
                await self.target(Uid(h, 0x1000 + i))
        gesendet = []
        original = self.m._senden
        self.m._senden = lambda v, c, d: (gesendet.append(lese_probe_anfrage(d)) if v == 1 else None,
                                         original(v, c, d))[1]
        with mock.patch.object(K, "LLRP_KNOWN_UID_SIZE", 3):
            ziele = await self.m.entdecken()
        self.assertEqual(len(ziele), 6)
        teilbereiche = {(a.untere, a.obere) for a in gesendet}
        self.assertIn((Uid(0, 0), Uid(0x7FFF, 0xFFFFFFFF)), teilbereiche)
        self.assertIn((Uid(0x8000, 0), Uid(0xFFFF, 0xFFFFFFFF)), teilbereiche)
        self.assertTrue(all(len(a.bekannte) <= 3 for a in gesendet))


class TestBrokerAlsTarget(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.netz = netz(MULTICAST)
        self.b = RdmnetBroker(BrokerKonfig(port=0, scope="labor", llrp=True, llrp_netz=self.netz))
        await self.b.starten()
        self.m = LlrpManager(self.netz, zeitlimit_s=0.15, wiederholungen=0)
        await self.m.starten()
        self.b.llrp_target.max_backoff_s = 0.03

    async def asyncTearDown(self):
        await self.m.stoppen()
        await self.b.stoppen()

    async def test_broker_findbar_scope_nur_lesbar(self):
        self.assertIsNotNone(self.b.llrp_target)
        (z,) = await self.m.entdecken(filter=K.LLRP_FILTER_NUR_BROKER)
        self.assertEqual((z.uid, z.cid, z.typ), (self.b.konfig.uid, self.b.konfig.cid, K.LLRP_TYP_BROKER))
        self.assertEqual((await self.m.scope_lesen(z)).scope, "labor")
        self.m.schreibfreigabe = True
        with self.assertRaises(RdmNackFehler) as ctx:
            await self.m.scope_setzen(z, "anders")
        self.assertEqual(ctx.exception.grund, K.NR_UNSUPPORTED_COMMAND_CLASS)
        await self.b.stoppen()
        self.assertIsNone(self.b.llrp_target)


if __name__ == "__main__":
    unittest.main()
