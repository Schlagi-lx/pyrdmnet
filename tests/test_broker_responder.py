"""Broker-eigener RDM-Responder (E1.33 6.2): Anfragen an die Broker-UID."""

import asyncio
import struct
import unittest

from pyrdmnet import konstanten as K
from pyrdmnet import pdu as P
from pyrdmnet import rdm_parameter as R
from pyrdmnet.broker_responder import BrokerResponder, BrokerResponderDaten, baue_component_scope
from pyrdmnet.controller import RdmNackFehler, RptStatusFehler, SchreibSperreFehler
from pyrdmnet.rdm import RdmNachricht, Uid

from hilfen import NetzTest

BROKER = Uid(K.ESTA_PROTOTYP_HERSTELLER, 1)
CONTROLLER = Uid(K.ESTA_PROTOTYP_HERSTELLER, 0x99)


def anfrage(pid, daten=b"", cc=K.CC_GET, ziel=BROKER, subdevice=K.RDM_ROOT_DEVICE):
    return RdmNachricht(ziel=ziel, quelle=CONTROLLER, transaktion=7, port_oder_antworttyp=1,
                        command_class=cc, pid=pid, daten=daten, subdevice=subdevice)


class TestResponderDirekt(unittest.TestCase):
    def setUp(self):
        self.ereignisse = []
        self.r = BrokerResponder(BrokerResponderDaten(uid=BROKER, scope="labor", suchdomain="local."),
                                 ereignis=lambda art, **kw: self.ereignisse.append((art, kw)))

    def _ack(self, a):
        n = self.r.beantworten(a)
        self.assertEqual(n.antworttyp, K.ANTWORT_ACK, n)
        self.assertEqual((n.ziel, n.quelle, n.transaktion), (CONTROLLER, BROKER, 7))
        return n.daten

    def _nack(self, a):
        n = self.r.beantworten(a)
        self.assertEqual(n.antworttyp, K.ANTWORT_NACK)
        return struct.unpack(">H", n.daten)[0]

    def test_component_scope(self):
        d = self._ack(anfrage(K.PID_COMPONENT_SCOPE, b"\x00\x01"))
        self.assertEqual(len(d), 2 + 63 + 1 + 4 + 16 + 2)
        self.assertEqual(d[:2], b"\x00\x01")
        self.assertEqual(d[2:65].rstrip(b"\x00"), b"labor")
        self.assertEqual(d[64], 0)  # nullterminiert
        self.assertEqual(d[65], K.E133_KEINE_STATISCHE_KONFIG)
        self.assertEqual(self._nack(anfrage(K.PID_COMPONENT_SCOPE, b"\x00\x02")), K.NR_DATA_OUT_OF_RANGE)
        self.assertEqual(self._nack(anfrage(K.PID_COMPONENT_SCOPE)), K.NR_FORMAT_ERROR)

    def test_langer_scope_wird_abgeschnitten(self):
        d = baue_component_scope(1, "x" * 80)
        self.assertEqual(d[2:65], b"x" * 62 + b"\x00")

    def test_search_domain_und_broker_status(self):
        self.assertEqual(self._ack(anfrage(K.PID_SEARCH_DOMAIN)), b"local.")
        self.assertEqual(self._ack(anfrage(K.PID_BROKER_STATUS)), bytes((0, K.E133_BROKER_AKTIV)))

    def test_basis_pids(self):
        pids = self._ack(anfrage(K.PID_SUPPORTED_PARAMETERS))
        liste = [struct.unpack(">H", pids[i:i + 2])[0] for i in range(0, len(pids), 2)]
        for p in (K.PID_COMPONENT_SCOPE, K.PID_SEARCH_DOMAIN, K.PID_BROKER_STATUS, K.PID_MANUFACTURER_LABEL):
            self.assertIn(p, liste)
        di = R.lese_device_info(self._ack(anfrage(K.PID_DEVICE_INFO)))
        self.assertEqual((di.footprint, di.startadresse, di.sensor_anzahl), (0, 0xFFFF, 0))
        self.assertTrue(self._ack(anfrage(K.PID_SOFTWARE_VERSION_LABEL)).startswith(b"pyrdmnet"))
        self.assertEqual(self._nack(anfrage(K.PID_DMX_START_ADDRESS)), K.NR_UNKNOWN_PID)

    def test_set_nur_identify(self):
        self.assertEqual(self._nack(anfrage(K.PID_COMPONENT_SCOPE, b"x", cc=K.CC_SET)),
                         K.NR_UNSUPPORTED_COMMAND_CLASS)
        self.assertEqual(self._nack(anfrage(K.PID_DMX_START_ADDRESS, b"\x00\x01", cc=K.CC_SET)), K.NR_UNKNOWN_PID)
        self._ack(anfrage(K.PID_IDENTIFY_DEVICE, b"\x01", cc=K.CC_SET))
        self.assertEqual(self._ack(anfrage(K.PID_IDENTIFY_DEVICE)), b"\x01")
        self.assertEqual(self.ereignisse[-1][0], "broker_identify")
        self.assertEqual(self._nack(anfrage(K.PID_IDENTIFY_DEVICE, b"\x02", cc=K.CC_SET)), K.NR_FORMAT_ERROR)

    def test_subdevice_und_broadcast(self):
        self.assertEqual(self._nack(anfrage(K.PID_DEVICE_INFO, subdevice=1)), K.NR_SUB_DEVICE_OUT_OF_RANGE)
        self.assertIsNone(self.r.beantworten(anfrage(K.PID_DEVICE_INFO, ziel=Uid(0xFFFF, 0xFFFFFFFF))))


class TestResponderUeberNetz(NetzTest):
    broker_konfig = {"scope": "default", "suchdomain": "local."}

    async def test_get_ueber_rdmnet(self):
        b = self.broker.konfig.uid
        self.assertEqual(self.c.broker_uid, b)
        d = await self.c.rdm_get(b, K.PID_COMPONENT_SCOPE, b"\x00\x01", timeout_s=2)
        self.assertEqual(d[2:65].rstrip(b"\x00"), b"default")
        self.assertEqual(await self.c.rdm_get(b, K.PID_SEARCH_DOMAIN, timeout_s=2), b"local.")
        self.assertEqual(await self.c.rdm_get(b, K.PID_BROKER_STATUS, timeout_s=2), b"\x00\x01")
        self.assertEqual(self.broker.zaehler["broker_rdm_anfrage"], 3)

    async def test_nack_und_status(self):
        b = self.broker.konfig.uid
        with self.assertRaises(RdmNackFehler) as ctx:
            await self.c.rdm_get(b, K.PID_DMX_PERSONALITY, timeout_s=2)
        self.assertEqual(ctx.exception.grund, K.NR_UNKNOWN_PID)
        with self.assertRaises(RptStatusFehler) as ctx:
            await self.c.rdm_get(b, K.PID_DEVICE_INFO, endpoint=1, timeout_s=2)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_UNKNOWN_ENDPOINT)
        with self.assertRaises(RptStatusFehler) as ctx:
            await self.c.rdm_get(b, K.PID_DEVICE_INFO, rdm_ziel=Uid(0x7FF0, 0x1234), timeout_s=2)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_UNKNOWN_RDM_UID)

    async def test_identify_braucht_schreibfreigabe(self):
        b = self.broker.konfig.uid
        with self.assertRaises(SchreibSperreFehler):
            await self.c.rdm_set(b, K.PID_IDENTIFY_DEVICE, b"\x01", timeout_s=2)
        self.c.schreibfreigabe = True
        await self.c.rdm_set(b, K.PID_IDENTIFY_DEVICE, b"\x01", timeout_s=2)
        self.assertTrue(self.broker.responder.identifizieren)

    async def test_mehrere_rdm_befehle_ungueltig(self):
        b = self.broker.konfig.uid
        a = anfrage(K.PID_DEVICE_INFO, ziel=b).kodieren()
        kopf = P.RptKopf(K.VECTOR_RPT_REQUEST, self.c.uid, 0, b, 0, 4711)
        fut = self.c._offen[4711] = asyncio.get_running_loop().create_future()
        cmds = P.baue_rdm_cmd_pdu(a) * 2  # E1.33: genau ein RDM-Befehl je Request erlaubt
        daten = P.flags_laenge(7 + len(cmds)) + struct.pack(">I", K.VECTOR_REQUEST_RDM_CMD) + cmds
        await self.c.sende_rpt(kopf, daten)
        with self.assertRaises(RptStatusFehler) as ctx:
            await asyncio.wait_for(fut, 2)
        self.assertEqual(ctx.exception.code, K.RPT_STATUS_INVALID_MESSAGE)


if __name__ == "__main__":
    unittest.main()
