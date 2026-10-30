"""Kodierung: RDM (E1.20), Parameterdaten, RDMnet-PDUs (E1.33)."""

import struct
import unittest
import uuid

from pyrdmnet import konstanten as K
from pyrdmnet import pdu as P
from pyrdmnet import rdm_parameter as R
from pyrdmnet.rdm import (ALLE_CONTROLLER, ALLE_GERAETE, RdmFormatFehler, RdmNachricht, TransaktionsZaehler, Uid,
                           hersteller_aus_mid_broadcast, ist_geraete_broadcast)


class TestUid(unittest.TestCase):
    def test_umwandlungen(self):
        u = Uid(0x7FF0, 0x12345678)
        self.assertEqual(u.als_bytes(), bytes.fromhex("7ff012345678"))
        self.assertEqual(Uid.aus_bytes(u.als_bytes()), u)
        self.assertEqual(Uid.aus_text("7ff0:12345678"), u)
        self.assertEqual(Uid.aus_text("7FF012345678"), u)
        self.assertEqual(Uid.aus_int(u.als_int()), u)
        self.assertEqual(str(u), "7ff0:12345678")

    def test_eigenschaften(self):
        self.assertTrue(Uid(0xFFF0, 0).ist_dynamisch_angefragt)
        self.assertFalse(Uid(0xFFF0, 5).ist_dynamisch_angefragt)
        self.assertTrue(Uid(0x1234, 0xFFFFFFFF).ist_broadcast)
        self.assertFalse(Uid(0, 0).ist_gueltig_fuer_client)
        self.assertTrue(ist_geraete_broadcast(ALLE_GERAETE))
        self.assertTrue(ist_geraete_broadcast(Uid(0xFFFD, 0x6574FFFF)))
        self.assertEqual(hersteller_aus_mid_broadcast(Uid(0xFFFD, 0x6574FFFF)), 0x6574)
        self.assertIsNone(hersteller_aus_mid_broadcast(ALLE_GERAETE))
        self.assertFalse(ist_geraete_broadcast(ALLE_CONTROLLER))

    def test_wertebereich(self):
        with self.assertRaises(ValueError):
            Uid(0x10000, 0)
        with self.assertRaises(ValueError):
            Uid.aus_text("123")


class TestRdmNachricht(unittest.TestCase):
    def _beispiel(self):
        return RdmNachricht(Uid(0x7FF0, 0x201), Uid(0xFFF0, 1), 5, 1, K.CC_GET, K.PID_SENSOR_VALUE, b"\x02")

    def test_bytes_unabhaengig_gebaut(self):
        roh = self._beispiel().kodieren()
        soll = bytearray([0xCC, 0x01, 25]) + bytes.fromhex("7ff000000201") + bytes.fromhex("fff000000001")
        soll += bytes([5, 1, 0]) + b"\x00\x00" + bytes([0x20]) + b"\x02\x01" + bytes([1, 2])
        soll += struct.pack(">H", sum(soll) & 0xFFFF)
        self.assertEqual(roh, bytes(soll))

    def test_rundreise(self):
        n = self._beispiel()
        m = RdmNachricht.dekodieren(n.kodieren())
        self.assertEqual(m, n)

    def test_antwort(self):
        a = self._beispiel().antwort(K.ANTWORT_ACK, b"\x01\x02")
        self.assertEqual((a.ziel, a.quelle, a.command_class, a.transaktion), (Uid(0xFFF0, 1), Uid(0x7FF0, 0x201),
                                                                             K.CC_GET_RESPONSE, 5))
        self.assertTrue(a.ist_antwort)

    def test_fehlerhafte_nachrichten(self):
        roh = bytearray(self._beispiel().kodieren())
        for kaputt in (roh[:10], bytes([0xCD]) + roh[1:], roh[:-1] + bytes([roh[-1] ^ 1])):
            with self.assertRaises(RdmFormatFehler):
                RdmNachricht.dekodieren(bytes(kaputt))
        falsche_pdl = bytearray(roh)
        falsche_pdl[23] = 5
        with self.assertRaises(RdmFormatFehler):
            RdmNachricht.dekodieren(bytes(falsche_pdl))
        with self.assertRaises(RdmFormatFehler):
            RdmNachricht(Uid(1, 1), Uid(1, 2), 0, 0, K.CC_GET, 1, b"x" * 232).kodieren()

    def test_transaktionszaehler(self):
        tz, u = TransaktionsZaehler(), Uid(1, 1)
        self.assertEqual([tz.naechste(u) for _ in range(3)], [0, 1, 2])
        for _ in range(253):
            tz.naechste(u)
        self.assertEqual(tz.naechste(u), 0)  # Überlauf nach 255


class TestParameter(unittest.TestCase):
    def test_device_info(self):
        di = R.DeviceInfo(0x0100, 0x0101, 0x0101, 0x00010000, 6, 2, 3, 100, 0, 4)
        roh = R.baue_device_info(di)
        self.assertEqual(len(roh), 19)
        self.assertEqual(R.lese_device_info(roh), di)
        self.assertEqual(di.produktkategorie_text, "Scheinwerfer")
        with self.assertRaises(RdmFormatFehler):
            R.lese_device_info(roh[:-1])

    def test_sensor_skalierung_und_relevanz(self):
        sd = R.SensorDefinition(2, 0x01, 0x04, 0x01, 900, 2650, 2000, 2500, 1, "Netz")
        self.assertAlmostEqual(sd.skaliert(2304), 230.4)
        self.assertTrue(sd.messrelevant)
        self.assertEqual(sd.einheit_text, "V AC (eff.)")
        self.assertEqual(R.lese_sensor_definition(R.baue_sensor_definition(sd)), sd)
        speicher = R.SensorDefinition(3, 0x1D, 0x1C, 0x13, 0, 1, 0, 1, 0, "Speicher")
        self.assertFalse(speicher.messrelevant)
        self.assertEqual(speicher.faktor, 1000.0)
        w = R.SensorRohwert(2, -5, -10, 300, 7)
        self.assertEqual(R.lese_sensor_wert(R.baue_sensor_wert(w)), w)

    def test_personality_und_texte(self):
        p = R.Personality(2, 6, "Direkt 6ch")
        self.assertEqual(R.lese_personality_beschreibung(R.baue_personality_beschreibung(p)), p)
        self.assertEqual(R.lese_text(b"ABC\x00\x00"), "ABC")
        with self.assertRaises(RdmFormatFehler):
            R.lese_text(b"x" * 33)

    def test_endpoints(self):
        roh = R.baue_endpoint_liste(7, [R.Endpoint(1, 1), R.Endpoint(2, 0)])
        self.assertEqual(R.lese_endpoint_liste(roh), (7, [R.Endpoint(1, 1), R.Endpoint(2, 0)]))
        u = [Uid(1, 2), Uid(3, 4)]
        self.assertEqual(R.lese_endpoint_responder(R.baue_endpoint_responder(1, 9, u)), (1, 9, u))
        with self.assertRaises(RdmFormatFehler):
            R.lese_endpoint_liste(b"\x00\x00\x00\x01\x00")


class TestPdu(unittest.TestCase):
    cid = uuid.UUID("12345678-1234-5678-1234-567812345678")

    def test_flags_laenge(self):
        self.assertEqual(P.flags_laenge(0x12345), bytes([0xF1, 0x23, 0x45]))
        self.assertEqual(P.lese_flags_laenge(bytes([0xF0, 0x00, 0x05, 0, 0]), 0), (5, 3))
        with self.assertRaises(P.ProtokollFehler):
            P.lese_flags_laenge(bytes([0x80, 0x00, 0x05, 0, 0]), 0)  # Vererbung
        with self.assertRaises(P.ProtokollFehler):
            P.lese_flags_laenge(bytes([0xF0, 0x00, 0x09]), 0)  # zu lang

    def test_paket_aufbau(self):
        paket = P.baue_paket(K.VECTOR_ROOT_E133_BROKER, self.cid, P.baue_broker(K.VECTOR_BROKER_NULL))
        self.assertEqual(paket[:12], b"ASC-E1.17\x00\x00\x00")
        (block,) = struct.unpack(">I", paket[12:16])
        self.assertEqual(block, len(paket) - 16)
        self.assertEqual(block, 23 + 5)  # Root-Kopf + Broker-Kopf (Referenz: BROKER_NULL_MSG_SIZE)
        (root,) = P.lese_root_pdus(paket[16:])
        self.assertEqual((root.vektor, root.cid), (K.VECTOR_ROOT_E133_BROKER, self.cid))
        self.assertEqual(P.lese_broker(root.daten).vektor, K.VECTOR_BROKER_NULL)

    def test_connect_groessen_wie_referenz(self):
        e = P.ClientEintrag(self.cid, Uid(0xFFF0, 0), K.RPT_CLIENT_TYP_CONTROLLER)
        self.assertEqual(len(e.kodieren()), 23 + 23)  # CLIENT_ENTRY_HEADER_SIZE + RPT_CLIENT_ENTRY_DATA_SIZE
        cd = P.ConnectDaten("default", 1, "local.", 1, e)
        roh = cd.kodieren()
        self.assertEqual(len(roh), 3 + 63 + 231 + 46)  # CLIENT_CONNECT_COMMON_FIELD_SIZE + Eintrag
        z = P.lese_connect(roh)
        self.assertEqual((z.scope, z.version, z.domain, z.flags, z.client.uid, z.client.typ),
                         ("default", 1, "local.", 1, Uid(0xFFF0, 0), 1))
        rep = P.ConnectReply(0, 1, Uid(1, 2), Uid(3, 4))
        self.assertEqual(len(rep.kodieren()), 16)  # BROKER_CONNECT_REPLY_DATA_SIZE
        self.assertEqual(P.lese_connect_reply(rep.kodieren()), rep)

    def test_rpt_request_notification_status(self):
        rdm = RdmNachricht(Uid(1, 2), Uid(3, 4), 0, 1, K.CC_GET, K.PID_DEVICE_INFO).kodieren()
        kopf = P.RptKopf(K.VECTOR_RPT_REQUEST, Uid(3, 4), 0, Uid(1, 2), 1, 0xDEADBEEF)
        roh = P.baue_rpt(kopf, P.baue_request(rdm))
        self.assertEqual(len(roh), 28 + 7 + 3 + len(rdm))  # RPT_PDU_HEADER, REQUEST_NOTIF_HEADER, RDM_CMD
        r = P.lese_rpt(roh)
        self.assertEqual(r.kopf, kopf)
        self.assertEqual(P.lese_request_oder_notification(r.daten, K.VECTOR_REQUEST_RDM_CMD), [rdm])
        n = P.baue_notification([rdm, rdm])
        self.assertEqual(P.lese_request_oder_notification(n, K.VECTOR_NOTIFICATION_RDM_CMD), [rdm, rdm])
        self.assertEqual(P.lese_status(P.baue_status(4, "test")), (4, "test"))
        with self.assertRaises(P.ProtokollFehler):
            P.baue_rdm_cmd_pdu(b"\x01\x02")

    def test_dynamische_uids(self):
        rid = uuid.uuid4()
        anfrage = Uid(0xFFF0, 0).als_bytes() + rid.bytes
        (a,) = P.lese_dyn_uid_anfragen(anfrage)
        self.assertEqual((a.uid, a.rid), (Uid(0xFFF0, 0), rid))
        z = [(Uid(0xFFF0, 1), rid, 0)]
        self.assertEqual(P.lese_dyn_uid_zuordnungen(P.baue_dyn_uid_zuordnungen(z)), z)
        with self.assertRaises(P.ProtokollFehler):
            P.lese_dyn_uid_anfragen(b"\x00" * 21)


if __name__ == "__main__":
    unittest.main()
