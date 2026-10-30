"""S11 (V0.21.12): RDM über Art-Net – Pakete, TOD, Auslesen, Sensoren, SET mit Schreibfreigabe. Nur 127.0.0.1."""
import struct
import unittest

from pyrdmnet import artnet_rdm as AR, konstanten as K
from pyrdmnet.abgleich import EinstellungsErwartung
from pyrdmnet.fassade import RdmKonfig, RdmModul
from pyrdmnet.rdm import RdmNachricht, Uid
from pyrdmnet.simulator import SimScheinwerfer


class Pakete(unittest.TestCase):
    def test_tod_request(self):
        p = AR.tod_request([0, 1, 17])
        self.assertEqual(p[:8], b'Art-Net\x00')
        self.assertEqual(struct.unpack('<H', p[8:10])[0], 0x8000)
        self.assertEqual(p[10:12], b'\x00\x0e')
        self.assertEqual((p[21], p[22], p[23]), (0, 0, 3))
        self.assertEqual(list(p[24:27]), [0, 1, 17])
        self.assertEqual(len(p), 56)
        with self.assertRaises(AR.ArtnetFormatFehler):
            AR.tod_request([0, 256])                        # verschiedene Nets in einem Paket

    def test_art_rdm_ohne_startcode_und_zurueck(self):
        a = RdmNachricht(ziel=Uid(0x1234, 5), quelle=Uid(0x7FF0, 1), transaktion=7, port_oder_antworttyp=1,
                         command_class=K.CC_GET, pid=K.PID_DEVICE_INFO, daten=b'')
        roh = a.kodieren()
        p = AR.art_rdm(0x0102, roh)
        self.assertEqual(struct.unpack('<H', p[8:10])[0], 0x8300)
        self.assertEqual((p[12], p[21], p[22], p[23]), (1, 0x01, 0, 0x02))
        self.assertEqual(p[24:], roh[1:])                    # ohne 0xCC
        op, f = AR.zerlegen(p)
        self.assertEqual((op, f['port_adresse'], f['rdm']), (0x8300, 0x0102, roh))

    def test_fremde_pakete(self):
        self.assertIsNone(AR.zerlegen(b'GARBAGE....'))
        self.assertEqual(AR.zerlegen(b'Art-Net\x00' + struct.pack('<H', 0x2000) + b'\x00\x0e')[1], None)  # ArtPoll


class MitNode(unittest.TestCase):
    def setUp(self):
        self.s1 = SimScheinwerfer(Uid(0x4A42, 0x538))
        self.s2 = SimScheinwerfer(Uid(0x4A42, 0x539), startadresse=1)
        self.node = AR.SimulierterArtnetNode([self.s1, self.s2], port_adresse=0)
        self.m = RdmModul(RdmKonfig(transport='ARTNET', artnet_ziel_ip='127.0.0.1', artnet_bind=('127.0.0.1', 0),
                                    artnet_ziel_port=0, artnet_tod_warte_s=0.2, timeout_s=1.0, strikt=False))
        self.m.starten()
        host, port = self.m._run(self.node.starten())
        self.m.konfig.artnet_ziel_port = port
        self.m.controller.ziel_port = port

    def tearDown(self):
        self.m._run(self.node.stoppen())
        self.m.stoppen()

    def test_finden_auslesen_sensoren(self):
        adr = self.m.geraete_finden()
        self.assertEqual(sorted(str(a.rdm_uid) for a in adr), sorted([str(self.s1.uid), str(self.s2.uid)]))
        p = self.m.geraet_auslesen(next(a for a in adr if a.rdm_uid == self.s1.uid), speichern=False)
        self.assertEqual(p.device_info.startadresse, 100)
        self.assertEqual(p.modell, 'SIM-PAR RGBL')
        self.assertEqual(p.universe, 1)                     # Port-Adresse 0 = Universum 1
        self.assertTrue(p.sensoren)
        s = self.m.sensor_schnappschuss(p)
        self.assertFalse(s.fehler, s.fehler)
        self.assertTrue(any('Temperatur' in w.beschreibung for w in s.werte))

    def test_leere_tod_loest_flush_aus(self):
        self.node.discovery_noetig = True
        adr = self.m.geraete_finden()
        self.assertEqual(len(adr), 2)
        self.assertIn(AR.OP_TOD_CONTROL, self.node.empfangen)

    def test_set_nur_mit_schreibfreigabe(self):
        adr = self.m.geraete_finden()
        p = self.m.geraet_auslesen(next(a for a in adr if a.rdm_uid == self.s2.uid), speichern=False)
        e = EinstellungsErwartung(p.personality_aktuell.beschreibung if p.personality_aktuell else 'Direkt 6ch',
                                  footprint=p.footprint, startadresse=200, universe=None)
        self.m.einstellung_anwenden(p, e)                   # ohne Schreibfreigabe: SET gesperrt (Fehler im Ergebnis)
        self.assertEqual(self.s2.startadresse, 1)
        self.assertEqual(self.s2.set_protokoll, [])
        self.m.schreibfreigabe_setzen(True)
        try:
            anw, neu, ab = self.m.einstellung_anwenden(p, e)
        finally:
            self.m.schreibfreigabe_setzen(False)
        self.assertEqual(self.s2.startadresse, 200)
        self.assertEqual(neu.device_info.startadresse, 200)

    def test_unbekannte_uid(self):
        from pyrdmnet.auslesen import GeraeteAdresse
        u = Uid(0x1111, 1)
        p = self.m.geraet_auslesen(GeraeteAdresse(u, 0, u), speichern=False)
        self.assertIsNone(p.device_info)
        self.assertTrue(p.nicht_verfuegbar)


if __name__ == '__main__':
    unittest.main()
