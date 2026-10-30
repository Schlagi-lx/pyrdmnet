"""S11 (V0.21.12): RDM über Art-Net – Pakete, TOD, Auslesen, Sensoren, SET mit Schreibfreigabe. Nur 127.0.0.1."""
import struct
import unittest

from pyrdmnet import artnet_rdm as AR, constants as C
from pyrdmnet.settings_check import ExpectedSetting
from pyrdmnet.facade import RdmConfig, RdmModule
from pyrdmnet.rdm import RdmMessage, Uid
from pyrdmnet.simulator import SimFixture


class Packets(unittest.TestCase):
    def test_tod_request(self):
        p = AR.tod_request([0, 1, 17])
        self.assertEqual(p[:8], b'Art-Net\x00')
        self.assertEqual(struct.unpack('<H', p[8:10])[0], 0x8000)
        self.assertEqual(p[10:12], b'\x00\x0e')
        self.assertEqual((p[21], p[22], p[23]), (0, 0, 3))
        self.assertEqual(list(p[24:27]), [0, 1, 17])
        self.assertEqual(len(p), 56)
        with self.assertRaises(AR.ArtnetFormatError):
            AR.tod_request([0, 256])                        # verschiedene Nets in einem Paket

    def test_art_rdm_without_start_code_and_back(self):
        a = RdmMessage(dest=Uid(0x1234, 5), source=Uid(0x7FF0, 1), transaction=7, port_or_response_type=1,
                         command_class=C.CC_GET, pid=C.PID_DEVICE_INFO, data=b'')
        raw = a.encode()
        p = AR.art_rdm(0x0102, raw)
        self.assertEqual(struct.unpack('<H', p[8:10])[0], 0x8300)
        self.assertEqual((p[12], p[21], p[22], p[23]), (1, 0x01, 0, 0x02))
        self.assertEqual(p[24:], raw[1:])                    # ohne 0xCC
        op, f = AR.parse_packet(p)
        self.assertEqual((op, f['port_address'], f['rdm']), (0x8300, 0x0102, raw))

    def test_foreign_packets(self):
        self.assertIsNone(AR.parse_packet(b'GARBAGE....'))
        self.assertEqual(AR.parse_packet(b'Art-Net\x00' + struct.pack('<H', 0x2000) + b'\x00\x0e')[1], None)  # ArtPoll


class WithNode(unittest.TestCase):
    def setUp(self):
        self.s1 = SimFixture(Uid(0x4A42, 0x538))
        self.s2 = SimFixture(Uid(0x4A42, 0x539), start_address=1)
        self.node = AR.SimulatedArtnetNode([self.s1, self.s2], port_address=0)
        self.m = RdmModule(RdmConfig(transport='ARTNET', artnet_target_ip='127.0.0.1', artnet_bind=('127.0.0.1', 0),
                                    artnet_target_port=0, artnet_tod_wait_s=0.2, timeout_s=1.0, strict=False))
        self.m.start()
        host, port = self.m._run(self.node.start())
        self.m.config.artnet_target_port = port
        self.m.controller.target_port = port

    def tearDown(self):
        self.m._run(self.node.stop())
        self.m.stop()

    def test_find_read_sensors(self):
        dmx_addr = self.m.find_devices()
        self.assertEqual(sorted(str(a.rdm_uid) for a in dmx_addr), sorted([str(self.s1.uid), str(self.s2.uid)]))
        p = self.m.read_device(next(a for a in dmx_addr if a.rdm_uid == self.s1.uid), save=False)
        self.assertEqual(p.device_info.start_address, 100)
        self.assertEqual(p.model, 'SIM-PAR RGBL')
        self.assertEqual(p.universe, 1)                     # Port-Adresse 0 = Universum 1
        self.assertTrue(p.sensors)
        s = self.m.sensor_snapshot(p)
        self.assertFalse(s.error, s.error)
        self.assertTrue(any('temperature' in w.description for w in s.values))

    def test_empty_tod_triggers_flush(self):
        self.node.discovery_needed = True
        dmx_addr = self.m.find_devices()
        self.assertEqual(len(dmx_addr), 2)
        self.assertIn(AR.OP_TOD_CONTROL, self.node.received)

    def test_set_only_with_write_permission(self):
        dmx_addr = self.m.find_devices()
        p = self.m.read_device(next(a for a in dmx_addr if a.rdm_uid == self.s2.uid), save=False)
        e = ExpectedSetting(p.personality_current.description if p.personality_current else 'Direct 6ch',
                                  footprint=p.footprint, start_address=200, universe=None)
        self.m.apply_setting(p, e)                   # ohne Schreibfreigabe: SET gesperrt (Fehler im Ergebnis)
        self.assertEqual(self.s2.start_address, 1)
        self.assertEqual(self.s2.set_log, [])
        self.m.set_write_enabled(True)
        try:
            app, new, ab = self.m.apply_setting(p, e)
        finally:
            self.m.set_write_enabled(False)
        self.assertEqual(self.s2.start_address, 200)
        self.assertEqual(new.device_info.start_address, 200)

    def test_unknown_uid(self):
        from pyrdmnet.readout import DeviceAddress
        u = Uid(0x1111, 1)
        p = self.m.read_device(DeviceAddress(u, 0, u), save=False)
        self.assertIsNone(p.device_info)
        self.assertTrue(p.unavailable)


if __name__ == '__main__':
    unittest.main()
