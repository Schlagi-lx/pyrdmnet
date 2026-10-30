"""Einbindung: synchrone Fassade mit Datenbank, wie sie das Messlabor nutzt."""

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pyrdmnet import settings_check as A
from pyrdmnet.__main__ import main as cli_main
from pyrdmnet.facade import RdmConfig, RdmModule
from pyrdmnet.simulator import SimulatedGateway
from pyrdmnet.storage import RdmStore

EXPECTED = A.ExpectedSetting("Direct 6ch", footprint=6, start_address=100, universe=1)


class TestFacade(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "mess.sqlite"
        self.module = RdmModule(RdmConfig(broker_port=0, db=self.db, timeout_s=1.0,
                                        event_file=Path(self.tmp.name) / "ereignisse.jsonl"))
        self.module.start()
        self.gw = SimulatedGateway()
        self.module._run(self.gw.connect("127.0.0.1", self.module.port))

    def tearDown(self):
        self.module._run(self.gw.disconnect())
        self.module.stop()
        self.tmp.cleanup()

    def test_complete_flow(self):
        self.gw.fixture.personality, self.gw.fixture.start_address = 1, 3
        (dmx_addr,) = self.module.find_devices()
        profile = self.module.read_device(dmx_addr)
        a = self.module.check_setting(profile, EXPECTED, reference="K1")
        self.assertEqual(a.status, A.DEVIATION)

        with self.assertRaises(ValueError):
            self.module.confirm_mapping(profile, "Direct 6ch", 9)
        self.module.confirm_mapping(profile, "Direct 6ch", 2, "Test")
        app, profile, a = self.module.apply_setting(profile, EXPECTED, reference="K1")
        self.assertFalse(app.success)  # ohne Schreibfreigabe
        self.module.set_write_enabled(True)
        app, profile, a = self.module.apply_setting(profile, EXPECTED, reference="K1")
        self.assertTrue(app.success, app.error)
        self.assertEqual((a.status, a.measurement_allowed), (A.OK, True))

        with self.module.accompany_measurement(profile, "M-1") as e:
            pass
        self.assertEqual((e["before"].status, e["after"].status), ("ok", "ok"))

        values = self.module.store.sensor_values_for_measurement("M-1")
        self.assertEqual([w["phase"] for w in values], ["before"] * 3 + ["after"] * 3)

        db = sqlite3.connect(self.db)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM rdm_device").fetchone()[0], 3)  # Erstauslesung + je eine nach beiden Anwendeversuchen
        self.assertEqual(db.execute("SELECT COUNT(*) FROM rdm_sensor_definition").fetchone()[0], 12)
        changed = db.execute("SELECT parameter, old, new, success FROM rdm_change ORDER BY id").fetchall()
        self.assertEqual(changed, [("NONE", None, changed[0][2], 0), ("DMX_PERSONALITY", "1", "2", 1),
                                ("DMX_START_ADDRESS", "3", "100", 1)])
        self.assertEqual([z[0] for z in db.execute("SELECT status FROM rdm_check ORDER BY id")],
                         ["DEVIATION", "DEVIATION", "OK"])
        json.loads(db.execute("SELECT profile_json FROM rdm_device").fetchone()[0])
        db.close()
        events = (Path(self.tmp.name) / "ereignisse.jsonl").read_text(encoding="utf-8").splitlines()
        self.assertTrue(any(json.loads(z)["event"] == "client_connected" for z in events))

    def test_measurement_runs_despite_rdm_failure(self):
        (dmx_addr,) = self.module.find_devices()
        profile = self.module.read_device(dmx_addr)
        self.gw.mute = True
        ran = False
        with self.module.accompany_measurement(profile, "M-2") as e:
            ran = True
        self.assertTrue(ran)
        self.assertEqual(e["before"].status, "failed")
        lines = self.module.store.sensor_values_for_measurement("M-2")
        self.assertEqual([(z["phase"], z["status"]) for z in lines], [("before", "failed"),
                                                                       ("after", "failed")])

    def test_reconnect_after_abort(self):
        (dmx_addr,) = self.module.find_devices()
        self.module._run(self.module.controller.disconnect())
        profile = self.module.read_device(dmx_addr)  # verbindet selbstständig neu
        self.assertIsNotNone(profile.device_info)

    def test_write_permission_survives_reconnect(self):
        self.module.set_write_enabled(True)
        self.module._run(self.module.controller.disconnect())
        self.module.find_devices()
        self.assertTrue(self.module.controller.write_enabled)


class TestStoreSchema(unittest.TestCase):
    def test_in_existing_connection(self):
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE measurement (id INTEGER PRIMARY KEY)")
        RdmStore(db)
        RdmStore(db)  # zweites Öffnen ändert nichts
        tables = {z[0] for z in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        db.close()
        self.assertTrue({"measurement", "rdm_device", "rdm_sensor_value", "rdm_personality_mapping"} <= tables)

    def test_wrong_schema_version(self):
        db = sqlite3.connect(":memory:")
        RdmStore(db)
        db.execute("UPDATE rdm_schema SET version = 99")
        with self.assertRaises(RuntimeError):
            RdmStore(db)
        db.close()


class TestCommandLine(unittest.TestCase):
    def test_demo(self):
        with tempfile.TemporaryDirectory() as t:
            with contextlib.redirect_stdout(io.StringIO()) as reader:
                self.assertEqual(cli_main(["demo", "--db", str(Path(t) / "d.sqlite")]), 0)
            self.assertIn("Neuer Abgleich: OK", reader.getvalue())


class TestDnssd(unittest.TestCase):
    def test_txt_matches_reference(self):
        import uuid
        from pyrdmnet.dnssd import txt_entries
        from pyrdmnet.rdm import Uid
        t = txt_entries("default", uuid.UUID("12345678-90ab-cdef-1234-567890abcdef"), Uid(0x7FF0, 1))
        self.assertEqual(t["CID"], "1234567890ABCDEF1234567890ABCDEF")
        self.assertEqual(t["UID"], "7FF000000001")
        self.assertEqual((t["TxtVers"], t["E133Vers"], t["E133Scope"]), ("1", "1", "default"))


if __name__ == "__main__":
    unittest.main()
