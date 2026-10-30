"""Einbindung: synchrone Fassade mit Datenbank, wie sie das Messlabor nutzt."""

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pyrdmnet import abgleich as A
from pyrdmnet.__main__ import main as cli_main
from pyrdmnet.fassade import RdmKonfig, RdmModul
from pyrdmnet.simulator import SimulierterGateway
from pyrdmnet.speicher import RdmSpeicher

ERW = A.EinstellungsErwartung("Direkt 6ch", footprint=6, startadresse=100, universe=1)


class TestFassade(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "mess.sqlite"
        self.modul = RdmModul(RdmKonfig(broker_port=0, db=self.db, timeout_s=1.0,
                                        ereignis_datei=Path(self.tmp.name) / "ereignisse.jsonl"))
        self.modul.starten()
        self.gw = SimulierterGateway()
        self.modul._run(self.gw.verbinden("127.0.0.1", self.modul.port))

    def tearDown(self):
        self.modul._run(self.gw.trennen())
        self.modul.stoppen()
        self.tmp.cleanup()

    def test_kompletter_ablauf(self):
        self.gw.scheinwerfer.personality, self.gw.scheinwerfer.startadresse = 1, 3
        (adr,) = self.modul.geraete_finden()
        profil = self.modul.geraet_auslesen(adr)
        a = self.modul.einstellung_pruefen(profil, ERW, bezug="K1")
        self.assertEqual(a.status, A.ABWEICHUNG)

        with self.assertRaises(ValueError):
            self.modul.zuordnung_bestaetigen(profil, "Direkt 6ch", 9)
        self.modul.zuordnung_bestaetigen(profil, "Direkt 6ch", 2, "Test")
        anw, profil, a = self.modul.einstellung_anwenden(profil, ERW, bezug="K1")
        self.assertFalse(anw.erfolgreich)  # ohne Schreibfreigabe
        self.modul.schreibfreigabe_setzen(True)
        anw, profil, a = self.modul.einstellung_anwenden(profil, ERW, bezug="K1")
        self.assertTrue(anw.erfolgreich, anw.fehler)
        self.assertEqual((a.status, a.messung_erlaubt), (A.OK, True))

        with self.modul.messung_begleiten(profil, "M-1") as e:
            pass
        self.assertEqual((e["vor"].status, e["nach"].status), ("ok", "ok"))

        werte = self.modul.speicher.sensorwerte_zu_messung("M-1")
        self.assertEqual([w["phase"] for w in werte], ["vor"] * 3 + ["nach"] * 3)

        db = sqlite3.connect(self.db)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM rdm_geraet").fetchone()[0], 3)  # Erstauslesung + je eine nach beiden Anwendeversuchen
        self.assertEqual(db.execute("SELECT COUNT(*) FROM rdm_sensor_definition").fetchone()[0], 12)
        aend = db.execute("SELECT parameter, alt, neu, erfolgreich FROM rdm_aenderung ORDER BY id").fetchall()
        self.assertEqual(aend, [("KEINE", None, aend[0][2], 0), ("DMX_PERSONALITY", "1", "2", 1),
                                ("DMX_START_ADDRESS", "3", "100", 1)])
        self.assertEqual([z[0] for z in db.execute("SELECT status FROM rdm_abgleich ORDER BY id")],
                         ["ABWEICHUNG", "ABWEICHUNG", "OK"])
        json.loads(db.execute("SELECT profil_json FROM rdm_geraet").fetchone()[0])
        db.close()
        ereignisse = (Path(self.tmp.name) / "ereignisse.jsonl").read_text(encoding="utf-8").splitlines()
        self.assertTrue(any(json.loads(z)["ereignis"] == "client_verbunden" for z in ereignisse))

    def test_messung_laeuft_trotz_rdm_ausfall(self):
        (adr,) = self.modul.geraete_finden()
        profil = self.modul.geraet_auslesen(adr)
        self.gw.stumm = True
        gelaufen = False
        with self.modul.messung_begleiten(profil, "M-2") as e:
            gelaufen = True
        self.assertTrue(gelaufen)
        self.assertEqual(e["vor"].status, "fehlgeschlagen")
        zeilen = self.modul.speicher.sensorwerte_zu_messung("M-2")
        self.assertEqual([(z["phase"], z["status"]) for z in zeilen], [("vor", "fehlgeschlagen"),
                                                                       ("nach", "fehlgeschlagen")])

    def test_neuverbindung_nach_abbruch(self):
        (adr,) = self.modul.geraete_finden()
        self.modul._run(self.modul.controller.trennen())
        profil = self.modul.geraet_auslesen(adr)  # verbindet selbstständig neu
        self.assertIsNotNone(profil.device_info)

    def test_schreibfreigabe_bleibt_nach_neuverbindung(self):
        self.modul.schreibfreigabe_setzen(True)
        self.modul._run(self.modul.controller.trennen())
        self.modul.geraete_finden()
        self.assertTrue(self.modul.controller.schreibfreigabe)


class TestSpeicherSchema(unittest.TestCase):
    def test_in_bestehender_verbindung(self):
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE messung (id INTEGER PRIMARY KEY)")
        RdmSpeicher(db)
        RdmSpeicher(db)  # zweites Öffnen ändert nichts
        tabellen = {z[0] for z in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        db.close()
        self.assertTrue({"messung", "rdm_geraet", "rdm_sensorwert", "rdm_personality_zuordnung"} <= tabellen)

    def test_falsche_schemaversion(self):
        db = sqlite3.connect(":memory:")
        RdmSpeicher(db)
        db.execute("UPDATE rdm_schema SET version = 99")
        with self.assertRaises(RuntimeError):
            RdmSpeicher(db)
        db.close()


class TestKommandozeile(unittest.TestCase):
    def test_demo(self):
        with tempfile.TemporaryDirectory() as t:
            with contextlib.redirect_stdout(io.StringIO()) as aus:
                self.assertEqual(cli_main(["demo", "--db", str(Path(t) / "d.sqlite")]), 0)
            self.assertIn("Neuer Abgleich: OK", aus.getvalue())


class TestDnssd(unittest.TestCase):
    def test_txt_wie_referenz(self):
        import uuid
        from pyrdmnet.dnssd import txt_eintraege
        from pyrdmnet.rdm import Uid
        t = txt_eintraege("default", uuid.UUID("12345678-90ab-cdef-1234-567890abcdef"), Uid(0x7FF0, 1))
        self.assertEqual(t["CID"], "1234567890ABCDEF1234567890ABCDEF")
        self.assertEqual(t["UID"], "7FF000000001")
        self.assertEqual((t["TxtVers"], t["E133Vers"], t["E133Scope"]), ("1", "1", "default"))


if __name__ == "__main__":
    unittest.main()
