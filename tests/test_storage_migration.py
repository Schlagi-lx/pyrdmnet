"""Migration of pyrdmnet 0.4 databases (schema 1, German names) to schema 2."""

import json
import sqlite3
import unittest

from pyrdmnet.storage import SCHEMA_VERSION, RdmStore

# Schema 1 exactly as written by pyrdmnet 0.4
SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS rdm_schema (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS rdm_geraet (
    id INTEGER PRIMARY KEY,
    rdm_uid TEXT NOT NULL, rpt_uid TEXT NOT NULL, endpoint INTEGER NOT NULL,
    hersteller_id INTEGER NOT NULL, hersteller TEXT, modell TEXT, geraete_label TEXT,
    software_label TEXT, modell_id INTEGER, software_version_id INTEGER, produktkategorie INTEGER,
    personality INTEGER, personality_text TEXT, footprint INTEGER, startadresse INTEGER, universe INTEGER,
    geraete_stunden INTEGER, lampen_stunden INTEGER,
    ausgelesen_utc TEXT NOT NULL, profil_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS rdm_geraet_uid ON rdm_geraet(rdm_uid);

CREATE TABLE IF NOT EXISTS rdm_sensor_definition (
    geraet_id INTEGER NOT NULL REFERENCES rdm_geraet(id),
    sensor_nr INTEGER NOT NULL, typ INTEGER, typ_text TEXT, einheit INTEGER, einheit_text TEXT,
    praefix INTEGER, faktor REAL, bereich_min INTEGER, bereich_max INTEGER,
    normal_min INTEGER, normal_max INTEGER, recorded_unterstuetzung INTEGER, beschreibung TEXT,
    messrelevant INTEGER NOT NULL,
    PRIMARY KEY (geraet_id, sensor_nr)
);

CREATE TABLE IF NOT EXISTS rdm_schnappschuss (
    id INTEGER PRIMARY KEY,
    geraet_id INTEGER REFERENCES rdm_geraet(id),
    messung_ref TEXT, phase TEXT NOT NULL CHECK (phase IN ('vor','nach','einzeln')),
    zeit_utc TEXT NOT NULL, dauer_s REAL, status TEXT NOT NULL, fehler_json TEXT
);
CREATE INDEX IF NOT EXISTS rdm_schnappschuss_messung ON rdm_schnappschuss(messung_ref);

CREATE TABLE IF NOT EXISTS rdm_sensorwert (
    schnappschuss_id INTEGER NOT NULL REFERENCES rdm_schnappschuss(id),
    sensor_nr INTEGER NOT NULL, beschreibung TEXT, typ_text TEXT, einheit TEXT,
    wert REAL NOT NULL, roh INTEGER NOT NULL, niedrigster REAL, hoechster REAL, aufgezeichnet REAL,
    ausserhalb_normal INTEGER NOT NULL,
    PRIMARY KEY (schnappschuss_id, sensor_nr)
);

CREATE TABLE IF NOT EXISTS rdm_abgleich (
    id INTEGER PRIMARY KEY,
    geraet_id INTEGER REFERENCES rdm_geraet(id),
    bezug TEXT, gdtf_modus TEXT, zeit_utc TEXT NOT NULL,
    status TEXT NOT NULL, messung_erlaubt INTEGER NOT NULL, details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rdm_aenderung (
    id INTEGER PRIMARY KEY,
    geraet_id INTEGER REFERENCES rdm_geraet(id),
    bezug TEXT, zeit_utc TEXT NOT NULL, parameter TEXT NOT NULL, alt TEXT, neu TEXT,
    erfolgreich INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rdm_personality_zuordnung (
    hersteller_id INTEGER NOT NULL, modell_id INTEGER NOT NULL, software_version_id INTEGER NOT NULL,
    gdtf_modus TEXT NOT NULL, personality_nr INTEGER NOT NULL,
    bestaetigt_utc TEXT NOT NULL, bestaetigt_von TEXT,
    PRIMARY KEY (hersteller_id, modell_id, software_version_id, gdtf_modus)
);
"""


def v1_database() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA_V1)
    db.execute("INSERT INTO rdm_schema VALUES (1)")
    db.execute("INSERT INTO rdm_geraet VALUES (7, '7ff0:00000201', '7ff0:00000100', 1, 32752, 'Hersteller', "
               "'Modell', 'Label', 'SW', 257, 65536, 257, 2, 'Direkt 6ch', 6, 100, 1, 1234, NULL, "
               "'2026-10-01T10:00:00+00:00', '{\"hersteller\": \"Hersteller\"}')")
    db.execute("INSERT INTO rdm_sensor_definition VALUES (7, 0, 0, 'Temperatur', 1, '°C', 0, 1.0, -40, 150, 10, 85, "
               "3, 'LED Temperatur', 1)")
    for sid, phase in ((1, "vor"), (2, "nach"), (3, "einzeln")):
        db.execute("INSERT INTO rdm_schnappschuss VALUES (?, 7, 'm1', ?, '2026-10-01T10:00:00+00:00', 0.1, ?, ?)",
                   (sid, phase, "teilweise" if sid == 2 else "ok",
                    '[{"pid": "SENSOR_VALUE", "grund": "Zeitüberschreitung"}]' if sid == 2 else None))
        db.execute("INSERT INTO rdm_sensorwert VALUES (?, 0, 'LED Temperatur', 'Temperatur', '°C', 25.5, 255, "
                   "NULL, NULL, NULL, 0)", (sid,))
    details = ('{"status": "ABWEICHUNG", "messung_erlaubt": false, "abweichungen": ["Startadresse 1 statt 100"], '
               '"soll_personality": 2, "unbekannt": {"typ": 1}}')
    db.execute("INSERT INTO rdm_abgleich VALUES (1, 7, 'sitzung', 'Direkt 6ch', 't', 'ABWEICHUNG', 0, ?)", (details,))
    db.execute("INSERT INTO rdm_abgleich VALUES (2, 7, 'sitzung', 'Direkt 6ch', 't', 'OK_UNBESTAETIGT', 1, '{}')")
    db.execute("INSERT INTO rdm_aenderung VALUES (1, 7, 'sitzung', 't', 'KEINE', NULL, 'Fehler', 0)")
    db.execute("INSERT INTO rdm_aenderung VALUES (2, 7, 'sitzung', 't', 'DMX_START_ADDRESS', '1', '100', 1)")
    db.execute("INSERT INTO rdm_personality_zuordnung VALUES (32752, 257, 65536, 'Direkt 6ch', 2, 't', 'Stefan')")
    db.commit()
    return db


class TestMigrationV1(unittest.TestCase):
    def test_all_rows_migrated(self):
        db = v1_database()
        store = RdmStore(db)
        self.assertEqual(store.migrated_from, 1)
        self.assertEqual(db.execute("SELECT version FROM rdm_schema").fetchone()[0], SCHEMA_VERSION)
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertFalse(tables & {"rdm_geraet", "rdm_schnappschuss", "rdm_sensorwert", "rdm_abgleich",
                                   "rdm_aenderung", "rdm_personality_zuordnung", "rdm_sensor_definition_v1",
                                   "rdm_sensor_definition_v2"})
        self.assertEqual(db.execute("SELECT id, manufacturer_id, model, start_address, profile_json FROM rdm_device")
                         .fetchone(), (7, 32752, "Modell", 100, '{"manufacturer": "Hersteller"}'))
        self.assertEqual(db.execute("SELECT device_id, sensor_num, description, measurement_relevant "
                                    "FROM rdm_sensor_definition").fetchone(), (7, 0, "LED Temperatur", 1))
        self.assertEqual([r[0] for r in db.execute("SELECT phase FROM rdm_snapshot ORDER BY id")],
                         ["before", "after", "single"])
        self.assertEqual(len(store.sensor_values_for_measurement("m1")), 3)
        self.assertEqual([r[0] for r in db.execute("SELECT status FROM rdm_check ORDER BY id")],
                         ["DEVIATION", "OK_UNCONFIRMED"])
        # keys inside JSON are translated, nested too; free text and unknown keys stay as they are
        details = json.loads(db.execute("SELECT details_json FROM rdm_check WHERE id = 1").fetchone()[0])
        self.assertEqual(details, {"status": "DEVIATION", "measurement_allowed": False,
                                   "deviations": ["Startadresse 1 statt 100"], "target_personality": 2,
                                   "unbekannt": {"type": 1}})
        self.assertEqual([r[0] for r in db.execute("SELECT status FROM rdm_snapshot ORDER BY id")],
                         ["ok", "partial", "ok"])
        error = json.loads(db.execute("SELECT error_json FROM rdm_snapshot WHERE id = 2").fetchone()[0])
        self.assertEqual(error, [{"pid": "SENSOR_VALUE", "reason": "Zeitüberschreitung"}])
        self.assertEqual([r[0] for r in db.execute("SELECT parameter FROM rdm_change ORDER BY id")],
                         ["NONE", "DMX_START_ADDRESS"])
        m = store.find_mapping(32752, 257, 65536, "Direkt 6ch")
        self.assertEqual((m.personality_num, m.confirmed_by), (2, "Stefan"))
        self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_second_open_does_nothing(self):
        db = v1_database()
        RdmStore(db)
        self.assertIsNone(RdmStore(db).migrated_from)

    def test_new_database_not_migrated(self):
        db = sqlite3.connect(":memory:")
        self.assertIsNone(RdmStore(db).migrated_from)


if __name__ == "__main__":
    unittest.main()
