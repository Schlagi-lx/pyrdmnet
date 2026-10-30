"""SQLite storage for RDM device profiles, settings checks and sensor data.

The tables use the prefix ``rdm_`` and can live in the measurement lab's
database (same file) or in a file of their own. The link to a measurement is
``measurement_ref`` (text, e.g. the measurement ID of the lab database), which
keeps this module independent of the lab's schema.

Schema version 2 uses English table and column names. Databases written by
pyrdmnet 0.4 (schema version 1, German names) are migrated automatically on
open, including the keys inside the JSON columns (``profile_json``,
``details_json``, ``error_json``). Free text inside JSON (hints, error
messages) stays in the language it was written in.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .readout import DeviceProfile, SensorSnapshot
from .settings_check import ApplyResult, CheckResult, PersonalityMapping

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rdm_schema (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS rdm_device (
    id INTEGER PRIMARY KEY,
    rdm_uid TEXT NOT NULL, rpt_uid TEXT NOT NULL, endpoint INTEGER NOT NULL,
    manufacturer_id INTEGER NOT NULL, manufacturer TEXT, model TEXT, device_label TEXT,
    software_label TEXT, model_id INTEGER, software_version_id INTEGER, product_category INTEGER,
    personality INTEGER, personality_text TEXT, footprint INTEGER, start_address INTEGER, universe INTEGER,
    device_hours INTEGER, lamp_hours INTEGER,
    read_utc TEXT NOT NULL, profile_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS rdm_device_uid ON rdm_device(rdm_uid);

CREATE TABLE IF NOT EXISTS rdm_sensor_definition (
    device_id INTEGER NOT NULL REFERENCES rdm_device(id),
    sensor_num INTEGER NOT NULL, type INTEGER, type_text TEXT, unit INTEGER, unit_text TEXT,
    prefix INTEGER, factor REAL, range_min INTEGER, range_max INTEGER,
    normal_min INTEGER, normal_max INTEGER, recorded_support INTEGER, description TEXT,
    measurement_relevant INTEGER NOT NULL,
    PRIMARY KEY (device_id, sensor_num)
);

CREATE TABLE IF NOT EXISTS rdm_snapshot (
    id INTEGER PRIMARY KEY,
    device_id INTEGER REFERENCES rdm_device(id),
    measurement_ref TEXT, phase TEXT NOT NULL CHECK (phase IN ('before','after','single')),
    time_utc TEXT NOT NULL, duration_s REAL, status TEXT NOT NULL, error_json TEXT
);
CREATE INDEX IF NOT EXISTS rdm_snapshot_measurement ON rdm_snapshot(measurement_ref);

CREATE TABLE IF NOT EXISTS rdm_sensor_value (
    snapshot_id INTEGER NOT NULL REFERENCES rdm_snapshot(id),
    sensor_num INTEGER NOT NULL, description TEXT, type_text TEXT, unit TEXT,
    value REAL NOT NULL, raw INTEGER NOT NULL, lowest REAL, highest REAL, recorded REAL,
    outside_normal INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, sensor_num)
);

CREATE TABLE IF NOT EXISTS rdm_check (
    id INTEGER PRIMARY KEY,
    device_id INTEGER REFERENCES rdm_device(id),
    reference TEXT, gdtf_mode TEXT, time_utc TEXT NOT NULL,
    status TEXT NOT NULL, measurement_allowed INTEGER NOT NULL, details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rdm_change (
    id INTEGER PRIMARY KEY,
    device_id INTEGER REFERENCES rdm_device(id),
    reference TEXT, time_utc TEXT NOT NULL, parameter TEXT NOT NULL, old TEXT, new TEXT,
    success INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rdm_personality_mapping (
    manufacturer_id INTEGER NOT NULL, model_id INTEGER NOT NULL, software_version_id INTEGER NOT NULL,
    gdtf_mode TEXT NOT NULL, personality_num INTEGER NOT NULL,
    confirmed_utc TEXT NOT NULL, confirmed_by TEXT,
    PRIMARY KEY (manufacturer_id, model_id, software_version_id, gdtf_mode)
);
"""

# Schema 1 (pyrdmnet 0.4, German) -> schema 2: old table, new table, column pairs (old, new).
# Columns are copied in this order; ids and foreign keys stay the same.
_MIGRATION_V1 = [
    ("rdm_geraet", "rdm_device", [
        ("id", "id"), ("rdm_uid", "rdm_uid"), ("rpt_uid", "rpt_uid"), ("endpoint", "endpoint"),
        ("hersteller_id", "manufacturer_id"), ("hersteller", "manufacturer"), ("modell", "model"),
        ("geraete_label", "device_label"), ("software_label", "software_label"), ("modell_id", "model_id"),
        ("software_version_id", "software_version_id"), ("produktkategorie", "product_category"),
        ("personality", "personality"), ("personality_text", "personality_text"), ("footprint", "footprint"),
        ("startadresse", "start_address"), ("universe", "universe"), ("geraete_stunden", "device_hours"),
        ("lampen_stunden", "lamp_hours"), ("ausgelesen_utc", "read_utc"), ("profil_json", "profile_json")]),
    ("rdm_sensor_definition", "rdm_sensor_definition_v2", [
        ("geraet_id", "device_id"), ("sensor_nr", "sensor_num"), ("typ", "type"), ("typ_text", "type_text"),
        ("einheit", "unit"), ("einheit_text", "unit_text"), ("praefix", "prefix"), ("faktor", "factor"),
        ("bereich_min", "range_min"), ("bereich_max", "range_max"), ("normal_min", "normal_min"),
        ("normal_max", "normal_max"), ("recorded_unterstuetzung", "recorded_support"),
        ("beschreibung", "description"), ("messrelevant", "measurement_relevant")]),
    ("rdm_schnappschuss", "rdm_snapshot", [
        ("id", "id"), ("geraet_id", "device_id"), ("messung_ref", "measurement_ref"),
        ("CASE phase WHEN 'vor' THEN 'before' WHEN 'nach' THEN 'after' ELSE 'single' END", "phase"),
        ("zeit_utc", "time_utc"), ("dauer_s", "duration_s"), ("status", "status"), ("fehler_json", "error_json")]),
    ("rdm_sensorwert", "rdm_sensor_value", [
        ("schnappschuss_id", "snapshot_id"), ("sensor_nr", "sensor_num"), ("beschreibung", "description"),
        ("typ_text", "type_text"), ("einheit", "unit"), ("wert", "value"), ("roh", "raw"),
        ("niedrigster", "lowest"), ("hoechster", "highest"), ("aufgezeichnet", "recorded"),
        ("ausserhalb_normal", "outside_normal")]),
    ("rdm_abgleich", "rdm_check", [
        ("id", "id"), ("geraet_id", "device_id"), ("bezug", "reference"), ("gdtf_modus", "gdtf_mode"),
        ("zeit_utc", "time_utc"), ("status", "status"), ("messung_erlaubt", "measurement_allowed"),
        ("details_json", "details_json")]),
    ("rdm_aenderung", "rdm_change", [
        ("id", "id"), ("geraet_id", "device_id"), ("bezug", "reference"), ("zeit_utc", "time_utc"),
        ("parameter", "parameter"), ("alt", "old"), ("neu", "new"), ("erfolgreich", "success")]),
    ("rdm_personality_zuordnung", "rdm_personality_mapping", [
        ("hersteller_id", "manufacturer_id"), ("modell_id", "model_id"), ("software_version_id", "software_version_id"),
        ("gdtf_modus", "gdtf_mode"), ("personality_nr", "personality_num"), ("bestaetigt_utc", "confirmed_utc"),
        ("bestaetigt_von", "confirmed_by")]),
]
# status values of schema 1 -> schema 2 (settings checks and sensor snapshots)
_STATUS_V1 = {"ABWEICHUNG": "DEVIATION", "NICHT_PRUEFBAR": "NOT_CHECKABLE", "OK_UNBESTAETIGT": "OK_UNCONFIRMED",
              "teilweise": "partial", "fehlgeschlagen": "failed"}
# JSON keys written by pyrdmnet 0.4 -> 0.5 (profile_json, details_json, error_json); unknown keys stay as they are
_JSON_KEYS_V1 = {
    "abgleich": "check", "abweichungen": "deviations", "adresse": "address", "aenderungen": "changes",
    "aktiv": "active", "aktuell": "current", "alt": "old", "antwort": "response", "anwendung": "application",
    "aufgezeichnet": "recorded", "ausgelesen_utc": "read_utc", "ausserhalb_normal": "outside_normal",
    "bereich_max": "range_max", "bereich_min": "range_min", "bericht": "report", "beschreibung": "description",
    "bestaetigt_utc": "confirmed_utc", "bestaetigt_von": "confirmed_by", "checkliste": "checklist",
    "dauer_s": "duration_s", "einheit": "unit", "einheit_text": "unit_text", "ende_utc": "end_utc",
    "ereignis": "event", "erfolgreich": "success", "erwartung": "expected", "faktor": "factor", "fehler": "error",
    "gdtf_modus": "gdtf_mode", "gegenstelle": "peer", "geraete": "devices", "geraete_label": "device_label",
    "geraete_stunden": "device_hours", "gesetzt": "set", "grund": "reason", "hersteller": "manufacturer",
    "hersteller_id": "manufacturer_id", "hinweise": "hints", "hoechster": "highest", "ist": "actual",
    "lampen_stunden": "lamp_hours", "messrelevant": "measurement_relevant",
    "messung_erlaubt": "measurement_allowed", "modell": "model", "modell_id": "model_id", "modus": "mode",
    "neu": "new", "nicht_verfuegbar": "unavailable", "niedrigster": "lowest", "nummer": "number",
    "personality_aktuell": "personality_current", "personality_anzahl": "personality_count",
    "personality_nr": "personality_num", "praefix": "prefix", "produktkategorie": "product_category",
    "produktkategorie_text": "product_category_text", "profil": "profile", "profil_nachher": "profile_after",
    "punkt": "item", "rdm_protokoll": "rdm_protocol", "recorded_unterstuetzung": "recorded_support", "roh": "raw",
    "schritt": "step", "schritte": "steps", "sensor_anzahl": "sensor_count", "sensoren": "sensors",
    "soll": "target", "soll_personality": "target_personality", "startadresse": "start_address",
    "statisch_ipv4": "static_ipv4", "statisch_ipv6": "static_ipv6", "statisch_port": "static_port",
    "statisch_typ": "static_type", "subdevice_anzahl": "subdevice_count", "typ": "type", "typ_text": "type_text",
    "unterstuetzte_pids": "supported_pids", "vorher": "before", "wert": "value", "werte": "values",
    "zeit_utc": "time_utc", "ziel": "target", "ziel_adresse": "target_address", "ziel_modus": "target_mode",
    "zurueck": "return", "zurueck_adresse": "return_address", "zurueck_modus": "return_mode",
}


def _translate_json_v1(value):
    """Translate the keys (and "status" values) of a JSON document written by pyrdmnet 0.4."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            new_key = _JSON_KEYS_V1.get(key, key)
            item = _translate_json_v1(item)
            if new_key == "status" and isinstance(item, str):
                item = _STATUS_V1.get(item, item)
            out[new_key] = item
        return out
    if isinstance(value, list):
        return [_translate_json_v1(item) for item in value]
    return value


def _translate_json_column(db: sqlite3.Connection, table: str, column: str, key: str) -> None:
    rows = db.execute(f"SELECT {key}, {column} FROM {table} WHERE {column} IS NOT NULL").fetchall()
    for row_id, text in rows:
        try:
            doc = json.loads(text)
        except ValueError:
            continue  # not JSON: leave unchanged
        db.execute(f"UPDATE {table} SET {column} = ? WHERE {key} = ?",
                   (json.dumps(_translate_json_v1(doc), ensure_ascii=False), row_id))


def _migrate_v1(db: sqlite3.Connection) -> None:
    """Rename schema 1 tables/columns to schema 2 in one transaction."""
    db.execute("PRAGMA foreign_keys = OFF")
    try:
        with db:
            # rdm_sensor_definition keeps its name: move the old one aside first
            db.execute("ALTER TABLE rdm_sensor_definition RENAME TO rdm_sensor_definition_v1")
            db.executescript("DROP INDEX IF EXISTS rdm_geraet_uid; DROP INDEX IF EXISTS rdm_schnappschuss_messung;")
            db.executescript(_SCHEMA.replace("rdm_sensor_definition (", "rdm_sensor_definition_v2 ("))
            for old, new, pairs in _MIGRATION_V1:
                source = "rdm_sensor_definition_v1" if old == "rdm_sensor_definition" else old
                cols_new = ", ".join(n for _, n in pairs)
                cols_old = ", ".join(o for o, _ in pairs)
                db.execute(f"INSERT INTO {new} ({cols_new}) SELECT {cols_old} FROM {source}")
                db.execute(f"DROP TABLE {source}")
            db.execute("ALTER TABLE rdm_sensor_definition_v2 RENAME TO rdm_sensor_definition")
            for old, new in _STATUS_V1.items():
                db.execute("UPDATE rdm_check SET status = ? WHERE status = ?", (new, old))
                db.execute("UPDATE rdm_snapshot SET status = ? WHERE status = ?", (new, old))
            _translate_json_column(db, "rdm_device", "profile_json", "id")
            _translate_json_column(db, "rdm_check", "details_json", "id")
            _translate_json_column(db, "rdm_snapshot", "error_json", "id")
            db.execute("UPDATE rdm_change SET parameter = 'NONE' WHERE parameter = 'KEINE'")
            db.execute("UPDATE rdm_schema SET version = ?", (SCHEMA_VERSION,))
    finally:
        db.execute("PRAGMA foreign_keys = ON")


class RdmStore:
    def __init__(self, dest: str | Path | sqlite3.Connection):
        if isinstance(dest, sqlite3.Connection):
            self.db, self._owned = dest, False
        else:
            self.db, self._owned = sqlite3.connect(str(dest)), True
        self.migrated_from: int | None = None
        version = self._schema_version()
        if version == 1:
            _migrate_v1(self.db)
            self.migrated_from = 1
        elif version is not None and version != SCHEMA_VERSION:
            raise RuntimeError(f"RDM schema version {version}, expected {SCHEMA_VERSION}")
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript(_SCHEMA)
        if self._schema_version() is None:
            self.db.execute("INSERT INTO rdm_schema VALUES (?)", (SCHEMA_VERSION,))
        self.db.commit()

    def _schema_version(self) -> int | None:
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rdm_schema'").fetchone():
            return None
        row = self.db.execute("SELECT version FROM rdm_schema").fetchone()
        return row[0] if row else None

    def close(self) -> None:
        if self._owned:
            self.db.close()

    # ----------------------------------------------------------------- profile
    def save_profile(self, p: DeviceProfile) -> int:
        di = p.device_info
        pa = p.personality_current
        with self.db:
            cur = self.db.execute(
                """INSERT INTO rdm_device (rdm_uid, rpt_uid, endpoint, manufacturer_id, manufacturer, model,
                   device_label, software_label, model_id, software_version_id, product_category, personality,
                   personality_text, footprint, start_address, universe, device_hours, lamp_hours,
                   read_utc, profile_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (str(p.address.rdm_uid), str(p.address.rpt_uid), p.address.endpoint, p.address.rdm_uid.manufacturer,
                 p.manufacturer, p.model, p.device_label, p.software_label,
                 di.model_id if di else None, di.software_version_id if di else None,
                 di.product_category if di else None, di.personality_current if di else None,
                 pa.description if pa else None, di.footprint if di else None, di.start_address if di else None,
                 p.universe, p.device_hours, p.lamp_hours, p.read_utc,
                 json.dumps(p.to_dict(), ensure_ascii=False)))
            gid = cur.lastrowid
            self.db.executemany(
                """INSERT INTO rdm_sensor_definition VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(gid, s.number, s.type, s.type_text, s.unit, s.unit_text, s.prefix, s.factor, s.range_min,
                  s.range_max, s.normal_min, s.normal_max, s.recorded_support, s.description,
                  int(s.measurement_relevant)) for s in p.sensors])
        return gid

    # ----------------------------------------------------------------- sensors
    def save_snapshot(self, device_id: int | None, s: SensorSnapshot, measurement_ref: str | None,
                      phase: str = "single") -> int:
        with self.db:
            cur = self.db.execute(
                """INSERT INTO rdm_snapshot (device_id, measurement_ref, phase, time_utc, duration_s, status,
                   error_json) VALUES (?,?,?,?,?,?,?)""",
                (device_id, measurement_ref, phase, s.time_utc, s.duration_s, s.status,
                 json.dumps(s.error, ensure_ascii=False) if s.error else None))
            sid = cur.lastrowid
            self.db.executemany(
                "INSERT INTO rdm_sensor_value VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [(sid, w.number, w.description, w.type_text, w.unit_text, w.value, w.raw, w.lowest,
                  w.highest, w.recorded, int(w.outside_normal)) for w in s.values])
        return sid

    def sensor_values_for_measurement(self, measurement_ref: str) -> list[dict]:
        sql = """SELECT s.phase, s.time_utc, s.status, w.sensor_num, w.description, w.type_text, w.value, w.unit,
                        w.outside_normal
                 FROM rdm_snapshot s LEFT JOIN rdm_sensor_value w ON w.snapshot_id = s.id
                 WHERE s.measurement_ref = ? ORDER BY s.id, w.sensor_num"""
        columns = ["phase", "time_utc", "status", "sensor_num", "description", "type", "value", "unit",
                   "outside_normal"]
        return [dict(zip(columns, row)) for row in self.db.execute(sql, (measurement_ref,))]

    # ----------------------------------------------------------------- checks / changes
    def save_check(self, device_id: int | None, e: CheckResult, gdtf_mode: str,
                   reference: str | None = None) -> int:
        with self.db:
            return self.db.execute(
                """INSERT INTO rdm_check (device_id, reference, gdtf_mode, time_utc, status, measurement_allowed,
                   details_json) VALUES (?,?,?,?,?,?,?)""",
                (device_id, reference, gdtf_mode, e.time_utc, e.status, int(e.measurement_allowed),
                 json.dumps(e.to_dict(), ensure_ascii=False))).lastrowid

    def save_application(self, device_id: int | None, a: ApplyResult, reference: str | None = None) -> None:
        with self.db:
            for changed in a.changes:
                self.db.execute(
                    """INSERT INTO rdm_change (device_id, reference, time_utc, parameter, old, new, success)
                       VALUES (?,?,?,?,?,?,?)""",
                    (device_id, reference, a.time_utc, changed["parameter"], str(changed["old"]), str(changed["new"]),
                     int(a.success)))
            if not a.changes and a.error:
                self.db.execute(
                    """INSERT INTO rdm_change (device_id, reference, time_utc, parameter, old, new, success)
                       VALUES (?,?,?,?,?,?,0)""", (device_id, reference, a.time_utc, "NONE", None, "; ".join(a.error)))

    # ----------------------------------------------------------------- mapping
    def confirm_mapping(self, m: PersonalityMapping) -> None:
        with self.db:
            self.db.execute(
                """INSERT OR REPLACE INTO rdm_personality_mapping VALUES (?,?,?,?,?,?,?)""",
                (m.manufacturer_id, m.model_id, m.software_version_id, m.gdtf_mode, m.personality_num,
                 m.confirmed_utc, m.confirmed_by))

    def find_mapping(self, manufacturer_id: int, model_id: int, software_version_id: int,
                     gdtf_mode: str) -> PersonalityMapping | None:
        row = self.db.execute(
            """SELECT personality_num, confirmed_utc, confirmed_by FROM rdm_personality_mapping
               WHERE manufacturer_id=? AND model_id=? AND software_version_id=? AND gdtf_mode=?""",
            (manufacturer_id, model_id, software_version_id, gdtf_mode)).fetchone()
        if not row:
            return None
        return PersonalityMapping(manufacturer_id, model_id, software_version_id, row[0], gdtf_mode, row[1], row[2] or "")
