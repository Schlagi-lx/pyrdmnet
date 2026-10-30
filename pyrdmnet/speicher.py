"""SQLite-Speicher für RDM-Geräteprofile, Einstellungsabgleich und Sensordaten.

Die Tabellen tragen das Präfix ``rdm_`` und können in der Messdatenbank des
Messlabors liegen (gleiche Datei) oder in einer eigenen Datei. Die Verbindung
zu einer Messung läuft über ``messung_ref`` (Text, z. B. Messungs-ID der
Messdatenbank) – so bleibt das Modul unabhängig vom Schema des Messlabors.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .abgleich import AbgleichErgebnis, AnwendungsErgebnis, PersonalityZuordnung
from .auslesen import GeraeteProfil, SensorSchnappschuss

SCHEMA_VERSION = 1

_SCHEMA = """
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


class RdmSpeicher:
    def __init__(self, ziel: str | Path | sqlite3.Connection):
        if isinstance(ziel, sqlite3.Connection):
            self.db, self._eigen = ziel, False
        else:
            self.db, self._eigen = sqlite3.connect(str(ziel)), True
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript(_SCHEMA)
        zeile = self.db.execute("SELECT version FROM rdm_schema").fetchone()
        if zeile is None:
            self.db.execute("INSERT INTO rdm_schema VALUES (?)", (SCHEMA_VERSION,))
        elif zeile[0] != SCHEMA_VERSION:
            raise RuntimeError(f"RDM-Schema Version {zeile[0]}, erwartet {SCHEMA_VERSION}")
        self.db.commit()

    def schliessen(self) -> None:
        if self._eigen:
            self.db.close()

    # ----------------------------------------------------------------- Profil
    def speichere_profil(self, p: GeraeteProfil) -> int:
        di = p.device_info
        pa = p.personality_aktuell
        with self.db:
            cur = self.db.execute(
                """INSERT INTO rdm_geraet (rdm_uid, rpt_uid, endpoint, hersteller_id, hersteller, modell,
                   geraete_label, software_label, modell_id, software_version_id, produktkategorie, personality,
                   personality_text, footprint, startadresse, universe, geraete_stunden, lampen_stunden,
                   ausgelesen_utc, profil_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (str(p.adresse.rdm_uid), str(p.adresse.rpt_uid), p.adresse.endpoint, p.adresse.rdm_uid.hersteller,
                 p.hersteller, p.modell, p.geraete_label, p.software_label,
                 di.modell_id if di else None, di.software_version_id if di else None,
                 di.produktkategorie if di else None, di.personality_aktuell if di else None,
                 pa.beschreibung if pa else None, di.footprint if di else None, di.startadresse if di else None,
                 p.universe, p.geraete_stunden, p.lampen_stunden, p.ausgelesen_utc,
                 json.dumps(p.als_dict(), ensure_ascii=False)))
            gid = cur.lastrowid
            self.db.executemany(
                """INSERT INTO rdm_sensor_definition VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(gid, s.nummer, s.typ, s.typ_text, s.einheit, s.einheit_text, s.praefix, s.faktor, s.bereich_min,
                  s.bereich_max, s.normal_min, s.normal_max, s.recorded_unterstuetzung, s.beschreibung,
                  int(s.messrelevant)) for s in p.sensoren])
        return gid

    # ----------------------------------------------------------------- Sensoren
    def speichere_schnappschuss(self, geraet_id: int | None, s: SensorSchnappschuss, messung_ref: str | None,
                                phase: str = "einzeln") -> int:
        with self.db:
            cur = self.db.execute(
                """INSERT INTO rdm_schnappschuss (geraet_id, messung_ref, phase, zeit_utc, dauer_s, status,
                   fehler_json) VALUES (?,?,?,?,?,?,?)""",
                (geraet_id, messung_ref, phase, s.zeit_utc, s.dauer_s, s.status,
                 json.dumps(s.fehler, ensure_ascii=False) if s.fehler else None))
            sid = cur.lastrowid
            self.db.executemany(
                "INSERT INTO rdm_sensorwert VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [(sid, w.nummer, w.beschreibung, w.typ_text, w.einheit_text, w.wert, w.roh, w.niedrigster,
                  w.hoechster, w.aufgezeichnet, int(w.ausserhalb_normal)) for w in s.werte])
        return sid

    def sensorwerte_zu_messung(self, messung_ref: str) -> list[dict]:
        sql = """SELECT s.phase, s.zeit_utc, s.status, w.sensor_nr, w.beschreibung, w.typ_text, w.wert, w.einheit,
                        w.ausserhalb_normal
                 FROM rdm_schnappschuss s LEFT JOIN rdm_sensorwert w ON w.schnappschuss_id = s.id
                 WHERE s.messung_ref = ? ORDER BY s.id, w.sensor_nr"""
        spalten = ["phase", "zeit_utc", "status", "sensor_nr", "beschreibung", "typ", "wert", "einheit",
                   "ausserhalb_normal"]
        return [dict(zip(spalten, z)) for z in self.db.execute(sql, (messung_ref,))]

    # ----------------------------------------------------------------- Abgleich / Änderungen
    def speichere_abgleich(self, geraet_id: int | None, e: AbgleichErgebnis, gdtf_modus: str,
                           bezug: str | None = None) -> int:
        with self.db:
            return self.db.execute(
                """INSERT INTO rdm_abgleich (geraet_id, bezug, gdtf_modus, zeit_utc, status, messung_erlaubt,
                   details_json) VALUES (?,?,?,?,?,?,?)""",
                (geraet_id, bezug, gdtf_modus, e.zeit_utc, e.status, int(e.messung_erlaubt),
                 json.dumps(e.als_dict(), ensure_ascii=False))).lastrowid

    def speichere_anwendung(self, geraet_id: int | None, a: AnwendungsErgebnis, bezug: str | None = None) -> None:
        with self.db:
            for aend in a.aenderungen:
                self.db.execute(
                    """INSERT INTO rdm_aenderung (geraet_id, bezug, zeit_utc, parameter, alt, neu, erfolgreich)
                       VALUES (?,?,?,?,?,?,?)""",
                    (geraet_id, bezug, a.zeit_utc, aend["parameter"], str(aend["alt"]), str(aend["neu"]),
                     int(a.erfolgreich)))
            if not a.aenderungen and a.fehler:
                self.db.execute(
                    """INSERT INTO rdm_aenderung (geraet_id, bezug, zeit_utc, parameter, alt, neu, erfolgreich)
                       VALUES (?,?,?,?,?,?,0)""", (geraet_id, bezug, a.zeit_utc, "KEINE", None, "; ".join(a.fehler)))

    # ----------------------------------------------------------------- Zuordnung
    def zuordnung_bestaetigen(self, z: PersonalityZuordnung) -> None:
        with self.db:
            self.db.execute(
                """INSERT OR REPLACE INTO rdm_personality_zuordnung VALUES (?,?,?,?,?,?,?)""",
                (z.hersteller_id, z.modell_id, z.software_version_id, z.gdtf_modus, z.personality_nr,
                 z.bestaetigt_utc, z.bestaetigt_von))

    def zuordnung_finden(self, hersteller_id: int, modell_id: int, software_version_id: int,
                         gdtf_modus: str) -> PersonalityZuordnung | None:
        z = self.db.execute(
            """SELECT personality_nr, bestaetigt_utc, bestaetigt_von FROM rdm_personality_zuordnung
               WHERE hersteller_id=? AND modell_id=? AND software_version_id=? AND gdtf_modus=?""",
            (hersteller_id, modell_id, software_version_id, gdtf_modus)).fetchone()
        if not z:
            return None
        return PersonalityZuordnung(hersteller_id, modell_id, software_version_id, z[0], gdtf_modus, z[1], z[2] or "")
