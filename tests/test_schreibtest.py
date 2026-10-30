"""Schreibtest-Ablauf offline gegen den Simulator (ersetzt nicht den Realtest)."""

import asyncio
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from pyrdmnet import konstanten as K
from pyrdmnet.livetest import LiveKonfig
from pyrdmnet.schreibtest import CHECKLISTE, SchreibKonfig, SchreibTest
from pyrdmnet.simulator import SimScheinwerfer, SimulierterGateway
from pyrdmnet import rdm_parameter as R
from pyrdmnet.rdm import Uid

from test_livetest import freier_port


def etc_par_sim() -> SimScheinwerfer:
    """Simulator mit den Modi des realen ETC ColorSource PAR aus Live-Test 2."""
    return SimScheinwerfer(uid=Uid(0x7FF0, 0x0201), personalities=[
        R.Personality(1, 5, "5 Channel"), R.Personality(2, 6, "Direct"),
        R.Personality(3, 1, "1 Channel"), R.Personality(4, 3, "RGB")], personality=2, startadresse=1)


class TestSchreibtest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ausgabe: list[str] = []

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _lauf(self, sk: SchreibKonfig, antworten=None, gateway=None):
        port = freier_port()
        gateway = gateway or SimulierterGateway(scheinwerfer=etc_par_sim())
        antworten = list(antworten if antworten is not None else ["j"] * len(CHECKLISTE))
        k = LiveKonfig(host="127.0.0.1", port=port, ordner=Path(self.tmp.name), warte_auf_geraete_s=3,
                       suche_responder_s=2, rdm_timeout_s=1, dnssd=False)
        st = SchreibTest(k, sk, ausgabe=self.ausgabe.append, eingabe=lambda _: antworten.pop(0) if antworten else "n")

        async def verbinden():
            await asyncio.sleep(0.5)
            await gateway.verbinden("127.0.0.1", port)
        t = asyncio.create_task(verbinden())
        erg = await st.ausfuehren()
        await t
        await gateway.trennen()
        return st, erg, gateway.scheinwerfer

    async def test_wunschablauf_100_modus4_und_zurueck(self):
        st, erg, sw = await self._lauf(SchreibKonfig(ziel_modus=4, ziel_adresse=100))
        self.assertTrue(erg["erfolgreich"], erg["fehler"])
        self.assertEqual(sw.set_protokoll, [(K.PID_DMX_PERSONALITY, b"\x04"), (K.PID_DMX_START_ADDRESS, b"\x00\x64"),
                                            (K.PID_DMX_PERSONALITY, b"\x02"), (K.PID_DMX_START_ADDRESS, b"\x00\x01")])
        self.assertEqual((sw.personality, sw.startadresse), (2, 1))  # Ausgangszustand
        s1, s2 = erg["schreibtest"]["schritte"]
        self.assertEqual(s1["ist"], {"modus": 4, "adresse": 100, "footprint": 3})
        self.assertEqual(s2["ist"], {"modus": 2, "adresse": 1, "footprint": 6})
        db = sqlite3.connect(st.ordner / "schreibtest.sqlite")
        aend = db.execute("SELECT parameter, alt, neu, erfolgreich FROM rdm_aenderung ORDER BY id").fetchall()
        db.close()
        self.assertEqual(aend, [("DMX_PERSONALITY", "2", "4", 1), ("DMX_START_ADDRESS", "1", "100", 1),
                                ("DMX_PERSONALITY", "4", "2", 1), ("DMX_START_ADDRESS", "100", "1", 1)])
        bericht = (st.ordner / "bericht.md").read_text()
        self.assertIn("## Schreibtest", bericht)
        self.assertEqual(len(erg["gateways"]), 1)
        self.assertEqual(bericht.count("## Schreibtest"), 1)
        self.assertTrue(bericht.startswith("# Schreibtest"))
        self.assertNotIn("nur lesend", bericht)
        self.assertIn("SIM Gateway", bericht)
        self.assertTrue(json.loads((st.ordner / "ergebnis.json").read_text())["erfolgreich"])

    async def test_rueckweg_ausdruecklich_modus1(self):
        st, erg, sw = await self._lauf(SchreibKonfig(4, 100, zurueck_modus=1, zurueck_adresse=1))
        self.assertTrue(erg["erfolgreich"], erg["fehler"])
        self.assertEqual((sw.personality, sw.startadresse), (1, 1))
        self.assertTrue(any("Rückweg weicht vom jetzigen Zustand ab" in z for z in self.ausgabe))

    async def test_checkliste_nein_aendert_nichts(self):
        st, erg, sw = await self._lauf(SchreibKonfig(4, 100), antworten=["j", "n"])
        self.assertFalse(erg["erfolgreich"])
        self.assertEqual(sw.set_protokoll, [])
        self.assertIn("Checkliste nicht vollständig bestätigt – nichts geändert", erg["fehler"])

    async def test_unbekannter_modus(self):
        st, erg, sw = await self._lauf(SchreibKonfig(9, 100))
        self.assertFalse(erg["erfolgreich"])
        self.assertEqual(sw.set_protokoll, [])

    async def test_adresse_ausserhalb(self):
        st, erg, sw = await self._lauf(SchreibKonfig(2, 510))  # 510 + 6 - 1 > 512
        self.assertFalse(erg["erfolgreich"])
        self.assertEqual(sw.set_protokoll, [])

    async def test_schreibschutz(self):
        g = SimulierterGateway(scheinwerfer=etc_par_sim())
        g.scheinwerfer.schreibschutz = True
        st, erg, sw = await self._lauf(SchreibKonfig(4, 100), gateway=g)
        self.assertFalse(erg["erfolgreich"])
        self.assertTrue(any("Schritt 1 hat nichts geändert" in f for f in erg["fehler"]))

    async def test_adresse_scheitert_rueckweg_laeuft(self):
        g = SimulierterGateway(scheinwerfer=etc_par_sim())
        sw = g.scheinwerfer
        orig = sw._set

        def nur_modus(a):  # Gerät nimmt Moduswechsel an, verweigert aber Adressen ≠ 1
            if a.pid == K.PID_DMX_START_ADDRESS and a.daten != b"\x00\x01":
                return a.antwort(K.ANTWORT_NACK, b"\x00\x06")
            return orig(a)
        sw._set = nur_modus
        st, erg, sw = await self._lauf(SchreibKonfig(4, 100), gateway=g)
        self.assertFalse(erg["erfolgreich"])
        self.assertEqual((sw.personality, sw.startadresse), (2, 1))  # Rückweg hat gegriffen
        self.assertFalse(erg["schreibtest"]["schritte"][0]["ok"])
        self.assertTrue(erg["schreibtest"]["schritte"][1]["ok"])

    async def test_langsame_checkliste_haelt_verbindung(self):
        """Eingabe dauert länger als der Heartbeat-Takt – Broker läuft weiter (Eingabe im Thread)."""
        antworten = ["j"] * len(CHECKLISTE)

        def langsam(_):
            time.sleep(0.3)
            return antworten.pop(0)
        port = freier_port()
        g = SimulierterGateway(scheinwerfer=etc_par_sim())
        k = LiveKonfig(host="127.0.0.1", port=port, ordner=Path(self.tmp.name), warte_auf_geraete_s=3,
                       suche_responder_s=2, rdm_timeout_s=1, dnssd=False)
        st = SchreibTest(k, SchreibKonfig(4, 100), ausgabe=self.ausgabe.append, eingabe=langsam)

        async def verbinden():
            await asyncio.sleep(0.5)
            await g.verbinden("127.0.0.1", port)
        t = asyncio.create_task(verbinden())
        erg = await st.ausfuehren()
        await t
        await g.trennen()
        self.assertTrue(erg["erfolgreich"], erg["fehler"])


if __name__ == "__main__":
    unittest.main()
