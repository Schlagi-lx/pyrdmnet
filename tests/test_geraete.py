"""Controller (GET/SET-Regeln), Geräteauslesung, Sensoren, Abgleich und Anwenden."""

import asyncio
import struct
import unittest

from pyrdmnet import abgleich as A
from pyrdmnet import konstanten as K
from pyrdmnet import rdm_parameter as R
from pyrdmnet.auslesen import GeraeteAdresse
from pyrdmnet.controller import (RdmAntwortFehler, RdmNackFehler, RdmZeitFehler, SchreibSperreFehler)
from pyrdmnet.klient import VerbindungsFehler
from pyrdmnet.rdm import Uid

from hilfen import NetzTest

ERW = A.EinstellungsErwartung("Direkt 6ch", footprint=6, startadresse=100, universe=1)


class TestController(NetzTest):
    async def _get(self, pid, daten=b""):
        a = self.adresse
        return await self.c.rdm_get(a.rpt_uid, pid, daten, endpoint=a.endpoint, rdm_ziel=a.rdm_uid, timeout_s=2)

    async def test_get_device_info_ueber_gateway(self):
        di = R.lese_device_info(await self._get(K.PID_DEVICE_INFO))
        self.assertEqual((di.footprint, di.startadresse, di.personality_aktuell), (6, 100, 2))

    async def test_ack_timer(self):
        self.sw.ack_timer_einmal.add(K.PID_DMX_START_ADDRESS)
        self.assertEqual(await self._get(K.PID_DMX_START_ADDRESS), struct.pack(">H", 100))

    async def test_ack_overflow(self):
        self.sw.overflow_pids.add(K.PID_SUPPORTED_PARAMETERS)
        pids = R.lese_pid_liste(await self._get(K.PID_SUPPORTED_PARAMETERS))
        self.assertEqual(pids, self.sw.pids())

    async def test_nack(self):
        with self.assertRaises(RdmNackFehler) as ctx:
            await self._get(K.PID_LAMP_HOURS)
        self.assertEqual(ctx.exception.grund, K.NR_UNKNOWN_PID)

    async def test_zeitueberschreitung(self):
        self.gateway.stumm = True
        with self.assertRaises(RdmZeitFehler):
            await self.c.rdm_get(self.adresse.rpt_uid, K.PID_DEVICE_INFO, endpoint=1, rdm_ziel=self.sw.uid,
                                 timeout_s=0.3)
        self.assertEqual(self.c._offen, {})

    async def test_set_regeln(self):
        a = self.adresse
        neu = struct.pack(">H", 200)
        with self.assertRaises(SchreibSperreFehler):  # keine Freigabe
            await self.c.rdm_set(a.rpt_uid, K.PID_DMX_START_ADDRESS, neu, endpoint=1, rdm_ziel=a.rdm_uid)
        self.c.schreibfreigabe = True
        with self.assertRaises(SchreibSperreFehler):  # PID nicht freigegeben
            await self.c.rdm_set(a.rpt_uid, K.PID_DEVICE_LABEL, b"x", endpoint=1, rdm_ziel=a.rdm_uid)
        self.assertEqual(self.sw.set_protokoll, [])
        await self.c.rdm_set(a.rpt_uid, K.PID_DMX_START_ADDRESS, neu, endpoint=1, rdm_ziel=a.rdm_uid)
        self.assertEqual(self.sw.startadresse, 200)
        with self.assertRaises(RdmNackFehler) as ctx:
            await self.c.rdm_set(a.rpt_uid, K.PID_DMX_START_ADDRESS, struct.pack(">H", 510), endpoint=1,
                                 rdm_ziel=a.rdm_uid)  # 510 + 6 - 1 > 512
        self.assertEqual(ctx.exception.grund, K.NR_DATA_OUT_OF_RANGE)

    async def test_discovery_nie(self):
        from pyrdmnet.rdm import RdmNachricht
        n = RdmNachricht(self.sw.uid, self.c.uid, 0, 1, K.CC_DISCOVERY, 0x0001)
        self.c.schreibfreigabe = True
        with self.assertRaises(SchreibSperreFehler):
            await self.c._sende_und_warte(self.gateway.uid, 1, n, 1)

    async def test_falsche_antwort_wird_erkannt(self):
        from pyrdmnet.rdm import RdmNachricht
        anfrage = RdmNachricht(self.sw.uid, self.c.uid, 3, 1, K.CC_GET, K.PID_DEVICE_INFO)
        fremd = RdmNachricht(self.c.uid, self.sw.uid, 4, 0, K.CC_GET_RESPONSE, K.PID_DEVICE_INFO)
        with self.assertRaises(RdmAntwortFehler):
            self.c._antwort_auswaehlen(anfrage, [anfrage.kodieren(), fremd.kodieren()])

    async def test_offene_anfragen_bei_trennung(self):
        self.gateway.stumm = True
        aufgabe = asyncio.create_task(self._get(K.PID_DEVICE_INFO))
        await asyncio.sleep(0.1)
        await self.broker.stoppen()
        with self.assertRaises(VerbindungsFehler):
            await aufgabe


class TestAuslesenGateway(NetzTest):
    async def test_finden(self):
        adressen = await self.aus.geraete_finden()
        self.assertEqual(adressen, [GeraeteAdresse(self.gateway.uid, 1, self.sw.uid)])

    async def test_profil(self):
        p = await self.aus.profil(self.adresse)
        self.assertEqual(p.hersteller, "pyrdmnet Simulator")
        self.assertEqual([x.nummer for x in p.personalities], [1, 2, 3])
        self.assertEqual(p.personality_aktuell.beschreibung, "Direkt 6ch")
        self.assertEqual(len(p.sensoren), 4)
        self.assertEqual(p.universe, 1)
        self.assertEqual(p.geraete_stunden, 1234)
        self.assertIsNone(p.lampen_stunden)  # nicht in SUPPORTED_PARAMETERS -> gar nicht abgefragt
        self.assertNotIn("LAMP_HOURS", p.nicht_verfuegbar)
        d = p.als_dict()
        self.assertEqual(d["device_info"]["produktkategorie_text"], "Scheinwerfer")

    async def test_profil_mit_lampenstunden(self):
        self.sw.lampen_stunden = 77
        p = await self.aus.profil(self.adresse)
        self.assertEqual(p.lampen_stunden, 77)

    async def test_sensoren_nur_messrelevant(self):
        p = await self.aus.profil(self.adresse)
        s = await self.aus.sensor_schnappschuss(self.adresse, p.sensoren)
        self.assertEqual(s.status, "ok")
        self.assertEqual([w.beschreibung for w in s.werte], ["LED Temperatur", "Gehaeuse", "Netzspannung"])
        netz = s.werte[2]
        self.assertAlmostEqual(netz.wert, 230.4)
        self.assertEqual(netz.roh, 2304)
        self.assertIsNone(netz.niedrigster)  # Gerät meldet keine Min/Max-Unterstützung
        self.assertFalse(netz.ausserhalb_normal)
        alle = await self.aus.sensor_schnappschuss(self.adresse, p.sensoren, alle_sensoren=True)
        self.assertEqual(len(alle.werte), 4)
        self.assertEqual(alle.werte[3].wert, 512000.0)  # Präfix kilo

    async def test_sensor_temperatur_steigt(self):
        p = await self.aus.profil(self.adresse)
        self.sw.start -= 10  # 10 s Betrieb simulieren: 0,5 K/s
        s = await self.aus.sensor_schnappschuss(self.adresse, p.sensoren)
        self.assertGreaterEqual(s.werte[0].wert, 30.0)

    async def test_sensor_ausserhalb_normal(self):
        self.sw.sensoren[1].definition.normal_max = 40  # vor dem Auslesen: Profil kennt den Normalbereich
        p = await self.aus.profil(self.adresse)
        self.sw.start -= 1000  # LED-Endwert 68 °C (Normal bis 85), Gehäuse-Endwert 41 °C (Normal bis 40)
        s = await self.aus.sensor_schnappschuss(self.adresse, p.sensoren)
        self.assertTrue(s.werte[1].ausserhalb_normal)
        self.assertFalse(s.werte[0].ausserhalb_normal)

    async def test_schnappschuss_bei_ausfall(self):
        p = await self.aus.profil(self.adresse)
        self.gateway.stumm = True
        self.aus.timeout_s = 0.2
        s = await self.aus.sensor_schnappschuss(self.adresse, p.sensoren)
        self.assertEqual(s.status, "fehlgeschlagen")
        self.assertEqual(len(s.fehler), 3)

    async def test_profil_bei_stummem_geraet(self):
        self.gateway.stumm = True
        self.aus.timeout_s = 0.2
        p = await self.aus.profil(self.adresse)
        self.assertIsNone(p.device_info)
        self.assertIn("DEVICE_INFO", p.nicht_verfuegbar)
        self.assertEqual(A.pruefen(p, ERW).status, A.NICHT_PRUEFBAR)


class TestAuslesenNativ(NetzTest):
    nativ = True

    async def test_finden_und_profil(self):
        adressen = await self.aus.geraete_finden()
        self.assertEqual(adressen, [GeraeteAdresse(self.gateway.uid, 0, self.gateway.uid)])
        p = await self.aus.profil(adressen[0])
        self.assertEqual(p.modell, "SIM-PAR RGBL")
        self.assertIsNone(p.universe)


class TestAbgleich(NetzTest):
    async def _profil(self):
        return await self.aus.profil(self.adresse)

    def _zuordnung(self, p, nr=2, modus="Direkt 6ch", fw=None):
        di = p.device_info
        return A.PersonalityZuordnung(p.adresse.rdm_uid.hersteller, di.modell_id,
                                      di.software_version_id if fw is None else fw, nr, modus, "t")

    async def test_ok_unbestaetigt_und_ok(self):
        p = await self._profil()
        e = A.pruefen(p, ERW)
        self.assertEqual(e.status, A.OK_UNBESTAETIGT)
        self.assertTrue(e.messung_erlaubt)
        self.assertFalse(A.pruefen(p, ERW, strikt=True).messung_erlaubt)
        self.assertIn("2 „Direkt 6ch“", e.hinweise[0])
        e = A.pruefen(p, ERW, self._zuordnung(p))
        self.assertEqual((e.status, e.messung_erlaubt, e.soll_personality), (A.OK, True, 2))

    async def test_abweichungen(self):
        self.sw.startadresse = 1
        self.gateway.universe = 3
        p = await self._profil()
        e = A.pruefen(p, ERW, self._zuordnung(p, nr=3))  # bestätigt: 3, Gerät hat 2
        self.assertEqual(e.status, A.ABWEICHUNG)
        self.assertFalse(e.messung_erlaubt)
        self.assertEqual(len(e.abweichungen), 3)  # Adresse, Universe, Personality

    async def test_zuordnung_andere_firmware_gilt_nicht(self):
        p = await self._profil()
        e = A.pruefen(p, ERW, self._zuordnung(p, fw=0x00020000))
        self.assertEqual(e.status, A.OK_UNBESTAETIGT)
        self.assertTrue(any("andere Firmware" in h for h in e.hinweise))

    async def test_kandidaten(self):
        p = await self._profil()
        k = A.kandidaten(p, A.EinstellungsErwartung("HSIC", 5, 1))
        self.assertEqual([x[0].nummer for x in k], [3])
        self.assertEqual(A.kandidaten(p, A.EinstellungsErwartung("x", 99, 1)), [])

    async def test_anwenden_erfolgreich(self):
        self.sw.personality, self.sw.startadresse = 1, 7
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, ERW, self._zuordnung(p))
        self.assertTrue(erg.erfolgreich, erg.fehler)
        self.assertEqual([x["parameter"] for x in erg.aenderungen], ["DMX_PERSONALITY", "DMX_START_ADDRESS"])
        self.assertEqual((self.sw.personality, self.sw.startadresse), (2, 100))
        self.assertEqual(A.pruefen(await self._profil(), ERW, self._zuordnung(p)).status, A.OK)

    async def test_anwenden_nur_adresse_ohne_zuordnung(self):
        self.sw.startadresse = 7  # Footprint stimmt -> Adresse darf ohne Zuordnung gesetzt werden
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, ERW, None)
        self.assertTrue(erg.erfolgreich, erg.fehler)
        self.assertEqual(self.sw.startadresse, 100)
        self.assertEqual(self.sw.personality, 2)

    async def test_anwenden_verweigert_ohne_zuordnung_bei_moduswechsel(self):
        self.sw.personality = 1
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, ERW, None)
        self.assertFalse(erg.erfolgreich)
        self.assertEqual(self.sw.set_protokoll, [])

    async def test_anwenden_ohne_freigabe(self):
        self.sw.startadresse = 7
        p = await self._profil()
        erg = await A.anwenden(self.c, self.adresse, p, ERW, self._zuordnung(p))
        self.assertFalse(erg.erfolgreich)
        self.assertIn("Schreibfreigabe", erg.fehler[0])
        self.assertEqual(self.sw.startadresse, 7)

    async def test_anwenden_schreibschutz(self):
        self.sw.startadresse, self.sw.schreibschutz = 7, True
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, ERW, self._zuordnung(p))
        self.assertFalse(erg.erfolgreich)
        self.assertIn("WRITE_PROTECT", erg.fehler[0])

    async def test_anwenden_adresse_ausserhalb(self):
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, A.EinstellungsErwartung("Direkt 6ch", 6, 510),
                               self._zuordnung(p))
        self.assertFalse(erg.erfolgreich)
        self.assertEqual(self.sw.set_protokoll, [])

    async def test_anwenden_zuordnung_mit_falschem_footprint(self):
        self.sw.personality = 1
        p = await self._profil()
        self.c.schreibfreigabe = True
        erg = await A.anwenden(self.c, self.adresse, p, ERW, self._zuordnung(p, nr=3))  # Nr. 3 hat 5 Kanäle
        self.assertFalse(erg.erfolgreich)
        self.assertEqual(self.sw.set_protokoll, [])

    async def test_ruecklesen_erkennt_abweichung(self):
        self.sw.startadresse = 7
        p = await self._profil()
        self.c.schreibfreigabe = True
        alt = self.sw._set

        def ignoriert(a):  # Gerät bestätigt, übernimmt aber nichts
            return a.antwort(K.ANTWORT_ACK)
        self.sw._set = ignoriert
        erg = await A.anwenden(self.c, self.adresse, p, ERW, self._zuordnung(p))
        self.sw._set = alt
        self.assertFalse(erg.erfolgreich)
        self.assertIn("Rücklesen: Startadresse 7", erg.fehler[0])


if __name__ == "__main__":
    unittest.main()
