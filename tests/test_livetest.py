"""Live-Test-Ablauf offline gegen den Simulator (ersetzt nicht den Realtest)."""

import asyncio
import json
import socket
import tempfile
import unittest
from pathlib import Path

from pyrdmnet import abgleich as A
from pyrdmnet.livetest import LiveKonfig, LiveTest, adapter_waehlen, ip_ist_lokal, netzwerkadapter
from pyrdmnet.simulator import SimulierterGateway


def freier_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestLiveTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ausgabe: list[str] = []

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _lauf(self, gateway: SimulierterGateway | None, scope_geraet="default", **kw):
        port = freier_port()
        kw.setdefault("dnssd", False)
        k = LiveKonfig(host="127.0.0.1", port=port, ordner=Path(self.tmp.name), warte_auf_geraete_s=3,
                       suche_responder_s=2, rdm_timeout_s=1, **kw)
        lt = LiveTest(k, ausgabe=self.ausgabe.append)

        async def spaeter_verbinden():
            await asyncio.sleep(0.5)
            if gateway:
                try:
                    await gateway.verbinden("127.0.0.1", port)
                except Exception:
                    pass
        t = asyncio.create_task(spaeter_verbinden())
        erg = await lt.ausfuehren()
        await t
        if gateway:
            await gateway.trennen()
        return lt, erg

    async def test_erfolgreicher_lauf(self):
        lt, erg = await self._lauf(SimulierterGateway(),
                                   erwartung=A.EinstellungsErwartung("Direkt 6ch", 6, 100, 1))
        self.assertTrue(erg["erfolgreich"], erg["fehler"])
        self.assertEqual(len(erg["gateways"]), 1)
        self.assertEqual(len(erg["geraete"]), 1)
        self.assertEqual(erg["geraete"][0]["abgleich"]["status"], A.OK_UNBESTAETIGT)
        self.assertEqual(len(erg["geraete"][0]["sensoren"]["werte"]), 4)
        for datei in ("bericht.md", "ergebnis.json", "livetest.sqlite", "broker_ereignisse.jsonl",
                      "paketmitschnitt.jsonl"):
            self.assertTrue((lt.ordner / datei).exists(), datei)
        zeilen = (lt.ordner / "paketmitschnitt.jsonl").read_text().splitlines()
        self.assertGreater(len(zeilen), 10)
        self.assertEqual(json.loads(zeilen[0])["richtung"], "ein")
        self.assertTrue(any("VERBUNDEN" in z for z in self.ausgabe))
        self.assertIn("SIM-PAR RGBL", (lt.ordner / "bericht.md").read_text())
        self.assertIn("nur lesend", (lt.ordner / "bericht.md").read_text())
        ports = erg["gateways"][0]["ports"]
        self.assertEqual(ports[0]["modus"], "Ausgang")
        self.assertEqual(ports[0]["name"], "DMX Port 1")
        self.assertEqual(ports[0]["universe"], 1)
        self.assertEqual(len(ports[0]["responder"]), 1)
        self.assertTrue(any("Modus Ausgang" in z for z in self.ausgabe))

    async def test_niemand_verbindet_sich(self):
        lt, erg = await self._lauf(None)
        self.assertFalse(erg["erfolgreich"])
        self.assertIn("Kein RDMnet-Gerät hat sich verbunden", erg["fehler"])
        self.assertTrue(any("Keine TCP-Verbindung" in h for h in erg["hinweise"]))

    async def test_scope_falsch_wird_angezeigt(self):
        lt, erg = await self._lauf(SimulierterGateway(scope="buehne"))
        self.assertFalse(erg["erfolgreich"])
        self.assertTrue(any("ABGELEHNT" in z and "buehne" in z for z in self.ausgabe))
        self.assertTrue(any("Scope" in h for h in erg["hinweise"]))

    async def test_gateway_ohne_scheinwerfer(self):
        g = SimulierterGateway()
        orig = g._responder_antwort

        def ohne(endpoint, a):  # Port 1 meldet keine Responder
            from pyrdmnet import konstanten as K
            from pyrdmnet import rdm_parameter as R
            if endpoint == 0 and a.pid == K.PID_ENDPOINT_RESPONDERS:
                return a.antwort(K.ANTWORT_ACK, R.baue_endpoint_responder(1, 1, []))
            return orig(endpoint, a)
        g._responder_antwort = ohne
        lt, erg = await self._lauf(g)
        self.assertFalse(erg["erfolgreich"])
        self.assertEqual(len(erg["gateways"]), 1)
        self.assertTrue(any("Keine RDM-Responder" in f for f in erg["fehler"]))
        self.assertEqual(erg["gateways"][0]["ports_nach_suche"][0]["responder"], [])
        self.assertTrue(any("DMX-Ausgang" in h for h in erg["hinweise"]))

    async def test_fremde_ip(self):
        k = LiveKonfig(host="203.0.113.7", ordner=Path(self.tmp.name))
        erg = await LiveTest(k, ausgabe=self.ausgabe.append).ausfuehren()
        self.assertFalse(erg.get("erfolgreich"))
        self.assertIn("gehört zu keinem Netzwerkadapter", erg["fehler"][0])


class TestAdapter(unittest.TestCase):
    def test_lokal(self):
        self.assertTrue(ip_ist_lokal("127.0.0.1"))
        self.assertFalse(ip_ist_lokal("203.0.113.7"))

    def test_ifconfig_macos(self):
        text = ("lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384\n"
                "\tinet 127.0.0.1 netmask 0xff000000\n"
                "en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
                "\tether aa:bb:cc:dd:ee:ff\n"
                "\tinet6 fe80::1%en0 prefixlen 64 scopeid 0x4\n"
                "\tinet 192.168.178.20 netmask 0xffffff00 broadcast 192.168.178.255\n"
                "en7: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
                "\tinet 2.0.0.10 netmask 0xff000000 broadcast 2.255.255.255\n")
        self.assertEqual(netzwerkadapter(text), [("lo0", "127.0.0.1"), ("en0", "192.168.178.20"),
                                                 ("en7", "2.0.0.10")])

    def test_auswahl(self):
        liste = [(n, ip) for n, ip in netzwerkadapter() if not ip.startswith("127.")]
        aus: list[str] = []
        if liste:
            self.assertEqual(adapter_waehlen(lambda _: "1", aus.append), liste[0][1])
        self.assertIsNone(adapter_waehlen(lambda _: "x", aus.append))


if __name__ == "__main__":
    unittest.main()


class TestMasterPortMitschnitt(unittest.TestCase):
    """Antworten aus dem echten Live-Test vom 26.10.2026 (LSS MasterPort 2, Firmware v2.13.0028)."""

    def test_antworten_dekodieren(self):
        from pyrdmnet import rdm_parameter as R
        di = R.lese_device_info(bytes.fromhex("010008510802020d001c00000101ffff000004"))
        self.assertEqual((di.modell_id, di.footprint, di.startadresse, di.sensor_anzahl), (0x0851, 0, 0xFFFF, 4))
        self.assertEqual(di.produktkategorie_text, "Datenverteilung")
        aend, eps = R.lese_endpoint_liste(bytes.fromhex("00000000000101000201000301000401"))
        self.assertEqual([(e.nummer, e.typ) for e in eps], [(1, 1), (2, 1), (3, 1), (4, 1)])
        self.assertEqual(R.lese_endpoint_responder(bytes.fromhex("000100000004")), (1, 4, []))
        sd = R.lese_sensor_definition(bytes.fromhex("0300010100000320000002ee024350552054656d706572617475726500"))
        self.assertEqual((sd.beschreibung, sd.einheit_text, sd.faktor, sd.normal_max), ("CPU Temperature", "°C", 0.1, 750))
        p = R.lese_personality_beschreibung(bytes.fromhex("01000044656661756c7400"))
        self.assertEqual((p.nummer, p.footprint, p.beschreibung), (1, 0, "Default"))
