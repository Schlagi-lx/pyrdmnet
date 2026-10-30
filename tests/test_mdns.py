"""mDNS-Anmeldung: Befehlszeile für Bonjour und Ablauf mit einem nachgebildeten dns-sd."""

import os
import stat
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from pyrdmnet import dnssd
from pyrdmnet.rdm import Uid

CID = uuid.UUID("12345678-90ab-cdef-1234-567890abcdef")

# Nachbildung von macOS dns-sd: -R meldet Erfolg und läuft weiter, -B listet den Dienst
FAKE = r'''#!/bin/sh
if [ "$1" = "-R" ]; then
  echo "$@" > "$(dirname "$0")/aufruf.txt"
  echo "$2" > "$(dirname "$0")/name.txt"
  echo "Registering Service $2.$3 port $5"
  echo "Got a reply for service $2.$3local.: Name now registered and active"
  exec sleep 30
fi
if [ "$1" = "-B" ]; then
  echo "Browsing for $2"
  echo "12:00:00.000  Add        3   4 local.               _rdmnet._tcp.        $(cat "$(dirname "$0")/name.txt")"
  exec sleep 30
fi
'''


class TestBefehl(unittest.TestCase):
    def test_dns_sd_befehl(self):
        b = dnssd.dns_sd_befehl("default", CID, Uid(0x7FF0, 1), 8888)
        self.assertEqual(b[:6], ["dns-sd", "-R", "pyrdmnet Broker default 12345678", "_rdmnet._tcp,_default",
                                 "local.", "8888"])
        self.assertIn("E133Scope=default", b)
        self.assertIn("CID=1234567890ABCDEF1234567890ABCDEF", b)
        self.assertIn("UID=7FF000000001", b)
        self.assertIn("TxtVers=1", b)

    def test_txt(self):
        t = dnssd.txt_eintraege("buehne", CID, Uid(0x7FF0, 1))
        self.assertEqual((t["E133Scope"], t["E133Vers"]), ("buehne", "1"))

    def test_kein_weg(self):
        with mock.patch("shutil.which", return_value=None), mock.patch.dict("sys.modules", {"zeroconf": None}):
            with self.assertRaises(RuntimeError):
                dnssd.DnssdAnmeldung("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)


class TestMitNachgebildetemDnsSd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        pfad = Path(self.tmp.name) / "dns-sd"
        pfad.write_text(FAKE)
        pfad.chmod(pfad.stat().st_mode | stat.S_IEXEC)
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.tmp.name}{os.pathsep}{os.environ['PATH']}"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_anmelden_selbsttest_beenden(self):
        d = dnssd.DnssdAnmeldung("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)
        self.assertEqual(d.weg, "dns-sd")
        aufruf = (Path(self.tmp.name) / "aufruf.txt").read_text()
        self.assertIn("_rdmnet._tcp,_default local. 8888", aufruf)
        self.assertEqual(len(d.selbsttest(dauer_s=1)), 1)
        d.beenden()
        self.assertIsNotNone(d._proc.poll())

    def test_livetest_meldet_mdns(self):
        import asyncio
        from pyrdmnet.livetest import LiveKonfig, LiveTest
        aus: list[str] = []
        k = LiveKonfig(host="127.0.0.1", port=0, ordner=Path(self.tmp.name), warte_auf_geraete_s=1, dnssd=True)
        erg = asyncio.run(LiveTest(k, ausgabe=aus.append).ausfuehren())
        self.assertTrue(erg["mdns"]["aktiv"])
        self.assertTrue(erg["mdns_selbsttest"])
        self.assertTrue(any("automatisch (mDNS/DNS-SD)" in z for z in aus))


if __name__ == "__main__":
    unittest.main()
