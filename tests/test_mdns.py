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


class TestCommand(unittest.TestCase):
    def test_dns_sd_command(self):
        b = dnssd.dns_sd_command("default", CID, Uid(0x7FF0, 1), 8888)
        self.assertEqual(b[:6], ["dns-sd", "-R", "pyrdmnet Broker default 12345678", "_rdmnet._tcp,_default",
                                 "local.", "8888"])
        self.assertIn("E133Scope=default", b)
        self.assertIn("CID=1234567890ABCDEF1234567890ABCDEF", b)
        self.assertIn("UID=7FF000000001", b)
        self.assertIn("TxtVers=1", b)

    def test_txt(self):
        t = dnssd.txt_entries("stage", CID, Uid(0x7FF0, 1))
        self.assertEqual((t["E133Scope"], t["E133Vers"]), ("stage", "1"))

    def test_no_method(self):
        with mock.patch("shutil.which", return_value=None), mock.patch.dict("sys.modules", {"zeroconf": None}):
            with self.assertRaises(RuntimeError):
                dnssd.DnssdRegistration("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)


class TestWithFakeDnsSd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path = Path(self.tmp.name) / "dns-sd"
        path.write_text(FAKE)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.tmp.name}{os.pathsep}{os.environ['PATH']}"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_register_self_test_stop(self):
        d = dnssd.DnssdRegistration("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)
        self.assertEqual(d.method, "dns-sd")
        call = (Path(self.tmp.name) / "aufruf.txt").read_text()
        self.assertIn("_rdmnet._tcp,_default local. 8888", call)
        self.assertEqual(len(d.self_test(duration_s=1)), 1)
        d.stop()
        self.assertIsNotNone(d._proc.poll())

    def test_live_test_reports_mdns(self):
        import asyncio
        from pyrdmnet.live_test import LiveConfig, LiveTest
        reader: list[str] = []
        k = LiveConfig(host="127.0.0.1", port=0, folder=Path(self.tmp.name), wait_for_devices_s=1, dnssd=True)
        res = asyncio.run(LiveTest(k, output=reader.append).run())
        self.assertTrue(res["mdns"]["active"])
        self.assertTrue(res["mdns_self_test"])
        self.assertTrue(any("automatisch (mDNS/DNS-SD)" in z for z in reader))


if __name__ == "__main__":
    unittest.main()
