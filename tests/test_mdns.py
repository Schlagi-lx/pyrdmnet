"""mDNS registration: command line for Bonjour and flow with a fake dns-sd."""

import os
import stat
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from pyrdmnet import dnssd
from pyrdmnet.rdm import Uid

from helpers import FAST_LIVE

CID = uuid.UUID("12345678-90ab-cdef-1234-567890abcdef")

# Fake dns-sd as a Python script (runs on macOS, Linux and Windows). On Windows it is started through a
# dns-sd.cmd wrapper.
#   -R  registers: reports success and keeps running (stores the arguments in reg.json)
#   -B  lists the own registration and the "other brokers" from others.txt (lines: name|host|port|TXT)
#   -L  resolves a name to host, port and TXT records like the real dns-sd output
FAKE = r'''
import json, pathlib, sys, time
here = pathlib.Path(__file__).resolve().parent
args = sys.argv[1:]
wait = 3 if sys.platform == "win32" else 30  # Windows: the wrapper's child outlives terminate(); keep it short
reg = json.loads((here / "reg.json").read_text()) if (here / "reg.json").exists() else None
others_file = here / "others.txt"
others = [line.split("|") for line in others_file.read_text().splitlines() if line] if others_file.exists() else []
if args[0] == "-R":
    (here / "call.txt").write_text(" ".join(args))
    (here / "reg.json").write_text(json.dumps(args))
    print(f"Registering Service {args[1]}.{args[2]} port {args[4]}", flush=True)
    print(f"Got a reply for service {args[1]}.{args[2]}local.: Name now registered and active", flush=True)
    time.sleep(wait)
elif args[0] == "-B":
    print("Browsing for", args[1], flush=True)
    names = ([reg[1]] if reg else []) + [o[0] for o in others]
    for name in names:
        print("12:00:00.000  Add        3   4 local.               _rdmnet._tcp.        " + name, flush=True)
    if sys.platform != "win32":
        time.sleep(wait)
elif args[0] == "-L":
    if reg and args[1] == reg[1]:
        host, port, txt = "fakehost.local.", reg[4], " ".join(t.replace(" ", "\\ ") for t in reg[5:])
    else:
        match = [o for o in others if o[0] == args[1]]
        if not match:
            sys.exit(0)
        _, host, port, txt = match[0]
    print(f"Lookup {args[1]}._rdmnet._tcp.local", flush=True)
    print(f"12:00:00.000  {args[1]}._rdmnet._tcp.local. can be reached at {host}:{port} (interface 1)", flush=True)
    print(" " + txt, flush=True)
'''


def install_fake(folder: Path) -> None:
    if sys.platform == "win32":
        (folder / "fake_dns_sd.py").write_text(FAKE)
        (folder / "dns-sd.cmd").write_text(f'@"{sys.executable}" "%~dp0fake_dns_sd.py" %*\r\n')
    else:
        path = folder / "dns-sd"
        path.write_text(f"#!{sys.executable}\n" + FAKE)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)


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
        self.assertEqual((t["E133Scope"], t["E133Vers"], t["Manuf"]), ("stage", "1", "pyrdmnet"))

    def test_no_method(self):
        with mock.patch("shutil.which", return_value=None), mock.patch.dict("sys.modules", {"zeroconf": None}):
            with self.assertRaises(RuntimeError):
                dnssd.DnssdRegistration("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)


class TestWithFakeDnsSd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        install_fake(Path(self.tmp.name))
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.tmp.name}{os.pathsep}{os.environ['PATH']}"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_register_self_test_stop(self):
        d = dnssd.DnssdRegistration("default", CID, Uid(0x7FF0, 1), "127.0.0.1", 8888)
        self.assertEqual(d.method, "dns-sd")
        call = (Path(self.tmp.name) / "call.txt").read_text()
        self.assertIn("_rdmnet._tcp,_default local. 8888", call)
        self.assertEqual(len(d.self_test(duration_s=1)), 1)
        d.stop()
        self.assertIsNotNone(d._proc.poll())

    def test_live_test_reports_mdns(self):
        import asyncio
        from pyrdmnet.live_test import LiveConfig, LiveTest
        reader: list[str] = []
        k = LiveConfig(host="127.0.0.1", port=0, folder=Path(self.tmp.name), wait_for_devices_s=0.5, dnssd=True,
                       **FAST_LIVE)
        res = asyncio.run(LiveTest(k, output=reader.append).run())
        self.assertTrue(res["mdns"]["active"])
        self.assertTrue(res["mdns_self_test"])
        self.assertTrue(any("automatic broker discovery (mDNS/DNS-SD)" in z for z in reader))


if __name__ == "__main__":
    unittest.main()
