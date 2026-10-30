"""Command line: python3 -m pyrdmnet <command>

Commands:
  broker     start our own broker (runs until Ctrl+C)
  simulator  connect a simulated RDMnet device to a broker
  read       find and read all devices on the broker (read only)
  livetest   live test with a real gateway (e.g. LSS MasterPort), read only
  writetest  set mode/start address on a real fixture, verify and restore
  llrp       find RDMnet components without a broker (LLRP); set scope/broker address
  demo       complete offline run: broker + simulator + readout +
             settings check + apply + 3 accompanied measurements (all SIMULATION)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

from . import __version__
from . import constants as C
from .settings_check import ExpectedSetting
from .readout import DeviceReader
from .broker import BrokerConfig, RdmnetBroker
from .controller import RdmnetController
from .facade import RdmConfig, RdmModule
from .simulator import SimulatedGateway


def _host_port(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host or "127.0.0.1", int(port)


async def _broker(args) -> None:
    b = RdmnetBroker(BrokerConfig(host=args.host, port=args.port, scope=args.scope, llrp=args.llrp,
                                  event_file=Path(args.log) if args.log else None))
    await b.start()
    reg = None
    if args.dnssd:
        from .dnssd import DnssdRegistration
        reg = DnssdRegistration(args.scope, b.config.cid, b.config.uid, args.host, b.port)
    print(f"Broker running on {args.host}:{b.port}, scope \"{args.scope}\", UID {b.config.uid}. Ctrl+C to quit.")
    try:
        await asyncio.Event().wait()
    finally:
        if reg:
            reg.stop()
        await b.stop()


async def _simulator(args) -> None:
    host, port = _host_port(args.broker)
    g = SimulatedGateway(scope=args.scope, native=args.native)
    uid = await g.connect(host, port)
    print(f"Simulator connected as {uid} ({'native' if args.native else 'gateway with fixture on port 1'}). "
          "Ctrl+C to quit.")
    await g.disconnected.wait()
    print("Disconnected:", g.disconnect_reason)


async def _read_devices(args) -> int:
    host, port = _host_port(args.broker)
    c = RdmnetController(scope=args.scope)
    await c.connect(host, port)
    await c.client_list()
    reader = DeviceReader(c, args.timeout)
    results = []
    for dmx_addr in await reader.find_devices():
        profile = await reader.profile(dmx_addr)
        s = await reader.sensor_snapshot(dmx_addr, profile.sensors, all_sensors=True)
        results.append({"profile": profile.to_dict(), "sensors": s.to_dict()})
        _print_profile(profile.to_dict(), s.to_dict())
    await c.disconnect()
    if args.json:
        Path(args.json).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"JSON written: {args.json}")
    if not results:
        print("No RDMnet devices on the broker.")
    return 0


def _print_profile(p: dict, s: dict | None = None) -> None:
    di = p.get("device_info") or {}
    pa = p.get("personality_current") or {}
    print(f"\nDevice {p['rdm_uid']} (via {p['rpt_uid']}, endpoint {p['endpoint']})")
    print(f"  {p['manufacturer']} - {p['model']} - firmware {p['software_label']}")
    print(f"  Mode {di.get('personality_current')}/{di.get('personality_count')} \"{pa.get('description', '?')}\", "
          f"footprint {di.get('footprint')}, start address {di.get('start_address')}, universe {p.get('universe')}")
    print(f"  Device hours {p.get('device_hours')}, lamp hours {p.get('lamp_hours')}")
    if p.get("unavailable"):
        print("  Not available:", ", ".join(f"{k} ({v})" for k, v in p["unavailable"].items()))
    if s:
        for w in s["values"]:
            mark = "  !" if w["outside_normal"] else "   "
            rel = "" if w["measurement_relevant"] else " (not measurement-relevant)"
            print(f" {mark} Sensor {w['number']} {w['description']}: {w['value']:.3g} {w['unit_text']}{rel}")


def _livetest(args) -> int:
    from .live_test import LiveConfig, LiveTest, choose_adapter
    host = args.host or choose_adapter()
    if not host:
        return 2
    expected = None
    if args.mode or args.footprint or args.start_address:
        if not (args.mode and args.footprint and args.start_address):
            print("For the settings check, specify --mode, --footprint and --start-address together.")
            return 2
        expected = ExpectedSetting(args.mode, args.footprint, args.start_address, args.universe)
    k = LiveConfig(host=host, port=args.port, scope=args.scope, folder=Path(args.folder),
                   wait_for_devices_s=args.wait, find_responders_s=args.search_s, rdm_timeout_s=args.timeout,
                   dnssd=not args.no_mdns, expected=expected)
    print(f"pyrdmnet {__version__} - LIVE TEST, read only (no RDM SET)")
    res = asyncio.run(LiveTest(k).run())
    return 0 if res.get("success") else 1


def _write_test(args) -> int:
    from .live_test import LiveConfig, choose_adapter
    from .rdm import Uid
    from .write_test import WriteConfig, WriteTest
    host = args.host or choose_adapter()
    if not host:
        return 2
    k = LiveConfig(host=host, port=args.port, scope=args.scope, folder=Path(args.folder),
                   wait_for_devices_s=args.wait, find_responders_s=args.search_s, rdm_timeout_s=args.timeout,
                   dnssd=not args.no_mdns)
    wc = WriteConfig(args.target_mode, args.target_address, args.return_mode, args.return_address,
                       Uid.from_text(args.uid) if args.uid else None)
    print(f"pyrdmnet {__version__} - WRITE TEST (RDM SET on the real device, with checklist)")
    res = asyncio.run(WriteTest(k, wc).run())
    return 0 if res.get("success") else 1


async def _llrp(args) -> int:
    from .llrp import LlrpManager, LlrpNetwork
    from .rdm import Uid
    network = LlrpNetwork(adapter=args.adapter, ttl=args.ttl)
    if args.ttl > 1:
        print("Note: TTL > 1 - responses may come from other network segments.")
    set_param = args.scope or args.domain
    async with LlrpManager(network) as m:
        filt = 0 if args.all else C.LLRP_FILTER_CLIENT_CONN_INACTIVE
        print(f"LLRP discovery via {args.adapter} ({'all' if args.all else 'only without broker connection'}) ...")
        targets = await m.discover(filter=filt)
        if args.uid:
            wanted = Uid.from_text(args.uid)
            targets = [z for z in targets if z.uid == wanted]
        results = []
        for z in targets:
            e = {"uid": str(z.uid), "cid": str(z.cid), "type": z.type_text, "hardware": z.hardware_text,
                 "address": z.address}
            try:
                cs = await m.read_scope(z)
                e.update(scope=cs.scope, static_broker=cs.static_address)
                e["search_domain"] = await m.read_search_domain(z)
            except Exception as error:  # a single target must not abort the list
                e["error"] = str(error)
            results.append(e)
            print(f"  {e['uid']}  {e['type']:<10} {e['address']:<15} HW {e['hardware']}  "
                  f"Scope \"{e.get('scope', '?')}\"  Broker {e.get('static_broker') or 'via mDNS'}"
                  f"  Domain {e.get('search_domain', '?')}" + (f"  ERROR: {e['error']}" if "error" in e else ""))
        if not targets:
            print("No LLRP targets found." + ("" if args.all else " (Use --all to include connected ones.)"))
        if args.json:
            Path(args.json).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        if not set_param:
            return 0
        if not args.uid or len(targets) != 1:
            print("To set values, specify exactly one target with --uid.")
            return 2
        z = targets[0]
        broker = _host_port(args.broker) if args.broker else None
        print(f"\nChange on {z.uid} ({z.type_text}, {z.address}):")
        if args.scope:
            print(f"  Scope -> \"{args.scope}\", broker -> {f'{broker[0]}:{broker[1]}' if broker else 'via mDNS'}")
        if args.domain:
            print(f"  Search domain -> {args.domain}")
        print("  A connected device will disconnect from the broker.")
        if not args.yes:
            response = (await asyncio.to_thread(input, "Proceed? [y/n] ")).strip().lower()
            if response not in ("j", "ja", "y", "yes"):
                print("Aborted, nothing changed.")
                return 1
        m.write_enabled = True
        if args.scope:
            old, new = await m.set_scope(z, args.scope, broker)
            print(f"  Scope: \"{old.scope}\" -> \"{new.scope}\" (read back), broker: "
                  f"{old.static_address or 'mDNS'} -> {new.static_address or 'mDNS'}")
        if args.domain:
            old_d, new_d = await m.set_search_domain(z, args.domain)
            print(f"  Search domain: {old_d} -> {new_d} (read back)")
    return 0


def _demo(args) -> int:
    db = Path(args.db)
    if db.exists() and not args.keep:
        db.unlink()
    print(f"pyrdmnet {__version__} - offline demo (all device data SIMULATION)\n")
    module = RdmModule(RdmConfig(broker_port=0, db=db, heartbeat_interval_s=15))
    module.start()
    gateway = SimulatedGateway()
    gateway.fixture.personality = 1  # deliberately wrong mode
    gateway.fixture.start_address = 1  # deliberately wrong address
    module._run(gateway.connect("127.0.0.1", module.port))
    try:
        addresses = module.find_devices()
        print("Found:", ", ".join(map(str, addresses)))
        profile = module.read_device(addresses[0])
        _print_profile(profile.to_dict())

        expected = ExpectedSetting("Direct 6ch", footprint=6, start_address=100, universe=1)
        a = module.check_setting(profile, expected, reference="Demo")
        print(f"\n1) Settings check against measurement plan: {a.status}, "
              f"measurement allowed: {'yes' if a.measurement_allowed else 'no'}")
        for x in a.deviations + a.hints:
            print("   -", x)

        print("\n2) Confirm mapping: personality 2 = GDTF mode \"Direct 6ch\" (demo confirmation)")
        module.confirm_mapping(profile, "Direct 6ch", 2, confirmed_by="Demo")

        print("3) Enable write permission for this session and apply settings")
        module.set_write_enabled(True)
        app, profile, a = module.apply_setting(profile, expected, reference="Demo")
        for x in app.changes:
            print(f"   - {x['parameter']}: {x['old']} -> {x['new']}")
        for x in app.error:
            print("   - ERROR:", x)
        print(f"   New check: {a.status}, measurement allowed: {'yes' if a.measurement_allowed else 'no'}")
        module.set_write_enabled(False)

        print("\n4) Three accompanied measurements (sensor snapshot before/after)")
        for i in range(1, 4):
            with module.accompany_measurement(profile, measurement_ref=f"DEMO-{i:03d}") as e:
                pass
            v, n = e["before"], e["after"]
            temp = [w for w in n.values if w.type == 0x00]
            print(f"   DEMO-{i:03d}: before {v.status} ({len(v.values)} values), after {n.status}; "
                  + ", ".join(f"{w.description} {w.value:.1f} °C" for w in temp))
        print("\nSensor data for DEMO-002 from the database:")
        for z in module.store.sensor_values_for_measurement("DEMO-002"):
            print(f"   {z['phase']:5} {z['description']:15} {z['value']:8.2f} {z['unit']}")
        print(f"\nDatabase: {db.resolve()}")
        status = module.broker_status()
        print(f"Broker: {len(status['clients'])} clients, counters {status['counters']}")
    finally:
        module._run(gateway.disconnect())
        module.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m pyrdmnet", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version=__version__)
    ap.add_argument("-v", "--verbose", action="store_true", help="log output")
    sub = ap.add_subparsers(dest="command", required=True)

    b = sub.add_parser("broker", help="start the broker")
    b.add_argument("--host", default="127.0.0.1", help="IP of the network adapter (0.0.0.0 = all)")
    b.add_argument("--port", type=int, default=8888)
    b.add_argument("--scope", default=C.E133_DEFAULT_SCOPE)
    b.add_argument("--log", help="write events as JSONL to this file")
    b.add_argument("--dnssd", action="store_true", help="register via mDNS (macOS: Bonjour dns-sd)")
    b.add_argument("--llrp", action="store_true", help="respond as LLRP target (multicast, UDP 5569)")

    s = sub.add_parser("simulator", help="connect a simulated device")
    s.add_argument("--broker", default="127.0.0.1:8888")
    s.add_argument("--scope", default=C.E133_DEFAULT_SCOPE)
    s.add_argument("--native", action="store_true", help="without gateway (the fixture is the RDMnet device)")

    a = sub.add_parser("read", help="find and read devices")
    a.add_argument("--broker", default="127.0.0.1:8888")
    a.add_argument("--scope", default=C.E133_DEFAULT_SCOPE)
    a.add_argument("--timeout", type=float, default=3.0)
    a.add_argument("--json", help="save the result as JSON")

    lt = sub.add_parser("livetest", help="live test with a real gateway (read only)")
    lt.add_argument("--host", help="IP of the adapter facing the gateway (omit: choose from list)")
    lt.add_argument("--port", type=int, default=8888)
    lt.add_argument("--scope", default=C.E133_DEFAULT_SCOPE)
    lt.add_argument("--no-mdns", action="store_true",
                    help="do NOT register the broker via mDNS (then enter IP/port in the gateway)")
    lt.add_argument("--wait", type=float, default=180, help="seconds to wait for the gateway to connect")
    lt.add_argument("--search", dest="search_s", type=float, default=60,
                    help="seconds to wait for fixtures on the ports")
    lt.add_argument("--timeout", type=float, default=5.0, help="RDM timeout per request in s")
    lt.add_argument("--folder", default=".", help="the result folder is created here")
    lt.add_argument("--mode", help="expected GDTF mode (for the settings check, optional)")
    lt.add_argument("--footprint", type=int)
    lt.add_argument("--start-address", type=int)
    lt.add_argument("--universe", type=int)

    st = sub.add_parser("writetest", help="set mode/address, read back, restore (with checklist)")
    st.add_argument("--host", help="IP of the adapter facing the gateway (omit: choose from list)")
    st.add_argument("--port", type=int, default=8888)
    st.add_argument("--scope", default=C.E133_DEFAULT_SCOPE)
    st.add_argument("--no-mdns", action="store_true")
    st.add_argument("--wait", type=float, default=180)
    st.add_argument("--search", dest="search_s", type=float, default=60)
    st.add_argument("--timeout", type=float, default=5.0)
    st.add_argument("--folder", default=".")
    st.add_argument("--uid", help="RDM UID of the fixture (only needed if there are several)")
    st.add_argument("--target-mode", type=int, required=True)
    st.add_argument("--target-address", type=int, required=True)
    st.add_argument("--return-mode", type=int, help="default: the mode read at the start")
    st.add_argument("--return-address", type=int, help="default: the address read at the start")

    ll = sub.add_parser("llrp", help="find components via LLRP; optionally set scope/broker/domain")
    ll.add_argument("--adapter", default="0.0.0.0", help="IP of the network adapter")
    ll.add_argument("--all", action="store_true", help="also find clients already connected to a broker")
    ll.add_argument("--uid", help="only this target (required for setting), e.g. 7ff0:00000100")
    ll.add_argument("--scope", help="set the scope")
    ll.add_argument("--broker", help="with --scope: static broker address IP:port (otherwise mDNS discovery)")
    ll.add_argument("--domain", help="set the search domain")
    ll.add_argument("--yes", action="store_true", help="set without confirmation prompt")
    ll.add_argument("--ttl", type=int, default=1)
    ll.add_argument("--json", help="save the result as JSON")

    d = sub.add_parser("demo", help="offline demo")
    d.add_argument("--db", default="pyrdmnet_demo.sqlite")
    d.add_argument("--keep", action="store_true", help="keep using an existing demo database")

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(message)s")
    try:
        if args.command == "broker":
            asyncio.run(_broker(args))
        elif args.command == "simulator":
            asyncio.run(_simulator(args))
        elif args.command == "read":
            return asyncio.run(_read_devices(args))
        elif args.command == "writetest":
            return _write_test(args)
        elif args.command == "livetest":
            return _livetest(args)
        elif args.command == "llrp":
            return asyncio.run(_llrp(args))
        elif args.command == "demo":
            return _demo(args)
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
