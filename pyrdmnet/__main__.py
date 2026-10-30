"""Kommandozeile: python3 -m pyrdmnet <befehl>

Befehle:
  broker     eigenen Broker starten (läuft bis Strg+C)
  simulator  simuliertes RDMnet-Gerät mit einem Broker verbinden
  auslesen   alle Geräte am Broker finden und auslesen (nur lesen)
  livetest   Live-Test mit realem Gateway (z. B. LSS MasterPort), nur lesend
  schreibtest  Modus/Startadresse am realen Scheinwerfer setzen, prüfen und zurückstellen
  demo       kompletter Offline-Durchlauf: Broker + Simulator + Auslesen +
             Abgleich + Anwenden + 3 begleitete Messungen (alles SIMULATION)
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
from . import konstanten as K
from .abgleich import EinstellungsErwartung
from .auslesen import Auslesung
from .broker import BrokerKonfig, RdmnetBroker
from .controller import RdmnetController
from .fassade import RdmKonfig, RdmModul
from .simulator import SimulierterGateway


def _host_port(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host or "127.0.0.1", int(port)


async def _broker(args) -> None:
    b = RdmnetBroker(BrokerKonfig(host=args.host, port=args.port, scope=args.scope,
                                  ereignis_datei=Path(args.log) if args.log else None))
    await b.starten()
    dn = None
    if args.dnssd:
        from .dnssd import DnssdAnmeldung
        dn = DnssdAnmeldung(args.scope, b.konfig.cid, b.konfig.uid, args.host, b.port)
    print(f"Broker läuft auf {args.host}:{b.port}, Scope „{args.scope}“, UID {b.konfig.uid}. Strg+C beendet.")
    try:
        await asyncio.Event().wait()
    finally:
        if dn:
            dn.beenden()
        await b.stoppen()


async def _simulator(args) -> None:
    host, port = _host_port(args.broker)
    g = SimulierterGateway(scope=args.scope, nativ=args.nativ)
    uid = await g.verbinden(host, port)
    print(f"Simulator verbunden als {uid} ({'nativ' if args.nativ else 'Gateway mit Scheinwerfer an Port 1'}). "
          "Strg+C beendet.")
    await g.getrennt.wait()
    print("Getrennt:", g.trenngrund)


async def _auslesen(args) -> int:
    host, port = _host_port(args.broker)
    c = RdmnetController(scope=args.scope)
    await c.verbinden(host, port)
    await c.clientliste()
    aus = Auslesung(c, args.timeout)
    ergebnisse = []
    for adr in await aus.geraete_finden():
        profil = await aus.profil(adr)
        s = await aus.sensor_schnappschuss(adr, profil.sensoren, alle_sensoren=True)
        ergebnisse.append({"profil": profil.als_dict(), "sensoren": s.als_dict()})
        _profil_drucken(profil.als_dict(), s.als_dict())
    await c.trennen()
    if args.json:
        Path(args.json).write_text(json.dumps(ergebnisse, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"JSON geschrieben: {args.json}")
    if not ergebnisse:
        print("Keine RDMnet-Geräte am Broker.")
    return 0


def _profil_drucken(p: dict, s: dict | None = None) -> None:
    di = p.get("device_info") or {}
    pa = p.get("personality_aktuell") or {}
    print(f"\nGerät {p['rdm_uid']} (über {p['rpt_uid']}, Endpoint {p['endpoint']})")
    print(f"  {p['hersteller']} – {p['modell']} – Firmware {p['software_label']}")
    print(f"  Modus {di.get('personality_aktuell')}/{di.get('personality_anzahl')} „{pa.get('beschreibung', '?')}“, "
          f"Footprint {di.get('footprint')}, Startadresse {di.get('startadresse')}, Universe {p.get('universe')}")
    print(f"  Betriebsstunden {p.get('geraete_stunden')}, Lampenstunden {p.get('lampen_stunden')}")
    if p.get("nicht_verfuegbar"):
        print("  Nicht verfügbar:", ", ".join(f"{k} ({v})" for k, v in p["nicht_verfuegbar"].items()))
    if s:
        for w in s["werte"]:
            mark = "  !" if w["ausserhalb_normal"] else "   "
            rel = "" if w["messrelevant"] else " (nicht messrelevant)"
            print(f" {mark} Sensor {w['nummer']} {w['beschreibung']}: {w['wert']:.3g} {w['einheit_text']}{rel}")


def _livetest(args) -> int:
    from .livetest import LiveKonfig, LiveTest, adapter_waehlen
    host = args.host or adapter_waehlen()
    if not host:
        return 2
    erwartung = None
    if args.modus or args.footprint or args.startadresse:
        if not (args.modus and args.footprint and args.startadresse):
            print("Für den Abgleich --modus, --footprint und --startadresse zusammen angeben.")
            return 2
        erwartung = EinstellungsErwartung(args.modus, args.footprint, args.startadresse, args.universe)
    k = LiveKonfig(host=host, port=args.port, scope=args.scope, ordner=Path(args.ordner),
                   warte_auf_geraete_s=args.warten, suche_responder_s=args.suche, rdm_timeout_s=args.timeout,
                   dnssd=not args.ohne_mdns, erwartung=erwartung)
    print(f"pyrdmnet {__version__} – LIVE-TEST, nur lesend (kein RDM SET)")
    erg = asyncio.run(LiveTest(k).ausfuehren())
    return 0 if erg.get("erfolgreich") else 1


def _schreibtest(args) -> int:
    from .livetest import LiveKonfig, adapter_waehlen
    from .rdm import Uid
    from .schreibtest import SchreibKonfig, SchreibTest
    host = args.host or adapter_waehlen()
    if not host:
        return 2
    k = LiveKonfig(host=host, port=args.port, scope=args.scope, ordner=Path(args.ordner),
                   warte_auf_geraete_s=args.warten, suche_responder_s=args.suche, rdm_timeout_s=args.timeout,
                   dnssd=not args.ohne_mdns)
    sk = SchreibKonfig(args.ziel_modus, args.ziel_adresse, args.zurueck_modus, args.zurueck_adresse,
                       Uid.aus_text(args.uid) if args.uid else None)
    print(f"pyrdmnet {__version__} – SCHREIBTEST (RDM SET am realen Gerät, mit Checkliste)")
    erg = asyncio.run(SchreibTest(k, sk).ausfuehren())
    return 0 if erg.get("erfolgreich") else 1


def _demo(args) -> int:
    db = Path(args.db)
    if db.exists() and not args.weiter:
        db.unlink()
    print(f"pyrdmnet {__version__} – Offline-Demo (alle Gerätedaten SIMULATION)\n")
    modul = RdmModul(RdmKonfig(broker_port=0, db=db, heartbeat_intervall_s=15))
    modul.starten()
    gateway = SimulierterGateway()
    gateway.scheinwerfer.personality = 1  # absichtlich falscher Modus
    gateway.scheinwerfer.startadresse = 1  # absichtlich falsche Adresse
    modul._run(gateway.verbinden("127.0.0.1", modul.port))
    try:
        adressen = modul.geraete_finden()
        print("Gefunden:", ", ".join(map(str, adressen)))
        profil = modul.geraet_auslesen(adressen[0])
        _profil_drucken(profil.als_dict())

        erwartung = EinstellungsErwartung("Direkt 6ch", footprint=6, startadresse=100, universe=1)
        a = modul.einstellung_pruefen(profil, erwartung, bezug="Demo")
        print(f"\n1) Abgleich gegen Messplan: {a.status}, Messung erlaubt: {'Ja' if a.messung_erlaubt else 'Nein'}")
        for x in a.abweichungen + a.hinweise:
            print("   -", x)

        print("\n2) Zuordnung bestätigen: Personality 2 = GDTF-Modus „Direkt 6ch“ (Demo-Bestätigung)")
        modul.zuordnung_bestaetigen(profil, "Direkt 6ch", 2, bestaetigt_von="Demo")

        print("3) Schreibfreigabe für diese Sitzung aktivieren und Einstellungen anwenden")
        modul.schreibfreigabe_setzen(True)
        anw, profil, a = modul.einstellung_anwenden(profil, erwartung, bezug="Demo")
        for x in anw.aenderungen:
            print(f"   - {x['parameter']}: {x['alt']} -> {x['neu']}")
        for x in anw.fehler:
            print("   - FEHLER:", x)
        print(f"   Neuer Abgleich: {a.status}, Messung erlaubt: {'Ja' if a.messung_erlaubt else 'Nein'}")
        modul.schreibfreigabe_setzen(False)

        print("\n4) Drei begleitete Messungen (Sensor-Schnappschuss vor/nach)")
        for i in range(1, 4):
            with modul.messung_begleiten(profil, messung_ref=f"DEMO-{i:03d}") as e:
                pass
            v, n = e["vor"], e["nach"]
            temp = [w for w in n.werte if w.typ == 0x00]
            print(f"   DEMO-{i:03d}: vor {v.status} ({len(v.werte)} Werte), nach {n.status}; "
                  + ", ".join(f"{w.beschreibung} {w.wert:.1f} °C" for w in temp))
        print("\nSensordaten zu DEMO-002 aus der Datenbank:")
        for z in modul.speicher.sensorwerte_zu_messung("DEMO-002"):
            print(f"   {z['phase']:5} {z['beschreibung']:15} {z['wert']:8.2f} {z['einheit']}")
        print(f"\nDatenbank: {db.resolve()}")
        status = modul.broker_status()
        print(f"Broker: {len(status['clients'])} Clients, Zähler {status['zaehler']}")
    finally:
        modul._run(gateway.trennen())
        modul.stoppen()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m pyrdmnet", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version=__version__)
    ap.add_argument("-v", "--ausfuehrlich", action="store_true", help="Protokollausgabe")
    sub = ap.add_subparsers(dest="befehl", required=True)

    b = sub.add_parser("broker", help="Broker starten")
    b.add_argument("--host", default="127.0.0.1", help="IP des Netzwerkadapters (0.0.0.0 = alle)")
    b.add_argument("--port", type=int, default=8888)
    b.add_argument("--scope", default=K.E133_STANDARD_SCOPE)
    b.add_argument("--log", help="Ereignisse als JSONL in diese Datei")
    b.add_argument("--dnssd", action="store_true", help="per mDNS anmelden (macOS: Bonjour dns-sd)")

    s = sub.add_parser("simulator", help="simuliertes Gerät verbinden")
    s.add_argument("--broker", default="127.0.0.1:8888")
    s.add_argument("--scope", default=K.E133_STANDARD_SCOPE)
    s.add_argument("--nativ", action="store_true", help="ohne Gateway (Scheinwerfer ist RDMnet-Gerät)")

    a = sub.add_parser("auslesen", help="Geräte finden und auslesen")
    a.add_argument("--broker", default="127.0.0.1:8888")
    a.add_argument("--scope", default=K.E133_STANDARD_SCOPE)
    a.add_argument("--timeout", type=float, default=3.0)
    a.add_argument("--json", help="Ergebnis als JSON speichern")

    lt = sub.add_parser("livetest", help="Live-Test mit realem Gateway (nur lesend)")
    lt.add_argument("--host", help="IP des Adapters zum Gateway (ohne Angabe: Auswahlliste)")
    lt.add_argument("--port", type=int, default=8888)
    lt.add_argument("--scope", default=K.E133_STANDARD_SCOPE)
    lt.add_argument("--ohne-mdns", action="store_true",
                    help="Broker NICHT per mDNS anmelden (dann IP/Port im Gateway fest eintragen)")
    lt.add_argument("--warten", type=float, default=180, help="Sekunden warten auf Gateway-Verbindung")
    lt.add_argument("--suche", type=float, default=60, help="Sekunden warten auf Scheinwerfer an den Ports")
    lt.add_argument("--timeout", type=float, default=5.0, help="RDM-Zeitlimit je Anfrage in s")
    lt.add_argument("--ordner", default=".", help="hier wird der Ergebnisordner angelegt")
    lt.add_argument("--modus", help="erwarteter GDTF-Modus (für Abgleich, optional)")
    lt.add_argument("--footprint", type=int)
    lt.add_argument("--startadresse", type=int)
    lt.add_argument("--universe", type=int)

    st = sub.add_parser("schreibtest", help="Modus/Adresse setzen, zurücklesen, zurückstellen (mit Checkliste)")
    st.add_argument("--host", help="IP des Adapters zum Gateway (ohne Angabe: Auswahlliste)")
    st.add_argument("--port", type=int, default=8888)
    st.add_argument("--scope", default=K.E133_STANDARD_SCOPE)
    st.add_argument("--ohne-mdns", action="store_true")
    st.add_argument("--warten", type=float, default=180)
    st.add_argument("--suche", type=float, default=60)
    st.add_argument("--timeout", type=float, default=5.0)
    st.add_argument("--ordner", default=".")
    st.add_argument("--uid", help="RDM-UID des Scheinwerfers (nur nötig bei mehreren)")
    st.add_argument("--ziel-modus", type=int, required=True)
    st.add_argument("--ziel-adresse", type=int, required=True)
    st.add_argument("--zurueck-modus", type=int, help="Standard: gelesener Ausgangsmodus")
    st.add_argument("--zurueck-adresse", type=int, help="Standard: gelesene Ausgangsadresse")

    d = sub.add_parser("demo", help="Offline-Demo")
    d.add_argument("--db", default="pyrdmnet_demo.sqlite")
    d.add_argument("--weiter", action="store_true", help="bestehende Demo-Datenbank weiterverwenden")

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.ausfuehrlich else logging.WARNING,
                        format="%(asctime)s %(name)s %(message)s")
    try:
        if args.befehl == "broker":
            asyncio.run(_broker(args))
        elif args.befehl == "simulator":
            asyncio.run(_simulator(args))
        elif args.befehl == "auslesen":
            return asyncio.run(_auslesen(args))
        elif args.befehl == "schreibtest":
            return _schreibtest(args)
        elif args.befehl == "livetest":
            return _livetest(args)
        elif args.befehl == "demo":
            return _demo(args)
    except KeyboardInterrupt:
        print("\nBeendet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
