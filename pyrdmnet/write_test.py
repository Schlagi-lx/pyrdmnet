"""Schreibtest am realen Scheinwerfer: Modus und Startadresse setzen, zurücklesen,
Ausgangszustand wiederherstellen.

Ablauf:
1. Broker + mDNS starten, auf Gateway warten, Scheinwerfer suchen (wie Live-Test)
2. Ausgangszustand lesen und Plan anzeigen (Ziel und Rückweg)
3. Checkliste vor Ort: jeder Punkt muss mit „j“ bestätigt werden, sonst Abbruch
   ohne jede Änderung
4. Ziel setzen (erst Modus, dann Adresse), zurücklesen und prüfen
5. Rückweg setzen (auch wenn Schritt 4 fehlschlug, sobald etwas geändert
   wurde), zurücklesen und prüfen
6. Alles in Bericht, JSON und SQLite (Tabelle rdm_aenderung) ablegen

Die Modusnummern gibt der Nutzer ausdrücklich an; es wird keine
GDTF-Zuordnung angenommen.
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass

from . import constants as C
from .settings_check import ApplyResult
from .readout import DeviceReader, DeviceAddress, DeviceProfile
from .broker import RdmnetBroker
from .controller import RdmError, RdmnetController
from .live_test import LiveConfig, LiveTest
from .rdm import Uid
from .storage import RdmStore

CHECKLIST = (
    "Der Scheinwerfer darf jetzt umgestellt werden (keine laufende Messung, keine Vorstellung)",
    "Kein Pult/keine andere Software steuert gerade diesen Scheinwerfer",
    "Niemand schaut direkt in den Scheinwerfer (Lichtzustand kann sich beim Moduswechsel ändern)",
    "Ich bleibe am Gerät, bis der Ausgangszustand wiederhergestellt ist",
)


@dataclass
class WriteConfig:
    target_mode: int
    target_address: int
    return_mode: int | None = None  # None = gelesener Ausgangszustand
    return_address: int | None = None
    uid: Uid | None = None  # bei mehreren Scheinwerfern Pflicht
    readback_attempts: int = 5
    pause_after_set_s: float = 0.5


class WriteTest(LiveTest):
    folder_prefix = "write_test"
    report_title = "Schreibtest"
    report_mode = "RDM SET (DMX_PERSONALITY, DMX_START_ADDRESS) nach Checkliste, mit Zurücklesen und Rückweg"

    def __init__(self, k: LiveConfig, wc: WriteConfig, output=print, input_fn=input):
        super().__init__(k, output)
        self.wc = wc
        self.input_fn = input_fn
        self.result["write_test"] = {"steps": [], "success": False}

    # ----------------------------------------------------------------- Ablauf
    async def _read_devices(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.connect(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.client_list()
        except Exception as f:
            self._error(f"Eigener Controller kann sich nicht verbinden: {f}")
            return
        reader = DeviceReader(c, self.k.rdm_timeout_s)
        store = RdmStore(self.folder / "schreibtest.sqlite")
        try:
            for e in c.devices():
                gp = await reader.profile(DeviceAddress(e.uid, 0, e.uid))
                self.result["gateways"].append(gp.to_dict())
            addresses = await self._find_responders(c, reader)
            dmx_addr = self._choose_target(addresses)
            if dmx_addr is None:
                return
            before = await reader.profile(dmx_addr)
            gid = store.save_profile(before)
            self.result["devices"].append({"profile": before.to_dict()})
            plan = self._plan(before)
            if plan is None:
                return
            if not await self._checklist():
                self._error("Checkliste nicht vollständig bestätigt – nichts geändert")
                return
            c.write_enabled = True
            try:
                await self._execute(c, reader, dmx_addr, plan, store, gid)
            finally:
                c.write_enabled = False
            after = await reader.profile(dmx_addr)
            store.save_profile(after)
            self.result["write_test"]["profile_after"] = after.to_dict()
        finally:
            store.close()
            await c.disconnect()

    def _choose_target(self, addresses: list[DeviceAddress]) -> DeviceAddress | None:
        if self.wc.uid:
            matches = [a for a in addresses if a.rdm_uid == self.wc.uid]
            if not matches:
                self._error(f"Scheinwerfer {self.wc.uid} nicht gefunden")
                return None
            return matches[0]
        if len(addresses) == 1:
            return addresses[0]
        if addresses:
            self._error("Mehrere Scheinwerfer gefunden – bitte mit --uid einen auswählen: "
                         + ", ".join(str(a.rdm_uid) for a in addresses))
        return None

    def _plan(self, p: DeviceProfile) -> dict | None:
        di = p.device_info
        if di is None:
            self._error(f"Ausgangszustand nicht lesbar ({p.unavailable}) – nichts geändert")
            return None
        modes = {x.number: x for x in p.personalities}
        t_mode, t_addr = self.wc.target_mode, self.wc.target_address
        r_mode = self.wc.return_mode if self.wc.return_mode is not None else di.personality_current
        r_addr = self.wc.return_address if self.wc.return_address is not None else di.start_address
        for num, dmx_addr, what in ((t_mode, t_addr, "Ziel"), (r_mode, r_addr, "Rückweg")):
            if num not in modes:
                self._error(f"{what}: Modus {num} gibt es an diesem Gerät nicht (vorhanden: {sorted(modes)})")
                return None
            if not 1 <= dmx_addr <= 512 or dmx_addr + modes[num].footprint - 1 > 512:
                self._error(f"{what}: Startadresse {dmx_addr} mit {modes[num].footprint} Kanälen passt nicht ins Universe")
                return None
        plan = {"before": (di.personality_current, di.start_address),
                "target": (t_mode, t_addr, modes[t_mode].footprint),
                "return": (r_mode, r_addr, modes[r_mode].footprint)}
        self.result["write_test"]["plan"] = {
            "before": {"mode": di.personality_current, "address": di.start_address},
            "target": {"mode": t_mode, "address": t_addr}, "return": {"mode": r_mode, "address": r_addr}}
        def txt(num): return f"{num} „{modes[num].description}“ ({modes[num].footprint} Kanäle)"
        self.p(f"\nSchreibtest an {p.manufacturer} {p.model} ({p.address.rdm_uid}):")
        self.p(f"  jetzt:      Modus {txt(di.personality_current)}, Startadresse {di.start_address}")
        self.p(f"  Schritt 1:  Modus {txt(t_mode)}, Startadresse {t_addr}  → zurücklesen")
        self.p(f"  Schritt 2:  Modus {txt(r_mode)}, Startadresse {r_addr}  → zurücklesen")
        if (r_mode, r_addr) != (di.personality_current, di.start_address):
            self.p("  ACHTUNG: Der Rückweg weicht vom jetzigen Zustand ab.")
        return plan

    async def _checklist(self) -> bool:
        """Eingaben laufen in einem eigenen Thread, damit Broker-Heartbeats während der Abfrage weiterlaufen."""
        self.p("\nCheckliste vor Ort (je Punkt j = ja, sonst Abbruch):")
        confirmed = []
        for item in CHECKLIST:
            response = (await asyncio.to_thread(self.input_fn, f"  {item}? [j/n] ")).strip().lower()
            ok = response in ("j", "ja", "y", "yes")
            confirmed.append({"item": item, "response": response})
            if not ok:
                self.result["write_test"]["checklist"] = confirmed
                return False
        self.result["write_test"]["checklist"] = confirmed
        return True

    async def _execute(self, c, reader, dmx_addr, plan, store: RdmStore, gid: int) -> None:
        t_mode, t_addr, t_fp = plan["target"]
        r_mode, r_addr, r_fp = plan["return"]
        st = self.result["write_test"]
        self.p("\nSchritt 1: Ziel setzen")
        s1 = await self._set_and_verify(c, reader, dmx_addr, t_mode, t_addr, t_fp, "Schritt 1", plan["before"])
        store.save_application(gid, s1["application"], "SCHREIBTEST Schritt 1")
        st["steps"].append(s1["report"])
        if not s1["application"].changes and not s1["report"]["ok"]:
            self._error("Schritt 1 hat nichts geändert – Rückweg entfällt")
            return
        self.p("\nSchritt 2: zurückstellen")
        s2 = await self._set_and_verify(c, reader, dmx_addr, r_mode, r_addr, r_fp, "Schritt 2", (t_mode, t_addr))
        store.save_application(gid, s2["application"], "SCHREIBTEST Schritt 2")
        st["steps"].append(s2["report"])
        st["success"] = s1["report"]["ok"] and s2["report"]["ok"]
        if not s2["report"]["ok"]:
            self._error("RÜCKWEG NICHT BESTÄTIGT – Scheinwerfer bitte von Hand prüfen und einstellen: "
                         f"Modus {r_mode}, Startadresse {r_addr}")
        elif not s1["report"]["ok"]:
            self._error("Schritt 1 nicht wie erwartet zurückgelesen (Rückweg war erfolgreich)")

    async def _set_and_verify(self, c: RdmnetController, reader: DeviceReader, dmx_addr: DeviceAddress, mode: int,
                                  address: int, footprint: int, name: str, old: tuple[int, int]) -> dict:
        app = ApplyResult(False)
        report = {"step": name, "target": {"mode": mode, "address": address, "footprint": footprint},
                   "set": [], "error": [], "actual": None, "ok": False}

        async def set_param(pid: int, data: bytes, param: str, value, old_value) -> bool:
            try:
                await c.rdm_set(dmx_addr.rpt_uid, pid, data, endpoint=dmx_addr.endpoint, rdm_dest=dmx_addr.rdm_uid,
                                timeout_s=self.k.rdm_timeout_s)
            except RdmError as f:
                report["error"].append(f"{param} setzen: {f}")
                app.error.append(f"{param} setzen: {f}")
                self.p(f"  {param} = {value}: FEHLER {f}")
                return False
            report["set"].append({param: value})
            app.changes.append({"parameter": param, "old": old_value, "new": value})
            self.p(f"  {param} = {value}: bestätigt (ACK)")
            await asyncio.sleep(self.wc.pause_after_set_s)
            return True

        if await set_param(C.PID_DMX_PERSONALITY, bytes((mode,)), "DMX_PERSONALITY", mode, old[0]):
            await set_param(C.PID_DMX_START_ADDRESS, struct.pack(">H", address), "DMX_START_ADDRESS", address, old[1])

        actual = None
        for attempt in range(self.wc.readback_attempts):
            try:
                actual = await reader.read_setting(dmx_addr)
            except RdmError as f:
                report["error"].append(f"Zurücklesen Versuch {attempt + 1}: {f}")
                await asyncio.sleep(1)
                continue
            if (actual.personality_current, actual.start_address, actual.footprint) == (mode, address, footprint):
                break
            await asyncio.sleep(1)  # Gerät braucht evtl. einen Moment nach dem Moduswechsel
        if actual:
            report["actual"] = {"mode": actual.personality_current, "address": actual.start_address,
                              "footprint": actual.footprint}
            report["ok"] = (actual.personality_current, actual.start_address, actual.footprint) == (mode, address, footprint)
            self.p(f"  zurückgelesen: Modus {actual.personality_current}, Startadresse {actual.start_address}, "
                   f"{actual.footprint} Kanäle → {'OK' if report['ok'] else 'ABWEICHUNG'}")
        else:
            self.p("  zurücklesen: FEHLGESCHLAGEN")
        app.success = report["ok"]
        return {"application": app, "report": report}

    # ----------------------------------------------------------------- Abschluss
    def _succeeded(self) -> bool:
        return bool(self.result["write_test"].get("success")) and not self.result["error"]

    def _write_files(self, broker) -> None:
        super()._write_files(broker)
        st = self.result["write_test"]
        z = ["", "## Schreibtest", ""]
        if "plan" in st:
            p = st["plan"]
            z.append(f"- Vorher: Modus {p['before']['mode']}, Adresse {p['before']['address']}")
            z.append(f"- Ziel: Modus {p['target']['mode']}, Adresse {p['target']['address']}")
            z.append(f"- Rückweg: Modus {p['return']['mode']}, Adresse {p['return']['address']}")
        for s in st["steps"]:
            z.append(f"- {s['step']}: Soll {s['target']}, Ist {s['actual']}, {'OK' if s['ok'] else 'NICHT OK'}"
                     + (f", Fehler: {'; '.join(s['error'])}" if s["error"] else ""))
        z.append(f"- Ergebnis: {'success' if st.get('success') else 'nicht erfolgreich'}")
        with open(self.folder / "bericht.md", "a", encoding="utf-8") as f:
            f.write("\n".join(z) + "\n")
