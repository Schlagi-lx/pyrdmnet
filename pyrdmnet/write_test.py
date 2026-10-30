"""Write test on a real fixture: set mode and start address, read back,
restore the initial state.

Flow:
1. Start broker + mDNS, wait for the gateway, find fixtures (like the live test)
2. Read the initial state and show the plan (target and return step)
3. On-site checklist: every item must be confirmed with "y", otherwise abort
   without any change
4. Set the target (mode first, then address), read back and verify
5. Apply the return step (also if step 4 failed, as soon as anything was
   changed), read back and verify
6. Store everything in report, JSON and SQLite (table rdm_change)

The user specifies the mode numbers explicitly; no GDTF mapping is assumed.
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
    "The fixture may be reconfigured now (no measurement running, no show)",
    "No console or other software is currently controlling this fixture",
    "Nobody is looking directly into the fixture (light output may change when the mode changes)",
    "I will stay at the device until the initial state has been restored",
)


@dataclass
class WriteConfig:
    target_mode: int
    target_address: int
    return_mode: int | None = None  # None = initial state as read
    return_address: int | None = None
    uid: Uid | None = None  # required if there are several fixtures
    readback_attempts: int = 5
    pause_after_set_s: float = 0.5


class WriteTest(LiveTest):
    folder_prefix = "write_test"
    report_title = "Write test"
    report_mode = "RDM SET (DMX_PERSONALITY, DMX_START_ADDRESS) after checklist, with read back and return step"

    def __init__(self, k: LiveConfig, wc: WriteConfig, output=print, input_fn=input):
        super().__init__(k, output)
        self.wc = wc
        self.input_fn = input_fn
        self.result["write_test"] = {"steps": [], "success": False}

    # ----------------------------------------------------------------- Flow
    async def _read_devices(self, broker: RdmnetBroker) -> None:
        c = RdmnetController()
        try:
            await c.connect(self.k.host, broker.port, timeout_s=self.k.rdm_timeout_s)
            await c.client_list()
        except Exception as f:
            self._error(f"Own controller cannot connect: {f}")
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
                self._error("Checklist not fully confirmed - nothing changed")
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
                self._error(f"Fixture {self.wc.uid} not found")
                return None
            return matches[0]
        if len(addresses) == 1:
            return addresses[0]
        if addresses:
            self._error("Several fixtures found - please select one with --uid: "
                         + ", ".join(str(a.rdm_uid) for a in addresses))
        return None

    def _plan(self, p: DeviceProfile) -> dict | None:
        di = p.device_info
        if di is None:
            self._error(f"Initial state not readable ({p.unavailable}) - nothing changed")
            return None
        modes = {x.number: x for x in p.personalities}
        t_mode, t_addr = self.wc.target_mode, self.wc.target_address
        r_mode = self.wc.return_mode if self.wc.return_mode is not None else di.personality_current
        r_addr = self.wc.return_address if self.wc.return_address is not None else di.start_address
        for num, dmx_addr, what in ((t_mode, t_addr, "Target"), (r_mode, r_addr, "Return step")):
            if num not in modes:
                self._error(f"{what}: mode {num} does not exist on this device (available: {sorted(modes)})")
                return None
            if not 1 <= dmx_addr <= 512 or dmx_addr + modes[num].footprint - 1 > 512:
                self._error(f"{what}: start address {dmx_addr} with {modes[num].footprint} channels "
                            "does not fit into the universe")
                return None
        plan = {"before": (di.personality_current, di.start_address),
                "target": (t_mode, t_addr, modes[t_mode].footprint),
                "return": (r_mode, r_addr, modes[r_mode].footprint)}
        self.result["write_test"]["plan"] = {
            "before": {"mode": di.personality_current, "address": di.start_address},
            "target": {"mode": t_mode, "address": t_addr}, "return": {"mode": r_mode, "address": r_addr}}
        def txt(num): return f"{num} \"{modes[num].description}\" ({modes[num].footprint} channels)"
        self.p(f"\nWrite test on {p.manufacturer} {p.model} ({p.address.rdm_uid}):")
        self.p(f"  now:        mode {txt(di.personality_current)}, start address {di.start_address}")
        self.p(f"  Step 1:     mode {txt(t_mode)}, start address {t_addr}  -> read back")
        self.p(f"  Step 2:     mode {txt(r_mode)}, start address {r_addr}  -> read back")
        if (r_mode, r_addr) != (di.personality_current, di.start_address):
            self.p("  WARNING: The return step differs from the current state.")
        return plan

    async def _checklist(self) -> bool:
        """Input runs in a separate thread so broker heartbeats continue while waiting for answers."""
        self.p("\nOn-site checklist (y = yes for each item, anything else aborts):")
        confirmed = []
        for item in CHECKLIST:
            response = (await asyncio.to_thread(self.input_fn, f"  {item}? [y/n] ")).strip().lower()
            ok = response in ("y", "yes")
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
        self.p("\nStep 1: set target")
        s1 = await self._set_and_verify(c, reader, dmx_addr, t_mode, t_addr, t_fp, "Step 1", plan["before"])
        store.save_application(gid, s1["application"], "WRITE TEST step 1")
        st["steps"].append(s1["report"])
        if not s1["application"].changes and not s1["report"]["ok"]:
            self._error("Step 1 changed nothing - return step skipped")
            return
        self.p("\nStep 2: restore")
        s2 = await self._set_and_verify(c, reader, dmx_addr, r_mode, r_addr, r_fp, "Step 2", (t_mode, t_addr))
        store.save_application(gid, s2["application"], "WRITE TEST step 2")
        st["steps"].append(s2["report"])
        st["success"] = s1["report"]["ok"] and s2["report"]["ok"]
        if not s2["report"]["ok"]:
            self._error("RETURN STEP NOT CONFIRMED - please check and set the fixture manually: "
                         f"mode {r_mode}, start address {r_addr}")
        elif not s1["report"]["ok"]:
            self._error("Step 1 not read back as expected (return step was successful)")

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
                report["error"].append(f"set {param}: {f}")
                app.error.append(f"set {param}: {f}")
                self.p(f"  {param} = {value}: ERROR {f}")
                return False
            report["set"].append({param: value})
            app.changes.append({"parameter": param, "old": old_value, "new": value})
            self.p(f"  {param} = {value}: confirmed (ACK)")
            await asyncio.sleep(self.wc.pause_after_set_s)
            return True

        if await set_param(C.PID_DMX_PERSONALITY, bytes((mode,)), "DMX_PERSONALITY", mode, old[0]):
            await set_param(C.PID_DMX_START_ADDRESS, struct.pack(">H", address), "DMX_START_ADDRESS", address, old[1])

        actual = None
        for attempt in range(self.wc.readback_attempts):
            try:
                actual = await reader.read_setting(dmx_addr)
            except RdmError as f:
                report["error"].append(f"Read back attempt {attempt + 1}: {f}")
                await asyncio.sleep(1)
                continue
            if (actual.personality_current, actual.start_address, actual.footprint) == (mode, address, footprint):
                break
            await asyncio.sleep(1)  # device may need a moment after the mode change
        if actual:
            report["actual"] = {"mode": actual.personality_current, "address": actual.start_address,
                              "footprint": actual.footprint}
            report["ok"] = (actual.personality_current, actual.start_address, actual.footprint) == (mode, address, footprint)
            self.p(f"  read back: mode {actual.personality_current}, start address {actual.start_address}, "
                   f"{actual.footprint} channels -> {'OK' if report['ok'] else 'MISMATCH'}")
        else:
            self.p("  read back: FAILED")
        app.success = report["ok"]
        return {"application": app, "report": report}

    # ----------------------------------------------------------------- Completion
    def _succeeded(self) -> bool:
        return bool(self.result["write_test"].get("success")) and not self.result["error"]

    def _write_files(self, broker) -> None:
        super()._write_files(broker)
        st = self.result["write_test"]
        z = ["", "## Write test", ""]
        if "plan" in st:
            p = st["plan"]
            z.append(f"- Before: mode {p['before']['mode']}, address {p['before']['address']}")
            z.append(f"- Target: mode {p['target']['mode']}, address {p['target']['address']}")
            z.append(f"- Return step: mode {p['return']['mode']}, address {p['return']['address']}")
        for s in st["steps"]:
            z.append(f"- {s['step']}: expected {s['target']}, actual {s['actual']}, {'OK' if s['ok'] else 'NOT OK'}"
                     + (f", errors: {'; '.join(s['error'])}" if s["error"] else ""))
        z.append(f"- Result: {'successful' if st.get('success') else 'not successful'}")
        with open(self.folder / "bericht.md", "a", encoding="utf-8") as f:
            f.write("\n".join(z) + "\n")
