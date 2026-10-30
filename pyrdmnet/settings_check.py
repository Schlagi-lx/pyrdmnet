"""Check the fixture's settings against the measurement plan and apply them.

Rule (2026-10-25): the function test and the spectral measurement run in exactly
the same DMX mode. Therefore:
* A deviation in start address, footprint or (confirmed) personality blocks the measurement.
* The mapping RDM personality <-> GDTF mode is never guessed. There are only
  suggestions; a mode is set only with a confirmed mapping for exactly
  this firmware (software_version_id).
* Apply (variant b, 2026-10-26): DMX_PERSONALITY and DMX_START_ADDRESS
  are set via RDM SET and then read back.
"""

from __future__ import annotations

import difflib
import struct
from dataclasses import asdict, dataclass, field

from . import constants as C
from . import rdm_params as R
from .readout import DeviceReader, DeviceAddress, DeviceProfile, now_utc
from .controller import RdmError, RdmnetController

OK = "OK"
OK_UNCONFIRMED = "OK_UNCONFIRMED"
DEVIATION = "DEVIATION"
NOT_CHECKABLE = "NOT_CHECKABLE"


@dataclass
class ExpectedSetting:
    """What the measurement plan requires (from GDTF mode and patch)."""

    gdtf_mode: str
    footprint: int
    start_address: int
    universe: int | None = None


@dataclass
class PersonalityMapping:
    manufacturer_id: int
    model_id: int
    software_version_id: int
    personality_num: int
    gdtf_mode: str
    confirmed_utc: str
    confirmed_by: str = ""


@dataclass
class CheckResult:
    status: str
    measurement_allowed: bool
    deviations: list[str] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)
    target_personality: int | None = None
    actual: dict = field(default_factory=dict)
    time_utc: str = field(default_factory=now_utc)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ApplyResult:
    success: bool
    changes: list[dict] = field(default_factory=list)  # {parameter, old, new}
    error: list[str] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)
    time_utc: str = field(default_factory=now_utc)

    def to_dict(self) -> dict:
        return asdict(self)


def mapping_matches(z: PersonalityMapping | None, profile: DeviceProfile) -> bool:
    """A mapping is only valid for the same model and the same firmware."""
    di = profile.device_info
    return bool(z and di and z.manufacturer_id == profile.address.rdm_uid.manufacturer
                and z.model_id == di.model_id and z.software_version_id == di.software_version_id)


def candidates(profile: DeviceProfile, expected: ExpectedSetting) -> list[tuple[R.Personality, float]]:
    """Suggestions (not confirmed!): personalities with matching footprint, sorted by name similarity."""
    res = []
    for p in profile.personalities:
        if p.footprint != expected.footprint:
            continue
        similar = difflib.SequenceMatcher(None, p.description.lower(), expected.gdtf_mode.lower()).ratio()
        res.append((p, round(similar, 3)))
    return sorted(res, key=lambda x: -x[1])


def check(profile: DeviceProfile | None, expected: ExpectedSetting,
            mapping: PersonalityMapping | None = None, strict: bool = False) -> CheckResult:
    """Compare the device state read out with the measurement plan.

    strict=False: NOT_CHECKABLE and OK_UNCONFIRMED allow the measurement (with a hint),
    strict=True: only OK allows the measurement.
    """
    if profile is None or profile.device_info is None:
        reason = "; ".join(f"{k}: {v}" for k, v in (profile.unavailable.items() if profile else [])) or "no data"
        return CheckResult(NOT_CHECKABLE, not strict, hints=[f"Device not readable via RDM ({reason})"])
    di = profile.device_info
    pa = profile.personality_current
    e = CheckResult(OK, True, actual={
        "personality": di.personality_current, "personality_text": pa.description if pa else None,
        "footprint": di.footprint, "start_address": di.start_address, "universe": profile.universe,
        "software_version_id": di.software_version_id})

    if di.start_address != expected.start_address:
        e.deviations.append(f"Start address {di.start_address} instead of {expected.start_address}")
    if di.footprint != expected.footprint:
        e.deviations.append(f"Footprint {di.footprint} instead of {expected.footprint} (GDTF mode "
                              f"\"{expected.gdtf_mode}\")")
    if expected.universe is not None:
        if profile.universe is None:
            e.hints.append("Universe not readable from the gateway - please check manually")
        elif profile.universe != expected.universe:
            e.deviations.append(f"Gateway universe {profile.universe} instead of {expected.universe}")

    if mapping_matches(mapping, profile) and mapping.gdtf_mode == expected.gdtf_mode:
        e.target_personality = mapping.personality_num
        if di.personality_current != mapping.personality_num:
            e.deviations.append(f"Personality {di.personality_current} instead of {mapping.personality_num} "
                                  f"(confirmed for \"{expected.gdtf_mode}\")")
    else:
        if mapping and not mapping_matches(mapping, profile):
            e.hints.append("Personality mapping applies to a different firmware/model - confirm again")
        suggestions = ", ".join(f"{p.number} \"{p.description}\"" for p, _ in candidates(profile, expected)) or "none"
        e.hints.append(f"Personality <-> GDTF mode not confirmed (suggestions with matching footprint: "
                          f"{suggestions})")

    if e.deviations:
        e.status, e.measurement_allowed = DEVIATION, False
    elif e.target_personality is None:
        e.status, e.measurement_allowed = OK_UNCONFIRMED, not strict
    return e


async def apply(controller: RdmnetController, address: DeviceAddress, profile: DeviceProfile,
                   expected: ExpectedSetting, mapping: PersonalityMapping | None,
                   timeout_s: float = 3.0) -> ApplyResult:
    """Set personality and start address according to the measurement plan and read them back.

    Precondition: ``controller.write_enabled`` is active for this session.
    """
    res = ApplyResult(False)
    di = profile.device_info
    if di is None:
        res.error.append("No device state read - nothing set")
        return res
    if not 1 <= expected.start_address <= 512 or expected.start_address + expected.footprint - 1 > 512:
        res.error.append(f"Start address {expected.start_address} with footprint {expected.footprint} "
                          "does not fit into the universe - nothing set")
        return res

    target_pers = None
    if mapping_matches(mapping, profile) and mapping.gdtf_mode == expected.gdtf_mode:
        target_pers = mapping.personality_num
        p = next((x for x in profile.personalities if x.number == target_pers), None)
        if p and p.footprint != expected.footprint:
            res.error.append(f"Confirmed personality {target_pers} has footprint {p.footprint}, measurement plan "
                              f"{expected.footprint} - check mapping, nothing set")
            return res
    elif di.footprint != expected.footprint:
        res.error.append("Mode would have to be changed, but personality <-> GDTF mode is not confirmed "
                          "- nothing set")
        return res

    async def set_param(pid: int, data: bytes, name: str, old, new) -> bool:
        try:
            await controller.rdm_set(address.rpt_uid, pid, data, endpoint=address.endpoint,
                                     rdm_dest=address.rdm_uid, timeout_s=timeout_s)
        except RdmError as f:
            res.error.append(f"Setting {name} failed: {f}")
            return False
        res.changes.append({"parameter": name, "old": old, "new": new})
        return True

    # Order: mode first (footprint changes), then address
    if target_pers is not None and di.personality_current != target_pers:
        if not await set_param(C.PID_DMX_PERSONALITY, bytes((target_pers,)), "DMX_PERSONALITY",
                            di.personality_current, target_pers):
            return res
    if di.start_address != expected.start_address:
        if not await set_param(C.PID_DMX_START_ADDRESS, struct.pack(">H", expected.start_address),
                            "DMX_START_ADDRESS", di.start_address, expected.start_address):
            return res

    # Read back
    try:
        after = await DeviceReader(controller, timeout_s).read_setting(address)
    except RdmError as f:
        res.error.append(f"Read back failed: {f}")
        return res
    if target_pers is not None and after.personality_current != target_pers:
        res.error.append(f"Read back: personality {after.personality_current} instead of {target_pers}")
    if after.footprint != expected.footprint:
        res.error.append(f"Read back: footprint {after.footprint} instead of {expected.footprint}")
    if after.start_address != expected.start_address:
        res.error.append(f"Read back: start address {after.start_address} instead of {expected.start_address}")
    if expected.universe is not None and profile.universe not in (None, expected.universe):
        res.hints.append("The universe is configured on the gateway, not set via RDM - please check")
    res.success = not res.error
    return res
