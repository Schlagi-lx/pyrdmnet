"""Einstellungen des Scheinwerfers mit dem Messplan abgleichen und anwenden.

Regel (25.10.2026): Funktionstest und Spektralmessung laufen im exakt gleichen
DMX-Modus. Deshalb:
* Abweichung von Startadresse, Footprint oder (bestätigter) Personality sperrt die Messung.
* Die Zuordnung RDM-Personality ↔ GDTF-Modus wird nie geraten. Es gibt nur
  Vorschläge; gesetzt wird ein Modus nur mit bestätigter Zuordnung für genau
  diese Firmware (software_version_id).
* Anwenden (Variante b, 26.10.2026): DMX_PERSONALITY und DMX_START_ADDRESS
  werden per RDM SET gesetzt und anschließend zurückgelesen.
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
    """Was der Messplan verlangt (aus GDTF-Modus und Patch)."""

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
    changes: list[dict] = field(default_factory=list)  # {parameter, alt, neu}
    error: list[str] = field(default_factory=list)
    hints: list[str] = field(default_factory=list)
    time_utc: str = field(default_factory=now_utc)

    def to_dict(self) -> dict:
        return asdict(self)


def mapping_matches(z: PersonalityMapping | None, profile: DeviceProfile) -> bool:
    """Zuordnung gilt nur für dasselbe Modell und dieselbe Firmware."""
    di = profile.device_info
    return bool(z and di and z.manufacturer_id == profile.address.rdm_uid.manufacturer
                and z.model_id == di.model_id and z.software_version_id == di.software_version_id)


def candidates(profile: DeviceProfile, expected: ExpectedSetting) -> list[tuple[R.Personality, float]]:
    """Vorschläge (nicht bestätigt!): Personalities mit passendem Footprint, nach Namensähnlichkeit sortiert."""
    res = []
    for p in profile.personalities:
        if p.footprint != expected.footprint:
            continue
        similar = difflib.SequenceMatcher(None, p.description.lower(), expected.gdtf_mode.lower()).ratio()
        res.append((p, round(similar, 3)))
    return sorted(res, key=lambda x: -x[1])


def check(profile: DeviceProfile | None, expected: ExpectedSetting,
            mapping: PersonalityMapping | None = None, strict: bool = False) -> CheckResult:
    """Vergleicht den gelesenen Gerätezustand mit dem Messplan.

    strikt=False: NICHT_PRUEFBAR und OK_UNBESTAETIGT erlauben die Messung (mit Hinweis),
    strikt=True: nur OK erlaubt die Messung.
    """
    if profile is None or profile.device_info is None:
        reason = "; ".join(f"{k}: {v}" for k, v in (profile.unavailable.items() if profile else [])) or "keine Daten"
        return CheckResult(NOT_CHECKABLE, not strict, hints=[f"Gerät nicht per RDM lesbar ({reason})"])
    di = profile.device_info
    pa = profile.personality_current
    e = CheckResult(OK, True, actual={
        "personality": di.personality_current, "personality_text": pa.description if pa else None,
        "footprint": di.footprint, "start_address": di.start_address, "universe": profile.universe,
        "software_version_id": di.software_version_id})

    if di.start_address != expected.start_address:
        e.deviations.append(f"Startadresse {di.start_address} statt {expected.start_address}")
    if di.footprint != expected.footprint:
        e.deviations.append(f"Footprint {di.footprint} statt {expected.footprint} (GDTF-Modus "
                              f"„{expected.gdtf_mode}“)")
    if expected.universe is not None:
        if profile.universe is None:
            e.hints.append("Universe vom Gateway nicht lesbar – bitte manuell prüfen")
        elif profile.universe != expected.universe:
            e.deviations.append(f"Gateway-Universe {profile.universe} statt {expected.universe}")

    if mapping_matches(mapping, profile) and mapping.gdtf_mode == expected.gdtf_mode:
        e.target_personality = mapping.personality_num
        if di.personality_current != mapping.personality_num:
            e.deviations.append(f"Personality {di.personality_current} statt {mapping.personality_num} "
                                  f"(bestätigt für „{expected.gdtf_mode}“)")
    else:
        if mapping and not mapping_matches(mapping, profile):
            e.hints.append("Personality-Zuordnung gilt für andere Firmware/Modell – neu bestätigen")
        suggestions = ", ".join(f"{p.number} „{p.description}“" for p, _ in candidates(profile, expected)) or "keine"
        e.hints.append(f"Personality ↔ GDTF-Modus nicht bestätigt (Vorschläge mit passendem Footprint: "
                          f"{suggestions})")

    if e.deviations:
        e.status, e.measurement_allowed = DEVIATION, False
    elif e.target_personality is None:
        e.status, e.measurement_allowed = OK_UNCONFIRMED, not strict
    return e


async def apply(controller: RdmnetController, address: DeviceAddress, profile: DeviceProfile,
                   expected: ExpectedSetting, mapping: PersonalityMapping | None,
                   timeout_s: float = 3.0) -> ApplyResult:
    """Setzt Personality und Startadresse laut Messplan und liest zurück.

    Voraussetzung: ``controller.schreibfreigabe`` ist für diese Sitzung aktiv.
    """
    res = ApplyResult(False)
    di = profile.device_info
    if di is None:
        res.error.append("Kein Gerätezustand gelesen – nichts gesetzt")
        return res
    if not 1 <= expected.start_address <= 512 or expected.start_address + expected.footprint - 1 > 512:
        res.error.append(f"Startadresse {expected.start_address} mit Footprint {expected.footprint} "
                          "passt nicht ins Universe – nichts gesetzt")
        return res

    target_pers = None
    if mapping_matches(mapping, profile) and mapping.gdtf_mode == expected.gdtf_mode:
        target_pers = mapping.personality_num
        p = next((x for x in profile.personalities if x.number == target_pers), None)
        if p and p.footprint != expected.footprint:
            res.error.append(f"Bestätigte Personality {target_pers} hat Footprint {p.footprint}, Messplan "
                              f"{expected.footprint} – Zuordnung prüfen, nichts gesetzt")
            return res
    elif di.footprint != expected.footprint:
        res.error.append("Modus müsste geändert werden, aber Personality ↔ GDTF-Modus ist nicht bestätigt "
                          "– nichts gesetzt")
        return res

    async def set_param(pid: int, data: bytes, name: str, old, new) -> bool:
        try:
            await controller.rdm_set(address.rpt_uid, pid, data, endpoint=address.endpoint,
                                     rdm_dest=address.rdm_uid, timeout_s=timeout_s)
        except RdmError as f:
            res.error.append(f"{name} setzen fehlgeschlagen: {f}")
            return False
        res.changes.append({"parameter": name, "old": old, "new": new})
        return True

    # Reihenfolge: erst Modus (Footprint ändert sich), dann Adresse
    if target_pers is not None and di.personality_current != target_pers:
        if not await set_param(C.PID_DMX_PERSONALITY, bytes((target_pers,)), "DMX_PERSONALITY",
                            di.personality_current, target_pers):
            return res
    if di.start_address != expected.start_address:
        if not await set_param(C.PID_DMX_START_ADDRESS, struct.pack(">H", expected.start_address),
                            "DMX_START_ADDRESS", di.start_address, expected.start_address):
            return res

    # Zurücklesen
    try:
        after = await DeviceReader(controller, timeout_s).read_setting(address)
    except RdmError as f:
        res.error.append(f"Zurücklesen fehlgeschlagen: {f}")
        return res
    if target_pers is not None and after.personality_current != target_pers:
        res.error.append(f"Rücklesen: Personality {after.personality_current} statt {target_pers}")
    if after.footprint != expected.footprint:
        res.error.append(f"Rücklesen: Footprint {after.footprint} statt {expected.footprint}")
    if after.start_address != expected.start_address:
        res.error.append(f"Rücklesen: Startadresse {after.start_address} statt {expected.start_address}")
    if expected.universe is not None and profile.universe not in (None, expected.universe):
        res.hints.append("Universe wird am Gateway eingestellt, nicht per RDM gesetzt – bitte prüfen")
    res.success = not res.error
    return res
