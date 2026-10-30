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

from . import konstanten as K
from . import rdm_parameter as R
from .auslesen import Auslesung, GeraeteAdresse, GeraeteProfil, jetzt_utc
from .controller import RdmFehler, RdmnetController

OK = "OK"
OK_UNBESTAETIGT = "OK_UNBESTAETIGT"
ABWEICHUNG = "ABWEICHUNG"
NICHT_PRUEFBAR = "NICHT_PRUEFBAR"


@dataclass
class EinstellungsErwartung:
    """Was der Messplan verlangt (aus GDTF-Modus und Patch)."""

    gdtf_modus: str
    footprint: int
    startadresse: int
    universe: int | None = None


@dataclass
class PersonalityZuordnung:
    hersteller_id: int
    modell_id: int
    software_version_id: int
    personality_nr: int
    gdtf_modus: str
    bestaetigt_utc: str
    bestaetigt_von: str = ""


@dataclass
class AbgleichErgebnis:
    status: str
    messung_erlaubt: bool
    abweichungen: list[str] = field(default_factory=list)
    hinweise: list[str] = field(default_factory=list)
    soll_personality: int | None = None
    ist: dict = field(default_factory=dict)
    zeit_utc: str = field(default_factory=jetzt_utc)

    def als_dict(self) -> dict:
        return asdict(self)


@dataclass
class AnwendungsErgebnis:
    erfolgreich: bool
    aenderungen: list[dict] = field(default_factory=list)  # {parameter, alt, neu}
    fehler: list[str] = field(default_factory=list)
    hinweise: list[str] = field(default_factory=list)
    zeit_utc: str = field(default_factory=jetzt_utc)

    def als_dict(self) -> dict:
        return asdict(self)


def zuordnung_passt(z: PersonalityZuordnung | None, profil: GeraeteProfil) -> bool:
    """Zuordnung gilt nur für dasselbe Modell und dieselbe Firmware."""
    di = profil.device_info
    return bool(z and di and z.hersteller_id == profil.adresse.rdm_uid.hersteller
                and z.modell_id == di.modell_id and z.software_version_id == di.software_version_id)


def kandidaten(profil: GeraeteProfil, erwartung: EinstellungsErwartung) -> list[tuple[R.Personality, float]]:
    """Vorschläge (nicht bestätigt!): Personalities mit passendem Footprint, nach Namensähnlichkeit sortiert."""
    erg = []
    for p in profil.personalities:
        if p.footprint != erwartung.footprint:
            continue
        aehnlich = difflib.SequenceMatcher(None, p.beschreibung.lower(), erwartung.gdtf_modus.lower()).ratio()
        erg.append((p, round(aehnlich, 3)))
    return sorted(erg, key=lambda x: -x[1])


def pruefen(profil: GeraeteProfil | None, erwartung: EinstellungsErwartung,
            zuordnung: PersonalityZuordnung | None = None, strikt: bool = False) -> AbgleichErgebnis:
    """Vergleicht den gelesenen Gerätezustand mit dem Messplan.

    strikt=False: NICHT_PRUEFBAR und OK_UNBESTAETIGT erlauben die Messung (mit Hinweis),
    strikt=True: nur OK erlaubt die Messung.
    """
    if profil is None or profil.device_info is None:
        grund = "; ".join(f"{k}: {v}" for k, v in (profil.nicht_verfuegbar.items() if profil else [])) or "keine Daten"
        return AbgleichErgebnis(NICHT_PRUEFBAR, not strikt, hinweise=[f"Gerät nicht per RDM lesbar ({grund})"])
    di = profil.device_info
    pa = profil.personality_aktuell
    e = AbgleichErgebnis(OK, True, ist={
        "personality": di.personality_aktuell, "personality_text": pa.beschreibung if pa else None,
        "footprint": di.footprint, "startadresse": di.startadresse, "universe": profil.universe,
        "software_version_id": di.software_version_id})

    if di.startadresse != erwartung.startadresse:
        e.abweichungen.append(f"Startadresse {di.startadresse} statt {erwartung.startadresse}")
    if di.footprint != erwartung.footprint:
        e.abweichungen.append(f"Footprint {di.footprint} statt {erwartung.footprint} (GDTF-Modus "
                              f"„{erwartung.gdtf_modus}“)")
    if erwartung.universe is not None:
        if profil.universe is None:
            e.hinweise.append("Universe vom Gateway nicht lesbar – bitte manuell prüfen")
        elif profil.universe != erwartung.universe:
            e.abweichungen.append(f"Gateway-Universe {profil.universe} statt {erwartung.universe}")

    if zuordnung_passt(zuordnung, profil) and zuordnung.gdtf_modus == erwartung.gdtf_modus:
        e.soll_personality = zuordnung.personality_nr
        if di.personality_aktuell != zuordnung.personality_nr:
            e.abweichungen.append(f"Personality {di.personality_aktuell} statt {zuordnung.personality_nr} "
                                  f"(bestätigt für „{erwartung.gdtf_modus}“)")
    else:
        if zuordnung and not zuordnung_passt(zuordnung, profil):
            e.hinweise.append("Personality-Zuordnung gilt für andere Firmware/Modell – neu bestätigen")
        vorschlaege = ", ".join(f"{p.nummer} „{p.beschreibung}“" for p, _ in kandidaten(profil, erwartung)) or "keine"
        e.hinweise.append(f"Personality ↔ GDTF-Modus nicht bestätigt (Vorschläge mit passendem Footprint: "
                          f"{vorschlaege})")

    if e.abweichungen:
        e.status, e.messung_erlaubt = ABWEICHUNG, False
    elif e.soll_personality is None:
        e.status, e.messung_erlaubt = OK_UNBESTAETIGT, not strikt
    return e


async def anwenden(controller: RdmnetController, adresse: GeraeteAdresse, profil: GeraeteProfil,
                   erwartung: EinstellungsErwartung, zuordnung: PersonalityZuordnung | None,
                   timeout_s: float = 3.0) -> AnwendungsErgebnis:
    """Setzt Personality und Startadresse laut Messplan und liest zurück.

    Voraussetzung: ``controller.schreibfreigabe`` ist für diese Sitzung aktiv.
    """
    erg = AnwendungsErgebnis(False)
    di = profil.device_info
    if di is None:
        erg.fehler.append("Kein Gerätezustand gelesen – nichts gesetzt")
        return erg
    if not 1 <= erwartung.startadresse <= 512 or erwartung.startadresse + erwartung.footprint - 1 > 512:
        erg.fehler.append(f"Startadresse {erwartung.startadresse} mit Footprint {erwartung.footprint} "
                          "passt nicht ins Universe – nichts gesetzt")
        return erg

    soll_pers = None
    if zuordnung_passt(zuordnung, profil) and zuordnung.gdtf_modus == erwartung.gdtf_modus:
        soll_pers = zuordnung.personality_nr
        p = next((x for x in profil.personalities if x.nummer == soll_pers), None)
        if p and p.footprint != erwartung.footprint:
            erg.fehler.append(f"Bestätigte Personality {soll_pers} hat Footprint {p.footprint}, Messplan "
                              f"{erwartung.footprint} – Zuordnung prüfen, nichts gesetzt")
            return erg
    elif di.footprint != erwartung.footprint:
        erg.fehler.append("Modus müsste geändert werden, aber Personality ↔ GDTF-Modus ist nicht bestätigt "
                          "– nichts gesetzt")
        return erg

    async def setzen(pid: int, daten: bytes, name: str, alt, neu) -> bool:
        try:
            await controller.rdm_set(adresse.rpt_uid, pid, daten, endpoint=adresse.endpoint,
                                     rdm_ziel=adresse.rdm_uid, timeout_s=timeout_s)
        except RdmFehler as f:
            erg.fehler.append(f"{name} setzen fehlgeschlagen: {f}")
            return False
        erg.aenderungen.append({"parameter": name, "alt": alt, "neu": neu})
        return True

    # Reihenfolge: erst Modus (Footprint ändert sich), dann Adresse
    if soll_pers is not None and di.personality_aktuell != soll_pers:
        if not await setzen(K.PID_DMX_PERSONALITY, bytes((soll_pers,)), "DMX_PERSONALITY",
                            di.personality_aktuell, soll_pers):
            return erg
    if di.startadresse != erwartung.startadresse:
        if not await setzen(K.PID_DMX_START_ADDRESS, struct.pack(">H", erwartung.startadresse),
                            "DMX_START_ADDRESS", di.startadresse, erwartung.startadresse):
            return erg

    # Zurücklesen
    try:
        nachher = await Auslesung(controller, timeout_s).einstellung_lesen(adresse)
    except RdmFehler as f:
        erg.fehler.append(f"Zurücklesen fehlgeschlagen: {f}")
        return erg
    if soll_pers is not None and nachher.personality_aktuell != soll_pers:
        erg.fehler.append(f"Rücklesen: Personality {nachher.personality_aktuell} statt {soll_pers}")
    if nachher.footprint != erwartung.footprint:
        erg.fehler.append(f"Rücklesen: Footprint {nachher.footprint} statt {erwartung.footprint}")
    if nachher.startadresse != erwartung.startadresse:
        erg.fehler.append(f"Rücklesen: Startadresse {nachher.startadresse} statt {erwartung.startadresse}")
    if erwartung.universe is not None and profil.universe not in (None, erwartung.universe):
        erg.hinweise.append("Universe wird am Gateway eingestellt, nicht per RDM gesetzt – bitte prüfen")
    erg.erfolgreich = not erg.fehler
    return erg
