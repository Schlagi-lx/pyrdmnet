"""Synchrone Einbindungsschnittstelle für das Messlabor.

Das Messlabor arbeitet synchron; RDMnet läuft intern in einem eigenen
asyncio-Thread. Alle Methoden blockieren bis zum Ergebnis.

Typischer Ablauf je Kampagne::

    modul = RdmModul(RdmKonfig(broker_host=adapter_ip, db=messdb_verbindung))
    modul.starten()
    adressen = modul.geraete_finden()
    profil = modul.geraet_auslesen(adressen[0])
    erwartung = EinstellungsErwartung("Direct 6ch", footprint=6, startadresse=100, universe=1)
    abgleich = modul.einstellung_pruefen(profil, erwartung, bezug="Kampagne 7")
    if not abgleich.messung_erlaubt:
        modul.schreibfreigabe_setzen(True)          # nach Checkliste vor Ort
        anw, profil, abgleich = modul.einstellung_anwenden(profil, erwartung, bezug="Kampagne 7")
    for messung_id in ...:
        with modul.messung_begleiten(profil, messung_ref=str(messung_id)):
            ...  # Spektralmessung
    modul.stoppen()

RDM-Fehler während ``messung_begleiten`` werden protokolliert und gespeichert,
aber nie an die Messung weitergereicht.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path

from . import abgleich as A
from . import konstanten as K
from .auslesen import Auslesung, GeraeteAdresse, GeraeteProfil, SensorSchnappschuss, jetzt_utc
from .broker import BrokerKonfig, RdmnetBroker
from .controller import RdmnetController
from .rdm import Uid
from .speicher import RdmSpeicher

log = logging.getLogger("pyrdmnet.fassade")


@dataclass
class RdmKonfig:
    broker_intern: bool = True  # eigenen Broker in diesem Prozess starten
    broker_host: str = "127.0.0.1"  # IP des gewählten Netzwerkadapters
    broker_port: int = 8888  # 0 = frei wählen (nur mit internem Broker sinnvoll)
    scope: str = K.E133_STANDARD_SCOPE
    broker_uid: Uid = field(default_factory=lambda: Uid(K.ESTA_PROTOTYP_HERSTELLER, 0x00000001))
    controller_uid: Uid | None = None  # None = dynamische UID anfordern
    timeout_s: float = 3.0
    alle_sensoren: bool = False  # False: nur messrelevante Sensortypen
    strikt: bool = False  # True: nur Abgleich OK erlaubt eine Messung
    db: str | Path | sqlite3.Connection | None = None
    ereignis_datei: Path | None = None
    dnssd: bool = False  # Broker per mDNS anmelden (benötigt zeroconf)
    heartbeat_intervall_s: float = K.E133_HEARTBEAT_INTERVALL_S
    heartbeat_timeout_s: float = K.E133_HEARTBEAT_TIMEOUT_S
    # V0.21.12 (S11): RDM über Art-Net statt RDMnet (kein Broker); Ziel = Node-IP oder Broadcast
    transport: str = "RDMNET"  # "RDMNET" | "ARTNET"
    artnet_ziel_ip: str | None = None
    artnet_port_adressen: tuple = tuple(range(32))  # Net 0, Universen 1–32 in GelMatch-Zählung
    artnet_bind: tuple = ("0.0.0.0", 6454)
    artnet_ziel_port: int = 6454
    artnet_tod_warte_s: float = 2.0


class RdmModul:
    def __init__(self, konfig: RdmKonfig | None = None):
        self.konfig = konfig or RdmKonfig()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self.broker: RdmnetBroker | None = None
        self.controller: RdmnetController | None = None
        self.speicher: RdmSpeicher | None = None
        self._dnssd = None
        self._geraet_ids: dict[Uid, int] = {}
        self.port: int | None = None

    # ----------------------------------------------------------------- Thread
    def _run(self, coro, timeout: float | None = None):
        if self._loop is None:
            raise RuntimeError("RdmModul nicht gestartet")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def starten(self) -> None:
        if self._loop:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="pyrdmnet", daemon=True)
        self._thread.start()
        if self.konfig.db is not None:
            # SQLite-Verbindungen sind threadgebunden: Speicher wird im Aufrufer-Thread genutzt.
            self.speicher = RdmSpeicher(self.konfig.db)
        try:
            self._run(self._starten_async(), timeout=15)
        except Exception:
            self.stoppen()
            raise

    async def _starten_async(self) -> None:
        k = self.konfig
        if k.transport == "ARTNET":
            from .artnet_rdm import ArtnetRdmController
            if not k.artnet_ziel_ip:
                raise ValueError("Art-Net-RDM braucht eine Ziel-IP (Node oder Broadcast)")
            self.controller = ArtnetRdmController(k.artnet_ziel_ip, uid=k.controller_uid, bind=k.artnet_bind,
                                                  ziel_port=k.artnet_ziel_port)
            await self.controller.verbinden()
            return
        port = k.broker_port
        if k.broker_intern:
            self.broker = RdmnetBroker(BrokerKonfig(
                host=k.broker_host, port=k.broker_port, scope=k.scope, uid=k.broker_uid,
                heartbeat_intervall_s=k.heartbeat_intervall_s, heartbeat_timeout_s=k.heartbeat_timeout_s,
                ereignis_datei=k.ereignis_datei))
            await self.broker.starten()
            port = self.broker.port
            if k.dnssd:
                from .dnssd import DnssdAnmeldung
                self._dnssd = DnssdAnmeldung(k.scope, self.broker.konfig.cid, k.broker_uid, k.broker_host, port)
        self.port = port
        await self._controller_verbinden()

    async def _controller_verbinden(self) -> None:
        k = self.konfig
        self.controller = RdmnetController(uid=k.controller_uid, scope=k.scope,
                                           heartbeat_intervall_s=k.heartbeat_intervall_s,
                                           heartbeat_timeout_s=k.heartbeat_timeout_s)
        await self.controller.verbinden(k.broker_host if k.broker_host != "0.0.0.0" else "127.0.0.1", self.port,
                                        timeout_s=k.timeout_s)
        await self.controller.clientliste(timeout_s=k.timeout_s)

    async def _verbunden(self) -> RdmnetController:
        if self.konfig.transport == "ARTNET":
            if not self.controller.verbunden:
                await self.controller.verbinden()
            return self.controller
        if self.controller is None or not self.controller.verbunden:
            freigabe = self.controller.schreibfreigabe if self.controller else False
            await self._controller_verbinden()
            self.controller.schreibfreigabe = freigabe
        return self.controller

    def stoppen(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._stoppen_async(), timeout=10)
        except Exception as fehler:  # beim Herunterfahren nur protokollieren
            log.warning("Stoppen unvollständig: %s", fehler)
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        self._loop.close()
        self._loop = None
        if self.speicher:
            self.speicher.schliessen()

    async def _stoppen_async(self) -> None:
        if self.controller:
            await self.controller.trennen()
        if self._dnssd:
            self._dnssd.beenden()
        if self.broker:
            await self.broker.stoppen()

    def broker_status(self) -> dict | None:
        return self.broker.status() if self.broker else None

    # ----------------------------------------------------------------- Geräte
    def geraete_finden(self) -> list[GeraeteAdresse]:
        k = self.konfig

        async def f():
            if k.transport == "ARTNET":
                return await (await self._verbunden()).tod_abfragen(list(k.artnet_port_adressen), k.artnet_tod_warte_s)
            return await Auslesung(await self._verbunden(), k.timeout_s).geraete_finden()
        return self._run(f(), timeout=None if k.transport != "ARTNET" else 4 * k.artnet_tod_warte_s + 10)

    def geraet_auslesen(self, adresse: GeraeteAdresse, speichern: bool = True) -> GeraeteProfil:
        async def f():
            return await Auslesung(await self._verbunden(), self.konfig.timeout_s).profil(adresse)
        profil = self._run(f())
        if self.konfig.transport == "ARTNET" and profil.universe is None:
            pa = self.controller.port_adresse(adresse.rdm_uid)
            profil.universe = None if pa is None else pa + 1   # GelMatch-Zählung: Art-Net-Port-Adresse 0 = Universum 1
        if speichern and self.speicher:
            self._geraet_ids[adresse.rdm_uid] = self.speicher.speichere_profil(profil)
        return profil

    # ----------------------------------------------------------------- Einstellungen
    def _zuordnung(self, profil: GeraeteProfil, gdtf_modus: str) -> A.PersonalityZuordnung | None:
        di = profil.device_info
        if not (self.speicher and di):
            return None
        return self.speicher.zuordnung_finden(profil.adresse.rdm_uid.hersteller, di.modell_id,
                                              di.software_version_id, gdtf_modus)

    def zuordnung_bestaetigen(self, profil: GeraeteProfil, gdtf_modus: str, personality_nr: int,
                              bestaetigt_von: str = "") -> A.PersonalityZuordnung:
        """Bestätigt ausdrücklich, welche Personality dem GDTF-Modus entspricht."""
        di = profil.device_info
        if di is None:
            raise ValueError("Profil ohne DEVICE_INFO – Zuordnung nicht möglich")
        if not any(p.nummer == personality_nr for p in profil.personalities):
            raise ValueError(f"Personality {personality_nr} gibt es an diesem Gerät nicht")
        if self.speicher is None:
            raise RuntimeError("Für Zuordnungen wird eine Datenbank benötigt (RdmKonfig.db)")
        z = A.PersonalityZuordnung(profil.adresse.rdm_uid.hersteller, di.modell_id, di.software_version_id,
                                   personality_nr, gdtf_modus, jetzt_utc(), bestaetigt_von)
        self.speicher.zuordnung_bestaetigen(z)
        return z

    def einstellung_pruefen(self, profil: GeraeteProfil, erwartung: A.EinstellungsErwartung,
                            bezug: str | None = None) -> A.AbgleichErgebnis:
        e = A.pruefen(profil, erwartung, self._zuordnung(profil, erwartung.gdtf_modus), self.konfig.strikt)
        if self.speicher:
            self.speicher.speichere_abgleich(self._geraet_ids.get(profil.adresse.rdm_uid), e,
                                             erwartung.gdtf_modus, bezug)
        return e

    def schreibfreigabe_setzen(self, aktiv: bool) -> None:
        """Freigabe für RDM SET in dieser Sitzung (nach Checkliste vor Ort)."""
        async def f():
            (await self._verbunden()).schreibfreigabe = aktiv
        self._run(f())

    def einstellung_anwenden(self, profil: GeraeteProfil, erwartung: A.EinstellungsErwartung,
                             bezug: str | None = None
                             ) -> tuple[A.AnwendungsErgebnis, GeraeteProfil, A.AbgleichErgebnis]:
        """Setzt Modus/Adresse, liest das Gerät neu aus und prüft erneut."""
        zuordnung = self._zuordnung(profil, erwartung.gdtf_modus)

        async def f():
            return await A.anwenden(await self._verbunden(), profil.adresse, profil, erwartung, zuordnung,
                                    self.konfig.timeout_s)
        anwendung = self._run(f())
        if self.speicher:
            self.speicher.speichere_anwendung(self._geraet_ids.get(profil.adresse.rdm_uid), anwendung, bezug)
        neu = self.geraet_auslesen(profil.adresse)
        return anwendung, neu, self.einstellung_pruefen(neu, erwartung, bezug)

    # ----------------------------------------------------------------- Sensoren
    def sensor_schnappschuss(self, profil: GeraeteProfil, messung_ref: str | None = None,
                             phase: str = "einzeln") -> SensorSchnappschuss:
        async def f():
            return await Auslesung(await self._verbunden(), self.konfig.timeout_s).sensor_schnappschuss(
                profil.adresse, profil.sensoren, self.konfig.alle_sensoren)
        try:
            s = self._run(f(), timeout=self.konfig.timeout_s * (len(profil.sensoren) + 2))
        except Exception as fehler:  # RDM darf die Messung nicht stören
            s = SensorSchnappschuss(str(profil.adresse.rdm_uid), jetzt_utc(), 0.0, fehler=[repr(fehler)])
        if self.speicher:
            self.speicher.speichere_schnappschuss(self._geraet_ids.get(profil.adresse.rdm_uid), s, messung_ref,
                                                  phase)
        return s

    @contextlib.contextmanager
    def messung_begleiten(self, profil: GeraeteProfil, messung_ref: str):
        """Sensor-Schnappschuss vor und nach einer Messung; RDM-Fehler werden nur gespeichert."""
        vor = self.sensor_schnappschuss(profil, messung_ref, "vor")
        ergebnis = {"vor": vor, "nach": None}
        try:
            yield ergebnis
        finally:
            ergebnis["nach"] = self.sensor_schnappschuss(profil, messung_ref, "nach")
