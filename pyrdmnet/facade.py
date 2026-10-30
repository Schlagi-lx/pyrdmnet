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

from . import settings_check as A
from . import constants as C
from .readout import DeviceReader, DeviceAddress, DeviceProfile, SensorSnapshot, now_utc
from .broker import BrokerConfig, RdmnetBroker
from .controller import RdmnetController
from .rdm import Uid
from .storage import RdmStore

log = logging.getLogger("pyrdmnet.fassade")


@dataclass
class RdmConfig:
    internal_broker: bool = True  # eigenen Broker in diesem Prozess starten
    broker_host: str = "127.0.0.1"  # IP des gewählten Netzwerkadapters
    broker_port: int = 8888  # 0 = frei wählen (nur mit internem Broker sinnvoll)
    scope: str = C.E133_DEFAULT_SCOPE
    broker_uid: Uid = field(default_factory=lambda: Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x00000001))
    controller_uid: Uid | None = None  # None = dynamische UID anfordern
    timeout_s: float = 3.0
    all_sensors: bool = False  # False: nur messrelevante Sensortypen
    strict: bool = False  # True: nur Abgleich OK erlaubt eine Messung
    db: str | Path | sqlite3.Connection | None = None
    event_file: Path | None = None
    dnssd: bool = False  # Broker per mDNS anmelden (benötigt zeroconf)
    heartbeat_interval_s: float = C.E133_HEARTBEAT_INTERVAL_S
    heartbeat_timeout_s: float = C.E133_HEARTBEAT_TIMEOUT_S
    # V0.21.12 (S11): RDM über Art-Net statt RDMnet (kein Broker); Ziel = Node-IP oder Broadcast
    transport: str = "RDMNET"  # "RDMNET" | "ARTNET"
    artnet_target_ip: str | None = None
    artnet_port_addresses: tuple = tuple(range(32))  # Net 0, Universen 1–32 in GelMatch-Zählung
    artnet_bind: tuple = ("0.0.0.0", 6454)
    artnet_target_port: int = 6454
    artnet_tod_wait_s: float = 2.0


class RdmModule:
    def __init__(self, config: RdmConfig | None = None):
        self.config = config or RdmConfig()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self.broker: RdmnetBroker | None = None
        self.controller: RdmnetController | None = None
        self.store: RdmStore | None = None
        self._dnssd = None
        self._device_ids: dict[Uid, int] = {}
        self.port: int | None = None

    # ----------------------------------------------------------------- Thread
    def _run(self, coro, timeout: float | None = None):
        if self._loop is None:
            raise RuntimeError("RdmModul nicht gestartet")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def start(self) -> None:
        if self._loop:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="pyrdmnet", daemon=True)
        self._thread.start()
        if self.config.db is not None:
            # SQLite-Verbindungen sind threadgebunden: Speicher wird im Aufrufer-Thread genutzt.
            self.store = RdmStore(self.config.db)
        try:
            self._run(self._start_async(), timeout=15)
        except Exception:
            self.stop()
            raise

    async def _start_async(self) -> None:
        k = self.config
        if k.transport == "ARTNET":
            from .artnet_rdm import ArtnetRdmController
            if not k.artnet_target_ip:
                raise ValueError("Art-Net-RDM braucht eine Ziel-IP (Node oder Broadcast)")
            self.controller = ArtnetRdmController(k.artnet_target_ip, uid=k.controller_uid, bind=k.artnet_bind,
                                                  target_port=k.artnet_target_port)
            await self.controller.connect()
            return
        port = k.broker_port
        if k.internal_broker:
            self.broker = RdmnetBroker(BrokerConfig(
                host=k.broker_host, port=k.broker_port, scope=k.scope, uid=k.broker_uid,
                heartbeat_interval_s=k.heartbeat_interval_s, heartbeat_timeout_s=k.heartbeat_timeout_s,
                event_file=k.event_file))
            await self.broker.start()
            port = self.broker.port
            if k.dnssd:
                from .dnssd import DnssdRegistration
                self._dnssd = DnssdRegistration(k.scope, self.broker.config.cid, k.broker_uid, k.broker_host, port)
        self.port = port
        await self._connect_controller()

    async def _connect_controller(self) -> None:
        k = self.config
        self.controller = RdmnetController(uid=k.controller_uid, scope=k.scope,
                                           heartbeat_interval_s=k.heartbeat_interval_s,
                                           heartbeat_timeout_s=k.heartbeat_timeout_s)
        await self.controller.connect(k.broker_host if k.broker_host != "0.0.0.0" else "127.0.0.1", self.port,
                                        timeout_s=k.timeout_s)
        await self.controller.client_list(timeout_s=k.timeout_s)

    async def _connected(self) -> RdmnetController:
        if self.config.transport == "ARTNET":
            if not self.controller.connected:
                await self.controller.connect()
            return self.controller
        if self.controller is None or not self.controller.connected:
            permission = self.controller.write_enabled if self.controller else False
            await self._connect_controller()
            self.controller.write_enabled = permission
        return self.controller

    def stop(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._stop_async(), timeout=10)
        except Exception as error:  # beim Herunterfahren nur protokollieren
            log.warning("Stoppen unvollständig: %s", error)
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        self._loop.close()
        self._loop = None
        if self.store:
            self.store.close()

    async def _stop_async(self) -> None:
        if self.controller:
            await self.controller.disconnect()
        if self._dnssd:
            self._dnssd.stop()
        if self.broker:
            await self.broker.stop()

    def broker_status(self) -> dict | None:
        return self.broker.status() if self.broker else None

    # ----------------------------------------------------------------- Geräte
    def find_devices(self) -> list[DeviceAddress]:
        k = self.config

        async def f():
            if k.transport == "ARTNET":
                return await (await self._connected()).query_tod(list(k.artnet_port_addresses), k.artnet_tod_wait_s)
            return await DeviceReader(await self._connected(), k.timeout_s).find_devices()
        return self._run(f(), timeout=None if k.transport != "ARTNET" else 4 * k.artnet_tod_wait_s + 10)

    def read_device(self, address: DeviceAddress, save: bool = True) -> DeviceProfile:
        async def f():
            return await DeviceReader(await self._connected(), self.config.timeout_s).profile(address)
        profile = self._run(f())
        if self.config.transport == "ARTNET" and profile.universe is None:
            pa = self.controller.port_address(address.rdm_uid)
            profile.universe = None if pa is None else pa + 1   # GelMatch-Zählung: Art-Net-Port-Adresse 0 = Universum 1
        if save and self.store:
            self._device_ids[address.rdm_uid] = self.store.save_profile(profile)
        return profile

    # ----------------------------------------------------------------- Einstellungen
    def _mapping(self, profile: DeviceProfile, gdtf_mode: str) -> A.PersonalityMapping | None:
        di = profile.device_info
        if not (self.store and di):
            return None
        return self.store.find_mapping(profile.address.rdm_uid.manufacturer, di.model_id,
                                              di.software_version_id, gdtf_mode)

    def confirm_mapping(self, profile: DeviceProfile, gdtf_mode: str, personality_num: int,
                              confirmed_by: str = "") -> A.PersonalityMapping:
        """Bestätigt ausdrücklich, welche Personality dem GDTF-Modus entspricht."""
        di = profile.device_info
        if di is None:
            raise ValueError("Profil ohne DEVICE_INFO – Zuordnung nicht möglich")
        if not any(p.number == personality_num for p in profile.personalities):
            raise ValueError(f"Personality {personality_num} gibt es an diesem Gerät nicht")
        if self.store is None:
            raise RuntimeError("Für Zuordnungen wird eine Datenbank benötigt (RdmKonfig.db)")
        z = A.PersonalityMapping(profile.address.rdm_uid.manufacturer, di.model_id, di.software_version_id,
                                   personality_num, gdtf_mode, now_utc(), confirmed_by)
        self.store.confirm_mapping(z)
        return z

    def check_setting(self, profile: DeviceProfile, expected: A.ExpectedSetting,
                            reference: str | None = None) -> A.CheckResult:
        e = A.check(profile, expected, self._mapping(profile, expected.gdtf_mode), self.config.strict)
        if self.store:
            self.store.save_check(self._device_ids.get(profile.address.rdm_uid), e,
                                             expected.gdtf_mode, reference)
        return e

    def set_write_enabled(self, active: bool) -> None:
        """Freigabe für RDM SET in dieser Sitzung (nach Checkliste vor Ort)."""
        async def f():
            (await self._connected()).write_enabled = active
        self._run(f())

    def apply_setting(self, profile: DeviceProfile, expected: A.ExpectedSetting,
                             reference: str | None = None
                             ) -> tuple[A.ApplyResult, DeviceProfile, A.CheckResult]:
        """Setzt Modus/Adresse, liest das Gerät neu aus und prüft erneut."""
        mapping = self._mapping(profile, expected.gdtf_mode)

        async def f():
            return await A.apply(await self._connected(), profile.address, profile, expected, mapping,
                                    self.config.timeout_s)
        application = self._run(f())
        if self.store:
            self.store.save_application(self._device_ids.get(profile.address.rdm_uid), application, reference)
        new = self.read_device(profile.address)
        return application, new, self.check_setting(new, expected, reference)

    # ----------------------------------------------------------------- Sensoren
    def sensor_snapshot(self, profile: DeviceProfile, measurement_ref: str | None = None,
                             phase: str = "single") -> SensorSnapshot:
        async def f():
            return await DeviceReader(await self._connected(), self.config.timeout_s).sensor_snapshot(
                profile.address, profile.sensors, self.config.all_sensors)
        try:
            s = self._run(f(), timeout=self.config.timeout_s * (len(profile.sensors) + 2))
        except Exception as error:  # RDM darf die Messung nicht stören
            s = SensorSnapshot(str(profile.address.rdm_uid), now_utc(), 0.0, error=[repr(error)])
        if self.store:
            self.store.save_snapshot(self._device_ids.get(profile.address.rdm_uid), s, measurement_ref,
                                                  phase)
        return s

    @contextlib.contextmanager
    def accompany_measurement(self, profile: DeviceProfile, measurement_ref: str):
        """Sensor-Schnappschuss vor und nach einer Messung; RDM-Fehler werden nur gespeichert."""
        before = self.sensor_snapshot(profile, measurement_ref, "before")
        result = {"before": before, "after": None}
        try:
            yield result
        finally:
            result["after"] = self.sensor_snapshot(profile, measurement_ref, "after")
