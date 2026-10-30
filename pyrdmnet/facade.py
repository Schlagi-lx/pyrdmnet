"""Synchronous integration interface for the measurement lab.

The measurement lab works synchronously; RDMnet runs internally in its own
asyncio thread. All methods block until the result is available.

Typical flow per campaign::

    module = RdmModule(RdmConfig(broker_host=adapter_ip, db=measurement_db_connection))
    module.start()
    addresses = module.find_devices()
    profile = module.read_device(addresses[0])
    expected = ExpectedSetting("Direct 6ch", footprint=6, start_address=100, universe=1)
    check = module.check_setting(profile, expected, reference="Campaign 7")
    if not check.measurement_allowed:
        module.set_write_enabled(True)          # after the on-site checklist
        app, profile, check = module.apply_setting(profile, expected, reference="Campaign 7")
    for measurement_id in ...:
        with module.accompany_measurement(profile, measurement_ref=str(measurement_id)):
            ...  # spectral measurement
    module.stop()

RDM errors during ``accompany_measurement`` are logged and stored,
but never passed on to the measurement.
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
from .client import RdmnetConnectionError
from .controller import RdmnetController, WritePolicy
from .rdm import Uid
from .storage import RdmStore

log = logging.getLogger("pyrdmnet.facade")


@dataclass
class RdmConfig:
    internal_broker: bool = True  # start our own broker in this process
    broker_host: str = "127.0.0.1"  # IP of the selected network adapter
    broker_port: int = 8888  # 0 = choose freely (only useful with the internal broker)
    broker_extra_hosts: tuple[str, ...] = ()  # internal broker: further addresses, e.g. ("::",) for IPv6
    scope: str = C.E133_DEFAULT_SCOPE
    broker_uid: Uid = field(default_factory=lambda: Uid(C.ESTA_PROTOTYPE_MANUFACTURER, 0x00000001))
    controller_uid: Uid | None = None  # None = request a dynamic UID
    timeout_s: float = 3.0
    all_sensors: bool = False  # False: only measurement-relevant sensor types
    strict: bool = False  # True: only settings check OK allows a measurement
    db: str | Path | sqlite3.Connection | None = None
    event_file: Path | None = None
    dnssd: bool = False  # register the broker via mDNS (requires zeroconf)
    heartbeat_interval_s: float = C.E133_HEARTBEAT_INTERVAL_S
    heartbeat_timeout_s: float = C.E133_HEARTBEAT_TIMEOUT_S
    reconnect_wait_s: float = 10.0  # how long a call waits for the automatic reconnection to the broker
    # which SETs are allowed; WritePolicy.lab() = only DMX mode, start address, identify (measurement lab)
    write_policy: WritePolicy = field(default_factory=WritePolicy)
    # V0.21.12 (S11): RDM over Art-Net instead of RDMnet (no broker); target = node IP or broadcast
    transport: str = "RDMNET"  # "RDMNET" | "ARTNET"
    artnet_target_ip: str | None = None
    artnet_port_addresses: tuple = tuple(range(32))  # Net 0, universes 1-32 in GelMatch numbering
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

    # ----------------------------------------------------------------- thread
    def _run(self, coro, timeout: float | None = None):
        if self._loop is None:
            raise RuntimeError("RdmModule not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def start(self) -> None:
        if self._loop:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="pyrdmnet", daemon=True)
        self._thread.start()
        if self.config.db is not None:
            # SQLite connections are bound to a thread: the store is used in the caller's thread.
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
                raise ValueError("Art-Net RDM needs a target IP (node or broadcast)")
            self.controller = ArtnetRdmController(k.artnet_target_ip, uid=k.controller_uid, bind=k.artnet_bind,
                                                  target_port=k.artnet_target_port, write_policy=k.write_policy)
            await self.controller.connect()
            return
        port = k.broker_port
        if k.internal_broker:
            self.broker = RdmnetBroker(BrokerConfig(
                host=k.broker_host, port=k.broker_port, extra_hosts=k.broker_extra_hosts, scope=k.scope,
                uid=k.broker_uid,
                heartbeat_interval_s=k.heartbeat_interval_s, heartbeat_timeout_s=k.heartbeat_timeout_s,
                event_file=k.event_file))
            await self.broker.start()
            port = self.broker.port
            if k.dnssd:  # E1.33 9.1.4: STANDBY instead of registering while another broker holds the scope
                from .dnssd import BrokerAnnouncer
                self._dnssd = BrokerAnnouncer(self.broker, k.broker_host, on_message=log.warning)
                await self._dnssd.start()
        self.port = port
        await self._connect_controller()

    async def _connect_controller(self) -> None:
        k = self.config
        self.controller = RdmnetController(uid=k.controller_uid, scope=k.scope, write_policy=k.write_policy,
                                           heartbeat_interval_s=k.heartbeat_interval_s,
                                           heartbeat_timeout_s=k.heartbeat_timeout_s)
        host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(k.broker_host, k.broker_host)  # all adapters: loopback
        await self.controller.connect(host, self.port,
                                      timeout_s=k.timeout_s, auto_reconnect=True)
        await self.controller.client_list(timeout_s=k.timeout_s)

    async def _connected(self) -> RdmnetController:
        if self.config.transport == "ARTNET":
            if not self.controller.connected:
                await self.controller.connect()
            return self.controller
        c = self.controller
        if c is not None and c.auto_reconnecting:
            # E1.33 6.2.5: the controller reconnects by itself - wait, never open a second connection
            if not c.connected:
                await c.wait_connected(self.config.reconnect_wait_s)
                await c.client_list(timeout_s=self.config.timeout_s)
            return c
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
        except Exception as error:  # only log during shutdown
            log.warning("Stop incomplete: %s", error)
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
            await self._dnssd.stop()
        if self.broker:
            await self.broker.stop()

    async def _read_with_retry(self, action):
        """Run a read-only action; if the connection drops meanwhile and the controller reconnects
        by itself, wait for it and try once more. Writing actions are never repeated automatically."""
        try:
            return await action(await self._connected())
        except RdmnetConnectionError:
            if self.config.transport == "ARTNET" or not (self.controller and self.controller.auto_reconnecting):
                raise
            return await action(await self._connected())

    def broker_status(self) -> dict | None:
        return self.broker.status() if self.broker else None

    # ----------------------------------------------------------------- devices
    def find_devices(self) -> list[DeviceAddress]:
        k = self.config

        async def f():
            if k.transport == "ARTNET":
                return await (await self._connected()).query_tod(list(k.artnet_port_addresses), k.artnet_tod_wait_s)
            return await self._read_with_retry(lambda c: DeviceReader(c, k.timeout_s).find_devices())
        return self._run(f(), timeout=None if k.transport != "ARTNET" else 4 * k.artnet_tod_wait_s + 10)

    def read_device(self, address: DeviceAddress, save: bool = True) -> DeviceProfile:
        async def f():
            return await self._read_with_retry(lambda c: DeviceReader(c, self.config.timeout_s).profile(address))
        profile = self._run(f())
        if self.config.transport == "ARTNET" and profile.universe is None:
            pa = self.controller.port_address(address.rdm_uid)
            # GelMatch numbering: Art-Net port address 0 = universe 1
            profile.universe = None if pa is None else pa + 1
        if save and self.store:
            self._device_ids[address.rdm_uid] = self.store.save_profile(profile)
        return profile

    # ----------------------------------------------------------------- settings
    def _mapping(self, profile: DeviceProfile, gdtf_mode: str) -> A.PersonalityMapping | None:
        di = profile.device_info
        if not (self.store and di):
            return None
        return self.store.find_mapping(profile.address.rdm_uid.manufacturer, di.model_id,
                                              di.software_version_id, gdtf_mode)

    def confirm_mapping(self, profile: DeviceProfile, gdtf_mode: str, personality_num: int,
                              confirmed_by: str = "") -> A.PersonalityMapping:
        """Explicitly confirm which personality corresponds to the GDTF mode."""
        di = profile.device_info
        if di is None:
            raise ValueError("Profile without DEVICE_INFO - mapping not possible")
        if not any(p.number == personality_num for p in profile.personalities):
            raise ValueError(f"Personality {personality_num} does not exist on this device")
        if self.store is None:
            raise RuntimeError("Mappings require a database (RdmConfig.db)")
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
        """Write permission for RDM SET in this session (after the on-site checklist)."""
        async def f():
            (await self._connected()).write_enabled = active
        self._run(f())

    def apply_setting(self, profile: DeviceProfile, expected: A.ExpectedSetting,
                             reference: str | None = None
                             ) -> tuple[A.ApplyResult, DeviceProfile, A.CheckResult]:
        """Set mode/address, read the device again and re-check."""
        mapping = self._mapping(profile, expected.gdtf_mode)

        async def f():
            return await A.apply(await self._connected(), profile.address, profile, expected, mapping,
                                    self.config.timeout_s)
        application = self._run(f())
        if self.store:
            self.store.save_application(self._device_ids.get(profile.address.rdm_uid), application, reference)
        new = self.read_device(profile.address)
        return application, new, self.check_setting(new, expected, reference)

    # ----------------------------------------------------------------- sensors
    def sensor_snapshot(self, profile: DeviceProfile, measurement_ref: str | None = None,
                             phase: str = "single") -> SensorSnapshot:
        async def f():
            return await self._read_with_retry(lambda c: DeviceReader(c, self.config.timeout_s).sensor_snapshot(
                profile.address, profile.sensors, self.config.all_sensors))
        try:
            s = self._run(f(), timeout=self.config.timeout_s * (len(profile.sensors) + 2))
        except Exception as error:  # RDM must not disturb the measurement
            s = SensorSnapshot(str(profile.address.rdm_uid), now_utc(), 0.0, error=[repr(error)])
        if self.store:
            self.store.save_snapshot(self._device_ids.get(profile.address.rdm_uid), s, measurement_ref,
                                                  phase)
        return s

    @contextlib.contextmanager
    def accompany_measurement(self, profile: DeviceProfile, measurement_ref: str):
        """Sensor snapshot before and after a measurement; RDM errors are only stored."""
        before = self.sensor_snapshot(profile, measurement_ref, "before")
        result = {"before": before, "after": None}
        try:
            yield result
        finally:
            result["after"] = self.sensor_snapshot(profile, measurement_ref, "after")
