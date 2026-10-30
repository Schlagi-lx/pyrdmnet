"""Controller (GET/SET rules), device readout, sensors, settings check and apply."""

import asyncio
import struct
import unittest

from pyrdmnet import settings_check as A
from pyrdmnet import constants as C
from pyrdmnet import rdm_params as R
from pyrdmnet.readout import DeviceAddress
from pyrdmnet.controller import (RdmResponseError, RdmNackError, RdmTimeoutError, WriteLockError)
from pyrdmnet.client import RdmnetConnectionError
from pyrdmnet.rdm import Uid

from helpers import NetworkTest

EXPECTED = A.ExpectedSetting("Direct 6ch", footprint=6, start_address=100, universe=1)


class TestController(NetworkTest):
    async def _get(self, pid, data=b""):
        a = self.address
        return await self.c.rdm_get(a.rpt_uid, pid, data, endpoint=a.endpoint, rdm_dest=a.rdm_uid, timeout_s=2)

    async def test_get_device_info_via_gateway(self):
        di = R.read_device_info(await self._get(C.PID_DEVICE_INFO))
        self.assertEqual((di.footprint, di.start_address, di.personality_current), (6, 100, 2))

    async def test_ack_timer(self):
        self.fx.ack_timer_once.add(C.PID_DMX_START_ADDRESS)
        self.assertEqual(await self._get(C.PID_DMX_START_ADDRESS), struct.pack(">H", 100))

    async def test_ack_overflow(self):
        self.fx.overflow_pids.add(C.PID_SUPPORTED_PARAMETERS)
        pids = R.read_pid_list(await self._get(C.PID_SUPPORTED_PARAMETERS))
        self.assertEqual(pids, self.fx.pids())

    async def test_nack(self):
        with self.assertRaises(RdmNackError) as ctx:
            await self._get(C.PID_LAMP_HOURS)
        self.assertEqual(ctx.exception.reason, C.NR_UNKNOWN_PID)

    async def test_timeout(self):
        self.gateway.mute = True
        with self.assertRaises(RdmTimeoutError):
            await self.c.rdm_get(self.address.rpt_uid, C.PID_DEVICE_INFO, endpoint=1, rdm_dest=self.fx.uid,
                                 timeout_s=0.3)
        self.assertEqual(self.c._pending, {})

    async def test_set_rules(self):
        a = self.address
        new = struct.pack(">H", 200)
        with self.assertRaises(WriteLockError):  # no write permission
            await self.c.rdm_set(a.rpt_uid, C.PID_DMX_START_ADDRESS, new, endpoint=1, rdm_dest=a.rdm_uid)
        self.c.write_enabled = True
        with self.assertRaises(WriteLockError):  # PID not permitted
            await self.c.rdm_set(a.rpt_uid, C.PID_DEVICE_LABEL, b"x", endpoint=1, rdm_dest=a.rdm_uid)
        self.assertEqual(self.fx.set_log, [])
        await self.c.rdm_set(a.rpt_uid, C.PID_DMX_START_ADDRESS, new, endpoint=1, rdm_dest=a.rdm_uid)
        self.assertEqual(self.fx.start_address, 200)
        with self.assertRaises(RdmNackError) as ctx:
            await self.c.rdm_set(a.rpt_uid, C.PID_DMX_START_ADDRESS, struct.pack(">H", 510), endpoint=1,
                                 rdm_dest=a.rdm_uid)  # 510 + 6 - 1 > 512
        self.assertEqual(ctx.exception.reason, C.NR_DATA_OUT_OF_RANGE)

    async def test_never_discovery(self):
        from pyrdmnet.rdm import RdmMessage
        n = RdmMessage(self.fx.uid, self.c.uid, 0, 1, C.CC_DISCOVERY, 0x0001)
        self.c.write_enabled = True
        with self.assertRaises(WriteLockError):
            await self.c._send_and_wait(self.gateway.uid, 1, n, 1)

    async def test_wrong_response_detected(self):
        from pyrdmnet.rdm import RdmMessage
        request = RdmMessage(self.fx.uid, self.c.uid, 3, 1, C.CC_GET, C.PID_DEVICE_INFO)
        foreign = RdmMessage(self.c.uid, self.fx.uid, 4, 0, C.CC_GET_RESPONSE, C.PID_DEVICE_INFO)
        with self.assertRaises(RdmResponseError):
            self.c._select_response(request, [request.encode(), foreign.encode()])

    async def test_pending_requests_on_disconnect(self):
        self.gateway.mute = True
        pending = asyncio.create_task(self._get(C.PID_DEVICE_INFO))
        await asyncio.sleep(0.1)
        await self.broker.stop()
        with self.assertRaises(RdmnetConnectionError):
            await pending


class TestReadGateway(NetworkTest):
    async def test_find(self):
        addresses = await self.reader.find_devices()
        self.assertEqual(addresses, [DeviceAddress(self.gateway.uid, 1, self.fx.uid)])

    async def test_profile(self):
        p = await self.reader.profile(self.address)
        self.assertEqual(p.manufacturer, "pyrdmnet Simulator")
        self.assertEqual([x.number for x in p.personalities], [1, 2, 3])
        self.assertEqual(p.personality_current.description, "Direct 6ch")
        self.assertEqual(len(p.sensors), 4)
        self.assertEqual(p.universe, 1)
        self.assertEqual(p.device_hours, 1234)
        self.assertIsNone(p.lamp_hours)  # not in SUPPORTED_PARAMETERS -> not queried at all
        self.assertNotIn("LAMP_HOURS", p.unavailable)
        d = p.to_dict()
        self.assertEqual(d["device_info"]["product_category_text"], "Scheinwerfer")

    async def test_profile_with_lamp_hours(self):
        self.fx.lamp_hours = 77
        p = await self.reader.profile(self.address)
        self.assertEqual(p.lamp_hours, 77)

    async def test_sensors_measurement_relevant_only(self):
        p = await self.reader.profile(self.address)
        s = await self.reader.sensor_snapshot(self.address, p.sensors)
        self.assertEqual(s.status, "ok")
        self.assertEqual([w.description for w in s.values], ["LED temperature", "Housing", "Mains voltage"])
        network = s.values[2]
        self.assertAlmostEqual(network.value, 230.4)
        self.assertEqual(network.raw, 2304)
        self.assertIsNone(network.lowest)  # device reports no min/max support
        self.assertFalse(network.outside_normal)
        all = await self.reader.sensor_snapshot(self.address, p.sensors, all_sensors=True)
        self.assertEqual(len(all.values), 4)
        self.assertEqual(all.values[3].value, 512000.0)  # prefix kilo

    async def test_sensor_temperature_rises(self):
        p = await self.reader.profile(self.address)
        self.fx.start -= 10  # simulate 10 s of operation: 0.5 K/s
        s = await self.reader.sensor_snapshot(self.address, p.sensors)
        self.assertGreaterEqual(s.values[0].value, 30.0)

    async def test_sensor_outside_normal(self):
        self.fx.sensors[1].definition.normal_max = 40  # before readout: the profile knows the normal range
        p = await self.reader.profile(self.address)
        self.fx.start -= 1000  # LED end value 68 °C (normal up to 85), housing end value 41 °C (normal up to 40)
        s = await self.reader.sensor_snapshot(self.address, p.sensors)
        self.assertTrue(s.values[1].outside_normal)
        self.assertFalse(s.values[0].outside_normal)

    async def test_snapshot_on_failure(self):
        p = await self.reader.profile(self.address)
        self.gateway.mute = True
        self.reader.timeout_s = 0.2
        s = await self.reader.sensor_snapshot(self.address, p.sensors)
        self.assertEqual(s.status, "failed")
        self.assertEqual(len(s.error), 3)

    async def test_profile_with_silent_device(self):
        self.gateway.mute = True
        self.reader.timeout_s = 0.2
        p = await self.reader.profile(self.address)
        self.assertIsNone(p.device_info)
        self.assertIn("DEVICE_INFO", p.unavailable)
        self.assertEqual(A.check(p, EXPECTED).status, A.NOT_CHECKABLE)


class TestReadNative(NetworkTest):
    native = True

    async def test_find_and_profile(self):
        addresses = await self.reader.find_devices()
        self.assertEqual(addresses, [DeviceAddress(self.gateway.uid, 0, self.gateway.uid)])
        p = await self.reader.profile(addresses[0])
        self.assertEqual(p.model, "SIM-PAR RGBL")
        self.assertIsNone(p.universe)


class TestSettingsCheck(NetworkTest):
    async def _profile(self):
        return await self.reader.profile(self.address)

    def _mapping(self, p, num=2, mode="Direct 6ch", fw=None):
        di = p.device_info
        return A.PersonalityMapping(p.address.rdm_uid.manufacturer, di.model_id,
                                      di.software_version_id if fw is None else fw, num, mode, "t")

    async def test_ok_unconfirmed_and_ok(self):
        p = await self._profile()
        e = A.check(p, EXPECTED)
        self.assertEqual(e.status, A.OK_UNCONFIRMED)
        self.assertTrue(e.measurement_allowed)
        self.assertFalse(A.check(p, EXPECTED, strict=True).measurement_allowed)
        self.assertIn('2 "Direct 6ch"', e.hints[0])
        e = A.check(p, EXPECTED, self._mapping(p))
        self.assertEqual((e.status, e.measurement_allowed, e.target_personality), (A.OK, True, 2))

    async def test_deviations(self):
        self.fx.start_address = 1
        self.gateway.universe = 3
        p = await self._profile()
        e = A.check(p, EXPECTED, self._mapping(p, num=3))  # confirmed: 3, device has 2
        self.assertEqual(e.status, A.DEVIATION)
        self.assertFalse(e.measurement_allowed)
        self.assertEqual(len(e.deviations), 3)  # address, universe, personality

    async def test_mapping_other_firmware_not_valid(self):
        p = await self._profile()
        e = A.check(p, EXPECTED, self._mapping(p, fw=0x00020000))
        self.assertEqual(e.status, A.OK_UNCONFIRMED)
        self.assertTrue(any("andere Firmware" in h for h in e.hints))

    async def test_candidates(self):
        p = await self._profile()
        k = A.candidates(p, A.ExpectedSetting("HSIC", 5, 1))
        self.assertEqual([x[0].number for x in k], [3])
        self.assertEqual(A.candidates(p, A.ExpectedSetting("x", 99, 1)), [])

    async def test_apply_success(self):
        self.fx.personality, self.fx.start_address = 1, 7
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, EXPECTED, self._mapping(p))
        self.assertTrue(res.success, res.error)
        self.assertEqual([x["parameter"] for x in res.changes], ["DMX_PERSONALITY", "DMX_START_ADDRESS"])
        self.assertEqual((self.fx.personality, self.fx.start_address), (2, 100))
        self.assertEqual(A.check(await self._profile(), EXPECTED, self._mapping(p)).status, A.OK)

    async def test_apply_address_only_without_mapping(self):
        self.fx.start_address = 7  # footprint matches -> address may be set without mapping
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, EXPECTED, None)
        self.assertTrue(res.success, res.error)
        self.assertEqual(self.fx.start_address, 100)
        self.assertEqual(self.fx.personality, 2)

    async def test_apply_refuses_mode_change_without_mapping(self):
        self.fx.personality = 1
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, EXPECTED, None)
        self.assertFalse(res.success)
        self.assertEqual(self.fx.set_log, [])

    async def test_apply_without_permission(self):
        self.fx.start_address = 7
        p = await self._profile()
        res = await A.apply(self.c, self.address, p, EXPECTED, self._mapping(p))
        self.assertFalse(res.success)
        self.assertIn("Schreibfreigabe", res.error[0])
        self.assertEqual(self.fx.start_address, 7)

    async def test_apply_write_protect(self):
        self.fx.start_address, self.fx.write_protect = 7, True
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, EXPECTED, self._mapping(p))
        self.assertFalse(res.success)
        self.assertIn("WRITE_PROTECT", res.error[0])

    async def test_apply_address_out_of_range(self):
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, A.ExpectedSetting("Direct 6ch", 6, 510),
                               self._mapping(p))
        self.assertFalse(res.success)
        self.assertEqual(self.fx.set_log, [])

    async def test_apply_mapping_with_wrong_footprint(self):
        self.fx.personality = 1
        p = await self._profile()
        self.c.write_enabled = True
        res = await A.apply(self.c, self.address, p, EXPECTED, self._mapping(p, num=3))  # no. 3 has 5 channels
        self.assertFalse(res.success)
        self.assertEqual(self.fx.set_log, [])

    async def test_readback_detects_deviation(self):
        self.fx.start_address = 7
        p = await self._profile()
        self.c.write_enabled = True
        old = self.fx._set

        def ignores(a):  # device acknowledges but applies nothing
            return a.response(C.RESPONSE_ACK)
        self.fx._set = ignores
        res = await A.apply(self.c, self.address, p, EXPECTED, self._mapping(p))
        self.fx._set = old
        self.assertFalse(res.success)
        self.assertIn("Read back: start address 7", res.error[0])


if __name__ == "__main__":
    unittest.main()
