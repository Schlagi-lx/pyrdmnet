"""Write policy: which RDM SET commands controllers, the Art-Net controller and the LLRP manager may send."""

import tempfile
import unittest
from pathlib import Path

from pyrdmnet import constants as C
from pyrdmnet.artnet_rdm import ArtnetRdmController
from pyrdmnet.controller import LAB_SET_PIDS, LLRP_LAB_SET_PIDS, RdmnetController, WriteLockError, WritePolicy
from pyrdmnet.facade import RdmConfig, RdmModule
from pyrdmnet.llrp import LlrpManager


class TestWritePolicy(unittest.TestCase):
    def test_default_any_pid_after_permission(self):
        p = WritePolicy()
        p.check(C.CC_GET, C.PID_DEVICE_INFO, write_enabled=False)  # reading is never restricted
        with self.assertRaises(WriteLockError):
            p.check(C.CC_SET, C.PID_DEVICE_LABEL, write_enabled=False)
        p.check(C.CC_SET, C.PID_DEVICE_LABEL, write_enabled=True)
        p.check(C.CC_SET, 0x8123, write_enabled=True)  # manufacturer-specific PID
        self.assertTrue(p.allows(0x1001))  # RESET_DEVICE: allowed by the default policy

    def test_lab_profiles(self):
        self.assertEqual(WritePolicy.lab().allowed_set_pids, LAB_SET_PIDS)
        self.assertEqual(LAB_SET_PIDS, {C.PID_DMX_PERSONALITY, C.PID_DMX_START_ADDRESS, C.PID_IDENTIFY_DEVICE})
        self.assertEqual(LLRP_LAB_SET_PIDS, {C.PID_COMPONENT_SCOPE, C.PID_SEARCH_DOMAIN, C.PID_IDENTIFY_DEVICE})
        with self.assertRaises(WriteLockError) as ctx:
            WritePolicy.lab().check(C.CC_SET, C.PID_DEVICE_LABEL, write_enabled=True)
        self.assertIn("DEVICE_LABEL", str(ctx.exception))
        WritePolicy.lab().check(C.CC_SET, C.PID_DMX_START_ADDRESS, write_enabled=True)

    def test_own_list_and_no_session_permission(self):
        p = WritePolicy([C.PID_DEVICE_LABEL], require_write_enabled=False)
        self.assertEqual(p.allowed_set_pids, frozenset({C.PID_DEVICE_LABEL}))  # any iterable, stored frozen
        p.check(C.CC_SET, C.PID_DEVICE_LABEL, write_enabled=False)
        with self.assertRaises(WriteLockError):
            p.check(C.CC_SET, C.PID_DMX_START_ADDRESS, write_enabled=False)
        with self.assertRaises(AttributeError):
            p.require_write_enabled = True  # frozen: a policy is changed by replacing it

    def test_discovery_never(self):
        for p in (WritePolicy(), WritePolicy(require_write_enabled=False), WritePolicy({0x0001})):
            with self.assertRaises(WriteLockError):
                p.check(C.CC_DISCOVERY, 0x0001, write_enabled=True)

    def test_defaults_and_wiring(self):
        self.assertEqual(RdmnetController().write_policy, WritePolicy())
        self.assertEqual(ArtnetRdmController("127.0.0.1").write_policy, WritePolicy())
        self.assertEqual(LlrpManager().write_policy, WritePolicy())
        self.assertEqual(RdmnetController(write_policy=WritePolicy.lab()).write_policy, WritePolicy.lab())
        self.assertEqual(LlrpManager(write_policy=WritePolicy.llrp_lab()).write_policy, WritePolicy.llrp_lab())


class TestFacadePolicy(unittest.TestCase):
    def test_config_reaches_controller(self):
        with tempfile.TemporaryDirectory() as tmp:
            module = RdmModule(RdmConfig(broker_port=0, db=Path(tmp) / "m.sqlite", timeout_s=1.0,
                                         write_policy=WritePolicy.lab()))
            module.start()
            try:
                self.assertEqual(module.controller.write_policy, WritePolicy.lab())
            finally:
                module.stop()


if __name__ == "__main__":
    unittest.main()
