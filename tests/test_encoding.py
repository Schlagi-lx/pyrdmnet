"""Encoding: RDM (E1.20), parameter data, RDMnet PDUs (E1.33)."""

import struct
import unittest
import uuid

from pyrdmnet import constants as C
from pyrdmnet import pdu as P
from pyrdmnet import rdm_params as R
from pyrdmnet.rdm import (ALL_CONTROLLERS, ALL_DEVICES, RdmFormatError, RdmMessage, TransactionCounter, Uid,
                           manufacturer_from_mid_broadcast, is_device_broadcast)


class TestUid(unittest.TestCase):
    def test_conversions(self):
        u = Uid(0x7FF0, 0x12345678)
        self.assertEqual(u.to_bytes(), bytes.fromhex("7ff012345678"))
        self.assertEqual(Uid.from_bytes(u.to_bytes()), u)
        self.assertEqual(Uid.from_text("7ff0:12345678"), u)
        self.assertEqual(Uid.from_text("7FF012345678"), u)
        self.assertEqual(Uid.from_int(u.to_int()), u)
        self.assertEqual(str(u), "7ff0:12345678")

    def test_properties(self):
        self.assertTrue(Uid(0xFFF0, 0).is_dynamic_request)
        self.assertFalse(Uid(0xFFF0, 5).is_dynamic_request)
        self.assertTrue(Uid(0x1234, 0xFFFFFFFF).is_broadcast)
        self.assertFalse(Uid(0, 0).is_valid_for_client)
        self.assertTrue(is_device_broadcast(ALL_DEVICES))
        self.assertTrue(is_device_broadcast(Uid(0xFFFD, 0x6574FFFF)))
        self.assertEqual(manufacturer_from_mid_broadcast(Uid(0xFFFD, 0x6574FFFF)), 0x6574)
        self.assertIsNone(manufacturer_from_mid_broadcast(ALL_DEVICES))
        self.assertFalse(is_device_broadcast(ALL_CONTROLLERS))

    def test_value_range(self):
        with self.assertRaises(ValueError):
            Uid(0x10000, 0)
        with self.assertRaises(ValueError):
            Uid.from_text("123")


class TestRdmMessage(unittest.TestCase):
    def _example(self):
        return RdmMessage(Uid(0x7FF0, 0x201), Uid(0xFFF0, 1), 5, 1, C.CC_GET, C.PID_SENSOR_VALUE, b"\x02")

    def test_bytes_built_independently(self):
        raw = self._example().encode()
        expected = bytearray([0xCC, 0x01, 25]) + bytes.fromhex("7ff000000201") + bytes.fromhex("fff000000001")
        expected += bytes([5, 1, 0]) + b"\x00\x00" + bytes([0x20]) + b"\x02\x01" + bytes([1, 2])
        expected += struct.pack(">H", sum(expected) & 0xFFFF)
        self.assertEqual(raw, bytes(expected))

    def test_round_trip(self):
        n = self._example()
        m = RdmMessage.decode(n.encode())
        self.assertEqual(m, n)

    def test_response(self):
        a = self._example().response(C.RESPONSE_ACK, b"\x01\x02")
        self.assertEqual((a.dest, a.source, a.command_class, a.transaction), (Uid(0xFFF0, 1), Uid(0x7FF0, 0x201),
                                                                             C.CC_GET_RESPONSE, 5))
        self.assertTrue(a.is_response)

    def test_malformed_messages(self):
        raw = bytearray(self._example().encode())
        for broken in (raw[:10], bytes([0xCD]) + raw[1:], raw[:-1] + bytes([raw[-1] ^ 1])):
            with self.assertRaises(RdmFormatError):
                RdmMessage.decode(bytes(broken))
        bad_pdl = bytearray(raw)
        bad_pdl[23] = 5
        with self.assertRaises(RdmFormatError):
            RdmMessage.decode(bytes(bad_pdl))
        with self.assertRaises(RdmFormatError):
            RdmMessage(Uid(1, 1), Uid(1, 2), 0, 0, C.CC_GET, 1, b"x" * 232).encode()

    def test_transaction_counter(self):
        tz, u = TransactionCounter(), Uid(1, 1)
        self.assertEqual([tz.next_tn(u) for _ in range(3)], [0, 1, 2])
        for _ in range(253):
            tz.next_tn(u)
        self.assertEqual(tz.next_tn(u), 0)  # wraps around after 255


class TestParameter(unittest.TestCase):
    def test_device_info(self):
        di = R.DeviceInfo(0x0100, 0x0101, 0x0101, 0x00010000, 6, 2, 3, 100, 0, 4)
        raw = R.build_device_info(di)
        self.assertEqual(len(raw), 19)
        self.assertEqual(R.read_device_info(raw), di)
        self.assertEqual(di.product_category_text, "Fixture")
        with self.assertRaises(RdmFormatError):
            R.read_device_info(raw[:-1])

    def test_sensor_scaling_and_relevance(self):
        sd = R.SensorDefinition(2, 0x01, 0x04, 0x01, 900, 2650, 2000, 2500, 1, "Mains")
        self.assertAlmostEqual(sd.scaled(2304), 230.4)
        self.assertTrue(sd.measurement_relevant)
        self.assertEqual(sd.unit_text, "V AC (RMS)")
        self.assertEqual(R.read_sensor_definition(R.build_sensor_definition(sd)), sd)
        store = R.SensorDefinition(3, 0x1D, 0x1C, 0x13, 0, 1, 0, 1, 0, "Memory")
        self.assertFalse(store.measurement_relevant)
        self.assertEqual(store.factor, 1000.0)
        w = R.SensorRawValue(2, -5, -10, 300, 7)
        self.assertEqual(R.read_sensor_value(R.build_sensor_value(w)), w)

    def test_personality_and_texts(self):
        p = R.Personality(2, 6, "Direct 6ch")
        self.assertEqual(R.read_personality_description(R.build_personality_description(p)), p)
        self.assertEqual(R.read_text_field(b"ABC\x00\x00"), "ABC")
        with self.assertRaises(RdmFormatError):
            R.read_text_field(b"x" * 33)

    def test_endpoints(self):
        raw = R.build_endpoint_list(7, [R.Endpoint(1, 1), R.Endpoint(2, 0)])
        self.assertEqual(R.read_endpoint_list(raw), (7, [R.Endpoint(1, 1), R.Endpoint(2, 0)]))
        u = [Uid(1, 2), Uid(3, 4)]
        self.assertEqual(R.read_endpoint_responders(R.build_endpoint_responders(1, 9, u)), (1, 9, u))
        with self.assertRaises(RdmFormatError):
            R.read_endpoint_list(b"\x00\x00\x00\x01\x00")


class TestPdu(unittest.TestCase):
    cid = uuid.UUID("12345678-1234-5678-1234-567812345678")

    def test_flags_length(self):
        self.assertEqual(P.flags_length(0x12345), bytes([0xF1, 0x23, 0x45]))
        self.assertEqual(P.read_flags_length(bytes([0xF0, 0x00, 0x05, 0, 0]), 0), (5, 3))
        with self.assertRaises(P.ProtocolError):
            P.read_flags_length(bytes([0x80, 0x00, 0x05, 0, 0]), 0)  # inheritance
        with self.assertRaises(P.ProtocolError):
            P.read_flags_length(bytes([0xF0, 0x00, 0x09]), 0)  # too long

    def test_packet_layout(self):
        packet = P.build_packet(C.VECTOR_ROOT_E133_BROKER, self.cid, P.build_broker(C.VECTOR_BROKER_NULL))
        self.assertEqual(packet[:12], b"ASC-E1.17\x00\x00\x00")
        (block,) = struct.unpack(">I", packet[12:16])
        self.assertEqual(block, len(packet) - 16)
        self.assertEqual(block, 23 + 5)  # root header + broker header (reference: BROKER_NULL_MSG_SIZE)
        (root,) = P.read_root_pdus(packet[16:])
        self.assertEqual((root.vector, root.cid), (C.VECTOR_ROOT_E133_BROKER, self.cid))
        self.assertEqual(P.read_broker(root.data).vector, C.VECTOR_BROKER_NULL)

    def test_connect_sizes_match_reference(self):
        e = P.ClientEntry(self.cid, Uid(0xFFF0, 0), C.RPT_CLIENT_TYPE_CONTROLLER)
        self.assertEqual(len(e.encode()), 23 + 23)  # CLIENT_ENTRY_HEADER_SIZE + RPT_CLIENT_ENTRY_DATA_SIZE
        cd = P.ConnectData("default", 1, "local.", 1, e)
        raw = cd.encode()
        self.assertEqual(len(raw), 3 + 63 + 231 + 46)  # CLIENT_CONNECT_COMMON_FIELD_SIZE + entry
        z = P.read_connect(raw)
        self.assertEqual((z.scope, z.version, z.domain, z.flags, z.client.uid, z.client.type),
                         ("default", 1, "local.", 1, Uid(0xFFF0, 0), 1))
        rep = P.ConnectReply(0, 1, Uid(1, 2), Uid(3, 4))
        self.assertEqual(len(rep.encode()), 16)  # BROKER_CONNECT_REPLY_DATA_SIZE
        self.assertEqual(P.read_connect_reply(rep.encode()), rep)

    def test_rpt_request_notification_status(self):
        rdm = RdmMessage(Uid(1, 2), Uid(3, 4), 0, 1, C.CC_GET, C.PID_DEVICE_INFO).encode()
        header = P.RptHeader(C.VECTOR_RPT_REQUEST, Uid(3, 4), 0, Uid(1, 2), 1, 0xDEADBEEF)
        raw = P.build_rpt(header, P.build_request(rdm))
        self.assertEqual(len(raw), 28 + 7 + 3 + len(rdm))  # RPT_PDU_HEADER, REQUEST_NOTIF_HEADER, RDM_CMD
        r = P.read_rpt(raw)
        self.assertEqual(r.header, header)
        self.assertEqual(P.read_request_or_notification(r.data, C.VECTOR_REQUEST_RDM_CMD), [rdm])
        n = P.build_notification([rdm, rdm])
        self.assertEqual(P.read_request_or_notification(n, C.VECTOR_NOTIFICATION_RDM_CMD), [rdm, rdm])
        self.assertEqual(P.read_status(P.build_status(4, "test")), (4, "test"))
        with self.assertRaises(P.ProtocolError):
            P.build_rdm_cmd_pdu(b"\x01\x02")

    def test_dynamic_uids(self):
        rid = uuid.uuid4()
        request = Uid(0xFFF0, 0).to_bytes() + rid.bytes
        (a,) = P.read_dyn_uid_requests(request)
        self.assertEqual((a.uid, a.rid), (Uid(0xFFF0, 0), rid))
        z = [(Uid(0xFFF0, 1), rid, 0)]
        self.assertEqual(P.read_dyn_uid_assignments(P.build_dyn_uid_assignments(z)), z)
        with self.assertRaises(P.ProtocolError):
            P.read_dyn_uid_requests(b"\x00" * 21)


if __name__ == "__main__":
    unittest.main()
