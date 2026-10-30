"""RDM über Art-Net (Art-Net 4: ArtTodRequest / ArtTodData / ArtTodControl / ArtRdm) – V0.21.12 (S11).

Gleiche Schnittstelle wie `RdmnetController` für alles, was `Auslesung` und der Abgleich brauchen (`rdm_get`, `rdm_set`,
`uid`, `schreibfreigabe`, `verbunden`); die Logik für ACK_TIMER/ACK_OVERFLOW und die Schreibsperre ist dieselbe Funktion
wie dort (wiederverwendet, nicht kopiert). Neu ist nur der Transport:

- Geräte finden: ArtTodRequest an die Ziel-IP (Node oder Broadcast) für eine Liste von Port-Adressen; die Nodes antworten
  mit ArtTodData (UIDs je Port-Adresse, ggf. in mehreren Blöcken). Leere Antwort → einmal ArtTodControl „AtcFlush“
  (Node macht eine volle RDM-Discovery) und erneut fragen.
- RDM-Befehle: ArtRdm an die IP des Nodes, der die UID gemeldet hat; die Antwort kommt als ArtRdm zurück.
- Discovery-Befehle (DISC_*) sendet das Messlabor nie selbst – die Discovery macht der Node.

Socket: UDP, gebunden an 0.0.0.0:6454 (Art-Net-Antworten gehen an Port 6454, ArtTodData teils als Broadcast), mit
SO_REUSEADDR/SO_REUSEPORT, damit z. B. ein Pult auf demselben Rechner nicht blockiert.
"""
from __future__ import annotations

import asyncio
import random
import socket
import struct

from . import constants as C
from .readout import DeviceAddress
from .controller import RdmnetController, RdmTimeoutError
from .rdm import RdmMessage, TransactionCounter, Uid

ARTNET_ID = b'Art-Net\x00'
ARTNET_PORT = 6454
PROT_VER = 14
OP_TOD_REQUEST, OP_TOD_DATA, OP_TOD_CONTROL, OP_RDM = 0x8000, 0x8100, 0x8200, 0x8300
TOD_FULL = 0x00
ATC_FLUSH = 0x01
AR_PROCESS = 0x00


class ArtnetFormatError(ValueError):
    pass


def _header(op):
    return ARTNET_ID + struct.pack('<H', op) + struct.pack('>H', PROT_VER)


def tod_request(port_addresses):
    """ArtTodRequest; alle Adressen muessen im selben Net liegen (je Net ein Paket), hoechstens 32 je Paket."""
    pa = sorted(set(int(x) for x in port_addresses))
    if not pa or len(pa) > 32 or len({x >> 8 for x in pa}) != 1 or any(not 0 <= x <= 32767 for x in pa):
        raise ArtnetFormatError('PORT_ADRESSEN_UNGUELTIG')
    net = pa[0] >> 8
    return _header(OP_TOD_REQUEST) + bytes(2) + bytes(7) + bytes((net, TOD_FULL, len(pa))) + \
        bytes(x & 0xFF for x in pa).ljust(32, b'\x00')


def tod_control(port_address, command=ATC_FLUSH):
    return _header(OP_TOD_CONTROL) + bytes(2) + bytes(7) + bytes((port_address >> 8, command, port_address & 0xFF))


def art_rdm(port_address, rdm_raw):
    """ArtRdm mit der RDM-Nachricht OHNE Startcode 0xCC (Art-Net-Konvention)."""
    if not rdm_raw or rdm_raw[0] != C.RDM_STARTCODE:
        raise ArtnetFormatError('RDM_OHNE_STARTCODE')
    return _header(OP_RDM) + bytes((1, 0)) + bytes(7) + bytes((port_address >> 8, AR_PROCESS, port_address & 0xFF)) + rdm_raw[1:]


def parse_packet(packet):
    """(opcode, felder) fuer ArtTodData und ArtRdm, sonst (opcode, None). Falsche Kennung → None."""
    if len(packet) < 12 or packet[:8] != ARTNET_ID:
        return None
    op = struct.unpack('<H', packet[8:10])[0]
    if op == OP_TOD_DATA:
        if len(packet) < 28:
            raise ArtnetFormatError('TOD_DATA_ZU_KURZ')
        net, cmd, dmx_addr = packet[21], packet[22], packet[23]
        count = packet[27]
        raw = packet[28:28 + 6 * count]
        if len(raw) != 6 * count:
            raise ArtnetFormatError('TOD_DATA_UIDS_UNVOLLSTAENDIG')
        uids = [Uid.from_bytes(raw[i:i + 6]) for i in range(0, len(raw), 6)]
        return op, {'port_address': (net << 8) | dmx_addr, 'total': (packet[24] << 8) | packet[25], 'block': packet[26],
                    'uids': uids, 'response': cmd}
    if op == OP_RDM:
        if len(packet) < 24 + C.RDM_HEADER_LENGTH + 1:
            raise ArtnetFormatError('ART_RDM_ZU_KURZ')
        return op, {'port_address': (packet[21] << 8) | packet[23], 'rdm': bytes((C.RDM_STARTCODE,)) + packet[24:]}
    return op, None


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, dest):
        self.dest = dest

    def datagram_received(self, data, addr):
        try:
            self.dest._on_datagram(data, addr)
        except Exception:      # fremde/kaputte Pakete im Netz stoeren nie
            pass


class ArtnetRdmController:
    """RDM-Controller ueber Art-Net. `ziel_ip`: Node oder Broadcast (fuer ArtTodRequest); RDM geht an den meldenden Node."""

    # gleiche Befehlslogik wie RDMnet (ACK_TIMER, ACK_OVERFLOW, NACK, Schreibsperre)
    rdm_get = RdmnetController.rdm_get
    rdm_set = RdmnetController.rdm_set
    _rdm_command = RdmnetController._rdm_command
    _check_write_permission = RdmnetController._check_write_permission
    _select_response = staticmethod(RdmnetController._select_response)

    def __init__(self, target_ip, *, uid=None, bind=('0.0.0.0', ARTNET_PORT), target_port=ARTNET_PORT):
        self.target_ip, self.target_port, self.bind = target_ip, target_port, bind
        self.uid = uid or Uid(C.ESTA_PROTOTYPE_MANUFACTURER, random.randrange(1, 0xFFFFFFFE))
        self._tn = TransactionCounter()
        self.ack_timer_max_s = 10.0
        self.ack_overflow_max = 32
        self.write_enabled = False
        self.transport = None
        self.tod = {}                 # Uid -> (node_ip, port_adresse)
        self._tod_new = None
        self._pending = {}              # (uid, tn) -> Future
        self.foreign_packets = 0

    @property
    def connected(self):
        return self.transport is not None

    async def connect(self, timeout_s=3.0):
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(self.bind)
        self.transport, _ = await loop.create_datagram_endpoint(lambda: _Protocol(self), sock=sock)

    async def disconnect(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    @property
    def port(self):
        return self.transport.get_extra_info('sockname')[1] if self.transport else None

    # ---- Empfang
    def _on_datagram(self, data, addr):
        z = parse_packet(data)
        if z is None:
            self.foreign_packets += 1
            return
        op, f = z
        if op == OP_TOD_DATA and f is not None:
            for u in f['uids']:
                self.tod[u] = (addr[0], f['port_address'])
            if self._tod_new is not None:
                self._tod_new.set()
        elif op == OP_RDM and f is not None:
            try:
                n = RdmMessage.decode(f['rdm'])
            except Exception:
                return
            if not n.is_response:
                return                    # eigene Anfrage (Broadcast-Echo) oder fremder Controller
            fut = self._pending.get((n.source, n.transaction))
            if fut is not None and not fut.done():
                fut.set_result([f['rdm']])

    # ---- Geraete finden
    async def query_tod(self, port_addresses, wait_s=2.0, flush=True):
        """UIDs je Port-Adresse; leere Tabelle → einmal AtcFlush (volle Discovery im Node), dann erneut fragen."""
        by_net = {}
        for pa in sorted(set(port_addresses)):
            by_net.setdefault(pa >> 8, []).append(pa)

        async def probe_round():
            self._tod_new = asyncio.Event()
            for group in by_net.values():
                for i in range(0, len(group), 32):
                    self.transport.sendto(tod_request(group[i:i + 32]), (self.target_ip, self.target_port))
            await asyncio.sleep(wait_s)

        await probe_round()
        if flush and not any(pa in port_addresses for _, pa in self.tod.values()):
            for pa in sorted(set(port_addresses)):
                self.transport.sendto(tod_control(pa), (self.target_ip, self.target_port))
            await asyncio.sleep(max(wait_s, 3.0))
            await probe_round()
        return [DeviceAddress(u, 0, u) for u, (_, pa) in sorted(self.tod.items(), key=lambda x: (x[1][1], str(x[0])))
                if pa in port_addresses]

    def port_address(self, uid):
        return self.tod.get(uid, (None, None))[1]

    # ---- RDM
    async def _send_and_wait(self, rpt_dest, endpoint, request, timeout_s):
        self._check_write_permission(request.command_class, request.pid)
        if request.dest not in self.tod:
            raise RdmTimeoutError('UID %s nicht in der TOD eines Art-Net-Nodes' % request.dest)
        node_ip, pa = self.tod[request.dest]
        fut = asyncio.get_running_loop().create_future()
        key = (request.dest, request.transaction)
        self._pending[key] = fut
        try:
            self.transport.sendto(art_rdm(pa, request.encode()), (node_ip, self.target_port))
            messages = await asyncio.wait_for(fut, timeout_s)
        except asyncio.TimeoutError as error:
            raise RdmTimeoutError('Keine Antwort auf %s von %s binnen %.1f s (Art-Net)'
                                % (C.PID_NAMES.get(request.pid, hex(request.pid)), request.dest, timeout_s)) from error
        finally:
            self._pending.pop(key, None)
        return self._select_response(request, messages)


class SimulatedArtnetNode:
    """Art-Net-Node fuer Tests/Simulation: beantwortet ArtTodRequest/ArtTodControl/ArtRdm mit simulierten Scheinwerfern
    (`SimScheinwerfer`) an einer Port-Adresse. Antworten gehen an den Absender (Ziel-Port wie empfangen)."""

    def __init__(self, fixture, port_address=0):
        self.fixture, self.pa = list(fixture), port_address
        self.transport = None
        self.received = []
        self.discovery_needed = False          # True: TOD bleibt leer bis AtcFlush

    async def start(self, host='127.0.0.1', port=0):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(lambda: _Protocol(self), local_addr=(host, port))
        return self.transport.get_extra_info('sockname')

    async def stop(self):
        if self.transport:
            self.transport.close()

    def _tod_data(self):
        uids = [] if self.discovery_needed else [s.uid for s in self.fixture]
        return _header(OP_TOD_DATA) + bytes((1, 1)) + bytes(6) + bytes((0, self.pa >> 8, 0, self.pa & 0xFF)) + \
            struct.pack('>H', len(uids)) + bytes((0, len(uids))) + b''.join(u.to_bytes() for u in uids)

    def _on_datagram(self, data, addr):
        if data[:8] != ARTNET_ID:
            return
        op = struct.unpack('<H', data[8:10])[0]
        self.received.append(op)
        if op == OP_TOD_REQUEST:
            net, n = data[21], data[23]
            if any(((net << 8) | a) == self.pa for a in data[24:24 + n]):
                self.transport.sendto(self._tod_data(), addr)
        elif op == OP_TOD_CONTROL:
            if ((data[21] << 8) | data[23]) == self.pa and data[22] == ATC_FLUSH:
                self.discovery_needed = False
                self.transport.sendto(self._tod_data(), addr)
        elif op == OP_RDM:
            if ((data[21] << 8) | data[23]) != self.pa:
                return
            a = RdmMessage.decode(bytes((C.RDM_STARTCODE,)) + data[24:])
            for s in self.fixture:
                response = s.respond(a)
                if response is not None:
                    self.transport.sendto(art_rdm(self.pa, response.encode()), addr)
