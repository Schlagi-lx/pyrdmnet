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

from . import konstanten as K
from .auslesen import GeraeteAdresse
from .controller import RdmnetController, RdmZeitFehler
from .rdm import RdmNachricht, TransaktionsZaehler, Uid

ARTNET_ID = b'Art-Net\x00'
ARTNET_PORT = 6454
PROT_VER = 14
OP_TOD_REQUEST, OP_TOD_DATA, OP_TOD_CONTROL, OP_RDM = 0x8000, 0x8100, 0x8200, 0x8300
TOD_FULL = 0x00
ATC_FLUSH = 0x01
AR_PROCESS = 0x00


class ArtnetFormatFehler(ValueError):
    pass


def _kopf(op):
    return ARTNET_ID + struct.pack('<H', op) + struct.pack('>H', PROT_VER)


def tod_request(port_adressen):
    """ArtTodRequest; alle Adressen muessen im selben Net liegen (je Net ein Paket), hoechstens 32 je Paket."""
    pa = sorted(set(int(x) for x in port_adressen))
    if not pa or len(pa) > 32 or len({x >> 8 for x in pa}) != 1 or any(not 0 <= x <= 32767 for x in pa):
        raise ArtnetFormatFehler('PORT_ADRESSEN_UNGUELTIG')
    net = pa[0] >> 8
    return _kopf(OP_TOD_REQUEST) + bytes(2) + bytes(7) + bytes((net, TOD_FULL, len(pa))) + \
        bytes(x & 0xFF for x in pa).ljust(32, b'\x00')


def tod_control(port_adresse, befehl=ATC_FLUSH):
    return _kopf(OP_TOD_CONTROL) + bytes(2) + bytes(7) + bytes((port_adresse >> 8, befehl, port_adresse & 0xFF))


def art_rdm(port_adresse, rdm_roh):
    """ArtRdm mit der RDM-Nachricht OHNE Startcode 0xCC (Art-Net-Konvention)."""
    if not rdm_roh or rdm_roh[0] != K.RDM_STARTCODE:
        raise ArtnetFormatFehler('RDM_OHNE_STARTCODE')
    return _kopf(OP_RDM) + bytes((1, 0)) + bytes(7) + bytes((port_adresse >> 8, AR_PROCESS, port_adresse & 0xFF)) + rdm_roh[1:]


def zerlegen(paket):
    """(opcode, felder) fuer ArtTodData und ArtRdm, sonst (opcode, None). Falsche Kennung → None."""
    if len(paket) < 12 or paket[:8] != ARTNET_ID:
        return None
    op = struct.unpack('<H', paket[8:10])[0]
    if op == OP_TOD_DATA:
        if len(paket) < 28:
            raise ArtnetFormatFehler('TOD_DATA_ZU_KURZ')
        net, cmd, adr = paket[21], paket[22], paket[23]
        anzahl = paket[27]
        roh = paket[28:28 + 6 * anzahl]
        if len(roh) != 6 * anzahl:
            raise ArtnetFormatFehler('TOD_DATA_UIDS_UNVOLLSTAENDIG')
        uids = [Uid.aus_bytes(roh[i:i + 6]) for i in range(0, len(roh), 6)]
        return op, {'port_adresse': (net << 8) | adr, 'gesamt': (paket[24] << 8) | paket[25], 'block': paket[26],
                    'uids': uids, 'antwort': cmd}
    if op == OP_RDM:
        if len(paket) < 24 + K.RDM_KOPF_LAENGE + 1:
            raise ArtnetFormatFehler('ART_RDM_ZU_KURZ')
        return op, {'port_adresse': (paket[21] << 8) | paket[23], 'rdm': bytes((K.RDM_STARTCODE,)) + paket[24:]}
    return op, None


class _Protokoll(asyncio.DatagramProtocol):
    def __init__(self, ziel):
        self.ziel = ziel

    def datagram_received(self, data, addr):
        try:
            self.ziel._empfangen(data, addr)
        except Exception:      # fremde/kaputte Pakete im Netz stoeren nie
            pass


class ArtnetRdmController:
    """RDM-Controller ueber Art-Net. `ziel_ip`: Node oder Broadcast (fuer ArtTodRequest); RDM geht an den meldenden Node."""

    # gleiche Befehlslogik wie RDMnet (ACK_TIMER, ACK_OVERFLOW, NACK, Schreibsperre)
    rdm_get = RdmnetController.rdm_get
    rdm_set = RdmnetController.rdm_set
    _rdm_befehl = RdmnetController._rdm_befehl
    _schreibrecht_pruefen = RdmnetController._schreibrecht_pruefen
    _antwort_auswaehlen = staticmethod(RdmnetController._antwort_auswaehlen)

    def __init__(self, ziel_ip, *, uid=None, bind=('0.0.0.0', ARTNET_PORT), ziel_port=ARTNET_PORT):
        self.ziel_ip, self.ziel_port, self.bind = ziel_ip, ziel_port, bind
        self.uid = uid or Uid(K.ESTA_PROTOTYP_HERSTELLER, random.randrange(1, 0xFFFFFFFE))
        self._tn = TransaktionsZaehler()
        self.ack_timer_max_s = 10.0
        self.ack_overflow_max = 32
        self.schreibfreigabe = False
        self.transport = None
        self.tod = {}                 # Uid -> (node_ip, port_adresse)
        self._tod_neu = None
        self._offen = {}              # (uid, tn) -> Future
        self.fremde_pakete = 0

    @property
    def verbunden(self):
        return self.transport is not None

    async def verbinden(self, timeout_s=3.0):
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
        self.transport, _ = await loop.create_datagram_endpoint(lambda: _Protokoll(self), sock=sock)

    async def trennen(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    @property
    def port(self):
        return self.transport.get_extra_info('sockname')[1] if self.transport else None

    # ---- Empfang
    def _empfangen(self, data, addr):
        z = zerlegen(data)
        if z is None:
            self.fremde_pakete += 1
            return
        op, f = z
        if op == OP_TOD_DATA and f is not None:
            for u in f['uids']:
                self.tod[u] = (addr[0], f['port_adresse'])
            if self._tod_neu is not None:
                self._tod_neu.set()
        elif op == OP_RDM and f is not None:
            try:
                n = RdmNachricht.dekodieren(f['rdm'])
            except Exception:
                return
            if not n.ist_antwort:
                return                    # eigene Anfrage (Broadcast-Echo) oder fremder Controller
            fut = self._offen.get((n.quelle, n.transaktion))
            if fut is not None and not fut.done():
                fut.set_result([f['rdm']])

    # ---- Geraete finden
    async def tod_abfragen(self, port_adressen, warte_s=2.0, flush=True):
        """UIDs je Port-Adresse; leere Tabelle → einmal AtcFlush (volle Discovery im Node), dann erneut fragen."""
        nach_net = {}
        for pa in sorted(set(port_adressen)):
            nach_net.setdefault(pa >> 8, []).append(pa)

        async def runde():
            self._tod_neu = asyncio.Event()
            for liste in nach_net.values():
                for i in range(0, len(liste), 32):
                    self.transport.sendto(tod_request(liste[i:i + 32]), (self.ziel_ip, self.ziel_port))
            await asyncio.sleep(warte_s)

        await runde()
        if flush and not any(pa in port_adressen for _, pa in self.tod.values()):
            for pa in sorted(set(port_adressen)):
                self.transport.sendto(tod_control(pa), (self.ziel_ip, self.ziel_port))
            await asyncio.sleep(max(warte_s, 3.0))
            await runde()
        return [GeraeteAdresse(u, 0, u) for u, (_, pa) in sorted(self.tod.items(), key=lambda x: (x[1][1], str(x[0])))
                if pa in port_adressen]

    def port_adresse(self, uid):
        return self.tod.get(uid, (None, None))[1]

    # ---- RDM
    async def _sende_und_warte(self, rpt_ziel, endpoint, anfrage, timeout_s):
        self._schreibrecht_pruefen(anfrage.command_class, anfrage.pid)
        if anfrage.ziel not in self.tod:
            raise RdmZeitFehler('UID %s nicht in der TOD eines Art-Net-Nodes' % anfrage.ziel)
        node_ip, pa = self.tod[anfrage.ziel]
        fut = asyncio.get_running_loop().create_future()
        schluessel = (anfrage.ziel, anfrage.transaktion)
        self._offen[schluessel] = fut
        try:
            self.transport.sendto(art_rdm(pa, anfrage.kodieren()), (node_ip, self.ziel_port))
            nachrichten = await asyncio.wait_for(fut, timeout_s)
        except asyncio.TimeoutError as fehler:
            raise RdmZeitFehler('Keine Antwort auf %s von %s binnen %.1f s (Art-Net)'
                                % (K.PID_NAMEN.get(anfrage.pid, hex(anfrage.pid)), anfrage.ziel, timeout_s)) from fehler
        finally:
            self._offen.pop(schluessel, None)
        return self._antwort_auswaehlen(anfrage, nachrichten)


class SimulierterArtnetNode:
    """Art-Net-Node fuer Tests/Simulation: beantwortet ArtTodRequest/ArtTodControl/ArtRdm mit simulierten Scheinwerfern
    (`SimScheinwerfer`) an einer Port-Adresse. Antworten gehen an den Absender (Ziel-Port wie empfangen)."""

    def __init__(self, scheinwerfer, port_adresse=0):
        self.scheinwerfer, self.pa = list(scheinwerfer), port_adresse
        self.transport = None
        self.empfangen = []
        self.discovery_noetig = False          # True: TOD bleibt leer bis AtcFlush

    async def starten(self, host='127.0.0.1', port=0):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(lambda: _Protokoll(self), local_addr=(host, port))
        return self.transport.get_extra_info('sockname')

    async def stoppen(self):
        if self.transport:
            self.transport.close()

    def _tod_data(self):
        uids = [] if self.discovery_noetig else [s.uid for s in self.scheinwerfer]
        return _kopf(OP_TOD_DATA) + bytes((1, 1)) + bytes(6) + bytes((0, self.pa >> 8, 0, self.pa & 0xFF)) + \
            struct.pack('>H', len(uids)) + bytes((0, len(uids))) + b''.join(u.als_bytes() for u in uids)

    def _empfangen(self, data, addr):
        if data[:8] != ARTNET_ID:
            return
        op = struct.unpack('<H', data[8:10])[0]
        self.empfangen.append(op)
        if op == OP_TOD_REQUEST:
            net, n = data[21], data[23]
            if any(((net << 8) | a) == self.pa for a in data[24:24 + n]):
                self.transport.sendto(self._tod_data(), addr)
        elif op == OP_TOD_CONTROL:
            if ((data[21] << 8) | data[23]) == self.pa and data[22] == ATC_FLUSH:
                self.discovery_noetig = False
                self.transport.sendto(self._tod_data(), addr)
        elif op == OP_RDM:
            if ((data[21] << 8) | data[23]) != self.pa:
                return
            a = RdmNachricht.dekodieren(bytes((K.RDM_STARTCODE,)) + data[24:])
            for s in self.scheinwerfer:
                antwort = s.beantworten(a)
                if antwort is not None:
                    self.transport.sendto(art_rdm(self.pa, antwort.kodieren()), addr)
