"""RDMnet-Controller: RDM GET frei, RDM SET nur eingeschränkt.

Entscheidung 26.10.2026 (Variante b): Die Software darf DMX-Modus und
Startadresse am Gerät setzen. Sicherungen:
* SET nur für die PIDs in :data:`ERLAUBTE_SET_PIDS`
* SET nur bei aktiver Schreibfreigabe (``schreibfreigabe = True``, gilt je Sitzung)
* Discovery-Befehle werden nie gesendet
Jede andere Schreibanfrage wird vor dem Senden abgewiesen (:class:`SchreibSperreFehler`).
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from . import konstanten as K
from . import pdu as P
from .klient import RptKlient, VerbindungsFehler
from .rdm import RdmFormatFehler, RdmNachricht, TransaktionsZaehler, Uid

log = logging.getLogger("pyrdmnet.controller")


class RdmFehler(Exception):
    """Oberklasse aller Fehler einer RDM-Abfrage."""


ERLAUBTE_SET_PIDS = frozenset({K.PID_DMX_PERSONALITY, K.PID_DMX_START_ADDRESS, K.PID_IDENTIFY_DEVICE})


class SchreibSperreFehler(RdmFehler):
    """Ein schreibender RDM-Befehl ist nicht erlaubt (PID nicht freigegeben oder keine Freigabe)."""


class RdmZeitFehler(RdmFehler):
    pass


class RdmNackFehler(RdmFehler):
    def __init__(self, pid: int, grund: int):
        self.pid, self.grund = pid, grund
        super().__init__(f"NACK für {K.PID_NAMEN.get(pid, hex(pid))}: {K.NACK_TEXT.get(grund, hex(grund))}")


class RptStatusFehler(RdmFehler):
    def __init__(self, code: int, text: str = ""):
        self.code = code
        super().__init__(f"RPT-Status: {K.RPT_STATUS_TEXT.get(code, hex(code))} {text}".strip())


class RdmAntwortFehler(RdmFehler):
    """Antwort formal ungültig oder passt nicht zur Anfrage."""


class RdmnetController(RptKlient):
    def __init__(self, uid: Uid | None = None, scope: str = K.E133_STANDARD_SCOPE, cid: uuid.UUID | None = None,
                 **kwargs):
        # Standard: dynamische UID im ESTA-Prototypenbereich anfordern
        uid = uid or Uid(0x8000 | K.ESTA_PROTOTYP_HERSTELLER, 0)
        super().__init__(K.RPT_CLIENT_TYP_CONTROLLER, uid, scope, cid, inkrementell=True, **kwargs)
        self.clients: dict[Uid, P.ClientEintrag] = {}
        self._clientliste_da = asyncio.Event()
        self._offen: dict[int, asyncio.Future] = {}
        self._sequenz = 0
        self._tn = TransaktionsZaehler()
        self.unaufgeforderte_meldungen: list[tuple[Uid, int, bytes]] = []
        self.ack_timer_max_s = 10.0
        self.ack_overflow_max = 32
        self.schreibfreigabe = False  # je Sitzung aktiv setzen (Checkliste vor Ort)

    # ----------------------------------------------------------------- Clientliste
    async def _nach_verbindung(self) -> None:
        self._clientliste_da.clear()
        await self.sende_broker(K.VECTOR_BROKER_FETCH_CLIENT_LIST)

    async def _nach_trennung(self, grund: str) -> None:
        for f in self._offen.values():
            if not f.done():
                f.set_exception(VerbindungsFehler(f"Verbindung getrennt: {grund}"))
        self._offen.clear()

    async def clientliste(self, timeout_s: float = 5.0, neu_abfragen: bool = False) -> dict[Uid, P.ClientEintrag]:
        if neu_abfragen:
            self._clientliste_da.clear()
            await self.sende_broker(K.VECTOR_BROKER_FETCH_CLIENT_LIST)
        await asyncio.wait_for(self._clientliste_da.wait(), timeout_s)
        return dict(self.clients)

    def geraete(self) -> list[P.ClientEintrag]:
        return [e for e in self.clients.values() if e.typ == K.RPT_CLIENT_TYP_GERAET]

    async def _bei_broker(self, bp: P.BrokerPdu) -> None:
        if bp.vektor == K.VECTOR_BROKER_CONNECTED_CLIENT_LIST:
            self.clients = {e.uid: e for e in P.lese_client_eintraege(bp.daten)
                            if e.protokoll == K.E133_CLIENT_PROTOKOLL_RPT}
            self._clientliste_da.set()
        elif bp.vektor in (K.VECTOR_BROKER_CLIENT_ADD, K.VECTOR_BROKER_CLIENT_ENTRY_CHANGE):
            for e in P.lese_client_eintraege(bp.daten):
                if e.protokoll == K.E133_CLIENT_PROTOKOLL_RPT:
                    self.clients[e.uid] = e
        elif bp.vektor == K.VECTOR_BROKER_CLIENT_REMOVE:
            for e in P.lese_client_eintraege(bp.daten):
                self.clients.pop(e.uid, None)

    # ----------------------------------------------------------------- RPT-Empfang
    async def _bei_rpt(self, rpt: P.RptPdu) -> None:
        k = rpt.kopf
        f = self._offen.get(k.sequenz)
        if k.vektor == K.VECTOR_RPT_STATUS:
            code, text = P.lese_status(rpt.daten)
            if f and not f.done():
                f.set_exception(RptStatusFehler(code, text))
            return
        if k.vektor != K.VECTOR_RPT_NOTIFICATION:
            return
        nachrichten = P.lese_request_oder_notification(rpt.daten, K.VECTOR_NOTIFICATION_RDM_CMD)
        if f and not f.done():
            f.set_result(nachrichten)
        else:
            for n in nachrichten:
                self.unaufgeforderte_meldungen.append((k.quelle, k.quelle_endpoint, n))
            del self.unaufgeforderte_meldungen[:-200]

    # ----------------------------------------------------------------- RDM GET
    def _naechste_sequenz(self) -> int:
        self._sequenz = (self._sequenz % 0xFFFFFFFF) + 1
        return self._sequenz

    async def _sende_und_warte(self, rpt_ziel: Uid, endpoint: int, anfrage: RdmNachricht,
                               timeout_s: float) -> RdmNachricht:
        self._schreibrecht_pruefen(anfrage.command_class, anfrage.pid)
        seq = self._naechste_sequenz()
        fut = asyncio.get_running_loop().create_future()
        self._offen[seq] = fut
        kopf = P.RptKopf(K.VECTOR_RPT_REQUEST, self.uid, K.E133_NULL_ENDPOINT, rpt_ziel, endpoint, seq)
        try:
            await self.sende_rpt(kopf, P.baue_request(anfrage.kodieren()))
            nachrichten = await asyncio.wait_for(fut, timeout_s)
        except asyncio.TimeoutError as fehler:
            raise RdmZeitFehler(f"Keine Antwort auf {K.PID_NAMEN.get(anfrage.pid, hex(anfrage.pid))} "
                                f"von {anfrage.ziel} binnen {timeout_s:.1f} s") from fehler
        finally:
            self._offen.pop(seq, None)
        return self._antwort_auswaehlen(anfrage, nachrichten)

    def _schreibrecht_pruefen(self, cc: int, pid: int) -> None:
        if cc == K.CC_GET:
            return
        if cc != K.CC_SET:
            raise SchreibSperreFehler("Discovery-Befehle sendet das Messlabor nicht")
        if pid not in ERLAUBTE_SET_PIDS:
            raise SchreibSperreFehler(f"SET für {K.PID_NAMEN.get(pid, hex(pid))} nicht freigegeben")
        if not self.schreibfreigabe:
            raise SchreibSperreFehler("Keine Schreibfreigabe für diese Sitzung")

    @staticmethod
    def _antwort_auswaehlen(anfrage: RdmNachricht, nachrichten: list[bytes]) -> RdmNachricht:
        """Notification enthält Anfrage und Antwort; die passende Antwort heraussuchen."""
        for roh in nachrichten:
            try:
                n = RdmNachricht.dekodieren(roh)
            except RdmFormatFehler as fehler:
                raise RdmAntwortFehler(f"RDM-Antwort ungültig: {fehler}") from fehler
            if not n.ist_antwort:
                continue
            soll_cc = K.CC_GET_RESPONSE if anfrage.command_class == K.CC_GET else K.CC_SET_RESPONSE
            if (n.pid != anfrage.pid or n.transaktion != anfrage.transaktion or n.quelle != anfrage.ziel
                    or n.command_class != soll_cc):
                raise RdmAntwortFehler(f"Antwort passt nicht zur Anfrage (PID {n.pid:#06x}, TN {n.transaktion})")
            return n
        raise RdmAntwortFehler("Notification ohne RDM-Antwort")

    async def rdm_get(self, rpt_ziel: Uid, pid: int, daten: bytes = b"", endpoint: int = K.E133_NULL_ENDPOINT,
                      rdm_ziel: Uid | None = None, subdevice: int = K.RDM_ROOT_DEVICE,
                      timeout_s: float = 3.0) -> bytes:
        return await self._rdm_befehl(K.CC_GET, rpt_ziel, pid, daten, endpoint, rdm_ziel, subdevice, timeout_s)

    async def rdm_set(self, rpt_ziel: Uid, pid: int, daten: bytes, endpoint: int = K.E133_NULL_ENDPOINT,
                      rdm_ziel: Uid | None = None, subdevice: int = K.RDM_ROOT_DEVICE,
                      timeout_s: float = 3.0) -> bytes:
        """RDM SET – nur für ERLAUBTE_SET_PIDS und mit aktiver Schreibfreigabe."""
        self._schreibrecht_pruefen(K.CC_SET, pid)
        return await self._rdm_befehl(K.CC_SET, rpt_ziel, pid, daten, endpoint, rdm_ziel, subdevice, timeout_s)

    async def _rdm_befehl(self, cc: int, rpt_ziel: Uid, pid: int, daten: bytes, endpoint: int,
                          rdm_ziel: Uid | None, subdevice: int, timeout_s: float) -> bytes:
        """RDM GET/SET über RDMnet. Behandelt ACK_TIMER und ACK_OVERFLOW.

        rpt_ziel: UID des RDMnet-Geräts (Gateway oder natives Gerät)
        rdm_ziel: UID des RDM-Responders (Standard: rpt_ziel, also Default-Responder)
        endpoint: 0 = Default-Responder, >0 = Gateway-Port
        """
        if not self.verbunden:
            raise VerbindungsFehler("Nicht mit Broker verbunden")
        rdm_ziel = rdm_ziel or rpt_ziel
        gesamt = b""
        overflow = 0
        wartezeit = 0.0
        while True:
            anfrage = RdmNachricht(ziel=rdm_ziel, quelle=self.uid, transaktion=self._tn.naechste(rdm_ziel),
                                   port_oder_antworttyp=1, command_class=cc, pid=pid, daten=daten,
                                   subdevice=subdevice)
            antwort = await self._sende_und_warte(rpt_ziel, endpoint, anfrage, timeout_s)
            typ = antwort.antworttyp
            if typ == K.ANTWORT_ACK:
                return gesamt + antwort.daten
            if typ == K.ANTWORT_NACK:
                grund = int.from_bytes(antwort.daten[:2], "big") if len(antwort.daten) >= 2 else 0xFFFF
                raise RdmNackFehler(pid, grund)
            if typ == K.ANTWORT_ACK_TIMER:
                zehntel = int.from_bytes(antwort.daten[:2], "big") if len(antwort.daten) >= 2 else 1
                pause = max(zehntel, 1) / 10.0
                wartezeit += pause
                if wartezeit > self.ack_timer_max_s:
                    raise RdmZeitFehler(f"ACK_TIMER summiert über {self.ack_timer_max_s} s")
                await asyncio.sleep(pause)
                if cc == K.CC_SET:
                    # SET wird nicht wiederholt (sonst doppelt ausgeführt); Ergebnis per Rücklesen prüfen
                    return b""
                continue  # E1.20 sieht QUEUED_MESSAGE vor; hier: dieselbe GET-Anfrage wiederholen
            if typ == K.ANTWORT_ACK_OVERFLOW:
                gesamt += antwort.daten
                overflow += 1
                if overflow > self.ack_overflow_max:
                    raise RdmAntwortFehler("Zu viele ACK_OVERFLOW-Teile")
                continue
            raise RdmAntwortFehler(f"Unbekannter Antworttyp {typ}")
