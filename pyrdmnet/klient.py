"""RPT-Client-Grundlage (Verbindungsaufbau, Empfang, Heartbeat) für Controller und Geräte."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

from . import konstanten as K
from . import pdu as P
from .rdm import Uid
from .verbindung import Verbindung

log = logging.getLogger("pyrdmnet.klient")


class VerbindungsFehler(ConnectionError):
    def __init__(self, text: str, code: int | None = None):
        super().__init__(text)
        self.code = code


class RptKlient:
    """Gemeinsame Logik für RPT-Controller und RPT-Geräte."""

    def __init__(self, typ: int, uid: Uid, scope: str = K.E133_STANDARD_SCOPE, cid: uuid.UUID | None = None,
                 inkrementell: bool = False, heartbeat_intervall_s: float = K.E133_HEARTBEAT_INTERVALL_S,
                 heartbeat_timeout_s: float = K.E133_HEARTBEAT_TIMEOUT_S):
        self.typ = typ
        self.uid_wunsch = uid
        self.scope = scope
        self.cid = cid or uuid.uuid4()
        self.inkrementell = inkrementell
        self.heartbeat_intervall_s = heartbeat_intervall_s
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.uid: Uid | None = None
        self.broker_uid: Uid | None = None
        self._v: Verbindung | None = None
        self._tasks: list[asyncio.Task] = []
        self.getrennt = asyncio.Event()
        self.trenngrund: str = ""

    @property
    def verbunden(self) -> bool:
        return self._v is not None and not self._v.geschlossen

    async def verbinden(self, host: str, port: int, timeout_s: float = 5.0) -> Uid:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout_s)
        except (OSError, asyncio.TimeoutError) as fehler:
            raise VerbindungsFehler(f"Broker {host}:{port} nicht erreichbar: {fehler}") from fehler
        v = Verbindung(reader, writer, self.cid)
        eintrag = P.ClientEintrag(self.cid, self.uid_wunsch, self.typ)
        flags = K.BROKER_CONNECT_FLAG_INKREMENTELL if self.inkrementell else 0
        cd = P.ConnectDaten(self.scope, K.E133_VERSION, K.E133_STANDARD_DOMAIN, flags, eintrag)
        try:
            await v.sende_broker(K.VECTOR_BROKER_CONNECT, cd.kodieren())
            roots = await asyncio.wait_for(v.lese(), timeout_s)
            bp = P.lese_broker(roots[0].daten)
            if roots[0].vektor != K.VECTOR_ROOT_E133_BROKER or bp.vektor != K.VECTOR_BROKER_CONNECT_REPLY:
                raise VerbindungsFehler("Broker antwortet nicht mit Connect Reply")
            rep = P.lese_connect_reply(bp.daten)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, P.ProtokollFehler, ConnectionError) as fehler:
            await v.schliessen()
            if isinstance(fehler, VerbindungsFehler):
                raise
            raise VerbindungsFehler(f"Connect fehlgeschlagen: {fehler!r}") from fehler
        if rep.code != K.E133_CONNECT_OK:
            await v.schliessen()
            raise VerbindungsFehler(f"Broker lehnt ab: {K.CONNECT_CODE_TEXT.get(rep.code, rep.code)}", rep.code)
        self._v, self.uid, self.broker_uid = v, rep.client_uid, rep.broker_uid
        self.getrennt.clear()
        self._tasks = [asyncio.create_task(self._empfangsschleife()),
                       asyncio.create_task(self._heartbeat_schleife())]
        await self._nach_verbindung()
        return rep.client_uid

    async def trennen(self) -> None:
        if self._v and not self._v.geschlossen:
            try:
                await self._v.sende_broker(K.VECTOR_BROKER_DISCONNECT,
                                           K.E133_DISCONNECT_SHUTDOWN.to_bytes(2, "big"))
            except (ConnectionError, OSError):
                pass
        await self._beenden("vom Programm getrennt")
        for t in self._tasks:
            if t is not asyncio.current_task():
                t.cancel()
        await asyncio.gather(*[t for t in self._tasks if t is not asyncio.current_task()],
                             return_exceptions=True)

    async def _beenden(self, grund: str) -> None:
        if self._v:
            await self._v.schliessen()
        if not self.getrennt.is_set():
            self.trenngrund = grund
            self.getrennt.set()
            await self._nach_trennung(grund)

    async def sende_broker(self, vektor: int, daten: bytes = b"") -> None:
        if not self.verbunden:
            raise VerbindungsFehler("Nicht mit Broker verbunden")
        await self._v.sende_broker(vektor, daten)

    async def sende_rpt(self, kopf: P.RptKopf, daten: bytes) -> None:
        if not self.verbunden:
            raise VerbindungsFehler("Nicht mit Broker verbunden")
        await self._v.sende_rpt(kopf, daten)

    async def _empfangsschleife(self) -> None:
        grund = "Broker hat Verbindung geschlossen"
        try:
            while True:
                for root in await self._v.lese():
                    try:
                        if root.vektor == K.VECTOR_ROOT_E133_BROKER:
                            bp = P.lese_broker(root.daten)
                            if bp.vektor == K.VECTOR_BROKER_DISCONNECT:
                                grund = "Broker sendet Disconnect"
                                return
                            await self._bei_broker(bp)
                        elif root.vektor == K.VECTOR_ROOT_E133_RPT:
                            await self._bei_rpt(P.lese_rpt(root.daten))
                    except P.ProtokollFehler as fehler:
                        log.warning("Ungültige Nachricht verworfen: %s", fehler)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except P.ProtokollFehler as fehler:
            grund = f"Protokollfehler: {fehler}"
        except asyncio.CancelledError:
            grund = "beendet"
            raise
        finally:
            await self._beenden(grund)

    async def _heartbeat_schleife(self) -> None:
        takt = min(1.0, self.heartbeat_intervall_s / 3)
        while self.verbunden:
            await asyncio.sleep(takt)
            jetzt = time.monotonic()
            if jetzt - self._v.letzter_empfang > self.heartbeat_timeout_s:
                await self._beenden("Heartbeat-Zeitüberschreitung (Broker antwortet nicht)")
                return
            if jetzt - self._v.letztes_senden >= self.heartbeat_intervall_s:
                try:
                    await self._v.sende_broker(K.VECTOR_BROKER_NULL)
                except (ConnectionError, OSError):
                    await self._beenden("Senden fehlgeschlagen")
                    return

    # --- Erweiterungspunkte
    async def _nach_verbindung(self) -> None:
        pass

    async def _nach_trennung(self, grund: str) -> None:
        pass

    async def _bei_broker(self, bp: P.BrokerPdu) -> None:
        pass

    async def _bei_rpt(self, rpt: P.RptPdu) -> None:
        pass
