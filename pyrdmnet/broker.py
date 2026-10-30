"""Eigener RDMnet-Broker nach ANSI E1.33 (RPT-Clients, TCP, asyncio).

Umfang V0.1:
* Client Connect mit Prüfung von Scope, E1.33-Version, Client-Eintrag, UID
* Statische und dynamische UIDs (Zuordnung je CID bleibt über Neuverbindungen gleich)
* Connected Client List, Client Add/Remove an Clients mit Inkrementell-Flag
* Request Dynamic UIDs / Fetch Dynamic UID List
* RPT-Weiterleitung Controller → Gerät (auch Broadcast an alle Geräte bzw.
  alle Geräte eines Herstellers), Gerät → Controller (gezielt oder an alle)
* Status UNKNOWN_RPT_UID und BROADCAST_COMPLETE vom Broker
* Broker-eigener RDM-Responder (siehe :mod:`pyrdmnet.broker_responder`)
* Heartbeat (Broker-NULL) und Verbindungsabbau bei Zeitüberschreitung
* Ereignisprotokoll im Speicher und optional als JSONL-Datei

Nicht enthalten: EPT-Clients, Redirect, LLRP. DNS-SD-Anmeldung über :mod:`pyrdmnet.dnssd`.
"""

from __future__ import annotations

import asyncio
import collections
import datetime as dt
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import konstanten as K
from . import pdu as P
from .broker_responder import BrokerResponder, BrokerResponderDaten
from .rdm import ALLE_CONTROLLER, RdmFormatFehler, RdmNachricht, Uid, hersteller_aus_mid_broadcast, ist_geraete_broadcast
from .verbindung import Verbindung

log = logging.getLogger("pyrdmnet.broker")


@dataclass
class BrokerKonfig:
    host: str = "127.0.0.1"
    port: int = 8888  # 0 = freien Port wählen
    scope: str = K.E133_STANDARD_SCOPE
    suchdomain: str = K.E133_STANDARD_DOMAIN  # für RDM SEARCH_DOMAIN
    geraete_label: str = "pyrdmnet Broker"  # für RDM DEVICE_LABEL
    uid: Uid = field(default_factory=lambda: Uid(K.ESTA_PROTOTYP_HERSTELLER, 0x00000001))
    cid: uuid.UUID = field(default_factory=uuid.uuid4)
    max_clients: int = 64
    heartbeat_intervall_s: float = K.E133_HEARTBEAT_INTERVALL_S
    heartbeat_timeout_s: float = K.E133_HEARTBEAT_TIMEOUT_S
    connect_timeout_s: float = 10.0
    ereignis_datei: Path | None = None
    mitschnitt_datei: Path | None = None  # Rohdaten aller Pakete als JSONL (Diagnose Live-Test)
    mitschnitt_max_zeilen: int = 50000


@dataclass
class _Client:
    verbindung: Verbindung
    eintrag: P.ClientEintrag
    inkrementell: bool
    verbunden_seit: str

    @property
    def uid(self) -> Uid:
        return self.eintrag.uid

    @property
    def ist_controller(self) -> bool:
        return self.eintrag.typ == K.RPT_CLIENT_TYP_CONTROLLER


def _jetzt() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


class RdmnetBroker:
    def __init__(self, konfig: BrokerKonfig | None = None):
        self.konfig = konfig or BrokerKonfig()
        self._server: asyncio.base_events.Server | None = None
        self._clients: dict[Uid, _Client] = {}
        self._dyn_uid_je_rid: dict[tuple[int, uuid.UUID], Uid] = {}
        self._dyn_naechste: dict[int, int] = collections.defaultdict(lambda: 1)
        self._heartbeat_task: asyncio.Task | None = None
        self._verbindungs_tasks: set[asyncio.Task] = set()
        self.ereignisse: collections.deque = collections.deque(maxlen=1000)
        self.zaehler = collections.Counter()
        self.port: int | None = None
        self._mitschnitt_zeilen = 0
        k = self.konfig
        self.responder = BrokerResponder(
            BrokerResponderDaten(uid=k.uid, scope=k.scope, suchdomain=k.suchdomain, geraete_label=k.geraete_label),
            ereignis=self._ereignis)

    # ----------------------------------------------------------------- Lebenszyklus
    async def starten(self) -> None:
        self._server = await asyncio.start_server(self._neue_verbindung, self.konfig.host, self.konfig.port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._heartbeat_task = asyncio.create_task(self._heartbeat_schleife())
        self._ereignis("broker_gestartet", host=self.konfig.host, port=self.port, scope=self.konfig.scope,
                       uid=str(self.konfig.uid), cid=str(self.konfig.cid))

    async def stoppen(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        for c in list(self._clients.values()):
            try:
                await c.verbindung.sende_broker(K.VECTOR_BROKER_DISCONNECT, K.E133_DISCONNECT_SHUTDOWN.to_bytes(2, "big"))
            except Exception:
                pass
            await c.verbindung.schliessen()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for t in list(self._verbindungs_tasks):
            t.cancel()
        if self._verbindungs_tasks:
            await asyncio.gather(*self._verbindungs_tasks, return_exceptions=True)
        self._clients.clear()
        self._ereignis("broker_gestoppt")

    def status(self) -> dict:
        return {
            "zeit_utc": _jetzt(),
            "host": self.konfig.host,
            "port": self.port,
            "scope": self.konfig.scope,
            "broker_uid": str(self.konfig.uid),
            "clients": [
                {"uid": str(c.uid), "typ": c.eintrag.typ_text, "cid": str(c.eintrag.cid),
                 "gegenstelle": c.verbindung.gegenstelle, "verbunden_seit": c.verbunden_seit}
                for c in self._clients.values()
            ],
            "zaehler": dict(self.zaehler),
        }

    # ----------------------------------------------------------------- Ereignisse
    def _ereignis(self, art: str, **daten) -> None:
        e = {"zeit_utc": _jetzt(), "ereignis": art, **daten}
        self.ereignisse.append(e)
        log.info("%s %s", art, daten)
        if self.konfig.ereignis_datei:
            try:
                with open(self.konfig.ereignis_datei, "a", encoding="utf-8") as f:
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
            except OSError as fehler:  # Protokoll darf den Broker nie stoppen
                log.warning("Ereignisdatei nicht schreibbar: %s", fehler)

    def _mitschnitt(self, v: Verbindung, richtung: str, daten: bytes) -> None:
        if self._mitschnitt_zeilen >= self.konfig.mitschnitt_max_zeilen:
            return
        self._mitschnitt_zeilen += 1
        e = {"zeit_utc": _jetzt(), "gegenstelle": v.gegenstelle, "richtung": richtung, "laenge": len(daten),
             "hex": daten[:4096].hex()}
        try:
            with open(self.konfig.mitschnitt_datei, "a", encoding="utf-8") as f:
                f.write(json.dumps(e) + "\n")
        except OSError:
            pass

    # ----------------------------------------------------------------- Verbindung
    async def _neue_verbindung(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        self._verbindungs_tasks.add(task)
        v = Verbindung(reader, writer, self.konfig.cid)
        if self.konfig.mitschnitt_datei:
            v.mitschnitt = lambda richtung, daten: self._mitschnitt(v, richtung, daten)
        self._ereignis("tcp_verbindung", gegenstelle=v.gegenstelle)
        client: _Client | None = None
        try:
            client = await asyncio.wait_for(self._connect_abwickeln(v), timeout=self.konfig.connect_timeout_s)
            if client is None:
                return
            while True:
                for root in await v.lese():
                    await self._root_verarbeiten(client, root)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass
        except P.ProtokollFehler as fehler:
            self.zaehler["protokollfehler"] += 1
            self._ereignis("protokollfehler", gegenstelle=v.gegenstelle, fehler=str(fehler))
        except asyncio.CancelledError:
            raise
        except Exception as fehler:  # unerwartet: protokollieren, Verbindung schließen
            self.zaehler["interner_fehler"] += 1
            self._ereignis("interner_fehler", gegenstelle=v.gegenstelle, fehler=repr(fehler))
        finally:
            await v.schliessen()
            if client is not None:
                await self._client_entfernen(client)
            self._verbindungs_tasks.discard(task)

    async def _connect_abwickeln(self, v: Verbindung) -> _Client | None:
        roots = await v.lese()
        root = roots[0]
        if root.vektor != K.VECTOR_ROOT_E133_BROKER:
            raise P.ProtokollFehler("Erste Nachricht ist keine Broker-PDU")
        bp = P.lese_broker(root.daten)
        if bp.vektor != K.VECTOR_BROKER_CONNECT:
            raise P.ProtokollFehler("Erste Nachricht ist kein Client Connect")
        try:
            cd = P.lese_connect(bp.daten)
        except P.ProtokollFehler:
            await self._connect_antwort(v, K.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0))
            raise
        self._ereignis("connect_anfrage", gegenstelle=v.gegenstelle, scope=cd.scope, version=cd.version,
                       uid=str(cd.client.uid), typ=cd.client.typ_text, protokoll=f"{cd.client.protokoll:#x}",
                       inkrementell=bool(cd.flags & K.BROKER_CONNECT_FLAG_INKREMENTELL))
        code, uid = self._connect_pruefen(cd, root.cid)
        await self._connect_antwort(v, code, uid)
        if code != K.E133_CONNECT_OK:
            self.zaehler["connect_abgelehnt"] += 1
            self._ereignis("connect_abgelehnt", gegenstelle=v.gegenstelle, grund=K.CONNECT_CODE_TEXT[code],
                           scope=cd.scope, uid=str(cd.client.uid))
            return None
        eintrag = P.ClientEintrag(cd.client.cid, uid, cd.client.typ, cd.client.binding_cid)
        client = _Client(v, eintrag, bool(cd.flags & K.BROKER_CONNECT_FLAG_INKREMENTELL), _jetzt())
        self._clients[uid] = client
        self.zaehler["connect_ok"] += 1
        self._ereignis("client_verbunden", uid=str(uid), typ=eintrag.typ_text, gegenstelle=v.gegenstelle)
        await self._an_inkrementelle(K.VECTOR_BROKER_CLIENT_ADD, eintrag.kodieren(), ausser=uid)
        return client

    def _connect_pruefen(self, cd: P.ConnectDaten, root_cid: uuid.UUID) -> tuple[int, Uid]:
        if cd.scope != self.konfig.scope:
            return K.E133_CONNECT_SCOPE_MISMATCH, Uid(0, 0)
        if cd.version != K.E133_VERSION or cd.client.protokoll != K.E133_CLIENT_PROTOKOLL_RPT:
            return K.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if cd.client.typ not in (K.RPT_CLIENT_TYP_GERAET, K.RPT_CLIENT_TYP_CONTROLLER):
            return K.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if cd.client.cid != root_cid:
            return K.E133_CONNECT_INVALID_CLIENT_ENTRY, Uid(0, 0)
        if len(self._clients) >= self.konfig.max_clients:
            return K.E133_CONNECT_CAPACITY_EXCEEDED, Uid(0, 0)
        uid = cd.client.uid
        if uid.ist_dynamisch_angefragt:
            uid = self._dyn_uid_vergeben(uid.hersteller & 0x7FFF, cd.client.cid)
        elif not uid.ist_gueltig_fuer_client or uid.ist_dynamisch:
            return K.E133_CONNECT_INVALID_UID, Uid(0, 0)
        if uid in self._clients or uid == self.konfig.uid:
            return K.E133_CONNECT_DUPLICATE_UID, Uid(0, 0)
        return K.E133_CONNECT_OK, uid

    def _dyn_uid_vergeben(self, hersteller: int, rid: uuid.UUID) -> Uid:
        schluessel = (hersteller, rid)
        if schluessel not in self._dyn_uid_je_rid:
            n = self._dyn_naechste[hersteller]
            self._dyn_naechste[hersteller] = n + 1
            self._dyn_uid_je_rid[schluessel] = Uid(0x8000 | hersteller, n)
        return self._dyn_uid_je_rid[schluessel]

    async def _connect_antwort(self, v: Verbindung, code: int, uid: Uid) -> None:
        rep = P.ConnectReply(code, K.E133_VERSION, self.konfig.uid, uid)
        await v.sende_broker(K.VECTOR_BROKER_CONNECT_REPLY, rep.kodieren())

    async def _client_entfernen(self, client: _Client) -> None:
        if self._clients.get(client.uid) is client:
            del self._clients[client.uid]
            self._ereignis("client_getrennt", uid=str(client.uid), typ=client.eintrag.typ_text)
            await self._an_inkrementelle(K.VECTOR_BROKER_CLIENT_REMOVE, client.eintrag.kodieren())

    async def _an_inkrementelle(self, vektor: int, daten: bytes, ausser: Uid | None = None) -> None:
        for c in list(self._clients.values()):
            if c.inkrementell and c.uid != ausser:
                await self._sicher_senden(c, c.verbindung.sende_broker(vektor, daten))

    async def _sicher_senden(self, client: _Client, coro) -> bool:
        try:
            await coro
            return True
        except (ConnectionError, OSError) as fehler:
            self._ereignis("senden_fehlgeschlagen", uid=str(client.uid), fehler=str(fehler))
            await client.verbindung.schliessen()
            return False

    # ----------------------------------------------------------------- Nachrichten
    async def _root_verarbeiten(self, client: _Client, root: P.RootPdu) -> None:
        if root.vektor == K.VECTOR_ROOT_E133_BROKER:
            await self._broker_verarbeiten(client, P.lese_broker(root.daten))
        elif root.vektor == K.VECTOR_ROOT_E133_RPT:
            await self._rpt_verarbeiten(client, P.lese_rpt(root.daten))
        else:
            self.zaehler["unbekannter_root_vektor"] += 1

    async def _broker_verarbeiten(self, client: _Client, bp: P.BrokerPdu) -> None:
        v = client.verbindung
        if bp.vektor == K.VECTOR_BROKER_NULL:
            return
        if bp.vektor == K.VECTOR_BROKER_FETCH_CLIENT_LIST:
            daten = b"".join(c.eintrag.kodieren() for c in self._clients.values())
            await v.sende_broker(K.VECTOR_BROKER_CONNECTED_CLIENT_LIST, daten)
        elif bp.vektor == K.VECTOR_BROKER_REQUEST_DYNAMIC_UIDS:
            antworten = []
            for a in P.lese_dyn_uid_anfragen(bp.daten):
                if not a.uid.ist_dynamisch_angefragt:
                    antworten.append((a.uid, a.rid, K.E133_DYN_UID_UNGUELTIG))
                else:
                    antworten.append((self._dyn_uid_vergeben(a.uid.hersteller & 0x7FFF, a.rid), a.rid,
                                      K.E133_DYN_UID_OK))
            await v.sende_broker(K.VECTOR_BROKER_ASSIGNED_DYNAMIC_UIDS, P.baue_dyn_uid_zuordnungen(antworten))
        elif bp.vektor == K.VECTOR_BROKER_FETCH_DYNAMIC_UID_LIST:
            rueck = {u: rid for (_h, rid), u in self._dyn_uid_je_rid.items()}
            antworten = [(u, rueck[u], K.E133_DYN_UID_OK) if u in rueck
                         else (u, uuid.UUID(int=0), K.E133_DYN_UID_NICHT_GEFUNDEN)
                         for u in P.lese_uid_liste(bp.daten)]
            await v.sende_broker(K.VECTOR_BROKER_ASSIGNED_DYNAMIC_UIDS, P.baue_dyn_uid_zuordnungen(antworten))
        elif bp.vektor == K.VECTOR_BROKER_DISCONNECT:
            self._ereignis("client_disconnect", uid=str(client.uid))
            await v.schliessen()
        else:
            self.zaehler["broker_vektor_ignoriert"] += 1

    async def _rpt_verarbeiten(self, client: _Client, rpt: P.RptPdu) -> None:
        k = rpt.kopf
        if k.quelle != client.uid:
            self.zaehler["rpt_falsche_quelle"] += 1
            self._ereignis("rpt_verworfen", grund="Quell-UID passt nicht zur Verbindung", uid=str(client.uid))
            return
        paket = P.baue_paket(K.VECTOR_ROOT_E133_RPT, self.konfig.cid, P.baue_rpt(k, rpt.daten))

        if k.vektor == K.VECTOR_RPT_REQUEST:
            if not client.ist_controller:
                self.zaehler["request_von_geraet_verworfen"] += 1
                return
            if ist_geraete_broadcast(k.ziel):
                mid = hersteller_aus_mid_broadcast(k.ziel)
                for c in list(self._clients.values()):
                    if not c.ist_controller and (mid is None or c.uid.hersteller & 0x7FFF == mid):
                        await self._sicher_senden(c, c.verbindung.senden_roh(paket))
                self.zaehler["rpt_broadcast"] += 1
                await self._status_an(client, k, K.RPT_STATUS_BROADCAST_COMPLETE)
                return
            if k.ziel == self.konfig.uid:
                await self._an_broker_responder(client, rpt)
                return
            ziel = self._clients.get(k.ziel)
            if ziel is None or ziel.ist_controller:
                self.zaehler["rpt_unbekannte_uid"] += 1
                await self._status_an(client, k, K.RPT_STATUS_UNKNOWN_RPT_UID)
                return
            if await self._sicher_senden(ziel, ziel.verbindung.senden_roh(paket)):
                self.zaehler["rpt_request_weitergeleitet"] += 1

        elif k.vektor in (K.VECTOR_RPT_NOTIFICATION, K.VECTOR_RPT_STATUS):
            if k.ziel == ALLE_CONTROLLER:
                for c in list(self._clients.values()):
                    if c.ist_controller:
                        await self._sicher_senden(c, c.verbindung.senden_roh(paket))
            else:
                ziel = self._clients.get(k.ziel)
                if ziel is None:
                    self.zaehler["rpt_antwort_ohne_ziel"] += 1
                    return
                await self._sicher_senden(ziel, ziel.verbindung.senden_roh(paket))
            self.zaehler["rpt_antwort_weitergeleitet"] += 1
        else:
            self.zaehler["rpt_unbekannter_vektor"] += 1
            await self._status_an(client, k, K.RPT_STATUS_UNKNOWN_VECTOR)

    async def _an_broker_responder(self, client: _Client, rpt: P.RptPdu) -> None:
        """RPT-Request an die Broker-UID: vom eigenen RDM-Responder beantworten (E1.33 6.2)."""
        k = rpt.kopf
        if k.ziel_endpoint != K.E133_NULL_ENDPOINT:
            await self._status_an(client, k, K.RPT_STATUS_UNKNOWN_ENDPOINT)
            return
        try:
            nachrichten = P.lese_request_oder_notification(rpt.daten, K.VECTOR_REQUEST_RDM_CMD)
            if len(nachrichten) != 1:
                raise P.ProtokollFehler("RPT-Request muss genau einen RDM-Befehl enthalten")
            roh = nachrichten[0]
            anfrage = RdmNachricht.dekodieren(roh)
        except (P.ProtokollFehler, RdmFormatFehler, ValueError):
            await self._status_an(client, k, K.RPT_STATUS_INVALID_MESSAGE)
            return
        if anfrage.ziel != self.konfig.uid and not anfrage.ziel.ist_broadcast:
            await self._status_an(client, k, K.RPT_STATUS_UNKNOWN_RDM_UID)
            return
        antwort = self.responder.beantworten(anfrage)
        self.zaehler["broker_rdm_anfrage"] += 1
        if antwort is None:
            return
        rkopf = P.RptKopf(K.VECTOR_RPT_NOTIFICATION, self.konfig.uid, K.E133_NULL_ENDPOINT, k.quelle,
                          k.quelle_endpoint, k.sequenz)
        await self._sicher_senden(client, client.verbindung.sende_rpt(
            rkopf, P.baue_notification([roh, antwort.kodieren()])))

    async def _status_an(self, client: _Client, k: P.RptKopf, code: int) -> None:
        antwort = P.RptKopf(K.VECTOR_RPT_STATUS, k.ziel, k.ziel_endpoint, k.quelle, k.quelle_endpoint, k.sequenz)
        await self._sicher_senden(client, client.verbindung.sende_rpt(antwort, P.baue_status(code)))

    # ----------------------------------------------------------------- Heartbeat
    async def _heartbeat_schleife(self) -> None:
        takt = min(1.0, self.konfig.heartbeat_intervall_s / 3)
        while True:
            await asyncio.sleep(takt)
            jetzt = time.monotonic()
            for c in list(self._clients.values()):
                v = c.verbindung
                if jetzt - v.letzter_empfang > self.konfig.heartbeat_timeout_s:
                    self.zaehler["heartbeat_timeout"] += 1
                    self._ereignis("heartbeat_timeout", uid=str(c.uid))
                    await v.schliessen()  # Lese-Schleife endet, Client wird entfernt
                elif jetzt - v.letztes_senden >= self.konfig.heartbeat_intervall_s:
                    await self._sicher_senden(c, v.sende_broker(K.VECTOR_BROKER_NULL))
