"""Gemeinsame TCP-Verbindung für Broker und Clients (Senden mit Sperre, Zeitstempel)."""

from __future__ import annotations

import asyncio
import time
import uuid

from . import konstanten as K
from . import pdu as P


class Verbindung:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, eigene_cid: uuid.UUID):
        self.reader = reader
        self.writer = writer
        self.eigene_cid = eigene_cid
        self._sperre = asyncio.Lock()
        self.letzter_empfang = time.monotonic()
        self.letztes_senden = time.monotonic()
        self.geschlossen = False
        self.mitschnitt = None  # optional: Funktion(richtung "ein"/"aus", rohdaten)
        try:
            peer = writer.get_extra_info("peername")
            self.gegenstelle = f"{peer[0]}:{peer[1]}" if peer else "?"
        except Exception:  # pragma: no cover - plattformabhängig
            self.gegenstelle = "?"

    async def senden_roh(self, paket: bytes) -> None:
        if self.geschlossen:
            raise ConnectionError("Verbindung geschlossen")
        async with self._sperre:
            if self.mitschnitt:
                self.mitschnitt("aus", paket)
            self.writer.write(paket)
            await self.writer.drain()
            self.letztes_senden = time.monotonic()

    async def sende_broker(self, vektor: int, daten: bytes = b"") -> None:
        await self.senden_roh(P.baue_paket(K.VECTOR_ROOT_E133_BROKER, self.eigene_cid, P.baue_broker(vektor, daten)))

    async def sende_rpt(self, kopf: P.RptKopf, daten: bytes) -> None:
        await self.senden_roh(P.baue_paket(K.VECTOR_ROOT_E133_RPT, self.eigene_cid, P.baue_rpt(kopf, daten)))

    async def lese(self) -> list[P.RootPdu]:
        block = await P.lese_tcp_block(self.reader)
        self.letzter_empfang = time.monotonic()
        if self.mitschnitt:
            self.mitschnitt("ein", block)
        return P.lese_root_pdus(block)

    async def schliessen(self) -> None:
        if self.geschlossen:
            return
        self.geschlossen = True
        try:
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
        except Exception:
            pass
