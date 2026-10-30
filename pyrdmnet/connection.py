"""Gemeinsame TCP-Verbindung für Broker und Clients (Senden mit Sperre, Zeitstempel)."""

from __future__ import annotations

import asyncio
import time
import uuid

from . import constants as C
from . import pdu as P


class TcpConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, own_cid: uuid.UUID):
        self.reader = reader
        self.writer = writer
        self.own_cid = own_cid
        self._lock = asyncio.Lock()
        self.last_received = time.monotonic()
        self.last_sent = time.monotonic()
        self.closed = False
        self.capture = None  # optional: Funktion(richtung "ein"/"aus", rohdaten)
        try:
            peer = writer.get_extra_info("peername")
            self.peer = f"{peer[0]}:{peer[1]}" if peer else "?"
        except Exception:  # pragma: no cover - plattformabhängig
            self.peer = "?"

    async def send_raw(self, packet: bytes) -> None:
        if self.closed:
            raise ConnectionError("Verbindung geschlossen")
        async with self._lock:
            if self.capture:
                self.capture("aus", packet)
            self.writer.write(packet)
            await self.writer.drain()
            self.last_sent = time.monotonic()

    async def send_broker(self, vector: int, data: bytes = b"") -> None:
        await self.send_raw(P.build_packet(C.VECTOR_ROOT_E133_BROKER, self.own_cid, P.build_broker(vector, data)))

    async def send_rpt(self, header: P.RptHeader, data: bytes) -> None:
        await self.send_raw(P.build_packet(C.VECTOR_ROOT_E133_RPT, self.own_cid, P.build_rpt(header, data)))

    async def read(self) -> list[P.RootPdu]:
        block = await P.read_tcp_block(self.reader)
        self.last_received = time.monotonic()
        if self.capture:
            self.capture("ein", block)
        return P.read_root_pdus(block)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
        except Exception:
            pass
