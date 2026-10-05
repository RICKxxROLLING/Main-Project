"""Hand-built packets and a simulated Atmotube for tests."""

from __future__ import annotations

import struct
from collections import deque

from custom_components.atmo_history.protocol import (
    TransferAborted,
    build_hok,
)

DISCONNECT = object()


def ht(first: int, count: int, size: int = 16, order: str = "big") -> bytes:
    """Build an HT packet."""
    return b"HT\x00" + first.to_bytes(4, order) + bytes([count, size])


def hd(number: int, *records: bytes) -> bytes:
    """Build an HD packet."""
    return b"HD\x00" + bytes([number]) + b"".join(records)


def record(
    temp: int = 21,
    hum: int = 45,
    voc: int = 120,
    pressure: int = 101325,
    pm1: int = 3,
    pm25: int = 5,
    pm10: int = 7,
    pad: bytes = b"\x00\x00",
) -> bytes:
    """Build one little-endian record (16 bytes by default)."""
    return struct.pack("<bBHIHHH", temp, hum, voc, pressure, pm1, pm25, pm10) + pad


def simple_batch(first: int, values: list[int], **kwargs: int) -> list[bytes]:
    """One record per HD packet, temperature taken from ``values``."""
    packets = [ht(first, len(values))]
    packets += [hd(i + 1, record(temp=v, **kwargs)) for i, v in enumerate(values)]
    return packets


class FakeAtmotube:
    """Simulates the device side of the UART history protocol.

    Each batch is a list of packets (HT first). A batch is only dropped from
    the device after an HOK acknowledgement. A packet may be DISCONNECT.
    """

    def __init__(
        self,
        batches: list[list[object]],
        reply_hok: bool = True,
        respond: bool = True,
    ) -> None:
        self.batches = deque(batches)
        self.reply_hok = reply_hok
        self.respond = respond
        self.writes: list[bytes] = []
        self.acks = 0
        self._queue: deque[object] = deque()

    @property
    def ack_writes(self) -> list[bytes]:
        return [w for w in self.writes if w.startswith(b"HOK")]

    async def write(self, data: bytes) -> None:
        self.writes.append(data)
        if data.startswith(b"HST"):
            if not self.respond:
                return
            if self.reply_hok:
                self._queue.append(build_hok(0)[:3])
            self._queue_next()
        elif data.startswith(b"HOK"):
            self.acks += 1
            self.batches.popleft()
            self._queue_next()

    def _queue_next(self) -> None:
        if self.batches:
            self._queue.extend(self.batches[0])

    async def receive(self, timeout: float) -> bytes:
        if not self._queue:
            raise TimeoutError
        item = self._queue.popleft()
        if item is DISCONNECT:
            raise TransferAborted("Device disconnected")
        assert isinstance(item, bytes)
        return item
