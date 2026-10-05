"""Atmotube PRO history protocol over the Nordic UART service.

This module has no Home Assistant or bleak imports so it can be tested in
isolation. The BLE transport is abstracted behind :class:`Transport`.

Flow (Atmotube "Bluetooth API" article, confirmed against the atmotuber
Flutter package):

1. Write ``HST`` + uint32 Unix time. The device answers ``HOK``.
2. If it has unsynced history it sends ``HT``:
   ``"HT", 0x00, uint32 first-record time, uint8 packet count, uint8 record size``
3. Then ``packet count`` ``HD`` packets:
   ``"HD", 0x00, uint8 packet number, records...``
4. After a complete batch, write ``HOK`` + uint32 Unix time. The device marks
   the batch as synced and sends the next ``HT`` if there is more.
   There is no end-of-history packet; silence means done.

Things that are NOT documented by Atmotube and are handled defensively:

* Timestamp byte order. atmotuber encodes/decodes it big-endian, while the
  record fields are little-endian. ``TIMESTAMP_BYTEORDER`` holds the choice
  and implausible header times abort the transfer without an ACK.
* The interval between records (atmotuber assumes 60 s). The caller supplies
  it and is expected to verify it from consecutive ``HT`` headers.
* Records per ``HD`` packet. atmotuber reads one; we split each payload by
  the record size from ``HT``.
* Packet numbering base. atmotuber implies 1..N; 0..N-1 is also accepted.
* Record size. The documented layout is 14 bytes, but atmotuber reports 16;
  trailing bytes are kept in ``HistoryRecord.extra``.
"""

from __future__ import annotations

import logging
import struct
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

_LOGGER = logging.getLogger(__name__)

UART_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
# Named from the device's point of view, as in Atmotube's Android library.
UART_RX_CHAR_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"  # we write here
UART_TX_CHAR_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # we get notified here

TIMESTAMP_BYTEORDER = "big"

RECORD_STRUCT = struct.Struct("<bBHIHHH")
RECORD_MIN_SIZE = RECORD_STRUCT.size  # 14
PM_NOT_AVAILABLE = 0xFFFF

HT_LENGTH = 9
HD_HEADER_LENGTH = 4

# 2016-01-01T00:00:00Z, before any Atmotube PRO shipped.
MIN_PLAUSIBLE_TIMESTAMP = 1451606400
MAX_FUTURE_SKEW = 86400

KIND_HOK = "HOK"
KIND_HT = "HT"
KIND_HD = "HD"


class ProtocolError(Exception):
    """The device sent something we cannot safely interpret."""


class TransferIncomplete(ProtocolError):
    """A batch was started but not finished."""


class NoResponse(ProtocolError):
    """The device never answered the HST request."""


class TransferAborted(Exception):
    """The transport went away (e.g. BLE disconnect)."""


def encode_timestamp(timestamp: int) -> bytes:
    """Encode a Unix time the way the device expects it."""
    return int(timestamp).to_bytes(4, TIMESTAMP_BYTEORDER, signed=False)


def decode_timestamp(data: bytes) -> int:
    """Decode a 4-byte Unix time from the device."""
    return int.from_bytes(data, TIMESTAMP_BYTEORDER, signed=False)


def build_hst(now: int) -> bytes:
    """Build the history request command."""
    return b"HST" + encode_timestamp(now)


def build_hok(now: int) -> bytes:
    """Build the batch acknowledgement command."""
    return b"HOK" + encode_timestamp(now)


def packet_kind(data: bytes) -> str | None:
    """Classify a notification from the TX characteristic."""
    if data.startswith(b"HO"):
        return KIND_HOK
    if data.startswith(b"HT"):
        return KIND_HT
    if data.startswith(b"HD"):
        return KIND_HD
    return None


@dataclass(frozen=True, slots=True)
class HistoryHeader:
    """Parsed HT packet."""

    first_timestamp: int
    packet_count: int
    record_size: int


@dataclass(frozen=True, slots=True)
class HistoryDataPacket:
    """Parsed HD packet."""

    number: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class HistoryRecord:
    """One decoded measurement."""

    timestamp: int
    temperature: int  # °C
    humidity: int  # %
    voc: int  # ppb
    pressure: int  # Pa (the device sends mbar * 100)
    pm1: int | None  # µg/m³, None when the PM sensor was off
    pm25: int | None
    pm10: int | None
    extra: bytes = b""

    def values(self) -> dict[str, float]:
        """Return the metrics that have a value, keyed by metric name."""
        out: dict[str, float] = {
            "temperature": self.temperature,
            "humidity": self.humidity,
            "voc": self.voc,
            "pressure": self.pressure,
        }
        for key in ("pm1", "pm25", "pm10"):
            if (value := getattr(self, key)) is not None:
                out[key] = value
        return out


def parse_ht(data: bytes) -> HistoryHeader:
    """Parse an HT packet."""
    if len(data) < HT_LENGTH or not data.startswith(b"HT"):
        raise ProtocolError(f"Malformed HT packet: {data.hex()}")
    header = HistoryHeader(
        first_timestamp=decode_timestamp(data[3:7]),
        packet_count=data[7],
        record_size=data[8],
    )
    if header.record_size < RECORD_MIN_SIZE:
        raise ProtocolError(
            f"HT record size {header.record_size} is smaller than the "
            f"documented {RECORD_MIN_SIZE}-byte layout"
        )
    return header


def parse_hd(data: bytes) -> HistoryDataPacket:
    """Parse an HD packet."""
    if len(data) < HD_HEADER_LENGTH or not data.startswith(b"HD"):
        raise ProtocolError(f"Malformed HD packet: {data.hex()}")
    return HistoryDataPacket(number=data[3], payload=bytes(data[4:]))


def check_timestamp_plausible(timestamp: int, now: int) -> None:
    """Refuse header times that point at a byte-order or clock problem."""
    if MIN_PLAUSIBLE_TIMESTAMP <= timestamp <= now + MAX_FUTURE_SKEW:
        return
    raw = timestamp.to_bytes(4, TIMESTAMP_BYTEORDER)
    other = "little" if TIMESTAMP_BYTEORDER == "big" else "big"
    alternative = int.from_bytes(raw, other)
    hint = ""
    if MIN_PLAUSIBLE_TIMESTAMP <= alternative <= now + MAX_FUTURE_SKEW:
        hint = (
            f"; read as {other}-endian it would be {alternative}, which is "
            "plausible, so the timestamp byte order is probably wrong"
        )
    raise ProtocolError(
        f"Implausible first-record time {timestamp} ({raw.hex()}) decoded as "
        f"{TIMESTAMP_BYTEORDER}-endian{hint}"
    )


def decode_record(chunk: bytes, timestamp: int) -> HistoryRecord:
    """Decode one record. ``chunk`` may be longer than the known layout."""
    temp, hum, voc, pressure, pm1, pm25, pm10 = RECORD_STRUCT.unpack_from(chunk)
    return HistoryRecord(
        timestamp=timestamp,
        temperature=temp,
        humidity=hum,
        voc=voc,
        pressure=pressure,
        pm1=None if pm1 == PM_NOT_AVAILABLE else pm1,
        pm25=None if pm25 == PM_NOT_AVAILABLE else pm25,
        pm10=None if pm10 == PM_NOT_AVAILABLE else pm10,
        extra=bytes(chunk[RECORD_MIN_SIZE:]),
    )


@dataclass
class HistoryBatch:
    """One HT header plus its HD packets."""

    header: HistoryHeader
    packets: dict[int, bytes] = field(default_factory=dict)

    def add(self, packet: HistoryDataPacket) -> None:
        """Add an HD packet, rejecting anything inconsistent."""
        if packet.number in self.packets:
            raise ProtocolError(f"Duplicate HD packet number {packet.number}")
        if packet.number > self.header.packet_count:
            raise ProtocolError(
                f"HD packet number {packet.number} exceeds the "
                f"{self.header.packet_count} announced in HT"
            )
        if len(packet.payload) % self.header.record_size:
            raise ProtocolError(
                f"HD packet {packet.number} payload of {len(packet.payload)} "
                f"bytes is not a multiple of the record size "
                f"{self.header.record_size}"
            )
        self.packets[packet.number] = packet.payload

    @property
    def complete(self) -> bool:
        """Return True once every announced packet has arrived."""
        count = self.header.packet_count
        if len(self.packets) < count:
            return False
        numbers = set(self.packets)
        if numbers in (set(range(1, count + 1)), set(range(count))):
            return True
        raise ProtocolError(
            f"HD packet numbers {sorted(numbers)} do not form a 0- or 1-based sequence of {count}"
        )

    @property
    def record_count(self) -> int:
        """Number of records received so far."""
        return sum(len(p) for p in self.packets.values()) // self.header.record_size

    def payload(self) -> bytes:
        """Concatenate packet payloads in packet-number order."""
        return b"".join(self.packets[n] for n in sorted(self.packets))

    def records(self, interval: int) -> list[HistoryRecord]:
        """Decode all records, timestamped ``interval`` seconds apart."""
        if not self.complete:
            raise TransferIncomplete("Batch is not complete")
        data = self.payload()
        size = self.header.record_size
        first = self.header.first_timestamp
        return [
            decode_record(data[offset : offset + size], first + index * interval)
            for index, offset in enumerate(range(0, len(data), size))
        ]


class Transport(Protocol):
    """Minimal async UART transport."""

    async def write(self, data: bytes) -> None:
        """Write a command to the device."""

    async def receive(self, timeout: float) -> bytes:
        """Return the next notification.

        Raises TimeoutError when nothing arrives in time and
        TransferAborted when the link is lost.
        """


@dataclass(frozen=True, slots=True)
class Timeouts:
    """Per-step timeouts in seconds."""

    response: float = 10.0  # HST -> first reply
    header: float = 15.0  # waiting for the next HT
    packet: float = 10.0  # gap between HD packets


DEFAULT_TIMEOUTS = Timeouts()


@dataclass(slots=True)
class TransferResult:
    """Outcome of a transfer."""

    batches_acked: int = 0
    records_acked: int = 0
    stopped_early: bool = False


BatchHandler = Callable[[HistoryBatch], Awaitable[bool]]


async def run_history_transfer(
    transport: Transport,
    on_batch: BatchHandler,
    now: Callable[[], int],
    timeouts: Timeouts = DEFAULT_TIMEOUTS,
    max_batches: int = 1000,
) -> TransferResult:
    """Download history, calling ``on_batch`` for every complete batch.

    ``on_batch`` must persist the batch and return True before we ACK it.
    Returning False stops the transfer without an ACK. Raising aborts it
    without an ACK. Partial batches are never passed to ``on_batch``.
    """
    result = TransferResult()
    got_reply = False
    batch: HistoryBatch | None = None

    await transport.write(build_hst(now()))

    while True:
        if batch is not None:
            timeout = timeouts.packet
        elif got_reply:
            timeout = timeouts.header
        else:
            timeout = timeouts.response
        try:
            data = await transport.receive(timeout)
        except TimeoutError:
            if batch is not None:
                raise TransferIncomplete(
                    f"Timed out after {len(batch.packets)} of "
                    f"{batch.header.packet_count} HD packets"
                ) from None
            if not got_reply:
                raise NoResponse("No reply to HST") from None
            return result

        kind = packet_kind(data)
        _LOGGER.debug("RX %s: %s", kind or "?", data.hex())

        if kind == KIND_HOK:
            got_reply = True
            continue
        if kind == KIND_HT:
            got_reply = True
            if batch is not None:
                raise TransferIncomplete(
                    f"New HT after {len(batch.packets)} of {batch.header.packet_count} HD packets"
                )
            header = parse_ht(data)
            check_timestamp_plausible(header.first_timestamp, now())
            _LOGGER.debug(
                "HT: first=%s packets=%s record_size=%s",
                header.first_timestamp,
                header.packet_count,
                header.record_size,
            )
            batch = HistoryBatch(header)
        elif kind == KIND_HD:
            if batch is None:
                _LOGGER.debug("Ignoring HD packet received before HT")
                continue
            batch.add(parse_hd(data))
        else:
            _LOGGER.debug("Ignoring unknown packet")
            continue

        if batch is None or not batch.complete:
            continue

        if not await on_batch(batch):
            result.stopped_early = True
            return result
        await transport.write(build_hok(now()))
        _LOGGER.debug("ACKed batch starting %s", batch.header.first_timestamp)
        result.batches_acked += 1
        result.records_acked += batch.record_count
        batch = None
        if result.batches_acked >= max_batches:
            return result
