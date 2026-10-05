"""Bleak transport for the history protocol."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection

from .protocol import (
    UART_RX_CHAR_UUID,
    UART_TX_CHAR_UUID,
    BatchHandler,
    ProtocolError,
    TransferAborted,
    TransferResult,
    run_history_transfer,
)

_LOGGER = logging.getLogger(__name__)


class BleakUartTransport:
    """Nordic UART transport on top of a connected BleakClient."""

    def __init__(self) -> None:
        """Initialize."""
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._client: BleakClient | None = None
        self._response = False

    def on_disconnect(self, _client: BleakClient | None = None) -> None:
        """Wake any pending receive with a disconnect marker."""
        self._queue.put_nowait(None)

    def _on_notify(self, _char: object, data: bytearray) -> None:
        self._queue.put_nowait(bytes(data))

    async def start(self, client: BleakClient) -> None:
        """Subscribe to the TX characteristic."""
        self._client = client
        rx_char = client.services.get_characteristic(UART_RX_CHAR_UUID)
        if rx_char is None or client.services.get_characteristic(UART_TX_CHAR_UUID) is None:
            raise ProtocolError("Nordic UART service not found on device")
        self._response = "write-without-response" not in rx_char.properties
        await client.start_notify(UART_TX_CHAR_UUID, self._on_notify)

    async def write(self, data: bytes) -> None:
        """Write a command."""
        assert self._client is not None
        _LOGGER.debug("TX: %s", data.hex())
        try:
            await self._client.write_gatt_char(UART_RX_CHAR_UUID, data, response=self._response)
        except BleakError as err:
            raise TransferAborted(f"Write failed: {err}") from err

    async def receive(self, timeout: float) -> bytes:
        """Return the next notification."""
        async with asyncio.timeout(timeout):
            item = await self._queue.get()
        if item is None:
            raise TransferAborted("Device disconnected")
        return item


async def async_download_history(
    ble_device: BLEDevice, name: str, on_batch: BatchHandler
) -> TransferResult:
    """Connect, run the history transfer and disconnect."""
    transport = BleakUartTransport()
    client = await establish_connection(
        BleakClientWithServiceCache,
        ble_device,
        name,
        disconnected_callback=transport.on_disconnect,
        max_attempts=3,
    )
    try:
        await transport.start(client)
        return await run_history_transfer(transport, on_batch, now=lambda: int(time.time()))
    finally:
        with suppress(BleakError, TimeoutError):
            await client.disconnect()
