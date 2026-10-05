"""Diagnostic sensors for Atmotube history sync."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import CONNECTION_BLUETOOTH, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import RESULTS, SIGNAL_UPDATED
from .manager import AtmoHistoryConfigEntry, AtmoHistoryManager


@dataclass(frozen=True, kw_only=True)
class AtmoHistorySensorDescription(SensorEntityDescription):
    """Describes a diagnostic sensor."""

    value_fn: Callable[[AtmoHistoryManager], Any]
    attrs_fn: Callable[[AtmoHistoryManager], dict[str, Any]] | None = None


SENSORS = (
    AtmoHistorySensorDescription(
        key="last_sync",
        translation_key="last_sync",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda m: m.status.last_success,
        attrs_fn=lambda m: {"last_attempt": m.status.last_attempt},
    ),
    AtmoHistorySensorDescription(
        key="records_imported",
        translation_key="records_imported",
        value_fn=lambda m: m.status.last_records,
        attrs_fn=lambda m: {"held_batches": m.pending_batches},
    ),
    AtmoHistorySensorDescription(
        key="last_result",
        translation_key="last_result",
        device_class=SensorDeviceClass.ENUM,
        options=RESULTS,
        value_fn=lambda m: m.status.last_result,
        attrs_fn=lambda m: {
            "error": m.status.last_error,
            "record_interval": m.interval,
            "interval_confirmed": m.interval_confirmed,
        },
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AtmoHistoryConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up sensors."""
    async_add_entities(
        AtmoHistorySensor(entry.runtime_data, description) for description in SENSORS
    )


class AtmoHistorySensor(SensorEntity):
    """A diagnostic sensor fed by the manager."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description: AtmoHistorySensorDescription

    def __init__(
        self, manager: AtmoHistoryManager, description: AtmoHistorySensorDescription
    ) -> None:
        """Initialize."""
        self._manager = manager
        self.entity_description = description
        self._attr_unique_id = f"{manager.address}_{description.key}"
        self._attr_device_info = DeviceInfo(
            connections={(CONNECTION_BLUETOOTH, manager.address)},
            name=manager.name,
            manufacturer="Atmotube",
            model="Atmotube PRO",
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe to updates."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                SIGNAL_UPDATED.format(self._manager.entry.entry_id),
                self._handle_update,
            )
        )

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def native_value(self) -> Any:
        """Return the value."""
        return self.entity_description.value_fn(self._manager)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return extra attributes."""
        if self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(self._manager)
