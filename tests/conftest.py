"""Shared fixtures."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from homeassistant.components.recorder import Recorder


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: Recorder,
    enable_custom_integrations: None,
    mock_bleak_scanner_start: MagicMock,
) -> None:
    """Every test gets an in-memory recorder (a manifest dependency)."""


@pytest.fixture(autouse=True)
def quiet_sqlalchemy() -> None:
    """Keep the in-memory recorder's SQL echo out of test output."""
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
