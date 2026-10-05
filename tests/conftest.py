"""Fixtures compartilhadas."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from ohip_streaming.config import load_settings

_PREFIXES = (
    "APP_",
    "OHIP_",
    "ORACLE_",
    "RABBITMQ_",
    "REDIS_",
    "CONSUMER_",
    "API_",
    "LOG_",
    "LEASE_",
    "PUBLISHER_",
)


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Cada teste roda sem as variáveis do projeto e fora da raiz (nenhum .env real é lido)."""
    for name in list(os.environ):
        if name.upper().startswith(_PREFIXES):
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)
    load_settings.cache_clear()
    yield
    load_settings.cache_clear()
