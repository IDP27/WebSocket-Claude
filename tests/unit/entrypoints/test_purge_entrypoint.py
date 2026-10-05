"""Processo ohip-purge: configuração, composição e códigos de saída (ADR-0020 §2)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from pydantic import ValidationError
from tests.fakes.oracle_driver import FakeOracle
from tests.unit.test_config import set_env

from ohip_streaming.adapters.oracle.purge_store import count_sql
from ohip_streaming.application.ports import PurgeTarget
from ohip_streaming.application.use_cases.purge import PurgeResult
from ohip_streaming.config import PurgeSettings, load_settings
from ohip_streaming.entrypoints import purge as entry

ENV = {
    "ORACLE_DSN": "db:1521/svc",
    "ORACLE_USER": "ohip_purge",
    "ORACLE_PASSWORD": "senha",
}


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> entry.PurgeConfig:
    set_env(monkeypatch, **ENV)
    load_settings.cache_clear()
    return entry.PurgeConfig.load()


def test_defaults_only_count(config: entry.PurgeConfig) -> None:
    options = entry.purge_options(config.purge)
    assert options.dry_run is True  # apagar exige PURGE_DRY_RUN=false
    assert (options.retention_days, options.batch_size, options.pause_s) == (90, 5000, 0.2)


@pytest.mark.parametrize(
    ("name", "value"),
    [("PURGE_RETENTION_DAYS", "7"), ("PURGE_BATCH_SIZE", "10"), ("PURGE_MAX_RUNTIME_S", "5")],
)
def test_settings_refuse_dangerous_values(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        PurgeSettings()


async def test_serve_counts_with_the_pool_and_closes_it(
    config: entry.PurgeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = FakeOracle()
    for target in PurgeTarget:
        pool.rows(count_sql(target), [(3,)])
    closed: list[bool] = []
    monkeypatch.setattr(entry, "open_pool", lambda settings: pool)
    monkeypatch.setattr(pool, "close", lambda force=False: closed.append(force))
    results = await entry.serve(config)
    assert [(r.target, r.rows) for r in results] == [(t, 3) for t in PurgeTarget]
    assert closed == [True]
    assert "commit" not in pool.kinds()  # dry run: nada apagado


@pytest.mark.parametrize(("error", "code"), [(None, 0), (RuntimeError("ORA-01031"), 1)])
def test_main_exit_codes(
    config: entry.PurgeConfig,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception | None,
    code: int,
) -> None:
    async def serve(_: Any) -> list[PurgeResult]:
        if error is not None:
            raise error
        return [PurgeResult(t, 0, complete=True) for t in PurgeTarget]

    monkeypatch.setattr(entry, "serve", serve)
    assert entry.main() == code
