"""Fixture ``backend``: cada cenário roda no fake em memória e, se configurado, no PostgreSQL
de teste (``make test-db``)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from tests.stores.backends import (
    Backend,
    MemoryBackend,
    PostgresBackend,
    postgres_test_settings,
)

_postgres_settings = postgres_test_settings()


@pytest.fixture(
    params=[
        "memory",
        pytest.param(
            "postgres",
            marks=[
                pytest.mark.postgres,
                pytest.mark.skipif(
                    _postgres_settings is None,
                    reason="PostgreSQL de teste não configurado (make test-db; "
                    "TEST_POSTGRES_DSN e TEST_POSTGRES_DISPOSABLE=sim)",
                ),
            ],
        ),
    ]
)
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    if request.param == "memory":
        yield MemoryBackend()
        return
    assert _postgres_settings is not None
    postgres = PostgresBackend.open(_postgres_settings)
    try:
        yield postgres
    finally:
        postgres.close()
