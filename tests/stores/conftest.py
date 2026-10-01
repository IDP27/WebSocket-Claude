"""Fixture ``backend``: cada cenário roda no fake em memória e, se configurado, no Oracle."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from tests.stores.backends import Backend, MemoryBackend, OracleBackend, oracle_test_settings

_oracle_settings = oracle_test_settings()


@pytest.fixture(
    params=[
        "memory",
        pytest.param(
            "oracle",
            marks=[
                pytest.mark.oracle,
                pytest.mark.skipif(
                    _oracle_settings is None,
                    reason="Oracle de teste não configurado (TEST_ORACLE_* e "
                    "TEST_ORACLE_DISPOSABLE_SCHEMA=sim)",
                ),
            ],
        ),
    ]
)
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    if request.param == "memory":
        yield MemoryBackend()
        return
    assert _oracle_settings is not None
    oracle = OracleBackend.open(_oracle_settings)
    try:
        yield oracle
    finally:
        oracle.close()
