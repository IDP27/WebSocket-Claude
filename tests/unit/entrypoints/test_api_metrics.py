"""Texto Prometheus do ``/metrics`` (ARCHITECTURE §10, ADR-0017)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ohip_streaming.adapters.redis.api_cache import ProcessSnapshot
from ohip_streaming.application.ports import (
    ChainStatus,
    DlqStage,
    OutboxCounts,
    StatusSnapshot,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.offset import Offset
from ohip_streaming.entrypoints.api.metrics import render

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def chain(**overrides: object) -> ChainStatus:
    values: dict[str, object] = {
        "chain_code": "C1",
        "state": ConsumerState.SUBSCRIBED,
        "instance_id": "vm1",
        "subscription_id": None,
        "last_offset": Offset("97863"),
        "last_message_at": NOW - timedelta(seconds=2),
        "lag_seconds_p95_5m": 1.5,
        "events_last_5m": 10,
        "reconnects_total": 4,
        "consecutive_failures": 0,
        "next_attempt_at": None,
        "last_close_code": None,
        "last_close_reason": None,
        "last_disconnect_at": None,
        "token_expires_at": None,
        "outbox": OutboxCounts(2, 1, NOW - timedelta(seconds=60)),
        "dlq_open": {DlqStage.ENRICH: 3},
    }
    values.update(overrides)
    return ChainStatus(**values)  # type: ignore[arg-type]


def test_chain_gauges() -> None:
    text = render(StatusSnapshot(NOW, (chain(),)), [])
    lines = text.splitlines()
    assert 'ohip_consumer_state{chain_code="C1",state="SUBSCRIBED"} 1' in lines
    assert 'ohip_consumer_state{chain_code="C1",state="STOPPED"} 0' in lines
    assert 'ohip_consumer_last_offset{chain_code="C1"} 97863' in lines
    assert 'ohip_consumer_last_message_age_seconds{chain_code="C1"} 2' in lines
    assert 'ohip_consumer_lag_seconds_p95_5m{chain_code="C1"} 1.5' in lines
    assert 'ohip_consumer_events_per_minute{chain_code="C1"} 2' in lines
    assert 'ohip_outbox_oldest_pending_seconds{chain_code="C1"} 60' in lines
    assert 'ohip_dlq_open{chain_code="C1",stage="ENRICH"} 3' in lines
    assert 'ohip_dlq_open{chain_code="C1",stage="CONSUME"} 0' in lines
    assert "# TYPE ohip_consumer_reconnects_total counter" in lines
    assert 'ohip_consumer_last_close_code{chain_code="C1"}' not in text  # sem valor, sem linha
    assert text.endswith("\n")
    # cada família aparece uma vez, com HELP e TYPE antes das amostras
    assert text.count("# TYPE ohip_consumer_state gauge") == 1


def test_process_counters_and_escaping() -> None:
    snapshot = ProcessSnapshot(
        "consumer",
        'vm"1\\x',
        (
            {"name": "ohip_events_total", "labels": {"chain_code": "C1"}, "value": 5},
            {"name": "nome inválido", "labels": {}, "value": 1},
            {"name": "ohip_x_total", "labels": {"rótulo-ruim": "a"}, "value": 1},
            {"name": "ohip_y_total", "labels": "não é dict", "value": 1},
            {"name": "ohip_z_total", "labels": {}, "value": "texto"},
        ),
    )
    text = render(None, [snapshot])
    assert 'ohip_events_total{chain_code="C1",process="consumer",instance="vm\\"1\\\\x"} 5' in text
    for dropped in ("nome inválido", "ohip_x_total", "ohip_y_total", "ohip_z_total"):
        assert dropped not in text
    assert 'ohip_metrics_source_up{source="oracle"} 0' in text
    assert 'ohip_metrics_source_up{source="redis"} 1' in text


def test_conflicting_kind_is_dropped() -> None:
    snapshot = ProcessSnapshot(
        "consumer", "vm1", ({"name": "ohip_outbox_pending", "labels": {}, "value": 9},)
    )
    text = render(StatusSnapshot(NOW, (chain(),)), [snapshot])
    assert "# TYPE ohip_outbox_pending gauge" in text
    assert 'process="consumer"' not in text


def test_all_sources_down() -> None:
    text = render(None, None)
    assert "ohip_api_up 1" in text
    assert 'ohip_metrics_source_up{source="redis"} 0' in text


def test_float_formatting() -> None:
    text = render(StatusSnapshot(NOW, (chain(lag_seconds_p95_5m=float("nan")),)), [])
    assert 'ohip_consumer_lag_seconds_p95_5m{chain_code="C1"} NaN' in text
    text = render(StatusSnapshot(NOW, (chain(lag_seconds_p95_5m=float("inf")),)), [])
    assert 'ohip_consumer_lag_seconds_p95_5m{chain_code="C1"} +Inf' in text
