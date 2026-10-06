"""Contadores de alerta registrados com 0 na partida (ADR-0020 §6, achado da revisão)."""

from __future__ import annotations

from ohip_streaming.adapters.redis.metrics import InMemoryMetrics
from ohip_streaming.entrypoints import consumer, enricher, publisher
from ohip_streaming.entrypoints.common import preregister


def test_preregistered_counters_appear_with_zero_and_keep_counting() -> None:
    metrics = InMemoryMetrics()
    preregister(metrics, consumer.alert_counters("CHAIN1"))
    snapshot = {
        (c["name"], tuple(sorted(c["labels"].items()))): c["value"] for c in metrics.snapshot()
    }
    poisoned = ("ohip_connection_poisoned_total", (("chain_code", "CHAIN1"),))
    assert snapshot[poisoned] == 0  # a série existe antes da primeira ocorrência
    assert snapshot[("ohip_lease_lost_total", (("lease", "consumer:CHAIN1"),))] == 0

    metrics.increment("ohip_connection_poisoned_total", chain_code="CHAIN1")  # mesmo rótulo
    after = {
        (c["name"], tuple(sorted(c["labels"].items()))): c["value"] for c in metrics.snapshot()
    }
    assert after[poisoned] == 1


def test_each_process_registers_its_alert_counters() -> None:
    assert ("ohip_lease_lost_total", {"lease": "publisher"}) in publisher.alert_counters()
    stages = {
        labels["stage"]
        for name, labels in enricher.alert_counters()
        if name == "ohip_enricher_systemic_failures_total"
    }
    assert stages == {"NORMALIZE", "ENRICH"}
