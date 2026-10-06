"""API pública de ``application.ports`` (refatoração 3 do /entender, caracterização).

Escrito antes de dividir o módulo em pacote: todo nome importado hoje por casos de uso,
adapters, entrypoints e testes continua disponível em ``ohip_streaming.application.ports``.
"""

from __future__ import annotations

import inspect
from typing import Protocol

import ohip_streaming.application.ports as ports

PUBLIC = {
    "AccessToken", "BatchResult", "BatchToPersist", "ChainStatus", "Clock", "ConnectionHealth",
    "ConsumeDlqRecord", "ConsumeRetryItem", "ConsumerStatusStore", "DisconnectSnapshot",
    "DlqEntry", "DlqItem", "DlqQuery", "DlqStage", "DomainWrite", "EnrichmentStore",
    "EventFilter", "EventRecord", "EventStore", "EventSummary", "ItemT", "LeaseStore",
    "MessagePublisher", "MetricsSink", "MonitoringStore", "NewEventRecord", "NormalizationRule",
    "OperationsStore", "OutboxCounts", "OutboxEntry", "OutboxQuery", "OutboxRow", "OutboxStore",
    "OutgoingMessage", "PURGE_ORDER", "Page", "PageRequest", "ProcessingStatus",
    "PublishOutcome", "PurgeStore", "PurgeTarget", "QueueDedup", "ReplayEntry", "ReplayQuery",
    "ReplayRequest", "ReplayStatus", "ReplayStore", "ResourceFetcher", "RowFailure",
    "SeenCache", "StatusSnapshot", "StoredEvent", "TokenCache", "TokenIssuer", "WsConnection",
    "WsConnector",
}  # fmt: skip

# Ports (protocolos) e os métodos de cada um: assinatura pública que os adapters implementam.
PROTOCOL_METHODS = {
    name: sorted(n for n, _ in inspect.getmembers(getattr(ports, name)) if not n.startswith("_"))
    for name in sorted(PUBLIC)
    if isinstance(getattr(ports, name, None), type)
    and issubclass(getattr(ports, name), Protocol)  # type: ignore[arg-type]
    and getattr(ports, name) is not Protocol
}


def test_every_public_name_is_still_importable() -> None:
    missing = sorted(name for name in PUBLIC if not hasattr(ports, name))
    assert missing == []


EXPECTED_PROTOCOLS = {
    "Clock": ["now", "sleep"],
    "ConsumerStatusStore": [
        "disconnect_snapshot",
        "record_disconnect",
        "record_health",
        "record_state",
        "record_subscribed",
    ],
    "EnrichmentStore": ["add_dlq", "apply", "get_event"],
    "EventStore": [
        "mark_consume_retry_failed",
        "pending_consume_retries",
        "persist_batch",
        "reserve_event_ids",
    ],
    "LeaseStore": ["acquire", "release", "renew"],
    "MessagePublisher": ["publish"],
    "MetricsSink": ["increment"],
    "MonitoringStore": [
        "dlq",
        "event",
        "events",
        "oldest_pending_age_s",
        "outbox",
        "ping",
        "replay",
        "replays",
        "status",
    ],
    "NormalizationRule": ["build", "event_names", "needs_resource"],
    "OperationsStore": [
        "enqueue",
        "enqueue_and_resolve",
        "find_event",
        "get_dlq_item",
        "get_event",
        "request_consume_retry",
        "retry_publish",
    ],
    "OutboxStore": [
        "chains_with_pending",
        "fetch_head",
        "mark_failed",
        "mark_sent",
        "record_failure",
    ],
    "PurgeStore": ["count_expired", "purge_batch"],
    "QueueDedup": ["is_processed", "mark_processed"],
    "ReplayStore": [
        "apply_request",
        "cancel_request",
        "create_request",
        "last_offset",
        "offset_received_at",
        "pending_request",
    ],
    "ResourceFetcher": ["fetch"],
    "SeenCache": ["filter_seen", "mark_seen"],
    "TokenCache": ["delete", "get", "put"],
    "TokenIssuer": ["issue"],
    "WsConnection": ["close", "recv", "send"],
    "WsConnector": ["connect"],
}


def test_protocols_keep_their_methods() -> None:
    assert PROTOCOL_METHODS == EXPECTED_PROTOCOLS
