"""Ports (interfaces) da aplicação. Os adapters (Fases 3 a 9) implementam; os testes usam fakes.

Todos assíncronos: o adapter Oracle roda o driver síncrono em executor próprio
(ADR-0003, ``ohip_streaming.logging.run_in_executor``), sem que os casos de uso saibam disso.

Contratos transacionais importantes estão descritos em cada método: o fake em memória
(tests/fakes) e o adapter Oracle precisam se comportar igual.

Pacote por contexto (refatoração 3 do /entender); os nomes continuam importáveis daqui.
"""

from ohip_streaming.application.ports.auth import (
    AccessToken,
    TokenCache,
    TokenIssuer,
)
from ohip_streaming.application.ports.common import (
    Clock,
    DlqStage,
    MetricsSink,
    ProcessingStatus,
    StoredEvent,
)
from ohip_streaming.application.ports.consumer import (
    BatchResult,
    BatchToPersist,
    ConnectionHealth,
    ConsumeDlqRecord,
    ConsumeRetryItem,
    ConsumerStatusStore,
    DisconnectSnapshot,
    EventStore,
    NewEventRecord,
    RowFailure,
    SeenCache,
    WsConnection,
    WsConnector,
)
from ohip_streaming.application.ports.enricher import (
    DomainWrite,
    EnrichmentStore,
    NormalizationRule,
    QueueDedup,
    ResourceFetcher,
)
from ohip_streaming.application.ports.lease import (
    LeaseStore,
)
from ohip_streaming.application.ports.monitoring import (
    ChainStatus,
    DlqEntry,
    DlqQuery,
    EventFilter,
    EventRecord,
    EventSummary,
    ItemT,
    MonitoringStore,
    OutboxCounts,
    OutboxEntry,
    OutboxQuery,
    Page,
    PageRequest,
    ReplayEntry,
    ReplayQuery,
    StatusSnapshot,
)
from ohip_streaming.application.ports.operations import (
    DlqItem,
    OperationsStore,
)
from ohip_streaming.application.ports.publisher import (
    MessagePublisher,
    OutboxRow,
    OutboxStore,
    OutgoingMessage,
    PublishOutcome,
)
from ohip_streaming.application.ports.purge import (
    PURGE_ORDER,
    PurgeStore,
    PurgeTarget,
)
from ohip_streaming.application.ports.replay import (
    ReplayRequest,
    ReplayStatus,
    ReplayStore,
)

__all__ = [
    "PURGE_ORDER",
    "AccessToken",
    "BatchResult",
    "BatchToPersist",
    "ChainStatus",
    "Clock",
    "ConnectionHealth",
    "ConsumeDlqRecord",
    "ConsumeRetryItem",
    "ConsumerStatusStore",
    "DisconnectSnapshot",
    "DlqEntry",
    "DlqItem",
    "DlqQuery",
    "DlqStage",
    "DomainWrite",
    "EnrichmentStore",
    "EventFilter",
    "EventRecord",
    "EventStore",
    "EventSummary",
    "ItemT",
    "LeaseStore",
    "MessagePublisher",
    "MetricsSink",
    "MonitoringStore",
    "NewEventRecord",
    "NormalizationRule",
    "OperationsStore",
    "OutboxCounts",
    "OutboxEntry",
    "OutboxQuery",
    "OutboxRow",
    "OutboxStore",
    "OutgoingMessage",
    "Page",
    "PageRequest",
    "ProcessingStatus",
    "PublishOutcome",
    "PurgeStore",
    "PurgeTarget",
    "QueueDedup",
    "ReplayEntry",
    "ReplayQuery",
    "ReplayRequest",
    "ReplayStatus",
    "ReplayStore",
    "ResourceFetcher",
    "RowFailure",
    "SeenCache",
    "StatusSnapshot",
    "StoredEvent",
    "TokenCache",
    "TokenIssuer",
    "WsConnection",
    "WsConnector",
]
