"""Expurgo por retenção (ADR-0020 §2)."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol


class PurgeTarget(StrEnum):
    """Tabelas expurgadas, na ordem das chaves estrangeiras (ARCHITECTURE §5.1)."""

    DLQ = "DLQ"  # resolvida antes do corte
    OUTBOX = "OUTBOX"  # SENT antes do corte, sem item de DLQ apontando
    # FAILED criada antes do corte e sem item de DLQ: o item PUBLISH já foi resolvido (retry
    # cria linha nova, descarte não) e expurgado; sem este passo, ficaria para sempre.
    OUTBOX_FAILED = "OUTBOX_FAILED"
    RAW = "RAW"  # recebido antes do corte, sem outbox nem DLQ


PURGE_ORDER: tuple[PurgeTarget, ...] = (
    PurgeTarget.DLQ,
    PurgeTarget.OUTBOX,
    PurgeTarget.OUTBOX_FAILED,
    PurgeTarget.RAW,
)


class PurgeStore(Protocol):
    """Corte = relógio **do banco** menos ``retention_days``."""

    async def purge_batch(self, target: PurgeTarget, retention_days: int, batch_size: int) -> int:
        """Apaga até ``batch_size`` linhas expiradas numa transação própria (commit no fim) e
        devolve quantas. Idempotente: o que sobrar fica para o próximo lote."""
        ...

    async def count_expired(self, target: PurgeTarget, retention_days: int) -> int:
        """Quantas linhas o expurgo apagaria (``PURGE_DRY_RUN``)."""
        ...
