"""Regras de lote, publicação e replay (ADR-0002, ADR-0006, ADR-0009, API.md)."""

from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import Final

from ohip_streaming.domain.backoff import exponential_backoff
from ohip_streaming.domain.errors import (
    InvalidOffsetError,
    ReplayConfirmationError,
    ReplayForwardNotAllowedError,
    ReplayOffsetInvalidError,
    ReplayReasonRequiredError,
)
from ohip_streaming.domain.events import Event, RejectedMessage
from ohip_streaming.domain.identifiers import normalize_event_name
from ohip_streaming.domain.offset import Offset

OHIP_RETENTION: Final = timedelta(days=7)
MAX_REPLAY_REASON: Final = 500

# ------------------------------------------------------------------ lote do consumer


def offset_to_persist(items: Sequence[Event | RejectedMessage]) -> Offset | None:
    """Offset da última mensagem do lote **com offset válido**, na ordem de chegada (ADR-0009).

    Mensagens gravadas, duplicadas ou mandadas para a DLQ foram todas tratadas, então contam.
    Se nenhuma tiver offset válido, o offset não muda (None).
    """
    for item in reversed(items):
        if item.offset is not None:
            return item.offset
    return None


def is_allowed(event_name: str, allowlist: Collection[str]) -> bool:
    """Filtro local opcional (DV-13). Lista vazia = todos permitidos."""
    return not allowlist or normalize_event_name(event_name) in allowlist


# ------------------------------------------------------------------ publicação

PUBLISH_RETRY_BASE_S: Final = 10.0
PUBLISH_RETRY_CAP_S: Final = 300.0


def publish_retry_delay_s(attempts: int) -> float:
    """Backoff de uma linha da outbox após ``attempts`` falhas da mensagem (nack).

    10, 20, 40, 80, 160, 300, 300... — com 10 tentativas soma cerca de 30 min (ADR-0002).
    """
    return exponential_backoff(attempts, PUBLISH_RETRY_BASE_S, PUBLISH_RETRY_CAP_S)


# ------------------------------------------------------------------ replay


def validate_replay_request(
    *,
    chain_code: str,
    from_offset_raw: str,
    confirm: str,
    reason: str,
    last_offset: Offset | None,
) -> Offset:
    """Regras de ``POST /api/v1/replay`` (API.md).

    ``from_offset`` é o último offset já processado antes da lacuna; não pode ser maior que o
    último confirmado (um replay "para frente" pularia eventos).
    """
    if confirm != chain_code:
        raise ReplayConfirmationError("confirm deve repetir o chain_code")
    if not reason.strip():
        raise ReplayReasonRequiredError("motivo obrigatório")
    if len(reason) > MAX_REPLAY_REASON:
        raise ReplayReasonRequiredError(f"motivo excede {MAX_REPLAY_REASON} caracteres")
    try:
        from_offset = Offset(from_offset_raw)
    except InvalidOffsetError as exc:
        raise ReplayOffsetInvalidError(str(exc)) from exc
    if last_offset is None:
        raise ReplayForwardNotAllowedError("a chain ainda não tem offset confirmado")
    if from_offset.numeric() > last_offset.numeric():
        raise ReplayForwardNotAllowedError(
            f"from_offset {from_offset} é maior que o último confirmado {last_offset}"
        )
    return from_offset


def replay_retention_warning(offset_received_at: datetime | None, now: datetime) -> str | None:
    """Aviso (não erro) quando o offset provavelmente já saiu da retenção de 7 dias do OHIP."""
    if offset_received_at is None:
        return (
            "offset não encontrado no histórico local; "
            "confirme que está dentro da retenção de 7 dias"
        )
    if now - offset_received_at > OHIP_RETENTION:
        return "offset recebido há mais de 7 dias: o OHIP pode não ter mais esses eventos"
    return None
