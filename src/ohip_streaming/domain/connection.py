"""Políticas do ciclo de vida da conexão com o OHIP (ADR-0007), sem I/O.

O laço assíncrono (Fase 5) só executa o que estas funções decidem: quanto esperar, se renova
o token, se para e alerta. Assim as regras do guia Oracle ficam testáveis isoladamente.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

# Códigos de fechamento documentados (guia Oracle, Troubleshooting).
NORMAL_CLOSE = 1000
UNAUTHORIZED = 4401
FORBIDDEN = 4403
SUBPROTOCOL_NOT_ACCEPTABLE = 4406
INIT_TIMEOUT = 4408
SINGLE_CONSUMER_LOCK = 4409
SERVICE_TIMEOUT = 4504


class ConsumerState(StrEnum):
    """Estados persistidos em OHIP_CONSUMER_STATUS.state (CHECK na DDL)."""

    ACQUIRING = "ACQUIRING"
    CONNECTING = "CONNECTING"
    INIT_SENT = "INIT_SENT"
    STATUS_CHECK = "STATUS_CHECK"
    SUBSCRIBED = "SUBSCRIBED"
    DRAINING = "DRAINING"
    WAITING = "WAITING"
    STOPPED = "STOPPED"


class CloseAction(StrEnum):
    RECONNECT = "RECONNECT"
    REFRESH_TOKEN_AND_RECONNECT = "REFRESH_TOKEN_AND_RECONNECT"  # noqa: S105 - não é segredo
    STOP = "STOP"


class AlertLevel(StrEnum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    min_gap_s: float = 10.0
    lockout_4409_s: float = 120.0
    lockout_jitter_max_s: float = 30.0
    backoff_4504_s: float = 15.0
    backoff_initial_s: float = 10.0
    backoff_max_s: float = 60.0
    backoff_jitter_ratio: float = 0.2
    repeated_failure_alert_after: int = 3  # 4401/4408/rede seguidos
    lockout_alert_after: int = 5  # 4409 seguidos (~10 min)
    oversize_max_repeats: int = 3


@dataclass(frozen=True, slots=True)
class CloseDecision:
    action: CloseAction
    wait_s: float
    reason: str
    alert: AlertLevel | None = None


Random = Callable[[], float]  # devolve um número em [0, 1)


def _exponential_backoff(policy: ReconnectPolicy, attempt: int, rng: Random) -> float:
    base = min(policy.backoff_max_s, policy.backoff_initial_s * float(2 ** max(0, attempt - 1)))
    return base * (1 + policy.backoff_jitter_ratio * rng())


def decide_on_close(
    code: int | None,
    consecutive_failures: int,
    policy: ReconnectPolicy,
    rng: Random,
) -> CloseDecision:
    """O que fazer quando a conexão fecha.

    ``consecutive_failures`` conta os fechamentos seguidos sem uma assinatura bem-sucedida,
    **incluindo** este. ``code`` é None para queda de rede/timeout sem código WebSocket.
    Toda espera respeita o intervalo mínimo de 10 s do OHIP.
    """
    attempt = max(1, consecutive_failures)
    repeated = attempt >= policy.repeated_failure_alert_after

    def decision(
        action: CloseAction, wait_s: float, reason: str, alert: AlertLevel | None = None
    ) -> CloseDecision:
        return CloseDecision(action, max(wait_s, policy.min_gap_s), reason, alert)

    if code == NORMAL_CLOSE:
        return decision(CloseAction.RECONNECT, policy.min_gap_s, "fechamento normal")
    if code == UNAUTHORIZED:
        return decision(
            CloseAction.REFRESH_TOKEN_AND_RECONNECT,
            policy.min_gap_s,
            "4401: token ou hash da app key inválido",
            AlertLevel.WARNING if repeated else None,
        )
    if code == FORBIDDEN:
        return decision(
            CloseAction.STOP,
            0,
            "4403: chain ou ambiente sem permissão de streaming (ação no Developer Portal)",
            AlertLevel.CRITICAL,
        )
    if code == SUBPROTOCOL_NOT_ACCEPTABLE:
        return decision(
            CloseAction.STOP,
            0,
            "4406: subprotocolo recusado (bug no handshake)",
            AlertLevel.CRITICAL,
        )
    if code == SINGLE_CONSUMER_LOCK:
        wait = policy.lockout_4409_s + policy.lockout_jitter_max_s * rng()
        alert = AlertLevel.WARNING if attempt >= policy.lockout_alert_after else None
        return decision(CloseAction.RECONNECT, wait, "4409: outro consumidor conectado", alert)
    if code == SERVICE_TIMEOUT:
        return decision(
            CloseAction.RECONNECT, policy.backoff_4504_s, "4504: streaming da Oracle fora"
        )
    if code == INIT_TIMEOUT:
        return decision(
            CloseAction.RECONNECT,
            _exponential_backoff(policy, attempt, rng),
            "4408: connection_init não chegou a tempo",
            AlertLevel.WARNING if repeated else None,
        )
    reason = "queda de rede ou timeout" if code is None else f"fechamento {code}"
    return decision(
        CloseAction.RECONNECT,
        _exponential_backoff(policy, attempt, rng),
        reason,
        AlertLevel.WARNING if repeated else None,
    )


def decide_on_oversize(repeats_at_same_offset: int, policy: ReconnectPolicy) -> CloseDecision:
    """Mensagem acima do limite: reconecta, mas para se repetir no mesmo offset (laço infinito)."""
    if repeats_at_same_offset >= policy.oversize_max_repeats:
        return CloseDecision(
            CloseAction.STOP,
            0,
            f"mensagem acima do limite {repeats_at_same_offset}x no mesmo offset",
            AlertLevel.CRITICAL,
        )
    return CloseDecision(CloseAction.RECONNECT, policy.min_gap_s, "mensagem acima do limite")


def wait_before_connect(
    *,
    db_now: datetime,
    last_disconnect_at: datetime | None,
    last_state: ConsumerState | None,
    min_gap_s: float,
) -> float:
    """Segundos a esperar antes de conectar (regra dos 10 s, inclusive após crash — ADR-0007).

    Os dois horários vêm do relógio do banco. Se o último estado não for WAITING/STOPPED, o
    processo anterior caiu sem registrar a desconexão: o horário real é desconhecido, então a
    espera é integral. ``last_disconnect_at`` nulo (primeira partida) também.
    """
    if last_disconnect_at is None or last_state not in (
        ConsumerState.WAITING,
        ConsumerState.STOPPED,
    ):
        return min_gap_s
    elapsed = (db_now - last_disconnect_at).total_seconds()
    return max(0.0, min_gap_s - elapsed)


def token_refresh_due(*, now: datetime, expires_at: datetime, margin_s: float) -> bool:
    return now >= expires_at - timedelta(seconds=margin_s)


@dataclass(slots=True)
class RttEstimator:
    """RTT suavizado do heartbeat (média exponencial, alfa = 1/8, como no TCP).

    Prova de vida = ``pong`` ou qualquer ``next``; o prazo é ``max(180 s, 4 x SRTT + jitter)``
    (guia Oracle, Performance Considerations).
    """

    srtt_s: float | None = None
    alpha: float = 0.125

    def update(self, sample_s: float) -> float:
        sample_s = max(0.0, sample_s)
        self.srtt_s = (
            sample_s
            if self.srtt_s is None
            else ((1 - self.alpha) * self.srtt_s + self.alpha * sample_s)
        )
        return self.srtt_s

    def liveness_timeout_s(self, *, min_timeout_s: float, jitter_s: float = 0.0) -> float:
        if self.srtt_s is None:
            return min_timeout_s
        return max(min_timeout_s, 4 * self.srtt_s + jitter_s)
