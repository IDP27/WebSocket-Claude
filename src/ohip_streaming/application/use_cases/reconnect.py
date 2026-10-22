"""Decisão de reconexão do consumer, a cada fim de sessão (ADR-0007).

``SessionEnd`` é o resultado de uma conexão; ``ReconnectDecider`` decide se reconecta, quanto
espera e se alerta. As regras por código de fechamento ficam em ``domain/connection.py``;
aqui fica o histórico que a decisão precisa entre uma sessão e outra:

- falhas seguidas (zeradas por uma sessão saudável), que alimentam o backoff e os alertas;
- recusas seguidas do OAuth: na ``auth_rejected_max``-ésima, para (credencial exige ação humana);
- mensagem grande demais repetida no mesmo offset: para antes de entrar em laço infinito.

Drenagem por troca de token ou por replay reconecta na espera mínima e não conta falha.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from ohip_streaming.application.use_cases.consumer_session import DrainCause
from ohip_streaming.domain.connection import (
    SINGLE_CONSUMER_LOCK,
    AlertLevel,
    CloseAction,
    CloseDecision,
    ReconnectPolicy,
    decide_on_close,
    decide_on_oversize,
)
from ohip_streaming.domain.offset import Offset


class EndReason(StrEnum):
    SERVER_CLOSED = "SERVER_CLOSED"  # fechou sem a gente pedir (código em close_code)
    DRAINED = "DRAINED"  # complete enviado e o servidor fechou (ou o prazo venceu)
    LIVENESS_TIMEOUT = "LIVENESS_TIMEOUT"  # sem pong nem next além do prazo
    ACK_TIMEOUT = "ACK_TIMEOUT"
    OVERSIZE = "OVERSIZE"
    HANDSHAKE_REJECTED = "HANDSHAKE_REJECTED"  # HTTP 400 no upgrade: chave/URL (config)
    CONNECTION_ACTIVE = "CONNECTION_ACTIVE"  # status check: há outra conexão
    AUTH_REJECTED = "AUTH_REJECTED"  # OAuth recusou as credenciais
    UNAVAILABLE = "UNAVAILABLE"  # OAuth ou banco fora antes de assinar
    INTERNAL_ERROR = "INTERNAL_ERROR"  # tarefa da sessão falhou com erro inesperado


@dataclass(slots=True)
class SessionEnd:
    reason: EndReason
    close_code: int | None = None
    detail: str = ""
    healthy: bool = False  # recebeu evento ou ficou assinada o bastante (zera as falhas)
    drain_cause: DrainCause | None = None
    offset: Offset | None = None  # offset confirmado ao assinar


class ReconnectDecider:
    def __init__(
        self, policy: ReconnectPolicy, rng: Callable[[], float], *, auth_rejected_max: int
    ) -> None:
        self._policy = policy
        self._rng = rng
        self._auth_rejected_max = auth_rejected_max
        self._failures = 0
        self._auth_rejections = 0
        self._oversize: tuple[Offset | None, int] = (None, 0)

    @property
    def failures(self) -> int:
        """Falhas seguidas (gravadas em ``OHIP_CONSUMER_STATUS.consecutive_failures``)."""
        return self._failures

    def settle(self, end: SessionEnd) -> None:
        """Sessão saudável zera as falhas. Vale mesmo quando não há decisão (parada, lease)."""
        if end.healthy:
            self._failures = 0

    def decide(self, end: SessionEnd) -> CloseDecision:
        policy, rng = self._policy, self._rng
        if end.reason is EndReason.OVERSIZE:
            offset, repeats = self._oversize
            repeats = repeats + 1 if offset == end.offset else 1
            self._oversize = (end.offset, repeats)
            return decide_on_oversize(repeats, policy)
        self._oversize = (None, 0)
        if end.reason is EndReason.DRAINED and end.drain_cause in (
            DrainCause.TOKEN,
            DrainCause.REPLAY,
        ):
            return CloseDecision(CloseAction.RECONNECT, policy.min_gap_s, str(end.drain_cause))
        if end.reason is EndReason.CONNECTION_ACTIVE:
            self._failures += 1
            return decide_on_close(SINGLE_CONSUMER_LOCK, self._failures, policy, rng)
        if not end.healthy:
            self._failures += 1
        failures = max(1, self._failures)
        if end.reason is EndReason.AUTH_REJECTED:
            self._auth_rejections += 1
            if self._auth_rejections >= self._auth_rejected_max:
                return CloseDecision(
                    CloseAction.STOP,
                    0,
                    f"OAuth recusou {self._auth_rejections}x seguidas: {end.detail}",
                    AlertLevel.CRITICAL,
                )
        else:
            self._auth_rejections = 0
        if end.reason in (
            EndReason.HANDSHAKE_REJECTED,
            EndReason.AUTH_REJECTED,
            EndReason.UNAVAILABLE,
        ):
            decision = decide_on_close(None, failures, policy, rng)
            critical = end.reason is not EndReason.UNAVAILABLE  # configuração: alerta já
            alert = AlertLevel.CRITICAL if critical else decision.alert
            return CloseDecision(decision.action, decision.wait_s, end.detail, alert)
        return decide_on_close(end.close_code, failures, policy, rng)
