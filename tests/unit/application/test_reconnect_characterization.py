"""Caracterização da decisão de reconexão do consumer (refatoração 14 do /entender).

Escrito **antes** de extrair a decisão do ``ChainConsumer`` para ``ReconnectDecider``
(RNF-15), contra o código antigo; depois da extração, só a construção do decisor mudou.
Cada cenário é uma sequência de fins de sessão e fixa, para cada um, ação, espera, alerta,
motivo e falhas seguidas. O jitter vem de uma sequência fixa de sorteios.
"""

from __future__ import annotations

from datetime import UTC

import pytest

from ohip_streaming.application.use_cases.consume_chain import ConsumerOptions
from ohip_streaming.application.use_cases.consumer_session import DrainCause
from ohip_streaming.application.use_cases.reconnect import (
    EndReason,
    ReconnectDecider,
    SessionEnd,
)
from ohip_streaming.domain.offset import Offset

R, D = EndReason, DrainCause
ROLLS = [0.0, 0.25, 0.5, 0.75, 0.99]


def end(
    reason: EndReason,
    code: int | None = None,
    *,
    healthy: bool = False,
    cause: DrainCause | None = None,
    offset: str | None = None,
) -> SessionEnd:
    return SessionEnd(reason, code, "x", healthy, cause, Offset(offset) if offset else None)


NOT, REDE = "4401: token ou hash da app key inválido", "queda de rede ou timeout"
LOCK, BIG = "4409: outro consumidor conectado", "mensagem acima do limite"

SCENARIOS: dict[str, list[tuple[SessionEnd, tuple[str, float, str | None, str, int]]]] = {
    "normal_e_rede": [
        (
            end(R.SERVER_CLOSED, 1000, healthy=True),
            ("RECONNECT", 10.0, None, "fechamento normal", 0),
        ),
        (end(R.SERVER_CLOSED), ("RECONNECT", 10.0, None, REDE, 1)),
        (end(R.SERVER_CLOSED), ("RECONNECT", 21.0, None, REDE, 2)),
        (end(R.LIVENESS_TIMEOUT), ("RECONNECT", 44.0, "WARNING", REDE, 3)),
        (end(R.ACK_TIMEOUT), ("RECONNECT", 69.0, "WARNING", REDE, 4)),
        (end(R.SERVER_CLOSED), ("RECONNECT", 71.88, "WARNING", REDE, 5)),
        (end(R.SERVER_CLOSED, healthy=True), ("RECONNECT", 10.0, None, REDE, 0)),
    ],
    "codigos_ohip": [
        (end(R.SERVER_CLOSED, 4401), ("REFRESH_TOKEN_AND_RECONNECT", 10.0, None, NOT, 1)),
        (end(R.SERVER_CLOSED, 4401), ("REFRESH_TOKEN_AND_RECONNECT", 10.0, None, NOT, 2)),
        (end(R.SERVER_CLOSED, 4401), ("REFRESH_TOKEN_AND_RECONNECT", 10.0, "WARNING", NOT, 3)),
        (
            end(R.SERVER_CLOSED, 4504),
            ("RECONNECT", 15.0, None, "4504: streaming da Oracle fora", 4),
        ),
        (end(R.SERVER_CLOSED, 4409), ("RECONNECT", 120.0, "WARNING", LOCK, 5)),
        (
            end(R.SERVER_CLOSED, 4403),
            (
                "STOP",
                10.0,
                "CRITICAL",
                "4403: chain ou ambiente sem permissão de streaming (ação no Developer Portal)",
                6,
            ),
        ),
        (
            end(R.SERVER_CLOSED, 4406),
            ("STOP", 10.0, "CRITICAL", "4406: subprotocolo recusado (bug no handshake)", 7),
        ),
        (
            end(R.SERVER_CLOSED, 4408),
            ("RECONNECT", 63.0, "WARNING", "4408: connection_init não chegou a tempo", 8),
        ),
        (end(R.INTERNAL_ERROR), ("RECONNECT", 66.0, "WARNING", REDE, 9)),
    ],
    "conexao_ativa": [
        (end(R.CONNECTION_ACTIVE, healthy=True), ("RECONNECT", 120.0, None, LOCK, 1)),
        (end(R.CONNECTION_ACTIVE), ("RECONNECT", 127.5, None, LOCK, 2)),
        (end(R.CONNECTION_ACTIVE), ("RECONNECT", 135.0, None, LOCK, 3)),
        (end(R.CONNECTION_ACTIVE), ("RECONNECT", 142.5, None, LOCK, 4)),
        (end(R.CONNECTION_ACTIVE), ("RECONNECT", 149.7, "WARNING", LOCK, 5)),
        (end(R.CONNECTION_ACTIVE), ("RECONNECT", 120.0, "WARNING", LOCK, 6)),
    ],
    "oauth": [
        (end(R.AUTH_REJECTED), ("RECONNECT", 10.0, "CRITICAL", "x", 1)),
        (end(R.AUTH_REJECTED), ("RECONNECT", 21.0, "CRITICAL", "x", 2)),
        (end(R.UNAVAILABLE), ("RECONNECT", 44.0, "WARNING", "x", 3)),  # zera as recusas
        (end(R.AUTH_REJECTED), ("RECONNECT", 69.0, "CRITICAL", "x", 4)),
        (end(R.AUTH_REJECTED), ("RECONNECT", 71.88, "CRITICAL", "x", 5)),
        (end(R.AUTH_REJECTED), ("RECONNECT", 60.0, "CRITICAL", "x", 6)),
        (end(R.AUTH_REJECTED), ("RECONNECT", 63.0, "CRITICAL", "x", 7)),
        (end(R.AUTH_REJECTED), ("STOP", 0, "CRITICAL", "OAuth recusou 5x seguidas: x", 8)),
    ],
    "handshake": [
        (end(R.HANDSHAKE_REJECTED), ("RECONNECT", 10.0, "CRITICAL", "x", 1)),
        (end(R.HANDSHAKE_REJECTED, healthy=True), ("RECONNECT", 10.5, "CRITICAL", "x", 0)),
    ],
    "mensagem_grande": [  # não conta falha; para na 3ª seguida no mesmo offset
        (end(R.OVERSIZE, offset="5"), ("RECONNECT", 10.0, None, BIG, 0)),
        (end(R.OVERSIZE, offset="5"), ("RECONNECT", 10.0, None, BIG, 0)),
        (end(R.OVERSIZE, offset="6"), ("RECONNECT", 10.0, None, BIG, 0)),
        (end(R.OVERSIZE, offset="6"), ("RECONNECT", 10.0, None, BIG, 0)),
        (end(R.SERVER_CLOSED), ("RECONNECT", 10.0, None, REDE, 1)),  # zera as repetições
        (end(R.OVERSIZE, offset="6"), ("RECONNECT", 10.0, None, BIG, 1)),
        (end(R.OVERSIZE, offset="6"), ("RECONNECT", 10.0, None, BIG, 1)),
        (
            end(R.OVERSIZE, offset="6"),
            ("STOP", 0, "CRITICAL", "mensagem acima do limite 3x no mesmo offset", 1),
        ),
    ],
    "drenagem": [  # token e replay: espera mínima, sem contar falha
        (end(R.SERVER_CLOSED), ("RECONNECT", 10.0, None, REDE, 1)),
        (end(R.DRAINED, 1000, cause=D.TOKEN), ("RECONNECT", 10.0, None, "TOKEN", 1)),
        (end(R.DRAINED, 1000, cause=D.REPLAY), ("RECONNECT", 10.0, None, "REPLAY", 1)),
        (end(R.DRAINED, 1000, cause=D.POISONED), ("RECONNECT", 10.0, None, "fechamento normal", 2)),
        (end(R.DRAINED, 4409, cause=D.SUBSCRIPTION_ENDED), ("RECONNECT", 127.5, None, LOCK, 3)),
        (end(R.DRAINED, 1000, cause=D.TOKEN, healthy=True), ("RECONNECT", 10.0, None, "TOKEN", 0)),
    ],
}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_reconnect_decisions(name: str) -> None:
    rolls = iter(ROLLS * 10)
    defaults = ConsumerOptions("C", "i", UTC)  # política e limite padrão do consumer
    decider = ReconnectDecider(
        defaults.policy, lambda: next(rolls), auth_rejected_max=defaults.auth_rejected_max
    )
    seen = []
    for session_end, _ in SCENARIOS[name]:
        decider.settle(session_end)
        d = decider.decide(session_end)
        alert = d.alert.name if d.alert else None
        seen.append((d.action.name, round(d.wait_s, 3), alert, d.reason, decider.failures))
    assert seen == [expected for _, expected in SCENARIOS[name]]
