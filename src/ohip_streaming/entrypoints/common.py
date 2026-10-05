"""Apoio comum aos processos (sem dependências de um processo específico)."""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Iterable, Mapping

from ohip_streaming.application.ports import MetricsSink

# (nome, rótulos) de um contador usado nas regras de alerta (deploy/prometheus).
AlertCounter = tuple[str, Mapping[str, str]]


def instance_id() -> str:
    """Identificação da instância no lease e nos logs: ``host:pid:aleatório``."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def preregister(metrics: MetricsSink, counters: Iterable[AlertCounter]) -> None:
    """Contadores de alerta já aparecem com 0 no primeiro snapshot. Sem isso, a série nasce
    com 1 e o ``increase()`` do Prometheus não vê a primeira ocorrência (ADR-0020 §6)."""
    for name, labels in counters:
        metrics.increment(name, 0, **labels)
