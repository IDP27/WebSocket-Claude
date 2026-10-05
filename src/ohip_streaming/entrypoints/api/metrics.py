"""``/metrics`` em texto Prometheus 0.0.4, gerado à mão (ARCHITECTURE §10, ADR-0017).

Gauges por chain tirados do status (Oracle) e os contadores dos processos (snapshots no
Redis), com os rótulos ``process`` e ``instance``. Nome ou rótulo fora do padrão é
descartado; valores de rótulo são escapados.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ohip_streaming.adapters.redis.api_cache import ProcessSnapshot
from ohip_streaming.application.ports import DlqStage, StatusSnapshot
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(labels: Mapping[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{key}="{_escape(str(value))}"' for key, value in labels.items())
    return "{" + inner + "}"


def _number(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return repr(float(value)) if not float(value).is_integer() else str(int(value))


@dataclass
class _Family:
    kind: str
    help: str
    samples: list[tuple[dict[str, str], float]] = field(default_factory=list)


class _Registry:
    def __init__(self) -> None:
        self._families: dict[str, _Family] = {}

    def add(
        self, name: str, value: float | None, labels: Mapping[str, str], *, kind: str, help: str
    ) -> None:
        if value is None:
            return
        if not _NAME_RE.fullmatch(name) or not all(_LABEL_RE.fullmatch(k) for k in labels):
            log.warning("metrica_descartada_nome_invalido", metric=name)
            return
        family = self._families.setdefault(name, _Family(kind, help))
        if family.kind != kind:
            log.warning("metrica_descartada_tipo_conflitante", metric=name)
            return
        family.samples.append((dict(labels), float(value)))

    def render(self) -> str:
        lines: list[str] = []
        for name in sorted(self._families):
            family = self._families[name]
            lines.append(f"# HELP {name} {family.help}")
            lines.append(f"# TYPE {name} {family.kind}")
            lines.extend(
                f"{name}{_labels(labels)} {_number(value)}" for labels, value in family.samples
            )
        return "\n".join(lines) + "\n"


def render(
    status: StatusSnapshot | None,
    processes: Sequence[ProcessSnapshot] | None,
) -> str:
    """``None`` = fonte indisponível (``ohip_metrics_source_up`` 0)."""
    registry = _Registry()
    registry.add("ohip_api_up", 1, {}, kind="gauge", help="API de controle de pé")
    for source, ok in (("oracle", status is not None), ("redis", processes is not None)):
        registry.add(
            "ohip_metrics_source_up",
            1 if ok else 0,
            {"source": source},
            kind="gauge",
            help="Fonte de métricas acessível",
        )
    if status is not None:
        _chain_metrics(registry, status)
    for snapshot in processes or ():
        _process_metrics(registry, snapshot)
    return registry.render()


def _chain_metrics(registry: _Registry, status: StatusSnapshot) -> None:
    now = status.generated_at
    for chain in status.chains:
        base = {"chain_code": chain.chain_code}
        for state in ConsumerState:
            registry.add(
                "ohip_consumer_state",
                1 if chain.state is state else 0,
                {**base, "state": state.value},
                kind="gauge",
                help="Estado da conexão do consumer (1 = estado atual)",
            )
        gauges: Iterable[tuple[str, float | None, str]] = (
            (
                "ohip_consumer_last_offset",
                chain.last_offset.numeric() if chain.last_offset else None,
                "Último offset confirmado",
            ),
            (
                "ohip_consumer_last_message_age_seconds",
                (now - chain.last_message_at).total_seconds() if chain.last_message_at else None,
                "Segundos desde a última mensagem recebida",
            ),
            (
                "ohip_consumer_lag_seconds_p95_5m",
                chain.lag_seconds_p95_5m,
                "p95 de received_at - event_ts nos últimos 5 minutos",
            ),
            (
                "ohip_consumer_events_per_minute",
                chain.events_last_5m / 5,
                "Eventos por minuto (média de 5 minutos)",
            ),
            (
                "ohip_consumer_consecutive_failures",
                chain.consecutive_failures,
                "Falhas de conexão seguidas",
            ),
            (
                "ohip_consumer_last_close_code",
                chain.last_close_code,
                "Último código de fechamento do WebSocket",
            ),
            ("ohip_outbox_pending", chain.outbox.pending, "Mensagens PENDING na outbox"),
            ("ohip_outbox_failed", chain.outbox.failed, "Mensagens FAILED na outbox"),
            (
                "ohip_outbox_oldest_pending_seconds",
                (now - chain.outbox.oldest_pending_at).total_seconds()
                if chain.outbox.oldest_pending_at
                else None,
                "Idade da mensagem PENDING mais antiga",
            ),
        )
        for name, value, help_text in gauges:
            registry.add(name, value, base, kind="gauge", help=help_text)
        registry.add(
            "ohip_consumer_reconnects_total",
            chain.reconnects_total,
            base,
            kind="counter",
            help="Reconexões acumuladas",
        )
        for stage in DlqStage:
            registry.add(
                "ohip_dlq_open",
                chain.dlq_open.get(stage, 0),
                {**base, "stage": stage.value},
                kind="gauge",
                help="Itens abertos na DLQ",
            )


def _process_metrics(registry: _Registry, snapshot: ProcessSnapshot) -> None:
    for counter in snapshot.counters:
        name = counter.get("name")
        value = counter.get("value")
        labels: Any = counter.get("labels") or {}
        if not isinstance(name, str) or not isinstance(value, int | float):
            continue
        if not isinstance(labels, dict):
            continue
        registry.add(
            name,
            value,
            {
                **{str(k): str(v) for k, v in labels.items()},
                "process": snapshot.process,
                "instance": snapshot.instance,
            },
            kind="counter",
            help="Contador do processo (snapshot no Redis)",
        )
