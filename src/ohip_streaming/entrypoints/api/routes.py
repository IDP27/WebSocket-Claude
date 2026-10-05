"""Rotas da API de controle (docs/API.md). Todas ``def`` (ADR-0003, ADR-0017).

Nenhuma rota publica na fila: reprocessamentos e retries inserem linhas na outbox (ADR-0002).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, Path, Query, Request, Security
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ohip_streaming.application.ports import (
    DlqQuery,
    DlqStage,
    EventFilter,
    OutboxQuery,
    PageRequest,
    ProcessingStatus,
    ReplayQuery,
    ReplayStatus,
)
from ohip_streaming.application.use_cases.replay import ReplayCommand
from ohip_streaming.domain.identifiers import normalize_event_name
from ohip_streaming.entrypoints.api.runtime import run_sync
from ohip_streaming.entrypoints.api.schemas import (
    DlqPage,
    DlqRetryAccepted,
    ErrorBody,
    EventDetailOut,
    EventPage,
    Health,
    OutboxPage,
    Ready,
    ReplayAccepted,
    ReplayCreate,
    ReplayOut,
    ReplayPage,
    ReprocessAccepted,
    StatusOut,
    dlq_out,
    event_detail_out,
    event_summary_out,
    outbox_out,
    replay_out,
)
from ohip_streaming.entrypoints.api.security import (
    Principal,
    authenticate,
    require_admin,
    validate_actor,
)
from ohip_streaming.entrypoints.api.services import ApiServices
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

PROMETHEUS_TYPE = "text/plain; version=0.0.4; charset=utf-8"
UNIQUE_EVENT_ID_MAX = 64  # ohip_event_raw.unique_event_id VARCHAR2(64)

_bearer = HTTPBearer(auto_error=False, description="Token de serviço (perfil read ou admin)")
_errors: dict[int | str, dict[str, object]] = {
    status: {"model": ErrorBody} for status in (400, 401, 403, 404, 409, 422, 503)
}

internal = APIRouter(tags=["interno"])
api = APIRouter(prefix="/api/v1", responses=_errors)


# ------------------------------------------------------------------- dependências


def services(request: Request) -> ApiServices:
    built: ApiServices = request.app.state.services
    return built


Services = Annotated[ApiServices, Depends(services)]


def principal(
    request: Request,
    app: Services,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
    x_actor: Annotated[str | None, Header(alias="X-Actor", max_length=400)] = None,
) -> Principal:
    role = authenticate(credentials.credentials if credentials else None, app.tokens)
    actor = validate_actor(x_actor)
    request.state.role = role.value
    request.state.actor = actor
    return Principal(role, actor)


Reader = Annotated[Principal, Depends(principal)]


def admin_actor(who: Reader) -> str:
    return require_admin(who)


Admin = Annotated[str, Depends(admin_actor)]
Limit = Annotated[int, Query(ge=1, le=200, description="Itens por página (máx. 200)")]
Cursor = Annotated[int | None, Query(ge=1, description="Último id visto (paginação por chave)")]
UniqueEventId = Annotated[str, Path(min_length=1, max_length=UNIQUE_EVENT_ID_MAX)]
ChainCode = Annotated[str | None, Query(min_length=1, max_length=20)]


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _unmasked_by(who: Principal, unmasked: bool) -> str | None:
    return require_admin(who) if unmasked else None


# ------------------------------------------------------------------- interno


@internal.get("/health", response_model=Health)
def health() -> Health:
    """Processo de pé (liveness). Não toca dependências."""
    return Health()


@internal.get("/ready", response_model=Ready, responses={503: {"model": Ready}})
def ready(app: Services) -> JSONResponse:
    """Oracle, Redis e RabbitMQ acessíveis (readiness)."""
    result = app.ready.get()
    return JSONResponse(result.model_dump(), status_code=200 if result.status == "ok" else 503)


@internal.get("/metrics", response_class=PlainTextResponse)
def metrics(app: Services) -> PlainTextResponse:
    """Formato Prometheus (ARCHITECTURE §10)."""
    return PlainTextResponse(app.metrics.get(), media_type=PROMETHEUS_TYPE)


# ------------------------------------------------------------------- leitura


@api.get("/status", response_model=StatusOut)
def status(app: Services, _who: Reader) -> Response:
    """Estado por chain. Cache de ``REDIS_API_STATUS_TTL_S`` (Redis + cópia local)."""
    return Response(app.status.payload(), media_type="application/json")


@api.get("/events", response_model=EventPage)
def list_events(
    app: Services,
    _who: Reader,
    chain_code: ChainCode = None,
    hotel_id: Annotated[str | None, Query(min_length=1, max_length=50)] = None,
    event_name: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    module_name: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    primary_key: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    received_from: Annotated[
        datetime | None, Query(alias="from", description="received_at >= (UTC)")
    ] = None,
    received_to: Annotated[
        datetime | None, Query(alias="to", description="received_at < (UTC)")
    ] = None,
    processing_status: ProcessingStatus | None = None,
    limit: Limit = 50,
    cursor: Cursor = None,
) -> EventPage:
    query = EventFilter(
        chain_code=chain_code,
        hotel_id=hotel_id,
        event_name=normalize_event_name(event_name) if event_name else None,
        module_name=module_name,
        primary_key=primary_key,
        received_from=_utc(received_from),
        received_to=_utc(received_to),
        processing_status=processing_status,
    )
    page = run_sync(app.monitoring.events(query, PageRequest(limit, cursor)))
    return EventPage(items=[event_summary_out(i) for i in page.items], next_cursor=page.next_cursor)


@api.get("/events/{unique_event_id}", response_model=EventDetailOut)
def get_event(
    app: Services, who: Reader, unique_event_id: UniqueEventId, unmasked: bool = False
) -> EventDetailOut:
    """Evento + status + outbox + DLQ. ``unmasked=true`` só admin com X-Actor (auditado)."""
    view = run_sync(app.monitoring.event(unique_event_id, unmasked_by=_unmasked_by(who, unmasked)))
    return event_detail_out(view)


@api.get("/outbox", response_model=OutboxPage)
def list_outbox(
    app: Services,
    _who: Reader,
    status: Literal["PENDING", "FAILED"] = "PENDING",
    chain_code: ChainCode = None,
    limit: Limit = 50,
    cursor: Cursor = None,
) -> OutboxPage:
    view = run_sync(
        app.monitoring.outbox(OutboxQuery(status, chain_code), PageRequest(limit, cursor))
    )
    age = view.oldest_pending_age_s
    return OutboxPage(
        items=[outbox_out(r) for r in view.page.items],
        next_cursor=view.page.next_cursor,
        oldest_pending_seconds=round(max(0.0, age), 1) if age is not None else None,
    )


@api.get("/dlq", response_model=DlqPage)
def list_dlq(
    app: Services,
    _who: Reader,
    stage: DlqStage | None = None,
    chain_code: ChainCode = None,
    resolved: bool | None = None,
    limit: Limit = 50,
    cursor: Cursor = None,
) -> DlqPage:
    page = run_sync(
        app.monitoring.dlq(DlqQuery(stage, chain_code, resolved), PageRequest(limit, cursor))
    )
    return DlqPage(items=[dlq_out(i) for i in page.items], next_cursor=page.next_cursor)


@api.get("/replay", response_model=ReplayPage)
def list_replays(
    app: Services,
    _who: Reader,
    chain_code: ChainCode = None,
    status: ReplayStatus | None = None,
    limit: Limit = 50,
    cursor: Cursor = None,
) -> ReplayPage:
    page = run_sync(
        app.monitoring.replays(ReplayQuery(chain_code, status), PageRequest(limit, cursor))
    )
    return ReplayPage(items=[replay_out(r) for r in page.items], next_cursor=page.next_cursor)


# ------------------------------------------------------------------- admin


@api.get("/events/{unique_event_id}/export")
def export_event(
    app: Services, actor: Admin, unique_event_id: UniqueEventId, unmasked: bool = False
) -> JSONResponse:
    """JSON para reprodução local (RF-13). Mascarado por padrão."""
    body = run_sync(app.monitoring.export(unique_event_id, unmasked_by=actor if unmasked else None))
    log.info("api_evento_exportado", unique_event_id=unique_event_id, actor=actor)
    return JSONResponse(
        body,
        headers={
            "Content-Disposition": f'attachment; filename="evento-{body["raw_event_id"]}.json"'
        },
    )


@api.post("/events/{unique_event_id}/reprocess", status_code=202, response_model=ReprocessAccepted)
def reprocess_event(
    app: Services, actor: Admin, unique_event_id: UniqueEventId
) -> ReprocessAccepted:
    """Mensagem para o exchange ``ohip.reprocess`` via outbox (só o enricher recebe)."""
    outbox_id = run_sync(app.reprocess.execute(unique_event_id, actor))
    return ReprocessAccepted(outbox_id=outbox_id)


@api.post("/replay", status_code=202, response_model=ReplayAccepted)
def request_replay(app: Services, actor: Admin, body: ReplayCreate) -> ReplayAccepted:
    """Pede replay de uma chain a partir do último offset já processado antes da lacuna."""
    result = run_sync(
        app.request_replay.execute(
            ReplayCommand(
                chain_code=body.chain_code,
                from_offset=body.from_offset,
                reason=body.reason,
                confirm=body.confirm,
                requested_by=actor,
            )
        )
    )
    request = result.request
    return ReplayAccepted(
        id=request.id,
        status=request.status,
        chain_code=request.chain_code,
        from_offset=request.from_offset.value,
        warnings=list(result.warnings),
    )


@api.delete("/replay/{request_id}", response_model=ReplayOut)
def cancel_replay(app: Services, actor: Admin, request_id: Annotated[int, Path(ge=1)]) -> ReplayOut:
    """Cancela um pedido ainda PENDING."""
    run_sync(app.monitoring.replay(request_id))  # 404 se não existe
    run_sync(app.cancel_replay.execute(request_id, actor))  # 409 se não está PENDING
    return replay_out(run_sync(app.monitoring.replay(request_id)))


@api.post("/dlq/{item_id}/retry", status_code=202, response_model=DlqRetryAccepted)
def retry_dlq(app: Services, actor: Admin, item_id: Annotated[int, Path(ge=1)]) -> DlqRetryAccepted:
    """Reprocessa o item conforme o estágio (API.md)."""
    result = run_sync(app.retry_dlq.execute(item_id, actor))
    return DlqRetryAccepted(result=result.kind.value, outbox_id=result.outbox_id)
