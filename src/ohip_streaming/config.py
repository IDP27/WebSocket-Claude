"""Configuração por ambiente (RF-12) com pydantic-settings.

Cada grupo lê suas variáveis com um prefixo próprio (``OHIP_``, ``ORACLE_``, ...), do
ambiente ou do arquivo ``.env`` no diretório de trabalho. Variáveis de ambiente têm
precedência sobre o ``.env`` (em produção, o systemd injeta via ``EnvironmentFile``).

Cada processo carrega só os grupos de que precisa (o painel, por exemplo, não recebe
credenciais do Oracle). Segredos são ``SecretStr``: nunca aparecem em ``repr`` nem em log.

Limites do protocolo OHIP (ADR-0007) são validados aqui, para que uma configuração que
provocaria 4409 ou 4408 não chegue a subir.
"""

from __future__ import annotations

import re
from enum import StrEnum
from functools import cache
from pathlib import Path
from typing import Annotated, Literal, Self, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from ohip_streaming.domain.errors import InvalidIdentifierError
from ohip_streaming.domain.identifiers import (
    normalize_event_name,
    validate_chain_code,
    validate_hotel_codes,
)

MIB = 1024 * 1024

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def _config(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # o .env é compartilhado entre os grupos
        case_sensitive=False,
        # Erros de validação não ecoam o valor recebido: ele pode ser uma senha ou um token
        # (ex.: RABBITMQ_URL com credenciais) e iria parar no journald ao falhar a partida.
        hide_input_in_errors=True,
    )


def _split_csv(value: object) -> object:
    """Aceita lista ou texto separado por vírgula (formato natural em variáveis de ambiente)."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


class Environment(StrEnum):
    DESENVOLVIMENTO = "desenvolvimento"
    HOMOLOGACAO = "homologacao"
    PRODUCAO = "producao"


class AuthMode(StrEnum):
    """Tipo de autenticação do ambiente OPERA (Q-10)."""

    CLIENT_CREDENTIALS = "client_credentials"  # OCIM, exige enterpriseId
    RESOURCE_OWNER = "resource_owner"  # usuário de integração


class AppSettings(BaseSettings):
    """Identificação do ambiente e da versão do código (vai em todo log)."""

    model_config = _config("APP_")

    environment: Environment = Environment.DESENVOLVIMENTO
    code_version: str = Field(default="dev", min_length=1, max_length=40)


class OhipSettings(BaseSettings):
    """Credenciais, assinatura e parâmetros do protocolo OHIP Streaming."""

    model_config = _config("OHIP_")

    # --- gateway e credenciais
    gateway_url: str
    app_key: SecretStr
    client_id: str = Field(min_length=1)
    client_secret: SecretStr
    auth_mode: AuthMode = AuthMode.CLIENT_CREDENTIALS
    enterprise_id: str | None = None
    integration_username: str | None = None
    integration_password: SecretStr | None = None
    # Caminho e escopo conferidos na spec oficial publishedoauth.json e na coleção Postman da
    # Oracle (D-2, ADR-0012); configuráveis por ambiente. Teste: tests/contract.
    oauth_token_path: str = "/oauth/v1/tokens"  # noqa: S105 - caminho da URL, não é segredo
    oauth_scope: str = "urn:opc:hgbu:ws:__myscopes__"

    # --- assinatura
    chain_code: str
    hotel_codes: Annotated[list[str], NoDecode] = Field(default_factory=list)
    delta: bool = False
    event_allowlist: Annotated[list[str], NoDecode] = Field(default_factory=list)  # DV-13
    module_codes: dict[str, str] = Field(default_factory=dict)  # moduleName → código da routing key
    event_timezone: str = "UTC"  # fuso do campo "timestamp" do evento (D-3)

    # --- protocolo (ADR-0007)
    ping_interval_s: float = Field(default=15.0, gt=0, le=15)
    ack_timeout_s: float = Field(default=10.0, gt=0, le=60)
    pong_timeout_min_s: float = Field(default=180.0, ge=180)  # max(180 s, 4 x SRTT), ADR-0007
    token_refresh_margin_s: int = Field(default=300, ge=60, le=1800)  # token vale 60 min
    reconnect_min_gap_s: float = Field(default=10.0, ge=10)
    lockout_4409_s: float = Field(default=120.0, ge=120)
    backoff_4504_s: float = Field(default=15.0, ge=15)
    backoff_initial_s: float = Field(default=10.0, ge=10)
    backoff_max_s: float = Field(default=60.0, ge=10)
    drain_timeout_s: float = Field(default=30.0, gt=0)
    ws_max_message_bytes: int = Field(default=16 * MIB, ge=1 * MIB)
    oversize_max_repeats: int = Field(default=3, ge=1)
    status_check_enabled: bool = False  # desligado até validar o D-4 no sandbox
    status_check_min_interval_s: float = Field(default=30.0, ge=30)

    @field_validator("gateway_url")
    @classmethod
    def _validate_gateway_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith("https://") or len(value) <= len("https://"):
            raise ValueError(
                "gateway_url deve ser https://<host> (o WebSocket usa wss:// no mesmo host)"
            )
        return value

    @field_validator("oauth_token_path")
    @classmethod
    def _validate_oauth_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("oauth_token_path deve começar com /")
        return value

    @field_validator("chain_code")
    @classmethod
    def _validate_chain_code(cls, value: str) -> str:
        try:
            return validate_chain_code(value)
        except InvalidIdentifierError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("hotel_codes", "event_allowlist", mode="before")
    @classmethod
    def _parse_csv(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("hotel_codes")
    @classmethod
    def _validate_hotel_codes(cls, value: list[str]) -> list[str]:
        try:
            return validate_hotel_codes(value)
        except InvalidIdentifierError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("event_allowlist")
    @classmethod
    def _normalize_events(cls, value: list[str]) -> list[str]:
        return [normalize_event_name(name) for name in value]

    @field_validator("module_codes")
    @classmethod
    def _normalize_module_codes(cls, value: dict[str, str]) -> dict[str, str]:
        # moduleName chega com caixas diferentes ("Reservation", "PROFILE").
        return {key.strip().lower(): code.strip() for key, code in value.items()}

    @field_validator("event_timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"fuso horário desconhecido: {value!r}") from exc
        return value

    @model_validator(mode="after")
    def _validate_combinations(self) -> Self:
        if self.auth_mode is AuthMode.CLIENT_CREDENTIALS and not self.enterprise_id:
            raise ValueError("auth_mode=client_credentials exige OHIP_ENTERPRISE_ID")
        if self.auth_mode is AuthMode.RESOURCE_OWNER and not (
            self.integration_username and self.integration_password
        ):
            raise ValueError(
                "auth_mode=resource_owner exige OHIP_INTEGRATION_USERNAME"
                " e OHIP_INTEGRATION_PASSWORD"
            )
        if self.backoff_max_s < self.backoff_initial_s:
            raise ValueError("backoff_max_s deve ser >= backoff_initial_s")
        return self


class OhipRoutingSettings(BaseSettings):
    """Só o que a API precisa do grupo ``OHIP_`` (routing key do reprocessamento): ela não
    recebe app key, client secret nem gateway (ADR-0017)."""

    model_config = _config("OHIP_")

    module_codes: dict[str, str] = Field(default_factory=dict)

    @field_validator("module_codes")
    @classmethod
    def _normalize_module_codes(cls, value: dict[str, str]) -> dict[str, str]:
        return {key.strip().lower(): code.strip() for key, code in value.items()}


class OracleSettings(BaseSettings):
    """Oracle 11g em modo Thick (ADR-0003)."""

    model_config = _config("ORACLE_")

    dsn: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: SecretStr
    client_lib_dir: Path | None = None  # None: Instant Client pelo caminho padrão do sistema
    pool_min: int = Field(default=1, ge=1)
    pool_max: int = Field(default=4, ge=1)
    # Espera máxima por uma conexão livre do pool e por uma chamada ao banco. Estourou →
    # "banco indisponível" (a conexão é descartada). 0 desliga o limite da chamada.
    pool_wait_timeout_ms: int = Field(default=10_000, ge=100)
    call_timeout_ms: int = Field(default=60_000, ge=0)

    @model_validator(mode="after")
    def _validate_pool(self) -> Self:
        if self.pool_max < self.pool_min:
            raise ValueError("pool_max deve ser >= pool_min")
        return self


class RabbitMQSettings(BaseSettings):
    """Broker e topologia da fila (ADR-0002)."""

    model_config = _config("RABBITMQ_")

    url: SecretStr  # amqps://usuario:senha@host/vhost — contém senha
    events_exchange: str = "ohip.events"
    unrouted_exchange: str = "ohip.events.unrouted"
    reprocess_exchange: str = "ohip.reprocess"
    enricher_queue: str = "ohip.enricher"
    unrouted_queue: str = "ohip.unrouted"
    unrouted_max_length: int = Field(default=100_000, ge=1)  # mais antigas descartadas
    connect_timeout_s: float = Field(default=10.0, gt=0)
    confirm_timeout_s: float = Field(default=30.0, gt=0)  # sem confirm: resultado desconhecido
    broker_max_message_bytes: int = Field(default=16 * MIB, ge=1 * MIB)
    publish_max_attempts: int = Field(default=10, ge=1)
    channel_fail_attribution: int = Field(default=3, ge=1)

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(("amqp://", "amqps://")):
            raise ValueError("RABBITMQ_URL deve começar com amqp:// ou amqps://")
        return value


class PublisherSettings(BaseSettings):
    """Laço do publisher (ARCHITECTURE §4.2, ADR-0016)."""

    model_config = _config("PUBLISHER_")

    poll_interval_s: float = Field(default=1.0, gt=0)  # sem nada a publicar
    batch_limit: int = Field(default=100, ge=1)  # linhas lidas da cabeça por chain e rodada
    max_parallel_chains: int = Field(default=8, ge=1)
    broker_backoff_initial_s: float = Field(default=1.0, gt=0)
    broker_backoff_max_s: float = Field(default=60.0, gt=0)


_ROUTING_KEY_RE = re.compile(r"^[A-Za-z0-9_*#-]+(\.[A-Za-z0-9_*#-]+)*$")
_PATH_FIELDS_RE = re.compile(r"\{([^}]*)\}")


class EnricherSettings(BaseSettings):
    """Enricher (ARCHITECTURE §4.3, ADR-0019). Sem regras até a Q-1; REST desligada (Q-2)."""

    model_config = _config("ENRICHER_")

    prefetch: int = Field(default=5, ge=1, le=1000)
    max_attempts: int = Field(default=3, ge=1)  # falhas da mensagem antes da DLQ
    retry_delay_s: float = Field(default=2.0, ge=0)
    transient_backoff_initial_s: float = Field(default=5.0, gt=0)
    transient_backoff_max_s: float = Field(default=60.0, gt=0)
    # Disjuntor: mensagens seguidas com a mesma falha = falha do sistema, não vão para a DLQ.
    systemic_failure_threshold: int = Field(default=5, ge=2)
    # Routing keys ligadas à fila ohip.enricher em ohip.events (vazio até a Q-1).
    bindings: Annotated[list[str], NoDecode] = Field(default_factory=list)
    dedup_ttl_s: int = Field(default=7 * 86_400, ge=60)  # ohip:processed:<message_id>
    # REST do OHIP (RF-09)
    rest_enabled: bool = False
    rest_timeout_s: float = Field(default=10.0, gt=0)
    rest_rate_per_s: float = Field(default=5.0, gt=0)
    rest_burst: int = Field(default=5, ge=1)
    rest_cache_ttl_s: int = Field(default=45, ge=1)
    rest_auth_rejected_alert: int = Field(default=5, ge=1)  # recusas seguidas do OAuth
    # moduleName (sem diferenciar maiúsculas) → caminho com {hotelId} e {primaryKey}
    # (docs/OHIP_APIS.md §4: getReservation e getProfile).
    resource_paths: dict[str, str] = Field(
        default_factory=lambda: {
            "reservation": "/rsv/v1/hotels/{hotelId}/reservations/{primaryKey}",
            "profile": "/crm/v1/profiles/{primaryKey}",
        }
    )

    @field_validator("bindings", mode="before")
    @classmethod
    def _parse_bindings(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("bindings")
    @classmethod
    def _validate_bindings(cls, value: list[str]) -> list[str]:
        for key in value:
            if not _ROUTING_KEY_RE.fullmatch(key):
                raise ValueError(f"routing key inválida em ENRICHER_BINDINGS: {key!r}")
        return value

    @field_validator("resource_paths")
    @classmethod
    def _validate_paths(cls, value: dict[str, str]) -> dict[str, str]:
        paths: dict[str, str] = {}
        for module, path in value.items():
            fields = set(_PATH_FIELDS_RE.findall(path))
            if not path.startswith("/") or "primaryKey" not in fields:
                raise ValueError(
                    f"ENRICHER_RESOURCE_PATHS[{module}] deve começar com / e ter {{primaryKey}}"
                )
            if fields - {"hotelId", "primaryKey"}:
                raise ValueError(
                    f"ENRICHER_RESOURCE_PATHS[{module}]: só {{hotelId}} e {{primaryKey}}"
                )
            paths[module.strip().lower()] = path
        return paths

    @model_validator(mode="after")
    def _validate_backoff(self) -> Self:
        if self.transient_backoff_max_s < self.transient_backoff_initial_s:
            raise ValueError("transient_backoff_max_s deve ser >= transient_backoff_initial_s")
        return self


class RedisSettings(BaseSettings):
    """Redis: token, caches e métricas — nunca travas (ADR-0008)."""

    model_config = _config("REDIS_")

    url: SecretStr  # redis(s)://:senha@host:porta/db
    seen_ttl_s: int = Field(default=86_400, ge=60)
    api_status_ttl_s: int = Field(default=5, ge=1)
    metrics_ttl_s: int = Field(default=60, ge=15)

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL deve começar com redis:// ou rediss://")
        return value


class ConsumerSettings(BaseSettings):
    """Fila interna, micro-lotes e lease do consumer (ARCHITECTURE §4.1, ADR-0008)."""

    model_config = _config("CONSUMER_")

    intake_queue_max: int = Field(default=5_000, ge=1)
    intake_queue_max_bytes: int = Field(default=64 * MIB, ge=1 * MIB)
    # < 180 s: o servidor fecha a conexão se não vir pong em 180 s (ARCHITECTURE §4.1).
    intake_stall_timeout_s: float = Field(default=120.0, gt=0, lt=180)
    batch_max_events: int = Field(default=200, ge=1)
    batch_max_wait_ms: int = Field(default=200, ge=1)
    batch_max_retries: int = Field(default=5, ge=0)
    control_poll_interval_s: float = Field(default=5.0, gt=0)


class LeaseSettings(BaseSettings):
    """Lease com prazo e epoch no Oracle, comum a consumer e publisher (ADR-0008, ADR-0014)."""

    model_config = _config("LEASE_")

    ttl_s: float = Field(default=30.0, ge=10)
    renew_interval_s: float = Field(default=10.0, gt=0)
    acquire_jitter_s: float = Field(default=5.0, ge=0)  # espera extra aleatória entre tentativas

    @model_validator(mode="after")
    def _validate_lease(self) -> Self:
        # Pelo menos duas renovações cabem no prazo: uma falha isolada não derruba o lease.
        if self.renew_interval_s * 2 > self.ttl_s:
            raise ValueError("renew_interval_s deve ser <= ttl_s / 2")
        return self


TokenRole = Literal["read", "admin"]


class ApiSettings(BaseSettings):
    """API de controle (ADR-0017). Tokens de serviço: SHA-256 (hex) do token → perfil; nunca
    o token em claro."""

    model_config = _config("API_")

    service_tokens: dict[str, TokenRole] = Field(default_factory=dict)
    host: str = "127.0.0.1"  # atrás do Nginx (TLS e bloqueio de /health, /ready, /metrics)
    port: int = Field(default=8080, ge=1, le=65535)
    workers: int = Field(default=2, ge=1)
    # Prazo de cada chamada ao Oracle feita pela API, menor que o do consumer: uma busca cara
    # não segura uma conexão do pool (ORACLE_POOL_MAX por worker) por muito tempo.
    oracle_call_timeout_ms: int = Field(default=15_000, ge=1_000)
    metrics_cache_s: float = Field(default=5.0, ge=0)
    ready_cache_s: float = Field(default=5.0, ge=0)

    @field_validator("service_tokens")
    @classmethod
    def _validate_hashes(cls, value: dict[str, TokenRole]) -> dict[str, TokenRole]:
        for token_hash in value:
            if not _SHA256_HEX_RE.fullmatch(token_hash):
                raise ValueError("API_SERVICE_TOKENS deve mapear SHA-256 em hex minúsculo → perfil")
        return value


class LogSettings(BaseSettings):
    model_config = _config("LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    json_output: bool = True  # False: saída legível, só em desenvolvimento

    @field_validator("level", mode="before")
    @classmethod
    def _upper_level(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value


SettingsT = TypeVar("SettingsT", bound=BaseSettings)


@cache
def load_settings(settings_cls: type[SettingsT]) -> SettingsT:
    """Carrega (uma vez por processo) um grupo de configuração.

    Uso nos entrypoints: ``ohip = load_settings(OhipSettings)``. Em caso de ``ValidationError``,
    logar ``str(exc)`` (sem o valor recebido); nunca ``exc.errors()`` sem ``include_input=False``.
    """
    return settings_cls()
