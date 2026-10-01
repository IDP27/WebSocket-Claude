"""Configuração por ambiente: carga, validações do protocolo e proteção de segredos."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from ohip_streaming import config
from ohip_streaming.config import (
    ApiSettings,
    AppSettings,
    AuthMode,
    ConsumerSettings,
    Environment,
    LogSettings,
    OhipSettings,
    OracleSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)

ROOT = Path(__file__).resolve().parents[2]

OHIP_MIN_ENV = {
    "OHIP_GATEWAY_URL": "https://gateway.example.com/",
    "OHIP_APP_KEY": "app-key-secreta",
    "OHIP_CLIENT_ID": "cliente",
    "OHIP_CLIENT_SECRET": "segredo-do-cliente",
    "OHIP_ENTERPRISE_ID": "ENT1",
    "OHIP_CHAIN_CODE": "CHAIN1",
}


def set_env(monkeypatch: pytest.MonkeyPatch, **values: str) -> None:
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def ohip(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> OhipSettings:
    set_env(monkeypatch, **{**OHIP_MIN_ENV, **overrides})
    return OhipSettings()


# ----------------------------------------------------------------------- OHIP


def test_ohip_defaults_follow_oracle_guide(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = ohip(monkeypatch)

    assert settings.gateway_url == "https://gateway.example.com"  # barra final removida
    assert settings.auth_mode is AuthMode.CLIENT_CREDENTIALS
    assert settings.ping_interval_s == 15
    assert settings.reconnect_min_gap_s == 10
    assert settings.lockout_4409_s == 120
    assert settings.backoff_4504_s == 15
    assert settings.status_check_enabled is False
    assert settings.hotel_codes == []
    assert settings.event_allowlist == []


def test_ohip_secrets_never_appear_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = ohip(monkeypatch)
    text = f"{settings!r} {settings!s} {settings.model_dump()}"

    assert "app-key-secreta" not in text
    assert "segredo-do-cliente" not in text
    assert settings.app_key.get_secret_value() == "app-key-secreta"


@pytest.mark.parametrize(
    ("settings_cls", "variable", "value"),
    [
        (RabbitMQSettings, "RABBITMQ_URL", "http://user:SENHA_VAZADA@broker"),
        (RedisSettings, "REDIS_URL", "memcached://:SENHA_VAZADA@cache"),
        (ApiSettings, "API_SERVICE_TOKENS", '{"TOKEN_SENHA_VAZADA": "admin"}'),
        (OhipSettings, "OHIP_GATEWAY_URL", "http://SENHA_VAZADA@gateway"),
    ],
)
def test_validation_errors_do_not_echo_input(
    monkeypatch: pytest.MonkeyPatch, settings_cls: type[BaseSettings], variable: str, value: str
) -> None:
    set_env(monkeypatch, **{**OHIP_MIN_ENV, variable: value})

    with pytest.raises(ValidationError) as error:
        settings_cls()

    assert "SENHA_VAZADA" not in str(error.value)
    # errors() devolve a entrada por padrão; os entrypoints logam str(exc) ou include_input=False.
    assert "SENHA_VAZADA" not in repr(error.value.errors(include_input=False))


def test_ohip_reads_dotenv_file(tmp_path: Path) -> None:
    lines = [f"{key}={value}" for key, value in OHIP_MIN_ENV.items()]
    (tmp_path / ".env").write_text("\n".join([*lines, "OUTRA_VARIAVEL=ignorada"]), encoding="utf-8")

    assert OhipSettings().chain_code == "CHAIN1"


def test_environment_variable_wins_over_dotenv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lines = [f"{key}={value}" for key, value in OHIP_MIN_ENV.items()]
    (tmp_path / ".env").write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setenv("OHIP_CHAIN_CODE", "CHAIN2")

    assert OhipSettings().chain_code == "CHAIN2"


def test_ohip_parses_csv_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = ohip(
        monkeypatch,
        OHIP_HOTEL_CODES=" HOTEL1 , HOTEL2,",
        OHIP_EVENT_ALLOWLIST="new  reservation, UPDATE RESERVATION",
    )

    assert settings.hotel_codes == ["HOTEL1", "HOTEL2"]
    assert settings.event_allowlist == ["NEW RESERVATION", "UPDATE RESERVATION"]


def test_ohip_module_codes_are_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = ohip(monkeypatch, OHIP_MODULE_CODES='{"Reservation": "rsv", "PROFILE": " crm "}')

    assert settings.module_codes == {"reservation": "rsv", "profile": "crm"}


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("OHIP_GATEWAY_URL", "http://gateway.example.com"),
        ("OHIP_GATEWAY_URL", "https://"),
        ("OHIP_CHAIN_CODE", "CHAIN/1"),
        ("OHIP_CHAIN_CODE", "C" * 21),
        ("OHIP_HOTEL_CODES", "HOTEL1,HOT/EL2"),
        ("OHIP_HOTEL_CODES", ",".join(["HOTEL1234"] * 6)),  # > 50 caracteres no total
        ("OHIP_EVENT_TIMEZONE", "Lua/Base_Alfa"),
        ("OHIP_OAUTH_TOKEN_PATH", "oauth/v1/tokens"),
        # Limites do protocolo (ADR-0007): valores que provocariam 4408/4409.
        ("OHIP_PING_INTERVAL_S", "20"),
        ("OHIP_RECONNECT_MIN_GAP_S", "5"),
        ("OHIP_LOCKOUT_4409_S", "60"),
        ("OHIP_BACKOFF_INITIAL_S", "1"),
        ("OHIP_STATUS_CHECK_MIN_INTERVAL_S", "10"),
        ("OHIP_WS_MAX_MESSAGE_BYTES", "1000"),
    ],
)
def test_ohip_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    with pytest.raises(ValidationError):
        ohip(monkeypatch, **{variable: value})


def test_ohip_client_credentials_requires_enterprise_id(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError, match="OHIP_ENTERPRISE_ID"):
        ohip(monkeypatch, OHIP_ENTERPRISE_ID="")


def test_ohip_resource_owner_requires_integration_user(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError, match="INTEGRATION_USERNAME"):
        ohip(monkeypatch, OHIP_AUTH_MODE="resource_owner")

    settings = ohip(
        monkeypatch,
        OHIP_AUTH_MODE="resource_owner",
        OHIP_INTEGRATION_USERNAME="integracao",
        OHIP_INTEGRATION_PASSWORD="senha",
    )
    assert settings.auth_mode is AuthMode.RESOURCE_OWNER


def test_ohip_backoff_max_must_not_be_below_initial(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError, match="backoff_max_s"):
        ohip(monkeypatch, OHIP_BACKOFF_INITIAL_S="30", OHIP_BACKOFF_MAX_S="20")


def test_ohip_missing_required_fields_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValidationError):
        OhipSettings()


# ----------------------------------------------------------------------- demais grupos


def test_app_settings_defaults() -> None:
    settings = AppSettings()
    assert settings.environment is Environment.DESENVOLVIMENTO
    assert settings.code_version == "dev"


def test_oracle_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    set_env(monkeypatch, ORACLE_DSN="db:1521/svc", ORACLE_USER="ohip_app", ORACLE_PASSWORD="pwd")
    settings = OracleSettings()

    assert settings.client_lib_dir is None
    assert "pwd" not in repr(settings)

    monkeypatch.setenv("ORACLE_POOL_MIN", "5")
    monkeypatch.setenv("ORACLE_POOL_MAX", "2")
    with pytest.raises(ValidationError, match="pool_max"):
        OracleSettings()


def test_rabbitmq_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBITMQ_URL", "amqps://user:senha-rabbit@broker/vhost")
    settings = RabbitMQSettings()

    assert settings.events_exchange == "ohip.events"
    assert settings.reprocess_exchange == "ohip.reprocess"
    assert "senha-rabbit" not in repr(settings)

    monkeypatch.setenv("RABBITMQ_URL", "http://broker")
    with pytest.raises(ValidationError, match="amqp"):
        RabbitMQSettings()


def test_redis_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_URL", "rediss://:senha-redis@cache:6380/0")
    assert "senha-redis" not in repr(RedisSettings())

    monkeypatch.setenv("REDIS_URL", "memcached://cache")
    with pytest.raises(ValidationError, match="redis"):
        RedisSettings()


def test_consumer_lease_renewal_must_fit_twice_in_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ConsumerSettings().lease_ttl_s == 30

    set_env(monkeypatch, CONSUMER_LEASE_TTL_S="30", CONSUMER_LEASE_RENEW_INTERVAL_S="20")
    with pytest.raises(ValidationError, match="lease_renew_interval_s"):
        ConsumerSettings()


def test_api_service_tokens_accept_only_sha256(monkeypatch: pytest.MonkeyPatch) -> None:
    token_hash = "a" * 64
    monkeypatch.setenv("API_SERVICE_TOKENS", f'{{"{token_hash}": "admin"}}')
    assert ApiSettings().service_tokens == {token_hash: "admin"}

    monkeypatch.setenv("API_SERVICE_TOKENS", '{"token-em-claro": "admin"}')
    with pytest.raises(ValidationError, match="SHA-256"):
        ApiSettings()

    monkeypatch.setenv("API_SERVICE_TOKENS", f'{{"{token_hash}": "root"}}')
    with pytest.raises(ValidationError):
        ApiSettings()


def test_log_settings_defaults() -> None:
    settings = LogSettings()
    assert settings.level == "INFO"
    assert settings.json_output is True


def test_load_settings_is_cached_per_class(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_CODE_VERSION", "abc123")
    first = load_settings(AppSettings)
    monkeypatch.setenv("APP_CODE_VERSION", "outro")

    assert load_settings(AppSettings) is first
    assert first.code_version == "abc123"


# ----------------------------------------------------------------------- .env.example


def _settings_classes() -> list[type[BaseSettings]]:
    return [
        value
        for value in vars(config).values()
        if isinstance(value, type) and issubclass(value, BaseSettings) and value is not BaseSettings
    ]


def _env_name(settings_cls: type[BaseSettings], field: str) -> str:
    prefix: Any = settings_cls.model_config.get("env_prefix", "")
    return f"{prefix}{field}".upper()


def test_env_example_documents_every_setting() -> None:
    """Toda variável de configuração aparece no .env.example (ativa ou comentada)."""
    documented = set(
        re.findall(r"^#?\s*([A-Z][A-Z0-9_]*)=", (ROOT / ".env.example").read_text(), re.MULTILINE)
    )
    expected = {_env_name(cls, field) for cls in _settings_classes() for field in cls.model_fields}

    assert expected - documented == set()
    assert documented - expected == set()


def test_env_example_has_no_inline_comments() -> None:
    """O EnvironmentFile do systemd não aceita comentário na mesma linha do valor."""
    for line in (ROOT / ".env.example").read_text().splitlines():
        if re.match(r"^[A-Z][A-Z0-9_]*=", line):
            assert " #" not in line, line
