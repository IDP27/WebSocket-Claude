"""Configuração do painel (ADR-0018). O painel não importa ``ohip_streaming.config``
(import-linter, ADR-0004): nunca recebe credenciais do Oracle, do Redis ou do OHIP.

Lê ``ADMIN_*`` e, com classes próprias, ``APP_ENVIRONMENT``/``APP_CODE_VERSION`` e ``LOG_*``
(as mesmas variáveis dos outros processos).
"""

from __future__ import annotations

from enum import StrEnum
from functools import cache
from typing import Literal, Self, TypeVar

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})


def _config(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        hide_input_in_errors=True,  # tokens e chave da sessão não vão para o journald
    )


class PanelEnvironment(StrEnum):
    DESENVOLVIMENTO = "desenvolvimento"
    HOMOLOGACAO = "homologacao"
    PRODUCAO = "producao"


class PanelAppSettings(BaseSettings):
    model_config = _config("APP_")

    environment: PanelEnvironment = PanelEnvironment.DESENVOLVIMENTO
    code_version: str = Field(default="dev", min_length=1, max_length=40)


class PanelLogSettings(BaseSettings):
    model_config = _config("LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    json_output: bool = True

    @field_validator("level", mode="before")
    @classmethod
    def _upper_level(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value


class AdminSettings(BaseSettings):
    """Painel ``ohip-admin``: URL e tokens da API, headers do Nginx e Gunicorn."""

    model_config = _config("ADMIN_")

    api_base_url: str = "http://127.0.0.1:8080"
    api_read_token: SecretStr
    api_admin_token: SecretStr
    api_timeout_s: float = Field(default=10.0, gt=0)
    secret_key: SecretStr = Field(min_length=32)  # assina a sessão (CSRF e mensagens)
    user_header: str = "X-Forwarded-User"  # definido pelo Nginx/SSO (Q-5)
    groups_header: str = "X-Forwarded-Groups"  # grupos separados por vírgula
    admin_group: str = Field(default="ohip-admin", min_length=1)
    read_group: str = Field(default="ohip-read", min_length=1)
    refresh_s: int = Field(default=15, ge=5, le=60)  # atualização dos fragmentos (HTMX)
    # Só o Nginx deve chegar: endereço local, ou segredo compartilhado Nginx → painel
    # (ADR-0018 §2). Com segredo, toda requisição precisa do header ADMIN_PROXY_SECRET_HEADER.
    host: str = "127.0.0.1"
    proxy_secret: SecretStr | None = Field(default=None, min_length=32)
    proxy_secret_header: str = "X-Admin-Proxy-Secret"  # noqa: S105 - nome do header
    port: int = Field(default=8081, ge=1, le=65535)
    workers: int = Field(default=2, ge=1)

    @field_validator("api_base_url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("ADMIN_API_BASE_URL deve começar com http:// ou https://")
        return value

    @model_validator(mode="after")
    def _trusted_proxy_only(self) -> Self:
        # Os headers de usuário só são confiáveis se só o Nginx alcança o painel.
        if self.host not in _LOOPBACK and self.proxy_secret is None:
            raise ValueError(
                "ADMIN_HOST fora do endereço local exige ADMIN_PROXY_SECRET "
                "(segredo compartilhado com o Nginx)"
            )
        return self

    @model_validator(mode="after")
    def _distinct_groups(self) -> Self:
        if self.admin_group == self.read_group:
            raise ValueError("ADMIN_ADMIN_GROUP e ADMIN_READ_GROUP devem ser diferentes")
        if self.api_read_token.get_secret_value() == self.api_admin_token.get_secret_value():
            raise ValueError("ADMIN_API_READ_TOKEN e ADMIN_API_ADMIN_TOKEN devem ser diferentes")
        return self


SettingsT = TypeVar("SettingsT", bound=BaseSettings)


@cache
def load_panel_settings(settings_cls: type[SettingsT]) -> SettingsT:
    return settings_cls()
