"""Identificadores do OHIP e limites de tamanho iguais aos da DDL (sql/002_core_tables.sql).

Padrões do schema GraphQL oficial (StreamingGraphQLSchema.json).
"""

from __future__ import annotations

import re
from typing import Final

from ohip_streaming.domain.errors import InvalidIdentifierError

CHAIN_CODE_RE: Final = re.compile(r"^[A-Za-z0-9 _%#$&-]{1,20}$")
HOTEL_CODE_RE: Final = re.compile(r"^[A-Za-z0-9 _%#$&-]+$")
HOTEL_CODES_MAX_LEN: Final = 50  # hotelCode é StringWithLength50 (lista por vírgula, D-9)

# Limites das colunas de OHIP_EVENT_RAW. Validados no domínio para que a mensagem ruim vá
# para a DLQ antes do banco (ADR-0009).
MAX_UNIQUE_EVENT_ID: Final = 64
MAX_MODULE_NAME: Final = 100
MAX_EVENT_NAME: Final = 100
MAX_PRIMARY_KEY: Final = 100
MAX_HOTEL_ID: Final = 50
MAX_PUBLISHER_ID: Final = 50
MAX_ACTION_INSTANCE_ID: Final = 50
MAX_SUBSCRIPTION_ID: Final = 36


def validate_chain_code(value: str) -> str:
    if not CHAIN_CODE_RE.fullmatch(value):
        raise InvalidIdentifierError(f"chain_code fora do padrão do OHIP: {value!r}")
    return value


def validate_hotel_codes(values: list[str]) -> list[str]:
    for code in values:
        if not HOTEL_CODE_RE.fullmatch(code):
            raise InvalidIdentifierError(f"hotel_code inválido: {code!r}")
    if len(",".join(values)) > HOTEL_CODES_MAX_LEN:
        raise InvalidIdentifierError(f"hotel_codes excede {HOTEL_CODES_MAX_LEN} caracteres")
    return values


def normalize_event_name(value: str) -> str:
    """Normaliza espaços e caixa: eventName chega como "UPDATE RESERVATION"."""
    return " ".join(value.split()).upper()
