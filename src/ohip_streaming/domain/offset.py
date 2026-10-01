"""Offset do OHIP (ADR-0006).

O guia diz que o offset "é uma string, não um número"; o schema valida ``^[0-9]+$`` com até 20
caracteres. Guardamos e enviamos sempre a string original. ``numeric()`` existe só para os
usos permitidos pelo ADR-0006: métricas, detecção de retrocesso, validação de replay e a
condição do ``MERGE`` do enricher.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from ohip_streaming.domain.errors import InvalidOffsetError

OFFSET_RE: Final = re.compile(r"^[0-9]{1,20}$")


@dataclass(frozen=True, slots=True)
class Offset:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not OFFSET_RE.fullmatch(self.value):
            raise InvalidOffsetError(f"offset fora de ^[0-9]{{1,20}}$: {self.value!r}")

    @classmethod
    def parse(cls, raw: object) -> Offset:
        """Aceita string ou inteiro não negativo (exemplos do guia trazem os dois formatos)."""
        if isinstance(raw, bool):
            raise InvalidOffsetError(f"offset inválido: {raw!r}")
        if isinstance(raw, int):
            if raw < 0:
                raise InvalidOffsetError(f"offset negativo: {raw!r}")
            return cls(str(raw))
        if isinstance(raw, str):
            return cls(raw)
        raise InvalidOffsetError(f"offset de tipo inesperado: {type(raw).__name__}")

    def numeric(self) -> int:
        return int(self.value)

    def __str__(self) -> str:
        return self.value
