"""Ponte entre as rotas ``def`` e os casos de uso assíncronos (ADR-0017).

A rota roda num thread do threadpool do FastAPI. ``run_sync`` executa o caso de uso com
``asyncio.run`` **nesse thread**; os stores da API usam ``InlineSession``, então o Oracle é
chamado no próprio thread da requisição, nunca no event loop do Uvicorn (ADR-0003).
O contexto de log (``request_id``) segue junto: ``asyncio.run`` copia os contextvars.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def run_sync(coroutine: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coroutine)
