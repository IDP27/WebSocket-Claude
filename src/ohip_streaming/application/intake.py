"""Fila interna entre o laço de leitura e a tarefa de gravação (ARCHITECTURE §4.1).

Limitada por quantidade **e** por bytes. Cheia, o ``put`` **espera** (o laço para de ler o
socket); nunca descarta. Quem decide envenenar a conexão depois de esperar demais é o
consumer (``INTAKE_STALL_TIMEOUT``).
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class IntakeItem:
    raw: str
    received_at: datetime
    size: int


class IntakeQueue:
    def __init__(self, *, max_items: int, max_bytes: int) -> None:
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._items: deque[IntakeItem] = deque()
        self._bytes = 0
        self._closed = False
        self._changed = asyncio.Condition()

    def __len__(self) -> int:
        return len(self._items)

    @property
    def bytes(self) -> int:
        return self._bytes

    def _has_room(self, size: int) -> bool:
        if not self._items:  # item maior que o limite de bytes ainda entra sozinho
            return True
        return len(self._items) < self._max_items and self._bytes + size <= self._max_bytes

    async def put(self, item: IntakeItem) -> None:
        async with self._changed:
            await self._changed.wait_for(lambda: self._closed or self._has_room(item.size))
            if self._closed:
                return
            self._items.append(item)
            self._bytes += item.size
            self._changed.notify_all()

    async def get_batch(self, max_items: int, max_wait_s: float) -> list[IntakeItem]:
        """Espera o primeiro item; depois junta até ``max_items`` por até ``max_wait_s``.

        Devolve ``[]`` quando a fila foi fechada e está vazia.
        """
        async with self._changed:
            await self._changed.wait_for(lambda: bool(self._items) or self._closed)
            if not self._items:
                return []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max_wait_s
        async with self._changed:
            while len(self._items) < max_items and not self._closed:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self._changed.wait(), remaining)
                except TimeoutError:
                    break
            batch = [self._items.popleft() for _ in range(min(max_items, len(self._items)))]
            self._bytes -= sum(i.size for i in batch)
            self._changed.notify_all()
            return batch

    async def close(self) -> None:
        """Nada mais entra; o que já está na fila ainda sai (drenagem)."""
        async with self._changed:
            self._closed = True
            self._changed.notify_all()

    async def discard(self) -> int:
        """Conexão envenenada: descarta o que não foi gravado e fecha. Devolve quantos."""
        async with self._changed:
            dropped = len(self._items)
            self._items.clear()
            self._bytes = 0
            self._closed = True
            self._changed.notify_all()
            return dropped
