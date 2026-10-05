"""Redis síncrono falso (o subconjunto de ``redis.Redis`` que a API usa)."""

from __future__ import annotations

from typing import Any


class FakeSyncRedis:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
        self.expires: dict[bytes, int] = {}
        self.fail = False
        self.closed = False
        self.sets: dict[bytes, set[bytes]] = {}

    def _check(self) -> None:
        if self.fail:
            raise ConnectionError("redis fora")

    def get(self, name: Any) -> Any:
        self._check()
        return self.data.get(name.encode())

    def set(self, name: Any, value: Any, *args: Any, **kwargs: Any) -> Any:
        self._check()
        self.data[name.encode()] = value
        self.expires[name.encode()] = kwargs["ex"]
        return True

    def smembers(self, name: Any) -> Any:
        self._check()
        return set(self.sets.get(name.encode(), set()))

    def srem(self, name: Any, *values: Any) -> Any:
        self._check()
        members = self.sets.get(name.encode(), set())
        removed = len(members & set(values))
        members.difference_update(values)
        return removed

    def sadd(self, name: str, *values: bytes) -> None:
        self.sets.setdefault(name.encode(), set()).update(values)

    def mget(self, keys: Any, *args: Any) -> Any:
        self._check()
        return [self.data.get(k) for k in keys]

    def ping(self, **kwargs: Any) -> Any:
        self._check()
        return True

    def close(self) -> None:
        self.closed = True
