"""Geradores de frames ``next`` do OHIP para testes (formato do guia Oracle)."""

from __future__ import annotations

import json
from typing import Any

SUBSCRIPTION_ID = "2b0e8f6e-1d1a-4c1e-9f57-8c3f0f6a1a01"


def new_event(
    offset: str | int = "100",
    unique_event_id: str = "uid-100",
    *,
    module_name: str = "RESERVATION",
    event_name: str = "UPDATE RESERVATION",
    primary_key: str = "123456",
    hotel_id: str | None = "HOTEL1",
    timestamp: str | None = "2026-10-01 11:59:57.900",
    detail: list[dict[str, Any]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "metadata": {"offset": offset, "uniqueEventId": unique_event_id},
        "moduleName": module_name,
        "eventName": event_name,
        "primaryKey": primary_key,
        "hotelId": hotel_id,
        "timestamp": timestamp,
        "publisherId": "15951",
        "actionInstanceId": "222222",
        "detail": detail
        if detail is not None
        else [{"elementName": "ARRIVAL DATE", "oldValue": "2026-10-10", "newValue": "2026-10-11"}],
    }
    event.update(extra)
    return event


def next_frame(event: dict[str, Any], subscription_id: str = SUBSCRIPTION_ID) -> str:
    return json.dumps(
        {"id": subscription_id, "type": "next", "payload": {"data": {"newEvent": event}}}
    )


def frame(offset: str | int, uid: str | None = None, **kwargs: Any) -> str:
    return next_frame(new_event(offset, uid or f"uid-{offset}", **kwargs))
