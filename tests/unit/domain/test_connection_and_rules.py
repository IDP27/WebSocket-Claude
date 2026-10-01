"""Políticas do ciclo de vida da conexão (ADR-0007) e regras de lote, publicação e replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ohip_streaming.domain import connection as c
from ohip_streaming.domain.errors import (
    ReplayConfirmationError,
    ReplayForwardNotAllowedError,
    ReplayOffsetInvalidError,
    ReplayReasonRequiredError,
)
from ohip_streaming.domain.events import RejectedMessage
from ohip_streaming.domain.offset import Offset
from ohip_streaming.domain.rules import (
    is_allowed,
    offset_to_persist,
    publish_retry_delay_s,
    replay_retention_warning,
    validate_replay_request,
)

POLICY = c.ReconnectPolicy()
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def zero() -> float:
    return 0.0


def almost_one() -> float:
    return 0.999


# ------------------------------------------------------------------ fechamentos


@pytest.mark.parametrize(
    ("code", "action", "wait"),
    [
        (1000, c.CloseAction.RECONNECT, 10),
        (4401, c.CloseAction.REFRESH_TOKEN_AND_RECONNECT, 10),
        (4403, c.CloseAction.STOP, 10),
        (4406, c.CloseAction.STOP, 10),
        (4409, c.CloseAction.RECONNECT, 120),
        (4504, c.CloseAction.RECONNECT, 15),
        (4408, c.CloseAction.RECONNECT, 10),
        (None, c.CloseAction.RECONNECT, 10),
        (1006, c.CloseAction.RECONNECT, 10),
    ],
)
def test_close_codes_follow_oracle_guide(
    code: int | None, action: c.CloseAction, wait: float
) -> None:
    decision = c.decide_on_close(code, 1, POLICY, zero)
    assert decision.action is action
    assert decision.wait_s == wait


@pytest.mark.parametrize("code", [4403, 4406])
def test_stop_codes_raise_critical_alert(code: int) -> None:
    assert c.decide_on_close(code, 1, POLICY, zero).alert is c.AlertLevel.CRITICAL


def test_4409_waits_lockout_plus_jitter() -> None:
    decision = c.decide_on_close(4409, 1, POLICY, almost_one)
    assert 120 <= decision.wait_s < 150
    assert decision.alert is None
    assert c.decide_on_close(4409, 5, POLICY, zero).alert is c.AlertLevel.WARNING


def test_network_backoff_is_exponential_with_cap_and_never_below_10s() -> None:
    waits = [c.decide_on_close(None, n, POLICY, zero).wait_s for n in range(1, 7)]
    assert waits == [10, 20, 40, 60, 60, 60]
    assert c.decide_on_close(None, 1, POLICY, almost_one).wait_s == pytest.approx(11.998)


def test_repeated_unauthorized_raises_alert() -> None:
    assert c.decide_on_close(4401, 2, POLICY, zero).alert is None
    assert c.decide_on_close(4401, 3, POLICY, zero).alert is c.AlertLevel.WARNING


def test_oversize_stops_after_repeats() -> None:
    assert c.decide_on_oversize(1, POLICY).action is c.CloseAction.RECONNECT
    stop = c.decide_on_oversize(3, POLICY)
    assert stop.action is c.CloseAction.STOP
    assert stop.alert is c.AlertLevel.CRITICAL


# ------------------------------------------------------------------ regra dos 10 s


@pytest.mark.parametrize(
    ("last_disconnect", "state", "expected"),
    [
        (None, None, 10),  # primeira partida
        (NOW - timedelta(seconds=3), c.ConsumerState.WAITING, 7),
        (NOW - timedelta(seconds=30), c.ConsumerState.STOPPED, 0),
        (
            NOW - timedelta(seconds=30),
            c.ConsumerState.SUBSCRIBED,
            10,
        ),  # crash: horário desconhecido
        (NOW - timedelta(seconds=30), None, 10),
    ],
)
def test_wait_before_connect(
    last_disconnect: datetime | None, state: c.ConsumerState | None, expected: float
) -> None:
    wait = c.wait_before_connect(
        db_now=NOW, last_disconnect_at=last_disconnect, last_state=state, min_gap_s=10
    )
    assert wait == expected


def test_token_refresh_due() -> None:
    expires = NOW + timedelta(minutes=10)
    assert not c.token_refresh_due(now=NOW, expires_at=expires, margin_s=300)
    assert c.token_refresh_due(now=NOW + timedelta(minutes=5), expires_at=expires, margin_s=300)


def test_rtt_estimator() -> None:
    rtt = c.RttEstimator()
    assert rtt.liveness_timeout_s(min_timeout_s=180) == 180
    rtt.update(80)
    assert rtt.srtt_s == 80
    rtt.update(0)
    assert rtt.srtt_s == pytest.approx(70)
    assert rtt.liveness_timeout_s(min_timeout_s=180, jitter_s=5) == pytest.approx(285)
    rtt.update(-1)  # amostra inválida não deixa o SRTT negativo
    assert rtt.srtt_s is not None
    assert rtt.srtt_s > 0


# ------------------------------------------------------------------ lote e publicação


def _rejected(offset: str | None) -> RejectedMessage:
    return RejectedMessage(
        raw="x", reason="r", received_at=NOW, offset=Offset(offset) if offset else None
    )


def test_offset_to_persist_uses_last_valid_offset_in_arrival_order() -> None:
    assert offset_to_persist([_rejected("5"), _rejected("6"), _rejected(None)]) == Offset("6")
    assert offset_to_persist([_rejected(None)]) is None
    assert offset_to_persist([]) is None


def test_is_allowed() -> None:
    assert is_allowed("NEW PROFILE", frozenset())
    assert is_allowed("new  profile", frozenset({"NEW PROFILE"}))
    assert not is_allowed("UPDATE PROFILE", frozenset({"NEW PROFILE"}))


def test_publish_retry_delays_add_up_to_about_30_minutes() -> None:
    delays = [publish_retry_delay_s(n) for n in range(1, 10)]
    assert delays[:6] == [10, 20, 40, 80, 160, 300]
    assert 25 * 60 <= sum(delays) <= 35 * 60


# ------------------------------------------------------------------ replay


def _replay(**overrides: object) -> Offset:
    args: dict[str, object] = {
        "chain_code": "CHAIN1",
        "from_offset_raw": "90",
        "confirm": "CHAIN1",
        "reason": "lacuna INC-1",
        "last_offset": Offset("100"),
    }
    args.update(overrides)
    return validate_replay_request(**args)  # type: ignore[arg-type]


def test_valid_replay_request() -> None:
    assert _replay() == Offset("90")
    assert _replay(from_offset_raw="100") == Offset("100")


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"confirm": "CHAIN2"}, ReplayConfirmationError),
        ({"reason": "  "}, ReplayReasonRequiredError),
        ({"reason": "x" * 501}, ReplayReasonRequiredError),
        ({"from_offset_raw": "-1"}, ReplayOffsetInvalidError),
        ({"from_offset_raw": "101"}, ReplayForwardNotAllowedError),
        ({"last_offset": None}, ReplayForwardNotAllowedError),
    ],
)
def test_invalid_replay_requests(overrides: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        _replay(**overrides)


def test_replay_retention_warning() -> None:
    assert replay_retention_warning(NOW - timedelta(days=1), NOW) is None
    assert "7 dias" in (replay_retention_warning(NOW - timedelta(days=8), NOW) or "")
    assert replay_retention_warning(None, NOW) is not None
