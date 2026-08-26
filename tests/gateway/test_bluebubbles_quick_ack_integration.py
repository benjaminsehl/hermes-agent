from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import Platform, SessionResetPolicy
from gateway.platforms.base import MessageEvent, MessageType
from gateway.run import GatewayRunner
from gateway.session import SessionSource


def _event(platform=Platform.BLUEBUBBLES):
    source = SessionSource(platform=platform, chat_id="chat", user_id="user")
    return MessageEvent(
        text="Please check this",
        message_type=MessageType.TEXT,
        source=source,
    )


@pytest.mark.asyncio
async def test_bluebubbles_quick_ack_is_sent_and_added_to_turn_sidecar():
    event = _event()
    adapter = SimpleNamespace(
        maybe_send_quick_ack=AsyncMock(return_value="I'm checking that now.")
    )
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._adapter_for_source = lambda source: adapter
    notes = []
    config = {"display": {"platforms": {"bluebubbles": {"quick_ack_enabled": True}}}}

    with patch("gateway.run._load_gateway_config", return_value=config):
        ack = await runner._maybe_send_bluebubbles_quick_ack(
            event, event.source, event.text, notes
        )

    assert ack == "I'm checking that now."
    adapter.maybe_send_quick_ack.assert_awaited_once_with(event, event.text, config)
    assert len(notes) == 1
    assert "visible quick acknowledgment" in notes[0]
    assert "Do not repeat it" in notes[0]


@pytest.mark.asyncio
async def test_quick_ack_integration_is_bluebubbles_only():
    event = _event(Platform.TELEGRAM)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._adapter_for_source = lambda source: (_ for _ in ()).throw(
        AssertionError("adapter lookup should not run")
    )
    notes = []

    assert (
        await runner._maybe_send_bluebubbles_quick_ack(
            event, event.source, event.text, notes
        )
        is None
    )
    assert notes == []


@pytest.mark.parametrize("reason", ["idle", "daily", "suspended", "resume_pending_expired"])
def test_notify_false_suppresses_every_auto_reset_reason(reason):
    policy = SessionResetPolicy(notify=False)

    assert not GatewayRunner._should_send_auto_reset_notification(
        policy=policy,
        reset_reason=reason,
        platform_name="bluebubbles",
        had_activity=True,
    )


def test_platform_exclusion_suppresses_forced_reset_notice():
    policy = SessionResetPolicy(
        notify=True,
        notify_exclude_platforms=("bluebubbles",),
    )

    assert not GatewayRunner._should_send_auto_reset_notification(
        policy=policy,
        reset_reason="resume_pending_expired",
        platform_name="bluebubbles",
        had_activity=True,
    )
