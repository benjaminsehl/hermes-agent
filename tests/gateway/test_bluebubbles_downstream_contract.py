"""Required downstream BlueBubbles safety and delivery contracts."""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.bluebubbles import BlueBubblesAdapter
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import GatewayRunner
from gateway.session import SessionSource


def _adapter(monkeypatch, **extra):
    monkeypatch.setenv("BLUEBUBBLES_SERVER_URL", "http://localhost:1234")
    monkeypatch.setenv("BLUEBUBBLES_PASSWORD", "secret")
    return BlueBubblesAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "server_url": "http://localhost:1234",
                "password": "secret",
                "send_read_receipts": False,
                **extra,
            },
        )
    )


def _payload(message_guid, *, event_type="new-message", attachments=None):
    return {
        "type": event_type,
        "data": {
            "guid": message_guid,
            "text": "hello",
            "handle": {"address": "user@example.com"},
            "isFromMe": False,
            "chatGuid": "iMessage;-;user@example.com",
            "chatIdentifier": "user@example.com",
            "attachments": attachments or [],
        },
    }


class _Request:
    def __init__(self, payload, password="secret"):
        import json

        self.query = {"password": password}
        self.headers = {}
        self._body = json.dumps(payload).encode()

    async def read(self):
        return self._body


@pytest.mark.asyncio
async def test_http_errors_never_expose_query_or_userinfo_in_send_result(monkeypatch):
    adapter = _adapter(monkeypatch, password="query-secret")

    async def fail(_path, _payload):
        request = httpx.Request(
            "POST",
            "http://url-user:url-pass@localhost:1234/api/v1/message/text"
            "?password=query-secret&other=value",
        )
        response = httpx.Response(500, request=request)
        response.raise_for_status()

    monkeypatch.setattr(adapter, "_api_post", fail)
    result = await adapter._post_message("/api/v1/message/text", {})

    assert result.success is False
    assert "query-secret" not in (result.error or "")
    assert "url-user" not in (result.error or "")
    assert "url-pass" not in (result.error or "")
    assert "other=value" not in (result.error or "")


@pytest.mark.asyncio
async def test_http_errors_never_expose_query_or_userinfo_in_logs(monkeypatch, caplog):
    adapter = _adapter(monkeypatch, password="query-secret")
    adapter.client = SimpleNamespace()
    request = httpx.Request(
        "GET",
        "http://url-user:url-pass@localhost:1234/api/v1/webhook"
        "?password=query-secret&other=value",
    )
    response = httpx.Response(500, request=request)
    with pytest.raises(httpx.HTTPStatusError) as raised:
        response.raise_for_status()
    failure = raised.value
    monkeypatch.setattr(adapter, "_find_registered_webhooks", AsyncMock(return_value=[]))
    monkeypatch.setattr(adapter, "_api_post", AsyncMock(side_effect=failure))
    caplog.set_level(logging.DEBUG, logger="gateway.platforms.bluebubbles")

    assert await adapter._register_webhook() is False

    rendered = "\n".join(record.getMessage() for record in caplog.records)
    assert "query-secret" not in rendered
    assert "url-user" not in rendered
    assert "url-pass" not in rendered
    assert "other=value" not in rendered


@pytest.mark.asyncio
async def test_rejected_admission_releases_reservation_and_retry_redispatches(monkeypatch):
    adapter = _adapter(monkeypatch)
    attempts = []

    async def admit_second(event):
        attempts.append(event.message_id)
        event._gateway_accepted = len(attempts) == 2

    monkeypatch.setattr(adapter, "handle_message", admit_second)
    request = _payload("retry-admission")

    first = await adapter._handle_webhook(_Request(request))
    second = await adapter._handle_webhook(_Request(request))

    assert first.status == 503
    assert second.status == 200
    assert attempts == ["retry-admission", "retry-admission"]
    assert adapter._seen_message_guids["retry-admission"]["state"] == "complete"


@pytest.mark.asyncio
async def test_admission_scheduling_failure_releases_reservation_for_retry(monkeypatch):
    adapter = _adapter(monkeypatch)
    attempts = 0

    async def fail_then_admit(event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("scheduler unavailable")
        event._gateway_accepted = True

    monkeypatch.setattr(adapter, "handle_message", fail_then_admit)
    request = _payload("retry-scheduling")

    assert (await adapter._handle_webhook(_Request(request))).status == 503
    assert (await adapter._handle_webhook(_Request(request))).status == 200
    assert attempts == 2


@pytest.mark.asyncio
async def test_lookup_failure_is_not_treated_as_an_empty_webhook_list(monkeypatch):
    adapter = _adapter(monkeypatch)
    adapter.client = SimpleNamespace()
    post = AsyncMock(return_value={"status": 200})
    monkeypatch.setattr(adapter, "_api_get", AsyncMock(side_effect=OSError("offline")))
    monkeypatch.setattr(adapter, "_api_post", post)

    assert await adapter._find_registered_webhooks(adapter._webhook_register_url) is None
    assert await adapter._register_webhook() is False
    post.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_cleanup_fails_when_any_matching_entry_lacks_an_id(monkeypatch):
    adapter = _adapter(monkeypatch)
    delete = AsyncMock()
    adapter.client = SimpleNamespace(delete=delete)

    assert await adapter._delete_webhook_entries([{"url": adapter._webhook_register_url}]) is False
    delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_connect_fails_closed_and_releases_registration_lock(monkeypatch):
    adapter = _adapter(monkeypatch)
    acquired = []
    released = []
    cleaned = []
    runner = SimpleNamespace(cleanup=AsyncMock(side_effect=lambda: cleaned.append(True)))
    client = SimpleNamespace(aclose=AsyncMock())

    monkeypatch.setattr(
        adapter,
        "_acquire_platform_lock",
        lambda scope, identity, desc: acquired.append((scope, identity, desc)) or True,
    )
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: released.append(True))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr(
        adapter,
        "_api_get",
        AsyncMock(
            side_effect=[
                {"status": 200},
                {"data": {"private_api": False, "helper_connected": False}},
            ]
        ),
    )
    monkeypatch.setattr(adapter, "_register_webhook", AsyncMock(return_value=False))
    monkeypatch.setattr(
        "gateway.platforms.shared_ingress.bind_listener",
        AsyncMock(return_value=runner),
    )

    assert await adapter.connect() is False
    assert acquired and acquired[0][0] == "bluebubbles-webhook"
    assert adapter.server_url in acquired[0][1]
    assert released == [True]
    assert cleaned == [True]
    client.aclose.assert_awaited_once()
    assert adapter.is_connected is False


@pytest.mark.asyncio
async def test_registration_lock_rejection_prevents_server_io(monkeypatch):
    adapter = _adapter(monkeypatch)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: False)
    client = MagicMock()
    monkeypatch.setattr(httpx, "AsyncClient", client)

    assert await adapter.connect() is False
    client.assert_not_called()


@pytest.mark.asyncio
async def test_listener_setup_failure_releases_registration_lock(monkeypatch):
    adapter = _adapter(monkeypatch)
    released = []
    client = SimpleNamespace(aclose=AsyncMock())
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: released.append(True))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr(
        adapter,
        "_api_get",
        AsyncMock(
            side_effect=[
                {"status": 200},
                {"data": {"private_api": False, "helper_connected": False}},
            ]
        ),
    )
    monkeypatch.setattr(
        "gateway.platforms.shared_ingress.bind_listener",
        AsyncMock(side_effect=OSError("bind failed")),
    )

    assert await adapter.connect() is False
    assert released == [True]
    client.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_same_process_adapters_contend_and_non_owner_cannot_release_registration(
    monkeypatch,
):
    adapters = [_adapter(monkeypatch) for _ in range(3)]
    clients = [SimpleNamespace(aclose=AsyncMock()) for _ in adapters]
    listeners = [SimpleNamespace(cleanup=AsyncMock()) for _ in adapters]
    lock_releases = []

    for adapter in adapters:
        async def api_get(path):
            if path.endswith("/ping"):
                return {"status": 200}
            return {"data": {"private_api": False, "helper_connected": False}}

        monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *_args: True)
        monkeypatch.setattr(
            adapter,
            "_release_platform_lock",
            lambda: lock_releases.append(True),
        )
        monkeypatch.setattr(adapter, "_api_get", AsyncMock(side_effect=api_get))
        monkeypatch.setattr(adapter, "_register_webhook", AsyncMock(return_value=True))
        monkeypatch.setattr(adapter, "_unregister_webhook", AsyncMock(return_value=False))
    monkeypatch.setattr(httpx, "AsyncClient", MagicMock(side_effect=clients))
    monkeypatch.setattr(
        "gateway.platforms.shared_ingress.bind_listener",
        AsyncMock(side_effect=listeners),
    )

    first, contender, successor = adapters
    try:
        assert await first.connect() is True
        assert await first.connect(is_reconnect=True) is True
        assert await contender.connect() is False

        await contender.disconnect()
        assert await successor.connect() is False
        assert lock_releases == []

        await first.disconnect()
        assert lock_releases == [True]
        assert await successor.connect() is True
    finally:
        for adapter in adapters:
            await adapter.disconnect()


@pytest.mark.asyncio
async def test_completed_cache_obeys_size_and_ttl(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    now = [100.0]
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_CACHE_SIZE", 2)
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_TTL_SECONDS", 5.0)
    monkeypatch.setattr(bluebubbles.time, "monotonic", lambda: now[0])
    adapter = _adapter(monkeypatch)

    for guid in ("one", "two", "three"):
        _, reservation, _ = adapter._reserve_message_delivery(guid, [])
        adapter._complete_message_reservation(
            guid, reservation, reservation["owner_generation"]
        )
    assert list(adapter._seen_message_guids) == ["two", "three"]

    now[0] += 6
    kind, _, _ = adapter._reserve_message_delivery("three", [])
    assert kind == "new"


@pytest.mark.asyncio
async def test_waiter_and_request_join_limits_are_bounded(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    adapter = _adapter(monkeypatch)
    pending = asyncio.get_running_loop().create_future()
    reservation = {
        "outcome": pending,
        "waiters": bluebubbles._MESSAGE_DEDUP_MAX_WAITERS,
    }
    assert await adapter._join_message_reservation(reservation, timeout=0.01) is None

    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_MAX_JOIN_ATTEMPTS", 2)
    settled = asyncio.get_running_loop().create_future()
    settled.set_result(False)
    reservation = {"outcome": settled, "waiters": 0}
    reserve = AsyncMock()
    reserve.side_effect = None
    calls = 0

    def always_wait(*_args):
        nonlocal calls
        calls += 1
        return "duplicate_wait", reservation, []

    monkeypatch.setattr(adapter, "_reserve_message_delivery", always_wait)
    monkeypatch.setattr(adapter, "_join_message_reservation", AsyncMock(return_value=False))
    response = await adapter._handle_webhook(_Request(_payload("join-bound")))

    assert response.status == 503
    assert calls == 3
    assert adapter._join_message_reservation.await_count == 2


@pytest.mark.asyncio
async def test_late_enrichment_rollback_allows_retry(monkeypatch):
    adapter = _adapter(monkeypatch)
    kind, reservation, _ = adapter._reserve_message_delivery("enrich", [])
    assert kind == "new"
    adapter._complete_message_reservation(
        "enrich", reservation, reservation["owner_generation"]
    )

    kind, enrichment, _ = adapter._reserve_message_delivery("enrich", ["attachment"])
    assert kind == "late_enrich"
    adapter._release_message_reservation(
        "enrich", enrichment, enrichment["owner_generation"]
    )

    kind, _, guids = adapter._reserve_message_delivery("enrich", ["attachment"])
    assert kind == "late_enrich"
    assert guids == ["attachment"]


@pytest.mark.asyncio
async def test_expired_owner_is_taken_over_and_waiters_are_released(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    now = [10.0]
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_OWNER_LEASE_SECONDS", 5.0)
    monkeypatch.setattr(bluebubbles.time, "monotonic", lambda: now[0])
    adapter = _adapter(monkeypatch)
    _, stale, _ = adapter._reserve_message_delivery("lease", ["attachment"])

    now[0] = 16.0
    kind, replacement, guids = adapter._reserve_message_delivery(
        "lease", ["attachment"]
    )

    assert kind == "takeover"
    assert replacement is not stale
    assert replacement["owner_generation"] > stale["owner_generation"]
    assert stale["outcome"].result() is False
    assert guids == ["attachment"]


@pytest.mark.asyncio
async def test_takeover_cancels_the_expired_owner_task(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    now = [10.0]
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_OWNER_LEASE_SECONDS", 5.0)
    monkeypatch.setattr(bluebubbles.time, "monotonic", lambda: now[0])
    adapter = _adapter(monkeypatch)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def own():
        adapter._reserve_message_delivery("cancel-owner", [])
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    owner = asyncio.create_task(own())
    try:
        await started.wait()
        now[0] = 16.0
        kind, _, _ = adapter._reserve_message_delivery("cancel-owner", [])

        assert kind == "takeover"
        for _ in range(10):
            if cancelled.is_set():
                break
            await asyncio.sleep(0)
        assert cancelled.is_set()
    finally:
        if not owner.done():
            owner.cancel()
        await asyncio.gather(owner, return_exceptions=True)
    assert owner.cancelled()


@pytest.mark.asyncio
async def test_stale_owner_completion_cannot_complete_replacement(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    now = [10.0]
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_OWNER_LEASE_SECONDS", 5.0)
    monkeypatch.setattr(bluebubbles.time, "monotonic", lambda: now[0])
    adapter = _adapter(monkeypatch)
    _, stale, _ = adapter._reserve_message_delivery("generation", [])
    stale_generation = stale["owner_generation"]
    now[0] = 16.0
    _, replacement, _ = adapter._reserve_message_delivery("generation", [])

    adapter._complete_message_reservation("generation", stale, stale_generation)

    assert adapter._seen_message_guids["generation"] is replacement
    assert replacement["state"] == "in_flight"


@pytest.mark.asyncio
async def test_expired_inflight_entry_cannot_hold_cache_capacity(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    now = [10.0]
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_CACHE_SIZE", 1)
    monkeypatch.setattr(bluebubbles, "_MESSAGE_DEDUP_OWNER_LEASE_SECONDS", 5.0)
    monkeypatch.setattr(bluebubbles.time, "monotonic", lambda: now[0])
    adapter = _adapter(monkeypatch)
    _, stale, _ = adapter._reserve_message_delivery("stale", [])
    now[0] = 16.0

    kind, current, _ = adapter._reserve_message_delivery("current", [])

    assert kind == "new"
    assert list(adapter._seen_message_guids) == ["current"]
    assert stale["outcome"].result() is False
    assert current["state"] == "in_flight"


@pytest.mark.asyncio
async def test_attachment_order_survives_reservation(monkeypatch):
    adapter = _adapter(monkeypatch)
    kind, _, guids = adapter._reserve_message_delivery(
        "ordered", ["z-first", "a-second", "z-first"]
    )
    assert kind == "new"
    assert guids == ["z-first", "a-second"]


@pytest.mark.asyncio
async def test_cancellation_releases_reservation_for_retry(monkeypatch):
    adapter = _adapter(monkeypatch)
    started = asyncio.Event()

    async def blocked_download(*_args):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(adapter, "_download_attachment", blocked_download)
    request = _payload(
        "cancelled",
        attachments=[{"guid": "attachment", "mimeType": "image/jpeg"}],
    )
    task = asyncio.create_task(adapter._handle_webhook(_Request(request)))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "cancelled" not in adapter._seen_message_guids

    monkeypatch.setattr(
        adapter, "_download_attachment", AsyncMock(return_value="/cache/photo.jpg")
    )

    async def admit(event):
        event._gateway_accepted = True

    monkeypatch.setattr(adapter, "handle_message", admit)
    assert (await adapter._handle_webhook(_Request(request))).status == 200


def _activate_webhook_session(adapter, payload):
    record = payload["data"]
    source = adapter.build_source(
        chat_id=record["chatGuid"],
        chat_name=record["chatIdentifier"],
        chat_type="dm",
        user_id=record["handle"]["address"],
        user_name=record["handle"]["address"],
        chat_id_alt=record["chatIdentifier"],
    )
    event = MessageEvent(text=record["text"], source=source)
    session_key = adapter._event_session_key(event)
    adapter._active_sessions[session_key] = asyncio.Event()
    return session_key


@pytest.mark.asyncio
async def test_busy_status_webhook_retry_is_deduplicated_after_positive_admission(
    monkeypatch,
):
    adapter = _adapter(monkeypatch)
    payload = _payload("busy-status")
    payload["data"]["text"] = "/status"
    session_key = _activate_webhook_session(adapter, payload)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._hm_admit_event = AsyncMock(
        side_effect=lambda event: (event, event.source, False)
    )
    runner._hm_estop_gate = lambda *_args: None
    runner._session_key_for_source = lambda _source: session_key
    runner._hm_pending_reply_intercepts = AsyncMock(return_value=None)
    runner._hm_evict_idle_stale_agent = lambda _key: None
    runner._hm_evict_reaped_agent = lambda _key: None
    runner._is_session_running = lambda _key: True
    runner._hm_handle_running_session_message = AsyncMock(return_value="status")
    adapter.set_message_handler(runner._handle_message)
    monkeypatch.setattr(
        adapter,
        "_send_with_retry",
        AsyncMock(return_value=SendResult(success=True, message_id="reply")),
    )

    responses = [
        await adapter._handle_webhook(_Request(payload)),
        await adapter._handle_webhook(_Request(payload)),
    ]

    assert [response.status for response in responses] == [200, 200]
    assert runner._hm_handle_running_session_message.await_count == 1


@pytest.mark.asyncio
async def test_busy_clarification_webhook_retry_is_deduplicated_after_positive_admission(
    monkeypatch,
):
    adapter = _adapter(monkeypatch)
    payload = _payload("busy-clarification")
    session_key = _activate_webhook_session(adapter, payload)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._hm_admit_event = AsyncMock(
        side_effect=lambda event: (event, event.source, False)
    )
    runner._hm_estop_gate = lambda *_args: None
    runner._session_key_for_source = lambda _source: session_key
    runner._hm_pending_reply_intercepts = AsyncMock(return_value="")
    adapter.set_message_handler(runner._handle_message)
    monkeypatch.setattr(
        "tools.clarify_gateway.get_pending_for_session",
        lambda key, include_choice_prompts=True: (
            SimpleNamespace(clarify_id="clarify") if key == session_key else None
        ),
    )

    responses = [
        await adapter._handle_webhook(_Request(payload)),
        await adapter._handle_webhook(_Request(payload)),
    ]

    assert [response.status for response in responses] == [200, 200]
    assert runner._hm_pending_reply_intercepts.await_count == 1


@pytest.mark.asyncio
async def test_busy_handler_webhook_retry_is_deduplicated_after_positive_admission(
    monkeypatch,
):
    adapter = _adapter(monkeypatch)
    payload = _payload("busy-handler")
    session_key = _activate_webhook_session(adapter, payload)
    runner = GatewayRunner.__new__(GatewayRunner)
    queued = []
    runner._draining = False
    runner._is_user_authorized_for_source = lambda _source: True
    runner._admit_bot_message_for_source = lambda _source: True
    runner._effective_busy_input_mode = lambda _source: "queue"
    runner._effective_busy_text_mode = lambda _source: "interrupt"
    runner._route_plaintext_approval_while_busy = AsyncMock(return_value=False)
    runner._adapter_for_source = lambda _source: adapter
    runner._peek_session_state = lambda _key: None
    runner._resolve_busy_steer_or_redirect = AsyncMock(
        return_value=runner._BusySteerOutcome(
            effective_mode="queue",
            demoted_for_subagents=False,
            demoted_for_compression=False,
            steered=False,
            redirected=False,
        )
    )
    def admit_queue(key, event):
        queued.append((key, event.message_id))
        event._gateway_accepted = True

    runner._queue_or_replace_pending_event = admit_queue
    adapter.set_message_handler(AsyncMock(side_effect=AssertionError("busy handler owns event")))
    adapter.set_busy_session_handler(runner._handle_active_session_busy_message)
    monkeypatch.setenv("HERMES_GATEWAY_BUSY_ACK_ENABLED", "false")

    responses = [
        await adapter._handle_webhook(_Request(payload)),
        await adapter._handle_webhook(_Request(payload)),
    ]

    assert [response.status for response in responses] == [200, 200]
    assert queued == [(session_key, "busy-handler")]


@pytest.mark.asyncio
async def test_busy_queue_cap_drop_remains_unaccepted_for_transport_retry(monkeypatch):
    adapter = _adapter(monkeypatch)
    source = adapter.build_source(chat_id="chat", user_id="user")
    event = MessageEvent(
        text="busy input",
        message_type=MessageType.TEXT,
        source=source,
        message_id="queue-cap-drop",
    )
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._draining = False
    runner._BUSY_QUEUE_MAX_PENDING = 0
    runner._is_user_authorized_for_source = lambda _source: True
    runner._admit_bot_message_for_source = lambda _source: True
    runner._effective_busy_input_mode = lambda _source: "queue"
    runner._effective_busy_text_mode = lambda _source: "interrupt"
    runner._route_plaintext_approval_while_busy = AsyncMock(return_value=False)
    runner._adapter_for_source = lambda _source: adapter
    runner._peek_session_state = lambda _key: None
    runner._resolve_busy_steer_or_redirect = AsyncMock(
        return_value=runner._BusySteerOutcome(
            effective_mode="queue",
            demoted_for_subagents=False,
            demoted_for_compression=False,
            steered=False,
            redirected=False,
        )
    )
    monkeypatch.setenv("HERMES_GATEWAY_BUSY_ACK_ENABLED", "false")

    handled = await runner._handle_active_session_busy_message(event, "key")

    assert handled is True
    assert event._gateway_accepted is False
    assert adapter._pending_messages == {}


@pytest.mark.asyncio
async def test_failed_busy_interrupt_with_full_queue_remains_unaccepted(monkeypatch):
    adapter = _adapter(monkeypatch)
    source = adapter.build_source(chat_id="chat", user_id="user")
    event = MessageEvent(
        text="busy input",
        message_type=MessageType.TEXT,
        source=source,
        message_id="failed-interrupt",
    )

    class FailingAgent:
        def interrupt(self, _text):
            raise RuntimeError("interrupt failed")

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._draining = False
    runner._BUSY_QUEUE_MAX_PENDING = 0
    runner._is_user_authorized_for_source = lambda _source: True
    runner._admit_bot_message_for_source = lambda _source: True
    runner._effective_busy_input_mode = lambda _source: "interrupt"
    runner._effective_busy_text_mode = lambda _source: "interrupt"
    runner._route_plaintext_approval_while_busy = AsyncMock(return_value=False)
    runner._adapter_for_source = lambda _source: adapter
    runner._peek_session_state = lambda _key: SimpleNamespace(
        turn=SimpleNamespace(agent=FailingAgent()),
        conversation=SimpleNamespace(queued_events=[]),
    )
    runner._resolve_busy_steer_or_redirect = AsyncMock(
        return_value=runner._BusySteerOutcome(
            effective_mode="interrupt",
            demoted_for_subagents=False,
            demoted_for_compression=False,
            steered=False,
            redirected=False,
        )
    )
    monkeypatch.setenv("HERMES_GATEWAY_BUSY_ACK_ENABLED", "false")

    handled = await runner._handle_active_session_busy_message(event, "key")

    assert handled is True
    assert event._gateway_accepted is False
    assert adapter._pending_messages == {}


def _ack_config(timeout):
    return {
        "display": {
            "platforms": {
                "bluebubbles": {
                    "quick_ack_enabled": True,
                    "quick_ack_timeout_seconds": timeout,
                    "quick_ack_fallback": "Checking now.",
                }
            }
        }
    }


def _ack_event(adapter):
    return MessageEvent(
        text="Please inspect this",
        message_type=MessageType.TEXT,
        source=adapter.build_source(chat_id="chat", user_id="user"),
    )


@pytest.mark.asyncio
async def test_quick_ack_generation_and_send_share_one_hard_deadline(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    monkeypatch.setattr(bluebubbles, "_QUICK_ACK_MIN_TIMEOUT_SECONDS", 0.01)
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)

    async def hung_generation(**_kwargs):
        await asyncio.Event().wait()

    sent = []

    async def send(_chat, text):
        sent.append(text)
        return SendResult(success=True)

    monkeypatch.setattr("agent.auxiliary_client.async_call_llm", hung_generation)
    monkeypatch.setattr(adapter, "send", send)
    started = asyncio.get_running_loop().time()
    ack = await adapter.maybe_send_quick_ack(event, event.text, _ack_config(0.06))

    assert ack == "Checking now."
    assert sent == ["Checking now."]
    assert asyncio.get_running_loop().time() - started < 0.12


@pytest.mark.asyncio
async def test_quick_ack_hung_send_cannot_exceed_hard_deadline(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    monkeypatch.setattr(bluebubbles, "_QUICK_ACK_MIN_TIMEOUT_SECONDS", 0.01)
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    monkeypatch.setattr(
        "agent.auxiliary_client.async_call_llm",
        AsyncMock(return_value={"choices": [{"message": {"content": "Checking now."}}]}),
    )

    async def hung_send(*_args):
        await asyncio.Event().wait()

    monkeypatch.setattr(adapter, "send", hung_send)
    started = asyncio.get_running_loop().time()
    ack = await adapter.maybe_send_quick_ack(event, event.text, _ack_config(0.05))

    # Once outbound processing starts, a hard timeout is conservatively
    # ambiguous: cancellation-resistant transport code may still emit later.
    assert ack is not None
    assert ack.delivery_state == "potentially_sent"
    assert ack.text == "Checking now."
    assert asyncio.get_running_loop().time() - started < 0.12


@pytest.mark.asyncio
async def test_quick_ack_rechecks_ownership_after_chat_lookup_before_post(monkeypatch):
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    ownership = [True]
    posted = []
    monkeypatch.setattr(
        "agent.auxiliary_client.async_call_llm",
        AsyncMock(return_value={"choices": [{"message": {"content": "Checking now."}}]}),
    )

    async def lookup(_chat_id):
        ownership[0] = False
        return "iMessage;-;user@example.com"

    async def post(_path, payload):
        posted.append(payload["message"])
        return {"data": {"guid": "ack"}}

    monkeypatch.setattr(adapter, "_resolve_chat_guid", lookup)
    monkeypatch.setattr(adapter, "_api_post", post)

    result = await adapter.maybe_send_quick_ack(
        event,
        event.text,
        _ack_config(1.0),
        admission_check=lambda: ownership[0],
    )

    assert result is None
    assert posted == []


@pytest.mark.asyncio
async def test_late_generation_cannot_emit_a_second_ack(monkeypatch):
    import gateway.platforms.bluebubbles as bluebubbles

    monkeypatch.setattr(bluebubbles, "_QUICK_ACK_MIN_TIMEOUT_SECONDS", 0.01)
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    late = asyncio.Event()

    async def ignores_cancellation(**_kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.02)
            late.set()
            return {"choices": [{"message": {"content": "I'll inspect this now."}}]}

    sent = []

    async def send(_chat, text):
        sent.append(text)
        return SendResult(success=True)

    monkeypatch.setattr("agent.auxiliary_client.async_call_llm", ignores_cancellation)
    monkeypatch.setattr(adapter, "send", send)

    assert await adapter.maybe_send_quick_ack(
        event, event.text, _ack_config(0.05)
    ) == "Checking now."
    await asyncio.wait_for(late.wait(), 0.1)
    assert sent == ["Checking now."]


@pytest.mark.asyncio
async def test_cancellation_resistant_quick_ack_reports_ambiguity_and_stages_sidecar(
    monkeypatch,
):
    import gateway.platforms.bluebubbles as bluebubbles

    monkeypatch.setattr(bluebubbles, "_QUICK_ACK_MIN_TIMEOUT_SECONDS", 0.01)
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._adapter_for_source = lambda _source: adapter
    late = asyncio.Event()
    posts = []
    monkeypatch.setattr(
        "agent.auxiliary_client.async_call_llm",
        AsyncMock(return_value={"choices": [{"message": {"content": "Checking now."}}]}),
    )
    monkeypatch.setattr(
        adapter,
        "_resolve_chat_guid",
        AsyncMock(return_value="iMessage;-;user@example.com"),
    )

    async def cancellation_resistant_post(_path, payload):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.02)
            posts.append(payload["message"])
            late.set()
            return {"data": {"guid": "late-ack"}}

    monkeypatch.setattr(adapter, "_api_post", cancellation_resistant_post)
    notes = []
    config = _ack_config(0.05)
    started = asyncio.get_running_loop().time()
    with pytest.MonkeyPatch.context() as config_patch:
        config_patch.setattr("gateway.run._load_gateway_config", lambda: config)
        outcome = await runner._maybe_send_bluebubbles_quick_ack(
            event, event.source, event.text, notes
        )

    assert asyncio.get_running_loop().time() - started < 0.12
    assert outcome is not None
    assert outcome.delivery_state == "potentially_sent"
    assert outcome.text == "Checking now."
    assert len(notes) == 1
    assert "potentially visible" in notes[0]
    assert "Do not repeat it" in notes[0]
    await asyncio.wait_for(late.wait(), 0.1)
    assert posts == ["Checking now."]


@pytest.mark.asyncio
async def test_cancellation_resistant_chat_lookup_reports_ambiguous_late_post(
    monkeypatch,
):
    import gateway.platforms.bluebubbles as bluebubbles

    monkeypatch.setattr(bluebubbles, "_QUICK_ACK_MIN_TIMEOUT_SECONDS", 0.01)
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    late = asyncio.Event()
    posts = []
    monkeypatch.setattr(
        "agent.auxiliary_client.async_call_llm",
        AsyncMock(return_value={"choices": [{"message": {"content": "Checking now."}}]}),
    )

    async def cancellation_resistant_lookup(_chat_id):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.02)
            return "iMessage;-;user@example.com"

    async def post(_path, payload):
        posts.append(payload["message"])
        late.set()
        return {"data": {"guid": "late-ack"}}

    monkeypatch.setattr(adapter, "_resolve_chat_guid", cancellation_resistant_lookup)
    monkeypatch.setattr(adapter, "_api_post", post)

    outcome = await adapter.maybe_send_quick_ack(
        event, event.text, _ack_config(0.05)
    )

    assert outcome is not None
    assert outcome.delivery_state == "potentially_sent"
    assert outcome.text == "Checking now."
    await asyncio.wait_for(late.wait(), 0.1)
    assert posts == ["Checking now."]


@pytest.mark.asyncio
async def test_quick_ack_http_timeout_after_post_start_is_ambiguous(monkeypatch):
    adapter = _adapter(monkeypatch)
    event = _ack_event(adapter)
    monkeypatch.setattr(
        "agent.auxiliary_client.async_call_llm",
        AsyncMock(return_value={"choices": [{"message": {"content": "Checking now."}}]}),
    )
    monkeypatch.setattr(
        adapter,
        "_resolve_chat_guid",
        AsyncMock(return_value="iMessage;-;user@example.com"),
    )
    monkeypatch.setattr(
        adapter,
        "_api_post",
        AsyncMock(side_effect=httpx.ReadTimeout("ambiguous write")),
    )

    outcome = await adapter.maybe_send_quick_ack(
        event, event.text, _ack_config(1.0)
    )

    assert outcome is not None
    assert outcome.delivery_state == "potentially_sent"
    assert outcome.text == "Checking now."


@pytest.mark.asyncio
async def test_paragraphs_split_in_order_and_partial_timeout_is_not_retried(monkeypatch):
    adapter = _adapter(monkeypatch)
    monkeypatch.setattr(
        adapter,
        "_resolve_chat_guid",
        AsyncMock(return_value="iMessage;-;user@example.com"),
    )
    sent = []

    async def post(_path, payload):
        sent.append(payload["message"])
        if len(sent) == 2:
            raise httpx.ReadTimeout("")
        return {"data": {"guid": "first"}}

    monkeypatch.setattr(adapter, "_api_post", post)
    result = await adapter._send_with_retry("chat", "first\n\nsecond")

    assert result.success is False
    assert BasePlatformAdapter._is_timeout_error(result.error)
    assert sent == ["first", "second"]


def _turn_runner(monkeypatch, *, current):
    runner = GatewayRunner.__new__(GatewayRunner)
    source = SessionSource(
        platform=Platform.BLUEBUBBLES,
        chat_id="chat",
        user_id="user",
    )
    entry = SimpleNamespace(session_id="session", session_key="key")
    event = MessageEvent(text="inspect", source=source, message_id="message")
    prepared = runner._PreparedTurn([], "context", "inspect", "inspect", None, None)
    runner._hmwa_resolve_session = AsyncMock(return_value=(source, entry, "key"))
    runner._hmwa_prepare_turn = AsyncMock(return_value=(prepared, {}))
    runner.hooks = SimpleNamespace(emit=AsyncMock())
    runner._clear_session_env = lambda _tokens: None
    runner._is_session_run_current = lambda _key, _generation: current
    runner._peek_session_state = lambda _key: SimpleNamespace()
    runner._reply_anchor_for_event = lambda _event: None
    return runner, event, source, prepared


@pytest.mark.asyncio
async def test_superseded_turn_cannot_emit_quick_ack_or_start_agent(monkeypatch):
    runner, event, source, _ = _turn_runner(monkeypatch, current=False)
    runner._maybe_send_bluebubbles_quick_ack = AsyncMock()
    runner._run_agent = AsyncMock()

    assert await runner._handle_message_with_agent(event, source, "key", 7) is None
    runner._maybe_send_bluebubbles_quick_ack.assert_not_awaited()
    runner._run_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_quick_ack_runs_after_final_admission_immediately_before_agent(monkeypatch):
    runner, event, source, prepared = _turn_runner(monkeypatch, current=True)
    order = []

    async def ack(*args, **_kwargs):
        order.append("ack")
        args[3].append("visible ack")
        return "Checking now."

    async def run(**_kwargs):
        order.append("run")
        raise asyncio.CancelledError

    runner._maybe_send_bluebubbles_quick_ack = ack
    runner._set_pending_turn_sidecar_notes = lambda _key, _notes: None
    runner._run_agent = run

    with pytest.raises(asyncio.CancelledError):
        await runner._handle_message_with_agent(event, source, "key", 7)

    assert order == ["ack", "run"]
    assert prepared.turn_sidecar_notes
