"""BlueBubbles iMessage platform adapter.

Uses the local BlueBubbles macOS server for outbound REST sends and inbound
webhooks. Designed as a native Hermes messaging gateway channel.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)

DEFAULT_WEBHOOK_HOST = "127.0.0.1"
DEFAULT_WEBHOOK_PORT = 8645
DEFAULT_WEBHOOK_PATH = "/bluebubbles-webhook"
MAX_TEXT_LENGTH = 4000


def check_bluebubbles_requirements() -> bool:
    try:
        import aiohttp  # noqa: F401
        import httpx as _httpx  # noqa: F401
    except ImportError:
        return False
    return True


def _normalize_server_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if not re.match(r"^https?://", value, flags=re.I):
        value = f"http://{value}"
    return value.rstrip("/")


def _strip_markdown(text: str) -> str:
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"\*(.+?)\*", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"__(.+?)__", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"_(.+?)_", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"```[a-zA-Z0-9_+-]*\n?", "", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\[([^\]]+)\]\(([^\)]+)\)", r"\1", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class BlueBubblesAdapter(BasePlatformAdapter):
    platform = Platform.BLUEBUBBLES
    MAX_MESSAGE_LENGTH = MAX_TEXT_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.BLUEBUBBLES)
        extra = config.extra or {}
        self.server_url = _normalize_server_url(extra.get("server_url") or os.getenv("BLUEBUBBLES_SERVER_URL", ""))
        self.password = extra.get("password") or os.getenv("BLUEBUBBLES_PASSWORD", "")
        self.webhook_host = extra.get("webhook_host") or os.getenv("BLUEBUBBLES_WEBHOOK_HOST", DEFAULT_WEBHOOK_HOST)
        self.webhook_port = int(extra.get("webhook_port") or os.getenv("BLUEBUBBLES_WEBHOOK_PORT", str(DEFAULT_WEBHOOK_PORT)))
        self.webhook_path = extra.get("webhook_path") or os.getenv("BLUEBUBBLES_WEBHOOK_PATH", DEFAULT_WEBHOOK_PATH)
        if not str(self.webhook_path).startswith("/"):
            self.webhook_path = f"/{self.webhook_path}"
        self.send_read_receipts = bool(extra.get("send_read_receipts", True))
        self.client: Optional[httpx.AsyncClient] = None
        self._runner = None
        self._private_api_enabled: Optional[bool] = None
        self._helper_connected: bool = False

    def _api_url(self, path: str) -> str:
        sep = '&' if '?' in path else '?'
        return f"{self.server_url}{path}{sep}password={quote(self.password, safe='')}"

    async def _api_get(self, path: str) -> Dict[str, Any]:
        assert self.client is not None
        res = await self.client.get(self._api_url(path))
        res.raise_for_status()
        return res.json()

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self.client is not None
        res = await self.client.post(self._api_url(path), json=payload)
        res.raise_for_status()
        return res.json()

    async def connect(self) -> bool:
        if not self.server_url or not self.password:
            logger.error("[bluebubbles] BLUEBUBBLES_SERVER_URL and BLUEBUBBLES_PASSWORD are required")
            return False
        from aiohttp import web
        self.client = httpx.AsyncClient(timeout=30.0)
        try:
            probe = await self._api_get('/api/v1/ping')
            info = await self._api_get('/api/v1/server/info')
            server_data = (info or {}).get('data', {})
            self._private_api_enabled = bool(server_data.get('private_api'))
            self._helper_connected = bool(server_data.get('helper_connected'))
            logger.info("[bluebubbles] connected to %s (private_api=%s, helper=%s)", self.server_url, self._private_api_enabled, self._helper_connected)
        except Exception as exc:
            logger.error("[bluebubbles] cannot reach server at %s: %s", self.server_url, exc)
            if self.client:
                await self.client.aclose()
                self.client = None
            return False

        app = web.Application()
        app.router.add_get('/health', lambda _: web.Response(text='ok'))
        app.router.add_post(self.webhook_path, self._handle_webhook)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.webhook_host, self.webhook_port)
        await site.start()
        self._mark_connected()
        logger.info("[bluebubbles] webhook listening on http://%s:%s%s", self.webhook_host, self.webhook_port, self.webhook_path)
        return True

    async def disconnect(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._mark_disconnected()

    async def _resolve_chat_guid(self, target: str) -> Optional[str]:
        target = (target or '').strip()
        if not target:
            return None
        if ';' in target:
            return target
        # direct handle lookup
        try:
            payload = await self._api_post('/api/v1/chat/query', {"limit": 100, "offset": 0, "with": ["participants"]})
            for chat in payload.get('data', []) or []:
                guid = chat.get('guid') or chat.get('chatGuid')
                identifier = chat.get('chatIdentifier') or chat.get('identifier')
                if identifier == target:
                    return guid
                for part in chat.get('participants', []) or []:
                    if (part.get('address') or '').strip() == target and guid:
                        return guid
        except Exception:
            pass
        return None

    async def _create_chat_for_handle(self, address: str, message: str) -> SendResult:
        payload = {
            'addresses': [address],
            'message': message,
            'tempGuid': f'temp-{datetime.utcnow().timestamp()}',
        }
        try:
            res = await self._api_post('/api/v1/chat/new', payload)
            data = res.get('data') or {}
            msg_id = data.get('guid') or data.get('messageGuid') or 'ok'
            return SendResult(success=True, message_id=str(msg_id), raw_response=res)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        text = _strip_markdown(content or '')
        if not text:
            return SendResult(success=False, error='BlueBubbles send requires text')
        chunks = self.truncate_message(text, max_length=self.MAX_MESSAGE_LENGTH)
        last = SendResult(success=True)
        for chunk in chunks:
            guid = await self._resolve_chat_guid(chat_id)
            if not guid:
                if self._private_api_enabled and ('@' in chat_id or re.match(r'^\+\d+', chat_id)):
                    return await self._create_chat_for_handle(chat_id, chunk)
                return SendResult(success=False, error=f'BlueBubbles chat not found for target: {chat_id}')
            payload: Dict[str, Any] = {'chatGuid': guid, 'tempGuid': f'temp-{datetime.utcnow().timestamp()}', 'message': chunk}
            if reply_to and self._private_api_enabled and self._helper_connected:
                payload['method'] = 'private-api'
                payload['selectedMessageGuid'] = reply_to
                payload['partIndex'] = 0
            try:
                res = await self._api_post('/api/v1/message/text', payload)
                data = res.get('data') or {}
                msg_id = data.get('guid') or data.get('messageGuid') or 'ok'
                last = SendResult(success=True, message_id=str(msg_id), raw_response=res)
            except Exception as exc:
                return SendResult(success=False, error=str(exc))
        return last

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        chat_type = 'group' if ';+;' in (chat_id or '') else 'dm'
        return {'name': chat_id, 'type': chat_type}

    def format_message(self, content: str) -> str:
        return _strip_markdown(content)

    def _extract_payload_record(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = payload.get('data')
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    return item
        if isinstance(payload.get('message'), dict):
            return payload.get('message')
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _value(*candidates: Any) -> Optional[str]:
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    async def _handle_webhook(self, request):
        from aiohttp import web
        token = (
            request.query.get('password')
            or request.query.get('guid')
            or request.headers.get('x-password')
            or request.headers.get('x-guid')
            or request.headers.get('x-bluebubbles-guid')
        )
        if token != self.password:
            return web.json_response({'error': 'unauthorized'}, status=401)
        try:
            raw = await request.read()
            body = raw.decode('utf-8', errors='replace')
            try:
                payload = json.loads(body)
            except Exception:
                from urllib.parse import parse_qs
                form = parse_qs(body)
                payload_str = (form.get('payload') or form.get('data') or form.get('message') or [''])[0]
                payload = json.loads(payload_str) if payload_str else {}
        except Exception as exc:
            logger.error('[bluebubbles] webhook parse error: %s', exc)
            return web.json_response({'error': 'invalid payload'}, status=400)

        event_type = self._value(payload.get('type'), payload.get('event')) or ''
        # Only process new-message events; silently acknowledge everything else
        # (typing indicators, read receipts, reactions, group name changes, etc.)
        _message_events = {'new-message', 'message', 'updated-message'}
        if event_type and event_type not in _message_events:
            return web.Response(text='ok')
        record = self._extract_payload_record(payload) or {}
        is_from_me = bool(record.get('isFromMe') or record.get('fromMe') or record.get('is_from_me'))
        if is_from_me:
            return web.Response(text='ok')
        text = self._value(record.get('text'), record.get('message'), record.get('body')) or ''
        attachments = record.get('attachments') or []
        if not text and attachments:
            text = '(attachment)'
        chat_guid = self._value(
            record.get('chatGuid'),
            payload.get('chatGuid'),
            record.get('chat_guid'),
            payload.get('chat_guid'),
            payload.get('guid'),
        )
        chat_identifier = self._value(record.get('chatIdentifier'), record.get('identifier'), payload.get('chatIdentifier'), payload.get('identifier'))
        sender = self._value(
            record.get('handle', {}).get('address') if isinstance(record.get('handle'), dict) else None,
            record.get('sender'),
            record.get('from'),
            record.get('address'),
        ) or chat_identifier or chat_guid
        if not (chat_guid or chat_identifier) and sender:
            chat_identifier = sender
        if not sender or not (chat_guid or chat_identifier) or not text:
            return web.json_response({'error': 'missing message fields'}, status=400)
        session_chat_id = chat_guid or chat_identifier
        is_group = bool(record.get('isGroup')) or (';+;' in (chat_guid or ''))
        source = self.build_source(
            chat_id=session_chat_id,
            chat_name=chat_identifier or sender,
            chat_type='group' if is_group else 'dm',
            user_id=sender,
            user_name=sender,
            chat_id_alt=chat_identifier,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=payload,
            message_id=self._value(record.get('guid'), record.get('messageGuid'), record.get('id')),
            reply_to_message_id=self._value(record.get('threadOriginatorGuid'), record.get('associatedMessageGuid')),
        )
        task = asyncio.create_task(self.handle_message(event))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return web.Response(text='ok')
