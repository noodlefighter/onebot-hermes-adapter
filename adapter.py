"""
OneBot v11 Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to a OneBot v11 compatible server
(NapCat, go-cqhttp, Lagrange, etc.) via WebSocket and relays messages to/from
the Hermes agent.

Configuration in config.yaml::

    gateway:
      platforms:
        onebot11:
          enabled: true
          extra:
            ws_url: "ws://localhost:6098/ws"
            access_token: ""
            allowed_users: []
            allow_all_users: false

Or via environment variables (overrides config.yaml):
    ONEBOT11_WS_URL, ONEBOT11_ACCESS_TOKEN, ONEBOT11_ALLOWED_USERS,
    ONEBOT11_ALLOW_ALL_USERS
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports from main repo
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
    cache_image_from_bytes,
)
from gateway.session import SessionSource
from gateway.config import PlatformConfig, Platform

# Lazy import websockets
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False


# ---------------------------------------------------------------------------
# OneBot v11 message segment helpers
# ---------------------------------------------------------------------------

def _extract_text(segments: list) -> str:
    """Extract plain text from OneBot v11 message segments."""
    parts = []
    for seg in segments:
        if seg.get("type") == "text":
            parts.append(seg.get("data", {}).get("text", ""))
    return "".join(parts).strip()


def _is_at_bot(segments: list, bot_id: str) -> bool:
    """Check if the message mentions the bot."""
    for seg in segments:
        if seg.get("type") == "at":
            data = seg.get("data", {})
            if str(data.get("qq", "")) == str(bot_id):
                return True
    return False


def _build_text_message(text: str) -> list:
    """Build OneBot v11 message segments from plain text."""
    return [{"type": "text", "data": {"text": text}}]
def _extract_images(segments: list) -> list:
    """Extract image URLs/data from OneBot v11 message segments.

    Returns list of dicts with 'url' and optionally 'data' (base64).
    NapCat image segments can have:
      - data.file: local file path or URL
      - data.url: direct URL
      - data.file_base64: base64 encoded image data
    """
    images = []
    for seg in segments:
        if seg.get("type") == "image":
            data = seg.get("data", {})
            img_info = {}
            # Prefer url, then file (if it looks like a URL)
            url = data.get("url") or ""
            file_val = data.get("file") or ""
            if url:
                img_info["url"] = url
            elif file_val and (file_val.startswith("http://") or file_val.startswith("https://")):
                img_info["url"] = file_val
            elif file_val and file_val.startswith("file://"):
                img_info["url"] = file_val
            elif file_val:
                # Could be a local path or base64
                img_info["url"] = file_val
            if img_info:
                images.append(img_info)
    return images


def _build_image_message(file: str) -> list:
    """Build OneBot v11 image message segment."""
    return [{"type": "image", "data": {"file": file}}]


# ---------------------------------------------------------------------------
# OneBot v11 Adapter
# ---------------------------------------------------------------------------

class OneBot11Adapter(BasePlatformAdapter):
    """Async OneBot v11 adapter implementing the BasePlatformAdapter interface.

    Connects to a OneBot v11 server via WebSocket and handles message
    exchange with the Hermes agent.
    """

    def __init__(self, config, **kwargs):
        platform = Platform("onebot11")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        # Connection settings (env vars override config.yaml)
        self.ws_url = os.getenv("ONEBOT11_WS_URL") or extra.get("ws_url", "")
        self.access_token = os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token", "")

        # Auth
        self.allowed_users: list = extra.get("allowed_users", [])
        if os.getenv("ONEBOT11_ALLOWED_USERS"):
            self.allowed_users = [
                u.strip() for u in os.getenv("ONEBOT11_ALLOWED_USERS").split(",") if u.strip()
            ]
        self._allowed_users_set: set = {str(u) for u in self.allowed_users}

        self.allow_all_users = (
            os.getenv("ONEBOT11_ALLOW_ALL_USERS", "").lower() in ("1", "true", "yes")
            if os.getenv("ONEBOT11_ALLOW_ALL_USERS")
            else extra.get("allow_all_users", False)
        )

        # Group chat whitelist — only process messages from allowed groups.
        # Empty/missing = deny all group messages (whitelist-not-configured = reject all).
        self._group_allowed_chats: set = set()
        group_allowed = extra.get("group_allowed_chats", [])
        if group_allowed:
            self._group_allowed_chats = {str(g) for g in group_allowed}

        # Runtime state
        self._ws: Any = None
        self._recv_task: Optional[asyncio.Task] = None
        self._bot_id: Optional[str] = None
        self._connected = False
        # Pending API call futures, keyed by echo value
        self._pending_api_calls: Dict[str, asyncio.Future] = {}

    @property
    def name(self) -> str:
        return "OneBot v11"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        """Connect to the OneBot v11 server via WebSocket."""
        if not WEBSOCKETS_AVAILABLE:
            logger.error("OneBot v11: websockets not installed. Run: pip install websockets")
            self._set_fatal_error(
                "dependency_missing",
                "websockets package not installed",
                retryable=False,
            )
            return False

        if not self.ws_url:
            logger.error("OneBot v11: ws_url must be configured")
            self._set_fatal_error(
                "config_missing",
                "ONEBOT11_WS_URL must be set",
                retryable=False,
            )
            return False

        # Build connection headers
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        try:
            connect_kwargs = {
                "ping_interval": 30,
                "ping_timeout": 10,
                "close_timeout": 5,
            }
            if headers:
                connect_kwargs["additional_headers"] = headers
            self._ws = await websockets.connect(self.ws_url, **connect_kwargs)
            self._connected = True
            logger.info("OneBot v11: connected to %s", self.ws_url)
        except Exception as e:
            logger.error("OneBot v11: failed to connect to %s — %s", self.ws_url, e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        # Start receive loop
        self._recv_task = asyncio.create_task(self._receive_loop())
        return True

    async def disconnect(self) -> None:
        """Disconnect from the OneBot v11 server."""
        self._connected = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        logger.info("OneBot v11: disconnected")

    # ── Message receiving ─────────────────────────────────────────────────

    async def _receive_loop(self) -> None:
        """Main receive loop — reads WebSocket messages and dispatches them."""
        try:
            async for raw_msg in self._ws:
                try:
                    data = json.loads(raw_msg)
                    # Check if this is a response to a pending API call
                    echo = data.get("echo")
                    if echo and echo in self._pending_api_calls:
                        fut = self._pending_api_calls.pop(echo)
                        if not fut.done():
                            fut.set_result(data)
                        continue
                    await self._handle_event(data)
                except json.JSONDecodeError:
                    logger.warning("OneBot v11: received non-JSON message")
                except Exception as e:
                    logger.error("OneBot v11: error handling event: %s", e, exc_info=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("OneBot v11: receive loop ended: %s", e)
            self._connected = False

    async def _handle_event(self, data: dict) -> None:
        """Handle a single OneBot v11 event."""
        post_type = data.get("post_type", "")

        # Handle meta events (lifecycle, heartbeat)
        if post_type == "meta_event":
            meta_type = data.get("meta_event_type", "")
            if meta_type == "lifecycle":
                sub_type = data.get("sub_type", "")
                if sub_type == "connect":
                    self._bot_id = str(data.get("self_id", ""))
                    logger.info("OneBot v11: bot connected, self_id=%s", self._bot_id)
            return

        # Only handle message events
        if post_type != "message":
            return

        message_type = data.get("message_type", "")
        user_id = str(data.get("user_id", ""))
        raw_message = data.get("raw_message", "")
        message = data.get("message", [])
        message_id = str(data.get("message_id", ""))
        group_id = data.get("group_id")

        # Determine chat_id and chat_type
        if message_type == "group":
            chat_id = str(group_id)
            chat_type = "group"
            # Group whitelist filter: deny all if list is empty, else allow only listed groups
            if chat_id not in self._group_allowed_chats:
                logger.info(
                    "OneBot v11: ignoring message from non-allowed group %s", chat_id
                )
                return
        elif message_type == "private":
            chat_id = user_id
            chat_type = "dm"
        else:
            return

        # Extract text content
        images = []
        if isinstance(message, list):
            text = _extract_text(message)
            images = _extract_images(message)
            # Check if bot is mentioned in group
            is_mention = _is_at_bot(message, self._bot_id) if self._bot_id else False
        elif isinstance(message, str):
            text = message
            is_mention = False
        else:
            text = str(raw_message)
            is_mention = False

        if not text and not images:
            return

        # Build session key
        session_key = f"onebot11:{chat_type}:{chat_id}"

        # Create source metadata
        source = SessionSource(
            platform=Platform("onebot11"),
            chat_id=chat_id,
            chat_type=chat_type,
            user_id=user_id,
            message_id=message_id,
        )

        # Create message event
        event = MessageEvent(
            text=text,
            source=source,
            message_id=message_id,
        )

        # Process images - download and cache locally for vision tool
        if images:
            media_urls = []
            media_types = []
            for img in images:
                url = img.get("url", "")
                if not url:
                    continue
                try:
                    if url.startswith("file://"):
                        # Local file - use directly
                        local_path = url[7:]
                        if os.path.exists(local_path):
                            media_urls.append(local_path)
                            ext = os.path.splitext(local_path)[1].lower() or ".jpg"
                            media_types.append(f"image/{ext.lstrip('.')}")
                    elif url.startswith("http://") or url.startswith("https://"):
                        # Remote URL - download and cache using stdlib
                        import urllib.request
                        def _download_image():
                            req = urllib.request.Request(url, headers={"User-Agent": "HermesBot/1.0"})
                            with urllib.request.urlopen(req, timeout=30) as resp:
                                return resp.read(), resp.headers.get("Content-Type", "")
                        img_bytes, ct = await asyncio.to_thread(_download_image)
                        ext = ".jpg"
                        if "png" in ct:
                            ext = ".png"
                        elif "webp" in ct:
                            ext = ".webp"
                        elif "gif" in ct:
                            ext = ".gif"
                        cached = cache_image_from_bytes(img_bytes, ext)
                        media_urls.append(cached)
                        media_types.append(f"image/{ext.lstrip('.')}")
                        logger.info("OneBot v11: cached image from URL: %s", url[:80])
                    else:
                        # Could be base64 or other format - skip for now
                        logger.debug("OneBot v11: skipping non-URL image: %s", url[:50])
                except Exception as e:
                    logger.warning("OneBot v11: failed to process image: %s", e)

            if media_urls:
                event.media_urls = media_urls
                event.media_types = media_types

        # Store OneBot-specific metadata for send operations
        event._onebot_chat_type = chat_type
        event._onebot_group_id = str(group_id) if group_id else None
        event._onebot_user_id = user_id

        # Dispatch to handler using base class method
        # This ensures proper session management, typing indicators, etc.
        if self._message_handler:
            try:
                await self.handle_message(event)
            except Exception as e:
                logger.error("OneBot v11: message handler error: %s", e, exc_info=True)

    # ── Message sending ───────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message via OneBot v11."""
        print(f"ONEBOT11 SEND CALLED: chat_id={chat_id}, content={content[:50]}", flush=True)
        logger.info("OneBot v11: send called, chat_id=%s, content=%s", chat_id, content[:50])
        if not self._ws or not self._connected:
            logger.error("OneBot v11: send failed - not connected")
            return SendResult(success=False, error="Not connected")

        metadata = metadata or {}

        # Build message segments
        segments = _build_text_message(content)

        # Determine send action based on available info
        # Supports "group:XXXXX" prefix convention for proactive push
        group_id = None
        effective_chat_id = chat_id

        if chat_id and chat_id.startswith("group:"):
            group_id = chat_id[6:]  # strip "group:" prefix
            effective_chat_id = group_id
        elif metadata and metadata.get("group_id"):
            group_id = metadata["group_id"]

        if group_id:
            action = "send_group_msg"
            params = {"group_id": int(group_id), "message": segments}
        else:
            # Default to private message - chat_id is the user_id
            action = "send_private_msg"
            params = {"user_id": int(chat_id), "message": segments}

        # Add reply if specified
        if reply_to:
            params["message"] = [
                {"type": "reply", "data": {"id": reply_to}},
                *segments,
            ]

        # Send via WebSocket
        request = {
            "action": action,
            "params": params,
            "echo": str(uuid.uuid4()),
        }

        try:
            await self._ws.send(json.dumps(request))
            return SendResult(success=True, message_id=str(uuid.uuid4()))
        except Exception as e:
            logger.error("OneBot v11: send failed: %s", e)
            return SendResult(success=False, error=str(e))

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image via OneBot v11."""
        if not self._ws or not self._connected:
            return SendResult(success=False, error="Not connected")

        metadata = metadata or {}

        # Build message segments
        segments = _build_image_message(image_url)
        if caption:
            segments.extend(_build_text_message(caption))

        # Determine send action — supports "group:XXXXX" prefix
        group_id = None
        if chat_id and chat_id.startswith("group:"):
            group_id = chat_id[6:]
        elif metadata and metadata.get("group_id"):
            group_id = metadata["group_id"]

        if group_id:
            action = "send_group_msg"
            params = {"group_id": int(group_id), "message": segments}
        else:
            action = "send_private_msg"
            params = {"user_id": int(chat_id), "message": segments}

        request = {
            "action": action,
            "params": params,
            "echo": str(uuid.uuid4()),
        }

        try:
            await self._ws.send(json.dumps(request))
            return SendResult(success=True, message_id=str(uuid.uuid4()))
        except Exception as e:
            logger.error("OneBot v11: send_image failed: %s", e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """OneBot v11 doesn't have a native typing indicator, so this is a no-op."""
        pass

    async def stop_typing(self, chat_id: str) -> None:
        """OneBot v11 doesn't have a native typing indicator, so this is a no-op."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get chat information."""
        # Try to resolve group name from cached group list
        if chat_id.startswith("group:"):
            gid = chat_id[6:]
            groups = await self.get_group_list()
            for g in groups:
                if str(g.get("group_id")) == gid:
                    return {"name": g.get("group_name", gid), "type": "group", "chat_id": chat_id}
        return {
            "name": f"OneBot v11 chat {chat_id}",
            "type": "group",
            "chat_id": chat_id,
        }

    # ── OneBot11 API helpers ─────────────────────────────────────────────

    async def _call_api(self, action: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Call a OneBot11 API action and return the response data.

        Sends the request over the existing WebSocket connection and waits
        for the matching echo response via the receive loop's dispatch.
        Returns the ``data`` field on success, or ``None`` on failure.
        """
        if not self._ws or not self._connected:
            logger.error("OneBot v11 _call_api: not connected")
            return None

        echo = str(uuid.uuid4())
        request = {"action": action, "params": params or {}, "echo": echo}

        # Register a future before sending to avoid race condition
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._pending_api_calls[echo] = fut

        try:
            await self._ws.send(json.dumps(request))
        except Exception as e:
            self._pending_api_calls.pop(echo, None)
            logger.error("OneBot v11 _call_api send failed: %s", e)
            return None

        try:
            data = await asyncio.wait_for(fut, timeout=10)
        except asyncio.TimeoutError:
            self._pending_api_calls.pop(echo, None)
            logger.warning("OneBot v11 _call_api: timeout waiting for %s response", action)
            return None

        if data.get("status") == "ok":
            return data.get("data")
        logger.warning("OneBot v11 _call_api %s error: %s", action, data.get("wording"))
        return None

    async def get_group_list(self) -> List[Dict[str, Any]]:
        """Return all groups the bot is in.

        Each entry contains at least ``group_id`` and ``group_name``.
        Result is fetched from NapCat on demand (not cached).
        """
        data = await self._call_api("get_group_list")
        if data is None:
            return []
        return data if isinstance(data, list) else []


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def _check_requirements() -> bool:
    """Check if websockets is installed."""
    return WEBSOCKETS_AVAILABLE


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Open an ephemeral WebSocket to NapCat, send, and close.

    Used by ``tools/send_message_tool._send_via_adapter`` when the gateway
    runner is not in this process (e.g. ``hermes cron`` running separately).
    """
    try:
        import websockets as _wsclient
    except ImportError:
        return {"error": "websockets not installed. Run: pip install websockets"}

    extra = getattr(pconfig, "extra", {}) or {}
    ws_url = os.getenv("ONEBOT11_WS_URL") or extra.get("ws_url", "")
    token = os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token", "")
    if not ws_url:
        return {"error": "OneBot11 standalone send: ONEBOT11_WS_URL is required"}

    try:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # Parse chat_id — supports "group:XXXXX" prefix
        if chat_id.startswith("group:"):
            group_id = int(chat_id[6:])
            action = "send_group_msg"
            params = {"group_id": group_id, "message": _build_text_message(message)}
        else:
            action = "send_private_msg"
            params = {"user_id": int(chat_id), "message": _build_text_message(message)}

        request = {
            "action": action,
            "params": params,
            "echo": str(uuid.uuid4()),
        }

        connect_kwargs = {"open_timeout": 10, "close_timeout": 5}
        if headers:
            connect_kwargs["additional_headers"] = headers

        async with _wsclient.connect(ws_url, **connect_kwargs) as ws:
            await ws.send(json.dumps(request))
            # Wait for response (skip lifecycle/meta events)
            for _ in range(20):
                resp = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(resp)
                if data.get("echo") == request["echo"]:
                    if data.get("status") == "ok":
                        return {"success": True, "message_id": str(data.get("data", {}).get("message_id", ""))}
                    return {"error": f"OneBot11 API error: {data.get('wording', data)}"}
            return {"error": "OneBot11 standalone send: no response received"}

    except Exception as e:
        return {"error": f"OneBot11 standalone send failed: {e}"}


def register(ctx) -> None:
    """Register the OneBot v11 adapter with the plugin context."""
    ctx.register_platform(
        name="onebot11",
        label="OneBot v11",
        adapter_factory=lambda cfg: OneBot11Adapter(cfg),
        check_fn=_check_requirements,
        validate_config=lambda cfg: bool(
            os.getenv("ONEBOT11_WS_URL")
            or (hasattr(cfg, "extra") and isinstance(cfg.extra, dict) and cfg.extra.get("ws_url"))
        ),
        required_env=["ONEBOT11_WS_URL"],
        install_hint="pip install websockets",
        allowed_users_env="ONEBOT11_ALLOWED_USERS",
        allow_all_env="ONEBOT11_ALLOW_ALL_USERS",
        emoji="🐧",
        cron_deliver_env_var="ONEBOT11_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
    )
