"""Gateway media-cache tests for the standalone OneBot v11 adapter."""

import asyncio
import importlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def _install_gateway_stubs():
    """Supply the Gateway surface required to import the plugin in isolation."""
    gateway = types.ModuleType("gateway")
    platforms = types.ModuleType("gateway.platforms")
    base = types.ModuleType("gateway.platforms.base")
    session = types.ModuleType("gateway.session")
    config = types.ModuleType("gateway.config")

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform
            self._message_handler = None

        async def handle_message(self, event):
            return await self._message_handler(event)

    class MessageType:
        TEXT = "text"
        VOICE = "voice"

    class MessageEvent:
        def __init__(self, text, message_type, source, message_id):
            self.text = text
            self.message_type = message_type
            self.source = source
            self.message_id = message_id
            self.media_urls = []
            self.media_types = []

    class SessionSource:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Platform(str):
        pass

    base.BasePlatformAdapter = BasePlatformAdapter
    base.SendResult = object
    base.MessageEvent = MessageEvent
    base.MessageType = MessageType
    base.cache_audio_from_bytes = lambda data, ext: f"/tmp/cached-audio{ext}"
    base.cache_image_from_bytes = lambda data, ext: f"/tmp/cached-image{ext}"
    session.SessionSource = SessionSource
    config.PlatformConfig = object
    config.Platform = Platform
    sys.modules.update({
        "gateway": gateway,
        "gateway.platforms": platforms,
        "gateway.platforms.base": base,
        "gateway.session": session,
        "gateway.config": config,
    })


_install_gateway_stubs()
sys.modules.pop("adapter", None)
adapter = importlib.import_module("adapter")


class _FakeResponse:
    def __init__(self, body, content_type="audio/ogg", content_length=None):
        self.body = body
        self.headers = {"Content-Type": content_type}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def read(self, size=-1):
        if size < 0:
            body, self.body = self.body, b""
            return body
        chunk, self.body = self.body[:size], self.body[size:]
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class VoiceReceivingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Keep unit tests deterministic and avoid creating real executor
        # threads for the adapter's intentionally synchronous stdlib helpers.
        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        self._to_thread_patch = patch("adapter.asyncio.to_thread", new=run_inline)
        self._to_thread_patch.start()
        self.addAsyncCleanup(self._stop_to_thread_patch)

    async def _stop_to_thread_patch(self):
        self._to_thread_patch.stop()

    def make_adapter(self, **extra):
        config = SimpleNamespace(extra={"allow_all_users": True, **extra})
        return adapter.OneBot11Adapter(config)

    @staticmethod
    def event(message):
        return {
            "post_type": "message",
            "message_type": "private",
            "user_id": 10001,
            "message_id": 99,
            "message": message,
        }

    async def test_url_ogg_record_does_not_call_get_record(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        instance._call_api = AsyncMock()
        body = b"OggS" + b"x" * 16
        with patch("urllib.request.urlopen", return_value=_FakeResponse(body, "audio/ogg")), patch(
            "adapter.cache_audio_from_bytes", return_value="/cache/voice.ogg"
        ) as cache_audio:
            await instance._handle_event(self.event([
                {"type": "record", "data": {
                    "url": "https://example.test/voice.ogg", "file": "voice.amr"
                }},
            ]))

        instance._message_handler.assert_awaited_once()
        event = instance._message_handler.await_args.args[0]
        self.assertEqual(event.text, "")
        self.assertEqual(event.message_type, adapter.MessageType.VOICE)
        self.assertEqual(event.media_urls, ["/cache/voice.ogg"])
        self.assertEqual(event.media_types, ["audio/ogg"])
        cache_audio.assert_called_once_with(body, ".ogg")
        instance._call_api.assert_not_awaited()

    async def test_url_amr_record_uses_get_record_wav_and_caches_wav(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        amr = b"#!AMR\nvoice"
        wav = b"RIFF\x00\x00\x00\x00WAVEfmt "
        instance._call_api = AsyncMock(return_value={
            "bytes": "base64://UklGRgAAAABXQVZFZm10IA==",
        })
        with patch("urllib.request.urlopen", return_value=_FakeResponse(amr, "audio/amr")), patch(
            "adapter.cache_audio_from_bytes", return_value="/cache/voice.wav"
        ) as cache_audio:
            await instance._handle_event(self.event([
                {"type": "record", "data": {
                    "url": "https://example.test/voice.amr", "file": "voice.amr"
                }},
            ]))

        instance._call_api.assert_awaited_once_with(
            "get_record", {"file": "voice.amr", "out_format": "wav"}
        )
        cache_audio.assert_called_once_with(wav, ".wav")
        event = instance._message_handler.await_args.args[0]
        self.assertEqual(event.media_urls, ["/cache/voice.wav"])
        self.assertEqual(event.media_types, ["audio/wav"])

    async def test_file_record_uses_get_record_then_caches_wav(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        wav = b"RIFF\x00\x00\x00\x00WAVEfmt "
        with tempfile.NamedTemporaryFile(suffix=".wav") as audio:
            audio.write(wav)
            audio.flush()
            instance._call_api = AsyncMock(return_value={"file": audio.name})
            with patch("adapter.cache_audio_from_bytes", return_value="/cache/voice.wav") as cache_audio:
                await instance._handle_event(self.event([
                    {"type": "record", "data": {"file": "voice.silk"}},
                ]))

        instance._call_api.assert_awaited_once_with(
            "get_record", {"file": "voice.silk", "out_format": "wav"}
        )
        cache_audio.assert_called_once_with(wav, ".wav")
        event = instance._message_handler.await_args.args[0]
        self.assertEqual(event.message_type, adapter.MessageType.VOICE)
        self.assertEqual(event.media_types, ["audio/wav"])

    async def test_get_record_container_file_uri_is_mapped_to_host_wav(self):
        wav = b"RIFF\x00\x00\x00\x00WAVEfmt "
        container_root = "/app/.config/QQ"
        container_source = f"{container_root}/voices/converted.wav"
        with tempfile.TemporaryDirectory() as host_root:
            host_audio = Path(host_root, "voices", "converted.wav")
            host_audio.parent.mkdir()
            host_audio.write_bytes(wav)
            instance = self.make_adapter(
                record_path_map=f"{container_root}={host_root}",
            )
            instance._message_handler = AsyncMock()
            instance._call_api = AsyncMock(return_value={
                "file": f"file://{container_source}",
            })
            with patch(
                "adapter.cache_audio_from_bytes", return_value="/cache/voice.wav"
            ) as cache_audio, self.assertLogs("adapter", level="INFO") as logs:
                await instance._handle_event(self.event([
                    {"type": "record", "data": {"file": "voice.silk"}},
                ]))

        cache_audio.assert_called_once_with(wav, ".wav")
        instance._message_handler.assert_awaited_once()
        self.assertTrue(any(
            f"{container_source} -> {host_audio}" in line for line in logs.output
        ))

    async def test_get_record_container_file_without_mapping_is_rejected(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        instance._call_api = AsyncMock(return_value={
            "file": "file:///app/.config/QQ/does-not-exist/converted.wav",
        })
        with patch("adapter.cache_audio_from_bytes") as cache_audio, self.assertLogs(
            "adapter", level="WARNING"
        ) as logs:
            await instance._handle_event(self.event([
                {"type": "record", "data": {"file": "voice.silk"}},
            ]))

        cache_audio.assert_not_called()
        instance._message_handler.assert_not_awaited()
        self.assertIn("not a readable regular file", logs.output[0])

    def test_record_path_map_list_prefers_longest_container_prefix(self):
        instance = self.make_adapter(record_path_map=[
            "/app=/host/app",
            "/app/.config/QQ=/host/napcat-data",
        ])

        self.assertEqual(
            instance._map_record_path("/app/.config/QQ/voices/converted.wav"),
            "/host/napcat-data/voices/converted.wav",
        )

    async def test_mixed_text_image_and_voice_preserves_media_alignment(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        instance._call_api = AsyncMock(return_value={"bytes": b"RIFF\x00\x00\x00\x00WAVEfmt "})

        with patch("urllib.request.urlopen", return_value=_FakeResponse(b"image", "image/png")), patch(
            "adapter.cache_image_from_bytes", return_value="/cache/image.png"
        ), patch("adapter.cache_audio_from_bytes", return_value="/cache/voice.wav"):
            await instance._handle_event(self.event([
                {"type": "text", "data": {"text": "typed"}},
                {"type": "image", "data": {"url": "https://example.test/image.png"}},
                {"type": "record", "data": {"file": "voice.amr"}},
            ]))

        event = instance._message_handler.await_args.args[0]
        self.assertEqual(event.text, "typed")
        self.assertEqual(event.message_type, adapter.MessageType.VOICE)
        self.assertEqual(event.media_urls, ["/cache/image.png", "/cache/voice.wav"])
        self.assertEqual(event.media_types, ["image/png", "audio/wav"])

    async def test_url_amr_without_file_is_rejected(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        instance._call_api = AsyncMock()
        with patch("urllib.request.urlopen", return_value=_FakeResponse(b"#!AMR\nvoice", "audio/amr")), patch(
            "adapter.cache_audio_from_bytes"
        ) as cache_audio, self.assertLogs("adapter", level="WARNING") as logs:
            await instance._handle_event(self.event([
                {"type": "record", "data": {"url": "https://example.test/voice.amr"}},
            ]))

        instance._call_api.assert_not_awaited()
        cache_audio.assert_not_called()
        instance._message_handler.assert_not_awaited()
        self.assertIn("AMR but no file is available for WAV conversion", logs.output[0])

    async def test_amr_conversion_returning_non_wav_is_rejected(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        instance._call_api = AsyncMock(return_value={"bytes": b"#!AMR\nvoice"})
        with patch("urllib.request.urlopen", return_value=_FakeResponse(b"#!AMR\nvoice", "audio/amr")), patch(
            "adapter.cache_audio_from_bytes"
        ) as cache_audio, self.assertLogs("adapter", level="WARNING") as logs:
            await instance._handle_event(self.event([
                {"type": "record", "data": {
                    "url": "https://example.test/voice.amr", "file": "voice.amr"
                }},
            ]))

        instance._call_api.assert_awaited_once_with(
            "get_record", {"file": "voice.amr", "out_format": "wav"}
        )
        cache_audio.assert_not_called()
        instance._message_handler.assert_not_awaited()
        self.assertIn("get_record WAV conversion did not return WAV data", logs.output[0])

    async def test_rejected_group_is_not_downloaded(self):
        instance = self.make_adapter()
        instance._message_handler = AsyncMock()
        group_event = self.event([
            {"type": "record", "data": {"url": "https://example.test/voice.ogg"}},
        ])
        group_event.update({"message_type": "group", "group_id": 123})
        with patch.object(instance, "_read_voice_source", new_callable=AsyncMock) as read_voice:
            await instance._handle_event(group_event)
        read_voice.assert_not_awaited()
        instance._message_handler.assert_not_awaited()

    def test_adapter_has_no_subprocess_stt_path(self):
        source = Path(adapter.__file__).read_text(encoding="utf-8")
        self.assertNotIn("voice_stt", source)
        self.assertNotIn("subprocess", source)
        self.assertIn("cache_audio_from_bytes", source)

    async def test_receive_loop_allows_get_record_echo_while_voice_is_processed(self):
        """The record handler must not block the receive loop's API echo dispatch."""
        instance = self.make_adapter()
        message_handled = asyncio.Event()

        async def on_message(_event):
            message_handled.set()

        instance._message_handler = AsyncMock(side_effect=on_message)
        source = self.event([{"type": "record", "data": {"file": "voice.amr"}}])

        class FakeWebSocket:
            def __init__(self):
                self._step = 0
                self.request_sent = asyncio.Event()
                self.request = None
                self.response_echo = None

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._step == 0:
                    self._step += 1
                    return json.dumps(source)
                if self._step == 1:
                    self._step += 1
                    await self.request_sent.wait()
                    self.response_echo = self.request["echo"]
                    return json.dumps({
                        "status": "ok",
                        "echo": self.response_echo,
                        "data": {"bytes": "base64://UklGRgAAAABXQVZFZm10IA=="},
                    })
                raise StopAsyncIteration

            async def send(self, raw_request):
                self.request = json.loads(raw_request)
                self.request_sent.set()

        fake_ws = FakeWebSocket()
        instance._ws = fake_ws
        instance._connected = True
        with patch("adapter.cache_audio_from_bytes", return_value="/cache/voice.wav"):
            await asyncio.wait_for(instance._receive_loop(), timeout=1)
            await asyncio.wait_for(message_handled.wait(), timeout=1)

        self.assertEqual(fake_ws.request["action"], "get_record")
        self.assertEqual(fake_ws.response_echo, fake_ws.request["echo"])
        event = instance._message_handler.await_args.args[0]
        self.assertEqual(event.message_type, adapter.MessageType.VOICE)


if __name__ == "__main__":
    unittest.main()
