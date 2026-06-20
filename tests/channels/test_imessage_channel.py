"""Tests for iMessage channel: send() branches, sidecar integration, space resolution."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.channels.imessage import IMessageChannel


def _make_channel(**overrides) -> IMessageChannel:
    """Create an IMessageChannel with a mock httpx client wired up."""
    bus = MagicMock()
    bus.publish_inbound = AsyncMock()
    cfg = {"enabled": True, "project_id": "sp-test", "project_secret": "s-test", "allow_from": ["*"]}
    cfg.update(overrides)
    ch = IMessageChannel(cfg, bus)
    ch._client = AsyncMock()
    ch._running = True
    return ch


def _mock_response(status: int = 200, json_body: dict | None = None):
    """Create a mock httpx response with raise_for_status wired up."""
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_body or {}
    if status >= 400:

        def _raise():
            import httpx

            raise httpx.HTTPStatusError("error", request=MagicMock(), response=resp)

        resp.raise_for_status.side_effect = _raise
    else:
        resp.raise_for_status.return_value = resp
    return resp


# ── #4: send() four-branch coverage ──────────────────────────────────


@pytest.mark.asyncio
async def test_send_text_only():
    """Only text content → single POST /send."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(200, {"ok": True})

    msg = OutboundMessage(channel="imessage", chat_id="space-1", content="hello")
    await ch.send(msg)

    ch._client.post.assert_called_once()
    url = ch._client.post.call_args[0][0]
    payload = ch._client.post.call_args[1]["json"]
    assert url.endswith("/send")
    assert payload == {"space": "space-1", "text": "hello"}


@pytest.mark.asyncio
async def test_send_media_only():
    """Only media (empty content) → single POST /send-attachment."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(200, {"ok": True})

    msg = OutboundMessage(
        channel="imessage", chat_id="space-1", content="", media=["/tmp/photo.jpg"]
    )
    await ch.send(msg)

    ch._client.post.assert_called_once()
    url = ch._client.post.call_args[0][0]
    payload = ch._client.post.call_args[1]["json"]
    assert url.endswith("/send-attachment")
    assert payload["filePath"] == "/tmp/photo.jpg"
    assert payload["fileName"] == "photo.jpg"


@pytest.mark.asyncio
async def test_send_both_text_and_media():
    """Both content and media → two POSTs: /send then /send-attachment."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(200, {"ok": True})

    msg = OutboundMessage(
        channel="imessage",
        chat_id="space-1",
        content="check this",
        media=["/tmp/doc.pdf"],
    )
    await ch.send(msg)

    assert ch._client.post.call_count == 2
    calls = ch._client.post.call_args_list
    text_url = calls[0][0][0]
    text_payload = calls[0][1]["json"]
    media_url = calls[1][0][0]
    media_payload = calls[1][1]["json"]

    assert text_url.endswith("/send")
    assert text_payload["text"] == "check this"
    assert media_url.endswith("/send-attachment")
    assert media_payload["filePath"] == "/tmp/doc.pdf"


@pytest.mark.asyncio
async def test_send_empty_skips_post():
    """Both content and media empty → no POST at all, just debug log."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(200, {"ok": True})

    msg = OutboundMessage(channel="imessage", chat_id="space-1", content="", media=[])
    await ch.send(msg)

    ch._client.post.assert_not_called()


# ── #5: client not connected → raise ──────────────────────────────────


@pytest.mark.asyncio
async def test_send_raises_when_client_not_connected():
    """send() must raise RuntimeError when _client is None."""
    ch = _make_channel()
    ch._client = None

    msg = OutboundMessage(channel="imessage", chat_id="space-1", content="hi")
    with pytest.raises(RuntimeError, match="not connected"):
        await ch.send(msg)


# ── #3: space resolution cold-start (sidecar 404 → raise) ─────────────


@pytest.mark.asyncio
async def test_send_raises_on_sidecar_404():
    """When sidecar returns 404 (space not found), Python raises."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(404, {"error": "Space not found: space-99"})

    msg = OutboundMessage(channel="imessage", chat_id="space-99", content="hi")
    with pytest.raises(Exception):
        await ch.send(msg)


# ── Sidecar integration mock ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_sidecar_inbound_dispatches_to_handle_message():
    """Verify that an inbound NDJSON line from sidecar triggers _handle_message."""
    ch = _make_channel()
    line = json.dumps({
        "type": "message",
        "sender": "+1234567890",
        "chat_id": "space-1",
        "content": "hello",
        "message_id": "msg-001",
        "is_group": False,
        "was_mentioned": False,
        "media": [],
    })

    await ch._handle_inbound(json.loads(line))

    ch.bus.publish_inbound.assert_called_once()
    inbound = ch.bus.publish_inbound.call_args[0][0]
    assert inbound.channel == "imessage"
    assert inbound.sender_id == "+1234567890"
    assert inbound.content == "hello"


@pytest.mark.asyncio
async def test_sidecar_inbound_dedup():
    """Duplicate message IDs are dropped."""
    ch = _make_channel()
    data = {
        "type": "message",
        "sender": "+1234567890",
        "chat_id": "space-1",
        "content": "hello",
        "message_id": "msg-dup",
        "is_group": False,
        "was_mentioned": False,
        "media": [],
    }

    await ch._handle_inbound(dict(data))
    assert ch.bus.publish_inbound.call_count == 1

    await ch._handle_inbound(dict(data))
    assert ch.bus.publish_inbound.call_count == 1  # still 1


@pytest.mark.asyncio
async def test_sidecar_send_routing():
    """POST /send and /send-attachment hit the correct URLs with correct payloads."""
    ch = _make_channel()
    ch._client.post.return_value = _mock_response(200, {"ok": True})

    # text
    await ch.send(OutboundMessage(channel="imessage", chat_id="s1", content="hi"))
    assert ch._client.post.call_args_list[-1][0][0].endswith("/send")

    # attachment
    ch._client.post.reset_mock()
    await ch.send(OutboundMessage(channel="imessage", chat_id="s1", content="", media=["/f.png"]))
    assert ch._client.post.call_args_list[-1][0][0].endswith("/send-attachment")


@pytest.mark.asyncio
async def test_single_inbound_consumer_guard():
    """Verify that the sidecar single-consumer guard is present in the source."""
    import pathlib

    src = (
        pathlib.Path(__file__).parent.parent.parent
        / "bridge"
        / "imessage-sidecar"
        / "src"
        / "index.ts"
    ).read_text()
    assert "inboundGeneration" in src
    assert "myGeneration !== inboundGeneration" in src

def test_spawn_sidecar_raises_when_port_in_use(monkeypatch):
    """_spawn_sidecar() should raise RuntimeError if the sidecar port is occupied."""
    import socket
    from pathlib import Path

    ch = _make_channel()
    ch.config.sidecar_port = 18998
    monkeypatch.setattr("nanobot.channels.imessage.subprocess.Popen", lambda *args, **kwargs: None)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", ch.config.sidecar_port))
        sock.listen(1)
        with pytest.raises(RuntimeError, match="already in use"):
            ch._spawn_sidecar(Path("/tmp/fake-sidecar"))
    finally:
        sock.close()
