"""iMessage channel implementation using Photon Spectrum sidecar."""

import asyncio
import hashlib
import io
import json
import mimetypes
import os
import shutil
import socket
import subprocess
import threading
from collections import OrderedDict
from contextlib import suppress
from pathlib import Path
from typing import Any, Literal

import httpx
from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base


class IMessageConfig(Base):
    """iMessage channel configuration via Photon Spectrum."""

    enabled: bool = False
    project_id: str = ""
    project_secret: str = ""
    sidecar_url: str = "http://127.0.0.1:8789"
    sidecar_port: int = 8789
    sidecar_autostart: bool = True
    node_bin: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["open", "mention"] = "open"
    mention_patterns: list[str] = Field(default_factory=list)


class IMessageChannel(BaseChannel):
    """
    iMessage channel that connects to a Photon Spectrum Node.js sidecar.

    The sidecar uses spectrum-ts SDK to handle the iMessage protocol.
    Communication between Python and Node.js is via HTTP loopback.
    """

    name = "imessage"
    display_name = "iMessage"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return IMessageConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = IMessageConfig.model_validate(config)
        super().__init__(config, bus)
        self._proc: subprocess.Popen | None = None
        self._client: httpx.AsyncClient | None = None
        self._inbound_client: httpx.AsyncClient | None = None
        self._stderr_thread: threading.Thread | None = None
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()

    async def login(self, force: bool = False) -> bool:
        """
        Check that project credentials are configured.

        Returns True if project_id and project_secret are set,
        otherwise prints instructions and returns False.
        """
        # TODO: device-code login
        if self.config.project_id and self.config.project_secret:
            return True
        self.logger.warning(
            "iMessage channel requires project_id and project_secret. "
            "Get them from https://app.photon.codes → Settings."
        )
        return False

    async def start(self) -> None:
        """Start the iMessage channel: setup sidecar, spawn it, and stream inbound messages."""
        try:
            sidecar_dir = _ensure_sidecar_setup()
        except RuntimeError:
            self.logger.exception("Sidecar setup failed")
            return

        if self.config.sidecar_autostart:
            self._spawn_sidecar(sidecar_dir)

        self._running = True
        # #1: separate clients for unbounded inbound stream vs normal outbound requests
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        self._inbound_client = httpx.AsyncClient(timeout=None)

        sidecar_url = self.config.sidecar_url
        inbound_url = f"{sidecar_url}/inbound"

        self.logger.info("Connecting to iMessage sidecar at {}...", sidecar_url)

        while self._running:
            try:
                async with self._inbound_client.stream("GET", inbound_url) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        self.logger.warning(
                            "Sidecar /inbound returned {}: {}; retrying in 5s...",
                            resp.status_code,
                            body[:200],
                        )
                        await asyncio.sleep(5)
                        continue

                    async for line in resp.aiter_lines():
                        if not line or not self._running:
                            continue
                        try:
                            data = json.loads(line)
                            await self._handle_inbound(data)
                        except json.JSONDecodeError:
                            self.logger.warning("Invalid JSON from sidecar: {}", line[:100])
                        except Exception:
                            self.logger.exception("Error handling inbound message")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning("iMessage sidecar connection error: {}", e)
                if self._running:
                    self.logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the iMessage channel."""
        self._running = False

        if self._client:
            with suppress(Exception):
                await self._client.aclose()
            self._client = None

        if self._inbound_client:
            with suppress(Exception):
                await self._inbound_client.aclose()
            self._inbound_client = None

        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
            self._proc = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through iMessage. Raises on delivery failure.

        Note: after a sidecar restart, the first outbound message to a group
        chat may fail with a 404 if no inbound message has been received yet
        (group chat spaces are not resolvable via the SDK's user-lookup API).
        The channel manager's retry will typically succeed once the next
        inbound message populates the sidecar's space cache.
        """
        if not self._client:
            raise RuntimeError("iMessage sidecar not connected")

        # #4: nothing to send
        if not msg.content and not msg.media:
            self.logger.debug("send() called with empty content and media; skipping")
            return

        sidecar_url = self.config.sidecar_url

        if msg.content:
            try:
                payload = {"space": msg.chat_id, "text": msg.content}
                resp = await self._client.post(f"{sidecar_url}/send", json=payload)
                resp.raise_for_status()
            except Exception:
                self.logger.exception("Error sending message")
                raise

        for media_path in msg.media or []:
            try:
                mime, _ = mimetypes.guess_type(media_path)
                payload = {
                    "space": msg.chat_id,
                    "filePath": media_path,
                    "mimetype": mime or "application/octet-stream",
                    "fileName": media_path.rsplit("/", 1)[-1],
                }
                resp = await self._client.post(
                    f"{sidecar_url}/send-attachment", json=payload
                )
                resp.raise_for_status()
            except Exception:
                self.logger.exception("Error sending media {}", media_path)
                raise

    def _spawn_sidecar(self, sidecar_dir: Path) -> None:
        """Spawn the Node.js sidecar as a subprocess."""
        node_bin = self.config.node_bin or shutil.which("node") or "node"
        port = self.config.sidecar_port

        # Detect orphan sidecar from a previous crash: if the port is already
        # in use, the old process is still alive.  Fail fast instead of
        # spawning another sidecar that will immediately fail to bind.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(
                    f"Port {port} is already in use — a sidecar from a previous "
                    "crash may still be running. Kill it and retry."
                )
        finally:
            sock.close()

        env = {
            **os.environ,
            "PROJECT_ID": self.config.project_id,
            "PROJECT_SECRET": self.config.project_secret,
            "PORT": str(port),
        }

        self.logger.info("Starting iMessage sidecar (node={}, port={})...", node_bin, port)
        self._proc = subprocess.Popen(
            [node_bin, "dist/index.js"],
            cwd=sidecar_dir,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Forward sidecar stderr to nanobot logger on a daemon thread so it
        # never blocks the main loop.  stdout stays DEVNULL — spectrum-ts
        # already logs to stderr via its own structured logger.
        if self._proc.stderr:
            self._stderr_thread = threading.Thread(
                target=_forward_stderr,
                args=(self._proc.stderr, self.logger),
                daemon=True,
            )
            self._stderr_thread.start()

    async def _handle_inbound(self, data: dict) -> None:
        """Handle an inbound message from the sidecar."""
        if data.get("type") != "message":
            return

        sender = data.get("sender", "")
        chat_id = data.get("chat_id", "")
        content = data.get("content", "")
        message_id = data.get("message_id", "")
        is_group = bool(data.get("is_group", False))
        was_mentioned = bool(data.get("was_mentioned", False))
        raw_media = data.get("media") or []

        # Group mention policy
        if is_group and self.config.group_policy == "mention":
            if not was_mentioned:
                # Also check mention_patterns against content
                hit = any(
                    pattern in content
                    for pattern in (self.config.mention_patterns or [])
                )
                if not hit:
                    return

        # Dedup
        if message_id:
            if message_id in self._processed_message_ids:
                return
            self._processed_message_ids[message_id] = None
            while len(self._processed_message_ids) > 1000:
                self._processed_message_ids.popitem(last=False)

        # Media: sidecar sends metadata (filename + mimetype); bytes via
        # content.read() is TODO in the sidecar.
        # For now, tag content with [file: filename] markers.
        # Inbound media paths are empty until content.read() is implemented;
        # the tags below let the LLM know about attachments.
        if raw_media:
            for m in raw_media:
                fname = m.get("fileName", "")
                mime = m.get("mimetype", "")
                if mime and mime.startswith("image/"):
                    tag = f"[image: {fname}]"
                else:
                    tag = f"[file: {fname}]"
                content = f"{content}\n{tag}" if content else tag

        # TODO: voice transcription — gated on sidecar content.read() support

        await self._handle_message(
            sender_id=sender,
            chat_id=chat_id,
            content=content,
            media=[],  # TODO: real paths once sidecar supports content.read()
            metadata={
                "message_id": message_id,
                "is_group": is_group,
                "was_mentioned": was_mentioned,
            },
            is_dm=not is_group,
        )


def _forward_stderr(stream: io.TextIOBase, logger: Any) -> None:
    """Read lines from *stream* and forward them to *logger* at WARNING level.

    Runs on a daemon thread so it never blocks the event loop.  Exits when
    the stream is closed (sidecar exits or pipe is broken).
    """
    for line in stream:
        line = line.rstrip("\n")
        if line:
            logger.warning("sidecar: {}", line)


def _ensure_sidecar_setup() -> Path:
    """
    Ensure the iMessage sidecar is installed and built.

    Copies sidecar source from the package or source tree into the runtime
    bridge directory, using source-hash caching to skip rebuilds when nothing
    changed.  Returns the built sidecar directory.

    Raises RuntimeError if npm is not found or the sidecar cannot be built.
    """
    from nanobot.config.paths import get_bridge_install_dir

    user_sidecar = get_bridge_install_dir() / "imessage-sidecar"
    stamp_file = user_sidecar / ".nanobot-sidecar-source-hash"

    # Find source sidecar — package tree first, then source tree
    current_file = Path(__file__)
    pkg_sidecar = current_file.parent.parent / "bridge" / "imessage-sidecar"
    src_sidecar = current_file.parent.parent.parent / "bridge" / "imessage-sidecar"

    source = None
    if (pkg_sidecar / "package.json").exists():
        source = pkg_sidecar
    elif (src_sidecar / "package.json").exists():
        source = src_sidecar

    if not source:
        raise RuntimeError(
            "iMessage sidecar source not found. "
            "Try reinstalling: pip install --force-reinstall nanobot"
        )

    def source_hash(root: Path) -> str:
        digest = hashlib.sha256()
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root)
            if rel.parts and rel.parts[0] in {"node_modules", "dist"}:
                continue
            digest.update(rel.as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()

    expected_hash = source_hash(source)
    current_hash = stamp_file.read_text().strip() if stamp_file.exists() else None

    if (user_sidecar / "dist" / "index.js").exists() and current_hash == expected_hash:
        return user_sidecar

    if (user_sidecar / "dist" / "index.js").exists() and current_hash != expected_hash:
        logger.info("iMessage sidecar source changed; rebuilding...")

    npm_path = shutil.which("npm")
    if not npm_path:
        raise RuntimeError("npm not found. Please install Node.js >= 18.17.")

    logger.info("Setting up iMessage sidecar...")
    user_sidecar.parent.mkdir(parents=True, exist_ok=True)
    if user_sidecar.exists():
        shutil.rmtree(user_sidecar)
    shutil.copytree(
        source, user_sidecar, ignore=shutil.ignore_patterns("node_modules", "dist")
    )

    logger.info("  Installing dependencies...")
    subprocess.run(
        [npm_path, "install"], cwd=user_sidecar, check=True, capture_output=True
    )

    logger.info("  Building...")
    subprocess.run(
        [npm_path, "run", "build"], cwd=user_sidecar, check=True, capture_output=True
    )
    stamp_file.write_text(expected_hash + "\n")

    logger.info("iMessage sidecar ready")
    return user_sidecar