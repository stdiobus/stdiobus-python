# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026-present Raman Marozau <raman@worktif.com>
# Copyright (c) 2026-present stdiobus contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main client classes for stdiobus."""

import asyncio
import json
import random
import shutil
import string
import sys
import time
import uuid
from typing import Any, Optional

from stdiobus.types import (
    BusState,
    BackendMode,
    BusConfig,
    BusStats,
    BusOptions,
    DockerOptions,
    SubprocessOptions,
    HelloParams,
    HelloResult,
    Identity,
    AuditEvent,
    RequestOptions,
    MessageHandler,
)
from stdiobus.errors import (
    InvalidArgumentError,
    InvalidStateError,
    TimeoutError,
    TransportError,
    error_from_code,
)
from stdiobus.backends.base import Backend


# ---------------------------------------------------------------------------
# Internal: pending request with streaming chunk aggregation
# ---------------------------------------------------------------------------

class _PendingRequest:
    """Tracks a pending request with optional streaming chunk aggregation."""
    __slots__ = ("future", "chunks")

    def __init__(self, future: "asyncio.Future[Any]"):
        self.future = future
        self.chunks: list[str] = []


def generate_client_session_id() -> str:
    """Generate a unique client session ID for stdiobus routing."""
    timestamp = int(time.time() * 1000)
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"client-{timestamp}-{random_suffix}"


def _resolve_backend(
    mode: BackendMode,
    config_path: Optional[str],
    config_json: Optional[str],
    docker_options: Optional[DockerOptions] = None,
    subprocess_options: Optional[SubprocessOptions] = None,
) -> Backend:
    """Determine and create the appropriate backend."""
    import platform

    if mode == BackendMode.DOCKER:
        return _create_docker_backend(config_path, config_json, docker_options)

    if mode == BackendMode.SUBPROCESS:
        return _create_subprocess_backend(config_path, config_json, subprocess_options)

    if mode == BackendMode.NATIVE:
        try:
            from stdiobus.backends.native import NativeBackend, is_native_available
            if not is_native_available():
                raise ImportError("Native bindings not built")
            return NativeBackend(config_path=config_path, config_json=config_json)
        except ImportError as e:
            raise InvalidArgumentError(
                f"Native backend not available: {e}. "
                "Use backend='subprocess' or backend='docker'."
            )

    # Auto mode
    system = platform.system().lower()

    if system == "windows":
        # Windows: try subprocess first (if binary found), fall back to docker
        opts = subprocess_options or SubprocessOptions()
        if shutil.which(opts.binary_path):
            return _create_subprocess_backend(config_path, config_json, subprocess_options)
        return _create_docker_backend(config_path, config_json, docker_options)

    # Unix: subprocess (if binary found) → native → docker
    opts = subprocess_options or SubprocessOptions()
    if shutil.which(opts.binary_path):
        return _create_subprocess_backend(config_path, config_json, subprocess_options)

    try:
        from stdiobus.backends.native import NativeBackend, is_native_available
        if is_native_available():
            return NativeBackend(config_path=config_path, config_json=config_json)
    except ImportError:
        pass

    return _create_docker_backend(config_path, config_json, docker_options)


def _create_subprocess_backend(
    config_path: Optional[str],
    config_json: Optional[str],
    subprocess_options: Optional[SubprocessOptions],
) -> "Backend":
    """Create subprocess backend."""
    from stdiobus.backends.subprocess import SubprocessBackend
    return SubprocessBackend(
        config_path=config_path,
        config_json=config_json,
        options=subprocess_options,
    )


def _create_docker_backend(
    config_path: Optional[str],
    config_json: Optional[str],
    docker_options: Optional[DockerOptions],
) -> "Backend":
    """Create Docker backend, materializing config to temp file if needed."""
    from stdiobus.backends.docker import DockerBackend

    if config_path:
        return DockerBackend(config_path, docker_options)

    # Materialize JSON to temp file for Docker
    import tempfile, os
    fd, tmp_path = tempfile.mkstemp(prefix="stdiobus-", suffix=".json")
    try:
        os.write(fd, (config_json or "{}").encode())
    finally:
        os.close(fd)
    os.chmod(tmp_path, 0o600)
    return DockerBackend(tmp_path, docker_options)


class AsyncStdioBus:
    """Async stdio_bus client.

    Spawns stdio_bus (subprocess or Docker), communicates via NDJSON,
    supports hello handshake, extensions, and automatic session routing.

    Example:
        async with AsyncStdioBus(config=BusConfig(
            pools=[PoolConfig(id='echo', command='node', args=['worker.js'], instances=1)]
        )) as bus:
            result = await bus.request('echo', {'message': 'hello'})
    """

    def __init__(
        self,
        config: Optional[BusConfig] = None,
        *,
        config_path: Optional[str] = None,
        backend: BackendMode | str = BackendMode.AUTO,
        timeout_ms: int = 30000,
        docker: Optional[DockerOptions] = None,
        subprocess: Optional[SubprocessOptions] = None,
    ):
        has_config = config is not None
        has_path = bool(config_path)

        if has_config and has_path:
            raise InvalidArgumentError("config and config_path are mutually exclusive")
        if not has_config and not has_path:
            raise InvalidArgumentError("config or config_path is required")

        if isinstance(backend, str):
            backend = BackendMode(backend)

        # Resolve config
        resolved_path: Optional[str] = None
        resolved_json: Optional[str] = None

        if has_config:
            config.validate()
            resolved_json = config.to_json()
        else:
            resolved_path = config_path

        self._backend_mode = backend
        self._timeout_ms = timeout_ms
        self._docker_options = docker
        self._subprocess_options = subprocess
        self._backend: Optional[Backend] = None
        self._message_handlers: list[MessageHandler] = []
        self._pending_requests: dict[str, _PendingRequest] = {}
        self._notification_handlers: list[MessageHandler] = []

        # Client session ID — auto-generated, injected into every message
        self._client_session_id = generate_client_session_id()
        # Agent session ID — set after hello or session/new
        self._agent_session_id: Optional[str] = None

        # Create backend
        self._backend = _resolve_backend(
            backend, resolved_path, resolved_json, docker, subprocess,
        )
        self._backend.on_message(self._handle_message)

        # Wire up close callback for subprocess backend (fail pending requests)
        from stdiobus.backends.subprocess import SubprocessBackend
        if isinstance(self._backend, SubprocessBackend):
            self._backend.set_on_close(self._on_backend_closed)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def client_session_id(self) -> str:
        """Client-generated session ID used for stdiobus routing."""
        return self._client_session_id

    @property
    def agent_session_id(self) -> Optional[str]:
        """Agent-returned session ID (from hello or session/new)."""
        return self._agent_session_id

    @agent_session_id.setter
    def agent_session_id(self, value: str) -> None:
        self._agent_session_id = value

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, message: str) -> None:
        """Handle incoming message from backend.

        Dispatches responses to pending futures, aggregates streaming chunks
        from ``agent_message_chunk`` notifications, and forwards notifications
        to registered handlers.
        """
        # User raw-message handlers
        for handler in self._message_handlers:
            try:
                handler(message)
            except Exception as e:
                print(f"[stdiobus] Handler error: {e}", file=sys.stderr)

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        has_id = "id" in data and data["id"] is not None
        has_method = "method" in data

        # --- Notification (has method, no id) ---
        if has_method and not has_id:
            self._handle_notification(data)
            return

        # --- Response (has id) ---
        if has_id:
            msg_id = str(data["id"])
            pending = self._pending_requests.get(msg_id)
            if pending is None:
                return
            self._pending_requests.pop(msg_id)
            if pending.future.done():
                return

            if "error" in data:
                error = data["error"]
                pending.future.set_exception(
                    error_from_code(
                        error.get("code", 99),
                        error.get("message", "Unknown error"),
                        error.get("data"),
                    )
                )
            else:
                result = data.get("result")
                # Attach aggregated streaming text if chunks were collected
                aggregated_text = "".join(pending.chunks)
                if aggregated_text and isinstance(result, dict):
                    result["text"] = aggregated_text
                pending.future.set_result(result)

    def _handle_notification(self, data: dict[str, Any]) -> None:
        """Handle a JSON-RPC notification.

        Extracts text from ``agent_message_chunk`` notifications and
        appends to all pending requests (ACP streaming protocol).
        """
        params = data.get("params")
        if isinstance(params, dict):
            update = params.get("update")
            if isinstance(update, dict):
                session_update = update.get("sessionUpdate")
                if session_update == "agent_message_chunk":
                    content = update.get("content")
                    if isinstance(content, dict):
                        text = content.get("text")
                        if isinstance(text, str) and text:
                            for pending in self._pending_requests.values():
                                pending.chunks.append(text)

        # Forward to notification handlers
        for handler in self._notification_handlers:
            try:
                handler(json.dumps(data))
            except Exception as e:
                print(f"[stdiobus] Notification handler error: {e}", file=sys.stderr)

    def on_message(self, handler: MessageHandler) -> None:
        """Register a raw message handler (receives all messages)."""
        self._message_handlers.append(handler)

    def on_notification(self, handler: MessageHandler) -> None:
        """Register a notification handler (receives only notifications)."""
        self._notification_handlers.append(handler)

    def _on_backend_closed(self) -> None:
        """Called when backend process exits unexpectedly. Fail all pending requests."""
        for msg_id, pending in list(self._pending_requests.items()):
            if not pending.future.done():
                pending.future.set_exception(
                    TransportError("stdio_bus process exited unexpectedly")
                )
        self._pending_requests.clear()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the bus and spawn worker processes."""
        if self._backend is None:
            raise InvalidStateError("Bus not initialized")
        await self._backend.start()

    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the bus gracefully. Cancels all pending requests."""
        # Cancel all in-flight requests
        for msg_id, pending in list(self._pending_requests.items()):
            if not pending.future.done():
                pending.future.set_exception(
                    TransportError("Bus is shutting down")
                )
        self._pending_requests.clear()

        if self._backend is None:
            return
        await self._backend.stop(timeout_sec)

    # ------------------------------------------------------------------
    # Hello handshake
    # ------------------------------------------------------------------

    async def connect(self, params: Optional[HelloParams] = None) -> Optional[HelloResult]:
        """Start the bus and optionally perform hello handshake.

        Args:
            params: Hello parameters. If provided, performs handshake.

        Returns:
            HelloResult if handshake performed, None otherwise.
        """
        await self.start()
        if params is not None:
            return await self.hello(params)
        return None

    async def hello(self, params: Optional[HelloParams] = None) -> HelloResult:
        """Perform stdio_bus/hello handshake.

        Args:
            params: Hello parameters (defaults used if None).

        Returns:
            HelloResult with negotiated protocol version and session ID.
        """
        if params is None:
            params = HelloParams()

        result = await self.request(
            "stdio_bus/hello",
            params.to_dict(),
        )
        hello_result = HelloResult.from_dict(result)
        self._agent_session_id = hello_result.session_id
        return hello_result

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send(self, message: str) -> bool:
        """Send a raw JSON-RPC message."""
        if self._backend is None:
            return False
        return self._backend.send(message)

    async def request(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        timeout_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        options: Optional[RequestOptions] = None,
    ) -> Any:
        """Send a JSON-RPC request and wait for response.

        Args:
            method: RPC method name.
            params: Method parameters.
            timeout_ms: Request timeout in milliseconds.
            session_id: Override session ID for routing.
            options: Advanced request options (identity, audit, agentId).

        Returns:
            Response result.
        """
        if self._backend is None or not self._backend.is_running():
            raise InvalidStateError("Bus not running")

        opts = options or RequestOptions()
        msg_id = str(uuid.uuid4())
        effective_timeout_ms = opts.timeout_ms or timeout_ms or self._timeout_ms
        timeout = effective_timeout_ms / 1000.0

        # Build request
        request: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": method,
        }
        if params:
            request["params"] = params

        # sessionId — always inject for stdiobus routing
        effective_session_id = (
            opts.session_id or session_id or self._client_session_id
        )
        request["sessionId"] = effective_session_id

        # agentId for registry-launcher routing
        if opts.agent_id:
            request["agentId"] = opts.agent_id

        # Extensions (_ext)
        ext: dict[str, Any] = {}
        if opts.identity:
            ext["identity"] = opts.identity.to_dict()
        if opts.audit:
            ext["audit"] = opts.audit.to_dict()
        if ext:
            request["_ext"] = ext

        # Create pending request with chunk aggregation
        loop = asyncio.get_event_loop()
        future: asyncio.Future[Any] = loop.create_future()
        pending = _PendingRequest(future)
        self._pending_requests[msg_id] = pending

        try:
            if not self.send(json.dumps(request)):
                raise TransportError("Failed to send message")
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(msg_id, None)
            raise TimeoutError(f"Request timeout: {method}")
        except Exception:
            self._pending_requests.pop(msg_id, None)
            raise

    async def notify(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        options: Optional[RequestOptions] = None,
    ) -> None:
        """Send a JSON-RPC notification (no response expected)."""
        opts = options or RequestOptions()
        notification: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params:
            notification["params"] = params

        # Always inject sessionId
        notification["sessionId"] = opts.session_id or self._client_session_id

        if opts.agent_id:
            notification["agentId"] = opts.agent_id

        ext: dict[str, Any] = {}
        if opts.identity:
            ext["identity"] = opts.identity.to_dict()
        if opts.audit:
            ext["audit"] = opts.audit.to_dict()
        if ext:
            notification["_ext"] = ext

        self.send(json.dumps(notification))

    # ------------------------------------------------------------------
    # State / Stats
    # ------------------------------------------------------------------

    def get_state(self) -> BusState:
        if self._backend is None:
            return BusState.STOPPED
        return self._backend.get_state()

    def get_stats(self) -> BusStats:
        if self._backend is None:
            return BusStats()
        return self._backend.get_stats()

    def get_backend_type(self) -> str:
        if self._backend is None:
            return "unknown"
        from stdiobus.backends.subprocess import SubprocessBackend
        from stdiobus.backends.docker import DockerBackend
        if isinstance(self._backend, SubprocessBackend):
            return "subprocess"
        elif isinstance(self._backend, DockerBackend):
            return "docker"
        # Check native
        try:
            from stdiobus.backends.native import NativeBackend
            if isinstance(self._backend, NativeBackend):
                return "native"
        except ImportError:
            pass
        return "unknown"

    def is_running(self) -> bool:
        return self.get_state() == BusState.RUNNING

    def destroy(self) -> None:
        if self._backend:
            self._backend.destroy()
            self._backend = None

    async def __aenter__(self) -> "AsyncStdioBus":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
        self.destroy()


class StdioBus:
    """Synchronous stdio_bus client.

    Wraps AsyncStdioBus for synchronous usage.

    Example:
        with StdioBus(config=BusConfig(
            pools=[PoolConfig(id='echo', command='node', args=['worker.js'], instances=1)]
        )) as bus:
            result = bus.request('echo', {'message': 'hello'})
    """

    def __init__(
        self,
        config: Optional[BusConfig] = None,
        *,
        config_path: Optional[str] = None,
        backend: BackendMode | str = BackendMode.AUTO,
        timeout_ms: int = 30000,
        docker: Optional[DockerOptions] = None,
        subprocess: Optional[SubprocessOptions] = None,
    ):
        self._async_bus = AsyncStdioBus(
            config,
            config_path=config_path,
            backend=backend,
            timeout_ms=timeout_ms,
            docker=docker,
            subprocess=subprocess,
        )
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
        return self._loop

    def _run(self, coro: Any) -> Any:
        loop = self._get_loop()
        return loop.run_until_complete(coro)

    @property
    def client_session_id(self) -> str:
        return self._async_bus.client_session_id

    @property
    def agent_session_id(self) -> Optional[str]:
        return self._async_bus.agent_session_id

    @agent_session_id.setter
    def agent_session_id(self, value: str) -> None:
        self._async_bus.agent_session_id = value

    def on_message(self, handler: MessageHandler) -> None:
        self._async_bus.on_message(handler)

    def on_notification(self, handler: MessageHandler) -> None:
        """Register a notification handler."""
        self._async_bus.on_notification(handler)

    def start(self) -> None:
        self._run(self._async_bus.start())

    def stop(self, timeout_sec: float = 30.0) -> None:
        self._run(self._async_bus.stop(timeout_sec))

    def connect(self, params: Optional[HelloParams] = None) -> Optional[HelloResult]:
        """Start and optionally perform hello handshake."""
        return self._run(self._async_bus.connect(params))

    def hello(self, params: Optional[HelloParams] = None) -> HelloResult:
        """Perform stdio_bus/hello handshake."""
        return self._run(self._async_bus.hello(params))

    def send(self, message: str) -> bool:
        return self._async_bus.send(message)

    def request(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        timeout_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        options: Optional[RequestOptions] = None,
    ) -> Any:
        return self._run(
            self._async_bus.request(
                method, params,
                timeout_ms=timeout_ms,
                session_id=session_id,
                options=options,
            )
        )

    def notify(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        options: Optional[RequestOptions] = None,
    ) -> None:
        self._run(self._async_bus.notify(method, params, options=options))

    def get_state(self) -> BusState:
        return self._async_bus.get_state()

    def get_stats(self) -> BusStats:
        return self._async_bus.get_stats()

    def get_backend_type(self) -> str:
        return self._async_bus.get_backend_type()

    def is_running(self) -> bool:
        return self._async_bus.is_running()

    def destroy(self) -> None:
        self._async_bus.destroy()
        if self._loop and not self._loop.is_closed():
            self._loop.close()
            self._loop = None

    def __enter__(self) -> "StdioBus":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()
        self.destroy()
