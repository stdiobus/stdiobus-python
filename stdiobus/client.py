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
import string
import sys
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any, Optional

from stdiobus.types import (
    BusState,
    BackendMode,
    ListenMode,
    BusConfig,
    BusStats,
    BusOptions,
    DockerOptions,
    SubprocessOptions,
    NativeOptions,
    HelloParams,
    HelloResult,
    Identity,
    AuditEvent,
    RequestOptions,
    StreamEvent,
    OverflowPolicy,
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
    """Tracks a pending request, in one of two delivery modes.

    Future-mode (``request()``)
        ``future`` is set and ``queue`` is ``None``. The final result/error is
        delivered by resolving the future, exactly as before this feature.

    Stream-mode (``stream_request()``)
        ``queue`` is set and ``future`` is ``None``. Chunk, result, and error
        events are enqueued as ``(kind, payload)`` tuples for an async
        generator to consume incrementally.

    In both modes ``chunks`` accumulates ``agent_message_chunk`` text so the
    aggregated ``result["text"]`` is computed identically. The stream queue is
    deliberately *unbounded*: a response stream must never drop a chunk, since
    the aggregated result depends on every chunk (contrast with the bounded,
    drop-capable notification subscriptions).
    """

    __slots__ = ("future", "chunks", "queue")

    def __init__(
        self,
        future: "Optional[asyncio.Future[Any]]" = None,
        queue: "Optional[asyncio.Queue[tuple[str, Any]]]" = None,
    ):
        self.future = future
        self.chunks: list[str] = []
        self.queue = queue

    @classmethod
    def for_future(cls, future: "asyncio.Future[Any]") -> "_PendingRequest":
        """Create a future-mode pending request (used by ``request()``)."""
        return cls(future=future)

    @classmethod
    def for_stream(cls) -> "_PendingRequest":
        """Create a stream-mode pending request (used by ``stream_request()``).

        The backing queue is unbounded so no chunk is ever dropped.
        """
        return cls(queue=asyncio.Queue())

    # -- Polymorphic delivery -------------------------------------------------
    # ``_handle_message_on_loop`` calls these without branching on mode.

    def deliver_chunk(self, text: str) -> None:
        """Record an incremental chunk; surface it live in stream-mode."""
        self.chunks.append(text)
        if self.queue is not None:
            self.queue.put_nowait(("chunk", text))

    def deliver_result(self, result: Any) -> None:
        """Deliver the final result.

        ``result`` already carries the aggregated ``result["text"]`` computed
        by the caller, identical to the pre-feature behavior.
        """
        if self.queue is not None:
            self.queue.put_nowait(("result", result))
        elif self.future is not None and not self.future.done():
            self.future.set_result(result)

    def deliver_error(self, exc: Exception) -> None:
        """Deliver a terminal error (transport, timeout, or JSON-RPC error)."""
        if self.queue is not None:
            self.queue.put_nowait(("error", exc))
        elif self.future is not None and not self.future.done():
            self.future.set_exception(exc)


# ---------------------------------------------------------------------------
# Internal: pull-based notification subscription
# ---------------------------------------------------------------------------

# Module-level sentinel enqueued to signal end-of-iteration for a subscription.
# Identity comparison (``is``) distinguishes it from any delivered notification
# dict, so it can never collide with real payload data.
_CLOSE_SENTINEL: object = object()


class NotificationSubscription:
    """Independent pull stream of JSON-RPC notifications, backed by a bounded queue.

    Each subscriber returned by :meth:`AsyncStdioBus.subscribe_notifications` owns
    its own bounded :class:`asyncio.Queue` and overflow policy, so one
    subscriber's full, slow, or closed queue never affects another (Property 6 /
    subscriber isolation). The object is an async iterator: ``async for n in sub``
    awaits and yields parsed notification dicts until the subscription is closed.

    Lifecycle / termination (Property 8 — termination liveness):
      * a delivered notification unblocks a waiting ``__anext__``;
      * :meth:`close` (explicit, or driven by ``stop()``/``destroy()``, or by the
        ``overflow="close"`` policy) terminates iteration with
        ``StopAsyncIteration``;
      * **drain-then-stop**: when closed, any items already buffered in the queue
        are delivered first; ``StopAsyncIteration`` is raised only once the queue
        drains. This holds even in the overflow-on-close edge where the queue was
        full and the close sentinel could not be enqueued — ``__anext__``
        re-checks ``_closed`` against an empty queue and stops.

    Delivery (:meth:`_deliver`) and :meth:`close` run on the owning loop, invoked
    by the bus from ``_handle_notification_on_loop`` and lifecycle teardown
    respectively; the consumer drives ``__anext__`` from the same loop.
    """

    __slots__ = ("_owner", "_queue", "_overflow", "_closed")

    def __init__(
        self,
        owner: "AsyncStdioBus",
        max_queue: int,
        overflow: OverflowPolicy,
    ) -> None:
        self._owner = owner
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue)
        self._overflow: OverflowPolicy = overflow
        self._closed = False

    def __aiter__(self) -> "NotificationSubscription":
        return self

    async def __anext__(self) -> dict[str, Any]:
        # Drain-then-stop: terminate only once closed AND the buffer is empty,
        # so buffered notifications are never dropped on teardown. This also
        # covers the overflow="close" edge where the queue was full and no
        # sentinel could be enqueued — the consumer drains the buffer, then this
        # check fires on the next call.
        if self._closed and self._queue.empty():
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is _CLOSE_SENTINEL:
            raise StopAsyncIteration
        return item

    def _deliver(self, notification: dict[str, Any]) -> None:
        """Enqueue one notification for this subscriber, applying overflow policy.

        Called on the owning loop by the bus. A full queue triggers the
        configured :class:`~stdiobus.types.OverflowPolicy`: ``"drop"`` discards
        the newest notification (leaving this and every other subscriber intact),
        ``"close"`` terminates only this subscriber. Either way no other
        subscriber is affected (Property 6).
        """
        if self._closed:
            return
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            if self._overflow == "drop":
                return  # discard newest; this subscriber and others stay intact
            self.close()  # overflow == "close": terminate only this subscriber

    def close(self) -> None:
        """Terminate this subscription and detach it from the owning bus.

        Idempotent. Removes the subscription from the owner's active set (so it
        receives no further notifications) and enqueues the close sentinel so a
        consumer blocked in ``__anext__`` is unblocked and stops. If the queue is
        full the sentinel cannot be enqueued, but the buffered items remain and
        ``__anext__`` raises ``StopAsyncIteration`` once they drain (drain-then-stop).
        """
        if self._closed:
            return
        self._closed = True
        self._owner._remove_subscription(self)
        try:
            self._queue.put_nowait(_CLOSE_SENTINEL)
        except asyncio.QueueFull:
            pass  # buffered items remain; __anext__ stops once the queue drains


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
    native_options: Optional[NativeOptions] = None,
) -> Backend:
    """Determine and create the appropriate backend.

    Listener contract: an external listener (``native_options.listen_mode`` other
    than NONE) is a native-backend-only capability. If a listener is requested,
    the resolved backend MUST be native — for explicit ``backend='docker'`` /
    ``'subprocess'`` this raises, and for ``backend='auto'`` it forces native
    rather than silently degrading to a transport with no listener.
    """
    import platform

    listener_requested = (
        native_options is not None
        and native_options.listen_mode != ListenMode.NONE
    )

    if mode == BackendMode.DOCKER:
        if listener_requested:
            raise InvalidArgumentError(
                "listen_mode is a native-backend capability and is not "
                "supported with backend='docker'. Use backend='native'."
            )
        return _create_docker_backend(config_path, config_json, docker_options)

    if mode == BackendMode.SUBPROCESS:
        if listener_requested:
            raise InvalidArgumentError(
                "listen_mode is a native-backend capability and is not "
                "supported with backend='subprocess'. Use backend='native'."
            )
        return _create_subprocess_backend(config_path, config_json, subprocess_options)

    if mode == BackendMode.NATIVE:
        return _create_native_backend(config_path, config_json, native_options)

    # Auto mode
    system = platform.system().lower()

    # A requested listener can only be served by the native backend, so honor
    # it directly instead of falling through to subprocess/docker.
    if listener_requested:
        return _create_native_backend(config_path, config_json, native_options)

    from stdiobus._resolve_binary import resolve_binary

    if system == "windows":
        # Windows: try subprocess first (if binary found), fall back to docker
        opts = subprocess_options or SubprocessOptions()
        if resolve_binary(opts.binary_path) is not None:
            return _create_subprocess_backend(config_path, config_json, subprocess_options)
        return _create_docker_backend(config_path, config_json, docker_options)

    # Unix: subprocess (if binary found) → native → docker
    opts = subprocess_options or SubprocessOptions()
    if resolve_binary(opts.binary_path) is not None:
        return _create_subprocess_backend(config_path, config_json, subprocess_options)

    try:
        from stdiobus.backends.native import is_native_available
        if is_native_available():
            return _create_native_backend(config_path, config_json, native_options)
    except ImportError:
        pass

    return _create_docker_backend(config_path, config_json, docker_options)


def _create_native_backend(
    config_path: Optional[str],
    config_json: Optional[str],
    native_options: Optional[NativeOptions],
) -> "Backend":
    """Create the native (in-process) backend, forwarding listener options.

    Raises InvalidArgumentError if the native bindings are not built, so the
    failure mode is identical whether selected explicitly or via auto+listener.
    """
    try:
        from stdiobus.backends.native import NativeBackend, is_native_available
        if not is_native_available():
            raise ImportError("Native bindings not built")
    except ImportError as e:
        raise InvalidArgumentError(
            f"Native backend not available: {e}. "
            "Use backend='subprocess' or backend='docker'."
        )

    opts = native_options or NativeOptions()
    return NativeBackend(
        config_path=config_path,
        config_json=config_json,
        listen_mode=opts.listen_mode.value,
        tcp_host=opts.tcp_host,
        tcp_port=opts.tcp_port or 0,
        unix_path=opts.unix_path or "",
        poll_interval_ms=opts.poll_interval_ms,
    )


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
        native: Optional[NativeOptions] = None,
    ):
        has_config = config is not None
        has_path = bool(config_path)

        if has_config and has_path:
            raise InvalidArgumentError("config and config_path are mutually exclusive")
        if not has_config and not has_path:
            raise InvalidArgumentError("config or config_path is required")

        if isinstance(backend, str):
            backend = BackendMode(backend)

        # Validate native options up front (fail fast, before backend creation).
        if native is not None:
            try:
                native.validate()
            except ValueError as e:
                raise InvalidArgumentError(str(e))

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
        self.native_options = native
        self._backend: Optional[Backend] = None
        self._message_handlers: list[MessageHandler] = []
        self._pending_requests: dict[str, _PendingRequest] = {}
        self._notification_handlers: list[MessageHandler] = []

        # Active pull-based notification subscribers (R2). Each owns an
        # independent bounded queue; the set is mutated only on the owning loop.
        self._subscriptions: set[NotificationSubscription] = set()

        # Owning event loop, captured in start(). Until then it is None and
        # _handle_message runs inline (e.g. unit tests drive it pre-start).
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Client session ID — auto-generated, injected into every message
        self._client_session_id = generate_client_session_id()
        # Agent session ID — set after hello or session/new
        self._agent_session_id: Optional[str] = None

        # Create backend
        self._backend = _resolve_backend(
            backend, resolved_path, resolved_json, docker, subprocess, native,
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
        """Thread-marshalling shim for incoming backend messages.

        Backends may invoke this from their own thread (the native backend uses
        a background poll thread). Because the streaming and subscription paths
        mutate ``asyncio.Queue`` objects, and ``_pending_requests`` is shared,
        all such mutations must occur on the owning loop. Foreign-thread calls
        are therefore marshalled onto that loop via ``call_soon_threadsafe``.

        Pre-start (no loop captured yet, e.g. unit tests that drive
        ``_handle_message`` directly) and same-loop calls run inline, preserving
        the existing synchronous behavior.
        """
        loop = self._loop
        if loop is None:
            # Pre-start: run inline (existing unit-test path).
            self._handle_message_on_loop(message)
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            self._handle_message_on_loop(message)
        else:
            loop.call_soon_threadsafe(self._handle_message_on_loop, message)

    def _handle_message_on_loop(self, message: str) -> None:
        """Handle an incoming message on the owning loop.

        Dispatches responses to pending requests, aggregates streaming chunks
        from ``agent_message_chunk`` notifications, and forwards notifications
        to registered handlers. Delivery is routed through the pending request's
        ``deliver_*`` methods so future-mode (``request()``) and stream-mode
        (``stream_request()``) share one dispatch path; future-mode behavior is
        identical to before this feature.
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
            self._handle_notification_on_loop(data)
            return

        # --- Response (has id) ---
        if has_id:
            msg_id = str(data["id"])
            pending = self._pending_requests.get(msg_id)
            if pending is None:
                return
            self._pending_requests.pop(msg_id)

            if "error" in data:
                error = data["error"]
                pending.deliver_error(
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
                pending.deliver_result(result)

    def _handle_notification_on_loop(self, data: dict[str, Any]) -> None:
        """Handle a JSON-RPC notification on the owning loop.

        Extracts text from ``agent_message_chunk`` notifications and delivers it
        to all pending requests (the ACP streaming protocol carries no
        per-request correlation id), then forwards to push handlers.
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
                                pending.deliver_chunk(text)

        # Forward to notification handlers
        for handler in self._notification_handlers:
            try:
                handler(json.dumps(data))
            except Exception as e:
                print(f"[stdiobus] Notification handler error: {e}", file=sys.stderr)

        # Deliver to pull-based subscribers (R2.1, R2.2). Each subscriber gets
        # its own shallow copy so an accidental mutation by one consumer cannot
        # leak into another's payload; iterate a snapshot because _deliver may
        # close (and thus remove) a subscriber mid-iteration under
        # overflow="close". Push callbacks above always fire first and remain
        # unchanged (R2.3).
        for sub in list(self._subscriptions):
            sub._deliver(dict(data))

    def on_message(self, handler: MessageHandler) -> None:
        """Register a raw message handler (receives all messages)."""
        self._message_handlers.append(handler)

    def on_notification(self, handler: MessageHandler) -> None:
        """Register a notification handler (receives only notifications)."""
        self._notification_handlers.append(handler)

    def _remove_subscription(self, sub: "NotificationSubscription") -> None:
        """Detach a subscription from the active set (idempotent).

        Called by :meth:`NotificationSubscription.close`. ``discard`` keeps this
        safe if the subscription was already removed (e.g. closed twice or torn
        down by ``stop()``/``destroy()``), satisfying the "remove on close" half
        of R2.6.
        """
        self._subscriptions.discard(sub)

    def _on_backend_closed(self) -> None:
        """Called when backend process exits unexpectedly. Fail all pending requests.

        Delivery is routed through :meth:`_PendingRequest.deliver_error` so both
        delivery modes terminate correctly: future-mode (``request()``) pendings
        have the exception set on their future (unchanged behavior), while
        stream-mode (``stream_request()``) pendings receive an ``("error", exc)``
        item on their queue, unblocking a consumer waiting in ``async for``.

        Per R1.6 a backend exit during an active stream always surfaces as
        ``TransportError`` (even if the total deadline also looms), and the
        pending table is cleared so Property 1 (total cleanup) holds.
        """
        for pending in list(self._pending_requests.values()):
            pending.deliver_error(
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
        # Capture the owning loop so foreign-thread backend callbacks can be
        # marshalled here (see _handle_message).
        self._loop = asyncio.get_running_loop()
        await self._backend.start()

    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the bus gracefully. Cancels all pending requests.

        Terminal delivery is routed through :meth:`_PendingRequest.deliver_error`
        so future-mode (``request()``) pendings have the exception set on their
        future (unchanged behavior) and stream-mode (``stream_request()``)
        pendings receive a terminal ``TransportError`` on their queue, unblocking
        any consumer iterating the stream (Property 8). The pending table is then
        cleared so cleanup is total (Property 1).
        """
        # Cancel all in-flight requests (future-mode and stream-mode alike)
        for pending in list(self._pending_requests.values()):
            pending.deliver_error(
                TransportError("Bus is shutting down")
            )
        self._pending_requests.clear()

        # Close every live subscription so consumers iterating their streams are
        # unblocked and terminate via StopAsyncIteration (R2.7, Property 8).
        # close() removes each from _subscriptions, so iterate a snapshot.
        for sub in list(self._subscriptions):
            sub.close()

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

    def _build_request(
        self,
        msg_id: str,
        method: str,
        params: Optional[dict[str, Any]],
        opts: RequestOptions,
        session_id: Optional[str],
    ) -> dict[str, Any]:
        """Assemble the JSON-RPC request message for the wire.

        Single source of truth for request serialization, shared by
        ``request()`` and ``stream_request()`` so both produce byte-identical
        messages. Behavior (sessionId injection/override precedence, optional
        ``params``, ``agentId``, and ``_ext`` identity/audit) is exactly as the
        previously inline assembly in ``request()``.
        """
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

        return request

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

        # Build request (shared wire assembly with stream_request)
        request = self._build_request(msg_id, method, params, opts, session_id)

        # Create pending request with chunk aggregation
        loop = asyncio.get_event_loop()
        future: asyncio.Future[Any] = loop.create_future()
        pending = _PendingRequest.for_future(future)
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

    async def stream_request(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        timeout_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        options: Optional[RequestOptions] = None,
    ) -> AsyncIterator[StreamEvent]:
        """Send a request and yield agent output incrementally as typed events.

        Yields zero or more :class:`~stdiobus.types.StreamEvent` ``chunk`` events
        (one per ``agent_message_chunk`` text, in arrival order), then exactly
        one ``result`` event carrying the final result — including the aggregated
        ``result["text"]``, identical to what :meth:`request` returns for the
        same response sequence.

        Args:
            method: RPC method name.
            params: Method parameters.
            timeout_ms: Total-deadline timeout in milliseconds for the whole
                stream (chunks + final result), not a per-chunk timeout.
            session_id: Override session ID for routing.
            options: Advanced request options (identity, audit, agentId).

        Yields:
            StreamEvent: ``chunk`` events followed by a single ``result`` event.

        Raises (from within the ``async for``):
            InvalidStateError: if the bus is not running, or if another
                ``stream_request`` is already active (only one active stream is
                supported because ``agent_message_chunk`` carries no per-request
                correlation id and is broadcast to every pending request).
            TimeoutError: on total-deadline expiry.
            TransportError: if the message cannot be sent or the backend exits
                during the stream.
            StdioBusError: the mapped SDK exception on a JSON-RPC error response,
                raised after any chunk events already received are yielded.

        Async-only. The synchronous :class:`StdioBus` wrapper does not provide
        this method.
        """
        if self._backend is None or not self._backend.is_running():
            raise InvalidStateError("Bus not running")

        # Fail-fast: only one active stream is safe. agent_message_chunk
        # notifications carry no per-request correlation id and are broadcast to
        # every pending request, so concurrent streams would share/duplicate
        # chunk events and silently corrupt each stream's output.
        if any(p.queue is not None for p in self._pending_requests.values()):
            raise InvalidStateError(
                "Only one active stream_request is supported at a time"
            )

        opts = options or RequestOptions()
        msg_id = str(uuid.uuid4())
        effective_timeout_ms = opts.timeout_ms or timeout_ms or self._timeout_ms
        deadline = time.monotonic() + effective_timeout_ms / 1000.0

        pending = _PendingRequest.for_stream()
        self._pending_requests[msg_id] = pending
        request = self._build_request(msg_id, method, params, opts, session_id)
        assert pending.queue is not None  # for_stream() always sets the queue
        try:
            if not self.send(json.dumps(request)):
                raise TransportError("Failed to send message")
            while True:
                # Drain any already-delivered item before considering the
                # deadline. This guarantees a terminal transport/JSON-RPC error
                # (or a buffered chunk/result) that is ready at or after the
                # deadline still wins over a timeout — R1.6 requires that a
                # backend exit ALWAYS surfaces as TransportError "regardless of
                # whether the exit also causes a timeout condition", and
                # Property 4 requires a single terminal outcome. A genuine
                # timeout (R1.4) only fires when the queue is empty.
                #
                # empty()+get_nowait() is race-free: this stream-mode queue has
                # a single consumer running on the owning loop, with no await
                # between the check and the get.
                if not pending.queue.empty():
                    item = pending.queue.get_nowait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"Request timeout: {method}")
                    try:
                        item = await asyncio.wait_for(
                            pending.queue.get(), remaining
                        )
                    except asyncio.TimeoutError:
                        raise TimeoutError(f"Request timeout: {method}")
                kind, payload = item
                if kind == "chunk":
                    yield StreamEvent(type="chunk", text=payload)
                elif kind == "result":
                    yield StreamEvent(type="result", result=payload)
                    return
                elif kind == "error":
                    raise payload
        finally:
            # Cleanup on every exit path (result, timeout, transport/JSON-RPC
            # error, consumer cancel/early-break): the pending entry and its
            # queue are released. Guarantees Property 1 (pending-table cleanup
            # is total).
            self._pending_requests.pop(msg_id, None)

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

    def subscribe_notifications(
        self,
        *,
        max_queue: int = 256,
        overflow: OverflowPolicy = "drop",
    ) -> "NotificationSubscription":
        """Return a pull-based async iterator over JSON-RPC notifications.

        Each call creates an independent :class:`NotificationSubscription` backed
        by its own bounded queue (``max_queue``) and overflow policy
        (``overflow``). Multiple subscribers are fully isolated — every active
        subscriber receives every notification delivered after it was created
        (R2.1, R2.2), and one subscriber's full/slow/closed queue never affects
        another (Property 6).

        This is additive and orthogonal to the existing :meth:`on_notification`
        push callback, which continues to fire for every notification (R2.3).

        Args:
            max_queue: Maximum buffered notifications for this subscriber. When
                full, the ``overflow`` policy is applied.
            overflow: Behavior when the bounded queue is full —
                ``"drop"`` discards the newest notification (default),
                ``"close"`` terminates this subscriber.

        Returns:
            NotificationSubscription: an async iterator yielding parsed
            notification dicts. Treat the yielded dicts as read-only.

        Usage::

            async for notification in bus.subscribe_notifications():
                handle(notification)

        Async-only. The synchronous :class:`StdioBus` wrapper does not provide
        this method; its :meth:`on_notification` push callback is unaffected.
        """
        sub = NotificationSubscription(self, max_queue, overflow)
        self._subscriptions.add(sub)
        return sub

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

    def get_listen_mode(self) -> ListenMode:
        """Return the effective external listener mode of the active backend.

        Returns :class:`ListenMode.NONE` when no backend exists or the backend
        exposes no user-controlled listener (subprocess/docker).
        """
        if self._backend is None:
            return ListenMode.NONE
        return self._backend.get_listen_mode()

    def get_worker_count(self) -> int:
        """Return the number of running workers.

        Returns -1 when the active backend cannot report this value (e.g.
        subprocess and docker have no worker-introspection channel).
        """
        if self._backend is None:
            return -1
        return self._backend.get_worker_count()

    def get_client_count(self) -> int:
        """Return the number of connected clients.

        Returns -1 when the active backend cannot report this value. For the
        native backend with a listener this is the count of external clients;
        for docker it is whether this SDK is connected to the container (0/1).
        """
        if self._backend is None:
            return -1
        return self._backend.get_client_count()

    def is_running(self) -> bool:
        return self.get_state() == BusState.RUNNING

    def destroy(self) -> None:
        # Close every live subscription so active iterators terminate via
        # StopAsyncIteration (R2.7, Property 8). close() removes each from
        # _subscriptions, so iterate a snapshot.
        for sub in list(self._subscriptions):
            sub.close()
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
        native: Optional[NativeOptions] = None,
    ):
        self._async_bus = AsyncStdioBus(
            config,
            config_path=config_path,
            backend=backend,
            timeout_ms=timeout_ms,
            docker=docker,
            subprocess=subprocess,
            native=native,
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

    def get_listen_mode(self) -> ListenMode:
        return self._async_bus.get_listen_mode()

    def get_worker_count(self) -> int:
        return self._async_bus.get_worker_count()

    def get_client_count(self) -> int:
        return self._async_bus.get_client_count()

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


# ---------------------------------------------------------------------------
# Fluent builder (R3)
# ---------------------------------------------------------------------------

class StdioBusBuilder:
    """Fluent, thin builder over ``AsyncStdioBus.__init__`` / ``StdioBus.__init__``.

    This builder exists to give consumers the step-by-step construction idiom of
    the Rust and Node SDKs *without* forking the flat constructor. It is
    deliberately a thin facade: it accumulates keyword arguments only and
    performs **no validation of its own**. All validation — ``config`` /
    ``config_path`` mutual exclusivity, the "config or config_path required"
    rule, ``backend`` string coercion, and ``NativeOptions`` validation — runs
    inside the unchanged ``__init__`` invoked by :meth:`build` / :meth:`build_sync`.
    Consequently, invalid inputs raise the *same* SDK exceptions as direct
    construction, with the validation logic living in exactly one place (R3.2,
    R3.4).

    Each setter mirrors one keyword of the constructor signature, stores it into
    the pending keyword-argument map, and returns ``self`` so calls chain
    fluently. Re-invoking a setter overwrites the previous value (last write
    wins), matching how a single keyword would behave if passed directly.

    Example:
        >>> bus = (
        ...     StdioBusBuilder()
        ...     .config(BusConfig(pools=[PoolConfig(id="echo", command="node")]))
        ...     .backend("subprocess")
        ...     .timeout_ms(15000)
        ...     .build()
        ... )

    The builder targets ``AsyncStdioBus`` via :meth:`build`; :meth:`build_sync`
    constructs the synchronous ``StdioBus`` wrapper from the identical keyword
    set, so the two builds stay in lockstep with no duplicated configuration.
    """

    __slots__ = ("_kwargs",)

    def __init__(self) -> None:
        self._kwargs: dict[str, Any] = {}

    def config(self, config: BusConfig) -> "StdioBusBuilder":
        """Set the in-memory :class:`BusConfig` (mutually exclusive with config_path)."""
        self._kwargs["config"] = config
        return self

    def config_path(self, path: str) -> "StdioBusBuilder":
        """Set the path to a config file (mutually exclusive with config)."""
        self._kwargs["config_path"] = path
        return self

    def backend(self, mode: "BackendMode | str") -> "StdioBusBuilder":
        """Select the backend mode; a string is coerced by the constructor."""
        self._kwargs["backend"] = mode
        return self

    def timeout_ms(self, timeout_ms: int) -> "StdioBusBuilder":
        """Set the default per-request timeout in milliseconds."""
        self._kwargs["timeout_ms"] = timeout_ms
        return self

    def docker(self, options: DockerOptions) -> "StdioBusBuilder":
        """Set Docker backend options."""
        self._kwargs["docker"] = options
        return self

    def subprocess(self, options: SubprocessOptions) -> "StdioBusBuilder":
        """Set subprocess backend options."""
        self._kwargs["subprocess"] = options
        return self

    def native(self, options: NativeOptions) -> "StdioBusBuilder":
        """Set native (in-process) backend options."""
        self._kwargs["native"] = options
        return self

    def build(self) -> "AsyncStdioBus":
        """Construct an :class:`AsyncStdioBus` from the accumulated keywords.

        Splats the stored keywords straight into the unchanged constructor, so
        the result is equivalent to a direct ``AsyncStdioBus(**kwargs)`` call and
        all validation/exception behavior is identical (R3.1, R3.2, R3.4).
        """
        return AsyncStdioBus(**self._kwargs)

    def build_sync(self) -> "StdioBus":
        """Construct a synchronous :class:`StdioBus` from the accumulated keywords.

        Uses the same keyword set as :meth:`build`, delegating to the unchanged
        ``StdioBus.__init__`` (which itself wraps ``AsyncStdioBus.__init__``), so
        validation is reused rather than duplicated.
        """
        return StdioBus(**self._kwargs)
