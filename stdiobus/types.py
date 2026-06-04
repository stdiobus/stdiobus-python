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

"""Type definitions for stdiobus."""

from dataclasses import dataclass, field
from enum import IntEnum, Enum
from typing import TypedDict, Any, Callable, Literal, Optional
import json


class BusState(IntEnum):
    """Bus lifecycle states."""
    CREATED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4


class BackendMode(str, Enum):
    """Backend selection modes."""
    AUTO = "auto"
    NATIVE = "native"
    DOCKER = "docker"
    SUBPROCESS = "subprocess"


class ListenMode(str, Enum):
    """Transport listen modes."""
    NONE = "none"
    TCP = "tcp"
    UNIX = "unix"


@dataclass
class BusStats:
    """Runtime statistics."""
    messages_in: int = 0
    messages_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    worker_restarts: int = 0
    routing_errors: int = 0
    client_connects: int = 0
    client_disconnects: int = 0


@dataclass
class DockerOptions:
    """Docker backend configuration."""
    image: str = "stdiobus/stdiobus:node20"
    pull_policy: str = "if-missing"  # never, if-missing, always
    engine_path: str = "docker"
    startup_timeout_sec: float = 15.0
    container_name_prefix: str = "stdiobus"
    extra_args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class BusOptions:
    """StdioBus configuration options."""
    config_path: str
    backend: BackendMode = BackendMode.AUTO
    timeout_ms: int = 30000
    docker: Optional[DockerOptions] = None


# ============================================================================
# Programmatic Configuration Types
# ============================================================================

@dataclass
class PoolConfig:
    """Worker pool configuration.
    
    Matches the C bus JSON schema: pools[].{id, command, args, instances}.
    """
    id: str
    command: str
    args: list[str] = field(default_factory=list)
    instances: int = 1


@dataclass
class LimitsConfig:
    """Operational limits. All optional — C bus applies defaults for omitted values."""
    max_input_buffer: Optional[int] = None
    max_output_queue: Optional[int] = None
    max_restarts: Optional[int] = None
    restart_window_sec: Optional[int] = None
    drain_timeout_sec: Optional[int] = None
    backpressure_timeout_sec: Optional[int] = None


@dataclass
class BusConfig:
    """stdio_bus JSON configuration.
    
    Primary way to configure the bus programmatically — no config.json file needed.
    
    Example:
        >>> config = BusConfig(
        ...     pools=[PoolConfig(id='echo', command='node', args=['worker.js'], instances=2)],
        ... )
    """
    pools: list[PoolConfig] = field(default_factory=list)
    limits: Optional[LimitsConfig] = None

    def validate(self) -> None:
        """Validate configuration. Raises ValueError if invalid."""
        if not self.pools:
            raise ValueError("at least one pool is required")
        for i, pool in enumerate(self.pools):
            if not pool.id:
                raise ValueError(f"pool {i} missing id")
            if not pool.command:
                raise ValueError(f"pool '{pool.id}' missing command")
            if pool.instances < 1:
                raise ValueError(f"pool '{pool.id}' instances must be >= 1")

    def to_json(self) -> str:
        """Serialize to JSON string matching C bus schema."""
        data: dict[str, Any] = {
            "pools": [
                {k: v for k, v in {
                    "id": p.id,
                    "command": p.command,
                    "args": p.args,
                    "instances": p.instances,
                }.items()}
                for p in self.pools
            ]
        }
        if self.limits is not None:
            limits_dict = {}
            for f_name in ("max_input_buffer", "max_output_queue", "max_restarts",
                           "restart_window_sec", "drain_timeout_sec", "backpressure_timeout_sec"):
                val = getattr(self.limits, f_name)
                if val is not None:
                    limits_dict[f_name] = val
            if limits_dict:
                data["limits"] = limits_dict
        return json.dumps(data)


@dataclass
class SubprocessOptions:
    """Subprocess backend configuration."""
    binary_path: str = "stdio_bus"
    start_timeout_sec: float = 5.0
    drain_timeout_sec: float = 30.0
    stderr_buffer_lines: int = 200
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class NativeOptions:
    """Native (in-process) backend configuration.

    Groups the native-only options that the embedded ``libstdio_bus`` exposes,
    mirroring the ``DockerOptions`` / ``SubprocessOptions`` convention used by
    the public client constructor.

    The listener (``listen_mode`` other than :class:`ListenMode.NONE`) turns the
    in-process bus into a server that accepts *external* clients over TCP or a
    Unix domain socket. It is a native-backend capability only: the subprocess
    and Docker backends do not expose a user-controlled listener, so requesting
    a non-NONE ``listen_mode`` with any other backend is rejected by the client.

    Defaults are explicit and intentional:
    - ``listen_mode=NONE``     → embedded mode, messages flow via send()/on_message().
    - ``tcp_host="127.0.0.1"`` → loopback bind (no external exposure) when TCP is used.
    - ``poll_interval_ms=1``   → native step granularity (matches NativeBackend default).
    """
    listen_mode: "ListenMode | str" = ListenMode.NONE
    tcp_host: str = "127.0.0.1"
    tcp_port: Optional[int] = None
    unix_path: Optional[str] = None
    poll_interval_ms: int = 1

    def __post_init__(self) -> None:
        """Normalize ``listen_mode`` to the :class:`ListenMode` enum.

        Strings are accepted for ergonomic parity with the client's
        ``backend: BackendMode | str`` option. Coercing here guarantees the
        invariant that ``listen_mode`` is always a ``ListenMode`` after
        construction, so downstream ``.value`` access is always safe.
        """
        if not isinstance(self.listen_mode, ListenMode):
            try:
                self.listen_mode = ListenMode(self.listen_mode)
            except ValueError as e:
                raise ValueError(f"invalid listen_mode: {self.listen_mode!r}") from e

    def validate(self) -> None:
        """Validate native options. Raises ValueError if invalid.

        Mirrors :meth:`BusConfig.validate` — a config object validates itself.
        """
        if self.listen_mode == ListenMode.TCP:
            # Port 0 (ephemeral) is intentionally rejected: the native backend
            # exposes no API to report back the OS-chosen port, so a bound
            # listener on port 0 would be undiscoverable by callers.
            if self.tcp_port is None:
                raise ValueError("listen_mode=TCP requires tcp_port")
            if not (1 <= self.tcp_port <= 65535):
                raise ValueError(
                    f"tcp_port must be in 1..65535, got {self.tcp_port}"
                )
        elif self.listen_mode == ListenMode.UNIX:
            if not self.unix_path:
                raise ValueError("listen_mode=UNIX requires unix_path")
        if self.poll_interval_ms < 1:
            raise ValueError(
                f"poll_interval_ms must be >= 1, got {self.poll_interval_ms}"
            )


# ============================================================================
# Protocol Types (Hello handshake, Extensions, Identity, Audit)
# ============================================================================

@dataclass
class ExtensionInfo:
    """Extension negotiation info."""
    version: str
    required: bool = False
    active: bool = False


@dataclass
class HelloParams:
    """Parameters for stdio_bus/hello handshake."""
    protocol_version: str = "0.1.0"
    extensions: dict[str, ExtensionInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON-RPC params."""
        return {
            "protocolVersion": self.protocol_version,
            "extensions": {
                k: {"version": v.version, "required": v.required}
                for k, v in self.extensions.items()
            },
        }


@dataclass
class HelloResult:
    """Result from stdio_bus/hello handshake."""
    negotiated_protocol_version: str
    session_id: str
    extensions: dict[str, ExtensionInfo] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HelloResult":
        """Deserialize from JSON-RPC result."""
        exts = {}
        for k, v in data.get("extensions", {}).items():
            exts[k] = ExtensionInfo(
                version=v.get("selected", v.get("version", "")),
                active=v.get("active", False),
            )
        return cls(
            negotiated_protocol_version=data.get("negotiatedProtocolVersion", ""),
            session_id=data.get("sessionId", ""),
            extensions=exts,
        )


@dataclass
class Identity:
    """Identity extension data."""
    subject_id: str
    role: str
    asserted_by: str = "self"

    def to_dict(self) -> dict[str, Any]:
        return {
            "subjectId": self.subject_id,
            "role": self.role,
            "assertedBy": self.asserted_by,
        }


@dataclass
class AuditEvent:
    """Audit event extension data."""
    event_id: str
    action: str
    parent_event_id: Optional[str] = None
    timestamp: Optional[str] = None
    actor: Optional[dict[str, str]] = None
    resource: Optional[str] = None
    outcome: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"eventId": self.event_id, "action": self.action}
        if self.parent_event_id:
            d["parentEventId"] = self.parent_event_id
        if self.timestamp:
            d["timestamp"] = self.timestamp
        if self.actor:
            d["actor"] = self.actor
        if self.resource:
            d["resource"] = self.resource
        if self.outcome:
            d["outcome"] = self.outcome
        return d


@dataclass
class RequestOptions:
    """Options for individual requests."""
    timeout_ms: Optional[int] = None
    session_id: Optional[str] = None
    agent_id: Optional[str] = None
    identity: Optional[Identity] = None
    audit: Optional[AuditEvent] = None


# ============================================================================
# Incremental Streaming Types (AsyncStdioBus.stream_request)
# ============================================================================

StreamEventType = Literal["chunk", "result"]


@dataclass(frozen=True)
class StreamEvent:
    """A typed event yielded by :meth:`AsyncStdioBus.stream_request`.

    A stream yields zero or more ``chunk`` events (one per
    ``agent_message_chunk`` text, in arrival order) followed by exactly one
    ``result`` event carrying the final JSON-RPC result.

    ``type == "chunk"``  → ``text`` holds one incremental chunk; ``result`` is None.
    ``type == "result"`` → ``result`` holds the final result (with the aggregated
                           ``result["text"]`` identical to ``request()``);
                           ``text`` is None.

    The event is frozen (immutable): consumers should treat it as read-only.
    ``Literal`` is used for ``type`` over an ``Enum`` for zero-ceremony call
    sites (``event.type == "chunk"``).
    """
    type: StreamEventType
    text: Optional[str] = None
    result: Optional[Any] = None


# ============================================================================
# Pull-based Notification Subscription Types (AsyncStdioBus.subscribe_notifications)
# ============================================================================

OverflowPolicy = Literal["drop", "close"]
"""Bounded-queue overflow behavior for a notification :class:`Subscriber`.

``"drop"``  → when the subscriber's queue is full, discard the *newest*
              notification, leaving the subscriber open and other subscribers
              untouched.
``"close"`` → when the subscriber's queue is full, terminate *that* subscriber
              only; buffered items are still drained before iteration stops.

Applies solely to notification subscriptions. The streaming response queue
(``stream_request``) is unbounded and never drops, since the aggregated result
depends on every chunk.
"""


class JsonRpcRequest(TypedDict, total=False):
    """JSON-RPC 2.0 request."""
    jsonrpc: str
    id: str
    method: str
    params: dict[str, Any]


class JsonRpcResponse(TypedDict, total=False):
    """JSON-RPC 2.0 response."""
    jsonrpc: str
    id: str
    result: Any
    error: dict[str, Any]


# Callback types
MessageHandler = Callable[[str], None]
ErrorHandler = Callable[[Exception], None]
