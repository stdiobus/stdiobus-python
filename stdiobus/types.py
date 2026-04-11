"""Type definitions for stdiobus."""

from dataclasses import dataclass, field
from enum import IntEnum, Enum
from typing import TypedDict, Any, Callable, Optional
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
