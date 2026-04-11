"""
stdiobus - Python SDK for building AI agents over stdio_bus.

Example:
    >>> from stdiobus import StdioBus, BusConfig, PoolConfig
    >>>
    >>> with StdioBus(config=BusConfig(
    ...     pools=[PoolConfig(id='echo', command='python', args=['echo_worker.py'], instances=1)]
    ... )) as bus:
    ...     result = bus.request('echo', {'message': 'hello'})

Async Example:
    >>> from stdiobus import AsyncStdioBus, BusConfig, PoolConfig
    >>>
    >>> async with AsyncStdioBus(config=BusConfig(
    ...     pools=[PoolConfig(id='echo', command='python', args=['echo_worker.py'], instances=1)]
    ... )) as bus:
    ...     result = await bus.request('echo', {'message': 'hello'})
"""

from stdiobus.client import StdioBus, AsyncStdioBus
from stdiobus.types import (
    BusState,
    BackendMode,
    BusStats,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    SubprocessOptions,
    HelloParams,
    HelloResult,
    Identity,
    AuditEvent,
    RequestOptions,
)
from stdiobus.errors import (
    StdioBusError,
    InvalidArgumentError,
    InvalidStateError,
    TimeoutError,
    CancelledError,
    TransportError,
    NegotiationFailedError,
    PolicyDeniedError,
    UnavailableError,
    ResourceExhaustedError,
    NotSupportedError,
    InternalError,
)

__version__ = "2.1.0"
__all__ = [
    # Main classes
    "StdioBus",
    "AsyncStdioBus",
    # Config types
    "BusConfig",
    "PoolConfig",
    "LimitsConfig",
    "SubprocessOptions",
    # Protocol types
    "HelloParams",
    "HelloResult",
    "Identity",
    "AuditEvent",
    "RequestOptions",
    # Types
    "BusState",
    "BackendMode",
    "BusStats",
    # Errors
    "StdioBusError",
    "InvalidArgumentError",
    "InvalidStateError",
    "TimeoutError",
    "CancelledError",
    "TransportError",
    "NegotiationFailedError",
    "PolicyDeniedError",
    "UnavailableError",
    "ResourceExhaustedError",
    "NotSupportedError",
    "InternalError",
]
