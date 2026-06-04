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
    ListenMode,
    BusStats,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    SubprocessOptions,
    DockerOptions,
    NativeOptions,
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

__version__ = "2.1.1"
__all__ = [
    # Main classes
    "StdioBus",
    "AsyncStdioBus",
    # Config types
    "BusConfig",
    "PoolConfig",
    "LimitsConfig",
    "SubprocessOptions",
    "DockerOptions",
    "NativeOptions",
    # Protocol types
    "HelloParams",
    "HelloResult",
    "Identity",
    "AuditEvent",
    "RequestOptions",
    # Types
    "BusState",
    "BackendMode",
    "ListenMode",
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
