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

"""Error definitions for stdiobus.

Canonical error codes per spec/host-api.md Section 4.
"""

from enum import IntEnum
from typing import Any, Optional


class ErrorCode(IntEnum):
    """Canonical error codes."""
    INVALID_ARGUMENT = 1
    INVALID_STATE = 2
    TIMEOUT = 3
    CANCELLED = 4
    TRANSPORT_ERROR = 5
    NEGOTIATION_FAILED = 6
    POLICY_DENIED = 7
    UNAVAILABLE = 8
    RESOURCE_EXHAUSTED = 9
    NOT_SUPPORTED = 10
    INTERNAL = 99


class StdioBusError(Exception):
    """Base exception for all stdiobus errors."""
    
    code: ErrorCode = ErrorCode.INTERNAL
    
    def __init__(self, message: str, details: Optional[Any] = None):
        super().__init__(message)
        self.message = message
        self.details = details
    
    def __str__(self) -> str:
        return f"[{self.code.name}] {self.message}"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        result: dict[str, Any] = {
            "code": self.code.value,
            "message": self.message,
        }
        if self.details is not None:
            result["details"] = self.details
        return result


class InvalidArgumentError(StdioBusError):
    """Invalid parameter or option."""
    code = ErrorCode.INVALID_ARGUMENT


class InvalidStateError(StdioBusError):
    """Operation not valid in current state."""
    code = ErrorCode.INVALID_STATE


class TimeoutError(StdioBusError):
    """Operation exceeded deadline."""
    code = ErrorCode.TIMEOUT


class CancelledError(StdioBusError):
    """Operation was cancelled."""
    code = ErrorCode.CANCELLED


class TransportError(StdioBusError):
    """Transport-level failure."""
    code = ErrorCode.TRANSPORT_ERROR


class NegotiationFailedError(StdioBusError):
    """Extension/capability negotiation failed."""
    code = ErrorCode.NEGOTIATION_FAILED


class PolicyDeniedError(StdioBusError):
    """Operation denied by policy."""
    code = ErrorCode.POLICY_DENIED


class UnavailableError(StdioBusError):
    """Service temporarily unavailable."""
    code = ErrorCode.UNAVAILABLE


class ResourceExhaustedError(StdioBusError):
    """Buffer/queue limits exceeded."""
    code = ErrorCode.RESOURCE_EXHAUSTED


class NotSupportedError(StdioBusError):
    """Operation not supported."""
    code = ErrorCode.NOT_SUPPORTED


class InternalError(StdioBusError):
    """Internal error."""
    code = ErrorCode.INTERNAL


def error_from_code(code: int, message: str, details: Optional[Any] = None) -> StdioBusError:
    """Create appropriate error instance from code."""
    error_classes = {
        ErrorCode.INVALID_ARGUMENT: InvalidArgumentError,
        ErrorCode.INVALID_STATE: InvalidStateError,
        ErrorCode.TIMEOUT: TimeoutError,
        ErrorCode.CANCELLED: CancelledError,
        ErrorCode.TRANSPORT_ERROR: TransportError,
        ErrorCode.NEGOTIATION_FAILED: NegotiationFailedError,
        ErrorCode.POLICY_DENIED: PolicyDeniedError,
        ErrorCode.UNAVAILABLE: UnavailableError,
        ErrorCode.RESOURCE_EXHAUSTED: ResourceExhaustedError,
        ErrorCode.NOT_SUPPORTED: NotSupportedError,
        ErrorCode.INTERNAL: InternalError,
    }
    
    try:
        error_code = ErrorCode(code)
        error_class = error_classes.get(error_code, InternalError)
    except ValueError:
        error_class = InternalError
    
    return error_class(message, details)
