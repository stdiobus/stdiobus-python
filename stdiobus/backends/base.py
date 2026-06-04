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

"""Base backend interface for stdiobus."""

from abc import ABC, abstractmethod
from typing import Callable, Optional

from stdiobus.types import BusState, BusStats, ListenMode


class Backend(ABC):
    """Abstract base class for stdiobus backends."""
    
    @abstractmethod
    async def start(self) -> None:
        """Start the backend."""
        ...
    
    @abstractmethod
    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the backend gracefully."""
        ...
    
    @abstractmethod
    def send(self, message: str) -> bool:
        """Send a message. Returns True if queued successfully."""
        ...
    
    @abstractmethod
    def on_message(self, handler: Callable[[str], None]) -> None:
        """Register a message handler."""
        ...
    
    @abstractmethod
    def get_state(self) -> BusState:
        """Get current state."""
        ...
    
    @abstractmethod
    def get_stats(self) -> BusStats:
        """Get statistics."""
        ...
    
    @abstractmethod
    def destroy(self) -> None:
        """Release all resources."""
        ...
    
    def is_running(self) -> bool:
        """Check if backend is running."""
        return self.get_state() == BusState.RUNNING

    # ------------------------------------------------------------------
    # Introspection — default ("not supported by this backend") behavior.
    #
    # The sentinel -1 follows the cross-SDK convention (see the Node SDK,
    # where Docker's getWorkerCount() returns -1) meaning "this backend has
    # no channel to report this value". Returning 0 would be a lie: workers
    # may exist, the backend simply cannot count them. Backends that *can*
    # report a value override these methods.
    # ------------------------------------------------------------------

    def get_listen_mode(self) -> ListenMode:
        """Return the effective external listener mode.

        Defaults to NONE: only the native backend exposes a user-controlled
        listener; subprocess/docker communicate over their own internal
        transport and have no external listener surface.
        """
        return ListenMode.NONE

    def get_worker_count(self) -> int:
        """Return the number of running workers, or -1 if not introspectable."""
        return -1

    def get_client_count(self) -> int:
        """Return the number of connected clients, or -1 if not introspectable."""
        return -1
