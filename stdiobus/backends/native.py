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

"""Native backend using cffi bindings to libstdio_bus.

Threading model
---------------
All C API calls (stdio_bus_step, stdio_bus_ingest, stdio_bus_stop,
stdio_bus_get_state, stdio_bus_get_stats) execute exclusively on the owning
asyncio event loop thread — matching the C library contract:

    "Single-threaded: must be called from one thread
     (integrate with host event loop)"

Integration modes (determined at start-time):

1. **fd-driven** (preferred): ``stdio_bus_get_poll_fd(bus)`` returns a valid fd
   (kqueue on macOS, epoll on Linux). Registered with asyncio via
   ``loop.add_reader()`` for zero-latency wake on kernel events.

2. **timer-driven** (fallback): When the library build does not expose a poll
   fd (returns -1), a repeating ``loop.call_later()`` timer drives
   ``stdio_bus_step(bus, 0)`` at ``poll_interval_ms`` cadence. This preserves
   the single-thread invariant while working with older library prebuilds.

Both modes guarantee that all C API calls occur on the event loop thread.
"""

import asyncio
import sys
import threading
from typing import Callable, List, Optional

from stdiobus.backends.base import Backend
from stdiobus.types import BusState, BusStats, ListenMode
from stdiobus.errors import (
    InvalidStateError,
    TransportError,
)

# Try to import cffi bindings
try:
    from stdiobus.native._ffi import ffi, lib
    NATIVE_AVAILABLE = True
except ImportError:
    NATIVE_AVAILABLE = False
    ffi = None
    lib = None


def is_native_available() -> bool:
    """Check if native backend is available."""
    return NATIVE_AVAILABLE


class NativeBackend(Backend):
    """Native backend using libstdio_bus via cffi.

    This backend embeds the C library directly, providing:
    - No external process spawning
    - Direct memory access
    - Lower latency
    - Unix-only (Linux, macOS)

    All C API calls are confined to the owning asyncio event loop thread.
    The event loop is notified of pending work either via the kernel's poll fd
    (zero-latency, if the library supports it) or via a timer-driven fallback
    (bounded by poll_interval_ms).

    Build the native extension:
        cd stdiobus-python
        python -m stdiobus.native.build_ffi
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config_json: Optional[str] = None,
        listen_mode: str = "none",
        tcp_host: str = "127.0.0.1",
        tcp_port: int = 0,
        unix_path: str = "",
        poll_interval_ms: int = 1,
    ):
        if not NATIVE_AVAILABLE:
            raise ImportError(
                "Native backend not available. "
                "Build with: cd stdiobus-python && python -m stdiobus.native.build_ffi"
            )

        self._config_path = config_path
        self._config_json = config_json
        self._listen_mode = listen_mode
        self._tcp_host = tcp_host
        self._tcp_port = tcp_port
        self._unix_path = unix_path
        self._poll_interval_ms = poll_interval_ms

        self._bus = None
        self._message_handlers: List[Callable[[str], None]] = []
        self._stats = BusStats()

        # Event loop integration state
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._poll_fd: Optional[int] = None  # None = timer mode, >= 0 = fd mode
        self._poll_timer: Optional[asyncio.TimerHandle] = None
        self._loop_thread_id: Optional[int] = None
        self._stopping: bool = False
        self._stepping: bool = False  # Reentrancy guard for step()

        # Keep references to callbacks to prevent GC
        self._message_cb = None
        self._error_cb = None
        self._log_cb = None
        self._worker_cb = None

        # Keep references to C strings
        self._c_strings: List["ffi.CData"] = []

    def _keep_string(self, s: str) -> "ffi.CData":
        """Create a C string and keep reference to prevent GC."""
        c_str = ffi.new("char[]", s.encode('utf-8'))
        self._c_strings.append(c_str)
        return c_str

    def _assert_loop_thread(self) -> None:
        """Assert that the current thread is the owning event loop thread.

        This enforces the C library's single-thread contract at runtime.
        Raises TransportError if called from a foreign thread.
        """
        if self._loop_thread_id is not None:
            current = threading.get_ident()
            if current != self._loop_thread_id:
                raise TransportError(
                    "NativeBackend C API calls must run on the owning event loop "
                    f"thread (expected tid={self._loop_thread_id}, "
                    f"got tid={current}). "
                    "Do not call send() from a background thread."
                )

    def _setup_callbacks(self) -> dict:
        """Set up cffi callbacks."""
        backend = self  # Capture for closures

        @ffi.callback("void(stdio_bus_t*, const char*, size_t, void*)")
        def on_message(bus, msg, length, user_data):
            try:
                message = ffi.string(msg, length).decode('utf-8')
                backend._stats.messages_out += 1
                for handler in backend._message_handlers:
                    try:
                        handler(message)
                    except Exception as e:
                        print(f"[stdiobus] Handler error: {e}", file=sys.stderr)
            except Exception as e:
                print(f"[stdiobus] Message decode error: {e}", file=sys.stderr)

        @ffi.callback("void(stdio_bus_t*, int, const char*, void*)")
        def on_error(bus, code, msg, user_data):
            error_msg = ffi.string(msg).decode('utf-8') if msg != ffi.NULL else "Unknown error"
            print(f"[stdiobus] Error {code}: {error_msg}", file=sys.stderr)

        @ffi.callback("void(stdio_bus_t*, int, const char*, void*)")
        def on_log(bus, level, msg, user_data):
            if msg == ffi.NULL:
                return
            log_msg = ffi.string(msg).decode('utf-8')
            # Only log warnings and errors (level >= 2)
            if level >= 2:
                print(f"[stdiobus] {log_msg}", file=sys.stderr)

        @ffi.callback("void(stdio_bus_t*, int, const char*, void*)")
        def on_worker(bus, worker_id, event, user_data):
            if event != ffi.NULL:
                event_str = ffi.string(event).decode('utf-8')
                if event_str == "restarting":
                    backend._stats.worker_restarts += 1

        # Store references to prevent GC
        self._message_cb = on_message
        self._error_cb = on_error
        self._log_cb = on_log
        self._worker_cb = on_worker

        return {
            'on_message': on_message,
            'on_error': on_error,
            'on_log': on_log,
            'on_worker': on_worker,
        }

    async def start(self) -> None:
        """Start the native backend.

        Captures the running event loop, creates the C bus instance, starts
        workers, and registers either the kernel poll fd (preferred) or a
        timer-based fallback with asyncio so that stdio_bus_step() is driven
        exclusively from the event loop thread.
        """
        if self._bus is not None:
            raise InvalidStateError("Backend already started")

        loop = asyncio.get_running_loop()
        callbacks = self._setup_callbacks()

        # Create options struct
        options = ffi.new("stdio_bus_options_t*")
        if self._config_path:
            options.config_path = self._keep_string(self._config_path)
            options.config_json = ffi.NULL
        elif self._config_json:
            options.config_path = ffi.NULL
            options.config_json = self._keep_string(self._config_json)
        else:
            raise TransportError("No config source provided")
        options.user_data = ffi.NULL
        options.on_message = callbacks['on_message']
        options.on_error = callbacks['on_error']
        options.on_log = callbacks['on_log']
        options.on_worker = callbacks['on_worker']
        options.on_client_connect = ffi.NULL
        options.on_client_disconnect = ffi.NULL
        options.log_level = 1  # INFO

        # Configure listener
        if self._listen_mode == "tcp":
            options.listener.mode = lib.STDIO_BUS_LISTEN_TCP
            options.listener.tcp_host = self._keep_string(self._tcp_host)
            options.listener.tcp_port = self._tcp_port
        elif self._listen_mode == "unix":
            options.listener.mode = lib.STDIO_BUS_LISTEN_UNIX
            options.listener.unix_path = self._keep_string(self._unix_path)
        else:
            options.listener.mode = lib.STDIO_BUS_LISTEN_NONE

        # Create bus
        self._bus = lib.stdio_bus_create(options)
        if self._bus == ffi.NULL:
            self._cleanup_strings()
            raise TransportError("Failed to create stdio_bus instance")

        # Start workers
        result = lib.stdio_bus_start(self._bus)
        if result != lib.STDIO_BUS_OK:
            lib.stdio_bus_destroy(self._bus)
            self._bus = None
            self._cleanup_strings()
            raise TransportError(f"Failed to start stdio_bus: error code {result}")

        # Capture loop state for thread-affinity enforcement
        self._loop = loop
        self._loop_thread_id = threading.get_ident()
        self._stopping = False

        # Attempt fd-driven integration (preferred, zero-latency wake)
        poll_fd = lib.stdio_bus_get_poll_fd(self._bus)
        if poll_fd >= 0 and hasattr(loop, 'add_reader'):
            self._poll_fd = poll_fd
            loop.add_reader(poll_fd, self._on_poll_ready)
            self._poll_timer = None
        else:
            # Fallback: timer-driven polling from the event loop thread.
            # Satisfies single-thread invariant with bounded latency.
            self._poll_fd = None
            self._schedule_poll_timer()

    def _schedule_poll_timer(self) -> None:
        """Schedule the next timer-driven poll step on the event loop."""
        if self._stopping or self._loop is None:
            return
        interval = self._poll_interval_ms / 1000.0
        self._poll_timer = self._loop.call_later(interval, self._on_poll_timer)

    def _on_poll_timer(self) -> None:
        """Timer callback: drive step and reschedule."""
        self._poll_timer = None
        if self._stopping:
            return
        self._step_nonblocking()
        self._schedule_poll_timer()

    def _on_poll_ready(self) -> None:
        """Called by the event loop when the poll fd is readable.

        Drives stdio_bus_step(bus, 0) non-blockingly. The step processes
        pending I/O (worker stdout, lifecycle events, backpressure) and
        invokes registered callbacks (on_message, on_worker, etc.) inline.
        """
        if self._stopping:
            return
        self._step_nonblocking()

    def _step_nonblocking(self) -> None:
        """Execute one non-blocking step, guarded against reentrancy.

        Called from either the fd-readiness callback or the timer callback.
        Both paths guarantee we are on the owning event loop thread.
        """
        if self._stepping or self._stopping:
            return
        if self._bus is None or self._bus == ffi.NULL:
            return

        self._stepping = True
        try:
            lib.stdio_bus_step(self._bus, 0)
        finally:
            self._stepping = False

    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the native backend.

        All C calls here run on the event loop thread (the caller is async).
        Removes poll integration before issuing stop/drain.
        """
        if self._bus is None or self._bus == ffi.NULL:
            return

        self._stopping = True

        # Remove event loop integration BEFORE stopping the bus
        if self._loop is not None and self._poll_fd is not None:
            self._loop.remove_reader(self._poll_fd)
            self._poll_fd = None

        if self._poll_timer is not None:
            self._poll_timer.cancel()
            self._poll_timer = None

        # Initiate graceful shutdown
        result = lib.stdio_bus_stop(self._bus, int(timeout_sec))
        if result != lib.STDIO_BUS_OK:
            print(f"[stdiobus] Warning: stop returned {result}", file=sys.stderr)

        # Continue stepping until fully stopped (drain pending I/O).
        # Use a bounded loop to avoid infinite spin if something is stuck.
        max_drain_steps = int(timeout_sec * 1000)  # ~1ms per step budget
        for _ in range(max_drain_steps):
            state = lib.stdio_bus_get_state(self._bus)
            if state == lib.STDIO_BUS_STATE_STOPPED:
                break
            lib.stdio_bus_step(self._bus, 1)
        else:
            print("[stdiobus] Warning: drain loop exhausted before STOPPED", file=sys.stderr)

    def send(self, message: str) -> bool:
        """Send a message to workers.

        Must be called from the owning event loop thread (enforced by
        thread-affinity check). Since AsyncStdioBus.send() is called from
        async methods running on the loop, this invariant holds in normal use.

        After ingest, schedules an immediate step so outbound data can make
        progress without waiting for the next timer/fd tick.
        """
        if self._bus is None or self._bus == ffi.NULL:
            return False

        self._assert_loop_thread()

        msg_bytes = message.encode('utf-8')
        result = lib.stdio_bus_ingest(self._bus, msg_bytes, len(msg_bytes))
        if result == lib.STDIO_BUS_OK:
            self._stats.messages_in += 1
            # Schedule an immediate step so outbound data progresses without
            # waiting for the next timer tick or fd readiness notification.
            if self._loop is not None and not self._stopping:
                self._loop.call_soon(self._step_nonblocking)
            return True
        return False

    def on_message(self, handler: Callable[[str], None]) -> None:
        """Register a message handler."""
        self._message_handlers.append(handler)

    def get_state(self) -> BusState:
        """Get current bus state."""
        if self._bus is None or self._bus == ffi.NULL:
            return BusState.STOPPED

        state = lib.stdio_bus_get_state(self._bus)
        state_map = {
            lib.STDIO_BUS_STATE_CREATED: BusState.CREATED,
            lib.STDIO_BUS_STATE_STARTING: BusState.STARTING,
            lib.STDIO_BUS_STATE_RUNNING: BusState.RUNNING,
            lib.STDIO_BUS_STATE_STOPPING: BusState.STOPPING,
            lib.STDIO_BUS_STATE_STOPPED: BusState.STOPPED,
        }
        return state_map.get(state, BusState.STOPPED)

    def get_stats(self) -> BusStats:
        """Get bus statistics."""
        if self._bus is None or self._bus == ffi.NULL:
            return self._stats

        stats = ffi.new("stdio_bus_stats_t*")
        lib.stdio_bus_get_stats(self._bus, stats)

        return BusStats(
            messages_in=stats.messages_in,
            messages_out=stats.messages_out,
            bytes_in=stats.bytes_in,
            bytes_out=stats.bytes_out,
            worker_restarts=stats.worker_restarts,
            routing_errors=stats.routing_errors,
            client_connects=stats.client_connects,
            client_disconnects=stats.client_disconnects,
        )

    def get_worker_count(self) -> int:
        """Get number of running workers."""
        if self._bus is None or self._bus == ffi.NULL:
            return 0
        return lib.stdio_bus_worker_count(self._bus)

    def get_client_count(self) -> int:
        """Get number of connected clients."""
        if self._bus is None or self._bus == ffi.NULL:
            return 0
        return lib.stdio_bus_client_count(self._bus)

    def get_listen_mode(self) -> ListenMode:
        """Return the configured external listener mode for this native bus."""
        return ListenMode(self._listen_mode)

    def _cleanup_strings(self) -> None:
        """Clean up C string references."""
        self._c_strings.clear()

    def destroy(self) -> None:
        """Release all resources."""
        # Remove fd from loop if still registered
        if self._loop is not None and self._poll_fd is not None:
            try:
                self._loop.remove_reader(self._poll_fd)
            except Exception:
                pass  # Loop may be closed already

        # Cancel timer if active
        if self._poll_timer is not None:
            self._poll_timer.cancel()
            self._poll_timer = None

        if self._bus is not None and self._bus != ffi.NULL:
            lib.stdio_bus_destroy(self._bus)
            self._bus = None

        self._poll_fd = None
        self._loop = None
        self._loop_thread_id = None
        self._stopping = False
        self._stepping = False
        self._message_handlers.clear()
        self._message_cb = None
        self._error_cb = None
        self._log_cb = None
        self._worker_cb = None
        self._cleanup_strings()
