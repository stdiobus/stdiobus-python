"""Native backend using cffi bindings to libstdio_bus."""

import asyncio
import sys
import threading
from typing import Callable, List, Optional

from stdiobus.backends.base import Backend
from stdiobus.types import BusState, BusStats
from stdiobus.errors import (
    InvalidStateError,
    TransportError,
)

# Try to import cffi bindings
try:
    from stdiobus._native._ffi import ffi, lib
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
    
    Build the native extension:
        cd sdk/python
        python -m stdiobus._native.build_ffi
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
                "Build with: cd sdk/python && python -m stdiobus._native.build_ffi"
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
        self._poll_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._stats = BusStats()
        
        # Keep references to callbacks to prevent GC
        self._message_cb = None
        self._error_cb = None
        self._log_cb = None
        self._worker_cb = None
        
        # Keep references to C strings
        self._c_strings = []
    
    def _keep_string(self, s: str) -> "ffi.CData":
        """Create a C string and keep reference to prevent GC."""
        c_str = ffi.new("char[]", s.encode('utf-8'))
        self._c_strings.append(c_str)
        return c_str
    
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
        """Start the native backend."""
        if self._bus is not None:
            raise InvalidStateError("Backend already started")
        
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
        
        # Start polling loop
        self._stop_event.clear()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
    
    def _poll_loop(self) -> None:
        """Background thread for polling messages."""
        while not self._stop_event.is_set():
            if self._bus is None or self._bus == ffi.NULL:
                break
            
            try:
                # Non-blocking step with small timeout
                lib.stdio_bus_step(self._bus, self._poll_interval_ms)
            except Exception as e:
                print(f"[stdiobus] Poll error: {e}", file=sys.stderr)
                break
    
    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the native backend."""
        if self._bus is None or self._bus == ffi.NULL:
            return
        
        # Signal poll thread to stop
        self._stop_event.set()
        
        # Stop the bus
        result = lib.stdio_bus_stop(self._bus, int(timeout_sec))
        if result != lib.STDIO_BUS_OK:
            print(f"[stdiobus] Warning: stop returned {result}", file=sys.stderr)
        
        # Continue stepping until fully stopped
        while lib.stdio_bus_get_state(self._bus) != lib.STDIO_BUS_STATE_STOPPED:
            lib.stdio_bus_step(self._bus, 100)
        
        # Wait for poll thread
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)
    
    def send(self, message: str) -> bool:
        """Send a message to workers."""
        if self._bus is None or self._bus == ffi.NULL:
            return False
        
        msg_bytes = message.encode('utf-8')
        result = lib.stdio_bus_ingest(self._bus, msg_bytes, len(msg_bytes))
        if result == lib.STDIO_BUS_OK:
            self._stats.messages_in += 1
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
    
    def _cleanup_strings(self) -> None:
        """Clean up C string references."""
        self._c_strings.clear()
    
    def destroy(self) -> None:
        """Release all resources."""
        if self._bus is not None and self._bus != ffi.NULL:
            lib.stdio_bus_destroy(self._bus)
            self._bus = None
        
        self._message_handlers.clear()
        self._message_cb = None
        self._error_cb = None
        self._log_cb = None
        self._worker_cb = None
        self._cleanup_strings()
