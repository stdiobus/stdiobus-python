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

"""Subprocess backend for stdiobus.

Spawns the stdio_bus binary and communicates via stdin/stdout NDJSON pipes.
Config delivery: --config <path> (legacy) or --config-fd 3 (programmatic, no temp files).

This is the primary backend for local usage on Unix systems.
"""

import asyncio
import atexit
import collections
import json
import logging
import os
import sys
import threading
from typing import Callable, Optional

from stdiobus.backends.base import Backend
from stdiobus.types import BusState, BusStats, SubprocessOptions
from stdiobus.errors import (
    InvalidStateError,
    TransportError,
)

logger = logging.getLogger("stdiobus.subprocess")

# Global registry for orphan cleanup
_active_processes: list["SubprocessBackend"] = []


def _cleanup_orphans() -> None:
    """atexit handler: kill any stdio_bus processes still alive."""
    for backend in _active_processes:
        try:
            backend._force_kill()
        except Exception:
            pass


atexit.register(_cleanup_orphans)


class StderrRingBuffer:
    """Thread-safe ring buffer for stderr lines."""

    def __init__(self, maxlen: int = 200):
        self._buf: collections.deque[str] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            self._buf.append(line)

    def get_lines(self) -> list[str]:
        with self._lock:
            return list(self._buf)

    def tail(self, n: int = 20) -> str:
        lines = self.get_lines()
        return "\n".join(lines[-n:])


class SubprocessBackend(Backend):
    """Subprocess-based backend — spawns stdio_bus binary.

    Config delivery:
    - config_path  → ``--config <path>``
    - config_json  → ``--config-fd 3`` (pipe, write JSON, close EOF)

    Transport:
    - stdin  → outbound NDJSON (client → bus)
    - stdout → inbound NDJSON  (bus → client)
    - stderr → logs / diagnostics (captured to ring buffer)
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config_json: Optional[str] = None,
        options: Optional[SubprocessOptions] = None,
    ):
        self._config_path = config_path
        self._config_json = config_json
        self._options = options or SubprocessOptions()
        self._state = BusState.CREATED
        self._process: Optional[asyncio.subprocess.Process] = None
        self._message_handlers: list[Callable[[str], None]] = []
        self._read_task: Optional[asyncio.Task[None]] = None
        self._stderr_task: Optional[asyncio.Task[None]] = None
        self._stderr_buf = StderrRingBuffer(self._options.stderr_buffer_lines)
        self._stats = BusStats()
        self._stdin = None
        self._stdout = None
        self._stderr_stream = None
        self._temp_config_path: Optional[str] = None
        self._on_close_callback: Optional[Callable[[], None]] = None

    async def start(self) -> None:
        """Start the stdio_bus subprocess."""
        if self._state != BusState.CREATED:
            raise InvalidStateError("Backend already started")

        self._state = BusState.STARTING
        _active_processes.append(self)

        try:
            await self._spawn_process()
            # Start stderr capture immediately (before early-exit check)
            self._stderr_task = asyncio.create_task(self._stderr_loop())
            # Check process didn't die within start_timeout
            grace = min(self._options.start_timeout_sec, 0.5)
            elapsed = 0.0
            while elapsed < self._options.start_timeout_sec:
                await asyncio.sleep(grace)
                elapsed += grace
                if self._process.returncode is not None:
                    stderr_tail = self._stderr_buf.tail()
                    raise TransportError(
                        f"stdio_bus exited with code "
                        f"{self._process.returncode}\n{stderr_tail}"
                    )
                # Process alive — good enough for startup
                break
            self._state = BusState.RUNNING
            self._read_task = asyncio.create_task(self._read_loop())
        except Exception:
            self._state = BusState.STOPPED
            self._remove_from_registry()
            raise

    async def _spawn_process(self) -> None:
        """Spawn the stdio_bus binary with appropriate config delivery.

        Config delivery follows the Node SDK pattern:
        - config_path  → ``--config <path>``
        - config_json  → ``--config-fd <N>`` where N is the read end of a pipe.
          The C daemon accepts any fd number via ``--config-fd``.
          Python uses ``pass_fds`` to keep the pipe fd open in the child.
          (Node.js uses ``stdio: ['pipe','pipe','inherit','pipe']`` which
          guarantees fd 3, but Python subprocess has no equivalent API.)

        On Windows, config_json falls back to a temp file + ``--config``.
        """
        import platform
        binary = self._options.binary_path
        args: list[str] = []

        config_write_fd: Optional[int] = None
        config_read_fd: Optional[int] = None

        is_windows = platform.system().lower() == "windows"

        if self._config_json is not None:
            if is_windows:
                # Windows: no pass_fds support, fall back to temp file
                import tempfile
                fd, tmp = tempfile.mkstemp(prefix="stdiobus-", suffix=".json")
                try:
                    os.write(fd, self._config_json.encode('utf-8'))
                finally:
                    os.close(fd)
                os.chmod(tmp, 0o600)
                self._temp_config_path = tmp
                args = ['--config', tmp, '--stdio']
            else:
                # Unix: pipe + pass_fds (canonical Python approach)
                config_read_fd, config_write_fd = os.pipe()
                args = ['--config-fd', str(config_read_fd), '--stdio']
        elif self._config_path is not None:
            args = ['--config', self._config_path, '--stdio']
        else:
            raise TransportError("No config source provided")

        env = os.environ.copy()
        env.update(self._options.env)

        import subprocess as _sp

        pass_fds: tuple[int, ...] = ()
        if config_read_fd is not None:
            pass_fds = (config_read_fd,)

        try:
            proc = _sp.Popen(
                [binary] + args,
                stdin=_sp.PIPE,
                stdout=_sp.PIPE,
                stderr=_sp.PIPE,
                env=env,
                close_fds=True,
                pass_fds=pass_fds,
            )
        finally:
            # Parent closes read end — child has its own copy via fork
            if config_read_fd is not None:
                try:
                    os.close(config_read_fd)
                except OSError:
                    pass

        # Write config JSON to pipe and close (EOF signals end of config)
        if config_write_fd is not None:
            try:
                data = self._config_json.encode('utf-8')
                offset = 0
                while offset < len(data):
                    written = os.write(config_write_fd, data[offset:])
                    offset += written
            except BrokenPipeError:
                pass
            finally:
                try:
                    os.close(config_write_fd)
                except OSError:
                    pass

        self._process = proc  # type: ignore[assignment]
        self._stdin = proc.stdin
        self._stdout = proc.stdout
        self._stderr_stream = proc.stderr

    async def _read_loop(self) -> None:
        """Read NDJSON lines from stdout."""
        if self._stdout is None:
            return

        loop = asyncio.get_event_loop()
        try:
            while self._state == BusState.RUNNING:
                line_bytes = await loop.run_in_executor(
                    None, self._stdout.readline
                )
                if not line_bytes:
                    break  # EOF — process exited

                line = line_bytes.decode('utf-8', errors='replace').rstrip('\n\r')
                if not line:
                    continue

                # Strict NDJSON: every stdout line must be valid JSON
                try:
                    json.loads(line)  # validate
                except json.JSONDecodeError:
                    logger.warning("Non-JSON on stdout (protocol error): %s", line[:200])
                    continue

                self._stats.messages_out += 1
                self._stats.bytes_out += len(line_bytes)

                for handler in self._message_handlers:
                    try:
                        handler(line)
                    except Exception as e:
                        logger.error("Handler error: %s", e)
        except Exception as e:
            if self._state == BusState.RUNNING:
                logger.error("Read loop error: %s", e)
        finally:
            # Process exited or read error — fail-fast all pending requests
            if self._state == BusState.RUNNING:
                self._state = BusState.STOPPED
                self._on_backend_closed()

    async def _stderr_loop(self) -> None:
        """Capture stderr lines into ring buffer and log them."""
        if self._stderr_stream is None:
            return

        loop = asyncio.get_event_loop()
        try:
            while self._state in (BusState.STARTING, BusState.RUNNING):
                line_bytes = await loop.run_in_executor(
                    None, self._stderr_stream.readline
                )
                if not line_bytes:
                    break
                line = line_bytes.decode('utf-8', errors='replace').rstrip('\n\r')
                if line:
                    self._stderr_buf.append(line)
                    logger.debug("[stdio_bus] %s", line)
        except Exception:
            pass

    async def stop(self, timeout_sec: Optional[float] = None) -> None:
        """Stop the subprocess gracefully.

        Sequence: close stdin → wait drain_timeout → SIGTERM → wait 5s → SIGKILL.

        Args:
            timeout_sec: Override drain timeout. Defaults to options.drain_timeout_sec.
        """
        if self._state not in (BusState.RUNNING, BusState.STARTING):
            return

        self._state = BusState.STOPPING
        drain_timeout = timeout_sec if timeout_sec is not None else self._options.drain_timeout_sec

        proc = self._process
        if proc is None:
            self._state = BusState.STOPPED
            return

        # 1. Close stdin (EOF signal)
        if self._stdin and not self._stdin.closed:
            try:
                self._stdin.close()
            except Exception:
                pass

        # 2. Wait for graceful exit
        try:
            exit_code = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, proc.wait  # type: ignore[union-attr]
                ),
                timeout=drain_timeout,
            )
            logger.debug("stdio_bus exited with code %s", exit_code)
        except asyncio.TimeoutError:
            # 3. SIGTERM
            logger.warning("stdio_bus did not exit in %.1fs, sending SIGTERM", drain_timeout)
            try:
                proc.terminate()  # type: ignore[union-attr]
            except OSError:
                pass

            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, proc.wait  # type: ignore[union-attr]
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                # 4. SIGKILL
                logger.warning("stdio_bus did not respond to SIGTERM, sending SIGKILL")
                try:
                    proc.kill()  # type: ignore[union-attr]
                except OSError:
                    pass

        # Cancel read tasks
        for task in (self._read_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._state = BusState.STOPPED
        self._remove_from_registry()

    def send(self, message: str) -> bool:
        """Send NDJSON message to stdin."""
        if self._state != BusState.RUNNING or self._stdin is None:
            return False

        try:
            data = message if message.endswith("\n") else message + "\n"
            self._stdin.write(data.encode('utf-8'))
            self._stdin.flush()
            self._stats.messages_in += 1
            self._stats.bytes_in += len(data)
            return True
        except (BrokenPipeError, OSError) as e:
            logger.error("Send failed: %s", e)
            return False

    def on_message(self, handler: Callable[[str], None]) -> None:
        """Register a message handler."""
        self._message_handlers.append(handler)

    def get_state(self) -> BusState:
        return self._state

    def get_stats(self) -> BusStats:
        return BusStats(
            messages_in=self._stats.messages_in,
            messages_out=self._stats.messages_out,
            bytes_in=self._stats.bytes_in,
            bytes_out=self._stats.bytes_out,
            worker_restarts=self._stats.worker_restarts,
            routing_errors=self._stats.routing_errors,
            client_connects=self._stats.client_connects,
            client_disconnects=self._stats.client_disconnects,
        )

    def get_stderr_tail(self, n: int = 20) -> str:
        """Get last N lines from stderr buffer."""
        return self._stderr_buf.tail(n)

    def set_on_close(self, callback: Callable[[], None]) -> None:
        """Set callback for when backend closes unexpectedly."""
        self._on_close_callback = callback

    def _on_backend_closed(self) -> None:
        """Called when process exits unexpectedly during RUNNING state."""
        logger.warning("stdio_bus process exited unexpectedly")
        if self._on_close_callback:
            try:
                self._on_close_callback()
            except Exception as e:
                logger.error("on_close callback error: %s", e)

    def _force_kill(self) -> None:
        """Force kill the process (for atexit cleanup)."""
        proc = self._process
        if proc is not None:
            try:
                proc.kill()  # type: ignore[union-attr]
            except OSError:
                pass

    def _remove_from_registry(self) -> None:
        """Remove from global orphan registry."""
        try:
            _active_processes.remove(self)
        except ValueError:
            pass

    def destroy(self) -> None:
        """Release all resources."""
        self._force_kill()
        self._remove_from_registry()

        for stream in (self._stdin, self._stdout, self._stderr_stream):
            if stream and not getattr(stream, 'closed', True):
                try:
                    stream.close()
                except Exception:
                    pass

        # Clean up temp config file (Windows fallback)
        if self._temp_config_path:
            try:
                os.unlink(self._temp_config_path)
            except OSError:
                pass
            self._temp_config_path = None

        self._process = None
        self._message_handlers.clear()
        self._state = BusState.STOPPED
