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

"""Docker backend for stdiobus.

Runs stdio_bus in a Docker container and communicates via TCP.
Works on Windows, macOS, Linux - anywhere Docker is available.
"""

import asyncio
import json
import os
import shutil
import subprocess
import socket
from typing import Callable, Optional

from stdiobus.backends.base import Backend
from stdiobus.types import BusState, BusStats, DockerOptions
from stdiobus.errors import (
    TransportError,
    InvalidStateError,
    UnavailableError,
)


DEFAULT_CONTAINER_PORT = 8765


class DockerBackend(Backend):
    """Docker-based backend for stdio_bus."""
    
    def __init__(self, config_path: str, options: Optional[DockerOptions] = None):
        self._config_path = os.path.abspath(config_path)
        self._options = options or DockerOptions()
        self._state = BusState.CREATED
        self._container_id: Optional[str] = None
        self._host_port: Optional[int] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._message_handlers: list[Callable[[str], None]] = []
        self._read_task: Optional[asyncio.Task[None]] = None
        self._stats = BusStats()
        
        # Validate config exists
        if not os.path.exists(self._config_path):
            raise TransportError(f"Config file not found: {self._config_path}")
        
        # Check Docker availability
        self._check_docker()
    
    def _check_docker(self) -> None:
        """Verify Docker is available."""
        docker_path = shutil.which(self._options.engine_path)
        if docker_path is None:
            raise UnavailableError(
                "Docker is not available. Please install Docker Desktop or ensure "
                "docker CLI is in PATH. Download: https://www.docker.com/products/docker-desktop"
            )
        
        try:
            subprocess.run(
                [self._options.engine_path, "--version"],
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise UnavailableError(f"Docker check failed: {e.stderr.decode()}")
    
    def _pull_image(self) -> None:
        """Pull Docker image if needed."""
        if self._options.pull_policy == "never":
            return
        
        if self._options.pull_policy == "if-missing":
            result = subprocess.run(
                [self._options.engine_path, "image", "inspect", self._options.image],
                capture_output=True,
            )
            if result.returncode == 0:
                return  # Image exists
        
        print(f"[stdiobus:docker] Pulling image {self._options.image}...")
        try:
            subprocess.run(
                [self._options.engine_path, "pull", self._options.image],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise TransportError(f"Failed to pull Docker image: {self._options.image}")
    
    def _find_free_port(self) -> int:
        """Find an available port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]
    
    async def _start_container(self) -> None:
        """Start the Docker container."""
        self._host_port = self._find_free_port()
        container_name = f"{self._options.container_name_prefix}-{os.getpid()}"
        
        args = [
            self._options.engine_path,
            "run",
            "--rm",
            "-d",
            "--name", container_name,
            "-p", f"127.0.0.1:{self._host_port}:{DEFAULT_CONTAINER_PORT}",
            "-v", f"{self._config_path}:/app/config.json:ro",
        ]
        
        # Add environment variables
        for key, value in self._options.env.items():
            args.extend(["-e", f"{key}={value}"])
        
        # Add extra args
        args.extend(self._options.extra_args)
        
        # Add image and command
        args.append(self._options.image)
        args.extend(["--config", "/app/config.json"])
        args.extend(["--tcp", f"0.0.0.0:{DEFAULT_CONTAINER_PORT}"])
        
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            raise TransportError(f"Failed to start container: {stderr.decode()}")
        
        self._container_id = stdout.decode().strip()[:12]
    
    async def _wait_for_ready(self) -> None:
        """Wait for container to be ready."""
        timeout = self._options.startup_timeout_sec
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", self._host_port),
                    timeout=1.0,
                )
                return
            except (ConnectionRefusedError, asyncio.TimeoutError, OSError):
                await asyncio.sleep(0.1)
        
        raise TransportError(
            f"Container failed to become ready within {timeout}s"
        )
    
    async def _read_loop(self) -> None:
        """Read messages from the container."""
        if self._reader is None:
            return
        
        buffer = ""
        try:
            while self._state == BusState.RUNNING:
                try:
                    data = await asyncio.wait_for(
                        self._reader.read(8192),
                        timeout=0.1,
                    )
                except asyncio.TimeoutError:
                    continue
                
                if not data:
                    break
                
                self._stats.bytes_out += len(data)
                buffer += data.decode()
                
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.strip():
                        self._stats.messages_out += 1
                        for handler in self._message_handlers:
                            try:
                                handler(line)
                            except Exception as e:
                                print(f"[stdiobus:docker] Handler error: {e}")
        except Exception as e:
            if self._state == BusState.RUNNING:
                print(f"[stdiobus:docker] Read loop error: {e}")
    
    async def start(self) -> None:
        """Start the Docker backend."""
        if self._state != BusState.CREATED:
            raise InvalidStateError("Bus already started")
        
        self._state = BusState.STARTING
        
        try:
            self._pull_image()
            await self._start_container()
            await self._wait_for_ready()
            self._state = BusState.RUNNING
            self._stats.client_connects += 1
            
            # Start read loop
            self._read_task = asyncio.create_task(self._read_loop())
            
        except Exception as e:
            self._state = BusState.STOPPED
            raise
    
    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the Docker backend."""
        if self._state != BusState.RUNNING:
            return
        
        self._state = BusState.STOPPING
        
        # Cancel read task
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
        
        # Close connection
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
        
        # Stop container
        if self._container_id:
            try:
                proc = await asyncio.create_subprocess_exec(
                    self._options.engine_path,
                    "stop",
                    "-t", str(int(timeout_sec)),
                    self._container_id,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
            except Exception:
                pass
            self._container_id = None
        
        self._state = BusState.STOPPED
        self._stats.client_disconnects += 1
    
    def send(self, message: str) -> bool:
        """Send a message to the container."""
        if self._state != BusState.RUNNING or self._writer is None:
            return False
        
        try:
            data = message if message.endswith("\n") else message + "\n"
            self._writer.write(data.encode())
            self._stats.messages_in += 1
            self._stats.bytes_in += len(data)
            return True
        except Exception:
            return False
    
    def on_message(self, handler: Callable[[str], None]) -> None:
        """Register a message handler."""
        self._message_handlers.append(handler)
    
    def get_state(self) -> BusState:
        """Get current state."""
        return self._state
    
    def get_stats(self) -> BusStats:
        """Get statistics."""
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

    def get_client_count(self) -> int:
        """Return whether this SDK is connected to the container (0 or 1).

        The Docker backend talks to the bus daemon over a single TCP socket,
        so this reports *this SDK's* connection to the container — not the
        number of clients the daemon itself serves (which is not introspectable
        from here). Mirrors the Node SDK's ``socket ? 1 : 0`` semantics.

        Worker count remains -1 (inherited): the daemon runs inside the
        container and exposes no worker introspection over TCP.
        """
        return 1 if (self._state == BusState.RUNNING and self._writer is not None) else 0
    
    def destroy(self) -> None:
        """Release all resources."""
        if self._writer:
            self._writer.close()
            self._writer = None
            self._reader = None
        
        if self._container_id:
            try:
                subprocess.run(
                    [self._options.engine_path, "kill", self._container_id],
                    capture_output=True,
                )
            except Exception:
                pass
            self._container_id = None
        
        self._state = BusState.STOPPED
