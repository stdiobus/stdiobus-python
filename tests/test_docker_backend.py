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

"""Docker backend integration tests for stdiobus Python SDK.

These tests require Docker to be installed and running.
"""

import json
import os
import shutil
import tempfile
import pytest

from stdiobus import AsyncStdioBus, StdioBus, BusState
from stdiobus.types import DockerOptions


# Skip all tests if Docker is not available
pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="Docker not available"
)


@pytest.fixture
def config_with_worker():
    """Create a temporary config file with echo worker."""
    # Create worker script
    worker_code = '''
const readline = require('readline');
const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', (line) => {
    try {
        const req = JSON.parse(line);
        if (req.method === 'echo') {
            console.log(JSON.stringify({
                jsonrpc: '2.0',
                id: req.id,
                result: { echo: req.params }
            }));
        } else if (req.method === 'tools/list') {
            console.log(JSON.stringify({
                jsonrpc: '2.0',
                id: req.id,
                result: { tools: [{ name: 'echo' }] }
            }));
        }
    } catch (e) {
        console.log(JSON.stringify({
            jsonrpc: '2.0',
            id: null,
            error: { code: -32700, message: 'Parse error' }
        }));
    }
});
'''
    
    # Create temp directory
    tmpdir = tempfile.mkdtemp()
    worker_path = os.path.join(tmpdir, 'worker.js')
    config_path = os.path.join(tmpdir, 'config.json')
    
    # Write worker
    with open(worker_path, 'w') as f:
        f.write(worker_code)
    
    # Write config
    config = {
        "pools": [{
            "id": "echo",
            "command": "node",
            "args": ["/worker.js"],
            "instances": 1
        }]
    }
    with open(config_path, 'w') as f:
        json.dump(config, f)
    
    yield {
        "config_path": config_path,
        "worker_path": worker_path,
        "tmpdir": tmpdir,
    }
    
    # Cleanup
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestAsyncStdioBusDocker:
    """Test AsyncStdioBus with Docker backend."""
    
    @pytest.mark.asyncio
    async def test_create_instance(self, config_with_worker):
        """Test creating instance with docker backend."""
        bus = AsyncStdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        )
        
        assert bus.get_backend_type() == "docker"
        assert bus.get_state() == BusState.CREATED
        bus.destroy()
    
    @pytest.mark.asyncio
    async def test_start_stop(self, config_with_worker):
        """Test starting and stopping the bus."""
        bus = AsyncStdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                startup_timeout_sec=30.0,
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        )
        
        try:
            await bus.start()
            assert bus.get_state() == BusState.RUNNING
            assert bus.is_running()
            
            await bus.stop(timeout_sec=5)
            assert bus.get_state() == BusState.STOPPED
        finally:
            bus.destroy()
    
    @pytest.mark.asyncio
    async def test_request_response(self, config_with_worker):
        """Test request/response cycle."""
        async with AsyncStdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                startup_timeout_sec=30.0,
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        ) as bus:
            result = await bus.request("tools/list", {}, timeout_ms=10000)
            assert "tools" in result
            assert len(result["tools"]) > 0
    
    @pytest.mark.asyncio
    async def test_echo(self, config_with_worker):
        """Test echo method."""
        async with AsyncStdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                startup_timeout_sec=30.0,
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        ) as bus:
            result = await bus.request(
                "echo",
                {"message": "hello from python"},
                timeout_ms=10000,
            )
            assert result["echo"]["message"] == "hello from python"
    
    @pytest.mark.asyncio
    async def test_stats(self, config_with_worker):
        """Test statistics tracking."""
        async with AsyncStdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                startup_timeout_sec=30.0,
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        ) as bus:
            await bus.request("tools/list", {}, timeout_ms=10000)
            
            stats = bus.get_stats()
            assert stats.messages_in >= 1
            assert stats.messages_out >= 1
            assert stats.bytes_in > 0
            assert stats.bytes_out > 0


class TestStdioBusDocker:
    """Test synchronous StdioBus with Docker backend."""
    
    def test_sync_request(self, config_with_worker):
        """Test synchronous request."""
        with StdioBus(
            config_path=config_with_worker["config_path"],
            backend="docker",
            docker=DockerOptions(
                startup_timeout_sec=30.0,
                extra_args=["-v", f"{config_with_worker['worker_path']}:/worker.js:ro"]
            ),
        ) as bus:
            result = bus.request("tools/list", {}, timeout_ms=10000)
            assert "tools" in result
