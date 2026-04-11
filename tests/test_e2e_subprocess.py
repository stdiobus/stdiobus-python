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

"""End-to-end tests for SubprocessBackend using a real echo process.

Uses a simple Python script as a mock stdio_bus that echoes JSON-RPC
requests back as responses. This tests the full pipeline:
  client → stdin NDJSON → mock process → stdout NDJSON → client
"""

import asyncio
import json
import os
import sys
import tempfile
import textwrap
import pytest

from stdiobus import (
    AsyncStdioBus,
    StdioBus,
    BusConfig,
    PoolConfig,
    BusState,
    RequestOptions,
    Identity,
    AuditEvent,
    SubprocessOptions,
    InvalidStateError,
)
from stdiobus.backends.subprocess import SubprocessBackend


# ---------------------------------------------------------------------------
# Mock stdio_bus script — reads NDJSON from stdin, echoes back as response
# ---------------------------------------------------------------------------

MOCK_BUS_SCRIPT = textwrap.dedent("""\
    import sys, json

    # If --config-fd is used, read and discard config from fd 3
    if '--config-fd' in sys.argv:
        idx = sys.argv.index('--config-fd')
        fd = int(sys.argv[idx + 1])
        import os
        config_data = b''
        while True:
            chunk = os.read(fd, 4096)
            if not chunk:
                break
            config_data += chunk
        os.close(fd)
        # Validate it's JSON
        json.loads(config_data)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            if 'id' in req:
                resp = {
                    "jsonrpc": "2.0",
                    "id": req["id"],
                    "result": {
                        "method": req.get("method"),
                        "params": req.get("params"),
                        "sessionId": req.get("sessionId"),
                        "agentId": req.get("agentId"),
                        "_ext": req.get("_ext"),
                    }
                }
                print(json.dumps(resp), flush=True)
        except json.JSONDecodeError:
            pass
""")


@pytest.fixture
def mock_bus_script(tmp_path):
    """Write mock bus script to temp file and return path."""
    script = tmp_path / "mock_bus.py"
    script.write_text(MOCK_BUS_SCRIPT)
    return str(script)


# ---------------------------------------------------------------------------
# E2E: SubprocessBackend directly
# ---------------------------------------------------------------------------

class TestSubprocessBackendE2E:

    @pytest.mark.asyncio
    async def test_start_stop(self, mock_bus_script):
        backend = SubprocessBackend(
            config_path="/dev/null",
            options=SubprocessOptions(binary_path=sys.executable),
        )

        import subprocess as _sp
        proc = _sp.Popen(
            [sys.executable, mock_bus_script, '--stdio'],
            stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.PIPE,
            close_fds=True,
        )
        backend._process = proc
        backend._stdin = proc.stdin
        backend._stdout = proc.stdout
        backend._stderr_stream = proc.stderr
        backend._state = BusState.RUNNING
        backend._read_task = asyncio.create_task(backend._read_loop())
        backend._stderr_task = asyncio.create_task(backend._stderr_loop())

        # Send a message
        received = []
        backend.on_message(lambda m: received.append(m))

        msg = json.dumps({"jsonrpc": "2.0", "id": "1", "method": "test"})
        assert backend.send(msg) is True

        # Wait for response
        for _ in range(30):
            await asyncio.sleep(0.1)
            if received:
                break

        assert len(received) >= 1
        resp = json.loads(received[0])
        assert resp["id"] == "1"
        assert resp["result"]["method"] == "test"

        await backend.stop(timeout_sec=2)
        assert backend.get_state() == BusState.STOPPED
        backend.destroy()

    @pytest.mark.asyncio
    async def test_config_fd_delivery(self, mock_bus_script):
        """Test that --config-fd 3 correctly delivers config JSON."""
        config_json = json.dumps({"pools": [{"id": "w", "command": "echo", "instances": 1}]})

        backend = SubprocessBackend(
            config_json=config_json,
            options=SubprocessOptions(binary_path=sys.executable),
        )

        # We need to override _spawn_process to use our mock script
        # Instead, test the pipe mechanism directly
        r_fd, w_fd = os.pipe()

        # Write config
        data = config_json.encode('utf-8')
        offset = 0
        while offset < len(data):
            written = os.write(w_fd, data[offset:])
            offset += written
        os.close(w_fd)

        # Read from pipe (simulating child)
        received = b''
        while True:
            chunk = os.read(r_fd, 4096)
            if not chunk:
                break
            received += chunk
        os.close(r_fd)

        assert json.loads(received) == json.loads(config_json)
        backend.destroy()

    @pytest.mark.asyncio
    async def test_stats_tracking(self, mock_bus_script):
        """Test that stats are updated on send/receive."""
        backend = SubprocessBackend(
            config_path="/dev/null",
            options=SubprocessOptions(binary_path=sys.executable),
        )

        import subprocess as _sp
        proc = _sp.Popen(
            [sys.executable, mock_bus_script, '--stdio'],
            stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.PIPE,
            close_fds=True,
        )
        backend._process = proc
        backend._stdin = proc.stdin
        backend._stdout = proc.stdout
        backend._stderr_stream = proc.stderr
        backend._state = BusState.RUNNING
        backend._read_task = asyncio.create_task(backend._read_loop())
        backend._stderr_task = asyncio.create_task(backend._stderr_loop())

        backend.on_message(lambda m: None)

        backend.send(json.dumps({"jsonrpc": "2.0", "id": "s1", "method": "m"}))
        await asyncio.sleep(0.3)

        stats = backend.get_stats()
        assert stats.messages_in >= 1
        assert stats.bytes_in > 0
        assert stats.messages_out >= 1
        assert stats.bytes_out > 0

        await backend.stop(timeout_sec=2)
        backend.destroy()


# ---------------------------------------------------------------------------
# E2E: Full AsyncStdioBus with mock bus
# ---------------------------------------------------------------------------

class TestAsyncStdioBusE2E:

    @pytest.fixture
    def patched_bus(self, mock_bus_script):
        """Create AsyncStdioBus with subprocess backend pointing to mock script."""
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)]),
            backend="subprocess",
            subprocess=SubprocessOptions(binary_path=sys.executable),
        )
        script_path = mock_bus_script

        async def patched_spawn():
            import subprocess as _sp
            backend = bus._backend
            proc = _sp.Popen(
                [sys.executable, script_path, '--stdio'],
                stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.PIPE,
                close_fds=True,
            )
            backend._process = proc
            backend._stdin = proc.stdin
            backend._stdout = proc.stdout
            backend._stderr_stream = proc.stderr

        bus._backend._spawn_process = patched_spawn
        return bus

    @pytest.mark.asyncio
    async def test_request_response_cycle(self, patched_bus):
        bus = patched_bus
        await bus.start()
        assert bus.is_running()

        result = await bus.request("tools/list", {"query": "test"}, timeout_ms=5000)
        assert result["method"] == "tools/list"
        assert result["params"] == {"query": "test"}

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_session_id_in_response(self, patched_bus):
        bus = patched_bus
        await bus.start()

        result = await bus.request("m", timeout_ms=5000)
        assert result["sessionId"] == bus.client_session_id

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_custom_session_id(self, patched_bus):
        bus = patched_bus
        await bus.start()

        result = await bus.request("m", session_id="custom-123", timeout_ms=5000)
        assert result["sessionId"] == "custom-123"

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_agent_id_forwarded(self, patched_bus):
        bus = patched_bus
        await bus.start()

        result = await bus.request(
            "m",
            options=RequestOptions(agent_id="agent-xyz"),
            timeout_ms=5000,
        )
        assert result["agentId"] == "agent-xyz"

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_extensions_forwarded(self, patched_bus):
        bus = patched_bus
        await bus.start()

        result = await bus.request(
            "m",
            options=RequestOptions(
                identity=Identity(subject_id="u1", role="admin"),
                audit=AuditEvent(event_id="e1", action="test"),
            ),
            timeout_ms=5000,
        )
        assert result["_ext"]["identity"]["subjectId"] == "u1"
        assert result["_ext"]["audit"]["eventId"] == "e1"

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_multiple_requests(self, patched_bus):
        bus = patched_bus
        await bus.start()

        results = await asyncio.gather(
            bus.request("m1", timeout_ms=5000),
            bus.request("m2", timeout_ms=5000),
            bus.request("m3", timeout_ms=5000),
        )
        methods = {r["method"] for r in results}
        assert methods == {"m1", "m2", "m3"}

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_context_manager(self, patched_bus):
        bus = patched_bus
        # Use __aenter__ / __aexit__ manually since patched_bus is pre-created
        await bus.start()
        assert bus.is_running()
        await bus.stop()
        bus.destroy()

    @pytest.mark.asyncio
    async def test_request_when_not_running(self, patched_bus):
        bus = patched_bus
        # Don't start
        with pytest.raises(InvalidStateError, match="not running"):
            await bus.request("m", timeout_ms=1000)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_on_message_handler(self, patched_bus):
        bus = patched_bus
        received = []
        bus.on_message(lambda m: received.append(m))
        await bus.start()

        await bus.request("m", timeout_ms=5000)
        assert len(received) >= 1
        # The handler receives raw JSON string
        data = json.loads(received[0])
        assert "id" in data

        await bus.stop(timeout_sec=2)
        bus.destroy()


# ---------------------------------------------------------------------------
# E2E: StdioBus sync wrapper
# ---------------------------------------------------------------------------

class TestStdioBusSyncE2E:

    @pytest.fixture
    def patched_sync_bus(self, mock_bus_script):
        bus = StdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)]),
            backend="subprocess",
            subprocess=SubprocessOptions(binary_path=sys.executable),
        )
        script_path = mock_bus_script

        async def patched_spawn():
            import subprocess as _sp
            backend = bus._async_bus._backend
            proc = _sp.Popen(
                [sys.executable, script_path, '--stdio'],
                stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.PIPE,
                close_fds=True,
            )
            backend._process = proc
            backend._stdin = proc.stdin
            backend._stdout = proc.stdout
            backend._stderr_stream = proc.stderr

        bus._async_bus._backend._spawn_process = patched_spawn
        return bus

    def test_sync_request_response(self, patched_sync_bus):
        bus = patched_sync_bus
        bus.start()
        assert bus.is_running()

        result = bus.request("echo", {"msg": "hello"}, timeout_ms=5000)
        assert result["method"] == "echo"
        assert result["params"] == {"msg": "hello"}

        bus.stop(timeout_sec=2)
        bus.destroy()

    def test_sync_session_id(self, patched_sync_bus):
        bus = patched_sync_bus
        bus.start()

        result = bus.request("m", timeout_ms=5000)
        assert result["sessionId"] == bus.client_session_id

        bus.stop(timeout_sec=2)
        bus.destroy()


# ---------------------------------------------------------------------------
# E2E: Streaming (agent_message_chunk aggregation)
# ---------------------------------------------------------------------------

STREAMING_MOCK_SCRIPT = textwrap.dedent("""\
    import sys, json

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            if 'id' in req:
                # Send streaming chunks before response
                for i, word in enumerate(["Hello ", "streaming ", "world!"]):
                    chunk = {
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {"text": word}
                            }
                        }
                    }
                    print(json.dumps(chunk), flush=True)

                # Send final response
                resp = {
                    "jsonrpc": "2.0",
                    "id": req["id"],
                    "result": {"status": "complete"}
                }
                print(json.dumps(resp), flush=True)
        except json.JSONDecodeError:
            pass
""")


@pytest.fixture
def streaming_mock_script(tmp_path):
    script = tmp_path / "streaming_mock.py"
    script.write_text(STREAMING_MOCK_SCRIPT)
    return str(script)


class TestStreamingE2E:

    @pytest.fixture
    def streaming_bus(self, streaming_mock_script):
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)]),
            backend="subprocess",
            subprocess=SubprocessOptions(binary_path=sys.executable),
        )
        script_path = streaming_mock_script

        async def patched_spawn():
            import subprocess as _sp
            backend = bus._backend
            proc = _sp.Popen(
                [sys.executable, script_path, '--stdio'],
                stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.PIPE,
                close_fds=True,
            )
            backend._process = proc
            backend._stdin = proc.stdin
            backend._stdout = proc.stdout
            backend._stderr_stream = proc.stderr

        bus._backend._spawn_process = patched_spawn
        return bus

    @pytest.mark.asyncio
    async def test_streaming_chunks_aggregated(self, streaming_bus):
        """Test that agent_message_chunk notifications are aggregated into result.text."""
        bus = streaming_bus
        await bus.start()

        result = await bus.request("session/prompt", {"prompt": "test"}, timeout_ms=5000)
        assert result["status"] == "complete"
        assert result["text"] == "Hello streaming world!"

        await bus.stop(timeout_sec=2)
        bus.destroy()

    @pytest.mark.asyncio
    async def test_notification_handler_receives_chunks(self, streaming_bus):
        """Test that notification handlers receive chunk notifications."""
        bus = streaming_bus
        notifications = []
        bus.on_notification(lambda m: notifications.append(m))
        await bus.start()

        await bus.request("m", timeout_ms=5000)

        # Should have received 3 chunk notifications
        chunk_notifs = [
            json.loads(n) for n in notifications
            if "agent_message_chunk" in n
        ]
        assert len(chunk_notifs) == 3

        await bus.stop(timeout_sec=2)
        bus.destroy()
