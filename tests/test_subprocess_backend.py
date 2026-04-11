"""Tests for subprocess backend."""

import json
import os
import shutil
import sys
import pytest

from stdiobus.backends.subprocess import SubprocessBackend, StderrRingBuffer
from stdiobus.types import BusState, SubprocessOptions


class TestStderrRingBuffer:
    """Test the stderr ring buffer."""

    def test_basic_append_and_get(self):
        buf = StderrRingBuffer(maxlen=5)
        buf.append("line1")
        buf.append("line2")
        assert buf.get_lines() == ["line1", "line2"]

    def test_overflow(self):
        buf = StderrRingBuffer(maxlen=3)
        for i in range(5):
            buf.append(f"line{i}")
        assert buf.get_lines() == ["line2", "line3", "line4"]

    def test_tail(self):
        buf = StderrRingBuffer(maxlen=10)
        for i in range(10):
            buf.append(f"line{i}")
        tail = buf.tail(3)
        assert "line7" in tail
        assert "line8" in tail
        assert "line9" in tail

    def test_empty_tail(self):
        buf = StderrRingBuffer()
        assert buf.tail() == ""


class TestSubprocessBackendCreation:
    """Test SubprocessBackend creation."""

    def test_create_with_config_path(self):
        backend = SubprocessBackend(config_path="/tmp/test.json")
        assert backend.get_state() == BusState.CREATED
        backend.destroy()

    def test_create_with_config_json(self):
        config = json.dumps({"pools": [{"id": "w", "command": "echo", "instances": 1}]})
        backend = SubprocessBackend(config_json=config)
        assert backend.get_state() == BusState.CREATED
        backend.destroy()

    def test_stats_initial(self):
        backend = SubprocessBackend(config_path="/tmp/test.json")
        stats = backend.get_stats()
        assert stats.messages_in == 0
        assert stats.messages_out == 0
        backend.destroy()

    def test_send_when_not_running(self):
        backend = SubprocessBackend(config_path="/tmp/test.json")
        assert backend.send('{"test": true}') is False
        backend.destroy()

    def test_stderr_tail_empty(self):
        backend = SubprocessBackend(config_path="/tmp/test.json")
        assert backend.get_stderr_tail() == ""
        backend.destroy()


class TestSubprocessBackendOptions:
    """Test SubprocessOptions."""

    def test_default_options(self):
        opts = SubprocessOptions()
        assert opts.binary_path == "stdio_bus"
        assert opts.start_timeout_sec == 5.0
        assert opts.drain_timeout_sec == 30.0
        assert opts.stderr_buffer_lines == 200

    def test_custom_options(self):
        opts = SubprocessOptions(
            binary_path="/usr/local/bin/stdio_bus",
            start_timeout_sec=10.0,
            env={"DEBUG": "1"},
        )
        assert opts.binary_path == "/usr/local/bin/stdio_bus"
        assert opts.start_timeout_sec == 10.0
        assert opts.env == {"DEBUG": "1"}


class TestBackendModeSubprocess:
    """Test BackendMode.SUBPROCESS in client."""

    def test_subprocess_mode_exists(self):
        from stdiobus import BackendMode
        assert BackendMode.SUBPROCESS == "subprocess"

    def test_subprocess_backend_type(self):
        """Verify SubprocessBackend is importable and has correct interface."""
        from stdiobus.backends.subprocess import SubprocessBackend
        from stdiobus.backends.base import Backend
        assert issubclass(SubprocessBackend, Backend)


@pytest.mark.skipif(
    not shutil.which("stdio_bus"),
    reason="stdio_bus binary not in PATH"
)
class TestSubprocessBackendIntegration:
    """Integration tests requiring the stdio_bus binary."""

    @pytest.fixture
    def config_json(self):
        return json.dumps({
            "pools": [{
                "id": "echo",
                "command": "/usr/bin/env",
                "args": ["node", "-e",
                         "process.stdin.pipe(process.stdout)"],
                "instances": 1,
            }]
        })

    @pytest.mark.asyncio
    async def test_start_stop(self, config_json):
        binary = shutil.which("stdio_bus") or "../../build/stdio_bus"
        backend = SubprocessBackend(
            config_json=config_json,
            options=SubprocessOptions(binary_path=binary),
        )
        await backend.start()
        assert backend.get_state() == BusState.RUNNING
        await backend.stop(timeout_sec=5)
        assert backend.get_state() == BusState.STOPPED
        backend.destroy()
