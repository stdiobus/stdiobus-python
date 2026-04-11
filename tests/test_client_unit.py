"""Unit tests for client.py — message handling, request building, session routing.

These tests use a mock backend to verify client logic without spawning processes.
"""

import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from stdiobus import (
    AsyncStdioBus,
    StdioBus,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    BusState,
    BackendMode,
    HelloParams,
    HelloResult,
    Identity,
    AuditEvent,
    RequestOptions,
    InvalidArgumentError,
    InvalidStateError,
    TransportError,
)
from stdiobus.client import generate_client_session_id
from stdiobus.types import ExtensionInfo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg():
    return BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])


def _make_bus(**kwargs):
    return AsyncStdioBus(config=_make_cfg(), **kwargs)


class FakeBackend:
    """Minimal fake backend for unit testing client logic."""

    def __init__(self):
        self._state = BusState.CREATED
        self._handlers = []
        self._sent: list[str] = []

    async def start(self):
        self._state = BusState.RUNNING

    async def stop(self, timeout_sec=30.0):
        self._state = BusState.STOPPED

    def send(self, message: str) -> bool:
        if self._state != BusState.RUNNING:
            return False
        self._sent.append(message)
        return True

    def on_message(self, handler):
        self._handlers.append(handler)

    def get_state(self):
        return self._state

    def get_stats(self):
        from stdiobus.types import BusStats
        return BusStats()

    def is_running(self):
        return self._state == BusState.RUNNING

    def destroy(self):
        self._state = BusState.STOPPED

    def inject_message(self, msg: str):
        """Simulate incoming message from bus."""
        for h in self._handlers:
            h(msg)


# ---------------------------------------------------------------------------
# generate_client_session_id
# ---------------------------------------------------------------------------

class TestGenerateClientSessionId:

    def test_format(self):
        sid = generate_client_session_id()
        assert sid.startswith("client-")
        parts = sid.split("-")
        assert len(parts) == 3
        assert parts[1].isdigit()

    def test_uniqueness(self):
        ids = {generate_client_session_id() for _ in range(100)}
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# AsyncStdioBus constructor
# ---------------------------------------------------------------------------

class TestAsyncStdioBusConstructor:

    def test_backend_string_conversion(self):
        bus = _make_bus(backend="subprocess")
        assert bus._backend_mode == BackendMode.SUBPROCESS
        bus.destroy()

    def test_config_path_only(self):
        import tempfile, os
        fd, path = tempfile.mkstemp(suffix='.json')
        os.write(fd, b'{"pools":[{"id":"w","command":"echo","instances":1}]}')
        os.close(fd)
        try:
            bus = AsyncStdioBus(config_path=path)
            bus.destroy()
        finally:
            os.unlink(path)

    def test_subprocess_options_passed(self):
        from stdiobus import SubprocessOptions
        bus = _make_bus(subprocess=SubprocessOptions(binary_path="/custom/path"))
        bus.destroy()


# ---------------------------------------------------------------------------
# Message handling (_handle_message)
# ---------------------------------------------------------------------------

class TestHandleMessage:

    def test_response_resolves_future(self):
        bus = _make_bus()
        bus._backend = FakeBackend()
        bus._backend.on_message(bus._handle_message)

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        bus._pending_requests["req-1"] = _PendingRequest(future)

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-1", "result": {"ok": True}
        }))

        assert future.done()
        assert future.result() == {"ok": True}
        bus.destroy()
        loop.close()

    def test_error_response_sets_exception(self):
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        bus._pending_requests["req-2"] = _PendingRequest(future)

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-2",
            "error": {"code": 3, "message": "timeout"}
        }))

        assert future.done()
        with pytest.raises(Exception, match="timeout"):
            future.result()
        bus.destroy()
        loop.close()

    def test_invalid_json_ignored(self):
        bus = _make_bus()
        bus._backend = FakeBackend()
        # Should not raise
        bus._handle_message("not json at all")
        bus.destroy()

    def test_unknown_id_ignored(self):
        bus = _make_bus()
        bus._backend = FakeBackend()
        # No pending request for this id — should not raise
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "unknown-id", "result": {}
        }))
        bus.destroy()

    def test_user_handler_called(self):
        bus = _make_bus()
        bus._backend = FakeBackend()
        received = []
        bus.on_message(lambda msg: received.append(msg))
        bus._handle_message('{"test": true}')
        assert len(received) == 1
        assert received[0] == '{"test": true}'
        bus.destroy()

    def test_user_handler_exception_caught(self, capsys):
        bus = _make_bus()
        bus._backend = FakeBackend()
        bus.on_message(lambda msg: 1 / 0)
        bus._handle_message('{"test": true}')  # should not raise
        bus.destroy()


# ---------------------------------------------------------------------------
# _on_backend_closed
# ---------------------------------------------------------------------------

class TestOnBackendClosed:

    def test_fails_all_pending_requests(self):
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        f1 = loop.create_future()
        f2 = loop.create_future()
        from stdiobus.client import _PendingRequest
        bus._pending_requests["a"] = _PendingRequest(f1)
        bus._pending_requests["b"] = _PendingRequest(f2)

        bus._on_backend_closed()

        assert f1.done()
        assert f2.done()
        with pytest.raises(TransportError):
            f1.result()
        with pytest.raises(TransportError):
            f2.result()
        assert len(bus._pending_requests) == 0
        bus.destroy()
        loop.close()

    def test_no_pending_requests_ok(self):
        bus = _make_bus()
        bus._backend = FakeBackend()
        bus._on_backend_closed()  # should not raise
        bus.destroy()


# ---------------------------------------------------------------------------
# request() — message building
# ---------------------------------------------------------------------------

class TestRequestMessageBuilding:

    @pytest.mark.asyncio
    async def test_session_id_auto_injected(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        # Start a request but don't await (we just want to see the sent message)
        task = asyncio.create_task(bus.request("test/method", {"key": "val"}))
        await asyncio.sleep(0.01)

        assert len(fake._sent) == 1
        msg = json.loads(fake._sent[0])
        assert msg["sessionId"] == bus.client_session_id
        assert msg["method"] == "test/method"
        assert msg["params"] == {"key": "val"}
        assert msg["jsonrpc"] == "2.0"
        assert "id" in msg

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_custom_session_id(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", session_id="custom-sess")
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert msg["sessionId"] == "custom-sess"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_agent_id_injected(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", options=RequestOptions(agent_id="agent-xyz"))
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert msg["agentId"] == "agent-xyz"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_extensions_injected(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", options=RequestOptions(
                identity=Identity(subject_id="u1", role="admin", asserted_by="bus"),
                audit=AuditEvent(event_id="e1", action="tools/call", outcome="success"),
            ))
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert "_ext" in msg
        assert msg["_ext"]["identity"]["subjectId"] == "u1"
        assert msg["_ext"]["identity"]["role"] == "admin"
        assert msg["_ext"]["audit"]["eventId"] == "e1"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_no_ext_when_no_identity_audit(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        task = asyncio.create_task(bus.request("m"))
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert "_ext" not in msg

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_request_not_running_raises(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        # NOT started — state is CREATED
        with pytest.raises(InvalidStateError, match="not running"):
            await bus.request("m")
        bus.destroy()


# ---------------------------------------------------------------------------
# notify() — message building
# ---------------------------------------------------------------------------

class TestNotifyMessageBuilding:

    @pytest.mark.asyncio
    async def test_notify_session_id(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        await bus.notify("events/ping", {"ts": 123})

        msg = json.loads(fake._sent[0])
        assert msg["sessionId"] == bus.client_session_id
        assert msg["method"] == "events/ping"
        assert "id" not in msg  # notifications have no id
        bus.destroy()

    @pytest.mark.asyncio
    async def test_notify_with_extensions(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        await bus.notify("m", options=RequestOptions(
            identity=Identity(subject_id="u1", role="user"),
            agent_id="agent-1",
        ))

        msg = json.loads(fake._sent[0])
        assert msg["agentId"] == "agent-1"
        assert msg["_ext"]["identity"]["subjectId"] == "u1"
        bus.destroy()


# ---------------------------------------------------------------------------
# State / Stats when backend is None
# ---------------------------------------------------------------------------

class TestNullBackend:

    def test_get_state_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        assert bus.get_state() == BusState.STOPPED

    def test_get_stats_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        stats = bus.get_stats()
        assert stats.messages_in == 0

    def test_send_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        assert bus.send("test") is False

    def test_get_backend_type_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        assert bus.get_backend_type() == "unknown"

    def test_is_running_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        assert bus.is_running() is False

    def test_destroy_no_backend(self):
        bus = _make_bus()
        bus._backend = None
        bus.destroy()  # should not raise


# ---------------------------------------------------------------------------
# get_backend_type
# ---------------------------------------------------------------------------

class TestGetBackendType:

    def test_subprocess_type(self):
        bus = _make_bus(backend="subprocess")
        assert bus.get_backend_type() == "subprocess"
        bus.destroy()

    def test_docker_type(self):
        import tempfile, os
        fd, path = tempfile.mkstemp(suffix='.json')
        os.write(fd, b'{"pools":[{"id":"w","command":"echo","instances":1}]}')
        os.close(fd)
        try:
            bus = AsyncStdioBus(config_path=path, backend="docker")
            assert bus.get_backend_type() == "docker"
            bus.destroy()
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# StdioBus sync wrapper
# ---------------------------------------------------------------------------

class TestStdioBusSync:

    def test_constructor(self):
        bus = StdioBus(config=_make_cfg())
        assert bus.client_session_id.startswith("client-")
        bus.destroy()

    def test_agent_session_id_property(self):
        bus = StdioBus(config=_make_cfg())
        assert bus.agent_session_id is None
        bus.agent_session_id = "sess-abc"
        assert bus.agent_session_id == "sess-abc"
        bus.destroy()

    def test_on_message(self):
        bus = StdioBus(config=_make_cfg())
        received = []
        bus.on_message(lambda m: received.append(m))
        bus.destroy()

    def test_get_state(self):
        bus = StdioBus(config=_make_cfg())
        assert bus.get_state() == BusState.CREATED
        bus.destroy()

    def test_get_stats(self):
        bus = StdioBus(config=_make_cfg())
        stats = bus.get_stats()
        assert stats.messages_in == 0
        bus.destroy()

    def test_is_running(self):
        bus = StdioBus(config=_make_cfg())
        assert bus.is_running() is False
        bus.destroy()

    def test_get_backend_type(self):
        bus = StdioBus(config=_make_cfg())
        # Will be subprocess or docker depending on env
        assert bus.get_backend_type() in ("subprocess", "docker", "native", "unknown")
        bus.destroy()

    def test_destroy_closes_loop(self):
        bus = StdioBus(config=_make_cfg())
        bus._get_loop()  # force loop creation
        assert bus._loop is not None
        bus.destroy()
        assert bus._loop is None


# ---------------------------------------------------------------------------
# Streaming: agent_message_chunk aggregation
# ---------------------------------------------------------------------------

class TestStreamingChunkAggregation:

    def test_chunks_aggregated_into_result(self):
        """Chunks from agent_message_chunk notifications are joined into result.text."""
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        pending = _PendingRequest(future)
        bus._pending_requests["req-1"] = pending

        # Simulate streaming chunks
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "Hello "}
                }
            }
        }))
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "world!"}
                }
            }
        }))

        # Now send the final response
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-1",
            "result": {"status": "done"}
        }))

        assert future.done()
        result = future.result()
        assert result["status"] == "done"
        assert result["text"] == "Hello world!"

        bus.destroy()
        loop.close()

    def test_no_chunks_no_text_field(self):
        """When no chunks received, result should not have 'text' field."""
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        bus._pending_requests["req-1"] = _PendingRequest(future)

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-1",
            "result": {"status": "done"}
        }))

        result = future.result()
        assert "text" not in result

        bus.destroy()
        loop.close()

    def test_chunks_not_added_to_non_dict_result(self):
        """If result is not a dict, chunks are not attached."""
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        pending = _PendingRequest(future)
        bus._pending_requests["req-1"] = pending

        # Add a chunk
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "chunk"}
                }
            }
        }))

        # Response with non-dict result
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-1",
            "result": "just a string"
        }))

        result = future.result()
        assert result == "just a string"

        bus.destroy()
        loop.close()

    def test_non_chunk_notification_ignored(self):
        """Non-chunk notifications don't affect pending requests."""
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        from stdiobus.client import _PendingRequest
        pending = _PendingRequest(future)
        bus._pending_requests["req-1"] = pending

        # Non-chunk notification
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "some/event",
            "params": {"data": "test"}
        }))

        assert len(pending.chunks) == 0
        bus.destroy()
        loop.close()

    def test_notification_handler_called(self):
        """on_notification handlers receive notifications."""
        bus = _make_bus()
        bus._backend = FakeBackend()
        received = []
        bus.on_notification(lambda m: received.append(m))

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "events/ping",
            "params": {}
        }))

        assert len(received) == 1
        data = json.loads(received[0])
        assert data["method"] == "events/ping"
        bus.destroy()

    def test_multiple_pending_requests_get_chunks(self):
        """Chunks are added to ALL pending requests (typically one, but test multiple)."""
        bus = _make_bus()
        bus._backend = FakeBackend()

        loop = asyncio.new_event_loop()
        f1 = loop.create_future()
        f2 = loop.create_future()
        from stdiobus.client import _PendingRequest
        p1 = _PendingRequest(f1)
        p2 = _PendingRequest(f2)
        bus._pending_requests["r1"] = p1
        bus._pending_requests["r2"] = p2

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "shared chunk"}
                }
            }
        }))

        assert p1.chunks == ["shared chunk"]
        assert p2.chunks == ["shared chunk"]

        bus.destroy()
        loop.close()


# ---------------------------------------------------------------------------
# Cancellation semantics
# ---------------------------------------------------------------------------

class TestCancellationSemantics:

    @pytest.mark.asyncio
    async def test_stop_cancels_pending_requests(self):
        """stop() should fail all pending requests with TransportError."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        from stdiobus.client import _PendingRequest
        loop = asyncio.get_event_loop()
        f1 = loop.create_future()
        f2 = loop.create_future()
        bus._pending_requests["a"] = _PendingRequest(f1)
        bus._pending_requests["b"] = _PendingRequest(f2)

        await bus.stop()

        assert f1.done()
        assert f2.done()
        with pytest.raises(TransportError, match="shutting down"):
            f1.result()
        with pytest.raises(TransportError, match="shutting down"):
            f2.result()
        assert len(bus._pending_requests) == 0
        bus.destroy()

    @pytest.mark.asyncio
    async def test_stop_with_no_pending_ok(self):
        """stop() with no pending requests should not raise."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()
        await bus.stop()  # should not raise
        bus.destroy()

    @pytest.mark.asyncio
    async def test_backend_closed_cancels_pending(self):
        """Backend crash should fail all pending requests."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake

        from stdiobus.client import _PendingRequest
        loop = asyncio.get_event_loop()
        f1 = loop.create_future()
        bus._pending_requests["x"] = _PendingRequest(f1)

        bus._on_backend_closed()

        assert f1.done()
        with pytest.raises(TransportError, match="exited unexpectedly"):
            f1.result()
        bus.destroy()
