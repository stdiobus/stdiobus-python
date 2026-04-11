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

"""Cross-SDK contract tests for stdiobus Python SDK.

These tests verify wire-level parity with Node and Rust SDKs.
They validate the exact JSON-RPC message format, routing fields,
and protocol behavior that must be identical across all SDKs.

Run against real stdio_bus binary:
    STDIOBUS_BINARY=./build/stdio_bus pytest tests/test_contract.py -v

Or skip if binary not available (default).
"""

import asyncio
import json
import os
import sys
import textwrap
import pytest

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    BusState,
    RequestOptions,
    Identity,
    AuditEvent,
    SubprocessOptions,
    HelloParams,
)
from stdiobus.client import _PendingRequest


# ---------------------------------------------------------------------------
# Contract: JSON-RPC message format
# ---------------------------------------------------------------------------

class TestWireFormatContract:
    """Verify exact wire format of outbound messages matches Node/Rust SDKs."""

    def _capture_sent(self):
        """Helper: create bus with fake backend that captures sent messages."""
        from tests.test_client_unit import FakeBackend
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )
        fake = FakeBackend()
        bus._backend = fake
        return bus, fake

    @pytest.mark.asyncio
    async def test_request_has_required_fields(self):
        """Every request must have: jsonrpc, id, method, sessionId."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(bus.request("tools/list", {"query": "test"}))
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert msg["jsonrpc"] == "2.0"
        assert isinstance(msg["id"], str) and len(msg["id"]) > 0
        assert msg["method"] == "tools/list"
        assert msg["params"] == {"query": "test"}
        assert "sessionId" in msg  # auto-injected
        assert msg["sessionId"].startswith("client-")

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_request_without_params(self):
        """Request without params should not include params field."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(bus.request("ping"))
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert "params" not in msg or msg.get("params") is None

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_session_id_override(self):
        """session_id parameter overrides auto-generated clientSessionId."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", session_id="explicit-session-123")
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert msg["sessionId"] == "explicit-session-123"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_agent_id_at_top_level(self):
        """agentId must be at top level of message (not in params)."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", options=RequestOptions(agent_id="agent-abc"))
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        assert msg["agentId"] == "agent-abc"
        assert "agentId" not in msg.get("params", {})

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_ext_identity_format(self):
        """_ext.identity must use camelCase keys matching Node/Rust SDKs."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", options=RequestOptions(
                identity=Identity(
                    subject_id="user-1",
                    role="admin",
                    asserted_by="bus",
                ),
            ))
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        ident = msg["_ext"]["identity"]
        # Must be camelCase (not snake_case)
        assert "subjectId" in ident
        assert "role" in ident
        assert "assertedBy" in ident
        assert ident["subjectId"] == "user-1"
        assert ident["role"] == "admin"
        assert ident["assertedBy"] == "bus"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_ext_audit_format(self):
        """_ext.audit must use camelCase keys matching Node/Rust SDKs."""
        bus, fake = self._capture_sent()
        await fake.start()

        task = asyncio.create_task(
            bus.request("m", options=RequestOptions(
                audit=AuditEvent(
                    event_id="evt-1",
                    action="tools/call",
                    parent_event_id="evt-0",
                    outcome="success",
                ),
            ))
        )
        await asyncio.sleep(0.01)

        msg = json.loads(fake._sent[0])
        audit = msg["_ext"]["audit"]
        assert "eventId" in audit
        assert "action" in audit
        assert "parentEventId" in audit
        assert "outcome" in audit

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()

    @pytest.mark.asyncio
    async def test_notification_format(self):
        """Notifications must have jsonrpc, method, sessionId but NO id."""
        bus, fake = self._capture_sent()
        await fake.start()

        await bus.notify("events/ping", {"ts": 123})

        msg = json.loads(fake._sent[0])
        assert msg["jsonrpc"] == "2.0"
        assert msg["method"] == "events/ping"
        assert "sessionId" in msg
        assert "id" not in msg

        bus.destroy()

    @pytest.mark.asyncio
    async def test_no_ext_when_empty(self):
        """_ext field must NOT be present when no identity/audit provided."""
        bus, fake = self._capture_sent()
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


# ---------------------------------------------------------------------------
# Contract: Streaming chunk aggregation
# ---------------------------------------------------------------------------

class TestStreamingContract:
    """Verify streaming chunk aggregation matches Rust SDK behavior."""

    def test_chunk_extraction_path(self):
        """Chunks must be extracted from params.update.content.text."""
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        pending = _PendingRequest(future)
        bus._pending_requests["r1"] = pending

        # Exact wire format from ACP protocol
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "chunk1"}
                }
            }
        }))

        assert pending.chunks == ["chunk1"]
        bus.destroy()
        loop.close()

    def test_aggregated_text_in_result(self):
        """Aggregated text must be set as result.text (matching Rust SDK)."""
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        pending = _PendingRequest(future)
        bus._pending_requests["r1"] = pending

        # Two chunks
        for text in ["Hello ", "world"]:
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"text": text}
                    }
                }
            }))

        # Response
        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "r1",
            "result": {"done": True}
        }))

        result = future.result()
        assert result["text"] == "Hello world"
        assert result["done"] is True

        bus.destroy()
        loop.close()


# ---------------------------------------------------------------------------
# Contract: Cancellation semantics
# ---------------------------------------------------------------------------

class TestCancellationContract:
    """Verify cancellation behavior matches across SDKs."""

    @pytest.mark.asyncio
    async def test_stop_error_type(self):
        """stop() must fail pending with TransportError (not CancelledError)."""
        from stdiobus.errors import TransportError
        from tests.test_client_unit import FakeBackend

        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        loop = asyncio.get_event_loop()
        f = loop.create_future()
        bus._pending_requests["x"] = _PendingRequest(f)

        await bus.stop()

        assert f.done()
        with pytest.raises(TransportError):
            f.result()
        bus.destroy()

    @pytest.mark.asyncio
    async def test_backend_crash_error_type(self):
        """Backend crash must fail pending with TransportError."""
        from stdiobus.errors import TransportError
        from tests.test_client_unit import FakeBackend

        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )
        fake = FakeBackend()
        bus._backend = fake

        loop = asyncio.get_event_loop()
        f = loop.create_future()
        bus._pending_requests["x"] = _PendingRequest(f)

        bus._on_backend_closed()

        with pytest.raises(TransportError, match="exited unexpectedly"):
            f.result()
        bus.destroy()


# ---------------------------------------------------------------------------
# Contract: Config serialization
# ---------------------------------------------------------------------------

class TestConfigContract:
    """Verify config JSON matches C bus schema exactly."""

    def test_minimal_config(self):
        cfg = BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])
        data = json.loads(cfg.to_json())
        assert data == {
            "pools": [{"id": "w", "command": "echo", "args": [], "instances": 1}]
        }

    def test_full_config(self):
        cfg = BusConfig(
            pools=[
                PoolConfig(id="a", command="node", args=["w.js"], instances=4),
                PoolConfig(id="b", command="python", args=["-m", "worker"], instances=2),
            ],
            limits=LimitsConfig(
                max_input_buffer=1048576,
                max_output_queue=4194304,
                max_restarts=5,
                restart_window_sec=60,
                drain_timeout_sec=30,
                backpressure_timeout_sec=60,
            ),
        )
        data = json.loads(cfg.to_json())
        assert len(data["pools"]) == 2
        assert data["pools"][0]["id"] == "a"
        assert data["pools"][0]["instances"] == 4
        assert data["limits"]["max_input_buffer"] == 1048576
        assert data["limits"]["drain_timeout_sec"] == 30

    def test_hello_params_wire_format(self):
        """HelloParams must serialize to exact wire format."""
        from stdiobus.types import ExtensionInfo
        params = HelloParams(
            protocol_version="0.1.0",
            extensions={
                "identity": ExtensionInfo(version="0.1.0", required=True),
            },
        )
        d = params.to_dict()
        assert d == {
            "protocolVersion": "0.1.0",
            "extensions": {
                "identity": {"version": "0.1.0", "required": True}
            }
        }
