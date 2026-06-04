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

"""Foundation tests for the bus-usage-practices feature (Task 1).

These tests pin the additive foundation introduced in Task 1:

  * the loop-marshalling ``_handle_message`` shim + ``_handle_message_on_loop``,
  * the mode-based ``_PendingRequest`` (future-mode preserves today's behavior),
  * the extracted ``_build_request`` wire-assembly helper.

They use the existing ``FakeBackend`` pattern (driving ``_handle_message``
directly, pre-start inline path) and assert that observable behavior is
identical to the pre-feature SDK.

Requirements covered: 1.3, 2.3, 2.8, 3.3; Properties 5, 7.
"""

import asyncio
import json
import threading

import pytest

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PoolConfig,
    Identity,
    AuditEvent,
    RequestOptions,
)
from stdiobus.client import _PendingRequest
from tests.test_client_unit import FakeBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg():
    return BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])


def _make_bus(**kwargs):
    return AsyncStdioBus(config=_make_cfg(), **kwargs)


# ---------------------------------------------------------------------------
# _PendingRequest mode extension (future-mode == today's behavior)
# ---------------------------------------------------------------------------

class TestPendingRequestModes:
    """Property 5 (additivity): future-mode delivery is byte-for-byte as before."""

    def test_positional_future_construction_still_supported(self):
        """Existing call sites use ``_PendingRequest(future)`` positionally."""
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest(fut)
            assert pending.future is fut
            assert pending.queue is None
            assert pending.chunks == []
        finally:
            loop.close()

    def test_for_future_factory(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest.for_future(fut)
            assert pending.future is fut
            assert pending.queue is None
        finally:
            loop.close()

    def test_for_stream_factory_has_unbounded_queue(self):
        pending = _PendingRequest.for_stream()
        assert pending.future is None
        assert isinstance(pending.queue, asyncio.Queue)
        # Unbounded: maxsize 0 means "infinite" for asyncio.Queue.
        assert pending.queue.maxsize == 0

    def test_future_mode_deliver_result_resolves_future(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest.for_future(fut)
            pending.deliver_result({"ok": True})
            assert fut.done()
            assert fut.result() == {"ok": True}
        finally:
            loop.close()

    def test_future_mode_deliver_error_sets_exception(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest.for_future(fut)
            err = RuntimeError("boom")
            pending.deliver_error(err)
            assert fut.done()
            with pytest.raises(RuntimeError, match="boom"):
                fut.result()
        finally:
            loop.close()

    def test_future_mode_deliver_is_noop_once_done(self):
        """Subsumes the old ``if pending.future.done(): return`` guard."""
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest.for_future(fut)
            pending.deliver_result({"first": True})
            # Second delivery must not raise (future already done) and must not
            # overwrite the result.
            pending.deliver_result({"second": True})
            pending.deliver_error(RuntimeError("late"))
            assert fut.result() == {"first": True}
        finally:
            loop.close()

    def test_deliver_chunk_appends_and_enqueues_in_stream_mode(self):
        pending = _PendingRequest.for_stream()
        pending.deliver_chunk("a")
        pending.deliver_chunk("b")
        assert pending.chunks == ["a", "b"]
        assert pending.queue.get_nowait() == ("chunk", "a")
        assert pending.queue.get_nowait() == ("chunk", "b")

    def test_deliver_chunk_only_aggregates_in_future_mode(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            pending = _PendingRequest.for_future(fut)
            pending.deliver_chunk("a")
            assert pending.chunks == ["a"]  # aggregation preserved
            assert pending.queue is None
        finally:
            loop.close()


# ---------------------------------------------------------------------------
# request() preserved behavior (the oracle is the existing test suite)
# ---------------------------------------------------------------------------

class TestRequestStillResolvesAndRaises:
    """Property 5: request() resolves/raises exactly as before the refactor."""

    @pytest.mark.asyncio
    async def test_request_resolves_with_aggregated_text(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()
        # Mirror the runtime: start() captures the owning loop.
        bus._loop = asyncio.get_running_loop()

        async def respond():
            await asyncio.sleep(0.01)
            sent = json.loads(fake._sent[0])
            msg_id = sent["id"]
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "method": "session/update",
                "params": {"update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "Hello "}}},
            }))
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "method": "session/update",
                "params": {"update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "world"}}},
            }))
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "id": msg_id, "result": {"done": True},
            }))

        responder = asyncio.create_task(respond())
        result = await bus.request("m")
        await responder
        assert result["done"] is True
        assert result["text"] == "Hello world"
        assert len(bus._pending_requests) == 0
        bus.destroy()

    @pytest.mark.asyncio
    async def test_request_raises_mapped_error(self):
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()
        bus._loop = asyncio.get_running_loop()

        async def respond():
            await asyncio.sleep(0.01)
            msg_id = json.loads(fake._sent[0])["id"]
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "id": msg_id,
                "error": {"code": 3, "message": "timeout"},
            }))

        responder = asyncio.create_task(respond())
        with pytest.raises(Exception, match="timeout"):
            await bus.request("m")
        await responder
        assert len(bus._pending_requests) == 0
        bus.destroy()


# ---------------------------------------------------------------------------
# Cross-thread dispatch marshalling (Property 7: loop affinity)
# ---------------------------------------------------------------------------

class TestCrossThreadDispatch:

    @pytest.mark.asyncio
    async def test_foreign_thread_marshals_onto_owning_loop(self):
        """A foreign-thread _handle_message must NOT mutate state inline; it is
        scheduled onto the owning loop via call_soon_threadsafe."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        loop = asyncio.get_running_loop()
        bus._loop = loop  # simulate start() having captured the loop

        future = loop.create_future()
        bus._pending_requests["req-x"] = _PendingRequest.for_future(future)

        response = json.dumps({
            "jsonrpc": "2.0", "id": "req-x", "result": {"ok": True},
        })

        def from_other_thread():
            bus._handle_message(response)

        t = threading.Thread(target=from_other_thread)
        t.start()
        t.join()

        # Immediately after the foreign thread returns, the loop callback has
        # not run yet: state must be untouched (no inline mutation).
        assert not future.done()
        assert "req-x" in bus._pending_requests

        # Once the loop turns, the marshalled callback resolves the request.
        result = await asyncio.wait_for(future, timeout=1.0)
        assert result == {"ok": True}
        assert "req-x" not in bus._pending_requests
        bus.destroy()

    @pytest.mark.asyncio
    async def test_same_loop_dispatch_runs_inline(self):
        """On the owning loop thread, dispatch runs inline (no deferral)."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()
        loop = asyncio.get_running_loop()
        bus._loop = loop

        future = loop.create_future()
        bus._pending_requests["req-y"] = _PendingRequest.for_future(future)

        bus._handle_message(json.dumps({
            "jsonrpc": "2.0", "id": "req-y", "result": {"v": 1},
        }))

        # Inline: resolved without yielding control back to the loop.
        assert future.done()
        assert future.result() == {"v": 1}
        bus.destroy()

    def test_pre_start_dispatch_runs_inline(self):
        """Before start() (self._loop is None), dispatch runs inline."""
        bus = _make_bus()
        bus._backend = FakeBackend()
        assert bus._loop is None

        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            bus._pending_requests["r"] = _PendingRequest.for_future(future)
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "id": "r", "result": {"inline": True},
            }))
            assert future.done()
            assert future.result() == {"inline": True}
        finally:
            bus.destroy()
            loop.close()


# ---------------------------------------------------------------------------
# _build_request wire-format parity (mirrors test_contract.py assertions)
# ---------------------------------------------------------------------------

class TestBuildRequestWireParity:
    """Property 5: _build_request output equals the previously inline wire format."""

    def _bus(self):
        return _make_bus()

    def test_bare_method(self):
        bus = self._bus()
        req = bus._build_request("id-1", "ping", None, RequestOptions(), None)
        assert req["jsonrpc"] == "2.0"
        assert req["id"] == "id-1"
        assert req["method"] == "ping"
        assert "params" not in req
        # sessionId always injected, defaulting to the client session id.
        assert req["sessionId"] == bus.client_session_id
        assert "agentId" not in req
        assert "_ext" not in req
        bus.destroy()

    def test_with_params(self):
        bus = self._bus()
        req = bus._build_request("id-2", "tools/list", {"query": "x"}, RequestOptions(), None)
        assert req["params"] == {"query": "x"}
        assert req["method"] == "tools/list"
        bus.destroy()

    def test_empty_params_omitted(self):
        """Falsy params (e.g. {}) are omitted, matching the inline ``if params``."""
        bus = self._bus()
        req = bus._build_request("id-3", "m", {}, RequestOptions(), None)
        assert "params" not in req
        bus.destroy()

    def test_session_id_injection_default(self):
        bus = self._bus()
        req = bus._build_request("id-4", "m", None, RequestOptions(), None)
        assert req["sessionId"] == bus.client_session_id
        bus.destroy()

    def test_session_id_from_positional_argument(self):
        bus = self._bus()
        req = bus._build_request("id-5", "m", None, RequestOptions(), "explicit-session")
        assert req["sessionId"] == "explicit-session"
        bus.destroy()

    def test_session_id_options_override_precedence(self):
        """opts.session_id wins over the session_id argument and the default."""
        bus = self._bus()
        req = bus._build_request(
            "id-6", "m", None,
            RequestOptions(session_id="opts-session"),
            "arg-session",
        )
        assert req["sessionId"] == "opts-session"
        bus.destroy()

    def test_agent_id(self):
        bus = self._bus()
        req = bus._build_request("id-7", "m", None, RequestOptions(agent_id="agent-abc"), None)
        assert req["agentId"] == "agent-abc"
        assert "agentId" not in req.get("params", {})
        bus.destroy()

    def test_ext_identity_and_audit(self):
        bus = self._bus()
        req = bus._build_request(
            "id-8", "m", None,
            RequestOptions(
                identity=Identity(subject_id="u1", role="admin", asserted_by="bus"),
                audit=AuditEvent(event_id="e1", action="tools/call",
                                 parent_event_id="e0", outcome="success"),
            ),
            None,
        )
        ident = req["_ext"]["identity"]
        assert ident == {"subjectId": "u1", "role": "admin", "assertedBy": "bus"}
        audit = req["_ext"]["audit"]
        assert audit["eventId"] == "e1"
        assert audit["action"] == "tools/call"
        assert audit["parentEventId"] == "e0"
        assert audit["outcome"] == "success"
        bus.destroy()

    def test_no_ext_when_absent(self):
        bus = self._bus()
        req = bus._build_request("id-9", "m", None, RequestOptions(), None)
        assert "_ext" not in req
        bus.destroy()

    @pytest.mark.asyncio
    async def test_request_uses_build_request_identically(self):
        """End-to-end: the message request() sends equals _build_request output
        for the same id, proving the extraction is wire-identical."""
        bus = _make_bus()
        fake = FakeBackend()
        bus._backend = fake
        await fake.start()

        opts = RequestOptions(
            agent_id="agent-1",
            identity=Identity(subject_id="u", role="r"),
        )
        task = asyncio.create_task(
            bus.request("m", {"k": "v"}, session_id="sess", options=opts)
        )
        await asyncio.sleep(0.01)
        sent = json.loads(fake._sent[0])

        expected = bus._build_request(sent["id"], "m", {"k": "v"}, opts, "sess")
        assert sent == expected

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        bus.destroy()
