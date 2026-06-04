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

"""Streaming happy-path + cleanup tests for ``AsyncStdioBus.stream_request`` (Task 2.1).

These tests pin the additive incremental-streaming surface introduced in Task 2:

  * incremental ordering — ``chunk`` events are yielded before the ``result`` event,
  * aggregation parity — the ``result`` event's ``result["text"]`` equals what
    ``request()`` produces for the identical chunk+response sequence,
  * no-chunk responses — exactly one ``result`` event, then the stream completes,
  * pending-table cleanup — total on normal completion and on consumer early-break,
  * single-active-stream — a second concurrent ``stream_request`` is rejected.

They follow the existing ``FakeBackend`` pattern (driving ``_handle_message``
directly, pre-start inline path), mirroring ``test_foundation_unit.py``.

Requirements covered: 1.1, 1.2, 1.3, 1.5, 1.8; Properties 1, 2, 3.
"""

import asyncio
import json

import pytest

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PoolConfig,
    InvalidStateError,
    StreamEvent,
)
from tests.test_client_unit import FakeBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg():
    return BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])


async def _make_running_bus():
    """Construct a bus wired to a started FakeBackend with the loop captured.

    Mirrors the runtime: ``start()`` captures the owning loop, after which
    ``_handle_message`` dispatches inline on the same loop.
    """
    bus = AsyncStdioBus(config=_make_cfg())
    fake = FakeBackend()
    bus._backend = fake
    await fake.start()
    bus._loop = asyncio.get_running_loop()
    return bus, fake


async def _await_sent(fake, count=1):
    """Yield control until the backend has observed ``count`` sent messages."""
    for _ in range(10_000):
        if len(fake._sent) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {count} sent message(s), saw {len(fake._sent)}")


def _last_sent_id(fake):
    return json.loads(fake._sent[-1])["id"]


def _chunk_msg(text: str) -> str:
    """A JSON-RPC ``agent_message_chunk`` session-update notification."""
    return json.dumps({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"text": text},
            }
        },
    })


def _result_msg(msg_id: str, result) -> str:
    """A JSON-RPC final response for ``msg_id``."""
    return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": result})


# ---------------------------------------------------------------------------
# Incremental ordering (R1.1; Property 2)
# ---------------------------------------------------------------------------

class TestIncrementalOrdering:

    @pytest.mark.asyncio
    async def test_chunks_yield_before_result_stepwise(self):
        """Each chunk is observed before the result is even delivered.

        Stepping the generator one ``__anext__`` at a time proves true
        incrementality (not merely FIFO ordering): the first chunk event is
        surfaced while no result has been enqueued yet.
        """
        bus, fake = await _make_running_bus()
        try:
            agen = bus.stream_request("m")

            # First __anext__ triggers the send and then blocks on the queue.
            step1 = asyncio.ensure_future(agen.__anext__())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            # Deliver one chunk; the blocked __anext__ resolves to it.
            bus._handle_message(_chunk_msg("Hello "))
            ev1 = await asyncio.wait_for(step1, timeout=1.0)
            assert ev1 == StreamEvent(type="chunk", text="Hello ")

            # Deliver a second chunk, then the result.
            step2 = asyncio.ensure_future(agen.__anext__())
            bus._handle_message(_chunk_msg("world"))
            ev2 = await asyncio.wait_for(step2, timeout=1.0)
            assert ev2 == StreamEvent(type="chunk", text="world")

            step3 = asyncio.ensure_future(agen.__anext__())
            bus._handle_message(_result_msg(msg_id, {"done": True}))
            ev3 = await asyncio.wait_for(step3, timeout=1.0)
            assert ev3.type == "result"
            assert ev3.result["done"] is True
            assert ev3.result["text"] == "Hello world"

            # The result event terminates the stream.
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(agen.__anext__(), timeout=1.0)
        finally:
            await agen.aclose()
            bus.destroy()

    @pytest.mark.asyncio
    async def test_full_iteration_order(self):
        """Consuming the whole stream yields chunks first, then a single result."""
        bus, fake = await _make_running_bus()
        try:
            events: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m"):
                    events.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            bus._handle_message(_chunk_msg("a"))
            bus._handle_message(_chunk_msg("b"))
            bus._handle_message(_chunk_msg("c"))
            bus._handle_message(_result_msg(msg_id, {"ok": True}))

            await asyncio.wait_for(task, timeout=1.0)

            assert [e.type for e in events] == ["chunk", "chunk", "chunk", "result"]
            assert [e.text for e in events[:3]] == ["a", "b", "c"]
            assert events[-1].result["text"] == "abc"
            assert events[-1].result["ok"] is True
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Aggregation parity (R1.2, R1.3; Property 3)
# ---------------------------------------------------------------------------

class TestAggregationParity:

    @pytest.mark.asyncio
    async def test_result_text_equals_request_text(self):
        """stream_request's result["text"] == request()'s result["text"].

        Drives the identical chunk+response sequence through both APIs and
        asserts the aggregated text is byte-for-byte equal.
        """
        chunks = ["The ", "quick ", "brown ", "fox"]
        final_result = {"status": "complete"}

        # --- request() oracle ---
        bus_a, fake_a = await _make_running_bus()
        try:
            req_task = asyncio.create_task(bus_a.request("m"))
            await _await_sent(fake_a)
            req_id = _last_sent_id(fake_a)
            for c in chunks:
                bus_a._handle_message(_chunk_msg(c))
            bus_a._handle_message(_result_msg(req_id, dict(final_result)))
            request_result = await asyncio.wait_for(req_task, timeout=1.0)
        finally:
            bus_a.destroy()

        # --- stream_request() under test ---
        bus_b, fake_b = await _make_running_bus()
        try:
            events: list[StreamEvent] = []

            async def consume():
                async for ev in bus_b.stream_request("m"):
                    events.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake_b)
            stream_id = _last_sent_id(fake_b)
            for c in chunks:
                bus_b._handle_message(_chunk_msg(c))
            bus_b._handle_message(_result_msg(stream_id, dict(final_result)))
            await asyncio.wait_for(task, timeout=1.0)
        finally:
            bus_b.destroy()

        result_event = events[-1]
        assert result_event.type == "result"
        assert request_result["text"] == "The quick brown fox"
        assert result_event.result["text"] == request_result["text"]
        # Chunk events mirror the same arrival sequence.
        assert [e.text for e in events if e.type == "chunk"] == chunks


# ---------------------------------------------------------------------------
# No-chunk response (R1.8)
# ---------------------------------------------------------------------------

class TestNoChunkResponse:

    @pytest.mark.asyncio
    async def test_single_result_event_no_chunks(self):
        """A response with no preceding chunk yields exactly one result event."""
        bus, fake = await _make_running_bus()
        try:
            events: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m"):
                    events.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            bus._handle_message(_result_msg(msg_id, {"status": "done"}))
            await asyncio.wait_for(task, timeout=1.0)

            assert len(events) == 1
            assert events[0].type == "result"
            assert events[0].result == {"status": "done"}
            # No chunks were received, so no aggregated text is attached.
            assert "text" not in events[0].result
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Pending-table cleanup (R1.5; Property 1)
# ---------------------------------------------------------------------------

class TestPendingCleanup:

    @pytest.mark.asyncio
    async def test_cleanup_after_normal_completion(self):
        """After the result event, the stream's pending entry is removed."""
        bus, fake = await _make_running_bus()
        try:
            events: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m"):
                    events.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            assert msg_id in bus._pending_requests  # active mid-stream

            bus._handle_message(_chunk_msg("x"))
            bus._handle_message(_result_msg(msg_id, {"ok": True}))
            await asyncio.wait_for(task, timeout=1.0)

            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_cleanup_after_consumer_early_break(self):
        """Closing the generator early removes the pending entry and frees the queue."""
        bus, fake = await _make_running_bus()
        try:
            agen = bus.stream_request("m")
            step1 = asyncio.ensure_future(agen.__anext__())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            bus._handle_message(_chunk_msg("partial"))
            ev = await asyncio.wait_for(step1, timeout=1.0)
            assert ev.type == "chunk"
            assert msg_id in bus._pending_requests  # still active before break

            # Consumer stops early (the async-for `break` path → aclose()).
            await agen.aclose()

            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Single active stream is enforced
# ---------------------------------------------------------------------------

class TestSingleActiveStream:

    @pytest.mark.asyncio
    async def test_second_concurrent_stream_raises(self):
        """A second stream_request while one is active raises InvalidStateError."""
        bus, fake = await _make_running_bus()
        try:
            agen1 = bus.stream_request("m1")
            step1 = asyncio.ensure_future(agen1.__anext__())
            await _await_sent(fake)  # first stream-mode pending now registered

            agen2 = bus.stream_request("m2")
            with pytest.raises(
                InvalidStateError,
                match="Only one active stream_request",
            ):
                await agen2.__anext__()

            # The first stream is unaffected and still completes cleanly.
            msg_id = _last_sent_id(fake)
            bus._handle_message(_result_msg(msg_id, {"ok": True}))
            ev = await asyncio.wait_for(step1, timeout=1.0)
            assert ev.type == "result"
        finally:
            await agen1.aclose()
            bus.destroy()

    @pytest.mark.asyncio
    async def test_sequential_streams_allowed(self):
        """After the first stream completes, a new stream may start."""
        bus, fake = await _make_running_bus()
        try:
            # First stream, run to completion.
            events1: list[StreamEvent] = []

            async def consume1():
                async for ev in bus.stream_request("m1"):
                    events1.append(ev)

            t1 = asyncio.create_task(consume1())
            await _await_sent(fake, count=1)
            bus._handle_message(_result_msg(_last_sent_id(fake), {"n": 1}))
            await asyncio.wait_for(t1, timeout=1.0)
            assert events1[-1].result["n"] == 1
            assert len(bus._pending_requests) == 0

            # Second stream, now permitted because the first is gone.
            events2: list[StreamEvent] = []

            async def consume2():
                async for ev in bus.stream_request("m2"):
                    events2.append(ev)

            t2 = asyncio.create_task(consume2())
            await _await_sent(fake, count=2)
            bus._handle_message(_result_msg(_last_sent_id(fake), {"n": 2}))
            await asyncio.wait_for(t2, timeout=1.0)
            assert events2[-1].result["n"] == 2
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Guard: streaming requires a running bus
# ---------------------------------------------------------------------------

class TestStreamGuards:

    @pytest.mark.asyncio
    async def test_stream_request_not_running_raises(self):
        """stream_request on a non-running bus raises InvalidStateError."""
        bus = AsyncStdioBus(config=_make_cfg())
        fake = FakeBackend()
        bus._backend = fake  # NOT started — state is CREATED
        try:
            agen = bus.stream_request("m")
            with pytest.raises(InvalidStateError, match="not running"):
                await agen.__anext__()
        finally:
            bus.destroy()
