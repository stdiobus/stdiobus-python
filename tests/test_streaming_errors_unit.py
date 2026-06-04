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

"""Streaming error + transport-teardown tests for ``stream_request`` (Task 3.1).

These pin the terminal failure paths of the additive incremental-streaming
surface introduced in Tasks 2/3:

  * total-deadline timeout — a stream with no final response raises the SDK
    ``TimeoutError`` and the pending entry is removed (R1.4),
  * backend crash mid-stream — ``_on_backend_closed`` / ``stop()`` deliver a
    terminal ``TransportError`` that unblocks the consumer and clears the
    pending entry, and the ``TransportError`` wins *even when the total deadline
    has also elapsed* (R1.6),
  * JSON-RPC error response — the mapped SDK exception is raised *after* any
    chunk events already received are delivered (R1.7).

Together these exercise the correctness properties for the error paths:
Property 1 (pending-table cleanup is total), Property 4 (a single terminal
event — exactly one raised exception, never also a result), and Property 8
(termination liveness — a blocked ``async for`` is always unblocked).

They follow the existing ``FakeBackend`` pattern (driving ``_handle_message``
directly on the captured loop), mirroring ``test_streaming_unit.py``.

Requirements covered: 1.4, 1.6, 1.7; Properties 1, 4, 8.
"""

import asyncio
import json

import pytest

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PolicyDeniedError,
    PoolConfig,
    StreamEvent,
    TransportError,
)
from stdiobus.errors import TimeoutError as StdioBusTimeoutError  # SDK, not builtin
from tests.test_client_unit import FakeBackend

# ---------------------------------------------------------------------------
# Helpers (mirror tests/test_streaming_unit.py)
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


def _error_msg(msg_id: str, code: int, message: str, data=None) -> str:
    """A JSON-RPC error response for ``msg_id``."""
    error: dict = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return json.dumps({"jsonrpc": "2.0", "id": msg_id, "error": error})


# ---------------------------------------------------------------------------
# Total-deadline timeout (R1.4; Properties 1, 8)
# ---------------------------------------------------------------------------

class TestStreamTimeout:

    @pytest.mark.asyncio
    async def test_no_response_raises_timeout_and_removes_pending(self):
        """A stream that never receives a final response raises TimeoutError.

        The consumer blocked in ``async for`` is unblocked by the total-deadline
        expiry (Property 8), and the pending entry is removed on the way out
        (R1.4; Property 1).
        """
        bus, fake = await _make_running_bus()
        try:
            received: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m", timeout_ms=30):
                    received.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)
            assert msg_id in bus._pending_requests  # active before the deadline

            with pytest.raises(StdioBusTimeoutError, match="Request timeout: m"):
                await asyncio.wait_for(task, timeout=1.0)

            # No terminal result was produced — the timeout is the sole terminal.
            assert all(e.type != "result" for e in received)
            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_timeout_after_chunks_still_removes_pending(self):
        """Chunks delivered before the deadline are observed; timeout then fires.

        Confirms the deadline is a *total* deadline over the whole stream, not a
        per-chunk timer, and that cleanup is total regardless (Property 1).
        """
        bus, fake = await _make_running_bus()
        try:
            received: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m", timeout_ms=40):
                    received.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            bus._handle_message(_chunk_msg("partial "))
            bus._handle_message(_chunk_msg("output"))

            with pytest.raises(StdioBusTimeoutError):
                await asyncio.wait_for(task, timeout=1.0)

            assert [e.text for e in received if e.type == "chunk"] == [
                "partial ", "output",
            ]
            assert msg_id not in bus._pending_requests
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Backend crash mid-stream (R1.6; Properties 1, 4, 8)
# ---------------------------------------------------------------------------

class TestStreamBackendCrash:

    @pytest.mark.asyncio
    async def test_backend_closed_raises_transport_error_and_removes_pending(self):
        """A backend exit during an active stream raises TransportError.

        ``_on_backend_closed`` delivers a terminal error to the stream-mode
        pending (it previously only set exceptions on future-mode pendings),
        unblocking the consumer (Property 8) and clearing the pending table
        (R1.6; Property 1).
        """
        bus, fake = await _make_running_bus()
        try:
            agen = bus.stream_request("m")
            step1 = asyncio.ensure_future(agen.__anext__())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)
            assert msg_id in bus._pending_requests

            bus._on_backend_closed()  # backend process exits unexpectedly

            with pytest.raises(TransportError, match="exited unexpectedly"):
                await asyncio.wait_for(step1, timeout=1.0)

            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            await agen.aclose()
            bus.destroy()

    @pytest.mark.asyncio
    async def test_transport_error_wins_even_when_deadline_also_elapsed(self):
        """R1.6: backend exit ALWAYS surfaces as TransportError, never Timeout.

        Sets up the adversarial race the requirement calls out: a terminal
        ``TransportError`` is already queued by the backend-exit path *and* the
        total deadline has elapsed before the generator next inspects the queue.
        The drain-first loop consumes the queued error before considering the
        deadline, so the single terminal outcome (Property 4) is the
        ``TransportError``, not a ``TimeoutError``.
        """
        bus, fake = await _make_running_bus()
        try:
            agen = bus.stream_request("m", timeout_ms=30)
            step1 = asyncio.ensure_future(agen.__anext__())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            # Deliver a chunk so the generator yields and suspends at `yield`.
            bus._handle_message(_chunk_msg("partial"))
            ev = await asyncio.wait_for(step1, timeout=1.0)
            assert ev == StreamEvent(type="chunk", text="partial")

            # Backend crashes: enqueues a terminal TransportError...
            bus._on_backend_closed()
            # ...and the total deadline elapses before the next step, so a
            # timeout *also* looms at the next queue inspection.
            await asyncio.sleep(0.05)

            with pytest.raises(TransportError, match="exited unexpectedly"):
                await asyncio.wait_for(agen.__anext__(), timeout=1.0)

            assert msg_id not in bus._pending_requests
        finally:
            await agen.aclose()
            bus.destroy()

    @pytest.mark.asyncio
    async def test_stop_terminates_active_stream_with_transport_error(self):
        """stop() delivers a terminal TransportError to an active stream.

        The task extends both ``_on_backend_closed`` and ``stop()``; this pins
        the graceful-shutdown path: a consumer mid-stream is unblocked with
        ``TransportError`` and the pending is cleared (Property 1, 8).
        """
        bus, fake = await _make_running_bus()
        try:
            agen = bus.stream_request("m")
            step1 = asyncio.ensure_future(agen.__anext__())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)
            assert msg_id in bus._pending_requests

            await bus.stop()

            with pytest.raises(TransportError, match="shutting down"):
                await asyncio.wait_for(step1, timeout=1.0)

            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            await agen.aclose()
            bus.destroy()


# ---------------------------------------------------------------------------
# JSON-RPC error response (R1.7; Properties 1, 4)
# ---------------------------------------------------------------------------

class TestStreamJsonRpcError:

    @pytest.mark.asyncio
    async def test_error_response_raises_mapped_exception_after_chunks(self):
        """A JSON-RPC error raises the mapped SDK exception after prior chunks.

        Chunks received before the error response are still yielded to the
        consumer (R1.7), then the mapped exception (code 7 → PolicyDeniedError)
        is raised as the single terminal — no result event is produced
        (Property 4) — and the pending is removed (Property 1).
        """
        bus, fake = await _make_running_bus()
        try:
            received: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m"):
                    received.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            # Chunks arrive first, then a JSON-RPC error response.
            bus._handle_message(_chunk_msg("before "))
            bus._handle_message(_chunk_msg("error"))
            bus._handle_message(_error_msg(msg_id, code=7, message="policy denied"))

            with pytest.raises(PolicyDeniedError, match="policy denied"):
                await asyncio.wait_for(task, timeout=1.0)

            # The chunk events delivered before the error are still observed.
            assert [e.text for e in received if e.type == "chunk"] == [
                "before ", "error",
            ]
            # The raised exception is the sole terminal: no result event.
            assert all(e.type != "result" for e in received)
            assert msg_id not in bus._pending_requests
            assert len(bus._pending_requests) == 0
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_error_response_with_no_prior_chunks_raises_mapped_exception(self):
        """An immediate JSON-RPC error (no chunks) raises the mapped exception."""
        bus, fake = await _make_running_bus()
        try:
            received: list[StreamEvent] = []

            async def consume():
                async for ev in bus.stream_request("m"):
                    received.append(ev)

            task = asyncio.create_task(consume())
            await _await_sent(fake)
            msg_id = _last_sent_id(fake)

            bus._handle_message(_error_msg(msg_id, code=7, message="denied"))

            with pytest.raises(PolicyDeniedError, match="denied"):
                await asyncio.wait_for(task, timeout=1.0)

            assert received == []  # no events at all, just the terminal raise
            assert msg_id not in bus._pending_requests
        finally:
            bus.destroy()
