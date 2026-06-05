#!/usr/bin/env python3

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

"""
Real E2E pytest suite — exercises stream_request, subscribe_notifications,
and StdioBusBuilder through the live stdio_bus kernel (subprocess backend
with programmatic BusConfig).

Tests SDK features against the real bus binary via SubprocessOptions(binary_path=...).
The worker (stream_echo_worker.py) emits session/update notifications with
sessionId for bus routing, then a final response.

Run:
    pytest tests/test_real_e2e.py -v
"""

import asyncio
import os
import sys

import pytest

from stdiobus import (
    AsyncStdioBus,
    StdioBusBuilder,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    SubprocessOptions,
    StreamEvent,
)

# ---------------------------------------------------------------------------
# Binary & worker resolution
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
STREAM_WORKER = os.path.join(TESTS_DIR, "stream_echo_worker.py")

# Use the same resolution logic as the SDK itself.
from stdiobus._resolve_binary import resolve_binary

skip_no_binary = pytest.mark.skipif(
    resolve_binary() is None,
    reason="stdio_bus binary not available (not bundled and not in PATH)",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config() -> BusConfig:
    """Programmatic BusConfig pointing at the streaming echo worker."""
    return BusConfig(
        pools=[PoolConfig(
            id="stream",
            command=sys.executable,
            args=[STREAM_WORKER],
            instances=1,
        )],
        limits=LimitsConfig(
            max_input_buffer=1048576,
            max_restarts=3,
        ),
    )


def _make_subprocess_options() -> SubprocessOptions:
    return SubprocessOptions(
        start_timeout_sec=5.0,
        drain_timeout_sec=10.0,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def bus():
    """Provide a started AsyncStdioBus with programmatic BusConfig."""
    client = AsyncStdioBus(
        config=_make_config(),
        backend="subprocess",
        subprocess=_make_subprocess_options(),
        timeout_ms=15000,
    )
    await client.start()
    await asyncio.sleep(0.5)
    yield client
    await client.stop(timeout_sec=5.0)
    client.destroy()


# ---------------------------------------------------------------------------
# Tests: sanity — plain request()
# ---------------------------------------------------------------------------


@skip_no_binary
@pytest.mark.asyncio
async def test_sanity_request(bus):
    """Plain request() through programmatic BusConfig returns expected echo."""
    result = await bus.request("echo", {"message": "hello from pytest"})
    assert result["echo"]["message"] == "hello from pytest"
    assert result["method"] == "echo"
    assert result["receivedSessionId"] == bus.client_session_id


# ---------------------------------------------------------------------------
# Tests: stream_request()
# ---------------------------------------------------------------------------


@skip_no_binary
@pytest.mark.asyncio
async def test_stream_request_incremental_chunks(bus):
    """stream_request() yields chunks incrementally, final result has aggregated text."""
    text = "alpha beta gamma delta"
    words = text.split()

    chunks_received: list[str] = []
    final_result = None

    async for event in bus.stream_request("stream_echo", {"text": text}):
        assert isinstance(event, StreamEvent)
        if event.type == "chunk":
            assert event.text is not None
            chunks_received.append(event.text)
        elif event.type == "result":
            final_result = event.result

    # One chunk per word
    assert len(chunks_received) == len(words)
    for i, word in enumerate(words):
        assert chunks_received[i] == word + " "

    # Final result carries aggregated text
    assert final_result is not None
    assert "text" in final_result
    assert final_result["text"] == "alpha beta gamma delta "
    assert final_result["chunks_sent"] == 4


@skip_no_binary
@pytest.mark.asyncio
async def test_stream_request_empty_text(bus):
    """stream_request() with empty text yields zero chunks and a result."""
    events: list[StreamEvent] = []
    async for event in bus.stream_request("stream_echo", {"text": ""}):
        events.append(event)

    chunk_events = [e for e in events if e.type == "chunk"]
    result_events = [e for e in events if e.type == "result"]
    assert len(chunk_events) == 0
    assert len(result_events) == 1
    assert result_events[0].result["chunks_sent"] == 0


@skip_no_binary
@pytest.mark.asyncio
async def test_stream_request_cleanup_after_completion(bus):
    """After stream_request completes, pending request table is empty."""
    async for _ in bus.stream_request("stream_echo", {"text": "one two"}):
        pass
    assert len(bus._pending_requests) == 0


# ---------------------------------------------------------------------------
# Tests: subscribe_notifications()
# ---------------------------------------------------------------------------


@skip_no_binary
@pytest.mark.asyncio
async def test_subscribe_notifications(bus):
    """subscribe_notifications() receives live notifications forwarded by the bus."""
    sub = bus.subscribe_notifications(max_queue=64, overflow="drop")

    # notify_test causes worker to emit a session/update notification
    result = await bus.request("notify_test", {"key": "value"})
    assert result["notified"] is True

    # Drain notifications
    received: list[dict] = []
    try:
        while True:
            notification = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            received.append(notification)
    except (asyncio.TimeoutError, StopAsyncIteration):
        pass

    sub.close()

    # The session/update notification from notify_test should be present
    session_updates = [
        n for n in received
        if n.get("method") == "session/update"
    ]
    assert len(session_updates) >= 1
    update = session_updates[0]["params"]["update"]
    assert update["sessionUpdate"] == "custom_event"
    assert update["event"] == "test_fired"
    assert update["payload"] == {"key": "value"}


@skip_no_binary
@pytest.mark.asyncio
async def test_subscribe_notifications_multiple_subscribers(bus):
    """Multiple subscribers each independently receive all notifications."""
    sub1 = bus.subscribe_notifications(max_queue=32, overflow="drop")
    sub2 = bus.subscribe_notifications(max_queue=32, overflow="drop")

    await bus.request("notify_test", {"seq": 1})

    async def drain(sub, timeout=1.0):
        items = []
        try:
            while True:
                items.append(await asyncio.wait_for(sub.__anext__(), timeout=timeout))
        except (asyncio.TimeoutError, StopAsyncIteration):
            pass
        return items

    items1 = await drain(sub1)
    items2 = await drain(sub2)

    sub1.close()
    sub2.close()

    # Both see the session/update notification
    updates1 = [n for n in items1 if n.get("method") == "session/update"]
    updates2 = [n for n in items2 if n.get("method") == "session/update"]
    assert len(updates1) >= 1
    assert len(updates2) >= 1


# ---------------------------------------------------------------------------
# Tests: StdioBusBuilder
# ---------------------------------------------------------------------------


@skip_no_binary
@pytest.mark.asyncio
async def test_builder_config_build():
    """StdioBusBuilder().config(...).build() produces a working client."""
    bus = (
        StdioBusBuilder()
        .config(_make_config())
        .backend("subprocess")
        .subprocess(_make_subprocess_options())
        .timeout_ms(15000)
        .build()
    )

    try:
        await bus.start()
        await asyncio.sleep(0.5)

        result = await bus.request("echo", {"builder": True})
        assert result["echo"]["builder"] is True
        assert result["method"] == "echo"
        assert result["receivedSessionId"] == bus.client_session_id
    finally:
        await bus.stop(timeout_sec=5.0)
        bus.destroy()


@skip_no_binary
@pytest.mark.asyncio
async def test_builder_stream_request():
    """StdioBusBuilder-created client supports stream_request with live chunks."""
    bus = (
        StdioBusBuilder()
        .config(_make_config())
        .backend("subprocess")
        .subprocess(_make_subprocess_options())
        .timeout_ms(15000)
        .build()
    )

    try:
        await bus.start()
        await asyncio.sleep(0.5)

        chunks: list[str] = []
        final = None
        async for event in bus.stream_request("stream_echo", {"text": "foo bar"}):
            if event.type == "chunk":
                chunks.append(event.text)
            elif event.type == "result":
                final = event.result

        assert len(chunks) == 2
        assert chunks[0] == "foo "
        assert chunks[1] == "bar "
        assert final is not None
        assert final["text"] == "foo bar "
    finally:
        await bus.stop(timeout_sec=5.0)
        bus.destroy()
