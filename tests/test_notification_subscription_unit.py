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

"""Pull-based notification subscription tests for ``subscribe_notifications`` (Task 4.1).

These tests pin the additive pull-subscription surface introduced in Task 4:

  * fan-out — every active subscriber receives every notification delivered
    after it was created (R2.1, R2.2),
  * push-callback coexistence — the existing ``on_notification`` callbacks still
    fire alongside subscriptions (R2.3),
  * bounded queue + overflow — ``max_queue`` is honoured, ``overflow="drop"``
    discards the newest for only the affected subscriber, ``overflow="close"``
    terminates only that subscriber and drains buffered items before stopping
    (R2.4, R2.5; Property 6),
  * teardown — ``close()``, ``stop()``, and ``destroy()`` remove subscribers and
    terminate active iterators via ``StopAsyncIteration`` (R2.6, R2.7; Property 8),
  * malformed input — non-JSON messages never reach subscriptions (R2.8).

They follow the existing ``FakeBackend`` pattern (driving ``_handle_message``
directly, pre-start inline path), mirroring ``test_streaming_unit.py``.

Requirements covered: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8; Properties 6, 8.
"""

import asyncio
import json

import pytest

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PoolConfig,
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


def _notif_msg(method: str, params=None) -> str:
    """A JSON-RPC notification (method, no id)."""
    msg = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    return json.dumps(msg)


async def _drain(sub) -> list:
    """Consume a subscription to its natural termination (StopAsyncIteration)."""
    out = []
    async for item in sub:
        out.append(item)
    return out


async def _take(sub, n: int, timeout: float = 1.0) -> list:
    """Pull exactly ``n`` items, each within ``timeout`` seconds."""
    out = []
    for _ in range(n):
        out.append(await asyncio.wait_for(sub.__anext__(), timeout=timeout))
    return out


# ---------------------------------------------------------------------------
# Fan-out: multiple independent subscribers (R2.1, R2.2)
# ---------------------------------------------------------------------------

class TestFanOut:

    @pytest.mark.asyncio
    async def test_single_subscriber_receives_notifications(self):
        """A subscriber receives every notification delivered after it subscribes."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()

            bus._handle_message(_notif_msg("events/a", {"n": 1}))
            bus._handle_message(_notif_msg("events/b", {"n": 2}))

            items = await _take(sub, 2)
            assert [i["method"] for i in items] == ["events/a", "events/b"]
            assert [i["params"]["n"] for i in items] == [1, 2]
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_multiple_subscribers_each_receive_every_notification(self):
        """Every active subscriber independently receives every notification."""
        bus, _ = await _make_running_bus()
        try:
            sub1 = bus.subscribe_notifications()
            sub2 = bus.subscribe_notifications()

            bus._handle_message(_notif_msg("events/x", {"v": 1}))
            bus._handle_message(_notif_msg("events/y", {"v": 2}))

            got1 = await _take(sub1, 2)
            got2 = await _take(sub2, 2)

            assert [i["method"] for i in got1] == ["events/x", "events/y"]
            assert [i["method"] for i in got2] == ["events/x", "events/y"]
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_only_notifications_after_subscribe_delivered(self):
        """Notifications delivered before a subscriber is created are not seen."""
        bus, _ = await _make_running_bus()
        try:
            # Delivered with no subscribers — nothing buffered anywhere.
            bus._handle_message(_notif_msg("events/early", {"v": 0}))

            sub = bus.subscribe_notifications()
            bus._handle_message(_notif_msg("events/late", {"v": 1}))

            item = (await _take(sub, 1))[0]
            assert item["method"] == "events/late"
            # The early notification was never buffered for this subscriber.
            assert sub._queue.empty()
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_subscribers_get_independent_copies(self):
        """Each subscriber receives a distinct top-level dict (per-subscriber shallow copy).

        Delivery is a shallow copy (``dict(data)``), so each subscriber owns an
        independent top-level mapping: adding/removing a top-level key in one
        cannot affect another. (Nested objects are shared by design; consumers
        are documented to treat yielded dicts as read-only.)
        """
        bus, _ = await _make_running_bus()
        try:
            sub1 = bus.subscribe_notifications()
            sub2 = bus.subscribe_notifications()

            bus._handle_message(_notif_msg("events/z", {"v": 1}))

            a = (await _take(sub1, 1))[0]
            b = (await _take(sub2, 1))[0]
            assert a == b
            assert a is not b  # per-subscriber shallow copy
            a["consumerTag"] = "mine"  # top-level mutation
            assert "consumerTag" not in b  # did not leak to the other subscriber
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Coexistence with the existing push callback (R2.3)
# ---------------------------------------------------------------------------

class TestPushCallbackCoexistence:

    @pytest.mark.asyncio
    async def test_on_notification_still_fires_with_subscriptions(self):
        """Push callbacks fire for every notification alongside pull subscribers."""
        bus, _ = await _make_running_bus()
        try:
            pushed: list[str] = []
            bus.on_notification(lambda m: pushed.append(m))

            sub = bus.subscribe_notifications()
            bus._handle_message(_notif_msg("events/p", {"v": 1}))

            pulled = (await _take(sub, 1))[0]

            assert len(pushed) == 1
            assert json.loads(pushed[0])["method"] == "events/p"
            assert pulled["method"] == "events/p"
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_push_callback_receives_string_payload(self):
        """The push callback still receives the raw string payload, unchanged."""
        bus, _ = await _make_running_bus()
        try:
            pushed: list[str] = []
            bus.on_notification(lambda m: pushed.append(m))
            bus.subscribe_notifications()

            bus._handle_message(_notif_msg("events/q"))

            assert len(pushed) == 1
            assert isinstance(pushed[0], str)
            assert json.loads(pushed[0])["method"] == "events/q"
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Bounded queue + overflow policy (R2.4, R2.5; Property 6)
# ---------------------------------------------------------------------------

class TestOverflowPolicy:

    @pytest.mark.asyncio
    async def test_max_queue_bound_honoured(self):
        """A bounded subscriber never buffers more than ``max_queue`` items."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications(max_queue=2)
            for i in range(5):
                bus._handle_message(_notif_msg("events/m", {"i": i}))
            assert sub._queue.qsize() == 2
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_drop_discards_newest_for_affected_subscriber_only(self):
        """``overflow="drop"`` keeps the oldest and drops newest, isolating others."""
        bus, _ = await _make_running_bus()
        try:
            small = bus.subscribe_notifications(max_queue=2, overflow="drop")
            big = bus.subscribe_notifications(max_queue=100, overflow="drop")

            for i in range(5):
                bus._handle_message(_notif_msg("events/d", {"i": i}))

            # Affected subscriber kept the first two (newest discarded).
            small_items = await _take(small, 2)
            assert [i["params"]["i"] for i in small_items] == [0, 1]
            assert small._queue.empty()

            # The other subscriber was completely unaffected (Property 6).
            big_items = await _take(big, 5)
            assert [i["params"]["i"] for i in big_items] == [0, 1, 2, 3, 4]
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_close_terminates_only_affected_subscriber_and_drains(self):
        """``overflow="close"`` terminates only that subscriber, draining buffered items.

        On the overflow event the queue is full so the close sentinel cannot be
        enqueued; the buffered items must still drain before
        ``StopAsyncIteration`` (drain-then-stop).
        """
        bus, _ = await _make_running_bus()
        try:
            closing = bus.subscribe_notifications(max_queue=2, overflow="close")
            survivor = bus.subscribe_notifications(max_queue=100, overflow="drop")

            for i in range(5):
                bus._handle_message(_notif_msg("events/c", {"i": i}))

            # Closing subscriber was removed from the active set on overflow (R2.6).
            assert closing not in bus._subscriptions
            assert closing._closed is True

            # Drain-then-stop: buffered items (the first two) come out, then the
            # iterator terminates via StopAsyncIteration.
            drained = await asyncio.wait_for(_drain(closing), timeout=1.0)
            assert [i["params"]["i"] for i in drained] == [0, 1]

            # The survivor was untouched and still receives everything (Property 6).
            survivor_items = await _take(survivor, 5)
            assert [i["params"]["i"] for i in survivor_items] == [0, 1, 2, 3, 4]
            assert survivor in bus._subscriptions
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Teardown: close() / stop() / destroy() (R2.6, R2.7; Property 8)
# ---------------------------------------------------------------------------

class TestTeardown:

    @pytest.mark.asyncio
    async def test_explicit_close_removes_and_terminates_iterator(self):
        """close() removes the subscriber and unblocks a waiting __anext__."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()
            waiter = asyncio.ensure_future(sub.__anext__())
            await asyncio.sleep(0)  # let __anext__ block on an empty queue

            sub.close()

            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(waiter, timeout=1.0)
            assert sub not in bus._subscriptions
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        """Calling close() twice is safe and keeps the subscriber detached."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()
            sub.close()
            sub.close()  # must not raise
            assert sub not in bus._subscriptions
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_closed_subscriber_receives_no_further_notifications(self):
        """A closed subscriber is detached and ignores subsequent deliveries."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()
            sub.close()
            bus._handle_message(_notif_msg("events/after", {"v": 1}))
            # Only the close sentinel is queued; no notification leaked in.
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_stop_closes_all_subscribers(self):
        """stop() terminates every active iterator via StopAsyncIteration (R2.7)."""
        bus, _ = await _make_running_bus()
        try:
            sub1 = bus.subscribe_notifications()
            sub2 = bus.subscribe_notifications()
            w1 = asyncio.ensure_future(sub1.__anext__())
            w2 = asyncio.ensure_future(sub2.__anext__())
            await asyncio.sleep(0)

            await bus.stop()

            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(w1, timeout=1.0)
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(w2, timeout=1.0)
            assert len(bus._subscriptions) == 0
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_destroy_closes_all_subscribers(self):
        """destroy() terminates every active iterator via StopAsyncIteration (R2.7)."""
        bus, _ = await _make_running_bus()
        sub = bus.subscribe_notifications()
        waiter = asyncio.ensure_future(sub.__anext__())
        await asyncio.sleep(0)

        bus.destroy()

        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(waiter, timeout=1.0)
        assert len(bus._subscriptions) == 0

    @pytest.mark.asyncio
    async def test_stop_drains_buffered_then_stops(self):
        """stop() preserves buffered items: drain-then-stop on teardown (Property 8)."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()
            bus._handle_message(_notif_msg("events/buf", {"v": 1}))

            await bus.stop()

            drained = await asyncio.wait_for(_drain(sub), timeout=1.0)
            assert [i["method"] for i in drained] == ["events/buf"]
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Malformed input excluded from subscriptions (R2.8)
# ---------------------------------------------------------------------------

class TestMalformedExcluded:

    @pytest.mark.asyncio
    async def test_non_json_message_never_reaches_subscriptions(self):
        """Non-JSON input is discarded before notification dispatch (R2.8)."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()

            bus._handle_message("not json at all")
            bus._handle_message("{ broken json")
            assert sub._queue.empty()

            # A subsequent valid notification is still delivered normally.
            bus._handle_message(_notif_msg("events/ok", {"v": 1}))
            item = (await _take(sub, 1))[0]
            assert item["method"] == "events/ok"
        finally:
            bus.destroy()

    @pytest.mark.asyncio
    async def test_response_messages_not_delivered_as_notifications(self):
        """Responses (have an id) are not delivered to notification subscribers."""
        bus, _ = await _make_running_bus()
        try:
            sub = bus.subscribe_notifications()
            # A JSON-RPC response (has id) is not a notification.
            bus._handle_message(json.dumps({
                "jsonrpc": "2.0", "id": "unknown", "result": {"ok": True}
            }))
            assert sub._queue.empty()
        finally:
            bus.destroy()
