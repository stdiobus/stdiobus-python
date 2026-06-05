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

"""Verification: Python code blocks from README.md
are copied here and executed.

If any test here fails, the README is lying to users.
"""

import json


# ============================================================================
# README § Quick Start (Async) — constructor portion
# ============================================================================
def test_readme_async_quick_start_constructor():
    from stdiobus import AsyncStdioBus, BusConfig, PoolConfig

    bus = AsyncStdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]
        )
    )
    assert bus.client_session_id.startswith("client-")
    bus.destroy()


# ============================================================================
# README § Quick Start (Sync) — constructor portion
# ============================================================================
def test_readme_sync_quick_start_constructor():
    from stdiobus import StdioBus, BusConfig, PoolConfig

    bus = StdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]
        )
    )
    bus.destroy()


# ============================================================================
# README § Configuration — Programmatic
# ============================================================================
def test_readme_programmatic_config():
    from stdiobus import BusConfig, PoolConfig, LimitsConfig

    config = BusConfig(
        pools=[
            PoolConfig(id="agent-a", command="python", args=["./worker_a.py"], instances=2),
            PoolConfig(id="agent-b", command="python", args=["-m", "worker_b"], instances=1),
        ],
        limits=LimitsConfig(
            max_input_buffer=2_097_152,
            max_restarts=10,
        ),
    )

    config.validate()
    data = json.loads(config.to_json())
    assert len(data["pools"]) == 2
    assert data["pools"][0]["id"] == "agent-a"
    assert data["limits"]["max_input_buffer"] == 2_097_152


# ============================================================================
# README § Configuration — File-based
# ============================================================================
def test_readme_file_config():
    import tempfile, os
    from stdiobus import StdioBus

    fd, path = tempfile.mkstemp(suffix='.json')
    os.write(fd, b'{"pools":[{"id":"w","command":"echo","instances":1}]}')
    os.close(fd)

    try:
        bus = StdioBus(config_path=path)
        bus.destroy()
    finally:
        os.unlink(path)


# ============================================================================
# README § Mutual exclusivity
# ============================================================================
def test_readme_mutual_exclusivity():
    import pytest
    from stdiobus import StdioBus, BusConfig, PoolConfig, InvalidArgumentError

    with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
        StdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)]),
            config_path='./config.json',
        )


def test_readme_no_config_source():
    import pytest
    from stdiobus import StdioBus, InvalidArgumentError

    with pytest.raises(InvalidArgumentError, match="config or config_path is required"):
        StdioBus()


# ============================================================================
# README § ACP agent flow — syntax verification
# ============================================================================
def test_readme_acp_agent_syntax():
    from stdiobus import (
        AsyncStdioBus, BusConfig, PoolConfig,
        HelloParams, RequestOptions, Identity, AuditEvent,
    )

    bus = AsyncStdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="acp-worker", command="python", args=["./acp_worker.py"], instances=1)]
        ),
        timeout_ms=60000,
    )

    # Verify request() accepts options parameter
    import inspect
    sig = inspect.signature(bus.request)
    params = list(sig.parameters.keys())
    assert 'method' in params
    assert 'options' in params

    # Verify types construct correctly
    hp = HelloParams()
    ro = RequestOptions(
        agent_id="agent-42",
        identity=Identity(subject_id="user-123", role="operator"),
        audit=AuditEvent(event_id="evt-1001", action="session/update"),
    )
    assert ro.agent_id == "agent-42"

    bus.destroy()


# ============================================================================
# README § Advanced: SubprocessOptions — syntax verification
# ============================================================================
def test_readme_subprocess_options():
    from stdiobus import StdioBus, BusConfig, PoolConfig, SubprocessOptions

    bus = StdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="w", command="echo", instances=1)]
        ),
        backend="subprocess",
        subprocess=SubprocessOptions(
            binary_path="/usr/local/bin/stdio_bus",
            start_timeout_sec=10.0,
        ),
    )
    bus.destroy()


# ============================================================================
# README § External Listener (Native Backend Only) — contract verification
#
# The listener requires native bindings (not built in CI), so this is a
# contract/syntax test, not a runnable start(): it proves the documented
# public surface (NativeOptions, ListenMode, the `native=` constructor
# parameter) and the documented validation rules exist as described.
# ============================================================================
def test_readme_native_listener_contract():
    import inspect
    import pytest
    from stdiobus import (
        AsyncStdioBus, BusConfig, PoolConfig,
        NativeOptions, ListenMode, InvalidArgumentError,
    )

    # NativeOptions / ListenMode are importable from the package root.
    assert ListenMode.TCP == "tcp"

    # AsyncStdioBus.__init__ exposes the documented `native=` parameter.
    sig = inspect.signature(AsyncStdioBus.__init__)
    assert "native" in sig.parameters

    # Documented validation: TCP requires tcp_port (1..65535).
    NativeOptions(listen_mode=ListenMode.TCP, tcp_host="127.0.0.1", tcp_port=8765).validate()
    with pytest.raises(ValueError):
        NativeOptions(listen_mode=ListenMode.TCP).validate()

    # Documented validation: UNIX requires unix_path.
    NativeOptions(listen_mode=ListenMode.UNIX, unix_path="/tmp/stdiobus.sock").validate()

    # Documented contract: a listener with a non-native backend is rejected.
    with pytest.raises(InvalidArgumentError, match="native-backend capability"):
        AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)]),
            backend="subprocess",
            native=NativeOptions(listen_mode=ListenMode.TCP, tcp_port=8765),
        )


# ============================================================================
# README § Streaming agent output — contract verification
#
# stream_request() is an async generator yielding StreamEvent objects. Without a
# real backend we verify the documented surface: the method exists, is an async
# generator, has the documented signature, and StreamEvent has the documented
# `type` / `text` / `result` shape used by the example (`event.type == "chunk"`
# / `event.result.get("text", ...)`).
# ============================================================================
def test_readme_streaming_agent_output_contract():
    import inspect
    from stdiobus import AsyncStdioBus, BusConfig, PoolConfig, StreamEvent

    bus = AsyncStdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="agent", command="python", args=["./agent_worker.py"], instances=1)]
        )
    )
    try:
        # The documented async-iterator entry point exists and is an async generator.
        assert hasattr(bus, "stream_request")
        assert inspect.isasyncgenfunction(AsyncStdioBus.stream_request)

        # Documented signature: method + params + keyword-only timeout_ms/session_id/options.
        params = inspect.signature(bus.stream_request).parameters
        assert "method" in params
        assert "params" in params
        assert "timeout_ms" in params
        assert "session_id" in params
        assert "options" in params

        # StreamEvent has the documented shape used by the README example.
        chunk = StreamEvent(type="chunk", text="Hello")
        assert chunk.type == "chunk"
        assert chunk.text == "Hello"
        result = StreamEvent(type="result", result={"text": "Hello world"})
        assert result.type == "result"
        assert result.result.get("text", "") == "Hello world"
    finally:
        bus.destroy()


# ============================================================================
# README § Streaming — async-only: sync StdioBus exposes no stream_request
# ============================================================================
def test_readme_sync_bus_has_no_stream_request():
    from stdiobus import StdioBus, BusConfig, PoolConfig

    bus = StdioBus(
        config=BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])
    )
    try:
        # The README states streaming is async-only; the sync wrapper must not
        # expose stream_request.
        assert not hasattr(bus, "stream_request")
    finally:
        bus.destroy()


# ============================================================================
# README § Subscribing to notifications — contract verification
#
# subscribe_notifications() returns an async iterator backed by a bounded queue.
# Without a real backend we verify the documented surface: the documented kwargs
# (max_queue / overflow) exist, the returned object is an async iterator, and the
# default-overflow value matches the README ("drop").
# ============================================================================
def test_readme_subscribe_notifications_contract():
    import inspect
    from stdiobus import AsyncStdioBus, BusConfig, PoolConfig, NotificationSubscription

    bus = AsyncStdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="agent", command="python", args=["./agent_worker.py"], instances=1)]
        )
    )
    try:
        assert hasattr(bus, "subscribe_notifications")

        # Documented keyword-only parameters and their defaults.
        sig = inspect.signature(bus.subscribe_notifications)
        assert "max_queue" in sig.parameters
        assert "overflow" in sig.parameters
        assert sig.parameters["max_queue"].default == 256
        assert sig.parameters["overflow"].default == "drop"

        # The returned object is an async iterator, as the README's
        # `async for notification in bus.subscribe_notifications(...)` requires.
        sub = bus.subscribe_notifications(max_queue=256, overflow="drop")
        assert isinstance(sub, NotificationSubscription)
        assert hasattr(sub, "__aiter__")
        assert hasattr(sub, "__anext__")
        assert sub.__aiter__() is sub
        sub.close()
    finally:
        bus.destroy()


# ============================================================================
# README § Subscriptions — async-only: sync StdioBus exposes no
# subscribe_notifications (but keeps on_notification)
# ============================================================================
def test_readme_sync_bus_has_no_subscribe_notifications():
    from stdiobus import StdioBus, BusConfig, PoolConfig

    bus = StdioBus(
        config=BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])
    )
    try:
        # Pull-based subscriptions are async-only.
        assert not hasattr(bus, "subscribe_notifications")
        # ...but the push callback remains available on the sync client.
        assert hasattr(bus, "on_notification")
    finally:
        bus.destroy()


# ============================================================================
# README § Builder — executable verification
#
# The builder is a thin fluent layer with no real backend involved, so the
# README snippet can be executed directly (minus the `async with`, which would
# spawn a worker). We verify fluent chaining, build()/build_sync() return types,
# and that the configured kwargs reach the constructor.
# ============================================================================
def test_readme_builder_example():
    from stdiobus import (
        StdioBusBuilder, AsyncStdioBus, StdioBus,
        BusConfig, PoolConfig, BackendMode,
    )

    builder = (
        StdioBusBuilder()
        .config(BusConfig(pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]))
        .backend("subprocess")
        .timeout_ms(15000)
    )
    # Fluent setters return the builder for chaining.
    assert builder.timeout_ms(15000) is builder

    bus = builder.build()
    try:
        assert isinstance(bus, AsyncStdioBus)
        assert bus._backend_mode == BackendMode.SUBPROCESS
        assert bus._timeout_ms == 15000
    finally:
        bus.destroy()

    # build_sync() produces a working synchronous StdioBus from the same kwargs.
    sync_bus = builder.build_sync()
    try:
        assert isinstance(sync_bus, StdioBus)
        assert sync_bus.get_backend_type() == "subprocess"
    finally:
        sync_bus.destroy()


# ============================================================================
# README § API Reference — StdioBusBuilder is part of the documented public API
# ============================================================================
def test_readme_builder_exported():
    import stdiobus

    assert "StdioBusBuilder" in stdiobus.__all__
    assert hasattr(stdiobus, "StdioBusBuilder")
