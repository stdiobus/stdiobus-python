"""Verification: Python code blocks from sdk/python/README.md
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
