#!/usr/bin/env python3
"""
Real E2E test: Python SDK → stdio_bus binary → Node.js echo worker.

This test uses:
- Real stdio_bus binary (build/stdio_bus)
- Real Node.js echo worker (tests/real_echo_worker.js)
- Programmatic BusConfig (--config-fd 3, no temp files)
- Full request/response cycle through the real transport

Usage:
    python tests/real_e2e_test.py
"""

import asyncio
import json
import logging
import os
import sys
import time

# Setup logging to see stdio_bus stderr output
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# Add SDK to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stdiobus import (
    AsyncStdioBus,
    BusConfig,
    PoolConfig,
    LimitsConfig,
    SubprocessOptions,
    RequestOptions,
    Identity,
    AuditEvent,
)

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SDK_PYTHON_DIR = os.path.dirname(SCRIPT_DIR)
SDK_DIR = os.path.dirname(SDK_PYTHON_DIR)
PROJECT_ROOT = os.path.dirname(SDK_DIR)
BINARY = os.path.join(PROJECT_ROOT, "build", "stdio_bus")
WORKER = os.path.join(SCRIPT_DIR, "real_echo_worker.py")


async def main():
    print("=" * 70)
    print("REAL E2E TEST: Python SDK → stdio_bus → Python worker")
    print("=" * 70)
    print(f"Binary:  {BINARY}")
    print(f"Worker:  {WORKER}")
    print()

    if not os.path.exists(BINARY):
        print(f"ERROR: stdio_bus binary not found at {BINARY}")
        print("Run 'make' from project root first.")
        sys.exit(1)

    if not os.path.exists(WORKER):
        print(f"ERROR: Worker not found at {WORKER}")
        sys.exit(1)

    # ---------------------------------------------------------------
    # 1. Create bus with PROGRAMMATIC config (no JSON file!)
    # ---------------------------------------------------------------
    print("1. Creating bus with programmatic BusConfig...")
    config = BusConfig(
        pools=[PoolConfig(
            id="echo",
            command=sys.executable,
            args=[WORKER],
            instances=1,
        )],
        limits=LimitsConfig(
            max_input_buffer=1048576,
            max_restarts=3,
        ),
    )
    print(f"   Config JSON: {config.to_json()}")

    bus = AsyncStdioBus(
        config=config,
        backend="subprocess",
        subprocess=SubprocessOptions(
            binary_path=BINARY,
            start_timeout_sec=5.0,
            drain_timeout_sec=10.0,
        ),
        timeout_ms=10000,
    )
    print(f"   Client session ID: {bus.client_session_id}")
    print(f"   Backend type: {bus.get_backend_type()}")
    print()

    # ---------------------------------------------------------------
    # 2. Start the bus
    # ---------------------------------------------------------------
    print("2. Starting bus (spawning stdio_bus + worker)...")
    await bus.start()
    print(f"   State: {bus.get_state().name}")
    print(f"   Running: {bus.is_running()}")
    print()

    # Give workers a moment to fully initialize
    await asyncio.sleep(0.5)

    # ---------------------------------------------------------------
    # 3. Simple request/response
    # ---------------------------------------------------------------
    print("3. Sending simple request: echo({'message': 'hello from Python SDK'})...")
    t0 = time.monotonic()
    result = await bus.request("echo", {"message": "hello from Python SDK"})
    elapsed = (time.monotonic() - t0) * 1000
    print(f"   Response ({elapsed:.1f}ms): {json.dumps(result, indent=2)}")
    assert result["echo"]["message"] == "hello from Python SDK", "Echo mismatch!"
    assert result["method"] == "echo", "Method mismatch!"
    print("   ✓ Simple request/response OK")
    print()

    # ---------------------------------------------------------------
    # 4. Verify sessionId routing
    # ---------------------------------------------------------------
    print("4. Verifying sessionId auto-injection...")
    result = await bus.request("check_session", {"test": True})
    received_sid = result.get("receivedSessionId")
    print(f"   Sent sessionId: {bus.client_session_id}")
    print(f"   Worker received: {received_sid}")
    assert received_sid == bus.client_session_id, "SessionId not routed!"
    print("   ✓ SessionId routing OK")
    print()

    # ---------------------------------------------------------------
    # 5. Request with extensions (Identity + Audit)
    # ---------------------------------------------------------------
    print("5. Sending request with Identity + Audit extensions...")
    result = await bus.request(
        "tools/call",
        {"name": "search", "arguments": {"query": "test"}},
        options=RequestOptions(
            identity=Identity(subject_id="user-42", role="admin", asserted_by="bus"),
            audit=AuditEvent(event_id="evt-001", action="tools/call", outcome="pending"),
            agent_id="agent-abc",
        ),
    )
    print(f"   Response: {json.dumps(result, indent=2)}")
    print("   ✓ Extensions request OK")
    print()

    # ---------------------------------------------------------------
    # 6. Multiple concurrent requests
    # ---------------------------------------------------------------
    print("6. Sending 5 concurrent requests...")
    t0 = time.monotonic()
    results = await asyncio.gather(*[
        bus.request(f"concurrent/{i}", {"index": i})
        for i in range(5)
    ])
    elapsed = (time.monotonic() - t0) * 1000
    methods = [r["method"] for r in results]
    print(f"   Got {len(results)} responses in {elapsed:.1f}ms")
    print(f"   Methods: {methods}")
    assert len(results) == 5, "Missing responses!"
    print("   ✓ Concurrent requests OK")
    print()

    # ---------------------------------------------------------------
    # 7. Stats
    # ---------------------------------------------------------------
    print("7. Checking stats...")
    stats = bus.get_stats()
    print(f"   Messages in:  {stats.messages_in}")
    print(f"   Messages out: {stats.messages_out}")
    print(f"   Bytes in:     {stats.bytes_in}")
    print(f"   Bytes out:    {stats.bytes_out}")
    assert stats.messages_in >= 7, f"Expected >= 7 messages in, got {stats.messages_in}"
    assert stats.messages_out >= 7, f"Expected >= 7 messages out, got {stats.messages_out}"
    print("   ✓ Stats OK")
    print()

    # ---------------------------------------------------------------
    # 8. Stderr capture
    # ---------------------------------------------------------------
    print("8. Checking stderr capture (stdio_bus logs)...")
    from stdiobus.backends.subprocess import SubprocessBackend
    if isinstance(bus._backend, SubprocessBackend):
        stderr_tail = bus._backend.get_stderr_tail(10)
        if stderr_tail:
            print(f"   Last stderr lines:")
            for line in stderr_tail.split("\n"):
                print(f"     | {line}")
        else:
            print("   (no stderr output captured)")
    print()

    # ---------------------------------------------------------------
    # 9. Graceful shutdown
    # ---------------------------------------------------------------
    print("9. Stopping bus (graceful shutdown)...")
    await bus.stop(timeout_sec=5.0)
    print(f"   State: {bus.get_state().name}")
    bus.destroy()
    print("   ✓ Shutdown OK")
    print()

    print("=" * 70)
    print("ALL REAL E2E TESTS PASSED ✓")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
