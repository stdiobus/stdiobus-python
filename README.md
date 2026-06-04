<h1 align="center" style="font-weight:500">
    <strong>stdio Bus Python SDK for AI Agent Transport</strong>
</h1>

<p align="center">
    Python SDK for building AI agents over stdio_bus.
</p>

<p align="center">
  <a href="https://pypi.org/project/stdiobus/"><img src="https://img.shields.io/pypi/v/stdiobus?style=for-the-badge&logo=pypi&logoColor=white" alt="PyPI" /></a>
  <a href="https://github.com/stdiobus"><img src="https://img.shields.io/badge/ecosystem-stdio%20Bus-ff4500?style=for-the-badge" alt="stdioBus" /></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/protocol-MCP-purple?style=for-the-badge&logo=jsonwebtokens" alt="MCP"></a>
  <a href="https://agentclientprotocol.com"><img src="https://img.shields.io/badge/protocol-ACP-purple?style=for-the-badge&logo=jsonwebtokens" alt="ACP"></a>
  <a href="https://www.python.org"><img src="https://img.shields.io/badge/python-%3E%3D3.10-brightgreen?style=for-the-badge&logo=python&logoColor=white" alt="Python" /></a>
  <a href="https://github.com/stdiobus/stdiobus-cpp"><img src="https://img.shields.io/badge/arch-x86__64%20%7C%20arm64-blue?style=for-the-badge" alt="Architecture"></a>
  <a href="https://hub.docker.com/r/stdiobus/stdiobus"><img src="https://img.shields.io/badge/docker-Windows%20fallback-blue?style=for-the-badge&logo=docker" alt="Docker" /></a>
  <a href="https://setuptools.pypa.io"><img src="https://img.shields.io/badge/build-setuptools-yellow?style=for-the-badge&logo=pypi" alt="Build" /></a>
  <a href="https://github.com/stdiobus/stdiobus-python/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue?style=for-the-badge&logo=opensourceinitiative" alt="License" /></a>
  <a href="https://mypy-lang.org"><img src="https://img.shields.io/badge/mypy-strict-blue?style=for-the-badge&logo=python" alt="Typing" /></a>
  <a href="https://github.com/stdiobus/stdiobus-python"><img src="https://img.shields.io/badge/status-stable-brightgreen?style=for-the-badge" alt="Stable" /></a>
</p>

stdiobus gives you a reliable transport layer for ACP/MCP-style workflows:
route requests to agent workers, keep request/response correlation stable,
handle streaming updates, and work with async or sync Python code.

Use it when you want to focus on agent logic, not process wiring and message transport.

## Why use stdiobus

- Build ACP/MCP agents without writing transport plumbing.
- Send typed JSON-RPC requests with automatic session routing.
- Receive streamed agent output as a final aggregated text.
- Use the same protocol model across Python, Node, and Rust SDKs.
- Run locally with a binary, or via Docker when needed.

## Features

- Simple client API: `AsyncStdioBus` and `StdioBus`.
- Programmatic config: define worker pools in Python or use a config file.
- Session routing by default: `clientSessionId` is injected automatically.
- Hello handshake: `stdio_bus/hello` negotiation support.
- Protocol extensions: identity, audit metadata, and `agentId` routing.
- Streaming support: `agent_message_chunk` aggregation into final response text.
- Predictable cancellation: in-flight requests fail with `TransportError` on shutdown or crash.
- Cross-platform: subprocess backend with Docker fallback.
- Typed API: dataclasses and type hints for IDE support.

## Installation

```bash
pip install stdiobus
```

Requirements:

- Python 3.10+
- `stdio_bus` binary in PATH (or Docker)

## Quick Start (Async)

```python
import asyncio
from stdiobus import AsyncStdioBus, BusConfig, PoolConfig


async def main():
    async with AsyncStdioBus(
            config=BusConfig(
                pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]
            )
    ) as bus:
        result = await bus.request("echo", {"message": "hello"})
        print(result)


asyncio.run(main())
```

## Quick Start (Sync)

```python
from stdiobus import StdioBus, BusConfig, PoolConfig

with StdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]
        )
) as bus:
    result = bus.request("echo", {"message": "hello"})
    print(result)
```

## Real Use Cases

### ACP agent flow

```python
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

await bus.start()

# Optional protocol handshake
hello = await bus.hello(HelloParams())
print("Negotiated:", hello.negotiated_protocol_version)

# Request with identity/audit metadata + agent routing
result = await bus.request(
    "session/update",
    {"input": "Summarize latest incident report"},
    options=RequestOptions(
        agent_id="agent-42",
        identity=Identity(subject_id="user-123", role="operator"),
        audit=AuditEvent(event_id="evt-1001", action="session/update"),
    ),
)

# If stream chunks were received, result["text"] contains aggregated output
print(result.get("text", result))

await bus.stop()
bus.destroy()
```

### MCP tools call

```python
from stdiobus import AsyncStdioBus, BusConfig, PoolConfig

async with AsyncStdioBus(
        config=BusConfig(
            pools=[PoolConfig(id="mcp-tools", command="python", args=["-m", "my_tools_worker"], instances=2)]
        )
) as bus:
    tools = await bus.request("tools/list")
    print("Tools:", tools)

    output = await bus.request("tools/call", {
        "name": "search_docs",
        "arguments": {"query": "retry policy"},
    })
    print(output)
```

## Configuration

### Programmatic (recommended)

```python
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
```

### File-based (legacy)

```python
from stdiobus import StdioBus

bus = StdioBus(config_path="./stdio-bus-config.json")
```

`config` and `config_path` are mutually exclusive.

## API Reference

### Main classes

- `AsyncStdioBus(config=..., config_path=..., backend="auto", timeout_ms=...)`
- `StdioBus(...)` — sync wrapper

### Lifecycle

| Method                 | Description                                |
|------------------------|--------------------------------------------|
| `start()`              | Start the bus and spawn workers            |
| `stop(timeout_sec=30)` | Stop gracefully, cancel in-flight requests |
| `connect(params)`      | Start + optional hello handshake           |
| `hello(params)`        | Perform stdio_bus/hello handshake          |
| `destroy()`            | Release all resources                      |

### Messaging

| Method                         | Description                               |
|--------------------------------|-------------------------------------------|
| `request(method, params, ...)` | Send request and wait for response        |
| `notify(method, params, ...)`  | Send notification (no response)           |
| `send(message)`                | Send raw JSON-RPC message                 |
| `on_message(handler)`          | Register handler for all inbound messages |
| `on_notification(handler)`     | Register handler for notifications only   |

### Properties

| Property             | Description                                |
|----------------------|--------------------------------------------|
| `client_session_id`  | Auto-generated routing session ID          |
| `agent_session_id`   | Agent-returned session ID (after hello)    |
| `get_state()`        | Current bus state                          |
| `get_stats()`        | Runtime statistics                         |
| `get_backend_type()` | Active backend: subprocess, native, docker |
| `get_listen_mode()`  | Effective external listener mode (native only; `none` otherwise) |
| `get_worker_count()` | Running workers, or `-1` if the backend cannot report it |
| `get_client_count()` | Connected clients, or `-1` if the backend cannot report it |

> `-1` is a deliberate "not introspectable" sentinel. The subprocess and Docker
> backends have no channel to count daemon workers, so they return `-1` rather
> than a misleading `0`. For Docker, `get_client_count()` reports whether this
> SDK is connected to the container (`0`/`1`).

### Protocol types

`HelloParams`, `HelloResult`, `RequestOptions`, `Identity`, `AuditEvent`,
`BusConfig`, `PoolConfig`, `LimitsConfig`, `SubprocessOptions`, `NativeOptions`,
`ListenMode`

### Errors

| Exception              | When                                  |
|------------------------|---------------------------------------|
| `InvalidArgumentError` | Bad parameter or config               |
| `InvalidStateError`    | Operation not valid in current state  |
| `TimeoutError`         | Request exceeded deadline             |
| `TransportError`       | Transport failure, shutdown, or crash |
| `PolicyDeniedError`    | Operation denied by policy            |

## Known Behavior

- No automatic reconnect. If the bus process exits, pending requests fail with `TransportError`. Create a new instance
  to reconnect.
- `stop()` cancels all in-flight requests with `TransportError` before stopping the backend.
- Streaming chunks (`agent_message_chunk`) are aggregated into `result["text"]` when the response result is a dict.
- `stdout` from the bus process is expected to carry NDJSON protocol messages only.

## Advanced: Backend Details

For most users, `backend="auto"` is the right choice. Details for those who need control:

| Backend      | When                               | Config delivery           |
|--------------|------------------------------------|---------------------------|
| `subprocess` | stdio_bus binary in PATH (default) | `--config-fd <N>` pipe    |
| `native`     | libstdio_bus.a built with cffi     | embed API (in-process)    |
| `docker`     | Docker available                   | `--config <mounted-file>` |

Auto-selection: subprocess → native → docker (Unix), subprocess → docker (Windows).

```python
from stdiobus import StdioBus, SubprocessOptions

bus = StdioBus(
    config=config,
    backend="subprocess",
    subprocess=SubprocessOptions(
        binary_path="/usr/local/bin/stdio_bus",
        start_timeout_sec=10.0,
    ),
)
```

### External Listener (Native Backend Only)

By default the bus runs embedded: messages flow through `request()`/`send()` and
`on_message()`. The native backend can instead open an external listener (TCP or
Unix socket) so that other processes connect and speak NDJSON directly.

This requires the native cffi bindings to be built
(`python -m stdiobus._native.build_ffi`). The subprocess and Docker backends do
not expose a user-controlled listener — passing a non-`none` `listen_mode` with
`backend="subprocess"` or `backend="docker"` raises `InvalidArgumentError`.

```python
from stdiobus import AsyncStdioBus, BusConfig, PoolConfig, NativeOptions, ListenMode

bus = AsyncStdioBus(
    config=BusConfig(
        pools=[PoolConfig(id="echo", command="python", args=["./echo_worker.py"], instances=1)]
    ),
    backend="native",
    native=NativeOptions(
        listen_mode=ListenMode.TCP,
        tcp_host="127.0.0.1",
        tcp_port=8765,
    ),
)
```

`NativeOptions` validates its own arguments: `ListenMode.TCP` requires `tcp_port`
(in `1..65535`) and `ListenMode.UNIX` requires `unix_path`.

## Development

```bash
pip install -e ".[dev]"
pytest -v
```

## License

Apache-2.0
