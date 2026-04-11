# Changelog

## 2.1.0 (2026-04-09)

### Added
- **Streaming support** — `agent_message_chunk` notification aggregation into `result.text` (ACP protocol parity with Rust SDK)
- **Cancellation semantics** — `stop()` fails all pending requests with `TransportError("Bus is shutting down")`
- **`on_notification()` handler** — subscribe to notifications separately from raw messages
- **Cross-SDK contract tests** — wire-format parity verification (sessionId, agentId, _ext, config schema, streaming, cancellation)
- **CI pipeline** — GitHub Actions workflow for unit, E2E, real-binary, and Docker tests
- **`_PendingRequest` internal class** — tracks pending requests with streaming chunk aggregation

### Changed
- `_handle_message` now dispatches notifications separately and aggregates streaming chunks
- `stop()` now cancels all in-flight requests before stopping backend
- Backend crash (`_on_backend_closed`) uses `_PendingRequest` wrapper

## 2.0.0 (2026-04-09)

### Breaking Changes
- Version bump to 2.0.0 — API additions, no removals
- `BackendMode.SUBPROCESS` is now the default for auto-selection when `stdio_bus` binary is in PATH

### Added
- **SubprocessBackend** — spawns `stdio_bus` binary, communicates via stdin/stdout NDJSON pipes
- **`--config-fd 3` support** — pipe-based config delivery for programmatic config (no temp files)
- **Hello handshake** — `hello()` and `connect()` methods for `stdio_bus/hello` protocol negotiation
- **Extensions support** — `_ext.identity`, `_ext.audit` in JSON-RPC messages
- **Auto `clientSessionId`** — generated at creation, injected into every outbound message
- **`agentId` routing** — for registry-launcher agent selection
- **New types**: `HelloParams`, `HelloResult`, `Identity`, `AuditEvent`, `RequestOptions`, `SubprocessOptions`, `ExtensionInfo`
- **`BackendMode.SUBPROCESS`** enum value
- **Stderr ring buffer** — last 200 lines captured for error diagnostics
- **Orphan process cleanup** — atexit handler kills leaked stdio_bus processes
- **Graceful shutdown sequence** — close stdin → drain timeout → SIGTERM → SIGKILL

### Changed
- Auto backend selection: subprocess → native → docker (was: native → docker)
- `request()` now accepts `options: RequestOptions` for identity/audit/agentId
- `notify()` now accepts `options: RequestOptions`
- `get_backend_type()` returns `"subprocess"` for new backend

### Fixed
- Version mismatch between `__init__.py` and `pyproject.toml`

## 1.0.2 (2026-03-15)

### Added
- `BusConfig` / `PoolConfig` / `LimitsConfig` for programmatic configuration
- Config validation with descriptive error messages
- `config` parameter (mutually exclusive with `config_path`)

## 1.0.1 (2026-03-01)

### Fixed
- Docker backend startup timeout handling

## 1.0.0 (2026-02-15)

### Added
- Initial release
- `StdioBus` and `AsyncStdioBus` clients
- Docker backend
- Native backend (cffi)
- Error hierarchy with canonical error codes
- Type hints and py.typed marker
