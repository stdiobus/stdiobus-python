"""Basic tests for stdiobus Python SDK."""

import json
import os
import tempfile
import pytest

from stdiobus import (
    StdioBus,
    AsyncStdioBus,
    BusState,
    BackendMode,
    StdioBusError,
    InvalidArgumentError,
)


class TestImports:
    """Test that all exports are available."""

    def test_main_classes(self):
        from stdiobus import StdioBus, AsyncStdioBus
        assert StdioBus is not None
        assert AsyncStdioBus is not None

    def test_types(self):
        from stdiobus import BusState, BackendMode, BusStats
        assert BusState.RUNNING == 2
        assert BackendMode.AUTO == "auto"
        assert BackendMode.SUBPROCESS == "subprocess"

    def test_errors(self):
        from stdiobus import (
            StdioBusError,
            InvalidArgumentError,
            TimeoutError,
            TransportError,
        )
        assert issubclass(InvalidArgumentError, StdioBusError)

    def test_protocol_types(self):
        from stdiobus import (
            HelloParams,
            HelloResult,
            Identity,
            AuditEvent,
            RequestOptions,
            SubprocessOptions,
        )
        assert HelloParams is not None
        assert HelloResult is not None
        assert Identity is not None
        assert AuditEvent is not None
        assert RequestOptions is not None
        assert SubprocessOptions is not None


class TestBusState:
    """Test BusState enum."""

    def test_values(self):
        assert BusState.CREATED == 0
        assert BusState.STARTING == 1
        assert BusState.RUNNING == 2
        assert BusState.STOPPING == 3
        assert BusState.STOPPED == 4


class TestBackendMode:
    """Test BackendMode enum."""

    def test_values(self):
        assert BackendMode.AUTO == "auto"
        assert BackendMode.NATIVE == "native"
        assert BackendMode.DOCKER == "docker"
        assert BackendMode.SUBPROCESS == "subprocess"


class TestStdioBusCreation:
    """Test StdioBus instance creation."""

    def test_requires_config_source(self):
        with pytest.raises(InvalidArgumentError, match="config or config_path is required"):
            StdioBus()

    def test_mutual_exclusivity(self):
        from stdiobus import BusConfig, PoolConfig
        with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
            StdioBus(
                config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)]),
                config_path='./config.json',
            )

    def test_config_validation_empty_pools(self):
        from stdiobus import BusConfig
        with pytest.raises(ValueError, match="at least one pool"):
            StdioBus(config=BusConfig(pools=[]))

    def test_config_validation_zero_instances(self):
        from stdiobus import BusConfig, PoolConfig
        with pytest.raises(ValueError, match="instances"):
            StdioBus(config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=0)]))

    def test_native_not_available(self):
        """Test that native backend raises appropriate error when not built."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"pools": []}, f)
            config_path = f.name

        try:
            with pytest.raises(InvalidArgumentError, match="Native backend not available"):
                StdioBus(config_path=config_path, backend="native")
        finally:
            os.unlink(config_path)


class TestErrors:
    """Test error classes."""

    def test_error_code(self):
        from stdiobus.errors import ErrorCode, InvalidArgumentError

        err = InvalidArgumentError("test message")
        assert err.code == ErrorCode.INVALID_ARGUMENT
        assert err.message == "test message"

    def test_error_to_dict(self):
        from stdiobus.errors import TimeoutError

        err = TimeoutError("request timed out", details={"method": "test"})
        d = err.to_dict()

        assert d["code"] == 3
        assert d["message"] == "request timed out"
        assert d["details"] == {"method": "test"}

    def test_error_from_code(self):
        from stdiobus.errors import error_from_code, TimeoutError

        err = error_from_code(3, "timeout")
        assert isinstance(err, TimeoutError)


class TestBusConfig:
    """Test BusConfig types and serialization."""

    def test_basic_config(self):
        from stdiobus import BusConfig, PoolConfig
        config = BusConfig(
            pools=[PoolConfig(id='worker', command='node', args=['worker.js'], instances=4)]
        )
        config.validate()
        json_str = config.to_json()
        assert '"id": "worker"' in json_str or '"id":"worker"' in json_str
        assert '"instances": 4' in json_str or '"instances":4' in json_str

    def test_config_with_limits(self):
        from stdiobus import BusConfig, PoolConfig, LimitsConfig
        config = BusConfig(
            pools=[PoolConfig(id='w', command='node', args=[], instances=2)],
            limits=LimitsConfig(max_input_buffer=2097152, max_restarts=10),
        )
        config.validate()
        json_str = config.to_json()
        data = json.loads(json_str)
        assert data['limits']['max_input_buffer'] == 2097152
        assert data['limits']['max_restarts'] == 10
        assert 'drain_timeout_sec' not in data['limits']

    def test_config_roundtrip(self):
        from stdiobus import BusConfig, PoolConfig, LimitsConfig
        config = BusConfig(
            pools=[PoolConfig(id='echo', command='/bin/cat', args=['--flag'], instances=3)],
            limits=LimitsConfig(max_restarts=7, drain_timeout_sec=15),
        )
        data = json.loads(config.to_json())
        assert data['pools'][0]['id'] == 'echo'
        assert data['pools'][0]['command'] == '/bin/cat'
        assert data['pools'][0]['args'] == ['--flag']
        assert data['pools'][0]['instances'] == 3
        assert data['limits']['max_restarts'] == 7
        assert data['limits']['drain_timeout_sec'] == 15

    def test_validation_empty_pools(self):
        from stdiobus import BusConfig
        with pytest.raises(ValueError, match="pool"):
            BusConfig(pools=[]).validate()

    def test_validation_missing_id(self):
        from stdiobus import BusConfig, PoolConfig
        with pytest.raises(ValueError, match="id"):
            BusConfig(pools=[PoolConfig(id='', command='echo', instances=1)]).validate()

    def test_validation_zero_instances(self):
        from stdiobus import BusConfig, PoolConfig
        with pytest.raises(ValueError, match="instances"):
            BusConfig(pools=[PoolConfig(id='w', command='echo', instances=0)]).validate()


class TestClientSessionId:
    """Test automatic client session ID generation."""

    def test_session_id_generated(self):
        from stdiobus import BusConfig, PoolConfig
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )
        assert bus.client_session_id.startswith("client-")
        bus.destroy()

    def test_session_id_unique(self):
        from stdiobus import BusConfig, PoolConfig
        cfg = BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        bus1 = AsyncStdioBus(config=cfg)
        bus2 = AsyncStdioBus(config=cfg)
        assert bus1.client_session_id != bus2.client_session_id
        bus1.destroy()
        bus2.destroy()

    def test_agent_session_id_initially_none(self):
        from stdiobus import BusConfig, PoolConfig
        bus = AsyncStdioBus(
            config=BusConfig(pools=[PoolConfig(id='w', command='echo', instances=1)])
        )
        assert bus.agent_session_id is None
        bus.destroy()


class TestProtocolTypes:
    """Test protocol types serialization."""

    def test_hello_params_to_dict(self):
        from stdiobus import HelloParams
        from stdiobus.types import ExtensionInfo
        params = HelloParams(
            protocol_version="0.1.0",
            extensions={"identity": ExtensionInfo(version="0.1.0", required=True)},
        )
        d = params.to_dict()
        assert d["protocolVersion"] == "0.1.0"
        assert d["extensions"]["identity"]["version"] == "0.1.0"
        assert d["extensions"]["identity"]["required"] is True

    def test_hello_result_from_dict(self):
        from stdiobus import HelloResult
        data = {
            "negotiatedProtocolVersion": "0.1.0",
            "sessionId": "sess-123",
            "extensions": {
                "identity": {"selected": "0.1.0", "active": True}
            },
        }
        result = HelloResult.from_dict(data)
        assert result.negotiated_protocol_version == "0.1.0"
        assert result.session_id == "sess-123"
        assert result.extensions["identity"].active is True

    def test_identity_to_dict(self):
        from stdiobus import Identity
        ident = Identity(subject_id="user-1", role="admin", asserted_by="bus")
        d = ident.to_dict()
        assert d["subjectId"] == "user-1"
        assert d["role"] == "admin"
        assert d["assertedBy"] == "bus"

    def test_audit_event_to_dict(self):
        from stdiobus import AuditEvent
        event = AuditEvent(
            event_id="evt-1",
            action="tools/call",
            outcome="success",
        )
        d = event.to_dict()
        assert d["eventId"] == "evt-1"
        assert d["action"] == "tools/call"
        assert d["outcome"] == "success"
        assert "parentEventId" not in d  # optional, not set

    def test_request_options_defaults(self):
        from stdiobus import RequestOptions
        opts = RequestOptions()
        assert opts.timeout_ms is None
        assert opts.session_id is None
        assert opts.agent_id is None
        assert opts.identity is None
        assert opts.audit is None
