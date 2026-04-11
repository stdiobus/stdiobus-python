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

"""Full coverage tests for types.py — all dataclasses, enums, serialization."""

import json
import pytest

from stdiobus.types import (
    BusState,
    BackendMode,
    ListenMode,
    BusStats,
    DockerOptions,
    BusOptions,
    PoolConfig,
    LimitsConfig,
    BusConfig,
    SubprocessOptions,
    ExtensionInfo,
    HelloParams,
    HelloResult,
    Identity,
    AuditEvent,
    RequestOptions,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TestListenMode:
    def test_values(self):
        assert ListenMode.NONE == "none"
        assert ListenMode.TCP == "tcp"
        assert ListenMode.UNIX == "unix"


# ---------------------------------------------------------------------------
# BusStats
# ---------------------------------------------------------------------------

class TestBusStats:
    def test_defaults(self):
        s = BusStats()
        assert s.messages_in == 0
        assert s.messages_out == 0
        assert s.bytes_in == 0
        assert s.bytes_out == 0
        assert s.worker_restarts == 0
        assert s.routing_errors == 0
        assert s.client_connects == 0
        assert s.client_disconnects == 0

    def test_custom_values(self):
        s = BusStats(messages_in=10, bytes_out=500)
        assert s.messages_in == 10
        assert s.bytes_out == 500


# ---------------------------------------------------------------------------
# DockerOptions
# ---------------------------------------------------------------------------

class TestDockerOptions:
    def test_defaults(self):
        d = DockerOptions()
        assert d.image == "stdiobus/stdiobus:node20"
        assert d.pull_policy == "if-missing"
        assert d.engine_path == "docker"
        assert d.startup_timeout_sec == 15.0
        assert d.extra_args == []
        assert d.env == {}


# ---------------------------------------------------------------------------
# PoolConfig
# ---------------------------------------------------------------------------

class TestPoolConfig:
    def test_defaults(self):
        p = PoolConfig(id="w", command="echo")
        assert p.args == []
        assert p.instances == 1

    def test_custom(self):
        p = PoolConfig(id="w", command="node", args=["a.js", "-v"], instances=4)
        assert p.args == ["a.js", "-v"]
        assert p.instances == 4


# ---------------------------------------------------------------------------
# LimitsConfig
# ---------------------------------------------------------------------------

class TestLimitsConfig:
    def test_all_none_by_default(self):
        lc = LimitsConfig()
        assert lc.max_input_buffer is None
        assert lc.max_output_queue is None
        assert lc.max_restarts is None
        assert lc.restart_window_sec is None
        assert lc.drain_timeout_sec is None
        assert lc.backpressure_timeout_sec is None

    def test_all_fields_set(self):
        lc = LimitsConfig(
            max_input_buffer=1024,
            max_output_queue=2048,
            max_restarts=3,
            restart_window_sec=60,
            drain_timeout_sec=30,
            backpressure_timeout_sec=120,
        )
        assert lc.max_input_buffer == 1024
        assert lc.backpressure_timeout_sec == 120


# ---------------------------------------------------------------------------
# BusConfig
# ---------------------------------------------------------------------------

class TestBusConfigFull:

    def test_validate_missing_command(self):
        with pytest.raises(ValueError, match="missing command"):
            BusConfig(pools=[PoolConfig(id="w", command="")]).validate()

    def test_validate_multiple_pools(self):
        cfg = BusConfig(pools=[
            PoolConfig(id="a", command="echo", instances=1),
            PoolConfig(id="b", command="cat", instances=2),
        ])
        cfg.validate()  # should not raise

    def test_to_json_no_limits(self):
        cfg = BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])
        data = json.loads(cfg.to_json())
        assert "limits" not in data
        assert len(data["pools"]) == 1

    def test_to_json_limits_all_none_omitted(self):
        cfg = BusConfig(
            pools=[PoolConfig(id="w", command="echo", instances=1)],
            limits=LimitsConfig(),
        )
        data = json.loads(cfg.to_json())
        assert "limits" not in data  # all None → omitted

    def test_to_json_limits_partial(self):
        cfg = BusConfig(
            pools=[PoolConfig(id="w", command="echo", instances=1)],
            limits=LimitsConfig(max_restarts=5, drain_timeout_sec=10),
        )
        data = json.loads(cfg.to_json())
        assert data["limits"] == {"max_restarts": 5, "drain_timeout_sec": 10}

    def test_to_json_limits_all_fields(self):
        cfg = BusConfig(
            pools=[PoolConfig(id="w", command="echo", instances=1)],
            limits=LimitsConfig(
                max_input_buffer=1, max_output_queue=2, max_restarts=3,
                restart_window_sec=4, drain_timeout_sec=5, backpressure_timeout_sec=6,
            ),
        )
        data = json.loads(cfg.to_json())
        assert len(data["limits"]) == 6

    def test_to_json_multiple_pools(self):
        cfg = BusConfig(pools=[
            PoolConfig(id="a", command="echo", args=["--flag"], instances=2),
            PoolConfig(id="b", command="cat", instances=1),
        ])
        data = json.loads(cfg.to_json())
        assert len(data["pools"]) == 2
        assert data["pools"][0]["id"] == "a"
        assert data["pools"][1]["id"] == "b"


# ---------------------------------------------------------------------------
# SubprocessOptions
# ---------------------------------------------------------------------------

class TestSubprocessOptionsFull:
    def test_env_default_empty(self):
        opts = SubprocessOptions()
        assert opts.env == {}

    def test_env_custom(self):
        opts = SubprocessOptions(env={"A": "1", "B": "2"})
        assert opts.env["A"] == "1"


# ---------------------------------------------------------------------------
# ExtensionInfo
# ---------------------------------------------------------------------------

class TestExtensionInfo:
    def test_defaults(self):
        e = ExtensionInfo(version="0.1.0")
        assert e.version == "0.1.0"
        assert e.required is False
        assert e.active is False

    def test_custom(self):
        e = ExtensionInfo(version="1.0", required=True, active=True)
        assert e.required is True
        assert e.active is True


# ---------------------------------------------------------------------------
# HelloParams
# ---------------------------------------------------------------------------

class TestHelloParamsFull:
    def test_defaults(self):
        hp = HelloParams()
        d = hp.to_dict()
        assert d["protocolVersion"] == "0.1.0"
        assert d["extensions"] == {}

    def test_with_extensions(self):
        hp = HelloParams(
            protocol_version="0.2.0",
            extensions={
                "identity": ExtensionInfo(version="0.1.0", required=True),
                "audit": ExtensionInfo(version="0.1.0"),
            },
        )
        d = hp.to_dict()
        assert d["protocolVersion"] == "0.2.0"
        assert d["extensions"]["identity"]["required"] is True
        assert d["extensions"]["audit"]["required"] is False


# ---------------------------------------------------------------------------
# HelloResult
# ---------------------------------------------------------------------------

class TestHelloResultFull:
    def test_from_dict_minimal(self):
        r = HelloResult.from_dict({})
        assert r.negotiated_protocol_version == ""
        assert r.session_id == ""
        assert r.extensions == {}

    def test_from_dict_full(self):
        r = HelloResult.from_dict({
            "negotiatedProtocolVersion": "0.1.0",
            "sessionId": "s-1",
            "extensions": {
                "identity": {"selected": "0.1.0", "active": True},
                "audit": {"version": "0.2.0", "active": False},
            },
        })
        assert r.negotiated_protocol_version == "0.1.0"
        assert r.session_id == "s-1"
        assert r.extensions["identity"].version == "0.1.0"
        assert r.extensions["identity"].active is True
        assert r.extensions["audit"].version == "0.2.0"
        assert r.extensions["audit"].active is False


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

class TestIdentityFull:
    def test_default_asserted_by(self):
        i = Identity(subject_id="u1", role="user")
        assert i.asserted_by == "self"
        d = i.to_dict()
        assert d["assertedBy"] == "self"

    def test_custom_asserted_by(self):
        i = Identity(subject_id="u1", role="admin", asserted_by="issuer:auth0")
        d = i.to_dict()
        assert d["assertedBy"] == "issuer:auth0"


# ---------------------------------------------------------------------------
# AuditEvent
# ---------------------------------------------------------------------------

class TestAuditEventFull:
    def test_minimal(self):
        ae = AuditEvent(event_id="e1", action="test")
        d = ae.to_dict()
        assert d == {"eventId": "e1", "action": "test"}

    def test_all_fields(self):
        ae = AuditEvent(
            event_id="e1",
            action="tools/call",
            parent_event_id="e0",
            timestamp="2026-04-09T12:00:00Z",
            actor={"subjectId": "u1", "role": "admin"},
            resource="/tools/search",
            outcome="success",
        )
        d = ae.to_dict()
        assert d["parentEventId"] == "e0"
        assert d["timestamp"] == "2026-04-09T12:00:00Z"
        assert d["actor"]["subjectId"] == "u1"
        assert d["resource"] == "/tools/search"
        assert d["outcome"] == "success"


# ---------------------------------------------------------------------------
# RequestOptions
# ---------------------------------------------------------------------------

class TestRequestOptionsFull:
    def test_all_fields(self):
        ro = RequestOptions(
            timeout_ms=5000,
            session_id="s-1",
            agent_id="a-1",
            identity=Identity(subject_id="u1", role="user"),
            audit=AuditEvent(event_id="e1", action="test"),
        )
        assert ro.timeout_ms == 5000
        assert ro.session_id == "s-1"
        assert ro.agent_id == "a-1"
        assert ro.identity.subject_id == "u1"
        assert ro.audit.event_id == "e1"
