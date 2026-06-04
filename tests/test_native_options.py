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

"""Tests for the native listener facade integration.

Covers the previously-unintegrated capability: exposing the native backend's
external listener (TCP/Unix) and the introspection methods through the public
client, plus the contract that a listener requires the native backend.

These tests assert client-facade behavior and do NOT require the native
bindings to be built — backend creation is the only native-dependent step and
is asserted via the documented InvalidArgumentError fallback.
"""

import pytest

from stdiobus import (
    AsyncStdioBus,
    StdioBus,
    BusConfig,
    PoolConfig,
    ListenMode,
    NativeOptions,
    InvalidArgumentError,
)
from stdiobus.backends.subprocess import SubprocessBackend
from stdiobus.types import NativeOptions as NativeOptionsType


def _cfg():
    return BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

class TestExports:
    def test_listen_mode_exported(self):
        import stdiobus
        assert "ListenMode" in stdiobus.__all__
        assert stdiobus.ListenMode is ListenMode

    def test_native_options_exported(self):
        import stdiobus
        assert "NativeOptions" in stdiobus.__all__
        assert stdiobus.NativeOptions is NativeOptionsType


# ---------------------------------------------------------------------------
# NativeOptions.validate() — self-validating config object
# ---------------------------------------------------------------------------

class TestNativeOptionsValidation:
    def test_defaults_are_explicit(self):
        opts = NativeOptions()
        assert opts.listen_mode == ListenMode.NONE
        assert opts.tcp_host == "127.0.0.1"
        assert opts.tcp_port is None
        assert opts.unix_path is None
        assert opts.poll_interval_ms == 1

    def test_none_mode_valid(self):
        NativeOptions().validate()  # must not raise

    def test_tcp_requires_port(self):
        with pytest.raises(ValueError, match="TCP requires tcp_port"):
            NativeOptions(listen_mode=ListenMode.TCP).validate()

    def test_tcp_port_zero_rejected(self):
        # Port 0 (ephemeral) is undiscoverable via the native API → rejected.
        with pytest.raises(ValueError, match="1..65535"):
            NativeOptions(listen_mode=ListenMode.TCP, tcp_port=0).validate()

    def test_tcp_port_out_of_range_rejected(self):
        with pytest.raises(ValueError, match="1..65535"):
            NativeOptions(listen_mode=ListenMode.TCP, tcp_port=70000).validate()

    def test_tcp_valid(self):
        NativeOptions(listen_mode=ListenMode.TCP, tcp_port=8765).validate()

    def test_unix_requires_path(self):
        with pytest.raises(ValueError, match="UNIX requires unix_path"):
            NativeOptions(listen_mode=ListenMode.UNIX).validate()

    def test_unix_valid(self):
        NativeOptions(listen_mode=ListenMode.UNIX, unix_path="/tmp/bus.sock").validate()

    def test_poll_interval_must_be_positive(self):
        with pytest.raises(ValueError, match="poll_interval_ms"):
            NativeOptions(poll_interval_ms=0).validate()

    # --- listen_mode normalization (string ergonomics, enum invariant) ---

    def test_listen_mode_string_normalized_to_enum(self):
        # Parity with the client's `backend: BackendMode | str` ergonomics:
        # a string is coerced so downstream `.value` access is always safe.
        opts = NativeOptions(listen_mode="tcp", tcp_port=8765)
        assert opts.listen_mode is ListenMode.TCP
        opts.validate()  # must treat it as TCP and accept the port

    def test_listen_mode_invalid_string_rejected_at_construction(self):
        with pytest.raises(ValueError, match="invalid listen_mode"):
            NativeOptions(listen_mode="bogus")


# ---------------------------------------------------------------------------
# Client-level validation: invalid native options fail fast as InvalidArgument
# ---------------------------------------------------------------------------

class TestClientValidatesNativeOptions:
    def test_invalid_native_options_raise_invalid_argument(self):
        # ValueError from validate() is surfaced as the public InvalidArgumentError.
        with pytest.raises(InvalidArgumentError, match="TCP requires tcp_port"):
            AsyncStdioBus(
                config=_cfg(),
                backend="native",
                native=NativeOptions(listen_mode=ListenMode.TCP),
            )


# ---------------------------------------------------------------------------
# Listener contract: a listener is a native-only capability
# ---------------------------------------------------------------------------

class TestListenerRequiresNativeBackend:
    def test_listener_with_docker_rejected(self):
        with pytest.raises(InvalidArgumentError, match="native-backend capability"):
            AsyncStdioBus(
                config=_cfg(),
                backend="docker",
                native=NativeOptions(listen_mode=ListenMode.TCP, tcp_port=8765),
            )

    def test_listener_with_subprocess_rejected(self):
        with pytest.raises(InvalidArgumentError, match="native-backend capability"):
            AsyncStdioBus(
                config=_cfg(),
                backend="subprocess",
                native=NativeOptions(listen_mode=ListenMode.TCP, tcp_port=8765),
            )

    def test_no_listener_with_subprocess_ok(self):
        # NativeOptions with NONE listener must not constrain backend choice.
        bus = AsyncStdioBus(
            config=_cfg(),
            backend="subprocess",
            native=NativeOptions(),  # listen_mode=NONE
        )
        assert bus.get_backend_type() == "subprocess"
        bus.destroy()


# ---------------------------------------------------------------------------
# Backend contract: honest introspection sentinels
# ---------------------------------------------------------------------------

class TestBackendIntrospectionDefaults:
    def test_base_default_listen_mode_is_none(self):
        # Default contract on the abstract base (verified via a concrete backend).
        backend = SubprocessBackend(config_path="/tmp/x.json")
        assert backend.get_listen_mode() == ListenMode.NONE
        backend.destroy()

    def test_subprocess_counts_are_unknown_sentinel(self):
        backend = SubprocessBackend(config_path="/tmp/x.json")
        # Subprocess has no worker/client introspection channel → -1, not 0.
        assert backend.get_worker_count() == -1
        assert backend.get_client_count() == -1
        backend.destroy()


# ---------------------------------------------------------------------------
# Client introspection delegation (no backend / subprocess)
# ---------------------------------------------------------------------------

class TestClientIntrospection:
    def test_no_backend_returns_sentinels(self):
        bus = AsyncStdioBus(config=_cfg())
        bus._backend = None
        assert bus.get_listen_mode() == ListenMode.NONE
        assert bus.get_worker_count() == -1
        assert bus.get_client_count() == -1
        bus.destroy()

    def test_subprocess_client_introspection(self):
        bus = AsyncStdioBus(config=_cfg(), backend="subprocess")
        assert bus.get_listen_mode() == ListenMode.NONE
        assert bus.get_worker_count() == -1
        assert bus.get_client_count() == -1
        bus.destroy()

    def test_sync_wrapper_delegates(self):
        bus = StdioBus(config=_cfg(), backend="subprocess")
        assert bus.get_listen_mode() == ListenMode.NONE
        assert bus.get_worker_count() == -1
        assert bus.get_client_count() == -1
        bus.destroy()


# ---------------------------------------------------------------------------
# Docker backend: client-count parity with Node (SDK-to-container connection)
# ---------------------------------------------------------------------------

class TestDockerClientCount:
    def _docker_backend(self, tmp_path):
        from stdiobus.backends.docker import DockerBackend
        cfg = tmp_path / "config.json"
        cfg.write_text('{"pools":[{"id":"w","command":"echo","instances":1}]}')
        # DockerBackend.__init__ runs a docker availability check; skip if absent.
        import shutil
        if shutil.which("docker") is None:
            pytest.skip("Docker not available")
        return DockerBackend(str(cfg))

    def test_client_count_zero_before_running(self, tmp_path):
        backend = self._docker_backend(tmp_path)
        # CREATED state, no writer → 0 (not -1): docker CAN report its own socket.
        assert backend.get_client_count() == 0
        # Worker count remains the unknown sentinel.
        assert backend.get_worker_count() == -1
