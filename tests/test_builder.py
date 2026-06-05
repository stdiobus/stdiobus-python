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

"""Unit tests for the fluent ``StdioBusBuilder`` (Task 5 / R3).

The builder is a thin facade: it accumulates keyword arguments only and
delegates entirely to the unchanged ``AsyncStdioBus.__init__`` /
``StdioBus.__init__``. These tests pin two properties:

  * **Equivalence (R3.1):** ``build()`` yields an ``AsyncStdioBus`` whose
    observable configuration matches a direct ``AsyncStdioBus(**kwargs)`` call,
    and ``build_sync()`` yields a working ``StdioBus``.
  * **Reused validation (R3.2, R3.4):** invalid inputs raise the *same* SDK
    exceptions as direct construction — proving validation lives in ``__init__``
    and is not duplicated in the builder.

Requirements covered: 3.1, 3.2, 3.4.
"""

import tempfile
import os

import pytest

from stdiobus import (
    StdioBus,
    AsyncStdioBus,
    StdioBusBuilder,
    BusConfig,
    PoolConfig,
    BackendMode,
    SubprocessOptions,
    NativeOptions,
    ListenMode,
    InvalidArgumentError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg():
    return BusConfig(pools=[PoolConfig(id="w", command="echo", instances=1)])


def _observable(bus: AsyncStdioBus) -> dict:
    """Capture the constructor-derived, deterministic state of a bus.

    Excludes the auto-generated ``client_session_id`` (intentionally unique per
    instance) so equivalence reflects the supplied configuration only.
    """
    return {
        "backend_mode": bus._backend_mode,
        "timeout_ms": bus._timeout_ms,
        "backend_type": bus.get_backend_type(),
        "docker_options": bus._docker_options,
        "subprocess_options": bus._subprocess_options,
        "native_options": bus.native_options,
    }


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

class TestExports:

    def test_builder_exported(self):
        import stdiobus
        assert "StdioBusBuilder" in stdiobus.__all__
        assert stdiobus.StdioBusBuilder is StdioBusBuilder


# ---------------------------------------------------------------------------
# Fluent API shape
# ---------------------------------------------------------------------------

class TestFluentApi:

    def test_setters_return_self_for_chaining(self):
        builder = StdioBusBuilder()
        assert builder.config(_make_cfg()) is builder
        assert builder.backend("subprocess") is builder
        assert builder.timeout_ms(15000) is builder
        assert builder.subprocess(SubprocessOptions()) is builder
        assert builder.docker(None) is builder  # stored verbatim, no validation
        assert builder.native(NativeOptions()) is builder

    def test_config_path_setter_returns_self(self):
        builder = StdioBusBuilder()
        assert builder.config_path("/tmp/x.json") is builder

    def test_builder_performs_no_validation_until_build(self):
        # Both config and config_path set (mutually exclusive) — the builder
        # itself must NOT raise; only build() (i.e. __init__) does.
        builder = (
            StdioBusBuilder()
            .config(_make_cfg())
            .config_path("/tmp/x.json")
        )
        with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
            builder.build()

    def test_last_write_wins(self):
        builder = StdioBusBuilder().config(_make_cfg()).timeout_ms(1000).timeout_ms(2000)
        bus = builder.build()
        try:
            assert bus._timeout_ms == 2000
        finally:
            bus.destroy()


# ---------------------------------------------------------------------------
# Equivalence to direct construction (R3.1)
# ---------------------------------------------------------------------------

class TestBuildEquivalence:

    def test_build_equivalent_to_direct_init_minimal(self):
        cfg = _make_cfg()
        built = StdioBusBuilder().config(cfg).build()
        direct = AsyncStdioBus(config=cfg)
        try:
            assert isinstance(built, AsyncStdioBus)
            assert _observable(built) == _observable(direct)
        finally:
            built.destroy()
            direct.destroy()

    def test_build_equivalent_to_direct_init_full_kwargs(self):
        cfg = _make_cfg()
        sub = SubprocessOptions(binary_path="/custom/path")
        built = (
            StdioBusBuilder()
            .config(cfg)
            .backend("subprocess")
            .timeout_ms(12345)
            .subprocess(sub)
            .build()
        )
        direct = AsyncStdioBus(
            config=cfg,
            backend="subprocess",
            timeout_ms=12345,
            subprocess=sub,
        )
        try:
            assert _observable(built) == _observable(direct)
            assert built._backend_mode == BackendMode.SUBPROCESS
            assert built._timeout_ms == 12345
            assert built._subprocess_options is sub
        finally:
            built.destroy()
            direct.destroy()

    def test_build_with_config_path_equivalent(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.write(fd, b'{"pools":[{"id":"w","command":"echo","instances":1}]}')
        os.close(fd)
        try:
            built = StdioBusBuilder().config_path(path).backend("subprocess").build()
            direct = AsyncStdioBus(config_path=path, backend="subprocess")
            try:
                assert _observable(built) == _observable(direct)
            finally:
                built.destroy()
                direct.destroy()
        finally:
            os.unlink(path)

    def test_build_sync_produces_working_stdiobus(self):
        cfg = _make_cfg()
        bus = StdioBusBuilder().config(cfg).backend("subprocess").timeout_ms(9000).build_sync()
        try:
            assert isinstance(bus, StdioBus)
            # Delegates to a private AsyncStdioBus constructed from the same kwargs.
            assert bus.client_session_id.startswith("client-")
            assert bus.is_running() is False
            assert bus.get_backend_type() == "subprocess"
            assert bus._async_bus._timeout_ms == 9000
        finally:
            bus.destroy()

    def test_build_and_build_sync_share_configuration(self):
        cfg = _make_cfg()
        builder = StdioBusBuilder().config(cfg).backend("subprocess").timeout_ms(7777)
        async_bus = builder.build()
        sync_bus = builder.build_sync()
        try:
            assert async_bus._backend_mode == sync_bus._async_bus._backend_mode
            assert async_bus._timeout_ms == sync_bus._async_bus._timeout_ms == 7777
        finally:
            async_bus.destroy()
            sync_bus.destroy()


# ---------------------------------------------------------------------------
# Reused validation (R3.2, R3.4)
# ---------------------------------------------------------------------------

class TestReusedValidation:
    """Invalid inputs must raise the SAME exception as direct construction."""

    def test_config_and_config_path_together_raise_like_direct(self):
        cfg = _make_cfg()
        # Direct construction oracle.
        with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
            AsyncStdioBus(config=cfg, config_path="/tmp/x.json")
        # Builder must raise the identical error via the unchanged __init__.
        with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
            StdioBusBuilder().config(cfg).config_path("/tmp/x.json").build()

    def test_missing_config_raises_like_direct(self):
        with pytest.raises(InvalidArgumentError, match="config or config_path is required"):
            AsyncStdioBus()
        with pytest.raises(InvalidArgumentError, match="config or config_path is required"):
            StdioBusBuilder().build()

    def test_missing_config_raises_on_build_sync(self):
        with pytest.raises(InvalidArgumentError, match="config or config_path is required"):
            StdioBusBuilder().build_sync()

    def test_invalid_native_options_raise_like_direct(self):
        cfg = _make_cfg()
        # listen_mode=TCP without tcp_port → InvalidArgumentError from __init__.
        with pytest.raises(InvalidArgumentError, match="TCP requires tcp_port"):
            AsyncStdioBus(
                config=cfg,
                backend="native",
                native=NativeOptions(listen_mode=ListenMode.TCP),
            )
        with pytest.raises(InvalidArgumentError, match="TCP requires tcp_port"):
            (
                StdioBusBuilder()
                .config(cfg)
                .backend("native")
                .native(NativeOptions(listen_mode=ListenMode.TCP))
                .build()
            )

    def test_invalid_config_raises_like_direct(self):
        # Empty pools fails BusConfig.validate() inside __init__.
        empty_cfg = BusConfig(pools=[])
        with pytest.raises(ValueError, match="at least one pool is required"):
            AsyncStdioBus(config=empty_cfg)
        with pytest.raises(ValueError, match="at least one pool is required"):
            StdioBusBuilder().config(empty_cfg).build()

    def test_exception_type_matches_direct_for_native(self):
        """The builder raises the exact same exception class as direct __init__."""
        cfg = _make_cfg()
        native = NativeOptions(listen_mode=ListenMode.TCP)

        direct_exc = None
        try:
            AsyncStdioBus(config=cfg, backend="native", native=native)
        except Exception as e:  # noqa: BLE001 — capture for type comparison
            direct_exc = e

        builder_exc = None
        try:
            StdioBusBuilder().config(cfg).backend("native").native(native).build()
        except Exception as e:  # noqa: BLE001 — capture for type comparison
            builder_exc = e

        assert direct_exc is not None and builder_exc is not None
        assert type(direct_exc) is type(builder_exc)
        assert str(direct_exc) == str(builder_exc)
