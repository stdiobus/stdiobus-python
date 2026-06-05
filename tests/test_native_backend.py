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

"""Tests for native backend."""

import asyncio
import json
import pytest
import sys
import os

# Skip all tests if native backend not available
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Native backend not available on Windows"
)


def test_native_import():
    """Test that native module can be imported (if built)."""
    try:
        from stdiobus.backends.native import NativeBackend, is_native_available
        # Just check import works
        assert callable(is_native_available)
    except ImportError as e:
        pytest.skip(f"Native bindings not built: {e}")


def test_native_availability_check():
    """Test is_native_available function."""
    try:
        from stdiobus.backends.native import is_native_available
        # Should return bool
        result = is_native_available()
        assert isinstance(result, bool)
    except ImportError:
        pytest.skip("Native bindings not built")


def test_native_backend_requires_bindings():
    """Test that NativeBackend raises if bindings not available."""
    try:
        from stdiobus.backends.native import NativeBackend, is_native_available
        if not is_native_available():
            with pytest.raises(ImportError):
                NativeBackend("./config.json")
    except ImportError:
        pytest.skip("Native bindings not built")


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ECHO_WORKER = os.path.join(TESTS_DIR, "real_echo_worker.py")


@pytest.mark.skipif(
    not os.path.exists(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "kernel", "prebuilds"
    )),
    reason="kernel/prebuilds not present"
)
class TestNativeBackendIntegration:
    """Integration tests for native backend (requires built library)."""
    
    @pytest.fixture
    def config_path(self, tmp_path):
        """Create a test config using the real JSON-RPC echo worker.
        
        The kernel requires a worker that participates in the JSON-RPC
        protocol (at minimum: reads NDJSON from stdin, responds on stdout).
        A raw pipe (e.g. node stdin.pipe(stdout)) does NOT satisfy this —
        stop() triggers a kernel assertion (SIGTRAP) on graceful shutdown.
        """
        config = tmp_path / "config.json"
        config_data = {
            "pools": [{
                "id": "echo",
                "command": sys.executable,
                "args": [ECHO_WORKER],
                "instances": 1,
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304,
            },
        }
        config.write_text(json.dumps(config_data))
        return str(config)
    
    def test_create_backend(self, config_path):
        """Test creating native backend."""
        try:
            from stdiobus.backends.native import NativeBackend, is_native_available
            if not is_native_available():
                pytest.skip("Native bindings not available")
            
            backend = NativeBackend(config_path)
            assert backend is not None
            backend.destroy()
        except ImportError:
            pytest.skip("Native bindings not built")
    
    @pytest.mark.asyncio
    async def test_start_stop(self, config_path):
        """Test starting and stopping native backend.
        
        Verifies the full lifecycle: create → start → running → stop → stopped → destroy.
        Uses real_echo_worker.py which correctly handles JSON-RPC, allowing the
        kernel to perform a clean graceful shutdown without assertion failures.
        """
        try:
            from stdiobus.backends.native import NativeBackend, is_native_available
            from stdiobus.types import BusState
            
            if not is_native_available():
                pytest.skip("Native bindings not available")
            
            backend = NativeBackend(config_path)
            
            await backend.start()
            # Allow worker to initialize and become ready
            await asyncio.sleep(0.3)
            assert backend.get_state() == BusState.RUNNING
            
            await backend.stop(timeout_sec=5.0)
            assert backend.get_state() == BusState.STOPPED
            
            backend.destroy()
        except ImportError:
            pytest.skip("Native bindings not built")
