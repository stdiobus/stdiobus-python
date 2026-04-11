"""Tests for native backend."""

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


@pytest.mark.skipif(
    not os.path.exists("../../build/libstdio_bus.a"),
    reason="libstdio_bus.a not built"
)
class TestNativeBackendIntegration:
    """Integration tests for native backend (requires built library)."""
    
    @pytest.fixture
    def config_path(self, tmp_path):
        """Create a test config file."""
        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/usr/bin/env",
                "args": ["node", "-e", "process.stdin.pipe(process.stdout)"],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')
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
        """Test starting and stopping native backend."""
        try:
            from stdiobus.backends.native import NativeBackend, is_native_available
            from stdiobus.types import BusState
            
            if not is_native_available():
                pytest.skip("Native bindings not available")
            
            backend = NativeBackend(config_path)
            
            await backend.start()
            assert backend.get_state() == BusState.RUNNING
            
            await backend.stop()
            assert backend.get_state() == BusState.STOPPED
            
            backend.destroy()
        except ImportError:
            pytest.skip("Native bindings not built")
