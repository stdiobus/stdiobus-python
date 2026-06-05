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

"""Tests verifying the NativeBackend single-thread model.

These tests confirm that:
1. All C API calls (ingest, step) are confined to the owning event loop thread.
2. send() called from a foreign thread raises TransportError.
3. The poll fd integration drives step() from the event loop (no background thread).
"""

import asyncio
import sys
import threading
import pytest

pytestmark = [
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="Native backend not available on Windows",
    ),
]


def _native_available() -> bool:
    """Check if native bindings are importable."""
    try:
        from stdiobus.backends.native import is_native_available
        return is_native_available()
    except ImportError:
        return False


class TestThreadAffinityEnforcement:
    """Verify that send() rejects calls from foreign threads."""

    @pytest.mark.skipif(not _native_available(), reason="Native bindings not built")
    @pytest.mark.asyncio
    async def test_send_from_foreign_thread_raises(self, tmp_path):
        """send() from a non-loop thread must raise TransportError."""
        from stdiobus.backends.native import NativeBackend
        from stdiobus.errors import TransportError as SdkTransportError

        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/bin/cat",
                "args": [],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')

        backend = NativeBackend(config_path=str(config))
        await backend.start()

        error_caught = threading.Event()
        caught_exception = [None]

        def call_send_from_thread():
            try:
                backend.send('{"jsonrpc":"2.0","method":"ping","id":"1"}')
            except SdkTransportError as e:
                caught_exception[0] = e
                error_caught.set()
            except Exception as e:
                caught_exception[0] = e
                error_caught.set()

        t = threading.Thread(target=call_send_from_thread)
        t.start()
        t.join(timeout=5.0)

        assert error_caught.is_set(), "send() from foreign thread did not raise"
        assert isinstance(caught_exception[0], SdkTransportError)
        assert "owning event loop thread" in str(caught_exception[0])

        await backend.stop()
        backend.destroy()

    @pytest.mark.skipif(not _native_available(), reason="Native bindings not built")
    @pytest.mark.asyncio
    async def test_send_from_loop_thread_succeeds(self, tmp_path):
        """send() from the event loop thread must succeed."""
        from stdiobus.backends.native import NativeBackend

        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/bin/cat",
                "args": [],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')

        backend = NativeBackend(config_path=str(config))
        await backend.start()

        # This runs on the event loop thread — should succeed
        result = backend.send('{"jsonrpc":"2.0","method":"ping","id":"1"}')
        assert result is True

        await backend.stop()
        backend.destroy()


class TestNoPollThread:
    """Verify that the refactored backend has no background polling thread."""

    @pytest.mark.skipif(not _native_available(), reason="Native bindings not built")
    @pytest.mark.asyncio
    async def test_no_background_thread_after_start(self, tmp_path):
        """After start(), no daemon thread named for polling should exist."""
        from stdiobus.backends.native import NativeBackend

        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/bin/cat",
                "args": [],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')

        # Record threads before start
        threads_before = set(threading.enumerate())

        backend = NativeBackend(config_path=str(config))
        await backend.start()

        # No new threads should be spawned by the backend
        threads_after = set(threading.enumerate())
        new_threads = threads_after - threads_before

        # Filter to daemon threads (the old _poll_thread was daemon=True)
        new_daemon_threads = [t for t in new_threads if t.daemon]

        # The native backend should NOT spawn any daemon threads
        assert len(new_daemon_threads) == 0, (
            f"Unexpected daemon threads after start: {new_daemon_threads}"
        )

        await backend.stop()
        backend.destroy()

    @pytest.mark.skipif(not _native_available(), reason="Native bindings not built")
    @pytest.mark.asyncio
    async def test_event_loop_integration_active(self, tmp_path):
        """After start(), the backend uses either fd-driven or timer-driven polling."""
        from stdiobus.backends.native import NativeBackend

        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/bin/cat",
                "args": [],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')

        backend = NativeBackend(config_path=str(config))
        await backend.start()

        # loop should be captured
        assert backend._loop is not None
        assert backend._loop is asyncio.get_running_loop()

        # Exactly one integration mode must be active:
        # Either fd-driven (poll_fd is a valid int) or timer-driven (poll_timer set)
        if backend._poll_fd is not None:
            assert backend._poll_fd >= 0
            assert backend._poll_timer is None
        else:
            assert backend._poll_timer is not None

        await backend.stop()
        backend.destroy()

    @pytest.mark.skipif(not _native_available(), reason="Native bindings not built")
    @pytest.mark.asyncio
    async def test_integration_cleaned_up_after_stop(self, tmp_path):
        """After stop(), both poll fd and timer must be deactivated."""
        from stdiobus.backends.native import NativeBackend

        config = tmp_path / "config.json"
        config.write_text('''{
            "pools": [{
                "id": "echo",
                "command": "/bin/cat",
                "args": [],
                "instances": 1
            }],
            "limits": {
                "max_input_buffer": 1048576,
                "max_output_queue": 4194304
            }
        }''')

        backend = NativeBackend(config_path=str(config))
        await backend.start()

        # Verify something was active
        had_fd = backend._poll_fd is not None
        had_timer = backend._poll_timer is not None
        assert had_fd or had_timer

        await backend.stop()
        # After stop, both must be deactivated
        assert backend._poll_fd is None
        assert backend._poll_timer is None

        backend.destroy()
