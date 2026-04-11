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

#!/usr/bin/env python3
"""Manual test script for stdiobus Python SDK.

Run: python3 test_manual.py
"""

import json
import os
import sys
import tempfile

print("=== Testing stdiobus Python SDK ===\n")

# Test 1: Imports
print("1. Testing imports...")
try:
    from stdiobus import (
        StdioBus,
        AsyncStdioBus,
        BusState,
        BackendMode,
        StdioBusError,
        InvalidArgumentError,
    )
    print("   ✓ All imports successful")
except ImportError as e:
    print(f"   ✗ Import failed: {e}")
    sys.exit(1)

# Test 2: Types
print("\n2. Testing types...")
print(f"   BusState.RUNNING = {BusState.RUNNING}")
print(f"   BackendMode.AUTO = {BackendMode.AUTO}")
assert BusState.RUNNING == 2
assert BackendMode.AUTO == "auto"
print("   ✓ Types correct")

# Test 3: Errors
print("\n3. Testing errors...")
from stdiobus.errors import TimeoutError, error_from_code

err = TimeoutError("test timeout")
assert err.code == 3
assert "TIMEOUT" in str(err)
print(f"   Error string: {err}")
print("   ✓ Errors work correctly")

# Test 4: Create instance (without Docker)
print("\n4. Testing instance creation...")
with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    json.dump({"pools": []}, f)
    config_path = f.name

try:
    # This should fail because native is not implemented
    try:
        bus = StdioBus(config_path=config_path, backend="native")
        print("   ✗ Should have raised error for native backend")
    except InvalidArgumentError as e:
        print(f"   ✓ Native backend correctly raises: {e.message}")
    
    # Docker backend should create successfully (if Docker available)
    import shutil
    if shutil.which("docker"):
        try:
            bus = StdioBus(config_path=config_path, backend="docker")
            print(f"   ✓ Docker backend created, state: {bus.get_state()}")
            bus.destroy()
        except Exception as e:
            print(f"   ⚠ Docker backend creation failed: {e}")
    else:
        print("   ⚠ Docker not available, skipping Docker backend test")
finally:
    os.unlink(config_path)

print("\n=== Basic Tests Complete ===")
print("\nTo test Docker backend fully, run:")
print("  python3 -m pytest tests/test_docker_backend.py -v")
