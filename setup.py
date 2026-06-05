# SPDX-License-Identifier: Apache-2.0
"""
setup.py for cffi native extension auto-build during pip install.

cffi_modules tells setuptools to invoke build_ffi.py:ffi during build_ext.
The string format "path/to/module.py:ffi_object" is resolved by cffi's
setuptools integration — no import needed here.

If the native extension cannot be built (missing lib, unsupported platform,
no compiler), the install proceeds without it (pure-Python, subprocess/docker).
"""

import os
import sys
from pathlib import Path
from setuptools import setup

# Ensure the source tree is importable during isolated builds.
HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

cffi_modules = []

try:
    # Verify the build script resolves paths (lib exists for this platform).
    from stdiobus._native.build_ffi import ffi, RESOLVED_TRIPLE  # noqa: F401
    cffi_modules = ["stdiobus/_native/build_ffi.py:ffi"]
    print(f"[setup.py] Native extension will be built for {RESOLVED_TRIPLE}")
except (ImportError, FileNotFoundError, RuntimeError, OSError) as e:
    print(f"[setup.py] Skipping native extension: {e}")

setup(cffi_modules=cffi_modules)
