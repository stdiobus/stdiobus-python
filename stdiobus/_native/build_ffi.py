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

"""Build script for cffi bindings to libstdio_bus."""

import os
import sys
from pathlib import Path

from cffi import FFI

# Find project root
SCRIPT_DIR = Path(__file__).parent
SDK_DIR = SCRIPT_DIR.parent.parent
PROJECT_ROOT = SDK_DIR.parent.parent

# Paths
INCLUDE_DIR = PROJECT_ROOT / "include"
LIB_DIR = PROJECT_ROOT / "build"
CDEFS_FILE = SCRIPT_DIR / "cdefs.h"

ffi = FFI()

# Read C definitions
with open(CDEFS_FILE, "r") as f:
    cdefs = f.read()

ffi.cdef(cdefs)

# Set source with library linking
ffi.set_source(
    "stdiobus._native._ffi",
    """
    #include "stdio_bus_embed.h"
    """,
    include_dirs=[str(INCLUDE_DIR)],
    library_dirs=[str(LIB_DIR)],
    libraries=["stdio_bus"],
    extra_compile_args=["-std=c11"],
)

if __name__ == "__main__":
    ffi.compile(verbose=True)
