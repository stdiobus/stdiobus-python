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

"""Build script for cffi bindings to libstdio_bus.

Single ABI source: stdiobus/native/cdefs.h
  - Used by ffi.cdef() for the Python/cffi side.
  - Used by ffi.set_source() for the C-compiler side (with system includes prepended).

No external header required. Links against prebuilt libstdio_bus.a from kernel/prebuilds/.

Usage:
    python -m stdiobus.native.build_ffi
"""

import platform
from pathlib import Path

from cffi import FFI

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
SDK_DIR = SCRIPT_DIR.parent.parent
CDEFS_FILE = SCRIPT_DIR / "cdefs.h"

KERNEL_PREBUILDS = SDK_DIR / "kernel" / "prebuilds"
PROJECT_ROOT = SDK_DIR.parent.parent


def _host_triple() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()

    arch = {
        "x86_64": "x86_64", "amd64": "x86_64",
        "arm64": "aarch64", "aarch64": "aarch64",
    }.get(machine)

    if arch is None:
        raise RuntimeError(f"Unsupported architecture for native build: {machine!r}")
    if system == "darwin":
        return f"{arch}-apple-darwin"
    if system == "linux":
        return f"{arch}-unknown-linux-gnu"
    raise RuntimeError(f"Unsupported OS for native build: {system!r}")


def _resolve_lib_dir() -> tuple[Path, str]:
    triple = _host_triple()
    bundled = KERNEL_PREBUILDS / triple
    if (bundled / "libstdio_bus.a").exists():
        return bundled, triple

    legacy = PROJECT_ROOT / "build"
    if (legacy / "libstdio_bus.a").exists():
        return legacy, f"{triple} (legacy)"

    raise FileNotFoundError(
        f"No prebuilt libstdio_bus.a for {triple!r}.\n"
        f"  Checked: {bundled}\n"
        f"       and: {legacy}\n"
        "Use backend='subprocess' or backend='docker'."
    )


LIB_DIR, RESOLVED_TRIPLE = _resolve_lib_dir()


def _arch_compile_args() -> list[str]:
    if platform.system().lower() != "darwin":
        return []
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "x86_64"
    return ["-arch", arch]


# ---------------------------------------------------------------------------
# FFI
# ---------------------------------------------------------------------------

ffi = FFI()

with open(CDEFS_FILE, "r") as f:
    cdefs = f.read()

# cffi parser: reads cdefs.h as-is (understands #define, typedef, etc.)
ffi.cdef(cdefs)

# C compiler: same cdefs.h text, with system includes prepended so clang
# resolves uint64_t, size_t, uint16_t, etc.
_SYSTEM_INCLUDES = "#include <stddef.h>\n#include <stdint.h>\n#include <stdbool.h>\n\n"

ffi.set_source(
    "stdiobus.native._ffi",
    _SYSTEM_INCLUDES + cdefs,
    library_dirs=[str(LIB_DIR)],
    libraries=["stdio_bus"],
    extra_compile_args=["-std=c11", *_arch_compile_args()],
    extra_link_args=_arch_compile_args(),
)

if __name__ == "__main__":
    print(f"[build_ffi] triple:  {RESOLVED_TRIPLE}")
    print(f"[build_ffi] lib:     {LIB_DIR}")
    print(f"[build_ffi] cdefs:   {CDEFS_FILE}")
    ffi.compile(verbose=True)
