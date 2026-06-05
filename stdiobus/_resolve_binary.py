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

"""Resolve the path to the bundled stdio_bus binary.

Resolution order:
1. Bundled kernel/dist/stdio_bus shell launcher (relative to package root).
2. Fallback: ``shutil.which("stdio_bus")`` — system PATH lookup.
3. If neither found: returns None (caller decides how to surface the error).

The shell launcher in kernel/dist/stdio_bus auto-selects the correct
platform binary (darwin-arm64, linux-amd64, etc.) at runtime.
"""

import shutil
from pathlib import Path

# Package root: stdiobus/ directory. The project root is one level up.
_PACKAGE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _PACKAGE_DIR.parent

# Bundled binary: kernel/dist/stdio_bus (shell launcher with platform dispatch)
_BUNDLED_BINARY = _PROJECT_ROOT / "kernel" / "dist" / "stdio_bus"


def resolve_binary(explicit_path: str = "") -> str | None:
    """Resolve the stdio_bus binary path.

    Args:
        explicit_path: User-provided explicit path. If non-empty, returned
            as-is (user override takes absolute precedence).

    Returns:
        Absolute path to the binary, or None if not found anywhere.

    Resolution order:
        1. explicit_path (non-empty) → returned directly.
        2. Bundled kernel/dist/stdio_bus → if file exists and is executable
           (or at least exists — the shell launcher handles platform dispatch).
        3. shutil.which("stdio_bus") → system PATH fallback.
    """
    if explicit_path:
        return explicit_path

    if _BUNDLED_BINARY.is_file():
        return str(_BUNDLED_BINARY)

    # Fallback: system PATH
    found = shutil.which("stdio_bus")
    if found:
        return found

    return None


def get_bundled_binary_path() -> str | None:
    """Return the bundled binary path if it exists, else None.

    Useful for _resolve_backend auto-mode probing without triggering PATH search.
    """
    if _BUNDLED_BINARY.is_file():
        return str(_BUNDLED_BINARY)
    return None
