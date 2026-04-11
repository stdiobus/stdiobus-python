#!/usr/bin/env python3

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

"""Echo worker for stdio_bus — reads NDJSON from stdin, responds on stdout."""

import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        if "id" in req:
            resp = {
                "jsonrpc": "2.0",
                "id": req["id"],
                "result": {
                    "echo": req.get("params", {}),
                    "method": req.get("method"),
                    "receivedSessionId": req.get("sessionId"),
                },
            }
            print(json.dumps(resp), flush=True)
    except json.JSONDecodeError:
        pass
