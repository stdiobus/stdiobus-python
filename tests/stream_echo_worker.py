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

"""
Streaming echo worker for stdio_bus E2E tests.

Protocol:
- On every JSON-RPC request, captures sessionId from the incoming message
  (injected by the bus for routing), then:
  - stream_echo: emits N session/update notifications with agent_message_chunk
    (one per word), then sends the final JSON-RPC response.
  - notify_test: emits a custom worker/custom_event notification, then responds.
  - default: echo (same as real_echo_worker.py).

Critical: notifications MUST include sessionId from the request so the bus
can route them back to the correct client.
"""

import json
import sys
import time


def emit(obj: dict) -> None:
    """Write NDJSON line to stdout."""
    print(json.dumps(obj), flush=True)


def send_chunk(text: str, session_id: str | None) -> None:
    """Emit an agent_message_chunk notification (ACP streaming protocol).

    The bus routes notifications by sessionId — without it, the notification
    is unroutable and gets dropped.
    """
    notif: dict = {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"text": text},
            }
        },
    }
    if session_id:
        notif["sessionId"] = session_id
    emit(notif)


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "id" not in req:
            continue  # ignore notifications sent to worker

        method = req.get("method", "")
        params = req.get("params", {})
        session_id = req.get("sessionId")

        if method == "stream_echo":
            # Stream chunks word-by-word, then respond with aggregated text.
            text = params.get("text", "")
            words = text.split() if text else []
            for word in words:
                chunk = word + " "
                send_chunk(chunk, session_id)
            aggregated = " ".join(words) + (" " if words else "")
            emit({
                "jsonrpc": "2.0",
                "id": req["id"],
                "result": {"text": aggregated, "chunks_sent": len(words)},
            })

        elif method == "notify_test":
            # Emit a custom notification, then respond.
            notif: dict = {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "custom_event",
                        "event": "test_fired",
                        "payload": params,
                    }
                },
            }
            if session_id:
                notif["sessionId"] = session_id
            emit(notif)
            emit({
                "jsonrpc": "2.0",
                "id": req["id"],
                "result": {"notified": True},
            })

        else:
            # Default: echo
            emit({
                "jsonrpc": "2.0",
                "id": req["id"],
                "result": {
                    "echo": params,
                    "method": method,
                    "receivedSessionId": session_id,
                },
            })


if __name__ == "__main__":
    main()
