#!/usr/bin/env python3
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
