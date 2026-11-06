"""Localhost shutdown check for an open 2026-07-28 subscriptions/listen stream.

The server is this environment's ``mcp``, on 127.0.0.1 only. One process serves
Streamable HTTP through ``MCPServer.streamable_http_app`` and uvicorn's default
graceful-shutdown timeout. The parent opens a client, sends SIGTERM, and records
whether that process exits inside a fixed bound.

Exit 0: the listen stream's process exited inside the bound, and the idle and
completed-request cases did too.
Exit 1: the listen stream was open and the process was still running at the
bound, while those two cases exited.
Exit 2: the idle case, the completed request, or the listen stream did not
reach the state under test.

By default legacy_get is recorded, preserving the original command. Explicit
--expect-legacy and --expect-listen bind both generations to held/drains/clean.
Prompt drain and a complete chunked response are separate observations.
``listen_grace_5`` remains a cancellation control. A return code of -15 is uvicorn re-raising SIGTERM after shutdown. A
return code of -9 is this process sending SIGKILL at the bound.

Stdout is one JSON object. The bound is a measurement cutoff, not a server setting.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import importlib.metadata
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

HANG_BOUND_SECONDS = 8.0
GRACE_SECONDS = 5.0
GRACE_BOUND_SECONDS = 12.0
OPEN_TIMEOUT_SECONDS = 20.0
READY_TIMEOUT_SECONDS = 30.0
PREVIEW_SECONDS = 2.0

MODERN = "2026-07-28"
LEGACY = "2025-06-18"
META_VERSION = "io.modelcontextprotocol/protocolVersion"
META_INFO = "io.modelcontextprotocol/clientInfo"
META_CAPS = "io.modelcontextprotocol/clientCapabilities"

_LOG_MARKERS = (
    "Application startup complete",
    "Uvicorn running",
    "Shutting down",
    "Waiting for",
    "Cancel ",
    "Finished server",
    "ERROR",
    "Error",
    "Exception",
    "ASGI",
)


def serve(port: int, graceful: str | None) -> None:
    """Run one MCP server until uvicorn stops it."""
    import uvicorn

    from mcp.server.mcpserver import MCPServer

    mcp = MCPServer("shutdown-repro")

    @mcp.tool()
    def ping() -> str:
        return "pong"

    app = mcp.streamable_http_app()
    if graceful is None:
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
        return
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        timeout_graceful_shutdown=float(graceful),
    )


def _envelope() -> dict[str, Any]:
    return {
        META_VERSION: MODERN,
        META_INFO: {"name": "repro", "version": "0"},
        META_CAPS: {},
    }


def _modern_headers(method: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MODERN,
        "Mcp-Method": method,
    }


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _source_probe() -> dict[str, Any]:
    import sse_starlette.sse as sse

    import mcp.server._streamable_http_modern as modern
    import mcp.server.streamable_http as legacy

    modern_text = Path(modern.__file__).read_text(encoding="utf-8")
    legacy_text = Path(legacy.__file__).read_text(encoding="utf-8")
    dependency_text = Path(sse.__file__).read_text(encoding="utf-8")
    return {
        "modern_file": modern.__file__,
        "legacy_file": legacy.__file__,
        "modern_sha256": hashlib.sha256(modern_text.encode()).hexdigest(),
        "modern_event_source_response": modern_text.count("EventSourceResponse"),
        "legacy_event_source_response": legacy_text.count("EventSourceResponse"),
        "modern_watch_disconnect": "async def watch_disconnect" in modern_text,
        "modern_should_exit": "should_exit" in modern_text,
        "modern_end_of_stream": modern_text.count("EndOfStream"),
        "sse_source_sha256": hashlib.sha256(dependency_text.encode()).hexdigest(),
        "sse_shutdown_watcher": "_shutdown_watcher" in dependency_text,
        "sse_context_var": "ContextVar" in dependency_text,
    }


def _git_head() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.strip()


def _interesting(lines: list[str]) -> list[str]:
    kept = [line.rstrip("\n") for line in lines if any(marker in line for marker in _LOG_MARKERS)]
    return kept[:40]


class _Server:
    def __init__(self, graceful: str | None) -> None:
        self.port = _free_port()
        self.lines: list[str] = []
        self._lines_lock = threading.Lock()
        argv = [sys.executable, str(Path(__file__).resolve()), "serve", str(self.port)]
        if graceful is not None:
            argv.append(graceful)
        self.proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        self._drain = threading.Thread(target=self._read, daemon=True)
        self._drain.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lines_lock:
                self.lines.append(line)

    def _snapshot(self) -> list[str]:
        with self._lines_lock:
            return list(self.lines)

    def ready(self) -> bool:
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            if any("Application startup complete" in line for line in self._snapshot()):
                return True
            time.sleep(0.05)
        return False

    def stop(self, bound: float) -> dict[str, Any]:
        started = time.monotonic()
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        hung = False
        try:
            code = self.proc.wait(timeout=bound)
        except subprocess.TimeoutExpired:
            hung = True
            self.proc.kill()
            code = self.proc.wait(timeout=5)
        self._drain.join(timeout=2)
        return {
            "hung": hung,
            "seconds": round(time.monotonic() - started, 3),
            "returncode": code,
            "log": _interesting(self._snapshot()),
        }

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self._drain.join(timeout=2)


def _request(
    port: int,
    method: str,
    path: str,
    body: dict[str, Any] | None,
    headers: dict[str, str],
) -> dict[str, Any]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=OPEN_TIMEOUT_SECONDS)
    try:
        raw = None if body is None else json.dumps(body).encode()
        conn.request(method, path, body=raw, headers=headers)
        response = conn.getresponse()
        payload = response.read()
        return {
            "status": response.status,
            "content_type": response.getheader("content-type") or "",
            "session_id": response.getheader("mcp-session-id"),
            "body": payload.decode("utf-8", errors="replace")[:500],
        }
    finally:
        conn.close()


def _preview(conn: http.client.HTTPConnection, response: http.client.HTTPResponse) -> str:
    if conn.sock is None:
        return ""
    conn.sock.settimeout(PREVIEW_SECONDS)
    chunks: list[bytes] = []
    size = 0
    try:
        while size < 800:
            line = response.readline()
            if not line:
                break
            chunks.append(line)
            size += len(line)
            if line in {b"\n", b"\r\n"} and size > 16:
                break
    except TimeoutError:
        pass
    finally:
        if conn.sock is not None:
            conn.sock.settimeout(None)
    return b"".join(chunks).decode("utf-8", errors="replace")


class _Held:
    def __init__(self) -> None:
        self.conn: http.client.HTTPConnection | None = None
        self.response: http.client.HTTPResponse | None = None

    def open(
        self,
        port: int,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        headers: dict[str, str],
        read_preview: bool = True,
    ) -> dict[str, Any]:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=OPEN_TIMEOUT_SECONDS)
        self.conn = conn
        raw = None if body is None else json.dumps(body).encode()
        conn.request(method, path, body=raw, headers=headers)
        response = conn.getresponse()
        self.response = response
        return {
            "status": response.status,
            "content_type": response.getheader("content-type") or "",
            # Legacy GET need not emit an initial event. Timing out readline
            # poisons its buffered reader and hides the later HTTP terminator.
            "preview": _preview(conn, response) if read_preview else "",
            "chunked": response.chunked,
        }

    def receive_end(self) -> dict[str, Any]:
        """Observe framing only AFTER our test server has terminated.

        Successful chunk decoding consumes the final zero-length chunk. An
        abrupt EOF instead raises IncompleteRead, even when the process drained
        promptly. This is a wire observation, not an inference from its exit.
        """
        response = self.response
        if response is None:
            return {"body_complete": None, "final_chunk_received": None}
        size = 0
        try:
            while size < 65536:
                chunk = response.read1(min(4096, 65536 - size))
                if not chunk:
                    return {
                        "body_complete": True,
                        "final_chunk_received": True if response.chunked else None,
                        "remaining_body_bytes": size,
                    }
                size += len(chunk)
            return {
                "body_complete": None,
                "final_chunk_received": None,
                "remaining_body_bytes": size,
                "error": "capture-limit",
            }
        except http.client.IncompleteRead as exc:
            return {
                "body_complete": False,
                "final_chunk_received": False if response.chunked else None,
                "remaining_body_bytes": size + len(exc.partial),
                "error": "IncompleteRead",
            }
        except (OSError, http.client.HTTPException, ValueError) as exc:
            return {
                "body_complete": None,
                "final_chunk_received": None,
                "remaining_body_bytes": size,
                "error": type(exc).__name__,
            }

    def close(self) -> None:
        if self.response is not None:
            self.response.close()
            self.response = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def _listen_body() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscriptions/listen",
        "params": {"notifications": {"toolsListChanged": True}, "_meta": _envelope()},
    }


def _prepare_closed(port: int) -> dict[str, Any]:
    opened = _request(
        port,
        "POST",
        "/mcp",
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"_meta": _envelope()}},
        _modern_headers("tools/list"),
    )
    opened["ok"] = opened["status"] == 200 and "tools" in str(opened["body"])
    return opened


def _prepare_legacy(port: int) -> tuple[_Held, dict[str, Any]]:
    init = _request(
        port,
        "POST",
        "/mcp",
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": LEGACY,
                "capabilities": {},
                "clientInfo": {"name": "repro", "version": "0"},
            },
        },
        {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"},
    )
    session = init["session_id"]
    if init["status"] != 200 or not isinstance(session, str) or not session:
        return _Held(), {"ok": False, "initialize": init}
    note = _request(
        port,
        "POST",
        "/mcp",
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "mcp-session-id": session,
            "MCP-Protocol-Version": LEGACY,
        },
    )
    held = _Held()
    stream = held.open(
        port,
        "GET",
        "/mcp",
        None,
        {"Accept": "text/event-stream", "mcp-session-id": session, "MCP-Protocol-Version": LEGACY},
        read_preview=False,
    )
    content_type = str(stream["content_type"]).split(";", 1)[0].strip()
    stream["ok"] = stream["status"] == 200 and content_type == "text/event-stream" and note["status"] == 202
    stream["initialize_status"] = init["status"]
    stream["initialized_status"] = note["status"]
    return held, stream


def _prepare_listen(port: int) -> tuple[_Held, dict[str, Any]]:
    held = _Held()
    stream = held.open(port, "POST", "/mcp", _listen_body(), _modern_headers("subscriptions/listen"))
    content_type = str(stream["content_type"]).split(";", 1)[0].strip()
    acknowledged = False
    for line in str(stream["preview"]).splitlines():
        if not line.startswith("data:"):
            continue
        try:
            message = json.loads(line[5:].strip())
            acknowledged = (
                message.get("jsonrpc") == "2.0"
                and message.get("method") == "notifications/subscriptions/acknowledged"
                and message.get("params", {}).get("_meta", {}).get("io.modelcontextprotocol/subscriptionId") == 1
            )
        except (ValueError, AttributeError):
            continue
        if acknowledged:
            break
    stream["acknowledged"] = acknowledged
    stream["ok"] = stream["status"] == 200 and content_type == "text/event-stream" and acknowledged
    return held, stream


def _run_case(name: str, graceful: str | None, bound: float, prepare: str) -> dict[str, Any]:
    server = _Server(graceful)
    held = _Held()
    try:
        if not server.ready():
            return {"name": name, "ok": False, "error": "server did not start", "log": _interesting(server._snapshot())}
        detail: dict[str, Any] = {"ok": True}
        if prepare == "closed":
            detail = _prepare_closed(server.port)
        elif prepare == "legacy":
            held, detail = _prepare_legacy(server.port)
        elif prepare == "listen":
            held, detail = _prepare_listen(server.port)
        if not detail.get("ok"):
            return {"name": name, "ok": False, "setup": detail, "log": _interesting(server._snapshot())}
        stopped = server.stop(bound)
        if prepare in {"legacy", "listen"}:
            stopped["stream_end"] = held.receive_end()
            stopped["asgi_response_error"] = any(
                "ASGI callable returned without completing response" in line for line in stopped["log"]
            )
        return {"name": name, "ok": True, "setup": detail, **stopped}
    except (OSError, http.client.HTTPException, TimeoutError) as exc:
        return {
            "name": name,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "log": _interesting(server._snapshot()),
        }
    finally:
        held.close()
        server.kill()


def _disposition(cases: dict[str, dict[str, Any]]) -> str:
    idle = cases["idle"]
    closed = cases["modern_closed"]
    listen = cases["listen"]
    if not _drains(idle) or not _drains(closed):
        return "harness-invalid"
    if not listen.get("ok"):
        return "harness-invalid"
    if listen.get("hung"):
        return "hang-reproduced"
    return "listen-exits" if _drains(listen) else "harness-invalid"


def _drains(case: dict[str, Any]) -> bool:
    return bool(case.get("ok") and not case.get("hung") and case.get("returncode") in {0, -15})


def _matches(case: dict[str, Any], expected: str) -> bool:
    if expected == "record":
        return True
    if not case.get("ok"):
        return False
    if expected == "held":
        return case.get("hung") is True
    if expected == "drains":
        return _drains(case)
    if expected == "clean":
        return bool(
            _drains(case)
            and not case.get("asgi_response_error")
            and case.get("stream_end", {}).get("final_chunk_received") is True
        )
    raise ValueError("unknown expectation")


def _selection(cases: dict[str, dict[str, Any]], legacy: str, listen: str) -> dict[str, Any]:
    invalid = _disposition(cases) == "harness-invalid" or (legacy != "record" and not cases["legacy_get"].get("ok"))
    checks = {"legacy_get": _matches(cases["legacy_get"], legacy), "listen": _matches(cases["listen"], listen)}
    return {
        "expected": {"legacy_get": legacy, "listen": listen},
        "checks": checks,
        "exit": 2 if invalid else (0 if all(checks.values()) else 1),
    }


def harness(expected_legacy: str = "record", expected_listen: str = "drains") -> int:
    cases = {
        "idle": _run_case("idle", None, HANG_BOUND_SECONDS, "idle"),
        "modern_closed": _run_case("modern_closed", None, HANG_BOUND_SECONDS, "closed"),
        "legacy_get": _run_case("legacy_get", None, HANG_BOUND_SECONDS, "legacy"),
        "listen": _run_case("listen", None, HANG_BOUND_SECONDS, "listen"),
        "listen_grace_5": _run_case("listen_grace_5", str(GRACE_SECONDS), GRACE_BOUND_SECONDS, "listen"),
    }
    disposition = _disposition(cases)
    selection = _selection(cases, expected_legacy, expected_listen)
    report = {
        "disposition": disposition,
        "exit": selection["exit"],
        "qualification": selection,
        "hang_bound_seconds": HANG_BOUND_SECONDS,
        "grace_seconds": GRACE_SECONDS,
        "grace_bound_seconds": GRACE_BOUND_SECONDS,
        "python": sys.version.split()[0],
        "git_head": _git_head(),
        "packages": {
            "mcp": _package_version("mcp"),
            "uvicorn": _package_version("uvicorn"),
            "sse-starlette": _package_version("sse-starlette"),
        },
        "source": _source_probe(),
        "cases": cases,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return int(report["exit"])


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        graceful = sys.argv[3] if len(sys.argv) > 3 else None
        serve(int(sys.argv[2]), graceful)
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-legacy", choices=("record", "held", "drains", "clean"), default="record")
    parser.add_argument("--expect-listen", choices=("held", "drains", "clean"), default="drains")
    args = parser.parse_args()
    return harness(args.expect_legacy, args.expect_listen)


if __name__ == "__main__":
    sys.exit(main())
