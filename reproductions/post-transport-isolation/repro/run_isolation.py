"""Localhost isolation check for one failed Streamable HTTP POST.

The stand-in speaks stateless JSON-mode MCP on 127.0.0.1. It never opens a
public socket. ``slow`` answers after a short delay. ``drop`` resets the TCP
connection without a response. ``midbody`` sends a short JSON prefix and
closes. A later phase stops the listener so the next POST cannot connect.

The caller owns the httpx2 client. This program does not retry a failed
request. Exit 0 means the failed POST stayed on that request. Exit 1 means
the reported session loss reproduced. Exit 2 means the harness could not
complete a healthy baseline.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import socket
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import anyio
import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

SLOW_SECONDS = 0.6
DROP_SECONDS = 0.15
CANCEL_AFTER_SECONDS = 0.12
RUN_LIMIT_SECONDS = 25.0

_LINGER = struct.pack("ii", 1, 0)


class StandIn(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, port: int) -> None:
        super().__init__(("127.0.0.1", port), _Handler)
        self.counts: dict[str, int] = {}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        self._empty(405)

    def do_DELETE(self) -> None:  # noqa: N802
        self._empty(405)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length) if length else b""
        try:
            message = json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            self._json(400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
            return
        method = message.get("method")
        request_id = message.get("id")
        if method == "initialize" and request_id is not None:
            params = message.get("params") or {}
            protocol = params.get("protocolVersion") or "2025-03-26"
            self._json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": protocol,
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "isolation-standin", "version": "0"},
                    },
                },
            )
            return
        if request_id is None:
            self._empty(202)
            return
        if method == "tools/list":
            schema = {"type": "object", "properties": {}}
            self._json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": [
                            {"name": "slow", "inputSchema": schema},
                            {"name": "drop", "inputSchema": schema},
                            {"name": "midbody", "inputSchema": schema},
                        ]
                    },
                },
            )
            return
        if method == "tools/call":
            name = str((message.get("params") or {}).get("name") or "")
            server = self.server
            assert isinstance(server, StandIn)
            server.counts[name] = server.counts.get(name, 0) + 1
            if name == "drop":
                time.sleep(DROP_SECONDS)
                self._reset()
                return
            if name == "midbody":
                self._mid_body()
                return
            if name == "slow":
                time.sleep(SLOW_SECONDS)
            self._json(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"content": [{"type": "text", "text": "ok"}], "isError": False},
                },
            )
            return
        self._json(200, {"jsonrpc": "2.0", "id": request_id, "result": {}})

    def _empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _json(self, status: int, payload: object) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    def _reset(self) -> None:
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER)
        self.connection.close()
        self.close_connection = True

    def _mid_body(self) -> None:
        prefix = b'{"jsonrpc":"2.0","id":'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "200")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(prefix)
        self.wfile.flush()
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()


def _start(port: int = 0) -> StandIn:
    server = StandIn(port)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _stop(server: StandIn) -> None:
    if getattr(server, "_stopped", False):
        return
    server._stopped = True  # type: ignore[attr-defined]
    server.shutdown()
    server.server_close()


def _describe(exc: BaseException, started: float) -> dict[str, object]:
    cancelled = isinstance(exc, anyio.get_cancelled_exc_class()) or type(exc).__name__ == "CancelledError"
    subs: list[dict[str, str]] = []
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop()
        for sub in getattr(current, "exceptions", ()) or ():
            subs.append({"type": f"{type(sub).__module__}.{type(sub).__name__}", "message": str(sub)[:240]})
            pending.append(sub)
    return {
        "ok": False,
        "text": None,
        "error_type": f"{type(exc).__module__}.{type(exc).__name__}",
        "error_code": getattr(exc, "code", None),
        "message": str(exc)[:300],
        "cancelled": cancelled,
        "subexceptions": subs,
        "seconds": round(time.monotonic() - started, 3),
    }


async def _call(session: ClientSession, name: str) -> dict[str, object]:
    started = time.monotonic()
    try:
        result = await session.call_tool(name, {})
    except BaseException as exc:
        return {"name": name, **_describe(exc, started)}
    text = "".join(getattr(part, "text", "") or "" for part in (getattr(result, "content", None) or []))
    return {
        "name": name,
        "ok": text == "ok",
        "text": text,
        "error_type": None,
        "error_code": None,
        "message": None,
        "cancelled": False,
        "subexceptions": [],
        "seconds": round(time.monotonic() - started, 3),
    }


async def _cancel_one(session: ClientSession) -> dict[str, object]:
    observed: dict[str, object] = {}

    async def runner() -> None:
        started = time.monotonic()
        try:
            await session.call_tool("slow", {})
        except BaseException as exc:
            observed.update(_describe(exc, started))
            raise
        observed.update({"ok": True, "cancelled": False, "error_type": None, "seconds": round(time.monotonic() - started, 3)})

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(runner)
            await anyio.sleep(CANCEL_AFTER_SECONDS)
            tg.cancel_scope.cancel()
    except BaseException as exc:
        observed["task_group_type"] = f"{type(exc).__module__}.{type(exc).__name__}"
    observed.setdefault("cancelled", False)
    return observed


def _failed_request(row: dict[str, object]) -> bool:
    return row.get("ok") is False and row.get("cancelled") is False and bool(row.get("error_type"))


def _healthy(row: dict[str, object]) -> bool:
    return row.get("ok") is True and row.get("cancelled") is False and float(row["seconds"]) >= SLOW_SECONDS - 0.15


async def _run(server: StandIn) -> dict[str, object]:
    port = int(server.server_address[1])
    url = f"http://127.0.0.1:{port}/mcp"
    http = httpx2.AsyncClient(timeout=httpx2.Timeout(20.0))
    report: dict[str, object] = {"cases": {}}
    cases: dict[str, object] = {}
    report["cases"] = cases
    replacement: StandIn | None = None
    try:
        async with streamable_http_client(url, http_client=http) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                cases["baseline"] = await _call(session, "slow")

                async def _store(key: str, name: str) -> None:
                    cases[key] = await _call(session, name)

                await asyncio.gather(_store("concurrent_slow", "slow"), _store("concurrent_drop", "drop"))
                cases["future"] = await _call(session, "slow")
                cases["midbody"] = await _call(session, "midbody")
                cases["after_midbody"] = await _call(session, "slow")
                cases["caller_cancel"] = await _cancel_one(session)
                await anyio.sleep(SLOW_SECONDS)
                cases["after_cancel"] = await _call(session, "slow")
                report["counts"] = dict(server.counts)
                await anyio.to_thread.run_sync(_stop, server)
                cases["connect"] = await _call(session, "slow")
                replacement = _start(port)
                cases["after_connect"] = await _call(session, "slow")
                counts = report["counts"]
                assert isinstance(counts, dict)
                counts["after_restart_slow"] = replacement.counts.get("slow", 0)
    except BaseException as exc:
        described = _describe(exc, time.monotonic())
        report["cleanup"] = {
            "raised": True,
            "error_type": described["error_type"],
            "message": described["message"],
            "subexceptions": described["subexceptions"],
        }
    else:
        report["cleanup"] = {"raised": False, "error_type": None, "message": None, "subexceptions": []}
    finally:
        if replacement is not None:
            await anyio.to_thread.run_sync(_stop, replacement)
    report["caller_owned_client_closed"] = http.is_closed
    await http.aclose()
    if "counts" not in report:
        report["counts"] = dict(server.counts)
    return report


def _isolated(report: dict[str, object]) -> bool:
    cases = report["cases"]
    assert isinstance(cases, dict)
    cleanup = report["cleanup"]
    assert isinstance(cleanup, dict)
    counts = report.get("counts") or {}
    assert isinstance(counts, dict)
    needed = (
        "baseline",
        "concurrent_slow",
        "concurrent_drop",
        "future",
        "midbody",
        "after_midbody",
        "caller_cancel",
        "after_cancel",
        "connect",
        "after_connect",
    )
    if any(name not in cases for name in needed):
        return False
    baseline = cases["baseline"]
    concurrent_slow = cases["concurrent_slow"]
    concurrent_drop = cases["concurrent_drop"]
    future = cases["future"]
    midbody = cases["midbody"]
    after_midbody = cases["after_midbody"]
    cancel = cases["caller_cancel"]
    after_cancel = cases["after_cancel"]
    connect = cases["connect"]
    after_connect = cases["after_connect"]
    if not all(isinstance(row, dict) for row in (baseline, concurrent_slow, future, after_midbody, after_cancel, after_connect)):
        return False
    if not all(isinstance(row, dict) for row in (concurrent_drop, midbody, connect, cancel)):
        return False
    return bool(
        _healthy(baseline)
        and _healthy(concurrent_slow)
        and _failed_request(concurrent_drop)
        and _healthy(future)
        and _failed_request(midbody)
        and _healthy(after_midbody)
        and cancel.get("cancelled") is True
        and cancel.get("ok") is not True
        and _healthy(after_cancel)
        and _failed_request(connect)
        and _healthy(after_connect)
        and cleanup.get("raised") is False
        and report.get("caller_owned_client_closed") is False
        and counts.get("drop") == 1
        and counts.get("midbody") == 1
    )


def main() -> int:
    started = time.monotonic()
    server = _start()
    try:
        report = asyncio.run(asyncio.wait_for(_run(server), RUN_LIMIT_SECONDS))
    except Exception as exc:  # harness failure before a report exists
        print(json.dumps({"harness_error": f"{type(exc).__name__}: {exc}"}, sort_keys=True))
        return 2
    finally:
        if getattr(server, "_BaseServer__is_shut_down", None) is not None:
            try:
                _stop(server)
            except Exception:
                pass
    report["mcp"] = importlib.metadata.version("mcp")
    report["httpx2"] = importlib.metadata.version("httpx2")
    report["python"] = __import__("sys").version.split()[0]
    report["wall_seconds"] = round(time.monotonic() - started, 3)
    baseline = (report.get("cases") or {}).get("baseline") if isinstance(report.get("cases"), dict) else None
    if not isinstance(baseline, dict) or baseline.get("ok") is not True:
        report["isolated"] = False
        print(json.dumps(report, sort_keys=True))
        return 2
    isolated = _isolated(report)
    report["isolated"] = isolated
    print(json.dumps(report, sort_keys=True))
    return 0 if isolated else 1


if __name__ == "__main__":
    raise SystemExit(main())
