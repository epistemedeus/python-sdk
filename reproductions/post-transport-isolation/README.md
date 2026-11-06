# One failed Streamable HTTP POST stays on that request

Localhost check and a source patch for the Python MCP SDK. One `httpx2.TransportError` on a request POST currently cancels unrelated in-flight calls and ends the session. After the patch, that failure stays on the one request.

Checked pins, read 2026-11-06:

- Released `v2.3.0` is commit `2118f14f8a19bc158d8a1cf90af58d85d187f849`.
- Current `main` is commit `77001c96f9507be0f4d009b83210a384a5e08bda`.
- `src/mcp/client/streamable_http.py` is identical between those two commits.
- Issue 3522 is open. Pull requests 3525, 3584, and 3741 are closed and unmerged. No open pull request names 3522.

The patch follows pull request 3584, head `4713b21129d6bd0f9fc904a5ccee4c15a1369bad`, retargeted onto `77001c96`.

## Run

`repro/run_isolation.py` listens on `127.0.0.1` only. It uses whatever `mcp` and `httpx2` are installed. Stdout is one JSON object. The delays inside the script bound this check. They are not a client timeout setting.

```bash
pip install -r repro/requirements-2.3.0.txt
python repro/run_isolation.py
```

`repro/requirements-2.3.0.txt` is the exact lock resolved for `mcp==2.3.0` on Python 3.12.3 (`httpx2==2.16.0`). Exit 1 is the session-loss boundary. Exit 0 means the failed POST stayed on that request and the later calls completed. Exit 2 means the healthy baseline did not complete.

The same command against an editable install of commit `77001c96` (`httpx2==2.12.0` from that tree's lock) also exits 1. A shallow checkout reports its version as `0.0.1.dev1+77001c9`. The pin is the commit.

## Patch

From a checkout of `77001c96f9507be0f4d009b83210a384a5e08bda`:

```bash
git apply patch/mcp-streamable-http-post-transport-77001c96.patch
```

`git apply --check` succeeds on that commit. The patch touches only:

- `src/mcp/client/streamable_http.py`
- `tests/client/test_streamable_http.py`
- `docs/client/transports.md`

`_handle_post_request` catches `httpx2.TransportError` for an ordinary message. A request resolves as `MCPError` code `CONNECTION_CLOSED` (-32000), and the message names the original exception. A notification is dropped. The exception is also handed to the session read stream. `initialize` and `server/discover` still raise the raw transport error. Caller cancellation still propagates. The failed request is sent once. Any other exception still leaves the transport task group. A caller-owned HTTP client stays under caller ownership.

SDK checks on the patched tree, same filter as the unpatched run: 18 passed, 53 deselected, 0.13s. On the unpatched file the new containment tests fail with `ExceptionGroup` from the transport task group (9 failed, 9 passed, 0.21s). The passing unpatched tests are the existing cancellation tests, connect-time propagation, programmer-error propagation, and the discover network-error test.

## Measured runs

| Tree | mcp | httpx2 | Exit | Wall |
| --- | --- | --- | --- | --- |
| v2.3.0 | 2.3.0 | 2.16.0 | 1 | 1.148s |
| 77001c96 | commit pin | 2.12.0 | 1 | 1.188s |
| 77001c96 plus patch | commit pin | 2.12.0 | 0 | 5.234s |

On both unpatched runs the healthy `slow` call returns `ok` in about 0.61s. The concurrent `slow` and `drop` calls then both surface as `CancelledError` at about 0.15s. Leaving the client raises `ExceptionGroup` containing `httpx2.ReadError`. The caller-owned client stays open. The stand-in receives one `drop`.

On the patched run the concurrent `slow` call returns `ok` in 0.605s. `drop` is `MCPError` -32000, `HTTP request failed: ReadError('')`, in 0.156s. A later call succeeds. A mid-body close is `RemoteProtocolError` on that call only. Stopping the listener makes the next call `ConnectError` on that call only, and the same session succeeds again after the listener returns. Cancelling the caller still raises `CancelledError` at 0.121s, and the following call succeeds. Context cleanup raises nothing. `drop` and `midbody` are each received once. The caller-owned client stays open.

Records: `evidence/`.
