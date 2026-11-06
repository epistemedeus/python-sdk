# Open `subscriptions/listen` holds SIGTERM

Measured on this checkout (`bbf500a79d031cfcee0da087a15b2f08f0b23667`, Python 3.12.3). There is no SDK commit to apply. [Issue 3806](https://github.com/modelcontextprotocol/python-sdk/issues/3806) is open, and no pull request in `modelcontextprotocol/python-sdk` names it. The regression below is the reusable check.

`handle_modern_request` keeps a `text/event-stream` response open until `recv_ch.receive()` raises `anyio.EndOfStream`, or until `watch_disconnect` sees `http.disconnect`. Nothing in that loop observes server shutdown. uvicorn's default `timeout_graceful_shutdown` is `None`, so `Server.shutdown` waits on the open connection with no deadline. The listen handler had already written its ack frame before SIGTERM, so the wait is the open SSE body, not a request that never started.

A completed modern `tools/list` and a server with no client both finish shutdown. Those are the non-fitting cases.

## Version boundary

The exit condition is the same shape on the released line and on current main: `watch_disconnect` is present, `EndOfStream` is present, `EventSourceResponse` is absent, and `should_exit` is absent.

| Pin | Modern file | `EventSourceResponse` in `streamable_http.py` / `_streamable_http_modern.py` |
| --- | --- | --- |
| this tree, parent `57394b0548d1e2dc2dce8d67d84985769df3b8bb` | sha256 `f8286b4b61f14d1cf3166a4236d46b08bf59dd8eb63fbd658a97c6c7fa8a714f` | 6 / 0 |
| `v2.0.0`, `v2.1.1`, `v2.2.0` | same exit tokens, no `should_exit` | not re-counted here; modern count is 0 |
| `v2.3.0` `2118f14f8a19bc158d8a1cf90af58d85d187f849` | SSE loop tail matches this tree | 6 / 0 |
| `af4c6f4db2cd207f44374cd324a12cfbee8d2da6` and main `77001c96f9507be0f4d009b83210a384a5e08bda` | blob `ad4257319d3f6082415c1722617032a259009d24` | 6 / 0 |

The main blob differs from this tree by inlining the same SSE response headers (`#3726`, `2af9e308`, 2026-10-17). That commit does not add a shutdown path. `MCPServer.run_streamable_http_async` on this tree and on main `77001c96` builds `uvicorn.Config` without `timeout_graceful_shutdown`.

`v2.0.0` (2026-07-28) is the first stable tag whose modern file has this loop. The loop arrived with the 2026 HTTP SSE entry (`#3001`, 2026-06-26) and `subscriptions/listen` (`#3035`, 2026-06-30).

The legacy GET stream is a separate dependency boundary. This checkout's lock, and the `v2.3.0` tag lock, pin `sse-starlette==3.0.2` and `uvicorn==0.35.0`. On 3.0.2, `AppStatus.handle_exit` records a context-local event. The SIGTERM handler does not see the event the SSE task is waiting on, so `EventSourceResponse` does not end the GET stream. `sse-starlette` 3.6.0 watches `AppStatus.should_exit` and uvicorn `Server.should_exit` from `_shutdown_watcher`, and `EventSourceResponse._listen_for_exit_signal` waits on that. The modern handler never constructs an `EventSourceResponse`, so that drain does not cover `subscriptions/listen`.

A floating install of `mcp==2.3.0` can resolve `sse-starlette` 3.6.0 even though the tag lock pins 3.0.2. The two runs below keep this tree's `mcp` and change only that pair.

## Run

From this checkout, against the lock:

```bash
uv run --frozen python reproductions/modern-sse-graceful-shutdown/repro/run_shutdown.py
```

Against the dependency pair from the issue report (`sse-starlette` 3.6.0, `uvicorn` 0.54.0), same `mcp` sources:

```bash
uv run --frozen --with 'sse-starlette==3.6.0' --with 'uvicorn==0.54.0' \
  python reproductions/modern-sse-graceful-shutdown/repro/run_shutdown.py
```

The server listens on `127.0.0.1` and an ephemeral port. Stdout is one JSON object.

Exit 0: `disposition` is `listen-exits`. The open listen stream's process finished inside 8 seconds, and the idle and completed-request cases did too.

Exit 1: `disposition` is `hang-reproduced`. The listen stream was open and the process was still running at 8 seconds. The idle and completed-request cases had finished.

Exit 2: `disposition` is `harness-invalid`. One of those three cases did not reach the state under test.

`legacy_get` and `listen_grace_5` are recorded and do not select the exit code. A return code of -15 is uvicorn re-raising the captured SIGTERM after `Finished server process`. A return code of -9 is the harness sending SIGKILL at the bound. The 8 second and 12 second cutoffs are harness limits, not server settings.

## Measured runs

Locked `sse-starlette` 3.0.2 and `uvicorn` 0.35.0. `mcp` `0.0.1.dev1038+bbf500a`. Exit 1.

| Case | What is open | Hung | Seconds | Code |
| --- | --- | --- | --- | --- |
| `idle` | no client | no | 0.214 | -15 |
| `modern_closed` | completed `tools/list`, then the connection is closed | no | 0.164 | -15 |
| `legacy_get` | `2025-06-18` GET `text/event-stream` | yes | 8.007 | -9 |
| `listen` | `2026-07-28` `subscriptions/listen`, ack frame received | yes | 8.007 | -9 |
| `listen_grace_5` | same listen stream, `timeout_graceful_shutdown=5` | no | 5.18 | -15 |

`listen_grace_5` logged `Cancel 1 running task(s), timeout graceful shutdown exceeded` and `CancelledError`. That is uvicorn cancelling the connection task. It is the report's workaround, and `MCPServer.run()` does not pass this timeout.

Same `mcp`, `sse-starlette` 3.6.0 and `uvicorn` 0.54.0. Exit 1.

| Case | What is open | Hung | Seconds | Code |
| --- | --- | --- | --- | --- |
| `idle` | no client | no | 0.214 | -15 |
| `modern_closed` | completed `tools/list`, then the connection is closed | no | 0.164 | -15 |
| `legacy_get` | `2025-06-18` GET `text/event-stream` | no | 0.215 | -15 |
| `listen` | `2026-07-28` `subscriptions/listen`, ack frame received | yes | 8.008 | -9 |
| `listen_grace_5` | same listen stream, `timeout_graceful_shutdown=5` | no | 5.23 | -15 |

An earlier run of that same 3.6.0 command finished the legacy GET in 0.616 seconds. The listen stream was still up at 8 seconds on that run too.

Records: `evidence/`.

## Later use

Run the locked command. Exit 1 means an open listen stream still holds shutdown on this source. Exit 0 means that stream's process now finishes inside 8 seconds and the idle and completed-request cases still do. The 3.6.0 command is the check that the legacy GET stream still drains while the listen stream is the case that changed.

The production stop counts in the issue were not remeasured. Issue comments about main `af4c6f4` and the 6/0 `EventSourceResponse` count match the files above. The timings in those comments are not these runs.

## Follow-through: explicit expectations and HTTP completion

November 6, 2026. The original records above are unchanged. The runner now binds
legacy and modern expectations separately. With Uvicorn held at 0.35.0 and the
same SDK source, these are the measured legacy outcomes:

| sse-starlette | Legacy process shutdown | Final HTTP chunk | Incomplete-response ASGI error | Modern listen |
| --- | --- | --- | --- | --- |
| 3.0.2 | Held at 8 seconds | Missing after cutoff | No | Held |
| 3.0.4 | Held at 8 seconds | Missing after cutoff | No | Held |
| 3.1.0 | 0.615 seconds | Missing | Yes | Held |
| 3.1.2 | 0.565 seconds | Missing | Yes | Held |
| 3.6.0 | 0.615 seconds | Missing | Yes | Held |

This verifies a dependency-specific process-drain boundary, not a complete SSE
response or an SDK repair. The source probe records each installed dependency's
source hash and whether it contains `_shutdown_watcher`. It does not establish
that raising a package floor alone repairs the modern handler.

Pin both observed behaviors in one command:

```bash
uv run --frozen python reproductions/modern-sse-graceful-shutdown/repro/run_shutdown.py \
  --expect-legacy held --expect-listen held

uv run --frozen --with 'sse-starlette==3.1.2' --with 'uvicorn==0.35.0' \
  python reproductions/modern-sse-graceful-shutdown/repro/run_shutdown.py \
  --expect-legacy drains --expect-listen held
```

`held` means that the acknowledged/open stream's server remained alive at the
cutoff. `drains` requires a normal process exit inside the bound. `clean` also
requires receiving the terminating HTTP chunk and no incomplete-response ASGI
error. Process exit alone does not satisfy `clean`. Legacy GET need not send an
initial event; it is qualified by initialization 200, initialized notification
202 and GET 200. Modern listen must send its actual subscription acknowledgment
before SIGTERM. Framing is read only after the owned test server terminates.

With explicit expectations, exit 0 means the selected profile matches, not that
the SDK bug is fixed. Exit 1 means a valid case mismatched its selected profile.
Exit 2 means setup or the idle/completed-request controls are invalid. The
default command keeps its original behavior: legacy is record-only and an open
modern stream still exits 1. `qualification` states the actual selected profile.

Two real negative controls on 3.6.0 exit 1: asking for a `clean` legacy drain,
and asking for the modern stream to `drain`. The default locked command also
retains exit 1. All five dependency profiles and those three checks are stored
under [`evidence/feedback-20261106/`](evidence/feedback-20261106/).

Seven focused unit/wire tests cover both-generation selection, invalid setup,
crashed-process refusal, and actual complete versus truncated chunked responses:

```bash
uv run --frozen python -m unittest discover \
  -s reproductions/modern-sse-graceful-shutdown/repro -p test_shutdown.py -v
```

Only this reproduction changes. SDK implementation and dependency requirements
remain unchanged. The bounded matrix does not remeasure production stop counts.
