# logs-gateway

A small Rust service that sits between a FiveM server and an external log panel, absorbing log traffic so the game server never blocks on the network or main thread (scripting).

FiveM POSTs a log, the gateway queues it and answers immediately; background tasks forward it to the panel, retry whatever fails, and keep a configurable set of paths in local files instead.

```
FiveM  ──POST──►  logs-gateway (localhost:3000)  ──async──►  log panel
                        │
                        ├─► logs/local-YYYY-MM-DD.log   (LOCAL_LOG_PATHS, never forwarded)
                        └─► failed_logs.json            (NDJSON, retried on a timer)
```

Extracted from a tool I ran for a client, with client specifics removed: the panel's endpoints, request shapes and any other details of that deployment are not part of this repo.

## Why

`PerformHttpRequest` in FiveM is fire-and-forget, but a slow or dead panel still means requests piling up, tokens expiring mid-session, and logs silently lost. The gateway takes ownership of all of that: auth, retries, backpressure and persistence live in one place, and the Lua side is reduced to a plain JSON POST with no token or retry logic.

## Features

- **Instant ACK** — requests are pushed onto a bounded mpsc queue (1000) and answered with `202 Accepted`; forwarding happens off the request path. If the live queue is full (panel slow or down), the log goes straight to the retry actor's file instead of being dropped, and the answer is still `202`.
- **Wildcard proxying** — `POST /api/*rest` forwards to `EXTERNAL_URL_BASE` preserving the original path, so new panel endpoints need no gateway changes.
- **JWT handling** — token fetched at startup with exponential backoff (the gateway survives a panel that's down), refreshed on a timer, and refreshed *reactively* on a `401` with a single retry. The token is published through a `watch` channel: forwarders read the latest value and can wait for the next one. Refresh requests go through a channel of capacity 1, so a burst of 401s triggers one login call, not fifty.
- **Race-free retry queue** — a single `retry` actor task owns both the in-memory queue and `failed_logs.json`; forwarders only send failures to it over a channel. Persistence is append-only NDJSON (O(1) per failure), compacted after each retry round via tmp file + fsync + rename. Legacy array-format files from the first version are migrated on startup.
- **Bounded concurrency** — a semaphore caps in-flight forwards (`MAX_INFLIGHT`, default 64), live and retried alike, so a burst can't flood the panel. When the permits run out the queue fills, and the HTTP handlers start diverting to the retry file: backpressure all the way from the panel to FiveM, without ever blocking the game server.
- **Timeouts** — 15s request / 5s connect on one shared `reqwest::Client`.
- **Graceful shutdown** — on `SIGTERM` or Ctrl-C the listener closes, the queue is drained, in-flight forwards finish, and the retry actor writes its file before the process exits.

## Configuration

All configuration comes from environment variables — no credentials are baked into the binary.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `EXTERNAL_URL_BASE` | yes | — | Panel base URL, e.g. `https://panel.example.ro`. The request path (`/api/...`) is appended to it as-is |
| `EXTERNAL_AUTH_URL` | yes | — | Login endpoint returning `{ "token": "..." }` |
| `EXTERNAL_EMAIL` | yes | — | Panel account used for auth |
| `EXTERNAL_PASSWORD` | yes | — | Panel account password |
| `PORT` | no | `3000` | Local listen port |
| `RETRY_INTERVAL_SECS` | no | `30` | How often the retry actor re-attempts failed logs |
| `TOKEN_REFRESH_SECS` | no | `1800` | Periodic token refresh interval |
| `MAX_INFLIGHT` | no | `64` | Max concurrent forwards to the panel |
| `LOCAL_LOG_PATHS` | no | — | Comma-separated request paths written to `logs/` instead of being forwarded, e.g. `/api/admin-action-log/store` |

Missing or invalid variables are all reported together and the process exits at startup rather than run half-configured.

## Running

```bash
cargo build --release

EXTERNAL_URL_BASE=https://panel.example.ro \
EXTERNAL_AUTH_URL=https://panel.example.ro/api/auth/login \
EXTERNAL_EMAIL=... \
EXTERNAL_PASSWORD=... \
./target/release/logs-gateway
```

Files are written relative to the working directory: `failed_logs.json` and `logs/`.

### systemd

```ini
[Unit]
Description=logs-gateway
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/logs-gateway
EnvironmentFile=/opt/logs-gateway/.env
ExecStart=/opt/logs-gateway/logs-gateway
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

`TimeoutStopSec` should be generous enough for the drain to finish on shutdown; with `MAX_INFLIGHT=64` and a 15s request timeout, the worst case is a few rounds of timeouts against a dead panel.

## Endpoints

| Route | Behaviour |
| --- | --- |
| `POST /api/*rest`, path listed in `LOCAL_LOG_PATHS` | Appended as one JSON line (`{ts, path, body}`) to `logs/local-YYYY-MM-DD.log`; never forwarded |
| `POST /api/*rest`, any other path | Queued and forwarded to the same path on the panel |

Bodies are accepted as arbitrary JSON (`serde_json::Value`), so no struct has to be defined per endpoint; the gateway never looks inside a body.

## Delivery semantics

- **At-least-once.** A log is only removed from the retry file after the panel answered 2xx. If the gateway dies between a successful forward and the next compaction, that log is sent again on restart; the panel should tolerate duplicates.
- **Not persisted before ACK.** The live queue is in memory. A crash with logs queued but not yet attempted loses them. Writing every log to disk before answering `202` (a write-ahead log) is the obvious next step; it wasn't needed at this volume.
- **No dead-letter limit.** A log the panel keeps rejecting (say, a `422`) is retried every round, forever. There is no attempt counter yet.
- **Order is not preserved** between live and retried logs.

## Project layout

| File | Role |
| --- | --- |
| `src/main.rs` | Wiring, routes, HTTP handler for `/api/*`, shutdown sequence |
| `src/config.rs` | `Config::from_env`, validation, URL joining |
| `src/auth.rs` | `TokenManager`: startup backoff, timed and on-demand refresh, `watch` publishing |
| `src/forward.rs` | One delivery attempt with 401 retry, the semaphore-capped forwarder loop |
| `src/retry.rs` | The retry actor and the NDJSON file (append, compact, legacy migration) |
| `src/local.rs` | Local-only paths and the daily NDJSON file they are written to |

## Testing

```bash
cargo test   # config parsing, file formats (NDJSON and legacy), local-path matching and line format
```

The runtime behaviour (backoff, retry rounds, 401 refresh, drain on `SIGTERM`) was checked end-to-end against a fake panel; those checks aren't part of the repo.

## Notes

- Built against **axum 0.7**. On 0.8 the wildcard route syntax changes to `/api/{*rest}`.
- Logging is `println!`/`eprintln!` to stdout/stderr, which systemd collects into the journal. Structured logging with `tracing` would be the next step.

## License

GPL-3.0
