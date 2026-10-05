# Component and build contract

The workspace contains six Rust crates. Cargo.lock and the pinned toolchain are
part of the reviewed build; use `--locked` for builds, tests and checks. A build
must never require funded account credentials or send authenticated venue orders.

| Component | Boundary |
| --- | --- |
| `mm2-core` | Exact deterministic order identities, cash reservations, inventory, delayed notifications, cancel-before-post and conservative risk. No network, clock, persistence or UI dependency. |
| `mm2-emulator` | Deterministic event-time venue, post/cancel/notification delay, price-time queue approximation, finite trade volume, injected faults and serializable recovery. No exact venue FIFO or obtainable-return claim. |
| `mm2-strategy` | Deterministic bounded BUY-only rules with explicit identity/freshness and optional signal gates. Inputs are supplied by the caller. |
| `mm2-portfolio` | Finite market-keyed capital/position accounting, fees, reservations, risk and idempotent synthetic/on-chain receipt contracts. Receipt types do not submit blockchain transactions. |
| `mm2-live` | Pinned V2 SDK adapter, signing/preparation, asynchronous post/cancel, reconciliation, heartbeat/user-stream supervision and independent durable account lifecycle. Technical tests use mocks. |
| `mm2-engine` | Validated feed inputs, startup profile, selected strategy, multi-market paper integration, owner API/CAS receipts, streamed segmented journal recovery and bounded secondary publication. |

## Runtime selection and durable state

`scripts/start.py --mode simulation|trading|both|shadow|demo` selects the packaged
configuration without editing `.env`; PowerShell uses `scripts/start.ps1 -Mode`.
The wrapper defaults to public simulation and performs configuration preflight
before Compose. `demo` maps to native `emulator` and uses synthetic inputs. Every
profile starts paused. `both` has independent paper/trading capital and journals;
it cannot reuse a paper fill as evidence of a real fill.

Trading/both require explicit account authorization, allowlist, supported account
and deployment identities, private environment references and shared
`live_state_dir`; packaged profiles use `/data/account-live` across mode-specific
owner directories. Unknown outcomes preserve commitments. Live startup remains
paused pending authoritative recovery and current readiness. Native `--check`
parses public configuration; it does not verify a funded account or host.

The owner/paper journal rotates without deleting audit evidence. Default segment
and retained budgets are 8 MiB and 128 MiB, with separate finite identity/operation
quotas. Live has its own finite journal bound. All capacities must be monitored;
rotation is not infinite retention. Damaged or partial evidence refuses startup.
Offline maintenance acquires the same local fences and restores into new paths.
See [EXECUTION.md](EXECUTION.md) and [OPERATIONS.md](OPERATIONS.md).

## Toolchains and dependencies

The Rust toolchain is **1.98.1**, with edition 2024 and workspace Rust floor 1.98.
Docker and CI pin the reviewed Rust Bookworm image by digest. The live crate pins
`polymarket_client_sdk_v2 = 0.7.0` with `clob`, `ws` and `data` features, plus locked
Alloy/network dependencies. HTTP and WebSocket clients use Rust TLS; the Debian
runtime installs CA certificates. There is no requirement to install a separate
Node trading SDK or place private keys in an image/build argument.

The console runtime uses Python **3.13** and the standard library, including
SQLite. Local startup wrappers use Python 3.11+ (`tomllib`); use the pinned runtime
when matching CI. The React/Vite dashboard build uses pinned Node **22** and
`npm ci` from `apps/dashboard/package-lock.json`. Strategy TypeScript parity has
a separate pinned Node **24** job and runs with `--experimental-transform-types`.
Optional data/latency tooling installs its own pinned requirements from
`tools/data/requirements.txt` and `tools/latency/requirements.txt`; those packages
are not dependencies of the execution loop or console.

`Dockerfile` builds/tests the Rust workspace and copies only engine/emulator
executables into the non-root runtime. `Dockerfile.console` tests/builds the
dashboard, then tests the Python console and copies its static bundle. Compose
uses separate writable persistent volumes and bounded resources. `.dockerignore`
excludes `.env`, private key files, local output and scratch SDK copies. Secret
references are configured publicly; secret values remain runtime-only.

## Secondary owner API

`apps/console` has one private engine sampler independent of browser count. It
owns cached state, bounded SQLite history/logs, owner authentication, a compiled
dashboard and optional Telegram long polling. Engine decisions and primary
durability must continue without console/Telegram/SQLite. Browser sessions never
receive the internal engine token. No development test sends external Telegram
messages or needs real bot credentials.

Console routes are `GET /healthz`, `POST /api/auth/local`,
`POST /api/auth/telegram`, authenticated `GET /api/state`, `GET /api/history`,
`GET /api/logs`, `POST /api/control`, `PUT /api/limits` and `POST /api/logout`.
Unsafe requests require the exact allowed Origin/Host and an authenticated owner
session. The proxy preserves command ID, payload and expected version across
unknown outcomes. Cached state includes source freshness, real/paper separation,
journal budgets and secondary storage counters; unavailable state is explicit.
Sampled charts are cash history, not PnL or the primary trading ledger. See
[CONSOLE.md](CONSOLE.md) for the exact API/session bounds.

## Acceptance scope

CI covers formatting, Clippy, locked workspace tests/release build, Python suites,
reference/parity fixtures, frontend tests/build and container integration. Current
outcomes are recorded in [VALIDATION.md](VALIDATION.md); definitions here do not
claim the latest remote CI run passed. The V2 adapter is a limited technical path
validated in mocks. V3 positions, deposit-wallet EIP-1271, sells and on-chain
merge/redemption are unsupported. Funded venue/account acceptance, economic
holdout, selected-host soak/latency and real HTTPS/Telegram delivery remain
separate deployment gates.
