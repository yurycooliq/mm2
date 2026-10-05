# Validation evidence

## Current checkpoint: October 5, 2026

This section describes the October 5 implementation checkpoint. The October 1 evidence
below is a historical baseline, not validation of the newer SDK, portfolio,
segmented journals or execution profiles. No funded venue orders, real Telegram
recipient or cloud deployment were used. Selected-host/eligibility, authenticated
account acceptance, HTTPS/Telegram delivery and economic holdout remain external.

| Current check | Confirmed result and scope |
| --- | --- |
| Console Python | **24 passed**: owner/auth/CSRF, stale publication and receipt recovery, mocked Telegram, real/paper summaries and bounded historical projection. |
| Operations Python | **23 passed** on Windows and pinned Linux: offline ledger integrity/fences/backup/restore, shared account sidecar, independent lane capacity, loopback-only soak and startup preflight. |
| Data/latency Python | **55 passed**, pinned optional dependencies, **no skipped tests**; includes Parquet and local fake HTTP/WebSocket servers. |
| Reference/strategy Node | **25 passed = 19 archive/execution + 6 scoped strategy parity vectors**; separate pinned Node 22/24 jobs. |
| Dashboard Node | **4 passed** for persisted command identity, uncertain/rejected outcomes, matching receipts and exact limits. |
| Final Playwright fixtures | **PASS** for three execution profiles, separate real/paper displays, mobile owner flow and retry of an unresolved command. These are controlled fixture accounts, not funded venue executions. |
| Final Rust workspace/fmt/Clippy/release | **137 tests passed**, formatting and Clippy with `-D warnings` passed; locked optimized workspace build passed inside the production Dockerfile. |
| Final production container build/integration | **Both images built; 13 checks passed** on the new local images. Includes all five packaged profiles, offline refusal of blank funded profiles, owner/receipt boundaries, delivered paper fills, console outage, crash recovery, journal fence and graceful shutdown. |

Detailed new code is documented in [STATUS_HANDOFF_2026-10-05.md](STATUS_HANDOFF_2026-10-05.md).
Passing individual subsets is not a complete workspace or remote-CI claim; nested
test counts must not be counted twice.

The final workspace pass uses two test threads, matching the two-worker build
budget in Docker/CI. Unlimited simultaneous durable-journal tests caused a
transport-fixture timeout on the resource-constrained local host. The isolated
integration retry and bounded final workspace pass succeed; no runtime freshness,
heartbeat or execution timeout was relaxed.

The container run took 23,453 ms; graceful demo-engine stop took 859 ms. These
are single local observations, not host or funded-execution SLAs. It submitted
zero live orders and removed only its owner-labelled disposable resources.
The local engine image is `sha256:435b93e925901f84d418ed77b487ab720c7a2b7a57f9af7c9c9eb463b54f808f`;
console is `sha256:d6da31422e6239b62e7b6401e1f2efcd0c5691020fc8e979e88b7b50a68ed335`.
These local image identifiers are not a published registry release. Remote CI
is tracked separately by the workflow and must be checked for the intended commit.

Regression coverage includes fill/account snapshot skew without duplicate
positions, actual maker-fee admission and hedge progress, eight-query bounded
reconciliation with no partial refresh, legal 100-to-2500 paper tick transitions,
portfolio loss halts after settlement, and replay of a Resume that never reached
the secondary actor before a crash.

### Implemented current boundaries

The package now has six Rust crates, fixed-point strategy/portfolio contracts,
paused demo/public simulation/shadow/trading/both profiles, separate lane capital,
a common trading/both account ledger/fence, multi-market paper identity and finite
journal rotation/recovery. Trading/both templates are unauthorized and blank by
default. The limited adapter pins V2 SDK 0.7.0 and validated Polygon deployment.
Its technical checks use mocks; deployed protocol/account/host compatibility is
not established. V3 positions, deposit-wallet EIP-1271, sells and on-chain
merge/redemption remain unsupported.

Owner/paper journal default capacity is 8 MiB per active segment and 128 MiB total,
with separate finite archive/command/operation quotas. Live retains its own
finite account journal; the companion paper has its own budget. No automatic
pruning deletes identities or obligations. Offline maintenance/soak scripts are
verified code, not evidence of a completed long-running host acceptance or
off-host backup.

Public paper observations drive an approximate queue/trade-volume model. Public
prints/book changes are not proof of our own fills or exact queue priority.
Synthetic complete-set merge/demo resolution and arithmetic multi-window runs
remain model assumptions. The new deterministic portfolio does not turn replay
into income evidence. Archive availability and import success are distinct from
point-in-time economic eligibility.

### Timing and evidence limits

Core/strategy nanosecond benchmarks measure local computation. Runtime decision
percentiles exclude transport, parsing, queue waits, execution durability and
snapshot publication. The historical 100,000-call benchmark below repeatedly
evaluates unchanged slots with orders already Live; it does not submit/cancel an
order per iteration. Loopback soak measures owner API responsiveness. None
establishes matching-engine arrival time or full authenticated post/cancel tails.

Production-storage durability is an execution dependency. Secondary formatting,
sampling and SQLite are bounded and separate, with measured copying and
coalescing costs. Zero overhead is not claimed. No untouched economic holdout,
realized trading PnL, funded canary, selected-host 24-hour soak or real Telegram
embedding/delivery is claimed by the current checks.

## Historical baseline: October 1, 2026

The following evidence is retained from the original demo/shadow package. All
counts, benchmark values, screenshots, public-readiness outcomes and statements
about then-unimplemented components apply to **that dated baseline only**.
Current mode/SDK/portfolio/journal state is described above.

Local validation on October 1, 2026, Windows host with Docker Desktop's Linux engine. No funded account, signer, real order endpoint or Telegram recipient was used. Cloud deployment and region selection are deferred.

### Execution and sources

- Rust 1.98.1: 78 workspace tests pass; formatting and Clippy with `-D warnings` pass. Core/emulator include 27 tests, including five shared golden tapes, cancel/fill races, dropped confirmations, unknown outcomes, 429, restart, conservation, queue priority and bounded state. Engine tests include atomic command/execution records, replay, reservations, market identity and telemetry publication ordering.
- Five golden tapes match the isolated TypeScript `ExecutionRun` final-state contract. Nineteen reference/archive-quality Node tests pass. This is bounded execution parity, not full strategy equivalence. Reference restart is not a separate TypeScript persistence implementation; Rust replay is tested independently.
- All 55 data/latency Python tests pass with pinned optional dependencies installed, including real Parquet fixtures and local fake HTTP/WebSocket servers. No historical dataset was acquired for economic scoring in this repository.
- Synthetic latency sweep: five tapes times four configured delay profiles, 20 executions conserve collateral and settle. Four long-disconnect cases report unsettled cleanup. The tape's strategy decisions stay fixed; neither actual venue latency nor return is inferred.

### Real images and runtime

Both production Dockerfiles build successfully with pinned base images. The Rust build runs its workspace tests; the console build runs owner/authentication/transport tests and the TypeScript/Vite production build.

`scripts/verify_stack.py` passed eleven checks on real local images: paused authenticated startup, owner cookie/Origin boundaries, built frontend and token separation, durable command replay/conflict, delivered simulated fills, execution with console stopped, abrupt restart preserving cash/inventory/kill/receipts, shared-journal process fence, non-root/read-only roots, console session invalidation after restart and graceful engine exit. The observed graceful engine stop was 781 ms; this is one local observation, not a deployment SLA. Disposable resources were removed by exact IDs after checking their owner labels.

The Compose stack also started with its declared CPU/memory limits and separate volumes. Linux bootstrap produced two distinct random keys with mode 600, preserved the existing environment file and printed no key. PowerShell bootstrap was tested separately on the host. Private runtime files, build outputs and diagnostic artifacts are ignored by Git.

### Performance scope

Release core benchmark: 100,000 repeated unchanged two-slot quote calls in steady state after both orders are Live, p50 284 ns, p95 302 ns, p99 577 ns, maximum 73,104 ns on the current local machine. The loop includes cloning the input and reading the timer; it does not create/cancel orders on each sample and excludes transport, parsing, queue wait, durable writes, engine integration and dashboard. It cannot predict full order-lifecycle or matching-engine latency.

Periodic formatting/percentiles execute on one background worker; busy publication coalesces rather than queues work. The actor still performs a bounded snapshot copy. Immediate owner/fault publication, simulator candidate validation/comparison and required journal `fsync` have execution costs. These boundaries are documented and observable; zero overhead is not claimed.

### Public availability and remaining evidence

The bounded 35-second read-only public profile check reached API readiness but failed the public IP eligibility metadata transport on this host. It retried without reaching shadow readiness, kept TLS validation and submitted zero real orders. The check did not diagnose the exact transport cause or establish a host's Polymarket eligibility, co-location or authenticated order latency.

The default strategy is experimental and book-only. Sell, fees, settlement/redemption, multi-market positions, the authenticated SDK gateway, governing TWAP60 history and an untouched economic holdout remain explicit gates in [ROADMAP.md](ROADMAP.md). A demo with positions pauses at market rollover and retains the original contract ledger.

### Owner console and browser

Sixteen Python console tests and four dashboard Node regressions pass, including owner authentication, stale publication refusal, secret separation, matching receipt checks, unresolved-command retention on 401/403/429, and identity persistence before dispatch. TypeScript and the Vite production build pass on the pinned Node image.

Root verified the integrated local UI in the Codex in-app browser. Desktop/mobile rendering was also inspected through the available Brave browser at 1440x1100 and 390x844; both concepts and final screenshots were opened with `view_image`. Owner login, all four controls, confirmation dialogs, limits, simulated balances, history and the market-rollover stop reason were checked. A separate fixture browser exercised lost response, reload, tab close/reopen and definitive 409 recovery without real engine or Telegram traffic. Page width did not overflow; table scrolling remains inside its region.

The [ten-point comparison ledger](DASHBOARD_DESIGN.md) records layout, palette, typography, controls, metrics, tables, copy and intentional differences: illustrative $1,000 values were replaced by observed API values, and required stop/age/recovery information was added. [Desktop](design/desktop-integrated-qa.jpg) and [mobile](design/mobile-integrated-qa.jpg) artifacts contain simulated QA data.

The [checks workflow](https://github.com/yurycooliq/mm2/actions/workflows/checks.yml) reproduces Rust, Python, reference/dashboard and production-container checks on push. External Telegram delivery, Mini App embedding in actual Telegram clients and funded order execution remain unverified.
