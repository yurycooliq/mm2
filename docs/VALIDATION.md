# Validation evidence

Local validation on October 1, 2026, Windows host with Docker Desktop's Linux engine. No funded account, signer, real order endpoint or Telegram recipient was used. Cloud deployment and region selection are deferred.

## Execution and sources

- Rust 1.98.1: 78 workspace tests pass; formatting and Clippy with `-D warnings` pass. Core/emulator include 27 tests, including five shared golden tapes, cancel/fill races, dropped confirmations, unknown outcomes, 429, restart, conservation, queue priority and bounded state. Engine tests include atomic command/execution records, replay, reservations, market identity and telemetry publication ordering.
- Five golden tapes match the isolated TypeScript `ExecutionRun` final-state contract. Nineteen reference/archive-quality Node tests pass. This is bounded execution parity, not full strategy equivalence. Reference restart is not a separate TypeScript persistence implementation; Rust replay is tested independently.
- All 55 data/latency Python tests pass with pinned optional dependencies installed, including real Parquet fixtures and local fake HTTP/WebSocket servers. No historical dataset was acquired for economic scoring in this repository.
- Synthetic latency sweep: five tapes times four configured delay profiles, 20 executions conserve collateral and settle. Four long-disconnect cases report unsettled cleanup. The tape's strategy decisions stay fixed; neither actual venue latency nor return is inferred.

## Real images and runtime

Both production Dockerfiles build successfully with pinned base images. The Rust build runs its workspace tests; the console build runs owner/authentication/transport tests and the TypeScript/Vite production build.

`scripts/verify_stack.py` passed eleven checks on real local images: paused authenticated startup, owner cookie/Origin boundaries, built frontend and token separation, durable command replay/conflict, delivered simulated fills, execution with console stopped, abrupt restart preserving cash/inventory/kill/receipts, shared-journal process fence, non-root/read-only roots, console session invalidation after restart and graceful engine exit. The observed graceful engine stop was 781 ms; this is one local observation, not a deployment SLA. Disposable resources were removed by exact IDs after checking their owner labels.

The Compose stack also started with its declared CPU/memory limits and separate volumes. Linux bootstrap produced two distinct random keys with mode 600, preserved the existing environment file and printed no key. PowerShell bootstrap was tested separately on the host. Private runtime files, build outputs and diagnostic artifacts are ignored by Git.

## Performance scope

Release core benchmark: 100,000 repeated unchanged two-slot quote calls in steady state after both orders are Live, p50 284 ns, p95 302 ns, p99 577 ns, maximum 73,104 ns on the current local machine. The loop includes cloning the input and reading the timer; it does not create/cancel orders on each sample and excludes transport, parsing, queue wait, durable writes, engine integration and dashboard. It cannot predict full order-lifecycle or matching-engine latency.

Periodic formatting/percentiles execute on one background worker; busy publication coalesces rather than queues work. The actor still performs a bounded snapshot copy. Immediate owner/fault publication, simulator candidate validation/comparison and required journal `fsync` have execution costs. These boundaries are documented and observable; zero overhead is not claimed.

## Public availability and remaining evidence

The bounded 35-second read-only public profile check reached API readiness but failed the public IP eligibility metadata transport on this host. It retried without reaching shadow readiness, kept TLS validation and submitted zero real orders. The check did not diagnose the exact transport cause or establish a host's Polymarket eligibility, co-location or authenticated order latency.

The default strategy is experimental and book-only. Sell, fees, settlement/redemption, multi-market positions, the authenticated SDK gateway, governing TWAP60 history and an untouched economic holdout remain explicit gates in [ROADMAP.md](ROADMAP.md). A demo with positions pauses at market rollover and retains the original contract ledger.

## Owner console and browser

Sixteen Python console tests and four dashboard Node regressions pass, including owner authentication, stale publication refusal, secret separation, matching receipt checks, unresolved-command retention on 401/403/429, and identity persistence before dispatch. TypeScript and the Vite production build pass on the pinned Node image.

Root verified the integrated local UI in the Codex in-app browser. Desktop/mobile rendering was also inspected through the available Brave browser at 1440x1100 and 390x844; both concepts and final screenshots were opened with `view_image`. Owner login, all four controls, confirmation dialogs, limits, simulated balances, history and the market-rollover stop reason were checked. A separate fixture browser exercised lost response, reload, tab close/reopen and definitive 409 recovery without real engine or Telegram traffic. Page width did not overflow; table scrolling remains inside its region.

The [ten-point comparison ledger](DASHBOARD_DESIGN.md) records layout, palette, typography, controls, metrics, tables, copy and intentional differences: illustrative $1,000 values were replaced by observed API values, and required stop/age/recovery information was added. [Desktop](design/desktop-integrated-qa.jpg) and [mobile](design/mobile-integrated-qa.jpg) artifacts contain simulated QA data.

The [checks workflow](https://github.com/yurycooliq/mm2/actions/workflows/checks.yml) reproduces Rust, Python, reference/dashboard and production-container checks on push. External Telegram delivery, Mini App embedding in actual Telegram clients and funded order execution remain unverified.
