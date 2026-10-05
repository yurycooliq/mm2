# Validation evidence

## Current checkpoint: October 5, 2026 — closeout and AWS package

This is local verification of the implemented package. No funded venue order,
real Polygon transaction, actual Telegram recipient or AWS deployment was used.
Account compatibility, selected-host eligibility/performance, real HTTPS/Telegram
acceptance and economic holdout remain external checks.

| Current check | Confirmed result and scope |
| --- | --- |
| Rust workspace | **214 tests passed** with two test threads; format, Clippy `-D warnings` and locked optimized workspace build passed on Rust 1.98.1. Includes **99 mm2-live tests**, exact signed recovery, finalized effects, imported/owned capital, receipt/account races, private-stream cleanup and persistence failure. |
| Console Python | **25 passed**: owner/auth/CSRF, stale publication and command recovery, mocked Telegram, independent lane summaries and public chain telemetry without signed bytes. |
| Operations Python | **60 passed on pinned Linux/Python 3.13, no skips**: source/image identity, private environment, mount refusal, encrypted retention, SSH revocation, engine restart preflight, independent secondary recovery, duplicate identities, timeout cleanup, backup/restore and lane capacity. |
| Data/latency Python | **55 passed**, pyarrow 25.0.0 / websockets 15.0.1, **no skipped tests**; Parquet and local fake HTTP/WebSocket servers. |
| Reference/strategy Node | **25 passed = 19 archive/execution + 6 scoped strategy parity vectors**. |
| Dashboard Node/build | **7 passed**, TypeScript and production Vite build passed; command identity and exact unsigned balance/signed loss formatting. |
| Playwright fixtures | **16 cases / 21 screenshots passed** at desktop 1440x1100 and mobile 390x844: simulation/trading/both, fixed initial capital, actual risk caps, signed loss, stale refusal, cancel confirmation and exact lost-ACK retry after reload. Pending closeout alone permits a fresh Resume when every readiness gate passes. |
| Production images/integration | **Earlier closeout image checkpoint: both production images built; 13 checks passed**, including five profiles, blank funded refusal, paper fills, console outage, crash recovery, journal fence and graceful stop. Observed run 21,203 ms / engine stop 641 ms; local observations, not an SLA. |
| AWS template/runtime | **Earlier closeout image checkpoint: 7 isolated TLS/auth proxy checks passed** and reviewed Caddy; CloudFormation eu-west-1 lint, Compose validation and shell syntax pass. Release export requires clean source and matching application-image OCI source labels; no cloud resources are created. |

Nested test counts are not counted twice. The bounded Rust test concurrency matches
the two-worker build budget; runtime freshness/heartbeat limits were not relaxed.
Mock RPCs use public fixture keys and local servers. Browser fixtures prove owner
flow behavior, not funded execution or real Telegram embedding.

### Implemented execution and recovery

Five profiles start paused: demo, public simulation, shadow, trading and both.
The six Rust crates keep deterministic core/strategy/portfolio separate from
transport and from Telegram/SQLite/UI. Trading/both share the funded account
ledger and fence; the companion paper lane has its own finite cash and journal.
Startup capital is immutable in the owner quote-settings flow; UI displays the
actual configured funded risk caps.

The BUY-only adapter pins V2 SDK 0.7.0 and the validated Polygon deployment.
Standard-market merge/redemption is implemented for EOA, canonical proxy,
one-owner Safe and the pinned native-beacon DepositWallet. Direct wallets use a
separate worker that persists exact signed raw transactions before
broadcast, retains hash/nonce through lost acknowledgements and only credits
canonical finalized transaction effects. Missing receipt is never proof of
absence. A pre-effect account result cannot resurrect settled inventory.

Owned return, retained cost basis and daily/session budgets are tested separately
from imported inventory. Redemption adapter dust is observable but cannot create
owned risk capital. Failed transactions charge actual bounded gas and credit no
cash. Final net PnL remains unavailable because conservative fees and POL currency
conversion are not invented. Signed wire material remains in private journals.

Failed write/rotation/fsync poisons the journal writer until checked restart;
Reset cannot clear it. Terminal diagnostics are bounded in later checkpoints,
while original audit records remain retained. Positive inventory and uncertainty
are never removed to recover capacity. Private WebSocket ownership, reconnect,
heartbeat and shutdown are bounded and independently tested.

DepositWallet type 3 uses canonical ERC7739 CLOB order signatures with the
controlling EOA in authenticated headers and the contract wallet as maker/order
signer. Its gasless closeout signs a fixed owner EIP712 Batch. The actor syncs the
signed body and then a dispatch marker before one HTTP submission; recovered
already-dispatched batches are never posted again. A batch digest is not a chain
transaction hash. Full factory calldata/signature, wallet completion/execution
and finalized transaction-specific CTF/pUSD effects establish success. Sponsored
gas is observable but does not debit the owner's POL budget. Expiry is a separate
canonical deadline/unchanged nonce/token proof: no fabricated receipt or capital
credit, followed by Faulted/Reset/fresh Resume. Tests cover lost acknowledgement,
restart, fsync failure, forged proofs, duplicate credit and private-body omission.

Public read-only inspection on Polygon block 95002118 identified the supplied
profile wallet 0x04Ad8Cb08F04b58fbcEa273603d6cd151Ba8aDB4 as the pinned beacon
DepositWallet controlled by 0xa2300f310b59df6d0dec94576776ce5435bd0b3f. Current
E111 uses the known ADA100874 collateral adapter as outcome factory; the selected
ADA100Db closeout adapter has the same pinned CTF/pUSD/USDCE tuple. Actual BTC5m
Gamma/CLOB token IDs matched on-chain USDCE CTF derivation. CLOB coarse end dates
are no longer compared with exact intraday expiry: independently refreshed Gamma
establishes canonical slug, eventStartTime/endDate, reference and full token pair.
These public observations do not establish private credentials or funded access.
[Wallet authentication](https://docs.polymarket.com/trading/wallets-auth),
[deployed factory source](https://sourcify.dev/server/v2/contract/137/0xada100874d00e3331d00f2007a9c336a65009718?fields=all).

The supplied wallet has approvals for two known external full-balance redeemers.
The code checks both and refuses Resume/preparation while either remains approved;
no approval was changed. The owner must resolve that competing authority before
funded activation. Native-beacon implementation/domain upgrades, legacy UUPS
DepositWallets, V3 position orders, sells and unsupported markets fail closed.
No funded order, transaction, API credential creation or actual Telegram message
was performed. The public trading/both profiles retain owner_authorized=false.

### Deployment and evidence limits

The AWS package retains encrypted account storage, binds immutable Linux/amd64
images and contains only explicitly allowed public files. Runtime secrets and
ledger state are supplied separately on the host. Production engine recovery is
owned by systemd and must repeat release/storage/profile preflight; console/proxy
recovery is independent. All recovery starts paused. The ordinary candidate region
is eu-west-1 (Ireland); direct London eu-west-2 co-location needs separate acceptance.
See [AWS_DEPLOY.md](AWS_DEPLOY.md) and [LIVE_GATEWAY.md](LIVE_GATEWAY.md).

Owner/paper journal defaults are 8 MiB per active segment and 128 MiB total;
live retains a separate bounded account journal. Capacity needs a selected-host
operational plan. Offline backup/restore and loopback soak verify code, not a
completed 24-hour host acceptance or an off-host backup.

Public paper queue/trade-volume observations are approximate; public prints are
not proof of our own fills. Synthetic merge/demo resolution and arithmetic
multi-window runs remain model evidence. Pendulumflow import availability is
separate from point-in-time scoring quality and an untouched economic holdout.

Local computation benchmarks, owner API responsiveness and container shutdown
times cannot establish matching-engine arrival or authenticated post/cancel tails.
Storage durability remains an execution dependency. Secondary sampling/copying
has bounded nonzero cost. No realized income, funded canary, long host soak or
real Telegram delivery is claimed by these checks.

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
