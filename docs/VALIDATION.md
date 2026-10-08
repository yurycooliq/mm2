# Validation evidence

## Simulation/comparison preparation: October 8, 2026, 17:30 UTC

The owner console now has separate MM2 simulation and mo-money public-observation
lanes, exact decimal values, source clocks, selected-period aggregates, recent
actions, positions, sampled NAV continuity and a complete sanitized JSON export.
Incomplete public accounting and bounded synthetic audit remain explicit;
neither turnover nor sampled NAV is claimed as authenticated bot profit.

An actual read-only v2 capture covered October 7 17:23:36 through October 8
17:23:36 UTC: 4,424 normalized events, 10 public GET requests, 3,840,424 received
bytes. Activity pagination completed, while financial completeness and MM2
attribution remain false. Observer ID:
`3922cb6d28d0dfc8d0e53613106487745994768b3f963c6be7870be7665b7621`.
The new dedicated comparison cache retains two verified hot copies plus
immutable daily rolling-window checkpoints, with explicit gaps and finite
quotas. Existing research archives and engine state were not pruned.

Deterministic engine tests reproduced timer starvation when durable periodic
work exceeds 100 ms. Overrun recovery now gives ready feeds an intake interval,
preserving durable writes and control/fault priority. Disconnection enqueue
failures retain overflow/closed classifications and the first fatal cause.
These reproductions do not establish the cause of historical AWS restarts.

Local Windows validation: 467 Python checks, 442 passed and 25 platform/dependency
skips (console 97, data 21, latency 34, operations 147, research 168). The six new
engine regression checks passed with 90 engine tests, formatting and targeted
Clippy. Dashboard 22 checks, production build, rendered fixture desktop/mobile
interaction and exact JSON-download parity passed. Full locked Rust/workspace,
Python Linux, parity/reference, new exact-image container acceptance and final
independent review remain required for this candidate; use final-head PR checks
for authoritative full-suite results.

AWS STS was verified expired on October 8. Ordinary console sign-in now loads in
the owner's browser; human authentication is pending. No candidate rollout,
real Telegram acceptance, cloud certificate trust or funded order occurred.
The historical checkpoints below retain their original dates and scope.

## Owner-directed preparation checkpoint: October 8, 2026 (Lisbon)

Windows research publication reproduced two transient directory-rename denials
on the unchanged `cd51558` candidate. Diagnostic-only retries of the same verified
staging directories succeeded after 62/63 ms with unchanged manifests. The lock
holder was not identified. Publication now retries only WinError 5 with an absent
destination and verified unchanged staging data, at most four rename attempts
with 50/150/300 ms delays. Permanent denial, corrupted staging, conflicting
publication and mutation during backoff still fail and retain staging evidence.
There is no archive deletion or retry of collection, journal writes or requests.

The final local Windows research suite ran 144 tests: 143 passed, one Linux-only
skip. Five new cross-platform fault-injection checks cover these failure modes.
The report generator now includes an allowlisted AWS preflight snapshot and
timestamps/ages in both views. It prioritizes restoring failed research access
without calling that failure an engine outage; historical diagnostics are never
labeled fresh. All 10 report tests passed locally, including three new access,
future-time and historical-age regression checks.
The current Python suite contains 422 checks: console 80, data 21, latency 34,
ops 140, research 147. Full Linux results for the latest candidate must be read
from [PR 5](https://github.com/yurycooliq/mm2/pull/5), rather than inferred from
the local Windows result. The preceding exact `cd51558` CI completed successfully:
244 Rust tests plus format/Clippy/release build, 414 Python tests without skips,
16 dashboard tests/build, 19 reference checks, 6 parity checks, 14 container smoke
checks and zero live orders ([run 37549266326](https://github.com/yurycooliq/mm2/actions/runs/37549266326)).

The separately authorized local MM2 CA and worker certificate are now enrolled
in the current user's Windows certificate store. RSA-3072/SHA256 software CNG
keys have nonexportable policies; this is not TPM evidence. Public chain and
official helper 1.8.5 signature verification passed. No cloud trust or machine
profile is enabled. October 8 STS reports the human session expired; two renewed
login flows returned browser 400 as reported by the owner. Latest server health
therefore remains unverified, and October 6 snapshots below remain dated evidence.
Independent review, paused rollout, real Telegram acceptance, machine access
renewal/revocation and financial/venue acceptance remain pending.

## Owner-directed recovery checkpoint: October 7, 2026 (Lisbon)

The recovery candidate addresses Telegram launch-data expiry with a live bound
session, missing cost declarations, per-window diagnostic context and control
reporting. Queue overflow and a closed consumer now have distinct fixed diagnostic
codes; queue size, whole-frame reservation, fault latching and stopping behavior
are unchanged. This diagnostic change does not claim to fix production restarts.

Verified application revision at this checkpoint:
`458764d2aaa99b1a568e10c86b3bc329ef4b1117`, based on `66ec7df`.
Subsequent documentation/guard changes do not replace exact-revision CI evidence.
See [draft PR 5](https://github.com/yurycooliq/mm2/pull/5) for final-head check status.

| Check | Confirmed scope |
| --- | --- |
| Python CI | 414 passed: console 80, data 21, latency 34, ops 140, research 139. Pinned Python 3.13; root-required atomic-runtime fixtures executed in CI. |
| Offline isolation | 359 candidate tests: 348 passed, 11 root-only skips. Separately mounted unchanged baseline: 128 passed. Real network/source-write/home/credentials/Docker-socket denial probes passed. This is owner-directed validation, not an independent broker receipt. |
| Dashboard and parity | Dashboard 16 tests and production build passed; 19 reference and 6 strategy parity checks passed in CI. |
| Rendered UI | Local Playwright API fixtures, desktop 1280×800 and mobile 390×844: expiry/reopen rendering, no relevant browser errors or width overflow, pending identity preserved, zero control/limits submissions. Not real Telegram acceptance. |
| AWS preparation | Read-only STS/access preflight verified after authorized human login. Disabled Roles Anywhere/SSM template passed AWS validate-template and cfn-lint 1.57.1; cfn-guard 3.2.1 checks default-disabled state, bounded sessions, exact transport actions/resources and certificate/anchor identity. No resources deployed. |
| Rust and containers | Locked workspace format/Clippy/tests/release and real container smoke remain required; final-head CI runs and their individual receipts in PR 5 are the authority. Do not infer these results from Python or UI passes. |

Production remained `e294475`, simulation paused, at the 23:50:10 UTC October 6
read-only snapshot. Both public feeds were streaming, `live_eligible=false` and
venue host geoblock reported `blocked=true` (IE/L). Bounded diagnosis observed
NRestarts 19 then 20 and fatal Binance `feed_queue_unavailable` rows; legacy codes
cannot distinguish overflow from closed consumer or establish historical causality.

Windows archive publication WinError 5 remains unresolved. Independent final-head
review, paused rollout/state preservation, real Telegram Desktop long-session
acceptance, machine certificate enrollment/renewal/revocation, venue eligibility,
full authenticated MM2 attribution/accounting, economic holdout and funded canary
remain separate gates. No automatic merge, state reset, approvals or trading
activation occurred. Prior checkpoints below remain dated historical evidence.

## Current checkpoint: October 6, 2026 — Builder Relayer and existing CLOB setup

The Relayer now accepts exactly one complete owner key/address pair or Builder
HMAC triplet. Tests cover the official HMAC vector, all 32 presence combinations,
controlling EOA binding, sensitive headers, GET pathname signing and exact durable
POST bytes. Lost, rejected, malformed and redirected responses never cause a retry.
The separate credential utility signs only CLOB L1 wallet-control authentication
for Polygon 137 and nonce 0; it performs GET time and GET existing-key derivation.
There is no key creation, revocation, funded order or transaction fallback.

Review fixed a provisioning race by checking the original environment snapshot
inside the existing update lock. The parent utility also requires a valid paused
boot/control identity, uses the local Docker socket with a minimal environment,
disables Docker logging, and captures private stdin/stdout. The Olympus Builder
source passed strict process-local validation; no values were printed or copied
into client files. Credentials have not yet been imported at this checkpoint.

| Check | Confirmed scope |
| --- | --- |
| Rust | **235 workspace tests passed**, including five Builder tests, eight CLOB utility tests and two CLI tests. Pinned Rust 1.98.1, locked optimized workspace build, format and workspace/all-targets Clippy `-D warnings` passed. |
| Operations/private setup | **116 Linux tests passed, no skips**, including safe Builder source parsing, exclusive auth selection, canonical padding, atomic stale-snapshot refusal, masked errors, private worker boundaries and supervisor failure classification. |
| Containers | **14 checks passed** on the optimized candidate engine and retained console, including the packaged credential binary refusing a foreign fixture wallet with network disabled. Zero live orders. Observed elapsed 25,218 ms, graceful stop 875 ms; these are local measurements, not an SLA. |
| Actual AWS state | Verified through SSM: `eu-west-1`, retained `0bffb99` release, simulation paused, API and public feeds ready, no funded actor, `live_eligible=false`. Wallet key present; CLOB, owner Relayer, Builder and RPC settings absent. AWS CLI identity and pinned SSH/SSM access renewed and verified. |

New exact-revision image export, selected-host rollout and authenticated existing
credential acceptance are subsequent checks. This checkpoint does not establish
venue/account eligibility or funded trading readiness. The current host's prior
negative geoblock evidence remains unresolved; saving keys cannot override it.

The retained AWS unit reported five restarts after its prior rollout. The masked
historical lifecycle messages do not establish their cause. Thirty current
Docker engine metadata reads took 14–21 ms, with none beyond the existing 400 ms
deadline. New diagnostics distinguish engine metadata timeout and other fixed
failure classes without exception arguments/output. The deadline, fail-closed
cleanup and recovery policy are unchanged; historical causality is unconfirmed.

## Previous checkpoint: October 5, 2026 — public feed access, TLS startup and private venue setup

The public source previously stopped before Gamma discovery whenever the
order-placement geoblock endpoint was negative or unavailable. Public read-only
discovery now proceeds independently. Current-market identity/rules, HTTPS,
response bounds, book freshness, connection recovery and every funded SDK gate
remain enforced. Fresh public books cannot grant permission to submit orders.

The actual eu-west-1 host returned `blocked=true`, country `IE`, while Gamma,
CLOB V2 REST and the market WebSocket supplied current market data. Both outcome
WebSocket snapshots were observed at 20 ms exchange age in a stateless probe.
These are public connectivity observations, not matching latency or trading
eligibility. The official documents and endpoint disagree; see
[POLYMARKET_ACCESS.md](POLYMARKET_ACCESS.md) for exact evidence and an unsent
support/co-location draft.

The first `5bb4762` rollout failed during Rust WebSocket startup: the unified
dependency graph enabled both Rustls providers, and the implicit provider choice
panicked. The preceding `55a37ce` release was restored and its paused API verified;
private settings and durable journals were retained. The engine now explicitly
installs the pinned AWS-LC provider before constructing Tokio or network workers,
with certificate and hostname verification unchanged. A regression exercises a
real default WSS TLS handshake under the dual-provider dependency graph.

The corrected optimized engine image passed a 35-second online public smoke on
the local Docker host (67 samples). Both fresh Polymarket outcome books and a
fresh Binance reference were observed together, with one stable paused shadow
boot, no funded actor and no orders. This is connectivity evidence, not latency
of authenticated order placement. The acceptance script now fails on absent
feeds, exits, faults, API loss, restarts, execution/control changes or cleanup
ownership mismatch, and includes publication lag in observer-time freshness.
Actual AWS acceptance remains a separate check before switching production.

The owner privately provisioned the wallet key. Server-side public-key derivation
confirmed the reviewed controlling EOA `0xa2300f310b59df6d0dec94576776ce5435bd0b3f`;
no key value was returned, no authentication signature or transaction was sent,
and the selected profile remained simulation. CLOB/Relayer/RPC credentials remain
absent at this checkpoint. The helper now accepts those existing settings through
hidden prompts and pinned SSH/SSM stdin without activation or network validation.
CLOB secret padding is normalized without changing its decoded HMAC bytes.

| Check | Confirmed scope |
| --- | --- |
| Rust | **220 workspace tests passed** on pinned Rust 1.98.1; includes read-only discovery, independent funded refusal, dual-provider default WSS startup and production healthcheck regressions. Locked optimized workspace build, format and Clippy workspace/all-targets `-D warnings` passed. |
| Console | **77 tests passed** in the production Linux container and again during image construction. |
| Operations/private setup | **97 Linux tests passed, no skips**, including 28 helper tests for hidden inputs, identity, atomic preservation, strict reply schemas, new credential modes and 31/32-byte CLOB padding, plus eight public smoke acceptance tests. |
| Data/latency | **55 tests passed**, with pyarrow 25.0.0 and websockets 15.0.1. |
| Strategy parity | **6 TypeScript vectors passed**; the corresponding Rust parity test passed in the workspace run. |
| Production containers | **13 checks passed** with the corrected optimized engine, including offline funded refusal, isolated console outage, durable command/crash recovery, ownership fence and read-only non-root images; zero live orders. Observed total 57,500 ms and graceful engine stop 1,250 ms are local observations, not an SLA. |

An additional Windows console run under local load had one socket timeout and
one time-sensitive fixture become stale (75 passed, one error, one failure).
The unchanged console source then passed all 77 tests in its production Linux
container and its image build. No production freshness threshold was relaxed.
Deployment and public-feed acceptance of this new release are checked after
image construction; the existing simulation remains paused until then.

## Previous checkpoint: October 5, 2026 — Telegram owner Mini App

The current changes implement automatic signed Telegram owner access, neutral 404
denial, hidden credential provisioning, private-chat reports and bounded fill
notifications. Production requires Telegram even before credentials exist; a
rejected Telegram account switch revokes the prior cookie. Masked auth 404 never
discards an unresolved browser command identity. Funded gates remain unchanged.

| Check | Confirmed scope |
| --- | --- |
| Console | **77 tests passed**, including real HTTP owner/non-owner/expired/replay/cookie/Origin boundaries, proxy trust, reports, minute-history retention/restart/gaps and notification cursor/cooldown behavior. |
| Operations/private setup | **78 tests passed on isolated Linux/Python 3.13, no skips**, including 17 hidden-input/secret transport, identity and atomic storage cases. No real key or Bot API credential was supplied. |
| Frontend | **16 tests passed**; TypeScript and production Vite build passed. Auth denial and unknown-command identity retention are covered. |
| Rust | **216 workspace tests passed**, locked optimized workspace build, Rust format and Clippy workspace/all-targets `-D warnings` passed. The only Rust change is an allocation-free aggregate held-market count in the bounded public snapshot; old snapshots deserialize it as unknown. Execution and private ledger types are unchanged. |
| Rendered frontend | Playwright 1.62.1, synthetic localhost console: desktop 1440x900 and mobile 393x852 owner entry/reload/logout, forged client owner ignored, signed foreign-owner denial, no token fallback and masked-auth retry retaining ID/CAS. No runtime errors or framework overlay. Browser plugin was unavailable; bundled Playwright was used. |
| Caddy boundary | Actual pinned Caddy and an isolated echo backend: verified fixture TLS and missing/forged/duplicate/malformed client-IP header replacement all passed; no external network or real credentials. |

The deposit curve stores one compact point/minute, up to 10,080 points/seven days;
the chart reads the latest 24 hours. It estimates cash plus complete positions at
fresh best bids without depth/fees; missing evidence produces gaps. A retained
aggregate proves an empty paper account even when older flat markets are omitted.
Telegram work stays in the separate capped console and adds no engine request.
Notifications sample execution evidence; restart baselines avoid historical
floods, and unknown send outcomes are not retried blindly. The engine journal is
the durable execution record.

The prior simulation release has been verified on AWS eu-west-1 with retained
EBS state, reboot recovery, DNS and strict public HTTPS at mm2.vstage.site. Its
Polymarket geoblock probe returned blocked=true. Real owner Telegram embedding,
message delivery and private funded credentials remain separate acceptance
checks; fixtures do not establish them. Deployment does not Resume execution.

## Previous checkpoint: October 5, 2026 — closeout and AWS package

This is local verification of the implemented package. No funded venue order,
real Polygon transaction, actual Telegram recipient or AWS deployment was used.
Authenticated account acceptance, selected-host eligibility/performance, real HTTPS/Telegram
acceptance and economic holdout remain external checks.

| Current check | Confirmed result and scope |
| --- | --- |
| Rust workspace | **215 tests passed** with two test threads; format, Clippy `-D warnings` and locked optimized workspace build passed on Rust 1.98.1. Includes **100 mm2-live tests**, exact signed recovery, finalized effects, imported/owned capital, receipt/account races, private-stream cleanup, shutdown fence release and persistence failure. |
| Console Python | **25 passed**: owner/auth/CSRF, stale publication and command recovery, mocked Telegram, independent lane summaries and public chain telemetry without signed bytes. |
| Operations Python | **61 passed on pinned Linux/Python 3.13, no skips**: source/image identity, private environment, mount refusal, encrypted retention, SSH revocation, engine restart preflight, independent secondary recovery, duplicate identities, timeout cleanup, backup/restore, lane capacity and Docker version/image-store refusal. |
| Data/latency Python | **55 passed**, pyarrow 25.0.0 / websockets 15.0.1, **no skipped tests**; Parquet and local fake HTTP/WebSocket servers. |
| Reference/strategy Node | **25 passed = 19 archive/execution + 6 scoped strategy parity vectors**. |
| Dashboard Node/build | **7 passed**, TypeScript and production Vite build passed; command identity and exact unsigned balance/signed loss formatting. |
| Playwright fixtures | **20 cases / 25 screenshots passed**: the earlier 16 controls cases at desktop 1440x1100 and mobile 390x844 cover simulation/trading/both, fixed initial capital, actual risk caps, signed loss, stale refusal, cancel confirmation and exact lost-ACK retry after reload. Pending closeout alone permits a fresh Resume when every readiness gate passes. Four additional final-build cases at 1440x1000 / 390x844 verify distinct batch/transaction labels, private-wire omission, no browser errors/overflow and durable Pause. |
| Production images/integration | **Both production images built from committed source; 13 checks passed**, including five profiles, blank funded refusal, paper fills, console outage, crash recovery, journal fence and graceful stop. Latest unchanged-source rerun observed 21,469 ms / engine stop 687 ms; local observations, not an SLA. The earlier failed run is recorded below. |
| AWS template/runtime | **7 isolated TLS/auth proxy checks passed** with the committed console image and reviewed Caddy; CloudFormation eu-west-1 lint, Compose validation and shell syntax pass. Release export requires clean source and matching application-image OCI source labels; no cloud resources are created. |

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

Remote container CI exposed a shutdown race: the actor acknowledged completion
before its destructor released the account journal fence. Shutdown now joins the
listener and workers, publishes the final snapshot, drops the journal and only
then acknowledges. A deterministic regression keeps the old actor alive while
reopening the same journal and verifies retained collateral, owned inventory and
sequence; the existing immediate runtime-restart test covers the complete path.

A loaded local BuildKit run also exceeded an extra two-second test wrapper around
owner Resume; the unchanged rerun passed. That independence test now uses the
production five-second command contract while keeping the paper actor permanently
unpolled, requiring the durable owner acknowledgement and primary orders, and
asserting paper version zero. Production timeouts and freshness remain unchanged;
the test establishes independence, not a disk-latency SLA.

An earlier local smoke of commit 9704d4c stopped before Resume with
`aggregate execution contradiction or mixed accounting` while selecting a demo
market (engine log UTC 15:04:55.928707 through 15:04:58.532369). A backward clock
jump that revisits a resolved market can produce this exact refusal; the failed
fixture journal was cleaned up, so that possible trigger is **not an established
cause**. The unchanged rerun passed all 13 checks with zero live orders. Windows
and Docker UTC agreed at the later 15:08:14 observation. The ledger invariant
was retained: a failing cloned candidate is never committed. Clock sync and
continuous operation remain selected-host acceptance requirements.

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

The OCI-pinned bundle passed a real local save/load round trip on Docker 29 with
containerd: all three exact image identities, Linux/amd64 platform and both
application source labels survived import. Independent archive inspection matched
all 21 public objects and their SHA-256/byte counts. The host installer now rejects
Engine versions below 29 before OS/package setup and requires the exact containerd
image-store indicator after startup. The guard is exercised with synthetic shell
commands; no host installation or daemon configuration change was performed.

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
