# Simulation and mo-money comparison

The owner-authenticated console opens with a Russian comparison view. Select 1h,
6h, 24h or all retained data. MM2 synthetic execution and the registered mo-money
public wallet remain separate: no public position/value, paper NAV change or
turnover is presented as authenticated live bot profit. Each lane shows its
actual source period, clock, freshness and omissions. The original operator
controls remain under “Управление и диагностика”.

The model panel separately exposes cumulative closed-position realized result,
initial cash, remaining cash, spending, included model fees and returned capital
from the exact synthetic portfolio snapshot. It verifies capital conservation;
stale, disconnected, missing or malformed snapshots show unknown values. These
totals span the ledger lifetime and do not establish selected-period net profit
or a mark-to-market value of remaining open positions.

The public wallet panel shows Polymarket's cumulative reported economic,
realized, unrealized and wallet-income estimates in USDC, with the response hash
and source clock. Selected-period change uses only the actual first/last retained
points and displays their UTC endpoints; fewer than two points give an unknown
change. A requested 24h range with only 23h of points is not described as a full
24h result. Daily source fidelity on an hourly grid remains visible. The API
estimate does not establish bot attribution, full cash-flow accounting or ROI.

`GET /api/comparison?period=1h` reads the cached engine state, bounded console
SQLite NAV samples and a verified local public view. It never fetches the venue
or issues an engine command. A missing public view does not disconnect the
engine. NAV samples are estimates at best bid; reset/context changes and gaps
break continuity. The recent paper audit is bounded and is not a full ledger.

The console sampler also stores a separate sanitized paper-action history before
raw snapshot compaction: at most 4,096 observations over seven days, inside the
existing 64 MiB SQLite cap. Comparison reads are side-effect free and show at most
256 retained rows per selected interval; sampling gaps and retention omissions
remain explicit. This secondary history survives normal console/engine restarts
without reading or changing an engine journal. Old compacted samples cannot
reconstruct missing actions.

Event identity includes observer/profile/source, boot, reset epoch, market and
audit sequence. Actual event and observation clocks remain distinct; logical
simulation time is never relabeled as UTC. The engine's published ledger locator
is a directory fingerprint, not account authentication or a global ledger ID.
Missing/changed locators, prefix rollback/conflicts and cross-boot uncertainty
are qualified separately. Observations across boots do not prove that their
capital ledgers are continuous. Current model totals still come only from the
current authenticated engine snapshot.

## Public refresher

```
python -B scripts/refresh_comparison.py --out /path/to/research-view --hours 24
python -B scripts/refresh_comparison.py --out /path/to/research-view --loop --interval-seconds 300
```

Only public GET activity/positions and one optional `/v2/user-pnl` call are made
for the exact registered account. Public collection budget: 90 seconds, 120 HTTP requests, 16 MB received, 10,000
activity rows, at most 24 hours. Successful captures keep canonical immutable
observer bundles, requests/coverage receipts and a view bundle. Only after
verification does `current.json` switch atomically. Failed activity capture
retains the prior pointer; its source clock ages naturally. Unknown fees,
opening collateral and strategy attribution stay unknown. Optional PnL failure
or invalid schema is reported separately and does not discard valid activity.
Its bounded raw response and exact GET receipt are stored inside the verified
observer evidence. No optional PnL request is retried.

The root must be NEW dedicated secondary storage, initialized with an explicit
cache marker. Never point this collector at an existing research archive or
engine state. A 512 MiB / 10,000-entry quota reserves 130 MiB for peak publication
before capture. Two recent verified hot captures are retained; only indexed
obsolete hot copies inside this marked root are removed. Unknown staging is
retained. An immutable observer/view copy is kept once per UTC capture day with
its exact rolling source window and gaps; this is not complete calendar-day
coverage or evidence of a remote upload. Audit checkpoints are never deleted.

At the earlier observed 9.8 MB/capture, retaining every five-minute full window
would consume about 2.8 GB/day. The actual v2 container capture measured about
12.7 MB per observer/view pair. This cache grows about one checkpoint/day plus
two hot captures; the conservative reserve gives roughly 29 days at that newer size,
not a capacity guarantee. Quota exhaustion stops publication and retains the
last verified view. Archive verified audit copies to a new destination before a
reviewed cleanup; never clear engine journals or account uncertainty to recover.
A partial/interrupted hot prune leaves the current view valid but blocks the
next collector preflight until the indexed cache is investigated.

The new collector uses documented v2 activity/positions contracts with bounded
cursor walks, exact decimal conversion and explicit schema validation. Official
migration reference: https://docs.polymarket.com/migrate/data-api-v1-to-v2 . v1
retirement is October 24, 2026. Legacy research-controller adapters remain
separate and need their own migration acceptance. API page completeness is never
assumed to establish full financial completeness.
PnL contract: https://docs.polymarket.com/api-reference/wallet/get-a-users-pnl-series .

## Local and AWS wiring

Local `compose.yaml` includes an isolated `reference-refresh` container with no
engine/owner/Telegram credentials or engine network. The console mounts its
view volume read-only. It updates every five minutes independently of the engine.
The collector uses the verified console image; its inherited HTTP healthcheck is
disabled because it is not a web server. The engine still starts paused.
The collector reserves 0.5 CPU and 192 MiB. A real cold 24h capture at these
limits published and verified in 21.6 seconds with about 122 MiB peak process RSS;
0.1 CPU failed to finish publication before the 90-second test deadline. These
are measured desktop-container results, not selected-host latency guarantees.
Artifact normalization, verification and publication follow the public HTTP
budget; it is not a hard wall-time bound for the complete process.

AWS uses a separate `mm2-comparison` project (`infra/aws/compose.comparison.yaml`),
so collector failure cannot trigger engine supervisor recovery. The console reads
`/data/research-view` inside its existing persistent console lane. Following
normal exact-release review, backup and paused deployment acceptance, prepare
only this secondary directory as UID/GID 10002, mode 0700. Do not restart or reset
the production engine merely to collect reference data. Start the collector with
the verified release image ID, without passing private runtime environment:

```
MM2_CONSOLE_IMAGE=sha256:<verified-image-id> docker compose -f infra/aws/compose.comparison.yaml up -d
```

This command is an operator deployment step, not evidence that AWS deployment
has occurred. Docker restarts the isolated public collector after process failure
or host reboot unless the operator stopped it. Both manual stop/restart and host
reboot recovery still need host acceptance. Existing production supervisor remains
responsible for its original three services only. Observe collector output and `/api/comparison` source age
without dumping container environment or private logs.

## Acceptance

Test successful verified publication, failed refresh retaining the pointer,
tampered/oversized artifacts, quotas, fresh vs stale source clocks, true empty
activity vs unavailable activity, null costs and context/gap NAV discontinuity.
Verify cumulative synthetic capital conservation, reported estimate provenance,
actual selected point endpoints, daily fidelity, missing optional PnL and an
insufficient point baseline independently of action-feed completeness.
Check saved observed actions after console and engine restart, oversized raw
snapshot compaction, sampled/retention gaps, source/boot/generation separation
and secondary SQLite failure. Never recreate an unobserved historical event.
Render desktop/mobile and exercise the shared period selector, action histories,
missing/stale reference, owner reauthentication and preserved pending command ID.
Finally validate real public capture and real engine/container/console wiring;
fixture screenshots alone do not establish production availability or profit.
