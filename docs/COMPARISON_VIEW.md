# Simulation and mo-money comparison

The owner-authenticated console opens with a Russian comparison view. Select 1h,
6h, 24h or all retained data. MM2 synthetic execution and the registered mo-money
public wallet remain separate: no public position/value, paper NAV change or
turnover is presented as authenticated live bot profit. Each lane shows its
actual source period, clock, freshness and omissions. The original operator
controls remain under “Управление и диагностика”.

`GET /api/comparison?period=1h` reads the cached engine state, bounded console
SQLite NAV samples and a verified local public view. It never fetches the venue
or issues an engine command. A missing public view does not disconnect the
engine. NAV samples are estimates at best bid; reset/context changes and gaps
break continuity. The recent paper audit is bounded and is not a full ledger.

## Public refresher

```
python -B scripts/refresh_comparison.py --out /path/to/research-view --hours 24
python -B scripts/refresh_comparison.py --out /path/to/research-view --loop --interval-seconds 300
```

Only public GET activity/positions calls are made for the exact registered
account. Per capture: 90 seconds, 120 HTTP requests, 16 MB received, 10,000
activity rows, at most 24 hours. Successful captures keep canonical immutable
observer bundles, requests/coverage receipts and a view bundle. Only after
verification does `current.json` switch atomically. Failed activity capture
retains the prior pointer; its source clock ages naturally. Unknown fees,
opening collateral and strategy attribution stay unknown.

The root must be NEW dedicated secondary storage, initialized with an explicit
cache marker. Never point this collector at an existing research archive or
engine state. A 512 MiB / 10,000-entry quota reserves 130 MiB for peak publication
before capture. Two recent verified hot captures are retained; only indexed
obsolete hot copies inside this marked root are removed. Unknown staging is
retained. An immutable observer/view copy is kept once per UTC capture day with
its exact rolling source window and gaps; this is not complete calendar-day
coverage or evidence of a remote upload. Audit checkpoints are never deleted.

At the observed 9.8 MB/capture, retaining every five-minute full window would
consume about 2.8 GB/day. This cache instead grows about one checkpoint/day plus
two hot captures; the conservative reserve gives roughly 37 days at that size,
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

## Local and AWS wiring

Local `compose.yaml` includes an isolated `reference-refresh` container with no
engine/owner/Telegram credentials or engine network. The console mounts its
view volume read-only. It updates every five minutes independently of the engine.
The collector uses the verified console image; its inherited HTTP healthcheck is
disabled because it is not a web server. The engine still starts paused.

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
has occurred. Both manual stop/restart and host reboot recovery need host
acceptance. Existing production supervisor remains responsible for its original
three services only. Observe collector output and `/api/comparison` source age
without dumping container environment or private logs.

## Acceptance

Test successful verified publication, failed refresh retaining the pointer,
tampered/oversized artifacts, quotas, fresh vs stale source clocks, true empty
activity vs unavailable activity, null costs and context/gap NAV discontinuity.
Render desktop/mobile and exercise the shared period selector, action histories,
missing/stale reference, owner reauthentication and preserved pending command ID.
Finally validate real public capture and real engine/container/console wiring;
fixture screenshots alone do not establish production availability or profit.
