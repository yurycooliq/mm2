# Validation evidence

## October 16, 2026 - opt-in fatal-ingress witnesses

Formatting, all-target Clippy with warnings denied and **385 Rust tests** pass,
with zero ignored tests (`rust-validation-5.log` in
`output/r18-stream-recovery-20261016`). Five regressions cover pre-rejection
CLOB clocks, Reference watermark resets, bounded sanitized error witnesses and
exact read-only level quantities with absent zero distinguished from unknown.
The native replay fixture checks full-report noninterference after restoring only
the explicitly disabled diagnostic selection fields.

The optional `--diagnostic-ingress` mode records up to 32 actual ingress failures
with hashes, source generation and available pre-error state. It preserves
execution/accounting and disables selection; the default report has no diagnostic
fields or frame hashing. Opt-in frame hashing adds linear processing cost. A
diagnostic rerun of a retained tape is not a new holdout or proof of income.

## October 16, 2026 - bounded capture writes and explicit recovered-input quality

Formatting, all-target Clippy with warnings denied and **380 Rust tests** pass,
with zero ignored tests. The complete accepted run is `rust-validation-3.log` in
`output/r18-stream-recovery-20261016`. Two real in-memory WebSocket regressions
cover stalled sends/close and successful application heartbeat/native Ping/Pong
ordering. Three quality regressions cover actual two-book restoration, timeout,
exact-age/after-close completion, malformed snapshots and historical recovery
deficits surviving a generation reset.

Three additional paused-time retry regressions cover repeated peer-close 1013
signals, capped jitter, stable-session reset, preservation of gaps/generations and
capture/market deadline clipping. Requested retry delays remain diagnostic data;
the patch cannot prevent remote closure or reconstruct missing trade evidence.
A further regression verifies frame admission under the writer lock at expired
global and exact socket cutoffs, while late genuine Gap evidence remains retained.

The default strict protocol and all sealed historical evidence retain their
meaning. Opt-in `recovered_native_v2` separates fatal faults from verified native
recovery without changing execution decisions or forgiving actual capture gaps.
Exact-image acceptance, whole-report strict compatibility and fresh bounded
public collection have separate receipts. No disconnect cause, clean economic
holdout, repeatable income or funded readiness is established by these tests.
See [public experiments](PUBLIC_EXPERIMENTS.md).

## October 16, 2026 - paired paper policy and accelerated common-input experiments

Formatting, all-target Clippy with warnings denied and **371 Rust tests** pass,
with zero ignored tests. The accepted complete run is `rust-validation-14.log` in
`output/r18-accelerated-20261016`; earlier compile/fixture attempts remain there.
Real-clock actor fixtures now use scoped test-only wall/monotonic clocks, retaining
their freshness, cancellation, grid-change, journal replay and deadline checks.
Production SystemTime/Instant behavior and the one-second freshness limit remain.

The opt-in public-paper policy preserves paired entry admission, lifetime
condition loss allocation, cost-capped ordinary hedges and staged bounded repair.
Tests cover pending possible fills, retained partial order identity, legacy unknown
allocations, immutable policy/strategy profiles and strict causal V3 public prints.
Historical V2 execution remains replayable. Native common-input replay tests cover
separate ledgers, unknown terminal value and training-only selection without
substituting a different winner after observing holdout failure.

The actor and replay now share native frame watermark filtering, split-delta
snapshot recovery and valid-prefix admission. Three regressions cover both-book
recovery, invalid suffix cutoff and obsolete validated snapshots. Missing REST
bootstrap/refresh evidence and actor scheduling/fatal-latch parity remain explicit.

The later capture-only patch adds bounded socket failure categories, close codes,
capacity and processing/heartbeat timings without retaining peer/error text.
Subscription-send failure now records one gap, advances the generation and takes
the common reconnect backoff. Three new tests cover those diagnostic/retry cases.

Replay and portfolio synchronization now read small account/progress/risk values
and borrow unresolved positions instead of cloning complete telemetry snapshots.
The new regression compares the original snapshot-based counters and persisted
state across partial fills, causal exclusions, cancellation, market rollover,
loss/settlement, UTC reset and audit eviction. Execution operations and validation
remain unchanged. Complete old/new report equivalence and observed-input timing
require separately retained runner receipts, not an economic profit claim.

Python collects 520 tests: **495 pass, 25 platform/optional skips**, including 128
console tests. Node passes **25 checks**: six strategy parity and nineteen
execution/reference checks. Both the first `516108f` and shared-ingress `169523b`
runtime images pass fourteen container integration checks and five explicit
experimental-profile acceptance checks, with separate immutable-image receipts.
The later recorder diagnostics do not change runtime/replay source or the retained
capture; their source and future collector build remain separately bound.
See [public experiments](PUBLIC_EXPERIMENTS.md).

The full R17 terminal prefix was independently audited: 98,085 records, 167,787
execution operations, 68 filled virtual orders over 36 markets and no observed
accounting discrepancy or risk breach. Its final model result is -$3.5044 with
zero open cost. That result does not establish real profitability. New parameters
and assumed execution latencies are experiments; no completed clean economic
holdout or funded readiness is claimed. Existing global $10/$20 limits remain.

## October 15, 2026 - public-paper filter order and quote subset admission

Fresh-source formatting, all-target Clippy with warnings denied and **341 Rust
tests** pass. Public-paper entry filtering now precedes risk admission; a complete
affordable set is preserved, otherwise a bounded subset is selected in existing
strategy order. Regressions cover both outcomes, strict budget equality, missing
and adverse references, retained unresolved markets and cancel-in-flight orders.
Initial test setup corrections remain in local logs; the accepted run is
`rust-validation-3.log`.

Python checks pass 495 tests with 25 platform/optional skips, including 128 console
tests. Six TypeScript strategy parity and five execution reference cases pass.
The full stopped R16 night, independent accounting, immutable release images and
fresh R17 startup require separate receipts under
`output/risk-admission-fix-20261015-r17`. The $10 daily gross loss and $20 realized
drawdown caps, settlement accounting and funded behavior remain unchanged.
See [R17 policy and replay limits](PAPER_STRATEGY_R17.md). This does not establish
counterfactual profit, a completed holdout or funded readiness.

## October 14, 2026 — experimental public-paper entry and inventory repair

Fresh-source validation passes **338 Rust tests**, formatting and all-target Clippy
with warnings denied. Public-paper new entries use a bounded one-second/one-basis-point
Binance direction filter; reference warmup/staleness fail closed. Passive complement
repair must strictly lower the gross-loss bound, including a minimum-sized overbuy.
Symmetric repair, partial-fill bounds, directional admission and unchanged venue tick
cancel/drain/replay contracts are covered. Initial failed local fixture/setup attempts
remain in operational logs; the accepted result is `validation-final-pass.log`.
Existing Python data/latency/ops/research suites pass 392 collected tests with 25
Windows/optional skips (367 passed). Six TypeScript strategy parity cases and five
execution reference cases pass. Console, exact release, container checks, R15 recovery
and R16 server acceptance have separate receipts under
`output/simulation-inventory-fix-20261014-r16`. Historical execution and global risk
limits are unchanged. See [policy and limitations](PAPER_STRATEGY_R16.md).
No completed holdout, real profitability or funded readiness is established.

## October 14, 2026 — discard obsolete full native snapshots

R14 never received Resume. It remained Paused with zero spending and then latched
the existing repeated-input guard. Its retained public clocks show three full
snapshots 6–16 ms older than the current book, with nonregressing receive clocks.
The complete rejected provider frames were not retained. A constructed fixture
using those exact clocks fails with the old ingress and passes with this repair;
the fixture's levels and trade are not a reconstruction of the missing payloads.

Ingress now maintains separate exchange watermarks for each outcome and discards
only well-formed full snapshots older than an already forwarded book update or
tick. It keeps independent trade witnesses, does not advance book freshness, and
cannot initialize recovery from obsolete depth. Malformed depth and regressed
receive clocks still reach strict validation; incremental and tick regressions
remain errors. Frame parsing remains atomic. The native snapshot deadline,
source-generation shutdown, repeated-failure guard and every strategy/risk/journal
limit are unchanged. Bounded diagnostics expose discarded counts and watermarks.

The fresh-source check passes **332 Rust tests**, formatting and all-target Clippy
with warnings denied. It covers outcome isolation, retained trades, unchanged
freshness and invalid depth/identity/receive inputs. Earlier incomplete probes and
test-fixture corrections remain in the operational evidence; only the final fresh
compilation and actual failing old-code regression establish this comparison.
Exact release images, stopped full-journal recovery, selected-host preflight and
original Resume/Pause receipts have separate evidence under
`output/snapshot-ordering-fix-20261014-r15`. A new daily experiment exists only
after its original Resume and independent monitor are confirmed. Synthetic
execution does not establish real profitability or economic suitability.

## October 13, 2026 — join the complete source generation

The fee refresher could finish shutdown before the CLOB/Binance futures. Selecting
that first completion dropped their enclosing JoinSet, which requests cancellation
without joining the nested workers. A deterministic regression confirms that the
previous coordinator acknowledged shutdown before its socket worker completed.
This permits an overlap during source refresh; it does not establish which clock
failed in the historical R12 frames, which were not retained.

The coordinator now cancels the remaining future and joins both before returning,
preserving the first failure. The existing six-second owner deadline still stops
an unresponsive generation and prevents a new generation from starting. No risk,
feed-age, backpressure or journal bound is increased. The FIFO recovery and public
clock diagnostics from the preceding patch remain included.

The final check passes **329 Rust tests**, formatting and all-target Clippy with
warnings denied. The premature-acknowledgement regression fails with the previous
select-first coordinator. The healthy R13 installation was never resumed: its
15-minute selected-host check passed 181 sampled observations through four markets
with zero costs and owner commands. Its complete preparation is preserved when
replaced; it is not a daily experiment. Exact images, preserved R12 recovery and
the actual new run are bound by `output/feed-generation-fix-20261013-r14` receipts.

## October 13, 2026 — preserve queued native recovery

R12 stopped after about 29 minutes with a confirmed protective Pause; it did not
complete a day. Its original failure sequence contained a native snapshot timeout
and two clock validation failures. The rejected frames and exact clock subtype
were not retained. A new isolated Paused public probe reproduced a native timeout;
it subsequently stopped on bounded feed backpressure while local checks were
running. That observation does not identify the old clock subtype.

A deterministic regression exposes a concrete recovery defect: the first split
crossed delta stopped the bounded batch, discarding already queued native books.
Its processing-time barrier also discarded valid later inputs from the same
source generation. The repair keeps that FIFO batch and independent trade
witnesses, skips intermediate deltas, and inhibits new quotes until both native
books are validated. Genuine invalid input still cuts its suffix; the original
source-generation barrier, 1,000 ms native deadline and three-failure guard remain.
Clock failures now retain distinct fixed public receive/exchange details and
bounded prior clock values, without raw provider content or credentials.

The old source fails the new queued-recovery regression. The repaired source
passes **327 Rust tests**, formatting and all-target Clippy with warnings denied,
including trade-witness retention during recovery. Exact image, stopped-copy
replay, selected-host installation and the new daily run have separate receipts
under `output/feed-recovery-fix-20261013-r13`; this check is not a completed day.
Strategy, capital, loss, drawdown, feed-age and journal limits are unchanged.

## October 13, 2026 — native snapshots after split public updates

The first R11 installation remained Paused and failed its pre-Resume acceptance:
no Resume was sent, no daily run began, and all costs/returns/reserves remained zero.
Its complete journal and original failed-command receipts are retained separately.
A public-only domain probe observed 17,798 events, including nine transient crossed
updates and ten subsequent deltas after book invalidation. Those are input events,
not fills. Normalized rejected events are retained; the full accepted tape and the
first primary rejected frame were not captured.

Split deltas are never treated as a trusted complete book. After a crossed delta,
new quotes are inhibited until full native snapshots have re-established both books.
Intervening deltas cannot initialize the cleared books. Actual public trade witnesses
and possible-fill reservations retain their separate processing. The native wait is
bounded by the unchanged 1,000 ms feed-age policy; timeout escalates to the existing
source-generation recovery, whose repeated failures still latch a fault. Full
snapshots remain subject to identity, clock, depth, spread and freshness admission.
An idempotent repeat of an already-confirmed tick cannot refresh book timestamps;
unsupported ticks, incompatible changes and regressed clocks remain errors.
Only fixed public failure categories/details are logged.

The final offline Rust check passed **326 tests**, formatting and all-target Clippy
with warnings denied. Its source binding and subsequent exact-image/container and
selected-host receipts belong to `output/audit-fixes-20261013-r12`. The 520 Linux
Python checks and six TypeScript parity cases from the previous candidate cover
unchanged Python/TypeScript source; image integration is verified again for the
final images. A daily test exists only after its original Resume receipt and run
identity are confirmed. Risk, capital, feed-age and journal quotas are unchanged;
public observation and synthetic execution do not establish real profitability.

## October 13, 2026 — exact paper costs, feed recovery and observed queues

New paper histories persist an `exact_accounting` version at a zero-spend
baseline. Exact cumulative outcome costs replace proportional allocation before
merge/redemption and prospective risk admission. Legacy operations retain their
historical accounting; legacy cost uncertainty blocks new quotes and cannot be
silently upgraded. The retained 34-operation R10 regression still reproduces the
original legacy result. With the new version its residual basis is $0.80 and the
additional lot is rejected by the unchanged $10 gross-loss policy.
Automatic refill is also deferred until an explicit new quote operation has
passed admission on the updated position. Legacy refill semantics remain intact.

Optional informational timestamps/assets and normalized BUY/SELL casing are
handled separately from required execution witnesses. Semantic trade fingerprints
deduplicate spelling differences. Required input and full-frame atomicity checks
remain strict. Core validation failures request a source-generation refresh without
owner commands, risk resets or released possible fills; three failures within one
minute latch a fault. Only fixed public failure classes are logged.

Versioned public inputs persist observed depth at the best bid and relevant order
prices. Queue decreases are credited only from witnessed prints, and a print is
applied only at its own price. This is a conservative synthetic queue model, not
calibrated venue execution. Sub-minimum residuals may retain an already-live legal
complement at its immutable price/size when pair cost and risk permit; they never
create a new undersized lot or recover queue priority. Telegram status explains
waiting, working virtual orders and the remaining gross-loss budget before open risk.

Offline Rust formatting, all-target Clippy and regression tests, console checks,
image integration, historical ledger recovery and selected-host deployment have
separate receipts under `output/audit-fixes-20261013-r11`. Deployment and a completed
day must be confirmed there; this source description does not establish either or
real profitability. The final local checks passed **324 Rust tests**, formatting
and Clippy with warnings denied; **520 Python tests** passed in Linux without skips
(128 console, 157 operations, 21 data, 34 latency and 180 research). The bounded
historical TypeScript quote oracle passed six cases. Image and selected-host
checks have their own receipts. No capital, loss, drawdown, feed-age or journal
limit is raised.

## October 13, 2026 — optional CLOB fields and feed failure diagnostics

The retained R9 engine log records a fatal CLOB `invalid_public_input` immediately
before `feed relay stopped unexpectedly`. The rejected original wire frame was not
retained, so its exact parser subtype cannot be established retrospectively.

The official CLOB SDK permits missing, null and empty optional decimal metadata.
The previous parser reproducibly rejected these forms for quote hints, trade fee
metadata and old tick metadata. They now remain absent metadata. Required book
levels, trade prices, present trade quantities, clocks, identity, frame bounds and
atomic batch validation retain their strict checks. A trade without a quantity
emits no execution witness; indicative BBO metadata never creates depth or fills.
See the [official SDK schema](https://github.com/Polymarket/ts-sdk/blob/main/packages/bindings/src/subscriptions/clob.ts).

Feed failures now log a fixed public parser detail, and relay exit reporting
distinguishes a returned failure, unexpected completion, panic and cancellation
without echoing rejected content. No strategy, capital, loss, feed-age or journal
limits changed; startup still requires an explicit Resume.

The previous code fails three new optional-field regressions. The candidate
passes formatting, all-target Clippy with warnings denied and **315 Rust workspace
tests**, including seven new regressions. Original R9 preservation, stopped-copy
replay, exact-image checks, deployment and the new daily test have separate dated
receipts under `output/feed-relay-fix-20261013`; local tests do not establish them
or economic suitability.

## October 12, 2026 — paper quote ordering and UTC risk windows

The actor recalculates paper quotes on the candidate after fill/cancel acknowledgements,
venue and fee rules, immediately before applying intents. Partial inventory therefore
cannot retain an ordinary lot from the earlier position. Cancel races keep their
possible-fill reservations until terminal acknowledgement.

An explicit journaled `risk_window` input advances the daily gross-loss window from
caller-supplied UTC before active admission, including idle ticks. Synthetic execution
time used to drain rollover events cannot advance that window. Legacy `advance` inputs
retain their original accounting semantics, and a day change preserves drawdown, old
inventory, reservations and latched halts. Late resolutions are charged at their recorded
processing time without rewinding the risk clock. No configured limits were increased.

Offline locked workspace formatting, Clippy with warnings denied, and **308 Rust tests**
passed, including seven new regressions. The retained first R8 journal segment replays
with its original execution/accounting under the new code; this is compatibility evidence,
not a new forward run or economic holdout. Full stopped-journal recovery, exact-image
container checks, selected-host installation and a fresh one-day run have separate receipts
under `output/simulation-restart-20261012`; do not infer their completion from unit tests.

The prior additional Telegram operator code is included in this release. The primary
owner and additional allowlist must be retained when migrating from the console overlay.

## October 12, 2026 — additional Telegram operator access

The optional bounded Telegram allowlist preserves the primary owner and signed
Mini App authentication. Both operators receive private replies; charts go to the
requesting chat. A pending command, including an older owner-only record, cannot
be taken over by another operator. Session TTL, launch-data age, replay fences,
aggregate rate limits and all execution risk limits retain their existing bounds.

The local Python console suite passed **126 tests**. The existing operations suite
passed **148 tests with 13 platform skips** on Windows. The final Linux operations
suite passed **157 tests without skips** offline. Nine console overlay tests also
passed on Windows, covering an unchanged
engine/proxy, exact IDs/images, safe no-op and refusal of unrelated deployments.
These are local fixture checks; actual Telegram-client acceptance is a separate gate.

A real isolated Compose fixture also verified that a never-started guard container
degrades only console recovery in the original supervisor, stays outside Compose's
managed-container filter, and is removed after a successful replacement. This
prevents simultaneous recovery from restoring the previous console during an update.
The engine supervisor continues polling throughout this procedure.

The console-only recovery overlay uses an independently pinned image and never
changes the current release, runtime secrets, active engine profile or engine
container. Its source/profile/image gates stop applying it after an unrelated
deployment. A later full release must carry the configured additional IDs forward.

## Simulation inventory cost and prospective loss admission: October 11, 2026

R7 stopped after a partial 18-minute run at the unchanged gross realized-loss
guard. Exact journal replay identifies new lots on the already overweight side
and complement quotes whose combined price with owned inventory exceeded $1.
The simulation policy now quotes only the lagging hedge, caps it by exact BUY
fill costs and the retained portfolio basis, and checks unresolved positions and
all possible fills against the original daily-loss/drawdown headroom before new
intents. See [STRATEGY.md](STRATEGY.md). No risk, capital, feed or journal limit,
realized accounting policy, old receipt, or funded quote policy was changed.

Formatting, all-target Clippy with `-D warnings` and **301 Rust workspace tests**
passed, including **87 engine library tests**. Thirteen new regressions exercise
partial/cancel-race costs, unknown legacy checkpoints, symmetric conservative
price caps, dust, old unresolved risk, retained-order deduplication, independent
possible fills, strict unchanged loss thresholds, drawdown and actual merge/
redemption of admitted partial-fill batches for either final outcome.

All **499 Python tests** passed in dependency-complete Linux, with no skips.
The unchanged historical strategy/reference **25 Node checks** passed. Simulation
inventory management is a separate contract from the historical quote oracle.
Timing-sensitive journal tests initially failed on Docker Desktop's slow temporary
filesystem; the successful complete suite and production image build use temporary
test journals on tmpfs, preserving all production freshness and timeout rules.
Selected-host durable ledger recovery remains a separate deployment check.

On the same short observed R7 tape, a synthetic counterfactual with the new rules
did not trip the loss guard. It remained negative and spent most quote opportunities
abstaining on a below-minimum imbalance. This is a regression diagnostic fitted to
the incident, not an independent holdout, a full day, profitability, real PnL or ROI.
Runtime image/browser integration and the fresh 24-hour experiment have separate
dated receipts; waiting due to `loss_budget` or `inventory_dust` must not be presented
as continuous active quoting or economic acceptance.

## Public simulation resolution and inspectable rollover failure: October 10, 2026

R6 exhausted 64 held unresolved markets because public rollover had no outcome
worker. The fix reads exact standard binary CTF payouts at one canonical finalized
Polygon block, journals checked evidence before synthetic credit, retains unknown
positions and preserves all original capital/risk/storage limits. A failed paper
rollover now remains paused with its diagnostic API available. See
[the resolution and rollback contract](PAPER_RESOLUTION.md).

Formatting, all-target Clippy with `-D warnings` and **288 Rust workspace tests**
passed, including **84 engine library tests**. Ten new regressions cover finalized
outcome/token identity, unfinalized/split/foreign/reorganized observations, more than
64 windows with unchanged capacity and finite cash, exact replay/idempotency,
delayed settlement across a complete-set merge, durable credit/sync failure, and
the retained capacity-faulted actor with Pause acknowledgement and blocked Resume.
Two existing batching tests now admit fresh timestamps and drain from the current
logical clock, preserving the production freshness policy on slow storage.

All **499 Python tests** passed in dependency-complete Linux (console 118, data 21,
latency 34, ops 148, research 178), with no skips. The strategy/reference **25 Node
checks** passed; the unchanged dashboard's 30 checks and production build are also
validated by the console image build. Initial Windows sandbox and read-only Linux
fixture attempts failed because their temporary directories were unavailable;
the successful run used an isolated writable Linux project copy.

The read-only production diagnostic verified a real completed R6 market against
the public CTF payout vector and independently derived both token IDs. This proves
the public observation path for that market; no engine ledger or funded wallet was
changed. Exact-image smoke, stopped-ledger recovery and the repeat forward run
remain separately recorded deployment receipts. A running repeat is not a completed
24-hour acceptance or economic result.

## Automated maker fee evidence and live reconciliation: October 9, 2026

Public quote readiness now requires strict current-market fee metadata, refreshed off
the execution path every 30 seconds with a 90-second maximum age. Missing fields,
wrong identities, unsupported protocol or nonzero maker schedules block new work.
Accepted paper policies are durable replay inputs; legacy amounts remain unchanged.
The authenticated adapter freezes its fee policy before submission and compares it
with canonical finalized V2 `OrderFilled.fee` evidence. Unknown coverage stays null;
pending new fills block quoting, and actual fee/role discrepancies retain the proof
before faulting. Collections and receipt-query fanout remain bounded. See
[the complete contract and limitations](FEE_RECONCILIATION.md).

Local formatting, all-targets Clippy with `-D warnings` and **278 Rust workspace
tests** passed, including **74 engine library tests**. Seven new regressions cover
explicit missing/invalid fee fields, exact identity/expiry, canonical finality/reorgs,
actual fee matching/idempotency/mismatch, malformed fill receipts, legacy financial
replay and the public actor's fee gate. Existing integration fixtures now supply
explicit synthetic fee evidence and continue testing Reset/source-generation fences,
paper/live isolation and tick cancellation/draining.

Local Python suites collected **499 tests**, with **25 platform/optional dependency
skips** and no failures (console 118, data 21, latency 34, ops 148, research 178).
All **55 Node tests** and the dashboard type check/production build passed. Final
commit CI, exact-image integration and selected-host recovery/run receipts are stored
with the deployment report; these local results alone do not establish those gates.

No authenticated funded execution was performed. Fee comparison tests use synthetic
receipt fixtures; the eligible account's real behavior remains an external gate.
Zero verified maker exchange fees do not establish zero gas, rebates, infrastructure
cost, executable open-position valuation, net profit, real PnL, ROI or a completed
economic holdout. Older binaries cannot replay new fee operations; rollback must
preserve the newer journal and use an isolated pre-deployment copy for investigation.

## CLOB backpressure and durable trade batching: October 9, 2026

The R2 public simulation on `b6a9646` stopped at 11:51:44 UTC with fatal
`feed_queue_overflow source=clob`. Code review reproduced immediate failure on
transient Full and valid native frames exceeding the queue's capacity. It also
found per-trade full-state comparison/sync overhead and a relay that could hide
shutdown while blocked. Exact frame size and disk latency at the AWS failure
remain unknown. See [the detailed queue review](QUEUE_BACKPRESSURE_REVIEW.md).

Fully validated CLOB frames now enter existing bounded queues in ordered chunks
of at most 16, with one finite admission deadline, explicit pending/discarded
suffix accounting and an independent fatal timeout signal. Reference coalescing
is confined to Binance. Emulator actors drain up to 16 already queued inputs,
validate ordered trade operations and sync one transaction before publishing
candidate finances; small segments reduce the batch size. Funded/shadow actors
retain one-event scheduling. The relay observes shutdown/fault during admission.

The local workspace check passed **271 Rust tests**, including **72 engine
library tests**, plus formatting and all-targets Clippy with `-D warnings`.
Twelve new regressions cover 4096 ordered events through capacity 16, a real
1024-event CLOB WebSocket frame, original clocks, permanent stalls, cancelled
admission, relay fault/shutdown, isolated paper overflow, exact financial replay,
duplicates, stale/future clocks, quota rejection, failed sync/poisoning and 1 KiB
segments. The production-image build repeats the suite for its actual inputs.

Local Python suites collected **498 tests** with **25 platform/optional dependency
skips** and no failures (console 118, data 21, latency 34, ops 148, research 177).
All **55 Node tests** passed (strategy parity 6, reference 19, dashboard 30), and
dashboard type checking/production build passed. Dependency-complete Linux CI
and exact-image integration are recorded with the final release evidence.

This change preserves queue capacities, capital, owner receipts, strategy,
journal/portfolio quotas and funded authorization. Source-boundary diagnostics
are per-boot counters, not durable financial reconciliation. No new forward run,
24-hour host acceptance, real PnL or economic holdout is established here.

## Binance reference burst isolation: October 9, 2026

The selected AWS simulation exited at 10:43:07 UTC with a fatal Binance
`feed_queue_overflow`. The source sent every BTCUSDT trade through the same
bounded queue as CLOB execution input. Binance supplies a latest reference
price, so this diagnostic traffic could exhaust capacity needed by execution.

The Binance reader now validates every trade and keeps one pending reference
per socket, attempting delivery every 50 ms with missed ticks skipped. Full
queues retain that slot; newer valid references replace it. Exchange/receive
clocks remain original, regressions remain errors, and socket termination
discards pending references before Disconnect. Recoverable Binance Disconnect
admission waits only until the existing send deadline and remains interruptible
by shutdown. CLOB whole-frame admission, ordering and fatal overflow behavior
are unchanged. Per-boot counters expose coalescing, delivery, capacity pressure
and discarded pending references.

All **259 Rust workspace tests passed**, including **60 engine library tests**
and six new regressions. The new checks cover 50,000 references against a full
queue, an actual 4,096-message loopback WebSocket burst, original timestamps,
regressions, closed consumers, socket cleanup, ordered Disconnect admission,
its finite deadline and shutdown priority. Workspace formatting and
all-targets Clippy with `-D warnings` passed. The local production build forces
engine recompilation using source mtimes only, as required by the earlier
shared-cache incident below.

Final-head Rust/release, Python, parity, frontend and container jobs in
[PR 5](https://github.com/yurycooliq/mm2/pull/5), exact-image smoke checks and
separate stopped-ledger recovery receipts establish release acceptance. These
tests do not establish a continuous 24-hour run or economic profitability.
The failed forward session remains a failed, partial technical acceptance;
restarting with a preserved ledger begins a separate observation period.

## Established WebSocket recovery: October 8, 2026

After its earlier successful 16-minute public-feed simulation, the retained
`144143f` preview exited while paused at 19:01:33 UTC: native CLOB diagnostics
reported `invalid_websocket_protocol`, fatal, exit code 1, OOM false. The
preterminal cached sample did not contain that later fault; it is not a terminal
receipt or proof of continued health. Bounded native logs and a byte-hashed
stopped backup preserve this event. Its exact protocol subtype is unknown.

The established-socket reader previously treated every tungstenite protocol
error as fatal, including `ResetWithoutClosingHandshake`, which denotes a lost
connection without the closing handshake ([pinned upstream contract](https://docs.rs/tungstenite/0.28.0/tungstenite/error/enum.ProtocolError.html#variant.ResetWithoutClosingHandshake)).
Only that variant now takes the existing recoverable transport path: mandatory
Disconnect admission, backoff, REST cutoffs and new native snapshots for both
outcomes. Other protocol violations, capacity violations, malformed data and
failed disconnect admission remain fatal. Static protocol categories preserve
diagnostic detail without formatting headers, payloads or nested exceptions.
This fixes a reproduced classification defect; it does not prove the subtype
or cause of the earlier production or local event.

Controlled loopback TCP FIN reproduces the old fatal classification. Three
new real-wire regressions check reconnect with cleared books and no intents
until both fresh books arrive, invalid opcode remaining fatal, and full/closed
Disconnect admission preserving the first fault. All 17 transport checks and
93 engine checks pass. Workspace formatting and engine all-target Clippy with
`-D warnings` pass; final-head CI is required before acceptance. An earlier bare socket-drop assertion was
ambiguous because unread data can produce a TCP reset instead of an EOF; it
does not establish the classification reproduction.

Virtual before/after testing exposed a stale Cargo artifact in the local shared
BuildKit target cache. A forced recompilation corrected the test builder; final
local image acceptance must force recompilation of the mm2-engine package and
verify new test counts, release compilation, exact revision labels and actual
runtime behavior. Cargo clean refused the shared target without a valid cache
directory marker; no files were deleted or guard bypassed. A one-use local copy
of the production Dockerfile touches the engine source before its unchanged
test/release command. That changes build freshness only. No engine journal or
capital reset is part of that build-cache operation.

Both push and PR CI for the earlier `81028a0` history candidate passed 803
automated checks without skips (250 Rust, 498 Python, 30 dashboard, 19 reference,
6 parity) plus 14 container checks and zero live orders
([push](https://github.com/yurycooliq/mm2/actions/runs/37829183818),
[PR](https://github.com/yurycooliq/mm2/actions/runs/37829189473)). Final-head
checks in [PR 5](https://github.com/yurycooliq/mm2/pull/5) must independently
establish this subsequent WebSocket candidate. AWS rollout, real Telegram,
continuous host capacity and economic/funded acceptance remain separate gates.

## Restarted simulation action history: October 8, 2026, 19:04 UTC

Actual-container acceptance on `144143f` first passed the desktop/mobile
comparison, but refreshing after engine restore lost the current audit list.
The synthetic portfolio and durable command receipts were preserved. This was
a reporting defect, not evidence that the capital journal was lost.

The console now captures a bounded sanitized action projection before raw-state
compaction, in its existing secondary SQLite store. Reads merge that retained
projection with the current ring without writing, issuing commands or parsing
the primary journal. Recovered paused snapshots with no active simulation can
still select a source-qualified retained history. Limits, sampling gaps,
API-tail omissions, boot uncertainty and generation changes remain visible;
actions/financial completeness remain false. A published directory locator is
not an authenticated account or globally unique financial ledger identity.

Repeated independent review reproduced and fixed an audit-sequence rollback
mixing old/new generations and a logical-time event being relabeled as a new
observation on every read. Existing IDs keep the first observation clock before
period filtering; evicted observations do not resurrect. Reset epochs separate
sources. Legacy backfill is restricted to validated current boot/profile/locator/
epoch because legacy samples do not prove their configured observer endpoint.
Published u64 counters retain exact text values instead of overflowing SQLite
signed integers. Missing or compacted old data cannot establish past actions.

The frozen console suite passes 118 checks, including 12 action-history
regressions; dashboard 30 checks and its production build pass. The earlier
exact `144143f` push and PR CI both passed 250 Rust, 486 Python, 29 dashboard,
19 reference and 6 parity checks, plus 14 real-container smoke checks and zero
live orders ([push run](https://github.com/yurycooliq/mm2/actions/runs/37822689889),
[PR run](https://github.com/yurycooliq/mm2/actions/runs/37822695840)). Those are
dated earlier-candidate receipts. Final-head checks in [PR 5](https://github.com/yurycooliq/mm2/pull/5)
and the separate final exact-image restart/browser receipts must establish this
new history candidate. No AWS rollout or economic/funded result follows from
these local checks.

## Simulation/comparison preparation: October 8, 2026, 18:10 UTC

The owner console now has separate MM2 simulation and mo-money public-observation
lanes, exact decimal values, source clocks, selected-period aggregates, recent
actions, positions, sampled NAV continuity and a complete sanitized JSON export.
Incomplete public accounting and bounded synthetic audit remain explicit;
neither turnover nor sampled NAV is claimed as authenticated bot profit.
Separate optional panels now show cumulative synthetic closed-position results
and the venue's cumulative public wallet estimates. Actual selected point
endpoints and daily source fidelity remain explicit; net selected-period PnL
and full public financial completeness remain unknown.

An actual read-only v2 capture covered October 7 18:05:05 through October 8
18:05:05 UTC: 4,353 normalized events, 11 public GET requests, 3,808,703 received
bytes. Activity pagination completed, while financial completeness and MM2
attribution remain false. Observer ID:
`aa614e28bb095effded2d2f9f6674edec272a7742a5d07a20c1e3ee7c8110c1e`.
The optional public PnL source was available and its exact response/receipt was
retained. This capture records a working-tree source revision; final images and
the final-head CI must establish the committed candidate independently.
The new dedicated comparison cache retains two verified hot copies plus
immutable daily rolling-window checkpoints, with explicit gaps and finite
quotas. Existing research archives and engine state were not pruned.

Deterministic engine tests reproduced timer starvation when durable periodic
work exceeds 100 ms. Overrun recovery now gives ready feeds an intake interval,
preserving durable writes and control/fault priority. Disconnection enqueue
failures retain overflow/closed classifications and the first fatal cause.
These reproductions do not establish the cause of historical AWS restarts.

Local Windows validation plus targeted final-review regressions: 486 Python
checks, 461 passed and 25 platform/dependency skips (console 106, data 21,
latency 34, operations 148, research 177). The six new
engine regression checks passed with 90 engine tests, formatting and targeted
Clippy. Dashboard 29 checks, production build, rendered fixture desktop/mobile
interaction and exact JSON-download parity passed. Full locked Rust/workspace,
Python Linux, parity/reference, new exact-image container acceptance and final
independent image/host acceptance remain required for this candidate; use final-head PR checks
for authoritative full-suite results.

Repeated integration review found and fixed chart connections across gaps over
120 seconds, boot-identity omission from NAV continuity, and dropped v2 TIP/open
position scope warnings. Requested-feed completeness is now distinguished from
all wallet activity. Legacy samples without known boot identity cannot establish
a continuous gain or connecting chart line; current samples from separate boots
never join.
Final review reproduced extreme optional PnL decimal overflow and silent
underflow. Both now yield an invalid optional source with null amounts while
valid activity still publishes. The regression verifies the full evidence chain;
the saved real provider response retains its exact values.
The primary screen also exposes sampled engine/simulation/live faults and
warning/critical/full journal capacity without expanding diagnostics. Rendered
desktop/mobile fault and capacity fixtures passed with no control submissions.
Actual isolated collector acceptance on `d5e48f2`: UID 10002, read-only root,
no execution credentials/network, 0.5 CPU/192 MiB, 4,370 events and 393 OPEN
positions, ten public GETs and 3,807,128 received bytes. Cold publication took
20.791 seconds; independent verification finished at 21.586 seconds. Peak
process RSS was 125,060 KiB and OOM was false. Observer/view/current identities,
source revision and v2 scope warnings were verified; financial completeness
remained false. At 0.1 CPU the preceding cold test did not finish publication
by 90 seconds; its partial public evidence was preserved. Only new labeled
test containers/network/volumes were removed. This does not establish AWS
performance, long retention or continuous engine capacity.

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
