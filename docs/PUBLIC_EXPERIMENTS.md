# Public input experiments

Optional comparison observations use `experiment_replay TAPE PLAN NEW_REPORT
--comparison-checkpoints REQUEST.json`. The bounded request is exactly
`{"schema":1,"wall_ms":[v0,v1]}` with two sorted times strictly inside the tape.
Each read occurs before the first existing replay operation at or after its
boundary. It adds no emulator advance, order, cancellation, reset or settlement.
The separate `comparison_checkpoints` object retains portfolio state and the
complete bounded active execution snapshot, including pending effects and
reservations. No NAV or atomic public-chain cutoff is inferred. Public snapshot
valuation labels and events within two seconds of either boundary require an
explicit accounting uncertainty check; observation granularity is reported.
The source and public snapshot sampling rule must be frozen before capture;
actual external valuation times cannot be chosen using strategy PnL.

When this optional observer is enabled, it also retains at most 16 already
validated raw Market identities: the record hash/sequence and receipt clocks,
source outcome/token pairs, canonical metadata hash and validated market fields.
Each boundary snapshot references only identities already observed at that point;
the final dictionary alone does not prove earlier observation. These are public
metadata witnesses, not authenticated settlement, fee or balance evidence.
The observer remains separate from execution, valuation, quality and selection.

The public-paper experiment loop evaluates several complete policies on one
recorded input stream. It does not wait for a new day after each code edit and
does not reconstruct a new strategy by deleting losing fills from an old ledger.
The production account, keys and execution ledger are outside this loop.

## Capture and replay

```text
experiment_capture NEW_TAPE.jsonl 3600 4294967296 4000000
experiment_replay COMPLETE_TAPE.jsonl FROZEN_PLAN.json NEW_REPORT.json
```

Capture uses public CLOB and Binance WebSockets, public market/fee metadata and
the native finalized-outcome observer. Each market has its own single connection;
the next market is prewarmed. Original receive order, exchange timestamps,
wall/monotonic receipt clocks, reconnect gaps and input hashes remain explicit.
The collector has finite duration, frame, line, market and byte limits. A valid
hash chain proves retained-file integrity, not complete delivery by the venue.

The optional capture profile is explicit:

```text
experiment_capture NEW_TAPE.jsonl 2100 4294967296 4000000 --capture-profile=no-initial-dump-v1
```

`no-initial-dump-v1` sends the documented `initial_dump:false` subscription and
prewarms the next market sixty seconds before opening. Its chained header source
is `native_public_ws_capture_no_initial_dump_v1`; launch and terminal stdout also
identify the profile. Unknown or repeated profile arguments fail before capture.
The five-argument default retains its original subscription, header source and
twenty-second prewarm. No tape schema or replay quality rule changes.

The [official WebSocket schema](https://docs.polymarket.com/asyncapi.json) defines
the initial-dump option separately from full books published after trades. This
profile tests whether avoiding the subscription dump prevents the observed
initial-snapshot/older-delta conflict. Every subsequent received native frame,
including full books, keeps its original order, text and source generation. No
book is fabricated, no delta is used as initialization and no fault is forgiven.
Both actual valid native books and all existing freshness/open-window checks
remain necessary. A quiet market can lack the required books and stay ineligible.
The profile has not established the cause of remote 1013 closes or eliminated
them; all close/gap evidence and finite retry bounds remain in effect.

Collector failure records distinguish fixed transport/capacity/close categories,
numeric close codes, generation, session age and processing/heartbeat delays.
They omit nested error text and peer payloads. Every failed subscription also
advances the source generation and takes the bounded reconnect backoff. These
diagnostics explain future failures; they cannot reconstruct a missing cause
from an older tape's generic disconnect reason.

Subscription, heartbeat, native Pong and graceful-close writes have a five-second
deadline. A timed-out connection is discarded before the next generation;
partially written messages are never retried on that connection. Missed heartbeat
ticks are skipped rather than replayed in a burst. These bounds prevent a stuck
write from indefinitely blocking reads and shutdown; they do not prove that an
observed remote disconnect has been eliminated.

Peer close code 1013 is the registered "Try Again Later" signal
([IANA registry](https://www.iana.org/assignments/websocket#close-code-number)).
Retries after that signal use increasing five/ten/twenty/thirty-second pauses,
with up to one second of random jitter and a thirty-second total cap. Resetting
the streak requires a connection age of thirty seconds and at least one observed
text frame, which can be a PONG; this is not evidence of fresh valid books. A short
unrelated failure cannot erase the streak. The recorded numeric retry
delay is the requested delay, clipped by the capture/market deadline. Every failed
session still retains its gap and advances the source generation. This avoids
aggressive retries; it cannot prevent a server from closing a healthy connection
or reconstruct missed events.

Replay is offline. Every candidate/latency combination owns a separate native
emulator and finite portfolio. Capital, old positions, gross losses and drawdown
survive rollovers and the training/holdout boundary. Queue position belongs to
unchanged orders; replacements and pending cancellations retain their normal
execution constraints. Latency scenarios are assumptions rather than measured
venue order round trips.

The actor and replay share native-frame watermark filtering, split-delta
snapshot recovery and valid-prefix checks before consuming trade witnesses.
Replay reports discarded events and recovery counts. The tape does not retain
production REST bootstrap responses/cutoffs, engine-triggered refresh receipts,
the actor's fatal repeated-refresh latch or actor batching across raw frames.
After a genuine fault or recovery timeout,
replay waits for an actually observed source generation rather than inventing
successful recovery. Exact production scheduling and ingestion parity remain
unproven; a shared parser alone does not establish them.

For a retained tape with fatal ingress errors, an optional final CLI argument
`--diagnostic-ingress` produces a separate diagnostic report. It retains at most
32 sanitized witnesses from actual rejected events, with record/frame hashes,
source generation, event clocks and available state immediately before rejection.
Raw payloads and arbitrary error text are excluded. A witness with unavailable
pre-error state says so explicitly. The default report remains unchanged.
Delta witnesses sample the exact current quantities of at most four touched
levels. An absent level in an initialized book is known zero; an uninitialized
book remains unknown. This observation cannot itself discard or accept a delta.
Diagnostic mode preserves execution and accounting, disables candidate selection
and cannot promote a strategy or erase faults. Reuse the original frozen tape and
plan with a new report path; a second analysis is not a new economic holdout.

The research-only `local_recovery_proof` example consumes five fixed sparse
fixtures from the retained capture. It reproduces CLOB prestate and strict
rejection, then checks whether two actually observed native books exceed retained
filter floors inside the existing freshness and close limits. It stays paused,
preserves the old invalid verdict and tests no orders or financial state. Missing
books or uncertain provenance remain failures or unknowns. Sparse fixtures retain
original record hashes and are not a new complete capture chain.

Freeze candidate parameters and retain their exact plan hash before collection.
The plan declares complete chronological training markets, an embargo for final
outcomes, later holdout markets, costs and minimum activity. Selection uses only
training information available before holdout starts. A failed winner on holdout
cannot be replaced using holdout knowledge. A declared timestamp in the plan is
not independent proof that the plan was frozen; retain the launch binding too.

## Quality protocols

An omitted `quality_protocol` keeps `strict_v1`: every historical invalid input,
including a recoverable split delta, rejects the window. Its reports and sealed
plans retain their original meaning.

A new preregistered plan may explicitly select `recovered_native_v2`. It retains
all raw invalid and split counts, but distinguishes fatal parse/fee/ingress faults
from recovery completed by both actually observed valid native full snapshots.
Both books must restore quoting strictly before market close and strictly within
the configured maximum feed age. One snapshot, an invalid snapshot, completion
at the age limit, a timeout or completion after close cannot qualify. A later
healthy generation cannot erase a previous unresolved recovery. Every genuine
capture gap still rejects the experiment. Execution, cancellation, queue and risk
semantics are unchanged; production ingestion parity remains unproven.

Short cycles may preregister two complete training markets, one embargo market
and two complete holdout markets. The existing minimum of two filled markets in
each scored partition still applies. This reduces collection time, not the
evidence required to claim repeatable income. Insufficient activity, outcomes or
quality remains inconclusive, with no automatic promotion.

## Experimental policy

An optional `[paper_policy]` is accepted only for public simulation. Its parameters
and the effective strategy are bound to the ledger profile. A different policy
needs a separate experiment; changing it cannot replenish an existing ledger.
No packaged or funded profile enables it by default.

The policy requests both entry legs together, while accounting for the possibility
that either fills alone. Directional and affordability filters cannot turn that
entry into an unsupported singleton. Each condition has a lifetime gross-loss
allocation which positive returns, merges and UTC day changes cannot replenish.
Legacy unknown allocations fail closed. Existing global loss/drawdown caps remain
in force, including old unresolved markets and all possible cancellation fills.

Ordinary complements remain capped by actual paid inventory cost. A separate
repair may lock a bounded loss only after the declared inventory-age or close
trigger and must reduce stress risk. A new repair cannot overlap a still possible
complement. Partial fills retain the immutable original order identity and only
the remaining commitment consumes prospective risk.

V3 public-print execution rejects an order accepted at or after the source trade.
The original exchange time, receipt wall time and normalized event clock are
retained and checked. Excluding ambiguous delayed prints can underfill both
winning and losing orders; this is not a lower bound on real profit. Historical
V2 execution remains separately replayable.

## Interpretation

Reports retain full portfolio cash, realized model result, open costs, unknown
terminal value, excluded-window results, gross losses, realized drawdown, fills,
activity, costs, capture gaps and candidate errors. Missing observations are not
zero returns. Initial/final partial markets still consume the candidate's capital
and are visible in the full result even when excluded from scored windows.

The current protocol requires a clean continuous capture for holdout eligibility.
An initial partial-market gap therefore closes that gate too: trades in that
prefix can affect every later capital/risk decision. Clean-window numbers on a
gapped prefix may be shown only as conditional model observations. They do not
become a completed economic holdout by dropping a bad window.

Positive paired quote margin, mechanical tests, a short selected sample and
synthetic fills do not establish positive expected return or stable income.
Infrastructure/extra-cost inputs are explicit assumptions; actual execution,
gas, capacity, adverse periods and funded eligibility require separate evidence.
There is no automatic funded promotion in these tools.

## Opt-in transport diagnosis

`experiment_capture ... --capture-profile=transport-diagnostic-v1` keeps the
same two market asset IDs, `custom_feature_enabled:true`, `initial_dump:false`
and 60-second prewarm as `no-initial-dump-v1`, with a distinct tape source
`native_public_ws_capture_transport_diagnostic_v1`. The existing default and
`no-initial-dump-v1` profiles keep their previous output and transport behavior.
The diagnostic profile retains a peer Close reason as at most 123 UTF8-safe
bytes encoded as hex, a SHA256 of those retained bytes, original length and an
explicit truncation flag. It renders no raw peer reason. An observed Close is
synchronously retained as a Gap before awaiting the acknowledgement, with its
original observation time and acknowledgement state `pending_or_unknown`.
It then flushes tungstenite's already queued acknowledgement once, bounded by
the original socket deadline and existing five-second write timeout. A completed
acknowledgement appears separately in stdout, bound to the retained Gap reason
hash. Cancellation during the flush cannot erase the recorded Close or count
the Gap or next generation twice. Acknowledging Close cannot explain or repair
the cause of the first 1013.

Bounded scalar session counters record received payload bytes, peak text frames
in aligned one-second buckets, completed read wait, time between completed read
attempts, writer-lock wait, append duration, and application/native heartbeat
counts and flush timing. Cancelled read attempts can contribute to the interval
between completed attempts; it is not proof that the socket was never polled.
Text `PONG` has no correlation ID, so its elapsed time after the last `PING` is an
unmatched observation rather than authenticated network RTT. These measurements
remain in each original Gap and diagnostic session stdout. All native text stays
in the raw chain; no financial parsing, recovery waiver or strategy change is
added. A new source binding, frozen diagnostic question and finite owner-approved
capture budget are required before a public test. Passing local protocol tests
does not establish a remote 1013 fix or a valid economic holdout.
