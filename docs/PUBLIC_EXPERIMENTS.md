# Public input experiments

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

Freeze candidate parameters and retain their exact plan hash before collection.
The plan declares complete chronological training markets, an embargo for final
outcomes, later holdout markets, costs and minimum activity. Selection uses only
training information available before holdout starts. A failed winner on holdout
cannot be replaced using holdout knowledge. A declared timestamp in the plan is
not independent proof that the plan was frozen; retain the launch binding too.

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
