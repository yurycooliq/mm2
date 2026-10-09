# Queue backpressure review — October 9, 2026

## Result and evidence boundary

The source-to-actor path now tolerates temporary pressure and valid frames larger
than its queue, retains every admitted CLOB event in FIFO order, and stops on a
finite deadline if the consumer cannot progress. Emulator trade durability is
batched to reduce the cost that can create pressure. Queue sizes and journal,
capital and portfolio quotas are not raised.

The R2 AWS forward session on source `b6a9646` ended with
`feed_queue_overflow source=clob` at 11:51:44 UTC. That receipt establishes the
failed boundary. It does not identify the exact offending frame size, disk
latency, arrival rate or consumer stall. The mechanisms below are verified in
code and controlled local regressions; they are not retrospective measurements
of that server. The separate historical result remains in
`output/simulation-20261009-r2/FINAL_REPORT_20261009.md`.

## Findings and changes

1. **Valid input could never fit.** The parser permits up to 4096 events in a
   bounded native message, while the configured feed queue permits 16–512
   entries. Whole-frame `try_reserve_many(events.len())` could reject a valid
   frame even when the queue was empty. Admission now streams a fully validated
   frame in chunks bounded by 16, remaining events and actual queue capacity.
2. **Transient pressure was fatal.** `try_reserve_many` immediately classified
   Full as overflow. Admission now waits for permits with one absolute deadline
   for the frame, using the existing 5-second public send deadline. Closed,
   timeout and shutdown remain distinct. Timeout has static diagnostic code
   `feed_backpressure_timeout`; old `feed_queue_overflow` remains decodable for
   historical evidence and sanitized incident capture.
3. **The consumer amplified write costs.** Each fresh public trade cloned the
   emulator/portfolio, serialized full state for a no-op comparison, started a
   blocking worker and synced its own record. The actor now drains only already
   available inputs, at most 16 per emulator turn, with no batching sleep. It
   clones once, validates ordered trade operations, skips known duplicate
   fingerprints without full-state JSON comparison, and syncs one transaction
   before exposing candidate finances. Small supported segments reduce the
   batch size using conservative encoded-record bounds.
4. **A blocked relay hid stop signals.** A primary `.send().await` inside the
   input select branch could not observe shutdown/fault while full. The relay
   now selects those signals while waiting, has a finite deadline, retains only
   one held event and is supervised/joined by main.

## Bounded state and intentional queue policies

| Boundary | Bound | Pressure policy |
| --- | --- | --- |
| Public socket input | 1 MiB message, 4096 parsed events | Validate the whole frame before offering events; retain one frame while waiting |
| Source → relay → primary actor | Two queues, each configured 16–512; relay holds one event | Ordered admission, chunks ≤16, finite 5-second wait, independent fault/shutdown |
| Binance reference | One pending price per socket, 50 ms publication cadence | Validate every input; coalesce diagnostic reference prices only; retain original clocks |
| Emulator actor turn | At most 16 queued inputs; smaller for small segments | One candidate and one durable transaction for fresh trades; return to priority select each turn |
| Optional paper fan-out | One configured bounded queue | Fault that secondary lane if it loses input; primary does not wait on its simulation or storage |
| Owner actor commands | Configured 1–128 | Reject Full before acceptance; preserve durable IDs and retry/CAS rules |
| Live actor commands | 32 | Reject Full before acceptance; no delayed implicit activation |
| Live worker results | 128, finite owned workers | Await capacity; maintain unknown outcomes and reservations until authoritative evidence |
| Private user notices | 256; parsed frame ≤128 notices/64 KiB | Existing fail-closed stream stop/reconciliation if notice admission fails; notices cannot credit cash |
| Desired quotes and telemetry | Latest-value watches / one formatting worker | Coalesce snapshots/desired state; never treat them as an execution ledger |
| Dedup / timing / journal | 8192 fingerprints, 1024 timing samples, configured durable quotas | Finite retention; preserve evidence and halt on exhaustion |

The optional paper lane cannot simultaneously be lossless with finite storage,
arbitrarily slower than primary, and unable to delay primary. Its explicit
isolated fault is retained. Owner-control rejection and private-stream
reconciliation are safety boundaries, not permission to grow queues indefinitely.
Funded gateway, strategy, capital, cancel/replace and settlement logic are not
changed by this fix.

## Ordering, durability and cancellation

Complete-frame **validation** remains atomic: a malformed later member cannot
leak an earlier valid member into ingress. **Admission** can now be partial on
shutdown, closure or a deadline. The admitted prefix is not rolled back; the
unadmitted suffix is explicitly counted. A guard also records cancellation of
an in-progress admission. A separate watch fault cannot be trapped behind the
full data queue, and preserves the first cause.

Original exchange and monotonic receive timestamps are never restamped on
admission. Existing age and future-clock checks continue to suppress stale
inputs; successful delivery does not imply freshness. A batch retains input
order and uses its actor processing clock for emulator operations, as before;
it is not a reconstruction of exact venue arrival/execution timing.

The journal format is unchanged: it already supports ordered transactions of
up to 32 operations. A trade batch is capped at 16. Candidate finances become
visible only after the record is synced. Capacity rejection exposes no partial
candidate; a complete write with failed sync poisons the writer, withholds its
committed prefix and requires restart reconciliation. No capital reset, journal
deletion, quota extension, automatic Resume or deployment is performed here.

## Regression evidence

New checks exercise 4096 ordered events through a queue of 16, a real loopback
WebSocket containing 1024 alternating outcome snapshots through that queue,
original clocks, admission suffix accounting, a permanently stalled consumer,
shutdown/fault during a blocked relay and isolated secondary overflow.

Emulator checks exercise 16 new trades in one commit with exact replay of cash,
inventory, reservations and fill notices, duplicate idempotence, stale/future
clocks, capacity rejection, injected failed sync and continued writer poisoning,
and 1 KiB segments without increased quotas. Existing recovery, rollover,
cancel/fill, owner-command and startup-mode tests remain required.

Completed validation counts and exact-image results are recorded in
[VALIDATION.md](VALIDATION.md). Local detailed logs are ignored artifacts in
`output/queue-backpressure-20261009/` in the implementation checkout. This is
bounded functional/load evidence, not selected-AWS-host throughput, a completed
24-hour acceptance, a profitability result or an economic holdout.

## Remaining operating limits

A consumer slower than its sustained input cannot be made lossless indefinitely
with finite memory. Continued stalls still stop after the deadline; feed freshness
can suppress decisions sooner. During admission wait, the socket reader applies
backpressure and does not drain further application frames. Kernel/socket/TCP
buffers and the peer remain finite. Monitor waits/timeouts, freshness, periodic
overruns, journal quotas and supervisor boots on any separately approved new run.

Per-boot ingress counters describe this source boundary, not downstream
acknowledgements or financial reconciliation. Financial/model results and fees,
open positions, real PnL and net return must continue to use their own evidence.

Tokio's primary contracts document
[bounded sender permits and cancellation](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html)
and [select priority/cancellation](https://docs.rs/tokio/latest/tokio/macro.select.html).
The build retains the locked Tokio dependency; no dependency update is part of
this change.
