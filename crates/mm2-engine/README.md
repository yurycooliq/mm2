# Engine boundary

The default `config/demo.toml` starts **paused** in emulator mode. Resume drives an
experimental book-only strategy into `mm2-emulator`; balances, inventory, order
acknowledgements and fills are simulated. Synthetic taker trades consume finite
venue volume and arrive on a deterministic logical timeline. They are test inputs,
not evidence of obtainable returns. `config/public.toml` only computes unsubmitted
shadow intentions. The binary has no funded order gateway and rejects live mode.

Owner HTTP requests use `Authorization: Bearer MM2_ENGINE_API_TOKEN` on private
port 8088. `/v1/state` and its `/v1/status` alias read an immutable published value;
they never request work from the execution actor. `/v1/events` exposes bounded SSE
readers. `/healthz` is liveness; `/readyz` distinguishes API and feed readiness.
`mode` is the owner control phase; `execution_mode` distinguishes emulator and
shadow. `simulation_market_condition` identifies the contract to which the demo
inventory belongs. Actual account fields remain unknown.

## Durability

One actor owns core state, the emulator and the journal. `commands.jsonl` contains
a sequenced hash chain of canonical owner commands and replayable emulator inputs.
A control command and its associated cancellation or limit mutation occupy the
same durable record. Append plus `sync_all` succeeds before the candidate state or
receipt becomes visible. Execution records do not advance owner command CAS.
After an acknowledgement timeout, reuse the same command ID and payload; a changed
payload conflicts. A partial record, broken chain or invalid execution replay
refuses startup. The retained `engine.lock` fences processes sharing one local
filesystem; it is not a distributed lock.

Startup reconstructs the simulated account and order identities, cancels remaining
quotes and settles scheduled local notifications, then stays paused. Pause, kill
and reset cancel open emulator quotes; reserves remain until cancellation or fill
notification proves the outcome. Reset does not erase inventory, collateral,
receipts or audit history. Initial simulated collateral cannot change in an
existing session. New windows with retained inventory pause execution and block
Resume into another contract. Settlement/redemption is not implemented.

Only causally changing execution transactions are synced. Unchanged book inputs
are coalesced and a clock-only idle tick writes nothing. The journal is bounded
by `journal_max_bytes`, up to 64 MiB, and 10,000 owner commands. Capacity exhaustion
stops new effects and requires owner maintenance; this package does not silently
delete or rotate the exact audit. The deployment needs a retention/archive policy
before an unattended long-running funded gateway can be enabled.

## Secondary statistics

Periodic snapshots are sampled at no more than 4 Hz. The actor checks a single
permit before copying bounded typed state. A background worker formats JSON and
sorts the bounded percentile sample; there can be only one queued or in-flight
periodic snapshot. A busy worker drops/coalesces samples, and an older result
cannot overwrite a newer command or fault snapshot. Owner command acknowledgements,
source transitions and faults publish immediately. The dashboard and console
database do not participate in execution decisions or durable writes.

Copying and publishing still have finite cost. `metrics.last_snapshot_copy_ns`
exposes the last bounded actor copy; `telemetry_coalesced` exposes skipped samples.
Decision percentiles measure `core.evaluate` only and exclude transport, queues,
emulator processing, journal sync and publication. They are not order latency or
a region comparison. The ledger can block execution on storage latency by design;
secondary database/chart/log work cannot block journal progress.
