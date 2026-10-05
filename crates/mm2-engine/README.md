# Engine boundary

Every profile starts **paused**. The engine supports public-data `simulation`,
unsubmitted `shadow`, the offline demo's `emulator`, one-account `trading` and
`both` with independent trading and paper lanes. Choose a profile from the
repository root:

```sh
python3 scripts/start.py --mode demo --build
python3 scripts/start.py --mode simulation --build
python3 scripts/start.py --mode both --dry-run
```

On PowerShell, use `./scripts/start.ps1 -Mode simulation -Build`. The wrapper
defaults to public simulation; an unconfigured direct Compose start defaults to
demo. `--dry-run` makes no activation request. Non-dry-run trading/both refuse
blank or unauthorized public templates before starting Docker. Native
`mm2-engine --config <path> [--mode <mode>] --check` checks configuration shape,
not credentials, venue access or selected-host readiness. See
[startup and operations](../../docs/OPERATIONS.md).

Demo uses deterministic synthetic books and finite taker volume. Public
simulation models local orders from observed books; shadow submits nothing.
These inputs and fills do not establish obtainable returns. The limited trading
adapter pins `polymarket_client_sdk_v2 = 0.7.0`, supported Polygon CLOB V2
deployment and BUY-only post-only token orders. Its technical tests use controlled
mocks. V3 positions, deposit-wallet EIP-1271, sells and on-chain merge/redemption
are unsupported. Live templates contain no account credentials or activation
permission. Actual account/protocol/eligibility checks remain deployment gates.

Owner HTTP requests use `Authorization: Bearer MM2_ENGINE_API_TOKEN` on private
port 8088. `/v1/state` and its `/v1/status` alias read an immutable published value;
they do not request work from the execution actor. `/v1/events` exposes bounded
SSE readers. `/healthz` is liveness; `/readyz` distinguishes API/feed readiness.
`mode` is the owner control phase; `execution_mode` identifies the selected
profile. `simulation` and `paper_portfolio` are virtual account state; `live`
contains separately reconciled account state. `parallel_simulation` reports the
companion's phase/fault/journal in `both`. `live_eligible` is a current technical
readiness gate, not profitability or unconditional permission to trade.

## Durability and recovery

The owner/paper actor owns core state and the exact command/execution journal.
A control command and its associated paper cancellation or limit mutation share
one durable record. Append plus `sync_all` succeeds before candidate state or
receipt becomes visible. Execution records do not advance owner command CAS.
After an acknowledgement timeout, reuse the same command ID and payload; a
changed payload conflicts. Partial records, missing archives, broken chains or
invalid execution replay refuse startup.

The hash chain spans `commands-00000000.jsonl` archives and active
`commands.jsonl`. A synced `journal-manifest.jsonl` seal precedes rotation;
startup completes a matching interrupted rotation or rejects inconsistent
evidence. Recovery streams bounded execution records. Default active segments
are 8 MiB and total retained capacity is 128 MiB, with additional finite archive,
owner receipt and execution-operation quotas. Configuration may set `[journal]`
bounds. The older `journal_max_bytes` field controls the segment-size fallback,
not unlimited retention. No archive is deleted or compacted automatically.
Capacity exhaustion stops new effects with identities/commitments retained.
Published `journal` metrics expose each quota; live and companion paper have
independent budgets. Offline fenced backup/restore preserves the audit into new
destinations, as described in the operations guide.

`MultiMarketPaper` preserves one finite capital base and market-keyed positions.
Rollover drains old possible fills/cancels before starting the next market;
old unpaired positions retain their original token identity. Pair merges use
explicitly synthetic receipts. Demo also permits labelled synthetic resolution;
public simulation retains unresolved positions without inventing a winner or
redemption value. Startup restores orders, cash, portfolio and receipt identity,
recovers/cancels remaining local quotes and stays paused. Reset does not erase
inventory, reserves, cash or history; initial collateral cannot replenish an
existing session.

The live actor has an independent durable account ledger. Packaged trading/both
share `live_state_dir = "/data/account-live"` across mode-specific state
directories, preserving account spend/loss history and uncertain commitments
when changing profiles. Signed intents are synced before posting; ambiguous
post/cancel responses retain reserves. Restart never blindly resends old signed
orders and requires authoritative order/account reconciliation, fresh user
stream and heartbeat before Resume. Controls preserve request identity across
lanes; their journals do not form a distributed atomic transaction. File fences
protect one local filesystem domain; cross-host HA and active/active execution
are outside the package.

## Secondary statistics

Periodic snapshots are sampled at no more than 4 Hz. The actor checks a single
permit before copying bounded typed state. A background worker formats JSON and
sorts bounded percentile samples; only one periodic snapshot can be queued or
in flight. A busy worker coalesces samples, and an older result cannot overwrite
a newer command/fault publication. Owner receipts, source transitions and faults
may publish immediately. Dashboard, Telegram and console SQLite do not
participate in decisions or primary journal writes.

Copying and publication still have finite cost. `metrics.last_snapshot_copy_ns`
measures the bounded actor copy; `telemetry_coalesced` counts skipped samples.
Decision percentiles cover core and strategy evaluation and exclude transport,
parsing, queue waits, emulator/live processing, journal sync and publication.
They are not exchange order latency or a region comparison. Durable storage is
an execution dependency; secondary history/chart work has its own process and
bounded storage. Current validation scope is recorded in
[VALIDATION.md](../../docs/VALIDATION.md).
