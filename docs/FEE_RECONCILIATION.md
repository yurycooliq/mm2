# Fee evidence and reconciliation

Public simulation now requires fresh exchange fee evidence before quoting. The source
worker checks CLOB protocol V2 and `/clob-markets/{condition_id}` at discovery and every
30 seconds. It validates the exact condition, both token/outcome identities, and explicit
`fd.r`, `fd.e`, `fd.to` fields. Missing fields cannot inherit the SDK's zero defaults.
Evidence expires after 90 seconds; a failed refresh immediately withdraws readiness.
The worker owns a single watch slot and shares the source generation's cancellation.
It does no HTTP work on the execution actor or CLOB queue. Market selection clears the
previous evidence, including a same-market refresh after Reset.

The supported execution contract is post-only BUY with a verified zero maker exchange
fee (`taker_only=true`, or an explicit zero rate). Nonzero maker schedules are rejected
until a reviewed numerical fee model supports them. A taker schedule is retained as
evidence but is not subtracted from maker turnover. The current public fee schedule
documents zero maker charges; this is checked per market rather than assumed globally.
See [official fees](https://docs.polymarket.com/trading/fees).

Each accepted paper fee policy, including retrieval time and response SHA-256, is synced
as a `fee_rules` operation before new simulated quotes. It changes no financial amounts.
Legacy operations/checkpoints still replay exactly, and absent historical fee evidence
remains unknown. New metadata does not certify old positions or old realized results.
The API exposes `fee_check.ready`, status and the underlying policy. The journal exporter
preserves these public records without copying credentials or signed wire material.

The authenticated SDK independently refreshes and freezes the same strict policy in the
durable pre-submission intent. It rejects builder-tagged orders and requires the already
configured Polygon RPC verifier before preparing a real order. This adds no new signing,
broadcast, approval or funded authorization. It does not make an ineligible host eligible.

Account reconciliation joins tracked authenticated trades to their transaction hashes.
Only a successful canonical receipt at or beyond the configured finalized confirmation
threshold supplies an actual fee. The decoder checks the pinned V2 exchange, order hash,
owner, BUY side, token, quantities, builder and transaction/block/log provenance. It uses
the six-decimal collateral `OrderFilled.fee`, not the API's `fee_rate_bps`. The ABI and BUY
fee transfer semantics follow the official [V2 interface](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/interfaces/ITrading.sol)
and [settlement implementation](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Trading.sol).

The live API reports `execution_fee_reconciliation` as `NO_FILLS`, `MATCH`,
`UNKNOWN_PENDING_FINALIZED_RECEIPTS`, or `MISMATCH`. Without complete receipt coverage,
the actual total is null. Newly tracked fills block more quoting while their fees are
pending; reconciliation and cancellation continue. Nonzero fees or an unexpected taker
role persist the proof and fault the executor for owner reconciliation. Proofs deduplicate
by transaction hash/log index; changed finalized identities or excess fill coverage fail
closed. Existing conservative spend/risk accounting is retained separately from actual
fee evidence and is not relabeled as exact cash PnL.

All collections are finite: at most 256 retained orders and fee policies, 256 cached
transaction receipts, 1024 fill proofs, 4096 logs per receipt, and eight new receipt lookups
per reconciliation pass. Filled order evidence is retained instead of being silently
pruned. Exhausting a bound blocks further work and requires a reviewed archive procedure.
Receipt lookup uses the existing isolated live worker; it adds no public feed events.

This is readiness for a bounded forward simulation with verified maker exchange fees.
It does not complete an economic holdout. Synthetic merge gas, rebates, executable
liquidation value of open/legacy positions, infrastructure allocation and real PnL/ROI
remain unknown. The fixed latency and queue/fill assumptions still require calibration
against eligible, explicitly authorized real executions. No funded canary is implied.

Rollback preserves the current paused ledger and every owner receipt. Restore only into
a new path and compare replay before activation. After a new `fee_rules` record exists,
older binaries do not understand the new operation; do not point an old image at that
newer journal. Use the retained pre-deployment backup for an isolated investigation,
not to discard newly committed executions or reset the capital pool.
# Retained mismatch and owner controls

A finalized nonzero maker fee or unexpected taker role remains an independent
admission failure even if an older journal has cleared its mutable fault flag.
Full quantity coverage does not imply fee agreement. Reset refuses this state;
repeated reconciliation of the same proof re-latches the fault. Neither Reset nor
replay deletes evidence, changes the capital history or authorizes more orders.
Any remediation requires a separately reviewed accounting/reconciliation path.
