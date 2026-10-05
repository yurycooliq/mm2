# Strategy and market portfolio contract

`mm2-strategy` and `mm2-portfolio` are deterministic Rust libraries without I/O, UI,
wall-clock access, floats, databases or network clients. The caller supplies the
clock, market identity, current tick/minimum, observed books and authenticated
execution evidence. Simulator fills and receipts remain explicitly synthetic.

## Frozen candidate

The deployment candidate is a BUY-only Up/Down spread strategy. The Up mid supplies
the book-only reference; the center is shrunk toward 0.50, then each side bids its
center minus a configurable half-width. Both outcomes' actual asks independently
bound post-only prices. Tick rounding is deterministic. There is no unobserved
fair price or expected rebate income.

The default `Rules` uses fixed lots, a 4.5c half-width, shrink 0.977, no stubs,
no external signal lean, no volatility widening and no time freeze. Engine limits
override lot, half-width, imbalance and freshness for the actual deployment.
`schedule_lot=true` explicitly enables the observed UTC session rule: weekdays
12:00–23:00 UTC use the full lot; otherwise use a whole-share rounded half lot.
This UTC observation is not a daylight-saving timezone conversion.

The lagging inventory side may add a hedge for its known net imbalance. Hedge,
regular lot and optional one-cent stub consume the same finite cash and independent
possible one-sided-fill cap. The hedge receives admission priority. All output is
bounded to six slots. Existing order reservations must be excluded from replacement
budgets only when the execution ledger separately retains and checks them; counting
the same retained desired order twice would cause artificial cancel/repost cycles.

Missing, stale, future-dated, crossed or hollow books return an empty desired list.
The close guard cancels every slot, including stubs. Empty desired lists require
the caller's ordinary cancel-before-replace lifecycle; they do not release capital
before authoritative terminal reconciliation. The strategy is not the execution
ledger and cannot itself make an existing order post-only after it reaches the venue.

`Rules::reconstructed_btc5m()` records the research fit: 502 full-session shares,
4.5c width, shrink 0.977, one-cent stubs, volatility-time widening (k=2, reference
150s), fair weight 0.12, depth lean +2.2c and flow lean −2.3c. It requires all enabled
signals and their point-in-time freshness. Missing TWAP-model probability does not
silently downgrade this reconstruction to book-only. This large research lot is
not a default capital allocation.

The upstream pure rules are
`rstl2/olympus/packages/updown-lab/src/strategy/rules.ts`, prepared in the provenance
commit described in [PROVENANCE.md](PROVENANCE.md). The initial floating power/exp
volatility regime profiles and 45s frozen-order hypothesis are not enabled by this
candidate. Source observations are not proof of profitability. Width uses bounded
integer square roots with conservative upward micro-price rounding; unsupported
signals/profiles require a separately frozen implementation and parity evidence.

## Exact account and market lifecycle

`Portfolio` owns one finite collateral pool and positions keyed by immutable market
identity/start/end. A new market never receives old outcome tokens and never resets
cash. Prices and shares use 1e6 units; cost and fees use exact 1e-12 USD integers.
Every serialized pico/PnL field is a decimal string. Public snapshots expose exact
strings and conservatively floored available micro-dollars.

Two mutually exclusive execution inputs are supported per market:

- `apply_fill`: immutable, payload-checked fill receipts with explicit fee-inclusive
  cost. A partial fill reduces its retained reservation by confirmed quantity and
  consumed fee bound; cancel dispatch and unknown outcomes do not release it.
- `record_execution_totals`: idempotent cumulative Core spent/inventory/fee snapshots.
  `spent_pico` includes `fee_pico`; backward or contradictory totals fail. Allocation
  of aggregate cost between sides is proportional to added quantities, because
  aggregate Core snapshots contain no per-side cost basis. Cash and total realized
  results stay exact; per-side allocated cost is not historical execution evidence.

Merge burns equal Up/Down quantities for $1 per complete set, using a receipt that
includes fees and provenance. Partial cost-basis division preserves integer dust
until the final burn. Redemption requires the market to be closed, finality confirmed,
no retained reservations and an explicit winning outcome. Production must verify
authoritative settlement and receipt evidence in its adapter. `allow_synthetic=true`
is exclusively an emulator policy; a price feed is never final resolution evidence.
An unresolved unpaired public-paper position stays keyed to its old market without
invented terminal price, valuation or redemption income.

Risk policy bounds total and per-market cost exposure, finite cash, unresolved
position count, daily realized losses and realized-equity drawdown. Loss limits
latch a halt; midnight does not rearm it. Open positions are not assigned a market
value by this library. Thus daily loss/drawdown are realized accounting limits,
while open risk is separately capped by full fee-inclusive committed cost. Adapters
must add any required mark-to-market stop using trustworthy price/valuation evidence.

Bounds: 64 reservations, 64 unresolved position-bearing markets by default,
4,096 retained market identities and 8,192 immutable receipts. Exhaustion fails
closed and requires an archived checkpoint/operator maintenance, not identity
pruning that would forget a duplicate fill. `validate()` verifies restored state,
cash/cost-basis conservation, identities and bounded financial arithmetic. Normal
mutation checks avoid scanning the entire receipt history on each fill.

## Offline verification and economic gate

```sh
cargo test --locked -p mm2-strategy -p mm2-portfolio
cargo clippy --locked -p mm2-strategy -p mm2-portfolio --all-targets -- -D warnings
node --experimental-transform-types --test crates/mm2-strategy/tests/parity.test.ts
```

Six shared Rust/TypeScript vectors cover the explicitly scoped fixed-point quote
contract, source-v1 prices, inventory hedge, actual Down post-only bound, stale-book
cancellation and insufficient cash. This is not full historical replay or full
upstream-profile parity. Rust contracts additionally test signal admission, near-close
integer volatility widening, 400 cash/inventory/tick combinations, fee-inclusive
partial fills, receipt collisions, preserved reservations, finality/provenance,
partial merge dust, checkpoint restore and a synthetic 288-window collateral run.
The synthetic run asserts arithmetic only and is never an economic score.

Historical economic eligibility remains blocked until Pendulumflow reconstruction
quality and point-in-time event ordering are resolved, exact governing TWAP60 is
available, rules and criteria are frozen before an untouched holdout, and measured
post/cancel tails, adverse queue fills, fees and finite capital are included.
See [DATA_AND_LATENCY.md](DATA_AND_LATENCY.md). Archive availability is not replay
eligibility. A passing contract, an available trading mode or fast local evaluation
does not establish profitable execution.
