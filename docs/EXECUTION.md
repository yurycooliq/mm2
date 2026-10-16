# Deterministic and authenticated execution contracts

`mm2-core` owns single-market order identities, desired slots, simulated collateral and inventory. `mm2-strategy` produces bounded BUY-only desired quotes; `mm2-portfolio` owns finite capital and retained positions keyed by market. These libraries have no network, clock, persistence or dashboard dependency. `mm2-emulator` supplies a deterministic event-time venue and notification transport. The engine journals paper transitions before adopting the next session. The console's sampled history is separate from the action ledger.

The execution candidate buys Up or Down outcome tokens with passive limit orders. `demo`/`emulator` uses synthetic observations; `simulation` uses public observations and local fills; `shadow` submits nothing. `trading` attaches the technical authenticated `mm2-live` adapter and `both` adds an independent paper lane. Every startup is paused. Trading templates are inactive until explicit account/protocol/permission gates pass.

`mm2-live` pins `polymarket_client_sdk_v2 = 0.7.0` to the Polygon CLOB V2 token-order path. Its network lifecycle is tested with controlled mocks, not a funded account. Native-beacon DepositWallet accounts use the type-3 ERC7739 order wrapper. Standard-market merge/redemption runs through a separate Polygon transport: types 0/1/2 retain exact signed raw transactions, while type 3 retains an owner-signed gasless batch and records dispatch before its single Relayer POST. Both paths require canonical finalized economic receipts; the batch intent hash and the sponsored transaction hash remain distinct. V3 positions, negative-risk closeout, legacy UUPS DepositWallets and sells remain unsupported. Portfolio receipt accounting and synthetic lifecycle tests do not submit settlement transactions or establish actual attainable returns.

## Exact units and risk

Prices and share quantities use unsigned integers scaled by 1,000,000. A share-price product remains an exact integer in 1e-12 USD units (`pico`). Fragmented fills cannot accumulate rounded cost that exceeds the reserved budget. Snapshots expose exact pico values as decimal strings and explicitly rounded micro-dollar convenience fields. They distinguish known simulated inventory from worst possible one-sided fill imbalance.

Every posting, live, cancelling and unknown order retains its possible-fill commitment. An opposite-side order does not net that commitment away: each outcome can fill while the other remains unfilled. A cancellation request does not make cash available. The core only releases cash after a confirmed rejection or an authoritative terminal observation, accounting for all cumulative fills in that terminal observation. This includes a fill which happened before cancellation but whose individual feed message arrives later.

Risk admission clips a quote to both exact remaining collateral and the independent outcome exposure cap. A clipped order retains the requested size as its identity, so repeating the same request does not create an acknowledgement-triggered replacement. Legal minimum size and current configured tick are validated before mutation. Lowering a risk cap cancels desired orders without resetting cash or inventory. Initial collateral cannot change in an existing session.

## Lifecycle

An emulator order has one immutable monotonically increasing ID within its single-market session. Its known phases are `posting`, `live`, `cancelling`, `unknown_post`, `unknown_cancel` and `closed`. A slot holds at most one possibly executable order and one latest desired quote. Changing a live quote requests cancellation. A replacement is posted only after the old order's terminal confirmation. Further changes during the cancellation coalesce into the latest desired quote. The replacement gets a new ID and joins the back of the external queue observed when it arrives. Engine portfolio identity includes the market, so a later session cannot reinterpret an old market's order or tokens.

Venue arrival and notification delivery are distinct events. The emulator independently configures post arrival, cancel arrival, acknowledgement, fill notification and timeout delays. Post-only crossing is checked against the book when the post reaches the venue. The venue can fill an accepted order before the client receives its post acknowledgement and while cancellation travels.

Fill observations report cumulative quantity for the immutable order, so duplicate or older messages cannot spend cash twice. A terminal observation includes cumulative executed quantity and thus closes a fill/cancel delivery race. A higher cumulative fill which contradicts an already confirmed terminal result is a latched execution fault, not silently discarded.

A lost acknowledgement moves an order into an unknown phase after timeout. It keeps reservations and queries the same identity. An absent result cannot close an order while its original delayed post remains in transport. Transport disconnection keeps outcomes unknown until bounded reconciliation succeeds. A confirmed post 429 releases the rejected post reservation and retries after its explicit cooldown with a new ID. A cancel 429 keeps the existing reservation and retries that same cancellation after cooldown. Query attempts and cancellation retries have bounded scheduling.

## Venue approximation

The emulator models separate outcome-token bid books. Taker sell volume visits eligible buy prices from best to worst. At each price it consumes external queue ahead, then own orders in arrival order, then remaining external quantity. All own orders at a level share external consumption, so one external trade is not counted repeatedly. Own fills plus consumed external volume plus unconsumed volume always equal the supplied trade quantity.

New external depth joins behind existing own orders. Reducing external depth conservatively removes from the back first; only removal reaching the known prefix reduces queue ahead. A replacement loses its previous priority. Level snapshots lack real venue order identities, so this is an explicit queue approximation, not proof of actual attainable fills or returns.

## Bounds and recovery

The core accepts up to 64 active slots (default 16), retains at most 2,048 orders, and prunes only terminal historical orders. The venue retains at most 2,048 orders, 8,192 pending events, 256 recent audit entries, 32 injected faults and 256 external levels per outcome. An advance call has a bounded event processing budget. Time must move forward; it may advance across a long idle interval without inventing elapsed trading events. Capacity exhaustion latches a fault and prevents new posts; reservations remain visible.

`PaperSession` derives `Clone`, `Serialize` and `Deserialize`. Restore must call `validate()` before use. Recovery includes original IDs, desired successors, reserved amounts, cumulative fills, venue queues, pending delayed events and injected faults. Serialization is intended for the engine's journal/replay boundary, not the live strategy hot path. Recent audit entries are bounded diagnostics with eviction counts; the engine's durable action journal is the exact audit source.

The engine wraps this single-market session in `MultiMarketPaper`. Before changing market it cancels/drains old simulated orders and checks terminality. The portfolio imports cumulative spent/inventory totals, retains unpaired positions under the old market ID and opens the new market only with remaining finite collateral. Equal Up/Down quantities can return complete-set collateral through an explicitly synthetic merge receipt. Demo-only deterministic synthetic resolution is separately labeled; public-paper rollover does not invent a winner or redemption price. New public quotes require fresh, durable evidence of zero post-only maker exchange fees; nonzero or unknown schedules block new work. Legacy zero modeled fees remain unverified, and synthetic closeout costs/valuation are incomplete. See [fee evidence and live reconciliation](FEE_RECONCILIATION.md).

Portfolio accounting bounds gross and per-market committed cost, retained unresolved markets, receipts, daily realized loss and realized-equity drawdown. It preserves old positions and blocks contradictory cumulative totals/receipt reuse. Public unresolved inventory has no invented mark-to-market value. Production settlement requires authoritative finality/provenance and a verified adapter; the deterministic library does not send on-chain operations. See [STRATEGY.md](STRATEGY.md) for exact rules, accounting inputs and bounds.

The engine's active command segment rotates with a durable seal into numbered archives. Hash/sequence continuity and owner identities cover the complete retained chain. Partial data, damaged seals and missing archives refuse recovery. Startup streams bounded execution records instead of retaining the tape in actor memory. All journal, operation, command and portfolio quotas are finite; reaching a quota stops new work while preserving evidence. Backup/restore holds the appropriate engine/paper/live fences and restores only to a new path. See [OPERATIONS.md](OPERATIONS.md).

## Authenticated V2 lane

The live actor has independent account accounting and a separate hash-chained checkpoint ledger. Funded profiles explicitly share that account ledger/fence through `live_state_dir`, so selecting trading/both cannot reset loss/spend history or uncertain reservations. It verifies the pinned protocol, signer/funder derivation, exchange/collateral addresses, allowed market/token identity, minimum size, tick and fee bound. A prepared signed wire intent and its possible-fill commitment are durable before posting. A successful post acknowledgement must return the expected immutable order hash. Request expiry and quote freshness prevent sending an expired prepared candidate.

Authenticated user-stream events request reconciliation; they do not alone establish terminality. The adapter queries known immutable IDs and validates cumulative fills/account identity. A missing open-order entry is insufficient to close an uncertain post or cancel. Cancellation responses retain uncertainty until authoritative order reconciliation, and the replacement waits for terminality. Reconciliation/heartbeat/stream freshness gate new work and Resume. A lost post acknowledgement never causes a blind repost of its signed intent after restart.

Risk admission retains every possible fill and checks reconciled collateral/allowance, finite session spend, gross fee-inclusive cost, independent one-sided exposure, market/order counts and configured conservative worst-loss budget. No expected rebate replenishes cash. Cancels, account reconciliation and heartbeat have scheduling distinct from new prepares/posts; a slow signing path must not grant new quote effects or release old reserves.

Owner controls and their exact IDs persist independently in the selected lanes. A lost acknowledgement must retry the same command payload/ID. The owner journal and companion/live journals do not form one atomic distributed transaction; a lane delivery or acknowledgement failure remains visible and is reconciled explicitly. Pause/Kill clear desired work and request cancellation without releasing uncertain funds. Reset returns to paused state without resetting account capital/history. Startup restores account identity and uncertainty, remains paused and reconciles before another explicit Resume.

The supported path is BUY-only, non-negative-risk Up/Down token markets on the pinned V2 deployment: EOA/canonical proxy/one-owner Safe and native-beacon DepositWallet type 3. DepositWallet CLOB order signatures follow the canonical SDK ERC7739/TypedDataSign wrapper; its owner EOA remains the L2 authentication address. Unsupported protocols, V3 positions and unpinned wallet implementations fail closed.

Standard-market closeout uses the separate Polygon worker. Direct types 0/1/2 persist exact raw transaction/hash/nonce before broadcast and pay finite owner POL gas. Type 3 persists the complete owner-signed Batch/body/digest/deadline, syncs its dispatch marker, and sends exactly one gasless relayer POST. The relayer ID and actual transaction hash supplement the durable batch identity. Lost acknowledgements never create a replacement nonce/signature or automatic POST retry. Success requires canonical finalized factory batch/signature and wallet event proofs plus exact token/collateral effects; sponsored gas does not consume the owner's POL budget.

Expiry is a distinct canonical proof: finalized block timestamp strictly exceeds the deadline, wallet nonce remains unchanged and condition token prestate is retained. It clears only the expired authorization, credits no payout, creates no transaction receipt and leaves execution faulted for investigation/Reset. Active external AutoRedeemer approvals block Resume/preparation because they can redeem positions without changing wallet nonce; current code neither imports those external receipts nor disables approvals. Details are in [LIVE_GATEWAY.md](LIVE_GATEWAY.md). SDK/mock checks establish the local technical contract only. Current deployment acceptance, private credential eligibility, authenticated venue behavior, funded-order timings and strategy economics remain separate gates in [ROADMAP.md](ROADMAP.md).

## Public library API

```rust
use mm2_core::{DesiredQuote, Outcome, RiskLimits};
use mm2_emulator::{Book, LatencyConfig, Level, PaperSession};

let mut session = PaperSession::new(RiskLimits::default(), LatencyConfig::default())?;
session.set_book(0, Outcome::Up, Book {
    bid: Some(500_000), ask: Some(600_000),
    bid_levels: vec![Level { price_micros: 500_000, quantity_micros: 3_000_000 }],
})?;
session.quote(0, vec![DesiredQuote {
    slot: "up:lot".into(), outcome: Outcome::Up,
    price_micros: 500_000, quantity_micros: 5_000_000,
}])?;
session.advance(40)?;
let volume = session.trade(41, Outcome::Up, 500_000, 5_000_000)?;
session.cancel_all(42)?;
session.advance(100)?;
let snapshot = session.snapshot();
# Ok::<(), String>(())
```

`set_risk_limits(now_ms, limits)` updates validated limits without replenishing cash. `inject(Fault)` supplies explicit deterministic transport/rejection faults for tests. `now_ms()` and `limits()` expose read-only session configuration. All mutation methods return errors for invalid inputs; the engine commits only successful candidates.

## Event tapes and verification

Run a finite synthetic tape locally:

```sh
cargo run --locked -p mm2-emulator -- fixtures/execution/shared-queue.json
cargo test --locked -p mm2-core -p mm2-emulator
cargo run --locked --release -p mm2-core --example bench_core
```

The CLI accepts one JSON file, capped at 2 MiB and 10,000 steps. It prints the final labelled simulated snapshot. A tape contains `name`, optional `limits` and `latency`, and `steps` tagged by `op`. Supported operations are `book`, `quote`, `trade`, `cancel_all`, `advance`, `fault` and `restart`. Fixtures include expected exact account values and venue counters. Their `expected` object is checked by Rust golden tests, while the CLI leaves it as documentation.

The shared bounded tapes cover shared queue volume, partial-fill cancellation delivery, cancel-before-replace coalescing, post-only rejection at arrival and budget clipping. The TypeScript comparison harness in `tools/reference/execution.test.ts` verifies all five against the isolated corrected reference, with acknowledgement delay set to zero and final settled account values compared. That bounded comparison is not full strategy parity. The Rust emulator additionally models independent acknowledgement delay, 429s, unknown outcomes, reconciliation, deterministic serialization and separate outcome books; these scenarios have their own Rust contract tests.

The microbenchmark reports observed local timings for unchanged desired quotes after the two existing slots are Live, including input cloning and clock measurement. It does not create/post/cancel an order each iteration and runs no network, disk or console. Its output cannot establish full lifecycle latency, regional placement or an exchange fill SLA. Engine core/strategy CPU metrics and live mock prepare/post/cancel probes retain their stated scope. Authenticated venue end-to-end tails require the actual validated account and selected host.

## Synthetic delay sensitivity

```sh
python3 scripts/scenarios.py --tape fixtures/execution/shared-queue.json \
  --binary target/release/mm2-emulator --out /tmp/mm2-sensitivity.json
```

The script replays the same fixed intents, books and trades under four configured delay sets, then requests cancellation and advances the local venue to clean up. It verifies exact collateral conservation and reports remaining pending events, active orders, reservations and faults explicitly. A long injected fault may remain unsettled; a successful process exit alone does not mean that cleanup settled.

This output is synthetic accounting sensitivity. The delay values are configured assumptions, not observed network latency. The strategy is not recomputed under each delay, and remaining outcome tokens are neither settled nor valued. There is no validated economic return, PnL ranking or permission to trade in the report. The output always marks `scoring_eligible`, `live_eligible`, `strategy_recomputed`, `observed_venue_data` and `economic_return_evaluated` false. Existing output files are preserved, and the CLI's byte, step and timestamp bounds remain enforced after cleanup operations are added.
