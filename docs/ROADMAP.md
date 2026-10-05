# Delivery and evidence gates

## Implemented offline package

- Independent Rust workspace with fixed-point lifecycle core, deterministic exchange emulator, strategy rules and a market-keyed finite-capital portfolio.
- Paused startup profiles: offline demo, public simulation, read-only shadow, gated trading, and parallel trading/simulation. Profile identity and separate lane state prevent reinterpretation of a paper ledger as a real account. Funded profiles share an explicit account ledger/fence, preserving reservations and loss/spend history across trading/both selection.
- Owner API, authenticated CAS commands, durable command identity/receipts, exact execution recovery and local filesystem fences.
- Segmented command/execution journal with sealed hash continuity, interrupted-rotation recovery, bounded streaming replay and explicit total/record/receipt capacity. Offline fenced backup/restore and read-only soak monitoring include companion paper/live evidence.
- BUY-only technical trading adapter pinned to `polymarket_client_sdk_v2 = 0.7.0`: signed intent before dispatch, post/cancel lifecycle, authenticated user-stream triggers, authoritative order/account reconciliation, heartbeat, bounded scheduling and finite risk.
- A separate Polygon closeout worker implements standard-market merge/redemption for EOA/canonical proxy/one-owner Safe and the pinned native-beacon DepositWallet. Direct accounts persist raw transaction identities and finite owner POL gas caps. DepositWallet persists owner-signed batches, sends one authenticated gasless relayer POST, binds distinct batch/transaction hashes, recovers success from complete factory/wallet/receipt evidence and checks canonical authorization expiry separately. Preparation/readiness reject deployment/token/domain/ownership drift, missing approvals and external AutoRedeemer capabilities. Tests use public fixture signatures and local RPC/HTTP servers.
- An offline AWS release package binds clean source and immutable images, retains encrypted account storage and exposes HTTPS owner ingress. Template/local TLS fixtures do not create a cloud host. See [AWS_DEPLOY.md](AWS_DEPLOY.md).
- Multi-market paper rollover preserves old positions and finite cash. Complete-set merge is explicitly synthetic; demo resolution is synthetic, while unresolved public-paper positions remain retained without invented payouts.
- Separate Telegram/SQLite operator process and React/Vite Mini App, public feed adapters, bounded Pendulumflow importer, archive quality checks and host-comparison tools.
- Reviewed Docker boundaries, local/container checks and CI. Current evidence and exact test counts belong in [VALIDATION.md](VALIDATION.md).

The funded adapter is a code capability validated against controlled mocks. No private-key/authenticated-account acceptance, funded canary, selected deployment host, Telegram delivery or profitability is established by those tests. `trading`/`both` templates contain the publicly checked DepositWallet/controller identities and remain inactive (`owner_authorized = false`), with no credential values. Historical dated handoffs remain historical evidence; current test outcomes belong in VALIDATION.md.

## Protocol and account acceptance

Validate the actual owner/signing/funder relationship, signature type, exchange address, collateral, token ID and deployed protocol as one contract. The implementation supports the pinned Polygon CLOB V2 token-order path, canonical EOA/proxy/one-owner Safe identities and the pinned native-beacon DepositWallet with ERC7739 orders. Reject V3 position orders, legacy UUPS DepositWallets, unsupported markets and unsupported operation types; do not reinterpret a newer protocol through an older signing schema.

Confirm actual authenticated minimum size, tick, fee bounds, asset identities, permissions, order-hash identity, account observations and user-stream behavior on the intended account. Prove unknown post/cancel reconciliation, heartbeat cancel protection and restart parity with real venue responses before a funded canary. Mock outcomes are not endpoint compatibility evidence.

The BUY-only strategy closes standard positions through separately signed merge/redemption transactions. Only finalized canonical transaction effects release inventory and recycle owned cash. Unknown direct transactions retain raw hash/nonce; unknown relayer batches retain digest/body/deadline/nonce and never repeat their POST. Imported holdings cannot replenish the owned risk budget. Direct types 0/1/2 require owner POL; the type 3 relayer sponsors gas and requires an owner-created Relayer API key, distinct from CLOB L2 credentials. Current account/RPC/API acceptance and sustained recycling remain selected-account canary checks. [Official wallet/Relayer authentication](https://docs.polymarket.com/trading/wallets-auth)

Public inspection identifies account/funder `0x04Ad8Cb08F04b58fbcEa273603d6cd151Ba8aDB4` as a native-beacon DepositWallet owned by signing address `0xa2300f310b59df6d0dec94576776ce5435bd0b3f`. Its two approved CTF AutoRedeemer operators are an unresolved owner activation gate: they can alter balances without advancing wallet nonce. Current code blocks Resume/prepare until the owner disables these approvals; an integrated external-receipt path is future work. Neither approval changes nor a funded canary occurred in this implementation. Provision private key, CLOB credentials, Relayer key/address and HTTPS RPC only in private runtime environment files; never paste them into chat or reports.

## Remaining economic evidence

Acquire a small complete development set from Pendulumflow with pinned market identities, manifests and hashes. The previously acquired September 29 development window had unresolved complementary-book/equal-clock reconstruction disagreements and is not a scoring set. A successful download is not a valid book or holdout.

Acquire authorized historical governing TWAP60 reports and verify identity, publication/receive availability, boundaries and settlement labels. Binance spot is only diagnostic reference and must not replace the governing settlement stream. Additional reconstructed strategy signals must be available point in time; missing signals must not silently turn a reconstruction into a different candidate.

Freeze the actual deployment rules, parameters, fees, dataset and acceptance criteria before a new untouched holdout. Run conservative queue, cancel and delay cases with finite capital and retained unresolved positions. Report exclusions, worst days, gross/one-sided exposure and fee assumptions. Neither synthetic emulator tapes, the arithmetic multi-window run nor passing Rust/TypeScript quote vectors establish return.

## Selected-host and owner-interface verification

AWS infrastructure creation is deferred. The prepared single-host package uses eu-west-1 (Ireland) as the ordinary candidate region; actual host IP eligibility remains mandatory. Direct eu-west-2 (London) co-location requires separate Polymarket acceptance. Once an eligible host is selected, repeat public TLS/WSS readiness, clock bounds, simultaneous CLOB/reference arrival comparisons, durable recovery under resource limits and full post/cancel latency tracing. Public HTTP or local owner API latency does not measure order arrival at the matching engine. Server geography alone does not establish eligibility or co-location access.

Run the bounded acceptance monitor against the actual data filesystem and all execution lane capacities. Measure retained-byte growth, slow-storage behavior, source outages, restarts and p95/p99 with console on/off. Verify a paused restore into a new directory, external alerts and rollback compatibility. Rotation preserves evidence but cannot provide unlimited unattended retention; journal, receipt and portfolio quotas still need an operational capacity plan.

Set HTTPS ingress and the exact Mini App origin, configure the owner's Telegram bot privately and test signed owner access on a real client. Hosting purchase, registry publication and live Telegram messages are separate external actions. Their absence does not prevent verification of the offline package.

## Funded execution gate

Selecting `trading` or `both` does not itself submit or authorize funded orders. All starts are paused. A funded canary requires the owner's explicit intended account/capital/markets/stop conditions, successful protocol/host/account checks, fresh reconciliation, user stream and heartbeat, and an explicit Resume. Unknown outcomes retain possible-fill reservations. Kill is durable; Reset does not replenish cash or erase history.

`live_eligible` remains false for demo, public simulation and shadow. In a trading profile, it is a technical readiness indicator that must reflect live gates; it is not economic approval, a profitability score or evidence that a canary happened. No monthly income estimate is attached to this repository.
