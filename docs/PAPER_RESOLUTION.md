# Public simulation position closeout: October 10, 2026

The R6 public simulation retained one-sided positions after market rollover, but
only the offline demo source supplied resolution operations. Expired public markets
therefore never redeemed their remaining tokens. The finite portfolio correctly
rejected the next held market at 64 unresolved markets; propagating that rejection
out of the source then killed the process and its diagnostic API. This explains the
observed `unresolved market capacity exhausted` failure without a queue or quota
increase.

The new public paper worker reads final outcomes independently of quoting. It
checks the exact five-minute Gamma market identity and standard binary Up/Down
token mapping, derives both tokens from the Polygon Conditional Tokens contract,
and reads its payout vector at one canonical finalized block. Every contract read
uses the same EIP-1898 block hash; a final canonical-height check rejects a changed
block. Gamma prices, suggested winners, spot prices and elapsed time do not prove
resolution. The public RPC is a trusted read provider, not independently verified
consensus. See the [CTF contract](https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol)
and [developer guide](https://github.com/gnosis/conditional-tokens-contracts/blob/master/docs/developer-guide.rst).

Only an exact full binary payout is supported. Unknown, malformed, unfinalized,
split or unsupported outcomes retain positions and capital. No fallback guesses a
winner. The dedicated worker has finite body, request, target and per-pass bounds;
network requests never run on the execution actor. It is enabled only for the
public simulation lane, including the independent paper lane in both mode. It has
no signer, wallet, relayer or broadcast operation.

The actor validates the retained market and applies an exact synthetic redemption
to a candidate portfolio. It syncs the evidence and operation to the primary journal
before publishing changed cash. Replay restores the same credit exactly once;
failed sync leaves the previous cash visible and poisons the writer. Realized losses
still enforce the original daily-loss and drawdown policy. The published
`paper_resolution` field labels the accounting synthetic and exposes its completed
count and latest checked observation. There is no real redemption receipt.

All capital, loss, queue, journal and unresolved-market limits remain unchanged.
If a paper rollover still fails, the actor retains its previous market identity,
latches Pause, drains cancellations and keeps the API inspectable. Resume and Reset
remain blocked by the fault. An owner Pause or Kill can be durably acknowledged if
the writer remains healthy; it cannot recover a poisoned writer. There is no
automatic restart, Resume, capital reset or deletion of unresolved history in this
recovery path.

`mm2-paper-outcome <condition-id> <window-start-seconds>` is a bounded, read-only
diagnostic using the same verification path. It prints public outcome evidence or
null and does not change an engine ledger. A restored copy of a failed ledger must
remain paused during investigation. A separately authorized repeat experiment
uses a separate fresh namespace, preserving the old ledger and its failed result.

Older binaries cannot replay the new public resolution operation. Before rollout,
preserve the stopped original ledger and recover into separate paths. For rollback,
stop the candidate, retain its complete journal, and investigate using an isolated
pre-deployment copy and the old image; never point the older binary at the newer
journal or reset finances to make it start.
