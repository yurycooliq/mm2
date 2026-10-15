# R17 public-paper admission

R16 could reject every desired order when one side exceeded its remaining gross
loss budget. Its directional reference filter ran after that rejection. R17
filters fresh public-paper entries first, then selects an affordable subset.

The complete filtered set is preserved whenever the existing portfolio guard
admits it. Otherwise, requests are considered in the strategy's existing order:
risk-reducing complement requests precede new lots. Each addition must pass the
same strict global guard. Rejected requests are removed; the process does not
resize an order below the venue minimum or manufacture a simultaneous pair fill.
At most six requests are considered. A retained request is the same commitment;
old possible fills remain counted until terminal cancellation reconciliation.
Execution independently checks the final quotes before recording or posting.

The daily gross realized loss cap remains $10 and the realized drawdown cap $20.
Gains do not replenish the gross loss counter. Merge/redemption attribution,
UTC reset, fixed five-share lot, finite capital, public-outcome settlement,
cancel-before-post and minimum-lot inventory repair are unchanged. The policy ID
is `r17-directional-admission-subsets-v1`. Demo, trading and both retain their
existing admission policies.

Regressions cover both outcome directions, an already profitable portfolio with
$2 of loss headroom, complete affordable sets, strict equality rejection,
retained order deduplication, cancel-in-flight replacements, missing references,
adverse-direction filtering and unresolved positions from earlier markets.

The stopped R16 night is preserved separately and replayed from genesis through
its final Pause with all four journal segments. Candidate and baseline internal
execution/portfolio state must match, including positions outside the public
64-position projection. Independent integer accounting checks fills, exact side
costs, merges, public outcome redemptions, UTC rollover and quote risk admission.

This is a compatibility and accounting replay, not a counterfactual profitability
backtest. R16 did not persist the complete reference/book decision inputs for
blocked entries; new quotes cannot be assigned historical fills or PnL from that
journal. A separate forward public-paper run starts with one fresh virtual
$1,000 pool. Synthetic execution proves neither real venue fills nor net profit.
