# R16 experimental public-paper entry and inventory policy

The R15 model spent 8.2413 USD on eight one-sided losing positions. Five positions
were smaller than the venue minimum of five shares. These observations identify a
failure mode, not an established live edge or proof that latency is its sole cause.

Public simulation now blocks a new Up bid when the observed Binance BTC price
falls at least one basis point over a one-second exchange-time window, and blocks
a new Down bid on the opposite move. It waits for fresh, ordered reference evidence;
missing evidence cancels new entries. The bounded 64-sample buffer is rebuilt after
restart. The one-basis-point/one-second parameters are an experimental choice,
not a fitted optimum. New entry commitments retain an anchor/latest price witness
in the existing execution transaction. Spot is never a settlement oracle.

For unmatched inventory, a passive bid at the complement's current best bid may
replace the break-even-only hedge. It must strictly reduce the gross-loss bound
after full execution, including a losing complete set and any overbought residual.
Minimum-sized overbuy is allowed only under that same criterion. Smaller partial
fills must remain inside the original bound. Unrepairable dust remains explicit;
there is no guarantee that every partial fill can be economically neutralized.
An already live legal complement can still be retained. All old possible fills,
cancel-before-post reservations, finite cash, imbalance and global loss gates apply.

The new policy applies only to the public simulation decision path. Historical
execution operations replay unchanged; trading/both and offline demo keep their
existing strategy contracts. No SELL support, funded requests, risk-limit increase,
new fee assumption, free queue priority or reset of retained accounting is introduced.

Validation includes the direction, warmup, stale/reference clock and buffer bounds;
minimum-sized symmetric repair; loss-locking repair; and partial-fill gross-risk
checks across a grid of inventory and prices. Exact R15 baseline/candidate journal
recovery and the new run's source/image/profile/boot bindings have separate receipts.
Those checks do not constitute a counterfactual PnL comparison: the old journal lacks
the full historical Binance stream needed to replay the new entry filter independently.
An untouched holdout, native-feed queue audit and net-portfolio comparison remain
necessary before claiming improvement or profitability.
