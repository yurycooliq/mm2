//! Replayable execution inputs. The hash journal records a complete transaction before
//! the candidate emulator becomes visible. Console samples are never this ledger.
use crate::{config::Limits, domain};
use mm2_core::{DesiredQuote, Outcome, RiskLimits};
use mm2_emulator::{Book, LatencyConfig, Level, PaperSession};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct MultiMarketPaper {
    session: PaperSession,
    /// Explicit experiment latency survives market rollover; old journals use the original default.
    #[serde(default)]
    latency: LatencyConfig,
    portfolio: mm2_portfolio::Portfolio,
    current_market: Option<String>,
    public_trades: VecDeque<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) fee_policy: Option<mm2_live::fees::FeePolicy>,
    #[serde(default)]
    public_resolution_count: u64,
    #[serde(default)]
    last_public_resolution_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_public_resolution: Option<crate::paper_resolution::Evidence>,
    #[serde(default, skip_serializing_if = "is_false")]
    exact_execution_costs: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
impl Deref for MultiMarketPaper {
    type Target = PaperSession;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}
impl DerefMut for MultiMarketPaper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}
impl MultiMarketPaper {
    pub(crate) fn resolution_targets(&self) -> Vec<crate::paper_resolution::Target> {
        let mut targets: Vec<_> = self
            .portfolio
            .snapshot()
            .positions
            .into_iter()
            .filter(|p| {
                self.current_market.as_deref() != Some(&p.market.id)
                    && p.resolution.is_none()
                    && (p.up.quantity_micros > 0 || p.down.quantity_micros > 0)
            })
            .map(|p| crate::paper_resolution::Target {
                condition_id: p.market.id,
                start_ms: p.market.start_ms,
                end_ms: p.market.end_ms,
            })
            .collect();
        targets.sort_by_key(|p| p.end_ms);
        targets
    }
    pub(crate) fn resolution_status(&self) -> serde_json::Value {
        serde_json::json!({"accounting":"synthetic_redemption_from_finalized_public_ctf_outcome",
            "completed":self.public_resolution_count,"last_evidence":self.last_public_resolution,
            "execution_costs":if self.exact_execution_costs { "exact_per_outcome" } else { "legacy_aggregate_uncertainty" },
            "risk_policy":self.portfolio.policy(),
            "real_redemption_receipt":null})
    }
    pub(crate) fn has_public_trade(&self, fingerprint: &str) -> bool {
        self.public_trades.iter().any(|prior| prior == fingerprint)
    }
    pub fn portfolio_snapshot(&self) -> mm2_portfolio::Snapshot {
        self.portfolio.snapshot()
    }
    pub fn telemetry_portfolio_snapshot(&self) -> mm2_portfolio::Snapshot {
        self.portfolio.compact_snapshot(64)
    }
    pub fn portfolio_halt(&self) -> Option<&str> {
        self.portfolio.halt_reason()
    }
    pub fn risk_window_due(&self, at_ms: u64) -> bool {
        self.portfolio.risk_window_due(at_ms)
    }
    pub fn risk_clock_ms(&self) -> u64 {
        self.portfolio.risk_clock_ms()
    }
    pub fn exact_execution_costs(&self) -> bool {
        self.exact_execution_costs
    }
    pub fn can_enable_exact_costs(&self) -> bool {
        !self.exact_execution_costs
            && self.portfolio.snapshot().spent_pico == "0"
            && self.session.snapshot().account.spent_pico == "0"
            && self.session.snapshot().account.outcome_spent_pico.is_some()
    }
    /// Strategy admission only: replayed execution inputs and accounting are unchanged.
    pub fn guard_strategy_quotes(
        &self,
        rules: &mm2_strategy::Rules,
        decision: &mut mm2_strategy::Decision,
    ) -> Result<(), String> {
        self.guard_strategy_quotes_inner(rules, decision, false, false)
    }
    /// Public-paper selection only. Preserve the full decision when affordable;
    /// otherwise retain a safe subset in the strategy's existing priority order.
    /// Every check still includes all old possible fills, even canceled intents.
    pub fn guard_public_strategy_quotes(
        &self,
        rules: &mm2_strategy::Rules,
        decision: &mut mm2_strategy::Decision,
    ) -> Result<(), String> {
        if decision.quotes.len() > mm2_strategy::MAX_QUOTES {
            return Err("unbounded public strategy decision".into());
        }
        self.guard_strategy_quotes_inner(rules, decision, true, false)
    }
    /// Opt-in maker hypothesis: affordability may cancel a pair, never turn it
    /// into a directional lottery. Lifetime market losses survive closeouts.
    pub fn guard_experimental_strategy_quotes(
        &self,
        rules: &mm2_strategy::Rules,
        decision: &mut mm2_strategy::Decision,
        policy: &mm2_strategy::PaperPolicy,
    ) -> Result<(), String> {
        policy.validate()?;
        if decision.quotes.len() > mm2_strategy::MAX_QUOTES {
            return Err("unbounded experimental strategy decision".into());
        }
        let current = self
            .current_market
            .as_deref()
            .ok_or("missing strategy market")?;
        let position = self
            .portfolio
            .market(current)
            .ok_or("missing strategy inventory")?;
        let snapshot = self.session.snapshot();
        let overlaps_complement = decision
            .quotes
            .iter()
            .filter(|quote| quote.slot.ends_with(":repair"))
            .any(|quote| {
                let retained = snapshot.orders.iter().any(|order| {
                    order.quote.slot == quote.slot
                        && order.quote.outcome == quote.outcome
                        && order.quote.price_micros == quote.price_micros
                        && order.requested_quantity_micros == quote.quantity_micros
                        && order.reserved_quantity_micros > 0
                });
                !retained
                    && snapshot.orders.iter().any(|order| {
                        order.quote.outcome == quote.outcome && order.reserved_quantity_micros > 0
                    })
            });
        if overlaps_complement {
            // Cancel first and wait for terminal reconciliation before a new
            // repair, including a minimum-size overbuy for partial inventory.
            decision.quotes.clear();
            decision.reason = mm2_strategy::Gate::InventoryCost;
            return Ok(());
        }
        if position.up.quantity_micros == position.down.quantity_micros
            && !decision.quotes.is_empty()
            && (decision.quotes.len() != 2
                || decision.quotes[0].outcome == decision.quotes[1].outcome
                || decision.quotes[0].quantity_micros != decision.quotes[1].quantity_micros
                || decision.quotes.iter().any(|q| !q.slot.ends_with(":lot"))
                || decision.quotes.iter().map(|q| q.price_micros).sum::<u64>()
                    > mm2_core::SCALE - policy.min_pair_edge_micros)
        {
            decision.quotes.clear();
            decision.reason = mm2_strategy::Gate::PairedEntry;
            return Ok(());
        }
        let mut effective = rules.clone();
        effective.max_pair_edge_micros = effective
            .max_pair_edge_micros
            .max(policy.min_pair_edge_micros);
        self.guard_strategy_quotes_inner(&effective, decision, false, true)?;
        if decision.quotes.is_empty() {
            return Ok(());
        }
        let Some(realized_loss) = position.realized_gross_loss_pico()? else {
            decision.quotes.clear();
            decision.reason = mm2_strategy::Gate::MarketLossBudget;
            return Ok(());
        };
        let mut possible = [0_u64; 2];
        let mut prices = [0_u64; 2];
        for order in &snapshot.orders {
            let side = usize::from(order.quote.outcome == Outcome::Down);
            possible[side] = possible[side]
                .checked_add(order.reserved_quantity_micros)
                .ok_or("possible fill overflow")?;
            prices[side] = prices[side].max(order.quote.price_micros);
        }
        for quote in &decision.quotes {
            if !snapshot.orders.iter().any(|order| {
                order.quote.slot == quote.slot
                    && order.quote.outcome == quote.outcome
                    && order.quote.price_micros == quote.price_micros
                    && order.requested_quantity_micros == quote.quantity_micros
            }) {
                let side = usize::from(quote.outcome == Outcome::Down);
                possible[side] = possible[side]
                    .checked_add(quote.quantity_micros)
                    .ok_or("new fill overflow")?;
                prices[side] = prices[side].max(quote.price_micros);
            }
        }
        let repair = decision
            .quotes
            .iter()
            .any(|quote| quote.slot.ends_with(":repair"));
        let mut worst = 0;
        let mut worst_locked = 0;
        for mask in 0..4 {
            let mut quantities = [position.up.quantity_micros, position.down.quantity_micros];
            let mut costs = [position.up.cost_pico, position.down.cost_pico];
            for side in 0..2 {
                if mask & (1 << side) != 0 {
                    quantities[side] = quantities[side]
                        .checked_add(possible[side])
                        .ok_or("prospective inventory overflow")?;
                    costs[side] += u128::from(possible[side]) * u128::from(prices[side]);
                }
            }
            worst = worst.max(mm2_strategy::inventory_loss_bound(quantities, costs));
            let matched = quantities[0].min(quantities[1]);
            if matched > 0 {
                let matched_cost = (0..2)
                    .map(|side| {
                        (costs[side] * u128::from(matched)).div_ceil(u128::from(quantities[side]))
                    })
                    .sum::<u128>();
                worst_locked = worst_locked.max(
                    matched_cost.saturating_sub(u128::from(matched) * u128::from(mm2_core::SCALE)),
                );
            }
        }
        if realized_loss + worst
            >= u128::from(policy.market_loss_cap_micros) * u128::from(mm2_core::SCALE)
            || (repair
                && worst_locked
                    > u128::from(policy.max_locked_loss_micros) * u128::from(mm2_core::SCALE))
        {
            decision.quotes.clear();
            decision.reason = mm2_strategy::Gate::MarketLossBudget;
        }
        Ok(())
    }
    fn guard_strategy_quotes_inner(
        &self,
        rules: &mm2_strategy::Rules,
        decision: &mut mm2_strategy::Decision,
        select_subset: bool,
        retained_repair_remaining: bool,
    ) -> Result<(), String> {
        if !self.exact_execution_costs {
            decision.quotes.clear();
            decision.reason = mm2_strategy::Gate::InventoryCost;
            return Ok(());
        }
        if decision.quotes.is_empty() && decision.reason != mm2_strategy::Gate::InventoryDust {
            return Ok(());
        }
        let current = self
            .current_market
            .as_deref()
            .ok_or("missing strategy market")?;
        let position = self
            .portfolio
            .market(current)
            .ok_or("missing strategy inventory")?;
        if decision.reason == mm2_strategy::Gate::InventoryDust {
            let (opposite, owned) = if position.up.quantity_micros > position.down.quantity_micros {
                (Outcome::Down, &position.up)
            } else {
                (Outcome::Up, &position.down)
            };
            if owned.quantity_micros > 0 {
                let cap = u128::from(mm2_core::SCALE - rules.max_pair_edge_micros)
                    .saturating_sub(owned.cost_pico.div_ceil(u128::from(owned.quantity_micros)));
                // Retain an immutable, already live legal order only. No fresh lot,
                // below-minimum request, replacement or recovered queue priority.
                for order in self.session.snapshot().orders {
                    if order.quote.outcome == opposite
                        && order.phase == mm2_core::OrderPhase::Live
                        && order.reserved_quantity_micros > 0
                        && order.requested_quantity_micros >= self.session.limits().min_order_micros
                        && u128::from(order.quote.price_micros) <= cap
                    {
                        let mut quote = order.quote;
                        quote.quantity_micros = order.requested_quantity_micros;
                        decision.quotes.push(quote);
                    }
                }
                if !decision.quotes.is_empty() {
                    decision.reason = mm2_strategy::Gate::Quote;
                }
            }
            if decision.quotes.is_empty() {
                return Ok(());
            }
        }
        // Exact side basis also caps every newly requested hedge price.
        for quote in &mut decision.quotes {
            if quote.slot.ends_with(":repair") {
                let quantities = [position.up.quantity_micros, position.down.quantity_micros];
                let costs = [position.up.cost_pico, position.down.cost_pico];
                let side = usize::from(quote.outcome == Outcome::Down);
                let quantity = if retained_repair_remaining {
                    self.session
                        .snapshot()
                        .orders
                        .iter()
                        .find(|order| {
                            order.quote.slot == quote.slot
                                && order.quote.outcome == quote.outcome
                                && order.quote.price_micros == quote.price_micros
                                && order.requested_quantity_micros == quote.quantity_micros
                        })
                        .map_or(quote.quantity_micros, |order| {
                            order.reserved_quantity_micros
                        })
                } else {
                    quote.quantity_micros
                };
                let mut after = quantities;
                after[side] = after[side]
                    .checked_add(quantity)
                    .ok_or("repair quantity overflow")?;
                let mut after_costs = costs;
                after_costs[side] += u128::from(quantity) * u128::from(quote.price_micros);
                if mm2_strategy::inventory_loss_bound(after, after_costs)
                    >= mm2_strategy::inventory_loss_bound(quantities, costs)
                {
                    return Err("paper repair does not reduce gross loss bound".into());
                }
            }
            if quote.slot.ends_with(":hedge") {
                let owned = if quote.outcome == Outcome::Up {
                    &position.down
                } else {
                    &position.up
                };
                if owned.quantity_micros == 0 {
                    return Err("hedge without owned inventory".into());
                }
                let average = owned.cost_pico.div_ceil(u128::from(owned.quantity_micros));
                let tick = self.session.limits().tick_micros;
                let cap = (u128::from(mm2_core::SCALE - rules.max_pair_edge_micros)
                    .saturating_sub(average)
                    / u128::from(tick)) as u64
                    * tick;
                quote.price_micros = quote.price_micros.min(cap);
            }
        }
        decision
            .quotes
            .retain(|q| q.price_micros >= rules.min_price_micros);
        if decision.quotes.is_empty() {
            decision.reason = mm2_strategy::Gate::InventoryCost;
            return Ok(());
        }
        if !self.quote_risk_permitted(&decision.quotes)? {
            let requested = std::mem::take(&mut decision.quotes);
            if select_subset {
                for quote in requested {
                    decision.quotes.push(quote);
                    if !self.quote_risk_permitted(&decision.quotes)? {
                        decision.quotes.pop();
                    }
                }
            }
            if decision.quotes.is_empty() {
                decision.reason = mm2_strategy::Gate::LossBudget;
            }
        }
        Ok(())
    }

    pub fn quote_risk_permitted(&self, quotes: &[DesiredQuote]) -> Result<bool, String> {
        if !self.exact_execution_costs {
            return Ok(false);
        }
        let current = self
            .current_market
            .as_deref()
            .ok_or("missing risk market")?;
        let snapshot = self.session.snapshot();
        let mut possible = [0_u64; 2];
        let mut max_price = 0;
        for order in &snapshot.orders {
            let side = usize::from(order.quote.outcome == Outcome::Down);
            possible[side] = possible[side]
                .checked_add(order.reserved_quantity_micros)
                .ok_or("possible fill overflow")?;
            max_price = max_price.max(order.quote.price_micros);
        }
        for quote in quotes {
            // A retained desired order is the same commitment, including partial fills.
            // Replacements still count both old possible fills and the new intent.
            if !snapshot.orders.iter().any(|order| {
                order.quote.slot == quote.slot
                    && order.quote.outcome == quote.outcome
                    && order.quote.price_micros == quote.price_micros
                    && order.requested_quantity_micros == quote.quantity_micros
            }) {
                let side = usize::from(quote.outcome == Outcome::Down);
                possible[side] = possible[side]
                    .checked_add(quote.quantity_micros)
                    .ok_or("new fill overflow")?;
                max_price = max_price.max(quote.price_micros);
            }
        }
        self.portfolio
            .permits_execution_risk(current, possible, max_price)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.exact_execution_costs != self.session.automatic_posts_deferred() {
            return Err("paper accounting and post admission versions disagree".into());
        }
        if let Some(policy) = &self.fee_policy {
            policy.validate()?;
            if self.current_market.as_deref() != Some(policy.condition_id.as_str())
                || !policy.maker_zero()
            {
                return Err("durable paper fee policy disagrees with market".into());
            }
        }
        self.session.validate()?;
        self.portfolio.validate()?;
        if let Some(id) = &self.current_market {
            let position = self
                .portfolio
                .market(id)
                .ok_or("missing current portfolio market")?;
            let account = self.session.snapshot().account;
            if self.exact_execution_costs
                && position.execution_outcome_spent_pico != account.outcome_spent_pico
            {
                return Err("current exact outcome costs disagree with durable portfolio".into());
            }
            if position.execution_spent_pico
                != account
                    .spent_pico
                    .parse::<u128>()
                    .map_err(|_| "invalid simulated spend")?
                || position.execution_up_micros != account.up_inventory_micros
                || position.execution_down_micros != account.down_inventory_micros
            {
                return Err("current paper session disagrees with durable portfolio totals".into());
            }
        }
        Ok(())
    }
    fn sync_portfolio(&mut self) -> Result<(), String> {
        if let Some(market) = &self.current_market {
            let account = self.session.snapshot().account;
            if self.exact_execution_costs {
                let costs = account
                    .outcome_spent_pico
                    .ok_or("missing exact execution costs")?;
                self.portfolio.record_execution_outcome_totals(
                    market,
                    [
                        costs[0].parse().map_err(|_| "invalid exact outcome cost")?,
                        costs[1].parse().map_err(|_| "invalid exact outcome cost")?,
                    ],
                    [account.up_inventory_micros, account.down_inventory_micros],
                )?;
            } else {
                self.portfolio.record_execution_totals(
                    market,
                    account
                        .spent_pico
                        .parse()
                        .map_err(|_| "invalid exact simulated spend")?,
                    account.up_inventory_micros,
                    account.down_inventory_micros,
                    0,
                )?;
            }
        }
        Ok(())
    }
    fn bind_market(
        &mut self,
        id: &str,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<(), String> {
        if self.current_market.as_deref() == Some(id) {
            let market = &self
                .portfolio
                .market(id)
                .ok_or("missing current market")?
                .market;
            if start_ms.is_some_and(|start| start != market.start_ms)
                || end_ms.is_some_and(|end| end != market.end_ms)
            {
                return Err("market window identity cannot change".into());
            }
            return Ok(());
        }
        self.sync_portfolio()?;
        if let Some(previous) = &self.current_market {
            let snapshot = self.session.snapshot();
            if snapshot.pending_events != 0 || snapshot.account.reserved_micros_ceil != 0 {
                return Err(
                    "market rollover requires authoritative terminal simulated orders".into(),
                );
            }
            let quantity = snapshot
                .account
                .up_inventory_micros
                .min(snapshot.account.down_inventory_micros);
            if quantity > 0 {
                self.portfolio.merge_complete_sets(
                    mm2_portfolio::MergeReceipt {
                        id: format!("paper-merge:{previous}"),
                        market_id: previous.clone(),
                        quantity_micros: quantity,
                        fee_pico: 0,
                        at_ms: self
                            .portfolio
                            .market(previous)
                            .ok_or("missing retained paper market")?
                            .market
                            .end_ms
                            .max(self.last_public_resolution_at_ms),
                        provenance: mm2_portfolio::Provenance::Synthetic,
                    },
                    true,
                )?;
            }
            let mut limits = self.session.limits().clone();
            limits.initial_cash_micros = self.portfolio.capital_for_new_market()?;
            if limits.initial_cash_micros == 0 {
                return Err("finite paper portfolio collateral exhausted".into());
            }
            let now = self.session.now_ms();
            self.session = PaperSession::new(limits, self.latency.clone())?;
            if self.exact_execution_costs {
                self.session.defer_automatic_posts();
            }
            self.session.advance(now)?;
        }
        self.portfolio.register_market(mm2_portfolio::Market {
            id: id.into(),
            start_ms: start_ms.unwrap_or(0),
            end_ms: end_ms.unwrap_or(86_400_000),
        })?;
        self.current_market = Some(id.into());
        self.fee_policy = None;
        self.public_trades.clear();
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// Versioned at a zero-spend baseline; never rewrites legacy allocations.
    ExactAccounting {},
    /// Audit witness only; execution replay still uses the original immutable quotes.
    PaperEntrySignal {
        anchor_exchange_ms: u64,
        anchor_price_micro: u64,
        latest_exchange_ms: u64,
        latest_price_micro: u64,
    },
    FeeRules {
        policy: mm2_live::fees::FeePolicy,
    },
    Market {
        condition_id: String,
        #[serde(default)]
        start_ms: Option<u64>,
        #[serde(default)]
        end_ms: Option<u64>,
    },
    Advance {
        now_ms: u64,
    },
    /// Explicit UTC risk window; legacy Advance keeps its original replay semantics.
    RiskWindow {
        at_ms: u64,
    },
    Quotes {
        now_ms: u64,
        quotes: Vec<Quote>,
    },
    CancelAll {
        now_ms: u64,
    },
    Book {
        now_ms: u64,
        outcome: String,
        bid: u64,
        ask: u64,
        ahead_micros: u64,
    },
    /// Observed public depth at the best bid and relevant immutable order prices.
    /// Levels omitted from this bounded projection cannot seed an order queue.
    DepthBook {
        now_ms: u64,
        outcome: String,
        bid: u64,
        ask: u64,
        bid_levels: Vec<Level>,
    },
    Trade {
        now_ms: u64,
        outcome: String,
        worst_price_micros: u64,
        quantity_micros: u64,
    },
    ObservedTrade {
        now_ms: u64,
        outcome: String,
        price_micros: u64,
        quantity_micros: u64,
        fingerprint: String,
    },
    ObservedTradeV2 {
        now_ms: u64,
        outcome: String,
        price_micros: u64,
        quantity_micros: u64,
        fingerprint: String,
    },
    /// Causal execution revision: original exchange clock and its caller-checked
    /// normalization are persisted. V2 historical replay remains unchanged.
    ObservedTradeV3 {
        now_ms: u64,
        received_wall_ms: u64,
        exchange_event_mono_ms: u64,
        exchange_ms: u64,
        outcome: String,
        price_micros: u64,
        quantity_micros: u64,
        fingerprint: String,
    },
    VenueRules {
        now_ms: u64,
        tick_micros: u64,
        min_order_micros: u64,
    },
    /// Explicit synthetic settlement input. Public feeds never generate this operation.
    DemoResolution {
        condition_id: String,
        winning_outcome: String,
        at_ms: u64,
    },
    /// Public final outcome, synthetic redemption and cash.
    PublicResolution {
        evidence: crate::paper_resolution::Evidence,
        at_ms: u64,
    },
    Limits {
        now_ms: u64,
        limits: Limits,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quote {
    pub slot: String,
    pub outcome: String,
    pub price_micros: u64,
    pub quantity_micros: u64,
}

fn outcome(name: &str) -> Result<Outcome, String> {
    match name {
        "up" => Ok(Outcome::Up),
        "down" => Ok(Outcome::Down),
        _ => Err("invalid simulation outcome".into()),
    }
}

pub fn risk(limits: &Limits) -> Result<RiskLimits, String> {
    let config = limits.core()?;
    Ok(RiskLimits {
        initial_cash_micros: config.initial_collateral_micro,
        max_imbalance_micros: config.max_imbalance_micro,
        min_order_micros: 5_000_000,
        tick_micros: 1_000,
        max_orders: 16,
    })
}

pub fn new(limits: &Limits) -> Result<MultiMarketPaper, String> {
    new_with_latency(limits, LatencyConfig::default())
}

pub fn new_with_latency(
    limits: &Limits,
    latency: LatencyConfig,
) -> Result<MultiMarketPaper, String> {
    latency.validate()?;
    let initial = limits.core()?.initial_collateral_micro;
    let portfolio = mm2_portfolio::Portfolio::new(
        initial,
        mm2_portfolio::RiskPolicy {
            max_total_exposure_micros: initial,
            max_market_exposure_micros: initial,
            ..Default::default()
        },
    )?;
    Ok(MultiMarketPaper {
        session: PaperSession::new(risk(limits)?, latency.clone())?,
        latency,
        portfolio,
        current_market: None,
        public_trades: VecDeque::new(),
        fee_policy: None,
        public_resolution_count: 0,
        last_public_resolution_at_ms: 0,
        last_public_resolution: None,
        exact_execution_costs: false,
    })
}

pub fn apply(session: &mut MultiMarketPaper, operation: &Operation) -> Result<(), String> {
    let result = match operation {
        Operation::PaperEntrySignal {
            anchor_exchange_ms,
            anchor_price_micro,
            latest_exchange_ms,
            latest_price_micro,
        } => {
            if !session.exact_execution_costs
                || *anchor_price_micro == 0
                || *latest_price_micro == 0
                || latest_exchange_ms
                    .checked_sub(*anchor_exchange_ms)
                    .is_none_or(|span| !(1_000..=1_500).contains(&span))
            {
                return Err("invalid paper entry signal witness".into());
            }
            Ok(())
        }
        Operation::ExactAccounting {} => {
            if !session.exact_execution_costs && !session.can_enable_exact_costs() {
                return Err("legacy accounting uncertainty blocks version change".into());
            }
            session.exact_execution_costs = true;
            session.session.defer_automatic_posts();
            Ok(())
        }
        Operation::FeeRules { policy } => {
            policy.validate()?;
            if session.current_market.as_deref() != Some(&policy.condition_id)
                || !policy.maker_zero()
            {
                return Err("simulation fee identity or maker schedule mismatch".into());
            }
            session.fee_policy = Some(policy.clone());
            Ok(())
        }
        Operation::Market {
            condition_id,
            start_ms,
            end_ms,
        } => {
            if condition_id.len() == 66
                && condition_id.starts_with("0x")
                && condition_id[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                session.bind_market(condition_id, *start_ms, *end_ms)
            } else {
                Err("invalid durable simulation market identity".into())
            }
        }
        Operation::Advance { now_ms } => {
            session.advance(*now_ms).map_err(|error| error.to_string())
        }
        Operation::RiskWindow { at_ms } => session.portfolio.advance_risk_window(*at_ms),
        Operation::CancelAll { now_ms } => session
            .cancel_all(*now_ms)
            .map_err(|error| error.to_string()),
        Operation::Quotes { now_ms, quotes } => {
            if !quotes.is_empty() && session.portfolio_halt().is_some() {
                return Err("paper portfolio loss/drawdown halt blocks new quotes".into());
            }
            if session.exact_execution_costs && !quotes.is_empty() {
                let desired = quotes
                    .iter()
                    .map(|quote| {
                        Ok(DesiredQuote {
                            slot: quote.slot.clone(),
                            outcome: outcome(&quote.outcome)?,
                            price_micros: quote.price_micros,
                            quantity_micros: quote.quantity_micros,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                if !session.quote_risk_permitted(&desired)? {
                    return Err("exact paper risk budget blocks new quotes".into());
                }
            }
            session
                .quote(
                    *now_ms,
                    quotes
                        .iter()
                        .map(|quote| {
                            Ok(DesiredQuote {
                                slot: quote.slot.clone(),
                                outcome: outcome(&quote.outcome)?,
                                price_micros: quote.price_micros,
                                quantity_micros: quote.quantity_micros,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                )
                .map_err(|error| error.to_string())
        }
        Operation::VenueRules {
            now_ms,
            tick_micros,
            min_order_micros,
        } => {
            let mut limits = session.limits().clone();
            limits.tick_micros = *tick_micros;
            limits.min_order_micros = *min_order_micros;
            session
                .set_risk_limits(*now_ms, limits)
                .map_err(|error| error.to_string())
        }
        Operation::Book {
            now_ms,
            outcome: name,
            bid,
            ask,
            ahead_micros,
        } => session
            .set_book(
                *now_ms,
                outcome(name)?,
                Book {
                    bid: Some(*bid),
                    ask: Some(*ask),
                    bid_levels: vec![Level {
                        price_micros: *bid,
                        quantity_micros: *ahead_micros,
                    }],
                },
            )
            .map_err(|error| error.to_string()),
        Operation::DepthBook {
            now_ms,
            outcome: name,
            bid,
            ask,
            bid_levels,
        } => {
            if !session.exact_execution_costs {
                return Err("observed depth requires versioned accounting".into());
            }
            session
                .set_book_conservative(
                    *now_ms,
                    outcome(name)?,
                    Book {
                        bid: Some(*bid),
                        ask: Some(*ask),
                        bid_levels: bid_levels.clone(),
                    },
                )
                .map_err(|error| error.to_string())
        }
        Operation::Trade {
            now_ms,
            outcome: name,
            worst_price_micros,
            quantity_micros,
        } => session
            .trade(
                *now_ms,
                outcome(name)?,
                *worst_price_micros,
                *quantity_micros,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Operation::ObservedTrade {
            now_ms,
            outcome: name,
            price_micros,
            quantity_micros,
            fingerprint,
        }
        | Operation::ObservedTradeV2 {
            now_ms,
            outcome: name,
            price_micros,
            quantity_micros,
            fingerprint,
        } => {
            if fingerprint.len() != 64 || !fingerprint.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err("invalid observed trade fingerprint".into());
            }
            if session.public_trades.contains(fingerprint) {
                return Ok(());
            }
            if matches!(operation, Operation::ObservedTradeV2 { .. }) {
                if !session.exact_execution_costs {
                    return Err("observed trade requires versioned accounting".into());
                }
                session.trade_at_price(*now_ms, outcome(name)?, *price_micros, *quantity_micros)?;
            } else {
                session.trade(*now_ms, outcome(name)?, *price_micros, *quantity_micros)?;
            }
            if session.public_trades.len() == 8192 {
                session.public_trades.pop_front();
            }
            session.public_trades.push_back(fingerprint.clone());
            Ok(())
        }
        Operation::ObservedTradeV3 {
            now_ms,
            received_wall_ms,
            exchange_event_mono_ms,
            exchange_ms,
            outcome: name,
            price_micros,
            quantity_micros,
            fingerprint,
        } => {
            if !session.exact_execution_costs
                || *exchange_ms == 0
                || received_wall_ms.checked_sub(*exchange_ms)
                    != now_ms.checked_sub(*exchange_event_mono_ms)
                || *received_wall_ms < *exchange_ms
                || *exchange_event_mono_ms > *now_ms
                || fingerprint.len() != 64
                || !fingerprint.bytes().all(|c| c.is_ascii_hexdigit())
            {
                return Err("invalid causal public trade witness".into());
            }
            if session.public_trades.contains(fingerprint) {
                return Ok(());
            }
            session.trade_at_price_causal(
                *now_ms,
                *exchange_event_mono_ms,
                outcome(name)?,
                *price_micros,
                *quantity_micros,
            )?;
            if session.public_trades.len() == 8192 {
                session.public_trades.pop_front();
            }
            session.public_trades.push_back(fingerprint.clone());
            Ok(())
        }
        Operation::DemoResolution {
            condition_id,
            winning_outcome,
            at_ms,
        } => {
            if session.current_market.as_deref() == Some(condition_id) {
                return Err("cannot resolve the current paper execution market".into());
            }
            session
                .portfolio
                .redeem(
                    mm2_portfolio::ResolutionReceipt {
                        id: format!("demo-resolution:{condition_id}"),
                        market_id: condition_id.clone(),
                        winning_outcome: outcome(winning_outcome)?,
                        fee_pico: 0,
                        at_ms: *at_ms,
                        finality_confirmed: true,
                        provenance: mm2_portfolio::Provenance::Synthetic,
                    },
                    true,
                )
                .map(|_| ())
        }
        Operation::PublicResolution { evidence, at_ms } => {
            let winner = evidence.winner()?;
            let target = &evidence.target;
            if session.current_market.as_deref() == Some(&target.condition_id) {
                return Err("cannot resolve the current paper execution market".into());
            }
            let market = session
                .portfolio
                .market(&target.condition_id)
                .ok_or("unknown public resolution market")?;
            if market.market.start_ms != target.start_ms
                || market.market.end_ms != target.end_ms
                || *at_ms < evidence.observed_at_ms
            {
                return Err("public resolution identity or observation clock mismatch".into());
            }
            let receipt_id = format!("paper-public-resolution:{}", target.condition_id);
            if let Some(previous) = &market.resolution {
                return if previous.id == receipt_id && previous.winning_outcome == winner {
                    Ok(())
                } else {
                    Err("public resolution conflicts with the retained receipt".into())
                };
            }
            let applied = session.portfolio.redeem(
                mm2_portfolio::ResolutionReceipt {
                    id: receipt_id,
                    market_id: target.condition_id.clone(),
                    winning_outcome: winner,
                    fee_pico: 0,
                    at_ms: *at_ms,
                    finality_confirmed: true,
                    provenance: mm2_portfolio::Provenance::Synthetic,
                },
                true,
            )?;
            if applied {
                session.public_resolution_count += 1;
                session.last_public_resolution_at_ms = *at_ms;
                session.last_public_resolution = Some(evidence.clone());
            }
            Ok(())
        }
        Operation::Limits { now_ms, limits } => {
            let mut risk = risk(limits)?;
            if u128::from(limits.core()?.initial_collateral_micro)
                != session
                    .portfolio_snapshot()
                    .initial_cash_pico
                    .parse::<u128>()
                    .map_err(|_| "corrupt portfolio initial cash")?
                    / 1_000_000
            {
                return Err("initial portfolio collateral cannot change".into());
            }
            risk.initial_cash_micros = session.limits().initial_cash_micros;
            risk.tick_micros = session.limits().tick_micros;
            risk.min_order_micros = session.limits().min_order_micros;
            session
                .set_risk_limits(*now_ms, risk)
                .map_err(|error| error.to_string())
        }
    };
    result?;
    session.sync_portfolio()
}

pub fn replay(session: &mut MultiMarketPaper, inputs: &[serde_json::Value]) -> Result<(), String> {
    for input in inputs {
        let operation: Operation = serde_json::from_value(input.clone())
            .map_err(|_| "invalid durable simulation operation")?;
        apply(session, &operation)?;
    }
    session.validate().map_err(|error| error.to_string())
}

pub fn values(inputs: &[Operation]) -> Result<Vec<serde_json::Value>, String> {
    inputs
        .iter()
        .map(|input| {
            serde_json::to_value(input).map_err(|_| "simulation input encoding failed".into())
        })
        .collect()
}

pub fn desired(decision: &domain::Decision) -> Vec<Quote> {
    decision
        .intents
        .iter()
        .map(|intent| Quote {
            slot: match intent.outcome {
                domain::Outcome::Up => "up_bid",
                domain::Outcome::Down => "down_bid",
            }
            .into(),
            outcome: match intent.outcome {
                domain::Outcome::Up => "up",
                domain::Outcome::Down => "down",
            }
            .into(),
            price_micros: intent.price_micro,
            quantity_micros: intent.quantity_micro,
        })
        .collect()
}

pub fn from_core_quotes(quotes: &[DesiredQuote]) -> Vec<Quote> {
    quotes
        .iter()
        .map(|quote| Quote {
            slot: quote.slot.clone(),
            outcome: if quote.outcome == Outcome::Up {
                "up"
            } else {
                "down"
            }
            .into(),
            price_micros: quote.price_micros,
            quantity_micros: quote.quantity_micros,
        })
        .collect()
}

/// Execution-clock-only advances have no causal effect. Recorded UTC risk windows,
/// any pending event, reservation,
/// account, venue, desired order, sequence or audit change remains compared.
pub fn same_state_without_clock(
    left: &MultiMarketPaper,
    right: &MultiMarketPaper,
) -> Result<bool, String> {
    fn state(session: &MultiMarketPaper) -> Result<serde_json::Value, String> {
        let mut value =
            serde_json::to_value(session).map_err(|_| "simulation comparison encoding failed")?;
        value
            .get_mut("session")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("simulation comparison schema failed")?
            .remove("now_ms");
        Ok(value)
    }
    Ok(state(left)? == state(right)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profitable_paper_with_two_dollars_remaining() -> MultiMarketPaper {
        let mut paper = new(&limits()).unwrap();
        apply(&mut paper, &Operation::ExactAccounting {}).unwrap();
        for (name, quantity, cost, winner, at) in [
            ('d', 20_000_000, 4_000_000_000_000, Outcome::Up, 1),
            ('e', 10_000_000, 8_000_000_000_000, Outcome::Down, 2),
        ] {
            let market = mm2_portfolio::Market {
                id: id(name),
                start_ms: at - 1,
                end_ms: at,
            };
            paper.portfolio.register_market(market.clone()).unwrap();
            paper
                .portfolio
                .record_execution_outcome_totals(&market.id, [cost, 0], [quantity, 0])
                .unwrap();
            paper
                .portfolio
                .redeem(
                    mm2_portfolio::ResolutionReceipt {
                        id: format!("resolve-{name}"),
                        market_id: market.id,
                        winning_outcome: winner,
                        fee_pico: 0,
                        at_ms: at,
                        finality_confirmed: true,
                        provenance: mm2_portfolio::Provenance::Synthetic,
                    },
                    true,
                )
                .unwrap();
        }
        apply(
            &mut paper,
            &Operation::Market {
                condition_id: id('f'),
                start_ms: Some(2),
                end_ms: Some(300_002),
            },
        )
        .unwrap();
        assert_eq!(
            paper.portfolio_snapshot().realized_pnl_pico,
            "8000000000000"
        );
        assert_eq!(paper.portfolio_snapshot().daily_loss_pico, "8000000000000");
        paper
    }
    fn decision(quotes: Vec<DesiredQuote>) -> mm2_strategy::Decision {
        mm2_strategy::Decision {
            quotes,
            reason: mm2_strategy::Gate::Quote,
            center_micros: None,
            half_width_micros: None,
        }
    }
    fn lot(outcome: Outcome, price_micros: u64) -> DesiredQuote {
        DesiredQuote {
            slot: format!("{}:lot", if outcome == Outcome::Up { "up" } else { "down" }),
            outcome,
            price_micros,
            quantity_micros: 5_000_000,
        }
    }
    fn experimental_paper() -> MultiMarketPaper {
        let mut paper = new(&limits()).unwrap();
        apply(&mut paper, &Operation::ExactAccounting {}).unwrap();
        apply(
            &mut paper,
            &Operation::Market {
                condition_id: id('a'),
                start_ms: Some(0),
                end_ms: Some(300_000),
            },
        )
        .unwrap();
        paper
    }
    #[test]
    fn experimental_pair_never_falls_back_to_singleton() {
        let paper = profitable_paper_with_two_dollars_remaining();
        let rules = mm2_strategy::Rules::default();
        let policy = mm2_strategy::PaperPolicy::default();
        let mut d = decision(vec![lot(Outcome::Up, 300_000), lot(Outcome::Down, 600_000)]);
        paper
            .guard_experimental_strategy_quotes(&rules, &mut d, &policy)
            .unwrap();
        assert!(d.quotes.is_empty());
        assert_eq!(d.reason, mm2_strategy::Gate::LossBudget);
        let mut d = decision(vec![lot(Outcome::Down, 300_000)]);
        paper
            .guard_experimental_strategy_quotes(&rules, &mut d, &policy)
            .unwrap();
        assert!(d.quotes.is_empty());
        assert_eq!(d.reason, mm2_strategy::Gate::PairedEntry);
    }
    #[test]
    fn experimental_market_budget_includes_negative_merge_even_after_basis_is_zero() {
        let mut paper = experimental_paper();
        paper
            .portfolio
            .record_execution_outcome_totals(
                &id('a'),
                [3_000_000_000_000, 2_500_000_000_000],
                [5_000_000, 5_000_000],
            )
            .unwrap();
        paper
            .portfolio
            .merge_complete_sets(
                mm2_portfolio::MergeReceipt {
                    id: "loss-merge".into(),
                    market_id: id('a'),
                    quantity_micros: 5_000_000,
                    fee_pico: 0,
                    at_ms: 1,
                    provenance: mm2_portfolio::Provenance::Synthetic,
                },
                true,
            )
            .unwrap();
        let mut d = decision(vec![lot(Outcome::Up, 500_000), lot(Outcome::Down, 480_000)]);
        paper
            .guard_experimental_strategy_quotes(
                &mm2_strategy::Rules::default(),
                &mut d,
                &mm2_strategy::PaperPolicy::default(),
            )
            .unwrap();
        assert!(d.quotes.is_empty()); // $0.50 spent + $2.50 possible = strict $3 cap.
        assert_eq!(d.reason, mm2_strategy::Gate::MarketLossBudget);
        let mut d = decision(vec![lot(Outcome::Up, 490_000), lot(Outcome::Down, 490_000)]);
        paper
            .guard_experimental_strategy_quotes(
                &mm2_strategy::Rules::default(),
                &mut d,
                &mm2_strategy::PaperPolicy::default(),
            )
            .unwrap();
        assert_eq!(d.quotes.len(), 2); // $0.50 + $2.45 remains below the allocation.
    }
    #[test]
    fn experimental_guard_retains_queue_but_counts_cancel_in_flight_replacements() {
        let mut paper = experimental_paper();
        for outcome in [Outcome::Up, Outcome::Down] {
            paper
                .set_book(
                    0,
                    outcome,
                    Book {
                        bid: Some(490_000),
                        ask: Some(510_000),
                        bid_levels: vec![],
                    },
                )
                .unwrap();
        }
        let quotes = vec![lot(Outcome::Up, 490_000), lot(Outcome::Down, 490_000)];
        paper.quote(0, quotes.clone()).unwrap();
        paper.advance(100).unwrap();
        let mut d = decision(quotes);
        paper
            .guard_experimental_strategy_quotes(
                &mm2_strategy::Rules::default(),
                &mut d,
                &mm2_strategy::PaperPolicy::default(),
            )
            .unwrap();
        assert_eq!(d.quotes.len(), 2);
        paper.cancel_all(100).unwrap();
        let replacement = vec![lot(Outcome::Up, 500_000), lot(Outcome::Down, 480_000)];
        let mut d = decision(replacement.clone());
        paper
            .guard_experimental_strategy_quotes(
                &mm2_strategy::Rules::default(),
                &mut d,
                &mm2_strategy::PaperPolicy::default(),
            )
            .unwrap();
        assert!(d.quotes.is_empty());
        assert_eq!(d.reason, mm2_strategy::Gate::MarketLossBudget);
        paper.advance(1000).unwrap();
        let mut d = decision(replacement);
        paper
            .guard_experimental_strategy_quotes(
                &mm2_strategy::Rules::default(),
                &mut d,
                &mm2_strategy::PaperPolicy::default(),
            )
            .unwrap();
        assert_eq!(d.quotes.len(), 2);
        paper.validate().unwrap();
    }
    #[test]
    fn experimental_repair_waits_for_complement_cancel_and_retains_partial_order_identity() {
        let mut paper = experimental_paper();
        for (name, bid, ask) in [("up", 480_000, 500_000), ("down", 550_000, 560_000)] {
            apply(
                &mut paper,
                &Operation::Book {
                    now_ms: 0,
                    outcome: name.into(),
                    bid,
                    ask,
                    ahead_micros: 0,
                },
            )
            .unwrap();
        }
        let mut entry = lot(Outcome::Up, 480_000);
        entry.slot = "up:lot".into();
        apply(
            &mut paper,
            &Operation::Quotes {
                now_ms: 0,
                quotes: from_core_quotes(&[entry]),
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 100 }).unwrap();
        apply(
            &mut paper,
            &Operation::Trade {
                now_ms: 100,
                outcome: "up".into(),
                worst_price_micros: 480_000,
                quantity_micros: 5_000_000,
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 200 }).unwrap();
        assert_eq!(
            paper.portfolio.market(&id('a')).unwrap().up.quantity_micros,
            5_000_000
        );
        let mut hedge = lot(Outcome::Down, 500_000);
        hedge.slot = "down:hedge".into();
        apply(
            &mut paper,
            &Operation::Quotes {
                now_ms: 200,
                quotes: from_core_quotes(&[hedge]),
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 300 }).unwrap();
        let mut repair = lot(Outcome::Down, 550_000);
        repair.slot = "down:repair".into();
        let mut d = decision(vec![repair.clone()]);
        let rules = mm2_strategy::Rules::default();
        let policy = mm2_strategy::PaperPolicy::default();
        paper
            .guard_experimental_strategy_quotes(&rules, &mut d, &policy)
            .unwrap();
        assert!(d.quotes.is_empty());
        assert_eq!(d.reason, mm2_strategy::Gate::InventoryCost);
        apply(&mut paper, &Operation::CancelAll { now_ms: 300 }).unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 1000 }).unwrap();
        let mut d = decision(vec![repair.clone()]);
        paper
            .guard_experimental_strategy_quotes(&rules, &mut d, &policy)
            .unwrap();
        assert_eq!(d.quotes, vec![repair.clone()]);
        apply(
            &mut paper,
            &Operation::Quotes {
                now_ms: 1000,
                quotes: from_core_quotes(&[repair.clone()]),
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 1100 }).unwrap();
        apply(
            &mut paper,
            &Operation::Trade {
                now_ms: 1100,
                outcome: "down".into(),
                worst_price_micros: 550_000,
                quantity_micros: 2_000_000,
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 1200 }).unwrap();
        assert_eq!(
            paper
                .portfolio
                .market(&id('a'))
                .unwrap()
                .down
                .quantity_micros,
            2_000_000
        );
        let mut d = decision(vec![repair.clone()]);
        paper
            .guard_experimental_strategy_quotes(&rules, &mut d, &policy)
            .unwrap();
        assert_eq!(d.quotes, vec![repair]); // original five-share identity; three still possible.
        paper.validate().unwrap();
    }
    #[test]
    fn causal_public_trade_clock_witness_and_duplicate_identity_are_replayable() {
        let mut paper = experimental_paper();
        apply(
            &mut paper,
            &Operation::Book {
                now_ms: 0,
                outcome: "up".into(),
                bid: 490_000,
                ask: 510_000,
                ahead_micros: 0,
            },
        )
        .unwrap();
        apply(
            &mut paper,
            &Operation::Quotes {
                now_ms: 0,
                quotes: from_core_quotes(&[lot(Outcome::Up, 490_000)]),
            },
        )
        .unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 100 }).unwrap();
        let before = serde_json::to_value(&paper).unwrap();
        let mut op = Operation::ObservedTradeV3 {
            now_ms: 100,
            received_wall_ms: 1100,
            exchange_event_mono_ms: 49,
            exchange_ms: 1050,
            outcome: "up".into(),
            price_micros: 490_000,
            quantity_micros: 1_000_000,
            fingerprint: "a".repeat(64),
        };
        assert!(apply(&mut paper, &op).is_err());
        assert_eq!(before, serde_json::to_value(&paper).unwrap());
        if let Operation::ObservedTradeV3 {
            exchange_event_mono_ms,
            ..
        } = &mut op
        {
            *exchange_event_mono_ms = 50;
        }
        apply(&mut paper, &op).unwrap();
        apply(&mut paper, &Operation::Advance { now_ms: 200 }).unwrap();
        assert_eq!(paper.snapshot().account.up_inventory_micros, 1_000_000);
        if let Operation::ObservedTradeV3 {
            now_ms,
            received_wall_ms,
            ..
        } = &mut op
        {
            *now_ms = 200;
            *received_wall_ms = 1200;
        }
        apply(&mut paper, &op).unwrap();
        assert_eq!(paper.snapshot().account.up_inventory_micros, 1_000_000);
        paper.validate().unwrap();
    }
    #[test]
    fn public_selection_keeps_affordable_side_symmetrically_without_replenishing_gross_budget() {
        let paper = profitable_paper_with_two_dollars_remaining();
        let rules = mm2_strategy::Rules::default();
        for cheap in [Outcome::Up, Outcome::Down] {
            let quotes = vec![
                lot(
                    Outcome::Up,
                    if cheap == Outcome::Up {
                        300_000
                    } else {
                        600_000
                    },
                ),
                lot(
                    Outcome::Down,
                    if cheap == Outcome::Down {
                        300_000
                    } else {
                        600_000
                    },
                ),
            ];
            let mut old = decision(quotes.clone());
            paper.guard_strategy_quotes(&rules, &mut old).unwrap();
            assert!(old.quotes.is_empty());
            let mut selected = decision(quotes);
            paper
                .guard_public_strategy_quotes(&rules, &mut selected)
                .unwrap();
            assert_eq!(selected.quotes, vec![lot(cheap, 300_000)]);
            assert_eq!(selected.reason, mm2_strategy::Gate::Quote);
            assert!(paper.quote_risk_permitted(&selected.quotes).unwrap());
        }
        let mut affordable = decision(vec![lot(Outcome::Up, 150_000), lot(Outcome::Down, 150_000)]);
        let original = affordable.clone();
        paper
            .guard_public_strategy_quotes(&rules, &mut affordable)
            .unwrap();
        assert_eq!(affordable, original);
        let mut too_expensive =
            decision(vec![lot(Outcome::Up, 400_000), lot(Outcome::Down, 600_000)]);
        paper
            .guard_public_strategy_quotes(&rules, &mut too_expensive)
            .unwrap();
        assert_eq!(too_expensive.reason, mm2_strategy::Gate::LossBudget);
        assert!(too_expensive.quotes.is_empty()); // Equality remains blocked.
        assert_eq!(paper.portfolio_snapshot().daily_loss_pico, "8000000000000");
        assert_eq!(paper.portfolio.policy().max_daily_loss_micros, 10_000_000);
        assert_eq!(paper.portfolio.policy().max_drawdown_micros, 20_000_000);
    }
    #[test]
    fn selected_quotes_keep_cancel_race_and_retained_order_reservations() {
        let mut paper = profitable_paper_with_two_dollars_remaining();
        paper
            .set_book(
                0,
                Outcome::Up,
                Book {
                    bid: Some(300_000),
                    ask: Some(320_000),
                    bid_levels: vec![],
                },
            )
            .unwrap();
        let quote = lot(Outcome::Up, 300_000);
        paper.quote(0, vec![quote.clone()]).unwrap();
        paper.advance(100).unwrap();
        let rules = mm2_strategy::Rules::default();
        let mut retained = decision(vec![quote]);
        paper
            .guard_public_strategy_quotes(&rules, &mut retained)
            .unwrap();
        assert_eq!(retained.quotes.len(), 1);
        paper.cancel_all(100).unwrap();
        let replacement = lot(Outcome::Up, 310_000);
        let mut pending = decision(vec![replacement.clone()]);
        paper
            .guard_public_strategy_quotes(&rules, &mut pending)
            .unwrap();
        assert!(pending.quotes.is_empty());
        paper.advance(1000).unwrap();
        let mut confirmed = decision(vec![replacement.clone()]);
        paper
            .guard_public_strategy_quotes(&rules, &mut confirmed)
            .unwrap();
        assert_eq!(confirmed.quotes, vec![replacement]);
        paper.validate().unwrap();
    }
    #[test]
    fn recorded_utc_window_replays_without_using_synthetic_future_time() {
        let mut session = new(&limits()).unwrap();
        let mut tape = executed_window();
        tape.extend([
            Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
            Operation::DemoResolution {
                condition_id: id('a'),
                winning_outcome: "down".into(),
                at_ms: 300_000,
            },
        ]);
        replay(&mut session, &values(&tape).unwrap()).unwrap();
        let loss = session.portfolio_snapshot().daily_loss_pico;
        assert_ne!(loss, "0");
        let before = session.clone();
        let future = Operation::Advance { now_ms: 86_401_000 };
        apply(&mut session, &future).unwrap();
        tape.push(future);
        assert_eq!(session.portfolio_snapshot().daily_loss_pico, loss);
        assert_eq!(session.portfolio_snapshot().risk_day_utc, Some(0));
        assert!(same_state_without_clock(&before, &session).unwrap());
        let risk = Operation::RiskWindow { at_ms: 86_400_000 };
        apply(&mut session, &risk).unwrap();
        tape.push(risk);
        assert_eq!(session.portfolio_snapshot().daily_loss_pico, "0");
        assert!(!same_state_without_clock(&before, &session).unwrap());
        let mut recovered = new(&limits()).unwrap();
        replay(&mut recovered, &values(&tape).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(&recovered).unwrap(),
            serde_json::to_value(&session).unwrap()
        );
    }
    fn limits() -> Limits {
        Limits {
            initial_collateral_usd: "100".into(),
            max_imbalance_shares: "10".into(),
            lot_shares: "5".into(),
            half_width: "0.01".into(),
            max_feed_age_ms: 1000,
        }
    }
    fn id(c: char) -> String {
        format!("0x{}", c.to_string().repeat(64))
    }
    #[test]
    fn fee_evidence_replays_without_rewriting_legacy_finances() {
        let mut session = new(&limits()).unwrap();
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('a'),
                start_ms: Some(0),
                end_ms: Some(300_000),
            },
        )
        .unwrap();
        let old = serde_json::to_value(&session).unwrap();
        assert!(old.get("fee_policy").is_none());
        let finance = serde_json::to_value(session.portfolio_snapshot()).unwrap();
        let policy = mm2_live::fees::parse_policy(
            &serde_json::json!({"c":id('a'),
            "t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}}),
            &id('a'),
            "1",
            "2",
            1000,
        )
        .unwrap();
        let operation = Operation::FeeRules { policy };
        apply(&mut session, &operation).unwrap();
        assert_eq!(
            finance,
            serde_json::to_value(session.portfolio_snapshot()).unwrap()
        );
        let mut replay: MultiMarketPaper = serde_json::from_value(old).unwrap();
        let persisted = values(&[operation]).unwrap().remove(0);
        apply(&mut replay, &serde_json::from_value(persisted).unwrap()).unwrap();
        assert!(same_state_without_clock(&session, &replay).unwrap());
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        assert!(session.fee_policy.is_none());
    }
    fn executed_window() -> Vec<Operation> {
        vec![
            Operation::Market {
                condition_id: id('a'),
                start_ms: Some(0),
                end_ms: Some(300_000),
            },
            Operation::Book {
                now_ms: 0,
                outcome: "up".into(),
                bid: 400_000,
                ask: 500_000,
                ahead_micros: 0,
            },
            Operation::Book {
                now_ms: 0,
                outcome: "down".into(),
                bid: 400_000,
                ask: 500_000,
                ahead_micros: 0,
            },
            Operation::Quotes {
                now_ms: 0,
                quotes: vec![
                    Quote {
                        slot: "up".into(),
                        outcome: "up".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000,
                    },
                    Quote {
                        slot: "down".into(),
                        outcome: "down".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000,
                    },
                ],
            },
            Operation::Advance { now_ms: 50 },
            Operation::ObservedTrade {
                now_ms: 60,
                outcome: "up".into(),
                price_micros: 400_000,
                quantity_micros: 5_000_000,
                fingerprint: "a".repeat(64),
            },
            Operation::Advance { now_ms: 100 },
            Operation::CancelAll { now_ms: 100 },
            Operation::Advance { now_ms: 1000 },
        ]
    }
    #[test]
    fn loss_admission_counts_cancel_races_and_deduplicates_retained_desired_orders() {
        let mut session = new(&limits()).unwrap();
        apply(&mut session, &Operation::ExactAccounting {}).unwrap();
        for index in 0..4 {
            for mut operation in executed_window() {
                match &mut operation {
                    Operation::Market {
                        condition_id,
                        start_ms,
                        end_ms,
                    } => {
                        *condition_id = format!("0x{:064x}", index + 1);
                        *start_ms = Some(index * 300_000);
                        *end_ms = Some((index + 1) * 300_000);
                    }
                    Operation::Book { now_ms, .. }
                    | Operation::Quotes { now_ms, .. }
                    | Operation::Advance { now_ms }
                    | Operation::CancelAll { now_ms } => *now_ms += index * 2000,
                    Operation::ObservedTrade {
                        now_ms,
                        fingerprint,
                        ..
                    } => {
                        *now_ms += index * 2000;
                        *fingerprint = format!("{index:064x}");
                    }
                    _ => {}
                }
                apply(&mut session, &operation).unwrap();
            }
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('f'),
                start_ms: Some(1_200_000),
                end_ms: Some(1_500_000),
            },
        )
        .unwrap();
        let now = session.now_ms();
        session
            .set_book(
                now,
                Outcome::Up,
                Book {
                    bid: Some(300_000),
                    ask: Some(310_000),
                    bid_levels: vec![],
                },
            )
            .unwrap();
        let quote = DesiredQuote {
            slot: "up:lot".into(),
            outcome: Outcome::Up,
            price_micros: 300_000,
            quantity_micros: 5_000_000,
        };
        assert!(
            session
                .quote_risk_permitted(std::slice::from_ref(&quote))
                .unwrap()
        );
        session.quote(now, vec![quote.clone()]).unwrap();
        session.advance(now + 100).unwrap();
        assert!(
            session
                .quote_risk_permitted(std::slice::from_ref(&quote))
                .unwrap()
        );
        session.cancel_all(now + 100).unwrap();
        let replacement = DesiredQuote {
            price_micros: 310_000,
            ..quote
        };
        assert!(
            !session
                .quote_risk_permitted(std::slice::from_ref(&replacement))
                .unwrap()
        );
        session.advance(now + 1000).unwrap();
        assert!(session.quote_risk_permitted(&[replacement]).unwrap());
        assert_eq!(session.portfolio.policy().max_daily_loss_micros, 10_000_000);
        session.validate().unwrap();
    }
    #[test]
    fn rollover_preserves_unpaired_positions_and_finite_cash_and_replays_exactly() {
        let mut session = new(&limits()).unwrap();
        let mut operations = executed_window();
        operations.push(Operation::Market {
            condition_id: id('b'),
            start_ms: Some(300_000),
            end_ms: Some(600_000),
        });
        for operation in &operations {
            apply(&mut session, operation).unwrap();
        }
        session.validate().unwrap();
        let portfolio = session.portfolio_snapshot();
        assert_eq!(portfolio.cash_pico, "98000000000000");
        assert_eq!(session.snapshot().account.initial_cash_micros, 98_000_000);
        let previous = portfolio
            .positions
            .iter()
            .find(|p| p.market.id == id('a'))
            .unwrap();
        assert_eq!(previous.up.quantity_micros, 5_000_000);
        assert!(previous.resolution.is_none());
        let mut recovered = new(&limits()).unwrap();
        replay(&mut recovered, &values(&operations).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            serde_json::to_value(recovered).unwrap()
        );
    }
    #[test]
    fn public_final_outcomes_close_more_than_64_windows_and_replay_one_finite_pool() {
        let mut config = limits();
        config.initial_collateral_usd = "1000".into();
        let mut session = new(&config).unwrap();
        let mut tape = Vec::new();
        for index in 0..72u64 {
            let condition = format!("0x{:064x}", index + 1);
            let mut operations = executed_window();
            for operation in &mut operations {
                match operation {
                    Operation::Market {
                        condition_id,
                        start_ms,
                        end_ms,
                    } => {
                        *condition_id = condition.clone();
                        *start_ms = Some(index * 300_000);
                        *end_ms = Some((index + 1) * 300_000);
                    }
                    Operation::Book { now_ms, .. }
                    | Operation::Quotes { now_ms, .. }
                    | Operation::Advance { now_ms }
                    | Operation::ObservedTrade { now_ms, .. }
                    | Operation::CancelAll { now_ms } => *now_ms += index * 20_000,
                    _ => {}
                }
            }
            if index > 0 {
                let target = crate::paper_resolution::Target {
                    condition_id: format!("0x{index:064x}"),
                    start_ms: (index - 1) * 300_000,
                    end_ms: index * 300_000,
                };
                operations.push(Operation::PublicResolution {
                    evidence: crate::paper_resolution::tests::evidence(
                        target,
                        true,
                        index * 300_000,
                    ),
                    at_ms: index * 300_000,
                });
            }
            for operation in &operations {
                apply(&mut session, operation).unwrap();
            }
            assert_eq!(session.portfolio_snapshot().held_market_count, Some(1));
            assert_eq!(session.portfolio.policy().max_unresolved_markets, 64);
            session.validate().unwrap();
            tape.extend(operations);
        }
        assert_eq!(session.public_resolution_count, 71);
        let snapshot = session.portfolio_snapshot();
        assert_eq!(snapshot.initial_cash_pico, "1000000000000000");
        assert_eq!(snapshot.realized_pnl_pico, "213000000000000");
        let mut restored = new(&config).unwrap();
        replay(&mut restored, &values(&tape).unwrap()).unwrap();
        assert!(same_state_without_clock(&session, &restored).unwrap());
    }
    #[test]
    fn late_public_resolution_keeps_the_next_complete_set_merge_clock_monotonic() {
        let mut session = new(&limits()).unwrap();
        let mut tape = executed_window();
        let mut second = executed_window();
        for operation in &mut second {
            match operation {
                Operation::Market {
                    condition_id,
                    start_ms,
                    end_ms,
                } => {
                    *condition_id = id('b');
                    *start_ms = Some(300_000);
                    *end_ms = Some(600_000);
                }
                Operation::Book { now_ms, .. }
                | Operation::Quotes { now_ms, .. }
                | Operation::Advance { now_ms }
                | Operation::ObservedTrade { now_ms, .. }
                | Operation::CancelAll { now_ms } => *now_ms += 20_000,
                _ => {}
            }
        }
        second.insert(
            6,
            Operation::ObservedTrade {
                now_ms: 20_060,
                outcome: "down".into(),
                price_micros: 400_000,
                quantity_micros: 5_000_000,
                fingerprint: "b".repeat(64),
            },
        );
        tape.extend(second);
        tape.push(Operation::PublicResolution {
            evidence: crate::paper_resolution::tests::evidence(
                crate::paper_resolution::Target {
                    condition_id: id('a'),
                    start_ms: 0,
                    end_ms: 300_000,
                },
                true,
                600_100,
            ),
            at_ms: 600_150,
        });
        tape.push(Operation::Market {
            condition_id: id('c'),
            start_ms: Some(600_000),
            end_ms: Some(900_000),
        });
        for operation in &tape {
            apply(&mut session, operation).unwrap();
        }
        assert_eq!(session.portfolio_snapshot().cash_pico, "104000000000000");
        assert_eq!(
            session.portfolio_snapshot().realized_pnl_pico,
            "4000000000000"
        );
        let mut restored = new(&limits()).unwrap();
        replay(&mut restored, &values(&tape).unwrap()).unwrap();
        assert!(same_state_without_clock(&session, &restored).unwrap());
    }
    #[test]
    fn public_resolution_rejects_wrong_market_and_clock_without_credit_and_is_exactly_idempotent() {
        let mut session = new(&limits()).unwrap();
        for operation in executed_window() {
            apply(&mut session, &operation).unwrap();
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        let target = crate::paper_resolution::Target {
            condition_id: id('a'),
            start_ms: 0,
            end_ms: 300_000,
        };
        let evidence = crate::paper_resolution::tests::evidence(target, true, 300_000);
        let before = session.clone();
        let mut wrong = evidence.clone();
        wrong.target.condition_id = id('c');
        assert!(
            apply(
                &mut session,
                &Operation::PublicResolution {
                    evidence: wrong,
                    at_ms: 300_000
                }
            )
            .is_err()
        );
        assert!(
            apply(
                &mut session,
                &Operation::PublicResolution {
                    evidence: evidence.clone(),
                    at_ms: 299_999
                }
            )
            .is_err()
        );
        assert!(same_state_without_clock(&before, &session).unwrap());
        let operation = Operation::PublicResolution {
            evidence,
            at_ms: 300_000,
        };
        apply(&mut session, &operation).unwrap();
        let after = session.clone();
        apply(&mut session, &operation).unwrap();
        assert!(same_state_without_clock(&after, &session).unwrap());
        let mut reobserved = operation.clone();
        if let Operation::PublicResolution { evidence, at_ms } = &mut reobserved {
            evidence.observed_at_ms += 1000;
            *at_ms += 1000;
        }
        apply(&mut session, &reobserved).unwrap();
        assert!(same_state_without_clock(&after, &session).unwrap());
        assert_eq!(session.portfolio_snapshot().held_market_count, Some(0));
        assert_eq!(session.public_resolution_count, 1);
    }
    #[test]
    fn observed_trade_duplicate_is_idempotent_and_changed_market_bounds_reject() {
        let mut session = new(&limits()).unwrap();
        let operations = executed_window();
        for operation in &operations {
            apply(&mut session, operation).unwrap();
        }
        let previous = serde_json::to_value(&session).unwrap();
        apply(&mut session, &operations[5]).unwrap();
        assert_eq!(serde_json::to_value(&session).unwrap(), previous);
        assert!(
            apply(
                &mut session,
                &Operation::Market {
                    condition_id: id('a'),
                    start_ms: Some(1),
                    end_ms: Some(300_000)
                }
            )
            .is_err()
        );
    }
    #[test]
    fn explicit_demo_resolution_releases_only_known_synthetic_payout_and_is_idempotent() {
        let mut session = new(&limits()).unwrap();
        for op in executed_window() {
            apply(&mut session, &op).unwrap();
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        let resolution = Operation::DemoResolution {
            condition_id: id('a'),
            winning_outcome: "up".into(),
            at_ms: 300_000,
        };
        apply(&mut session, &resolution).unwrap();
        apply(&mut session, &resolution).unwrap();
        assert_eq!(session.portfolio_snapshot().cash_pico, "103000000000000");
        session.validate().unwrap();
        assert!(
            apply(
                &mut session,
                &Operation::DemoResolution {
                    condition_id: id('b'),
                    winning_outcome: "up".into(),
                    at_ms: 600_000
                }
            )
            .is_err()
        );
    }
    #[test]
    fn wrapper_validation_detects_corrupt_imported_execution_totals() {
        let mut session = new(&limits()).unwrap();
        for operation in executed_window() {
            apply(&mut session, &operation).unwrap();
        }
        let mut value = serde_json::to_value(session).unwrap();
        value["portfolio"]["markets"][id('a')]["execution_up_micros"] =
            serde_json::json!(6_000_000);
        let corrupt: MultiMarketPaper = serde_json::from_value(value).unwrap();
        assert!(corrupt.validate().is_err());
    }
    #[test]
    fn venue_grid_changes_require_terminal_orders_and_replay_sub_millitick_quotes() {
        let mut session = new(&limits()).unwrap();
        let mut tape = executed_window();
        for op in &tape {
            apply(&mut session, op).unwrap();
        }
        let fine = Operation::VenueRules {
            now_ms: 1000,
            tick_micros: 100,
            min_order_micros: 5_000_000,
        };
        apply(&mut session, &fine).unwrap();
        tape.push(fine);
        let quote = Operation::Quotes {
            now_ms: 1000,
            quotes: vec![Quote {
                slot: "fine".into(),
                outcome: "up".into(),
                price_micros: 400_100,
                quantity_micros: 5_000_000,
            }],
        };
        apply(&mut session, &quote).unwrap();
        tape.push(quote);
        let coarse = Operation::VenueRules {
            now_ms: 1000,
            tick_micros: 2500,
            min_order_micros: 5_000_000,
        };
        assert!(apply(&mut session, &coarse).is_err());
        for op in [
            Operation::CancelAll { now_ms: 1000 },
            Operation::Advance { now_ms: 2000 },
            Operation::VenueRules {
                now_ms: 2000,
                tick_micros: 2500,
                min_order_micros: 5_000_000,
            },
            Operation::Quotes {
                now_ms: 2000,
                quotes: vec![Quote {
                    slot: "coarse".into(),
                    outcome: "up".into(),
                    price_micros: 402_500,
                    quantity_micros: 5_000_000,
                }],
            },
        ] {
            apply(&mut session, &op).unwrap();
            tape.push(op);
        }
        let mut recovered = new(&limits()).unwrap();
        replay(&mut recovered, &values(&tape).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            serde_json::to_value(&recovered).unwrap()
        );
    }
    #[test]
    fn settlement_loss_halt_blocks_quotes_in_already_funded_next_window() {
        let mut session = new(&limits()).unwrap();
        session.portfolio = mm2_portfolio::Portfolio::new(
            100_000_000,
            mm2_portfolio::RiskPolicy {
                max_daily_loss_micros: 1_000_000,
                max_drawdown_micros: 1_000_000,
                ..Default::default()
            },
        )
        .unwrap();
        for op in executed_window() {
            apply(&mut session, &op).unwrap();
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        apply(
            &mut session,
            &Operation::DemoResolution {
                condition_id: id('a'),
                winning_outcome: "down".into(),
                at_ms: 300_000,
            },
        )
        .unwrap();
        assert!(session.portfolio_halt().is_some());
        let prior = serde_json::to_value(&session).unwrap();
        assert!(
            apply(
                &mut session,
                &Operation::Quotes {
                    now_ms: 1000,
                    quotes: vec![Quote {
                        slot: "forbidden".into(),
                        outcome: "up".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000
                    }]
                }
            )
            .is_err()
        );
        assert_eq!(prior, serde_json::to_value(&session).unwrap());
        apply(&mut session, &Operation::CancelAll { now_ms: 1000 }).unwrap();
        session.validate().unwrap();
    }
}
