//! One finite collateral ledger with market-keyed positions. No network or valuation oracle.
use mm2_core::{Outcome, SCALE};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

pub type Result<T> = std::result::Result<T, String>;
pub const MAX_MARKETS: usize = 4_096;
pub const MAX_RECEIPTS: usize = 8_192;
pub const MAX_RESERVATIONS: usize = 64;
const MAX_QUANTITY: u64 = 1_000_000_000 * SCALE;
const MAX_MONEY: u128 = MAX_QUANTITY as u128 * SCALE as u128 * MAX_RECEIPTS as u128 * 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskPolicy {
    pub max_total_exposure_micros: u64,
    pub max_market_exposure_micros: u64,
    pub max_daily_loss_micros: u64,
    pub max_drawdown_micros: u64,
    pub max_unresolved_markets: usize,
}

impl Default for RiskPolicy {
    fn default() -> Self {
        Self {
            max_total_exposure_micros: 100 * SCALE,
            max_market_exposure_micros: 100 * SCALE,
            max_daily_loss_micros: 10 * SCALE,
            max_drawdown_micros: 20 * SCALE,
            max_unresolved_markets: 64,
        }
    }
}

impl RiskPolicy {
    pub fn validate(&self) -> Result<()> {
        if [
            self.max_total_exposure_micros,
            self.max_market_exposure_micros,
            self.max_daily_loss_micros,
            self.max_drawdown_micros,
        ]
        .iter()
        .any(|v| *v == 0 || *v > MAX_QUANTITY)
            || self.max_market_exposure_micros > self.max_total_exposure_micros
            || !(1..=64).contains(&self.max_unresolved_markets)
        {
            return Err("invalid finite portfolio risk policy".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Market {
    pub id: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl Market {
    pub fn validate(&self) -> Result<()> {
        check_id(&self.id)?;
        if self.start_ms >= self.end_ms || self.end_ms - self.start_ms > 86_400_000 {
            return Err("invalid bounded market window".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub quantity_micros: u64,
    #[serde(with = "u128_decimal")]
    pub cost_pico: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketPosition {
    pub market: Market,
    pub up: Position,
    pub down: Position,
    pub resolution: Option<ResolutionReceipt>,
    /// Aggregate upstream execution totals, before local merge/redeem operations.
    #[serde(with = "u128_decimal")]
    pub execution_spent_pico: u128,
    pub execution_up_micros: u64,
    pub execution_down_micros: u64,
    #[serde(with = "u128_decimal")]
    pub execution_fee_pico: u128,
    /// Present only for versioned imports with exact, fee-inclusive side costs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_outcome_spent_pico: Option<[String; 2]>,
    pub totals_imported: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Explicit emulator outcome/receipt. Never accepted as production settlement.
    Synthetic,
    /// Authoritative adapter must verify final venue/on-chain state before admission.
    Authoritative,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub id: String,
    pub market_id: String,
    pub order_id: Option<String>,
    pub outcome: Outcome,
    pub price_micros: u64,
    pub quantity_micros: u64,
    #[serde(with = "u128_decimal")]
    pub fee_pico: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub order_id: String,
    pub market_id: String,
    pub outcome: Outcome,
    pub price_micros: u64,
    pub quantity_micros: u64,
    /// Upper bound of additional collateral fee; no expected rebate credit.
    #[serde(with = "u128_decimal")]
    pub max_fee_pico: u128,
}

impl Reservation {
    pub fn cost_pico(&self) -> u128 {
        u128::from(self.price_micros) * u128::from(self.quantity_micros) + self.max_fee_pico
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeReceipt {
    pub id: String,
    pub market_id: String,
    pub quantity_micros: u64,
    #[serde(with = "u128_decimal")]
    pub fee_pico: u128,
    pub at_ms: u64,
    pub provenance: Provenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionReceipt {
    pub id: String,
    pub market_id: String,
    pub winning_outcome: Outcome,
    #[serde(with = "u128_decimal")]
    pub fee_pico: u128,
    pub at_ms: u64,
    /// Finality is checked by caller; provisional outcomes must never be admitted.
    pub finality_confirmed: bool,
    pub provenance: Provenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Receipt {
    Fill(Fill),
    Merge(MergeReceipt),
    Resolution(ResolutionReceipt),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub initial_cash_pico: String,
    pub cash_pico: String,
    pub reserved_pico: String,
    pub available_pico: String,
    pub spent_pico: String,
    pub fees_pico: String,
    pub returned_pico: String,
    pub realized_pnl_pico: String,
    pub daily_loss_pico: String,
    #[serde(default)]
    pub risk_day_utc: Option<u64>,
    #[serde(default)]
    pub risk_clock_ms: u64,
    pub drawdown_pico: String,
    pub open_cost_pico: String,
    pub available_cash_micros: u64,
    pub positions: Vec<MarketPosition>,
    #[serde(default)]
    pub position_count: usize,
    #[serde(default)]
    pub positions_truncated: usize,
    /// Aggregate over all retained markets, including those omitted by telemetry.
    /// Older telemetry without this field cannot prove omitted inventory is zero.
    #[serde(default)]
    pub held_market_count: Option<usize>,
    pub reservations: Vec<Reservation>,
    pub halted: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Portfolio {
    #[serde(with = "u128_decimal")]
    initial_cash_pico: u128,
    #[serde(with = "u128_decimal")]
    cash_pico: u128,
    #[serde(with = "u128_decimal")]
    spent_pico: u128,
    #[serde(with = "u128_decimal")]
    fees_pico: u128,
    #[serde(with = "u128_decimal")]
    returned_pico: u128,
    #[serde(with = "i128_decimal")]
    realized_pnl_pico: i128,
    #[serde(with = "i128_decimal")]
    high_water_realized_pico: i128,
    day: Option<u64>,
    last_event_ms: u64,
    #[serde(with = "u128_decimal")]
    daily_loss_pico: u128,
    policy: RiskPolicy,
    markets: BTreeMap<String, MarketPosition>,
    reservations: BTreeMap<String, Reservation>,
    retired_orders: BTreeSet<String>,
    receipts: BTreeMap<String, Receipt>,
    halted: Option<String>,
}

fn gross_loss_bound(quantities: [u64; 2], costs: [u128; 2]) -> u128 {
    let matched = quantities[0].min(quantities[1]);
    let owned = usize::from(quantities[1] > quantities[0]);
    let residual = if quantities[owned] == 0 {
        0
    } else {
        let quantity = u128::from(quantities[owned]);
        let difference = u128::from(quantities[owned] - matched);
        (costs[owned] / quantity) * difference
            + ((costs[owned] % quantity) * difference).div_ceil(quantity)
    };
    // Matched profit cannot offset a later negative redemption under gross-loss policy.
    residual.max((costs[0] + costs[1]).saturating_sub(u128::from(matched) * u128::from(SCALE)))
}

fn check_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_:./".contains(&b))
    {
        return Err("invalid bounded portfolio identity".into());
    }
    Ok(())
}

impl Portfolio {
    pub fn new(initial_cash_micros: u64, policy: RiskPolicy) -> Result<Self> {
        policy.validate()?;
        if initial_cash_micros == 0 || initial_cash_micros > MAX_QUANTITY {
            return Err("invalid finite initial collateral".into());
        }
        let cash = u128::from(initial_cash_micros) * u128::from(SCALE);
        Ok(Self {
            initial_cash_pico: cash,
            cash_pico: cash,
            spent_pico: 0,
            fees_pico: 0,
            returned_pico: 0,
            realized_pnl_pico: 0,
            high_water_realized_pico: cash as i128,
            day: None,
            last_event_ms: 0,
            daily_loss_pico: 0,
            policy,
            markets: BTreeMap::new(),
            reservations: BTreeMap::new(),
            retired_orders: BTreeSet::new(),
            receipts: BTreeMap::new(),
            halted: None,
        })
    }

    pub fn register_market(&mut self, market: Market) -> Result<bool> {
        market.validate()?;
        if let Some(prior) = self.markets.get(&market.id) {
            return if prior.market == market {
                Ok(false)
            } else {
                Err("market identity collision".into())
            };
        }
        if self.markets.len() >= MAX_MARKETS {
            return Err("portfolio market history capacity exhausted".into());
        }
        if self
            .markets
            .values()
            .filter(|m| {
                m.resolution.is_none() && (m.up.quantity_micros > 0 || m.down.quantity_micros > 0)
            })
            .count()
            >= self.policy.max_unresolved_markets
        {
            return Err("unresolved market capacity exhausted".into());
        }
        self.markets.insert(
            market.id.clone(),
            MarketPosition {
                market,
                up: Position::default(),
                down: Position::default(),
                resolution: None,
                execution_spent_pico: 0,
                execution_up_micros: 0,
                execution_down_micros: 0,
                execution_fee_pico: 0,
                execution_outcome_spent_pico: None,
                totals_imported: false,
            },
        );
        Ok(true)
    }

    pub fn market(&self, id: &str) -> Option<&MarketPosition> {
        self.markets.get(id)
    }
    pub fn policy(&self) -> &RiskPolicy {
        &self.policy
    }

    pub fn risk_window_due(&self, at_ms: u64) -> bool {
        self.day != Some(at_ms / 86_400_000)
    }

    pub fn risk_clock_ms(&self) -> u64 {
        self.last_event_ms
    }

    /// Caller-supplied UTC, persisted by the execution actor before new risk.
    /// Advancing a day never releases reservations, old inventory or a latched halt.
    pub fn advance_risk_window(&mut self, at_ms: u64) -> Result<()> {
        self.advance_day(at_ms)
    }

    /// Admission before execution, including every retained unresolved market.
    /// Positive realized returns never replenish the configured gross loss budget.
    pub fn permits_execution_risk(
        &self,
        market_id: &str,
        possible_fills: [u64; 2],
        max_fill_price_micros: u64,
    ) -> Result<bool> {
        if max_fill_price_micros >= SCALE
            || possible_fills.iter().any(|q| *q > MAX_QUANTITY)
            || (max_fill_price_micros == 0 && possible_fills != [0, 0])
        {
            return Err("invalid prospective execution risk".into());
        }
        let current = self.market(market_id).ok_or("unknown execution market")?;
        let remaining = (u128::from(self.policy.max_daily_loss_micros) * u128::from(SCALE))
            .saturating_sub(self.daily_loss_pico)
            .min(
                (u128::from(self.policy.max_drawdown_micros) * u128::from(SCALE))
                    .saturating_sub(self.drawdown_pico()),
            );
        let retained: u128 = self
            .markets
            .values()
            .filter(|p| p.market.id != market_id)
            .map(|p| {
                gross_loss_bound(
                    [p.up.quantity_micros, p.down.quantity_micros],
                    [p.up.cost_pico, p.down.cost_pico],
                )
            })
            .sum();
        let mut worst = 0;
        for mask in 0..4 {
            let mut quantities = [current.up.quantity_micros, current.down.quantity_micros];
            let mut costs = [current.up.cost_pico, current.down.cost_pico];
            for side in 0..2 {
                if mask & (1 << side) != 0 {
                    quantities[side] = quantities[side]
                        .checked_add(possible_fills[side])
                        .filter(|q| *q <= MAX_QUANTITY)
                        .ok_or("prospective inventory bound")?;
                    // The aggregate adapter may allocate a batch's cost between sides.
                    // Its allocated unit cost cannot exceed the highest possible fill price.
                    costs[side] +=
                        u128::from(possible_fills[side]) * u128::from(max_fill_price_micros);
                }
            }
            worst = worst.max(gross_loss_bound(quantities, costs));
        }
        // Equality trips the existing realized guard; admit strictly below it.
        Ok(self.halted.is_none() && retained + worst < remaining)
    }
    pub fn halt(&mut self, reason: impl Into<String>) {
        self.halted = Some(reason.into());
    }
    pub fn reserved_pico(&self) -> u128 {
        self.reservations.values().map(Reservation::cost_pico).sum()
    }
    pub fn available_cash_pico(&self) -> u128 {
        self.cash_pico.saturating_sub(self.reserved_pico())
    }
    pub fn available_cash_micros(&self) -> u64 {
        u64::try_from(self.available_cash_pico() / u128::from(SCALE)).unwrap_or(u64::MAX)
    }
    pub fn capital_for_new_market(&self) -> Result<u64> {
        self.can_trade()?;
        if self
            .markets
            .values()
            .filter(|m| {
                m.resolution.is_none() && (m.up.quantity_micros > 0 || m.down.quantity_micros > 0)
            })
            .count()
            >= self.policy.max_unresolved_markets
        {
            return Err("unresolved market capacity exhausted".into());
        }
        let total_limit = u128::from(self.policy.max_total_exposure_micros) * u128::from(SCALE);
        let remaining_exposure =
            total_limit.saturating_sub(self.open_cost_pico() + self.reserved_pico());
        let market_limit = u128::from(self.policy.max_market_exposure_micros) * u128::from(SCALE);
        Ok((self
            .available_cash_pico()
            .min(remaining_exposure)
            .min(market_limit)
            / u128::from(SCALE)) as u64)
    }

    fn can_trade(&self) -> Result<()> {
        if let Some(reason) = &self.halted {
            return Err(format!("portfolio halted: {reason}"));
        }
        if self.daily_loss_pico >= u128::from(self.policy.max_daily_loss_micros) * u128::from(SCALE)
            || self.drawdown_pico()
                >= u128::from(self.policy.max_drawdown_micros) * u128::from(SCALE)
        {
            return Err("portfolio loss limit reached".into());
        }
        Ok(())
    }

    fn open_cost_pico(&self) -> u128 {
        self.markets
            .values()
            .map(|m| m.up.cost_pico + m.down.cost_pico)
            .sum()
    }

    fn exposure_admission(&self, market_id: &str, additional: u128) -> Result<()> {
        self.can_trade()?;
        let market = self.markets.get(market_id).ok_or("unknown market")?;
        if market.resolution.is_some() {
            return Err("market already resolved".into());
        }
        if market.up.quantity_micros == 0
            && market.down.quantity_micros == 0
            && self
                .markets
                .values()
                .filter(|m| {
                    m.resolution.is_none()
                        && (m.up.quantity_micros > 0 || m.down.quantity_micros > 0)
                })
                .count()
                >= self.policy.max_unresolved_markets
        {
            return Err("unresolved market capacity exhausted".into());
        }
        let reserved_market: u128 = self
            .reservations
            .values()
            .filter(|r| r.market_id == market_id)
            .map(Reservation::cost_pico)
            .sum();
        if self.open_cost_pico() + self.reserved_pico() + additional
            > u128::from(self.policy.max_total_exposure_micros) * u128::from(SCALE)
            || market.up.cost_pico + market.down.cost_pico + reserved_market + additional
                > u128::from(self.policy.max_market_exposure_micros) * u128::from(SCALE)
            || additional > self.available_cash_pico()
        {
            return Err("portfolio collateral or exposure limit".into());
        }
        Ok(())
    }

    pub fn reserve(&mut self, reservation: Reservation) -> Result<bool> {
        check_id(&reservation.order_id)?;
        check_id(&reservation.market_id)?;
        if self.retired_orders.contains(&reservation.order_id) {
            return Err("terminal order identity cannot be reused".into());
        }
        if reservation.price_micros == 0
            || reservation.price_micros >= SCALE
            || reservation.quantity_micros == 0
            || reservation.quantity_micros > MAX_QUANTITY
            || reservation.max_fee_pico > self.initial_cash_pico
        {
            return Err("invalid finite portfolio reservation".into());
        }
        if let Some(prior) = self.reservations.get(&reservation.order_id) {
            return if prior == &reservation {
                Ok(false)
            } else {
                Err("reservation identity collision".into())
            };
        }
        if self.reservations.len() >= MAX_RESERVATIONS {
            return Err("reservation capacity exhausted".into());
        }
        self.exposure_admission(&reservation.market_id, reservation.cost_pico())?;
        self.reservations
            .insert(reservation.order_id.clone(), reservation);
        Ok(true)
    }

    /// Call only on authoritative terminal reconciliation/rejection, never cancel dispatch/timeout.
    pub fn release_reservation(&mut self, order_id: &str) -> Result<bool> {
        check_id(order_id)?;
        if !self.reservations.contains_key(order_id) {
            return Ok(false);
        }
        if self.retired_orders.len() >= MAX_RECEIPTS {
            return Err("terminal order history capacity exhausted".into());
        }
        self.retired_orders.insert(order_id.into());
        self.reservations.remove(order_id);
        Ok(true)
    }

    fn receipt_seen(&self, id: &str, receipt: &Receipt) -> Result<bool> {
        check_id(id)?;
        if let Some(prior) = self.receipts.get(id) {
            return if prior == receipt {
                Ok(true)
            } else {
                Err("receipt identity collision".into())
            };
        }
        if self.receipts.len() >= MAX_RECEIPTS {
            return Err("receipt capacity exhausted; archive checkpoint required".into());
        }
        Ok(false)
    }

    /// Actual reported fill is accounted even if it makes risk fail; no fresh risk admission here.
    /// A reservation is reduced only by the exact confirmed quantity and fee consumed.
    pub fn apply_fill(&mut self, fill: Fill, at_ms: u64) -> Result<bool> {
        let receipt = Receipt::Fill(fill.clone());
        if self.receipt_seen(&fill.id, &receipt)? {
            return Ok(false);
        }
        if fill.price_micros == 0
            || fill.price_micros >= SCALE
            || fill.quantity_micros == 0
            || fill.quantity_micros > MAX_QUANTITY
            || fill.fee_pico > self.initial_cash_pico
        {
            return Err("invalid finite portfolio fill".into());
        }
        let market = self
            .markets
            .get(&fill.market_id)
            .ok_or("unknown fill market")?;
        if market.resolution.is_some() || market.totals_imported {
            return Err(
                "cannot mix fill receipts with resolved/aggregate market accounting".into(),
            );
        }
        let cost = u128::from(fill.price_micros) * u128::from(fill.quantity_micros) + fill.fee_pico;
        if cost > self.cash_pico {
            return Err("reported fill exceeds known finite collateral".into());
        }
        let reservation = if let Some(order_id) = &fill.order_id {
            let reservation = self
                .reservations
                .get(order_id)
                .ok_or("fill has no retained reservation")?;
            if reservation.market_id != fill.market_id
                || reservation.outcome != fill.outcome
                || fill.price_micros > reservation.price_micros
                || fill.quantity_micros > reservation.quantity_micros
                || fill.fee_pico > reservation.max_fee_pico
            {
                return Err("fill contradicts retained reservation".into());
            }
            Some(reservation.clone())
        } else {
            None
        };
        if reservation.is_none() && cost > self.available_cash_pico() {
            return Err("unreserved fill would consume another order's commitment".into());
        }
        let prior = if fill.outcome == Outcome::Up {
            &market.up
        } else {
            &market.down
        };
        let next_quantity = prior
            .quantity_micros
            .checked_add(fill.quantity_micros)
            .filter(|q| *q <= MAX_QUANTITY)
            .ok_or("inventory bound exceeded")?;
        self.advance_day(at_ms)?;
        let market = self
            .markets
            .get_mut(&fill.market_id)
            .expect("checked market");
        let position = if fill.outcome == Outcome::Up {
            &mut market.up
        } else {
            &mut market.down
        };
        position.quantity_micros = next_quantity;
        position.cost_pico += cost;
        self.cash_pico -= cost;
        self.spent_pico += cost;
        self.fees_pico += fill.fee_pico;
        if let Some(mut reservation) = reservation {
            reservation.quantity_micros -= fill.quantity_micros;
            reservation.max_fee_pico -= fill.fee_pico;
            // Even zero remaining quantity may retain an unapplied fee bound until terminal.
            self.reservations
                .insert(reservation.order_id.clone(), reservation);
        }
        self.receipts.insert(fill.id.clone(), receipt);
        self.check_cash_invariant()?;
        Ok(true)
    }

    /// Idempotent adapter for cumulative Core snapshots. `spent_pico` INCLUDES `fee_pico`.
    /// Never call on an execution core whose collateral/inventory was reset within this market.
    /// Fills split fee-inclusive cost between outcomes by the cumulative added share quantities.
    /// This aggregate adapter has no true per-side cost evidence; realized PnL is portfolio-exact,
    /// side allocation is explicitly deterministic proportional accounting.
    pub fn record_execution_totals(
        &mut self,
        market_id: &str,
        spent_pico: u128,
        up_micros: u64,
        down_micros: u64,
        fee_pico: u128,
    ) -> Result<bool> {
        let market = self
            .markets
            .get(market_id)
            .ok_or("unknown execution market")?;
        if market.execution_outcome_spent_pico.is_some() {
            return Err("cannot mix aggregate and exact outcome accounting".into());
        }
        if market.resolution.is_some()
            || up_micros > MAX_QUANTITY
            || down_micros > MAX_QUANTITY
            || fee_pico > spent_pico
            || spent_pico < market.execution_spent_pico
            || fee_pico < market.execution_fee_pico
            || up_micros < market.execution_up_micros
            || down_micros < market.execution_down_micros
            || (!market.totals_imported
                && (market.up.quantity_micros != 0 || market.down.quantity_micros != 0))
        {
            return Err("aggregate execution contradiction or mixed accounting".into());
        }
        let delta_cost = spent_pico - market.execution_spent_pico;
        let delta_fee = fee_pico - market.execution_fee_pico;
        let delta_up = up_micros - market.execution_up_micros;
        let delta_down = down_micros - market.execution_down_micros;
        let total_delta = u128::from(delta_up) + u128::from(delta_down);
        if delta_cost == 0 && total_delta == 0 && delta_fee == 0 {
            return Ok(false);
        }
        if total_delta == 0
            || delta_cost <= delta_fee
            || delta_cost > self.available_cash_pico()
            || delta_cost > u128::from(SCALE) * total_delta + delta_fee
            || market
                .up
                .quantity_micros
                .checked_add(delta_up)
                .is_none_or(|q| q > MAX_QUANTITY)
            || market
                .down
                .quantity_micros
                .checked_add(delta_down)
                .is_none_or(|q| q > MAX_QUANTITY)
            || self.reservations.values().any(|r| r.market_id == market_id)
        {
            return Err("invalid cumulative cost/inventory or mixed reservations".into());
        }
        let up_cost = delta_cost * u128::from(delta_up) / total_delta;
        let market = self.markets.get_mut(market_id).expect("checked market");
        market.up.quantity_micros += delta_up;
        market.down.quantity_micros += delta_down;
        market.up.cost_pico += up_cost;
        market.down.cost_pico += delta_cost - up_cost;
        market.execution_spent_pico = spent_pico;
        market.execution_up_micros = up_micros;
        market.execution_down_micros = down_micros;
        market.execution_fee_pico = fee_pico;
        market.totals_imported = true;
        self.cash_pico -= delta_cost;
        self.spent_pico += delta_cost;
        self.fees_pico += delta_fee;
        self.check_cash_invariant()?;
        Ok(true)
    }

    /// Exact cumulative maker execution costs. Never reinterpret an aggregate history.
    /// The current paper execution core has no maker fees; nonzero fee history is rejected.
    pub fn record_execution_outcome_totals(
        &mut self,
        market_id: &str,
        costs: [u128; 2],
        quantities: [u64; 2],
    ) -> Result<bool> {
        let prior = self.market(market_id).ok_or("unknown execution market")?;
        let previous = match &prior.execution_outcome_spent_pico {
            Some(value) => [
                value[0]
                    .parse::<u128>()
                    .map_err(|_| "invalid exact outcome cost")?,
                value[1]
                    .parse::<u128>()
                    .map_err(|_| "invalid exact outcome cost")?,
            ],
            None if prior.execution_spent_pico == 0 => [0, 0],
            None => return Err("legacy outcome cost uncertainty blocks exact import".into()),
        };
        if prior.execution_fee_pico != 0 {
            return Err("exact maker accounting cannot import fee uncertainty".into());
        }
        for side in 0..2 {
            let old_quantity = [prior.execution_up_micros, prior.execution_down_micros][side];
            if quantities[side] < old_quantity || costs[side] < previous[side] {
                return Err("exact execution totals regressed".into());
            }
            let quantity = quantities[side] - old_quantity;
            let cost = costs[side] - previous[side];
            if (quantity == 0) != (cost == 0) || cost > u128::from(quantity) * u128::from(SCALE) {
                return Err("exact outcome cost contradicts quantity".into());
            }
        }
        let total = costs[0]
            .checked_add(costs[1])
            .ok_or("exact cost overflow")?;
        let old_basis = [prior.up.cost_pico, prior.down.cost_pico];
        let was_exact = prior.execution_outcome_spent_pico.is_some();
        // Reuse the aggregate import's capital, quantity, reservation and identity
        // validation on a private candidate, then replace only its side allocation.
        let mut candidate = self.clone();
        candidate
            .markets
            .get_mut(market_id)
            .expect("checked market")
            .execution_outcome_spent_pico = None;
        let changed =
            candidate.record_execution_totals(market_id, total, quantities[0], quantities[1], 0)?;
        let position = candidate
            .markets
            .get_mut(market_id)
            .expect("checked market");
        position.up.cost_pico = old_basis[0] + costs[0] - previous[0];
        position.down.cost_pico = old_basis[1] + costs[1] - previous[1];
        position.execution_outcome_spent_pico = Some(costs.map(|cost| cost.to_string()));
        candidate.validate()?;
        *self = candidate;
        Ok(changed || !was_exact)
    }

    pub fn merge_complete_sets(
        &mut self,
        receipt: MergeReceipt,
        allow_synthetic: bool,
    ) -> Result<bool> {
        let record = Receipt::Merge(receipt.clone());
        if self.receipt_seen(&receipt.id, &record)? {
            return Ok(false);
        }
        if receipt.provenance == Provenance::Synthetic && !allow_synthetic {
            return Err("synthetic receipt forbidden in real portfolio".into());
        }
        let market = self
            .markets
            .get(&receipt.market_id)
            .ok_or("unknown merge market")?;
        if receipt.quantity_micros == 0
            || receipt.quantity_micros > market.up.quantity_micros
            || receipt.quantity_micros > market.down.quantity_micros
            || market.resolution.is_some()
            || self
                .reservations
                .values()
                .any(|r| r.market_id == receipt.market_id)
        {
            return Err("merge exceeds complete sets or unsettled execution".into());
        }
        let payout = u128::from(receipt.quantity_micros) * u128::from(SCALE);
        if receipt.fee_pico > payout + self.cash_pico {
            return Err("merge fee exceeds collateral".into());
        }
        let removed_up = proportional_cost(&market.up, receipt.quantity_micros);
        let removed_down = proportional_cost(&market.down, receipt.quantity_micros);
        self.advance_day(receipt.at_ms)?;
        let market = self
            .markets
            .get_mut(&receipt.market_id)
            .expect("checked market");
        market.up.quantity_micros -= receipt.quantity_micros;
        market.down.quantity_micros -= receipt.quantity_micros;
        market.up.cost_pico -= removed_up;
        market.down.cost_pico -= removed_down;
        self.account_return(payout, receipt.fee_pico, removed_up + removed_down);
        self.receipts.insert(receipt.id.clone(), record);
        self.check_cash_invariant()?;
        Ok(true)
    }

    pub fn redeem(&mut self, receipt: ResolutionReceipt, allow_synthetic: bool) -> Result<bool> {
        let record = Receipt::Resolution(receipt.clone());
        if self.receipt_seen(&receipt.id, &record)? {
            return Ok(false);
        }
        if !receipt.finality_confirmed
            || (receipt.provenance == Provenance::Synthetic && !allow_synthetic)
        {
            return Err("unconfirmed or synthetic real settlement".into());
        }
        let market = self
            .markets
            .get(&receipt.market_id)
            .ok_or("unknown redemption market")?;
        if receipt.at_ms < market.market.end_ms
            || market.resolution.is_some()
            || self
                .reservations
                .values()
                .any(|r| r.market_id == receipt.market_id)
        {
            return Err("redemption before close, duplicate outcome or unsettled execution".into());
        }
        let winning = if receipt.winning_outcome == Outcome::Up {
            &market.up
        } else {
            &market.down
        };
        let payout = u128::from(winning.quantity_micros) * u128::from(SCALE);
        if receipt.fee_pico > payout + self.cash_pico {
            return Err("redemption fee exceeds collateral".into());
        }
        let cost = market.up.cost_pico + market.down.cost_pico;
        self.advance_day(receipt.at_ms)?;
        let market = self
            .markets
            .get_mut(&receipt.market_id)
            .expect("checked market");
        market.up = Position::default();
        market.down = Position::default();
        market.resolution = Some(receipt.clone());
        self.account_return(payout, receipt.fee_pico, cost);
        self.receipts.insert(receipt.id.clone(), record);
        self.check_cash_invariant()?;
        Ok(true)
    }

    fn advance_day(&mut self, at_ms: u64) -> Result<()> {
        if at_ms < self.last_event_ms {
            return Err("portfolio event clock moved backwards".into());
        }
        let day = at_ms / 86_400_000;
        if self.day != Some(day) {
            self.day = Some(day);
            self.daily_loss_pico = 0;
        }
        self.last_event_ms = at_ms;
        Ok(())
    }

    fn account_return(&mut self, payout: u128, fee: u128, removed_cost: u128) {
        self.cash_pico = self.cash_pico + payout - fee;
        self.returned_pico += payout;
        self.spent_pico += fee;
        self.fees_pico += fee;
        let pnl = payout as i128 - fee as i128 - removed_cost as i128;
        self.realized_pnl_pico += pnl;
        if pnl < 0 {
            self.daily_loss_pico += pnl.unsigned_abs();
        }
        let equity = self.initial_cash_pico as i128 + self.realized_pnl_pico;
        self.high_water_realized_pico = self.high_water_realized_pico.max(equity);
        if self.can_trade().is_err() {
            self.halted = Some("realized loss limit reached".into());
        }
    }

    fn drawdown_pico(&self) -> u128 {
        (self.high_water_realized_pico - self.initial_cash_pico as i128 - self.realized_pnl_pico)
            .max(0) as u128
    }

    fn check_cash_invariant(&self) -> Result<()> {
        if self.initial_cash_pico + self.returned_pico != self.cash_pico + self.spent_pico {
            return Err("portfolio exact cash invariant failed".into());
        }
        Ok(())
    }

    /// Validate deserialized checkpoints before accepting them. Unknown capital is never replenished.
    pub fn validate(&self) -> Result<()> {
        self.policy.validate()?;
        if [
            self.cash_pico,
            self.spent_pico,
            self.fees_pico,
            self.returned_pico,
            self.daily_loss_pico,
        ]
        .iter()
        .any(|x| *x > MAX_MONEY)
            || self.realized_pnl_pico.unsigned_abs() > MAX_MONEY
            || self.high_water_realized_pico.unsigned_abs() > MAX_MONEY
            || self.markets.values().any(|m| {
                [
                    m.up.cost_pico,
                    m.down.cost_pico,
                    m.execution_spent_pico,
                    m.execution_fee_pico,
                ]
                .iter()
                .any(|x| *x > MAX_MONEY)
            })
            || self.reservations.values().any(|r| {
                r.max_fee_pico > MAX_MONEY
                    || r.price_micros >= SCALE
                    || r.quantity_micros > MAX_QUANTITY
            })
        {
            return Err("portfolio monetary bound exceeded".into());
        }
        if self.initial_cash_pico == 0
            || self.initial_cash_pico > u128::from(MAX_QUANTITY) * u128::from(SCALE)
            || self.cash_pico > self.initial_cash_pico + self.returned_pico
            || self.initial_cash_pico + self.returned_pico != self.cash_pico + self.spent_pico
            || self.fees_pico > self.spent_pico
            || self.reserved_pico() > self.cash_pico
            || self.markets.len() > MAX_MARKETS
            || self.receipts.len() > MAX_RECEIPTS
            || self.reservations.len() > MAX_RESERVATIONS
            || self.retired_orders.len() > MAX_RECEIPTS
            || self.realized_pnl_pico
                != self.returned_pico as i128 - self.spent_pico as i128
                    + self.open_cost_pico() as i128
            || self.high_water_realized_pico
                < self.initial_cash_pico as i128 + self.realized_pnl_pico
        {
            return Err("portfolio exact accounting or capacity invariant failed".into());
        }
        for (id, market) in &self.markets {
            market.market.validate()?;
            if let Some(costs) = &market.execution_outcome_spent_pico {
                let up = costs[0]
                    .parse::<u128>()
                    .map_err(|_| "invalid exact outcome cost")?;
                let down = costs[1]
                    .parse::<u128>()
                    .map_err(|_| "invalid exact outcome cost")?;
                if up.checked_add(down) != Some(market.execution_spent_pico)
                    || market.execution_fee_pico != 0
                {
                    return Err("exact outcome cost invariant failed".into());
                }
            }
            if id != &market.market.id
                || market.up.quantity_micros > MAX_QUANTITY
                || market.down.quantity_micros > MAX_QUANTITY
                || (market.up.quantity_micros == 0 && market.up.cost_pico != 0)
                || (market.down.quantity_micros == 0 && market.down.cost_pico != 0)
                || market.execution_fee_pico > market.execution_spent_pico
                || market.resolution.is_some()
                    && (market.up.quantity_micros != 0 || market.down.quantity_micros != 0)
            {
                return Err("portfolio market position invariant failed".into());
            }
        }
        for (id, reservation) in &self.reservations {
            check_id(id)?;
            if id != &reservation.order_id
                || !self.markets.contains_key(&reservation.market_id)
                || reservation.price_micros == 0
                || reservation.price_micros >= SCALE
                || reservation.quantity_micros > MAX_QUANTITY
                || reservation.max_fee_pico > self.initial_cash_pico
                || self.retired_orders.contains(id)
            {
                return Err("portfolio reservation invariant failed".into());
            }
        }
        for id in &self.retired_orders {
            check_id(id)?;
        }
        for (id, receipt) in &self.receipts {
            check_id(id)?;
            let (receipt_id, market_id) = match receipt {
                Receipt::Fill(r) => (&r.id, &r.market_id),
                Receipt::Merge(r) => (&r.id, &r.market_id),
                Receipt::Resolution(r) => (&r.id, &r.market_id),
            };
            if id != receipt_id || !self.markets.contains_key(market_id) {
                return Err("portfolio receipt identity invariant failed".into());
            }
        }
        Ok(())
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_with_positions(self.markets.values().cloned().collect())
    }
    pub fn halt_reason(&self) -> Option<&str> {
        self.halted.as_deref()
    }

    /// Bounded telemetry copy: retains latest market windows without cloning history.
    /// Exact global amounts still include all positions. Recovery uses the full Portfolio.
    pub fn compact_snapshot(&self, max_positions: usize) -> Snapshot {
        let capacity = max_positions.min(MAX_MARKETS);
        let mut newest = BinaryHeap::with_capacity(capacity);
        if capacity > 0 {
            for (id, market) in &self.markets {
                let key = Reverse((market.market.start_ms, id.as_str()));
                if newest.len() < capacity {
                    newest.push(key);
                } else if newest.peek().is_some_and(|oldest| key < *oldest) {
                    newest.pop();
                    newest.push(key);
                }
            }
        }
        let positions = newest
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse((_, id))| self.markets[id].clone())
            .collect();
        self.snapshot_with_positions(positions)
    }

    fn snapshot_with_positions(&self, positions: Vec<MarketPosition>) -> Snapshot {
        Snapshot {
            initial_cash_pico: self.initial_cash_pico.to_string(),
            cash_pico: self.cash_pico.to_string(),
            reserved_pico: self.reserved_pico().to_string(),
            available_pico: self.available_cash_pico().to_string(),
            spent_pico: self.spent_pico.to_string(),
            fees_pico: self.fees_pico.to_string(),
            returned_pico: self.returned_pico.to_string(),
            realized_pnl_pico: self.realized_pnl_pico.to_string(),
            daily_loss_pico: self.daily_loss_pico.to_string(),
            risk_day_utc: self.day,
            risk_clock_ms: self.last_event_ms,
            drawdown_pico: self.drawdown_pico().to_string(),
            open_cost_pico: self.open_cost_pico().to_string(),
            available_cash_micros: self.available_cash_micros(),
            position_count: self.markets.len(),
            positions_truncated: self.markets.len().saturating_sub(positions.len()),
            held_market_count: Some(
                self.markets
                    .values()
                    .filter(|position| {
                        position.up.quantity_micros > 0 || position.down.quantity_micros > 0
                    })
                    .count(),
            ),
            positions,
            reservations: self.reservations.values().cloned().collect(),
            halted: self.halted.clone(),
        }
    }
}

fn proportional_cost(position: &Position, removed_micros: u64) -> u128 {
    if removed_micros == position.quantity_micros {
        position.cost_pico
    } else {
        position.cost_pico * u128::from(removed_micros) / u128::from(position.quantity_micros)
    }
}

mod u128_decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.is_empty() || text.len() > 39 || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(serde::de::Error::custom("invalid exact unsigned decimal"));
        }
        text.parse().map_err(serde::de::Error::custom)
    }
}

mod i128_decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let text = String::deserialize(deserializer)?;
        let digits = text.strip_prefix('-').unwrap_or(&text);
        if digits.is_empty() || text.len() > 40 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(serde::de::Error::custom("invalid exact signed decimal"));
        }
        text.parse().map_err(serde::de::Error::custom)
    }
}
