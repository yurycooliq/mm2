//! Bounded, fixed-point BUY-only quoting. Signals and clocks are inputs, never I/O.
use mm2_core::{DesiredQuote, Outcome, SCALE};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, String>;
pub const MAX_QUOTES: usize = 6;
const MAX_QUANTITY: u64 = 1_000_000_000 * SCALE;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Widening {
    #[default]
    None,
    Vol {
        k_ppm: u64,
    },
    VolTime {
        k_ppm: u64,
        tau_ref_ms: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Rules {
    pub lot_full_micros: u64,
    /// Explicit research UTC schedule; default deployment uses a fixed configured lot.
    pub schedule_lot: bool,
    pub half_width_micros: u64,
    pub shrink_ppm: u64,
    pub stub_price_micros: Option<u64>,
    pub min_price_micros: u64,
    pub max_book_spread_micros: u64,
    /// Required gross edge of a complete pair, before fees. No rebate credit.
    pub max_pair_edge_micros: u64,
    pub max_feed_age_ms: u64,
    /// Cancel, including stubs, before the close. Never leave frozen orders.
    pub close_guard_ms: u64,
    pub max_imbalance_micros: u64,
    pub widening: Widening,
    pub fair_weight_ppm: u64,
    pub imbalance_k_micros: i64,
    pub flow_k_micros: i64,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            lot_full_micros: 10 * SCALE,
            schedule_lot: false,
            half_width_micros: 45_000,
            shrink_ppm: 977_000,
            stub_price_micros: None,
            min_price_micros: 20_000,
            max_book_spread_micros: 50_000,
            max_pair_edge_micros: 0,
            max_feed_age_ms: 1_000,
            close_guard_ms: 1_000,
            max_imbalance_micros: 20 * SCALE,
            widening: Widening::None,
            fair_weight_ppm: 0,
            imbalance_k_micros: 0,
            flow_k_micros: 0,
        }
    }
}

impl Rules {
    /// Research reconstruction; not the safe small-capital default or proven economics.
    pub fn reconstructed_btc5m() -> Self {
        Self {
            lot_full_micros: 502 * SCALE,
            schedule_lot: true,
            stub_price_micros: Some(10_000),
            widening: Widening::VolTime {
                k_ppm: 2 * SCALE,
                tau_ref_ms: 150_000,
            },
            fair_weight_ppm: 120_000,
            imbalance_k_micros: 22_000,
            flow_k_micros: -23_000,
            max_imbalance_micros: 1_000 * SCALE,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        let bad_widening = match self.widening {
            Widening::None => false,
            Widening::Vol { k_ppm } => k_ppm > 100 * SCALE,
            Widening::VolTime { k_ppm, tau_ref_ms } => {
                k_ppm > 100 * SCALE || tau_ref_ms == 0 || tau_ref_ms > 86_400_000
            }
        };
        if self.lot_full_micros < 5 * SCALE
            || self.lot_full_micros > MAX_QUANTITY
            || self.half_width_micros >= SCALE / 2
            || self.shrink_ppm > SCALE
            || self.min_price_micros == 0
            || self.min_price_micros >= SCALE
            || self
                .stub_price_micros
                .is_some_and(|x| x == 0 || x >= self.min_price_micros)
            || self.max_book_spread_micros == 0
            || self.max_book_spread_micros >= SCALE
            || self.max_pair_edge_micros >= SCALE
            || self.max_feed_age_ms == 0
            || self.max_feed_age_ms > 60_000
            || self.close_guard_ms > 60_000
            || self.max_imbalance_micros < 5 * SCALE
            || self.max_imbalance_micros > MAX_QUANTITY
            || self.fair_weight_ppm > SCALE
            || self.imbalance_k_micros.unsigned_abs() > SCALE
            || self.flow_k_micros.unsigned_abs() > SCALE
            || bad_widening
        {
            return Err("invalid bounded strategy rules".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bbo {
    pub bid_micros: Option<u64>,
    pub ask_micros: Option<u64>,
    pub received_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signals {
    /// Population stdev of 250ms mid changes over 10s, supplied by caller.
    pub mid_vol_micros: Option<u64>,
    /// Requires a point-in-time, eligible settlement model; never inferred here.
    pub model_up_micros: Option<u64>,
    /// Normalized signed depth/flow, in [-1_000_000, 1_000_000].
    pub imbalance_ppm: Option<i64>,
    pub flow_ppm: Option<i64>,
    pub received_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Input {
    pub now_ms: u64,
    pub market_start_ms: u64,
    pub market_end_ms: u64,
    pub up: Bbo,
    pub down: Bbo,
    pub inventory_up_micros: u64,
    pub inventory_down_micros: u64,
    pub reserved_up_micros: u64,
    pub reserved_down_micros: u64,
    pub available_cash_micros: u64,
    pub tick_micros: u64,
    pub min_order_micros: u64,
    pub signals: Signals,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Gate {
    Quote,
    BeforeOpen,
    CloseGuard,
    StaleBook,
    InvalidBook,
    HollowBook,
    MissingSignal,
    StaleSignal,
    InsufficientCapital,
    PairEdge,
    OwnedCostUnknown,
    InventoryDust,
    InventoryCost,
    LossBudget,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub quotes: Vec<DesiredQuote>,
    pub reason: Gate,
    pub center_micros: Option<u64>,
    pub half_width_micros: Option<u64>,
}

fn abstain(reason: Gate) -> Decision {
    Decision {
        quotes: Vec::new(),
        reason,
        center_micros: None,
        half_width_micros: None,
    }
}

/// Observed UTC schedule, not a timezone rule: weekdays 12:00 <= hour < 23:00.
pub fn session_lot(now_ms: u64, lot_full_micros: u64) -> u64 {
    let seconds = now_ms / 1_000;
    let day = seconds / 86_400;
    let weekday = (day + 4) % 7; // Unix epoch was Thursday.
    let hour = (seconds % 86_400) / 3_600;
    if (1..=5).contains(&weekday) && (12..23).contains(&hour) {
        lot_full_micros
    } else {
        // TS Math.round(lotFull / 2), whole-share lots.
        ((lot_full_micros + SCALE) / (2 * SCALE)) * SCALE
    }
}

fn sqrt_floor(n: u128) -> u128 {
    if n < 2 {
        return n;
    }
    let mut x = 1_u128 << (n.ilog2() + 1).div_ceil(2);
    loop {
        let next = (x + n / x) / 2;
        if next >= x {
            return x;
        }
        x = next;
    }
}

fn sqrt_ceil(n: u128) -> u128 {
    let root = sqrt_floor(n);
    root + u128::from(root * root != n)
}

/// Integer-only conservative rounding of width; no floating transcendental profile.
pub fn half_width(rules: &Rules, input: &Input) -> Result<u64> {
    rules.validate()?;
    if input.signals.mid_vol_micros.is_some_and(|x| x > SCALE) {
        return Err("invalid bounded mid volatility".into());
    }
    let width = match rules.widening {
        Widening::None => u128::from(rules.half_width_micros),
        Widening::Vol { k_ppm } => {
            let sigma = input.signals.mid_vol_micros.ok_or("missing volatility")?;
            (u128::from(k_ppm) * u128::from(sigma))
                .div_ceil(u128::from(SCALE))
                .max(u128::from(rules.half_width_micros))
        }
        Widening::VolTime { k_ppm, tau_ref_ms } => {
            let sigma = input.signals.mid_vol_micros.ok_or("missing volatility")?;
            let left = input.market_end_ms.saturating_sub(input.now_ms).max(1_000);
            // sqrt(k^2*sigma^2*tau / left / SCALE^2), rounded up.
            let squared =
                u128::from(k_ppm).pow(2) * u128::from(sigma).pow(2) * u128::from(tau_ref_ms);
            let denominator = u128::from(left) * u128::from(SCALE).pow(2);
            sqrt_ceil(squared.div_ceil(denominator)).max(u128::from(rules.half_width_micros))
        }
    };
    u64::try_from(width).map_err(|_| "width overflow".into())
}

/// Fixed 6-slot output, bounded arithmetic. Empty desired list means cancel all.
pub fn decide(rules: &Rules, input: &Input) -> Result<Decision> {
    rules.validate()?;
    if input.market_start_ms >= input.market_end_ms
        || ![100_000, 10_000, 5_000, 2_500, 1_000, 100].contains(&input.tick_micros)
        || input.min_order_micros < 5 * SCALE
        || input.min_order_micros > MAX_QUANTITY
        || [
            input.inventory_up_micros,
            input.inventory_down_micros,
            input.reserved_up_micros,
            input.reserved_down_micros,
        ]
        .iter()
        .any(|x| *x > MAX_QUANTITY)
        || input.available_cash_micros > 1_000_000_000 * SCALE
        || input.signals.mid_vol_micros.is_some_and(|x| x > SCALE)
        || input.signals.model_up_micros.is_some_and(|x| x > SCALE)
        || [input.signals.imbalance_ppm, input.signals.flow_ppm]
            .into_iter()
            .flatten()
            .any(|x| x.unsigned_abs() > SCALE)
    {
        return Err("invalid bounded strategy input".into());
    }
    if input.now_ms < input.market_start_ms {
        return Ok(abstain(Gate::BeforeOpen));
    }
    if input.now_ms >= input.market_end_ms.saturating_sub(rules.close_guard_ms) {
        return Ok(abstain(Gate::CloseGuard));
    }
    let mut prices = [(0, 0); 2];
    for (index, book) in [input.up, input.down].into_iter().enumerate() {
        if book.received_ms > input.now_ms
            || input.now_ms - book.received_ms > rules.max_feed_age_ms
        {
            return Ok(abstain(Gate::StaleBook));
        }
        let (Some(bid), Some(ask)) = (book.bid_micros, book.ask_micros) else {
            return Ok(abstain(Gate::InvalidBook));
        };
        if bid == 0 || bid >= ask || ask >= SCALE {
            return Ok(abstain(Gate::InvalidBook));
        }
        if ask - bid > rules.max_book_spread_micros {
            return Ok(abstain(Gate::HollowBook));
        }
        prices[index] = (bid, ask);
    }
    let needs_signal = rules.widening != Widening::None
        || rules.fair_weight_ppm > 0
        || rules.imbalance_k_micros != 0
        || rules.flow_k_micros != 0;
    if needs_signal {
        if (rules.widening != Widening::None && input.signals.mid_vol_micros.is_none())
            || (rules.fair_weight_ppm > 0 && input.signals.model_up_micros.is_none())
            || (rules.imbalance_k_micros != 0 && input.signals.imbalance_ppm.is_none())
            || (rules.flow_k_micros != 0 && input.signals.flow_ppm.is_none())
        {
            return Ok(abstain(Gate::MissingSignal));
        }
        if input.signals.received_ms > input.now_ms
            || input.now_ms - input.signals.received_ms > rules.max_feed_age_ms
        {
            return Ok(abstain(Gate::StaleSignal));
        }
    }
    let mid = (prices[0].0 + prices[0].1) / 2;
    let fair = input.signals.model_up_micros.unwrap_or(mid);
    let reference = (i128::from(SCALE - rules.fair_weight_ppm) * i128::from(mid)
        + i128::from(rules.fair_weight_ppm) * i128::from(fair))
        / i128::from(SCALE);
    let lean = (i128::from(rules.imbalance_k_micros)
        * i128::from(input.signals.imbalance_ppm.unwrap_or(0))
        + i128::from(rules.flow_k_micros) * i128::from(input.signals.flow_ppm.unwrap_or(0)))
        / i128::from(SCALE);
    let center = (i128::from(SCALE / 2)
        + i128::from(rules.shrink_ppm) * (reference - i128::from(SCALE / 2)) / i128::from(SCALE)
        + lean)
        .clamp(0, i128::from(SCALE)) as u64;
    let width = half_width(rules, input)?;
    let round_tick =
        |value: u64| ((value + input.tick_micros / 2) / input.tick_micros) * input.tick_micros;
    let passive = |target: u64, ask: u64| {
        let max_bid = (ask.saturating_sub(1) / input.tick_micros) * input.tick_micros;
        round_tick(target).min(max_bid)
    };
    let quote_prices = [
        passive(center.saturating_sub(width), prices[0].1),
        passive(
            SCALE.saturating_sub(center).saturating_sub(width),
            prices[1].1,
        ),
    ];
    if quote_prices[0] + quote_prices[1] > SCALE - rules.max_pair_edge_micros {
        return Ok(abstain(Gate::PairEdge));
    }
    let imbalance = i128::from(input.inventory_up_micros) - i128::from(input.inventory_down_micros);
    let mut capacity = [
        (i128::from(rules.max_imbalance_micros) - imbalance - i128::from(input.reserved_up_micros))
            .max(0) as u64,
        (i128::from(rules.max_imbalance_micros) + imbalance
            - i128::from(input.reserved_down_micros))
        .max(0) as u64,
    ];
    let mut cash = u128::from(input.available_cash_micros) * u128::from(SCALE);
    let lot = if rules.schedule_lot {
        session_lot(input.now_ms, rules.lot_full_micros)
    } else {
        rules.lot_full_micros
    };
    let mut quotes = Vec::with_capacity(MAX_QUOTES);
    let mut add = |side: usize, kind: &str, price: u64, quantity: u64| {
        if price == 0 || price >= SCALE || !price.is_multiple_of(input.tick_micros) {
            return;
        }
        let affordable = u64::try_from(cash / u128::from(price)).unwrap_or(u64::MAX);
        let quantity = quantity.min(capacity[side]).min(affordable);
        if quantity < input.min_order_micros {
            return;
        }
        let outcome = if side == 0 {
            Outcome::Up
        } else {
            Outcome::Down
        };
        quotes.push(DesiredQuote {
            slot: format!("{}:{kind}", if side == 0 { "up" } else { "down" }),
            outcome,
            price_micros: price,
            quantity_micros: quantity,
        });
        cash -= u128::from(price) * u128::from(quantity);
        capacity[side] -= quantity;
    };
    // Risk-reducing hedge is admitted first; all opposite commitments remain independent.
    let lagging = usize::from(imbalance > 0);
    if imbalance != 0 && quote_prices[lagging] >= rules.min_price_micros {
        add(
            lagging,
            "hedge",
            quote_prices[lagging],
            imbalance.unsigned_abs() as u64,
        );
    }
    for (side, price) in quote_prices.into_iter().enumerate() {
        if price >= rules.min_price_micros {
            add(side, "lot", price, lot);
        }
    }
    if let Some(stub) = rules.stub_price_micros {
        for (side, (_, ask)) in prices.into_iter().enumerate() {
            if stub < ask {
                add(side, "stub", stub, lot);
            }
        }
    }
    let reason = if quotes.is_empty() {
        Gate::InsufficientCapital
    } else {
        Gate::Quote
    };
    Ok(Decision {
        quotes,
        reason,
        center_micros: Some(center),
        half_width_micros: Some(width),
    })
}

/// Public paper policy. While inventory is unmatched, quote only its complement.
/// Use immutable BUY fill costs, not the portfolio adapter's allocated side basis.
/// Historical research `decide` and funded behavior remain separate contracts.
pub fn decide_inventory_aware(
    rules: &Rules,
    input: &Input,
    outcome_spent_pico: Option<[u128; 2]>,
) -> Result<Decision> {
    let mut decision = decide(rules, input)?;
    if decision.reason != Gate::Quote {
        return Ok(decision);
    }
    let quantities = [input.inventory_up_micros, input.inventory_down_micros];
    let Some(costs) = outcome_spent_pico else {
        return Ok(abstain(Gate::OwnedCostUnknown));
    };
    if costs.iter().zip(quantities).any(|(cost, quantity)| {
        (*cost == 0) != (quantity == 0) || *cost > u128::from(quantity) * u128::from(SCALE)
    }) {
        return Err("invalid exact inventory cost".into());
    }
    if quantities[0] == quantities[1] {
        return Ok(decision);
    }
    let owned = usize::from(quantities[1] > quantities[0]);
    let lagging = 1 - owned;
    let remaining = quantities[owned]
        .abs_diff(quantities[lagging])
        .saturating_sub([input.reserved_up_micros, input.reserved_down_micros][lagging]);
    if remaining < input.min_order_micros {
        return Ok(abstain(Gate::InventoryDust));
    }
    let average_paid = costs[owned].div_ceil(u128::from(quantities[owned]));
    let cap = u128::from(SCALE - rules.max_pair_edge_micros).saturating_sub(average_paid);
    // Round down: rounding to the nearest tick can turn a break-even pair into a loss.
    let cap = (cap / u128::from(input.tick_micros)) as u64 * input.tick_micros;
    decision.quotes.retain_mut(|quote| {
        if quote.slot
            != if lagging == 0 {
                "up:hedge"
            } else {
                "down:hedge"
            }
        {
            return false;
        }
        quote.price_micros = quote.price_micros.min(cap);
        quote.quantity_micros = quote.quantity_micros.min(remaining);
        quote.price_micros >= rules.min_price_micros
            && quote.quantity_micros >= input.min_order_micros
    });
    if decision.quotes.is_empty() {
        decision.reason = Gate::InventoryCost;
    }
    Ok(decision)
}
