use crate::settings::{LiveSettings, condition};
use mm2_core::Outcome;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveQuote {
    pub slot: String,
    pub condition_id: String,
    pub market_slug: String,
    pub token_id: String,
    pub outcome: Outcome,
    pub price_micros: u64,
    pub quantity_micros: u64,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub tick_micros: u64,
    pub min_order_micros: u64,
    pub fee_bps: u32,
    pub protocol: u8,
    pub neg_risk: bool,
}
impl LiveQuote {
    pub fn validate(&self, settings: &LiveSettings, now_ms: u64) -> Result<(), String> {
        let family_allowed = settings.allowed_market_prefixes.iter().any(|prefix| {
            self.market_slug
                .strip_prefix(prefix)
                .and_then(|s| s.parse::<u64>().ok())
                .is_some_and(|start| {
                    start.checked_mul(1000) == Some(self.window_start_ms)
                        && start.is_multiple_of(300)
                        && self.window_end_ms == self.window_start_ms.saturating_add(300_000)
                })
        });
        if self.slot.is_empty()
            || self.slot.len() > 64
            || !condition(&self.condition_id)
            || self.market_slug.is_empty()
            || self.market_slug.len() > 128
            || (!settings.allowed_conditions.contains(&self.condition_id) && !family_allowed)
            || self.token_id.is_empty()
            || self.token_id.len() > 78
            || !self.token_id.bytes().all(|b| b.is_ascii_digit())
            || self.token_id == "0"
            || self.price_micros == 0
            || self.price_micros >= 1_000_000
            || ![100_000, 10_000, 5000, 2500, 1000, 100].contains(&self.tick_micros)
            || !self.price_micros.is_multiple_of(self.tick_micros)
            || self.min_order_micros == 0
            || self.quantity_micros < self.min_order_micros
            || self.quantity_micros > settings.risk.max_order_micros
            || !self.quantity_micros.is_multiple_of(10_000)
            || now_ms < self.window_start_ms
            || self.window_end_ms <= now_ms.saturating_add(1000)
            || self.window_end_ms > now_ms.saturating_add(3_600_000)
            || self.fee_bps > 10_000
            || self.protocol != 2
            || self.neg_risk
        {
            return Err("live quote violates explicit market/size/expiry/risk contract".into());
        }
        Ok(())
    }
    pub fn key(&self) -> String {
        format!("{}:{}", self.condition_id, self.slot)
    }
    pub fn commitment_pico(&self) -> u128 {
        let base = u128::from(self.price_micros) * u128::from(self.quantity_micros);
        base + (base * u128::from(self.fee_bps)).div_ceil(10_000)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Pause,
    Resume,
    Kill,
    Reset,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivePhase {
    Paused,
    Running,
    Killed,
    Faulted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveControlReceipt {
    pub command_id: String,
    pub action: ControlAction,
    pub phase: LivePhase,
    pub durable_sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderPhase {
    Prepared,
    Posting,
    Live,
    Cancelling,
    UnknownPost,
    UnknownCancel,
    Closed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveOrder {
    pub order_id: String,
    pub quote: LiveQuote,
    pub phase: OrderPhase,
    pub cumulative_filled_micros: u64,
    pub created_at_ms: u64,
    pub last_operation_ms: u64,
    pub retry_at_ms: u64,
}
impl LiveOrder {
    pub fn reserve_pico(&self) -> u128 {
        if self.phase == OrderPhase::Closed {
            return 0;
        }
        let remaining = self
            .quote
            .quantity_micros
            .saturating_sub(self.cumulative_filled_micros);
        let base = u128::from(self.quote.price_micros) * u128::from(remaining);
        base + (base * u128::from(self.quote.fee_bps)).div_ceil(10_000)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivePosition {
    pub condition_id: String,
    pub token_id: String,
    pub outcome: Outcome,
    pub quantity_micros: u64,
    pub cost_pico: String,
    pub redeemable: bool,
    pub mergeable: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountObservation {
    pub collateral_micros: u64,
    pub allowance_micros: u64,
    pub positions: Vec<LivePosition>,
    pub orders: Vec<OrderObservation>,
    pub foreign_order_ids: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderObservation {
    pub order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub price_micros: u64,
    pub original_quantity_micros: u64,
    pub cumulative_filled_micros: u64,
    /// Only the authenticated order endpoint can establish terminality, not missing open orders.
    pub terminal: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub phase: LivePhase,
    pub orders: BTreeMap<String, LiveOrder>,
    pub positions: BTreeMap<String, LivePosition>,
    /// Pre-effect baseline plus tracked fill deltas. Account observations never increment this floor.
    pub position_floor: BTreeMap<String, LivePosition>,
    pub receipts: BTreeMap<String, LiveControlReceipt>,
    /// Signed wire intent is durable before sending; never exposed through the dashboard.
    pub signed_intents: BTreeMap<String, serde_json::Value>,
    pub collateral_micros: u64,
    pub allowance_micros: u64,
    pub initial_collateral_micros: Option<u64>,
    pub session_spent_pico: String,
    pub daily_loss_micros: u64,
    pub loss_day: u64,
    pub foreign_order_ids: Vec<String>,
    pub fault: Option<String>,
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            phase: LivePhase::Paused,
            orders: BTreeMap::new(),
            positions: BTreeMap::new(),
            position_floor: BTreeMap::new(),
            receipts: BTreeMap::new(),
            signed_intents: BTreeMap::new(),
            collateral_micros: 0,
            allowance_micros: 0,
            initial_collateral_micros: None,
            session_spent_pico: "0".into(),
            daily_loss_micros: 0,
            loss_day: 0,
            foreign_order_ids: vec![],
            fault: None,
        }
    }
}
impl Ledger {
    pub fn reserved_pico(&self) -> u128 {
        self.orders.values().map(LiveOrder::reserve_pico).sum()
    }
    pub fn observe(&mut self, obs: &OrderObservation) -> Result<(), String> {
        let order = self
            .orders
            .get_mut(&obs.order_id)
            .ok_or("observation refers to untracked order")?;
        if order.quote.condition_id != obs.condition_id
            || order.quote.token_id != obs.token_id
            || order.quote.price_micros != obs.price_micros
            || order.quote.quantity_micros != obs.original_quantity_micros
            || obs.cumulative_filled_micros > order.quote.quantity_micros
        {
            return Err("authoritative order identity or quantity mismatch".into());
        }
        if order.phase == OrderPhase::Closed
            && obs.cumulative_filled_micros > order.cumulative_filled_micros
        {
            return Err("fill contradicts already terminal live order".into());
        }
        if obs.terminal && obs.cumulative_filled_micros < order.cumulative_filled_micros {
            return Err("terminal cumulative fill regressed".into());
        }
        let filled = obs
            .cumulative_filled_micros
            .saturating_sub(order.cumulative_filled_micros);
        if filled > 0 {
            let mut spent: u128 = self
                .session_spent_pico
                .parse()
                .map_err(|_| "corrupt exact spend")?;
            let base = u128::from(filled) * u128::from(order.quote.price_micros);
            let charged = base + (base * u128::from(order.quote.fee_bps)).div_ceil(10_000);
            spent += charged;
            self.session_spent_pico = spent.to_string();
            order.cumulative_filled_micros = obs.cumulative_filled_micros;
            // Increment the durable floor, never the potentially ahead Data API quantity.
            let key = format!("{}:{}", order.quote.condition_id, order.quote.token_id);
            let p = self
                .position_floor
                .entry(key.clone())
                .or_insert(LivePosition {
                    condition_id: order.quote.condition_id.clone(),
                    token_id: order.quote.token_id.clone(),
                    outcome: order.quote.outcome,
                    quantity_micros: 0,
                    cost_pico: "0".into(),
                    redeemable: false,
                    mergeable: false,
                });
            p.quantity_micros = p
                .quantity_micros
                .checked_add(filled)
                .ok_or("position overflow")?;
            p.cost_pico = (p
                .cost_pico
                .parse::<u128>()
                .map_err(|_| "corrupt position cost")?
                + charged)
                .to_string();
            let position = self.positions.entry(key).or_insert_with(|| p.clone());
            position.quantity_micros = position.quantity_micros.max(p.quantity_micros);
            position.cost_pico = position
                .cost_pico
                .parse::<u128>()
                .map_err(|_| "corrupt observed cost")?
                .max(
                    p.cost_pico
                        .parse::<u128>()
                        .map_err(|_| "corrupt floor cost")?,
                )
                .to_string();
        }
        if obs.terminal {
            order.phase = OrderPhase::Closed;
        } else if !matches!(
            order.phase,
            OrderPhase::Cancelling | OrderPhase::UnknownCancel | OrderPhase::Closed
        ) {
            order.phase = OrderPhase::Live;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveSnapshot {
    pub simulated: bool,
    pub phase: LivePhase,
    pub can_resume: bool,
    pub account_reconciled: bool,
    pub user_stream_connected: bool,
    pub last_reconciliation_ms: Option<u64>,
    pub last_heartbeat_ms: Option<u64>,
    pub collateral_micros: u64,
    pub allowance_micros: u64,
    pub reserved_pico: String,
    pub available_micros: u64,
    pub session_spent_pico: String,
    pub daily_loss_micros: u64,
    pub daily_loss_metric: String,
    pub pnl_available: bool,
    pub orders: Vec<LiveOrder>,
    pub tracked_order_count: usize,
    pub active_order_count: usize,
    pub positions: Vec<LivePosition>,
    pub foreign_order_ids: Vec<String>,
    pub fault: Option<String>,
    pub durable_sequence: u64,
    pub journal_bytes: u64,
    pub journal_max_bytes: u64,
    pub quotes_coalesced: u64,
    pub prepare_attempts: u64,
    pub prepare_failures: u64,
    pub last_prepare_error: Option<String>,
    pub unsupported_operations: Vec<String>,
}
