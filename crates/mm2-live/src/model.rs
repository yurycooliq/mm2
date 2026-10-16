use crate::settings::{LiveSettings, condition};
use crate::{ChainReceipt, PreparedChainTransaction};
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
    #[serde(default)]
    pub execution_fees: Vec<crate::fees::ExecutionFee>,
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub owned_positions: BTreeMap<String, crate::settlement::OwnedPosition>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub closeout_markets: BTreeMap<String, crate::settlement::SettlementMarket>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_pending: Option<PreparedChainTransaction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain_receipts: Vec<ChainReceipt>,
    #[serde(default, skip_serializing_if = "crate::settlement::is_zero")]
    pub returned_collateral_micros: u64,
    #[serde(default, skip_serializing_if = "crate::settlement::zero_string")]
    pub realized_cost_pico: String,
    #[serde(default, skip_serializing_if = "crate::settlement::zero_string")]
    pub session_gas_cost_wei: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain_expiries: Vec<crate::relay::RelayExpiry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fee_policies: BTreeMap<String, crate::fees::FeePolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub execution_fees: BTreeMap<String, crate::fees::ExecutionFee>,
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
            owned_positions: BTreeMap::new(),
            closeout_markets: BTreeMap::new(),
            chain_pending: None,
            chain_receipts: Vec::new(),
            returned_collateral_micros: 0,
            realized_cost_pico: String::new(),
            session_gas_cost_wei: String::new(),
            chain_expiries: Vec::new(),
            fee_policies: BTreeMap::new(),
            execution_fees: BTreeMap::new(),
        }
    }
}
impl Ledger {
    pub(crate) fn fees_mismatched(&self) -> bool {
        crate::fees::mismatched(&self.fee_policies, &self.execution_fees)
    }
    fn latch_fee_mismatch(&mut self) {
        if self.fees_mismatched() {
            self.phase = LivePhase::Faulted;
            self.fault = Some("execution fee or role mismatch; finalized evidence retained; owner reconciliation required".into());
        }
    }
    pub(crate) fn fees_pending(&self) -> bool {
        self.orders.values().any(|order| {
            self.fee_policies.contains_key(&order.order_id)
                && u128::from(order.cumulative_filled_micros)
                    != self
                        .execution_fees
                        .values()
                        .filter(|p| p.order_id == order.order_id)
                        .map(|p| u128::from(p.quantity_micros))
                        .sum::<u128>()
        })
    }
    pub(crate) fn fee_summary(&self) -> serde_json::Value {
        crate::fees::summary(&self.orders, &self.fee_policies, &self.execution_fees)
    }
    pub(crate) fn observe_fee(&mut self, proof: crate::fees::ExecutionFee) -> Result<(), String> {
        let order = self
            .orders
            .get(&proof.order_id)
            .ok_or("fee proof refers to untracked order")?;
        let key = proof.key();
        if let Some(prior) = self.execution_fees.get(&key) {
            let mut comparable = proof.clone();
            comparable.finalized_block_number = prior.finalized_block_number;
            if prior != &comparable {
                return Err("finalized fee proof changed".into());
            }
            self.latch_fee_mismatch();
            return Ok(());
        }
        if self.execution_fees.len() >= 1024 {
            return Err("bounded execution fee proof ledger exhausted".into());
        }
        let already: u64 = self
            .execution_fees
            .values()
            .filter(|p| p.order_id == proof.order_id)
            .try_fold(0u64, |sum, p| {
                sum.checked_add(p.quantity_micros)
                    .ok_or("fee quantity overflow")
            })?;
        if proof.quantity_micros == 0
            || already
                .checked_add(proof.quantity_micros)
                .is_none_or(|q| q > order.cumulative_filled_micros)
        {
            return Err("finalized fee quantity exceeds authoritative order fill".into());
        }
        self.execution_fees.insert(key, proof);
        self.latch_fee_mismatch();
        Ok(())
    }
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
            let owned = self
                .owned_positions
                .entry(format!(
                    "{}:{}",
                    order.quote.condition_id, order.quote.token_id
                ))
                .or_default();
            if owned.day != self.loss_day {
                owned.day = self.loss_day;
                owned.daily_quantity_micros = 0;
            }
            owned.quantity_micros = owned
                .quantity_micros
                .checked_add(filled)
                .ok_or("owned quantity overflow")?;
            owned.daily_quantity_micros = owned
                .daily_quantity_micros
                .checked_add(filled)
                .ok_or("daily quantity overflow")?;
            owned.cost_pico = (owned
                .cost_pico
                .parse::<u128>()
                .map_err(|_| "invalid owned cost")?
                + charged)
                .to_string();
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
    pub execution_fee_reconciliation: serde_json::Value,
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
    /// Research readers must obey this committed prefix, never the mutable file's EOF.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_committed_prefix: Option<serde_json::Value>,
    pub quotes_coalesced: u64,
    pub prepare_attempts: u64,
    pub prepare_failures: u64,
    pub last_prepare_error: Option<String>,
    pub unsupported_operations: Vec<String>,
    pub returned_collateral_micros: u64,
    pub conservative_realized_pnl_pico: String,
    pub chain_pending_hash: Option<String>,
    pub chain_pending_kind: Option<String>,
    pub chain_receipts: Vec<ChainReceipt>,
    pub chain_expiries: Vec<crate::relay::RelayExpiry>,
    pub last_chain_error: Option<String>,
    pub risk_limits: crate::LiveRiskLimits,
    pub session_gas_cost_wei: String,
}
