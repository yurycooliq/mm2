//! Pure capital accounting. Only finalized chain receipts can release inventory or cash.
use crate::{
    ChainOperation, ChainPlan, ChainReceipt, OrderPhase, PreparedChainTransaction, model::Ledger,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) fn is_zero(value: &u64) -> bool {
    *value == 0
}
pub(crate) fn zero_string(value: &String) -> bool {
    value.is_empty() || value == "0"
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OwnedPosition {
    pub quantity_micros: u64,
    pub cost_pico: String,
    pub day: u64,
    pub daily_quantity_micros: u64,
}
impl Default for OwnedPosition {
    fn default() -> Self {
        Self {
            quantity_micros: 0,
            cost_pico: "0".into(),
            day: 0,
            daily_quantity_micros: 0,
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SettlementMarket {
    pub token_ids: [String; 2],
    pub window_end_ms: u64,
}
fn index(outcome: mm2_core::Outcome) -> usize {
    if outcome == mm2_core::Outcome::Up {
        0
    } else {
        1
    }
}
impl Ledger {
    pub(crate) fn remember_market(&mut self, quote: &crate::LiveQuote) -> Result<(), String> {
        let market = self
            .closeout_markets
            .entry(quote.condition_id.clone())
            .or_default();
        let slot = &mut market.token_ids[index(quote.outcome)];
        if (!slot.is_empty() && *slot != quote.token_id)
            || (market.window_end_ms != 0 && market.window_end_ms != quote.window_end_ms)
        {
            return Err("closeout market identity changed".into());
        }
        *slot = quote.token_id.clone();
        market.window_end_ms = quote.window_end_ms;
        Ok(())
    }
    pub(crate) fn net_spent_pico(&self) -> Result<u128, String> {
        Ok(self
            .session_spent_pico
            .parse::<u128>()
            .map_err(|_| "invalid session spend")?
            .saturating_sub(u128::from(self.returned_collateral_micros) * 1_000_000))
    }
    pub(crate) fn collect_terminal_diagnostics(&mut self) {
        // Older terminal orders remain in immutable audit segments, rather than every new checkpoint.
        let mut terminal: Vec<_> = self
            .orders
            .values()
            .filter(|o| o.phase == OrderPhase::Closed)
            .map(|o| (o.created_at_ms, o.order_id.clone()))
            .collect();
        terminal.sort();
        let remove = terminal.len().saturating_sub(16);
        for (_, id) in terminal.into_iter().take(remove) {
            self.orders.remove(&id);
            self.signed_intents.remove(&id);
        }
        let protected: BTreeSet<_> = self
            .orders
            .values()
            .map(|o| format!("{}:{}", o.quote.condition_id, o.quote.token_id))
            .collect();
        self.positions
            .retain(|key, p| p.quantity_micros > 0 || protected.contains(key));
        self.position_floor
            .retain(|key, p| p.quantity_micros > 0 || protected.contains(key));
        self.owned_positions.retain(|_, p| p.quantity_micros > 0);
        self.closeout_markets.retain(|condition, _| {
            self.positions
                .values()
                .any(|p| &p.condition_id == condition && p.quantity_micros > 0)
                || self
                    .orders
                    .values()
                    .any(|o| &o.quote.condition_id == condition)
                || self
                    .chain_pending
                    .as_ref()
                    .is_some_and(|p| &p.plan.condition_id == condition)
        });
    }
    pub(crate) fn closeout_candidate(&self, now_ms: u64, after: Option<&str>) -> Option<ChainPlan> {
        if self.chain_pending.is_some() {
            return None;
        }
        let mut candidates = Vec::new();
        for (condition, market) in &self.closeout_markets {
            if market.window_end_ms > now_ms
                || self
                    .orders
                    .values()
                    .any(|o| &o.quote.condition_id == condition && o.phase != OrderPhase::Closed)
            {
                continue;
            }
            let mut quantities = [0u64; 2];
            for position in self
                .positions
                .values()
                .filter(|p| &p.condition_id == condition)
            {
                quantities[index(position.outcome)] = position.quantity_micros;
            }
            if quantities == [0, 0] {
                continue;
            }
            let amount = quantities[0].min(quantities[1]);
            candidates.push(ChainPlan {
                id: format!(
                    "closeout:{}:{}",
                    &condition[2..18],
                    self.chain_receipts.last().map_or(0, |r| r.block_number)
                ),
                condition_id: condition.clone(),
                token_ids: market.token_ids.clone(),
                operation: if amount > 0 {
                    ChainOperation::Merge {
                        amount_micros: amount,
                    }
                } else {
                    ChainOperation::Redeem
                },
            });
        }
        candidates.sort_by_key(|p| {
            (
                matches!(p.operation, ChainOperation::Redeem),
                p.condition_id.clone(),
            )
        });
        let index = after
            .and_then(|previous| candidates.iter().position(|p| p.condition_id == previous))
            .map_or(0, |i| (i + 1) % candidates.len().max(1));
        candidates.into_iter().nth(index)
    }
    pub(crate) fn admit_chain(&self, prepared: &PreparedChainTransaction) -> Result<(), String> {
        if self.chain_pending.is_some()
            || self.orders.values().any(|o| {
                o.quote.condition_id == prepared.plan.condition_id && o.phase != OrderPhase::Closed
            })
        {
            return Err("closeout overlaps a pending transaction or uncertain order".into());
        }
        let market = self
            .closeout_markets
            .get(&prepared.plan.condition_id)
            .ok_or("closeout market absent")?;
        for i in 0..2 {
            if !market.token_ids[i].is_empty()
                && !prepared.plan.token_ids.contains(&market.token_ids[i])
            {
                return Err("closeout outcome token changed".into());
            }
            let key = format!(
                "{}:{}",
                prepared.plan.condition_id, prepared.plan.token_ids[i]
            );
            let tracked = self.positions.get(&key).map_or(0, |p| p.quantity_micros);
            if prepared.before_tokens_micros[i] != tracked {
                return Err("onchain balance differs from retained position; refresh account or investigate external mutation".into());
            }
        }
        Ok(())
    }
    pub(crate) fn apply_chain_receipt(&mut self, receipt: &ChainReceipt) -> Result<(), String> {
        let prepared = self
            .chain_pending
            .clone()
            .ok_or("no pending transaction for chain receipt")?;
        let identity_matches = if prepared.relayer.is_some() {
            receipt.intent_hash.as_deref() == Some(prepared.hash.as_str())
                && crate::settings::condition(&receipt.transaction_hash)
                && receipt.success
                && receipt.gas_cost_wei == "0"
                && receipt
                    .sponsored_gas_cost_wei
                    .as_ref()
                    .is_some_and(|s| s.parse::<u128>().is_ok())
        } else {
            receipt.transaction_hash == prepared.hash
                && receipt.intent_hash.is_none()
                && receipt.sponsored_gas_cost_wei.is_none()
        };
        if !identity_matches {
            return Err("chain receipt identity mismatch".into());
        }
        if receipt.success {
            let expected_consumption = match prepared.plan.operation {
                ChainOperation::Merge { amount_micros } => [amount_micros; 2],
                ChainOperation::Redeem => prepared.before_tokens_micros,
            };
            if receipt.consumed_tokens_micros != expected_consumption
                || receipt.payout_numerators != prepared.payout_numerators
                || receipt.payout_denominator != prepared.payout_denominator
                || receipt.collateral_delta_micros < prepared.expected_payout_micros
                || (matches!(prepared.plan.operation, ChainOperation::Merge { .. })
                    && receipt.collateral_delta_micros != prepared.expected_payout_micros)
            {
                return Err("chain receipt economic effects differ from signed intent".into());
            }
        }
        self.session_gas_cost_wei = (self
            .session_gas_cost_wei
            .parse::<u128>()
            .unwrap_or(0)
            .checked_add(
                receipt
                    .gas_cost_wei
                    .parse::<u128>()
                    .map_err(|_| "invalid receipt gas cost")?,
            )
            .ok_or("session gas cost overflow")?)
        .to_string();
        if !receipt.success {
            if receipt.consumed_tokens_micros != [0, 0] || receipt.collateral_delta_micros != 0 {
                return Err("failed chain receipt contains economic effects".into());
            }
        } else {
            let mut consumed_owned = [0u64; 2];
            let mut consumed_today = [0u64; 2];
            let mut cost_removed = 0u128;
            for i in 0..2 {
                let consumed = receipt.consumed_tokens_micros[i];
                if consumed > prepared.before_tokens_micros[i] {
                    return Err("chain token consumption exceeds prestate".into());
                }
                let key = format!(
                    "{}:{}",
                    prepared.plan.condition_id, prepared.plan.token_ids[i]
                );
                if let Some(owned) = self.owned_positions.get_mut(&key) {
                    // Unknown imported holdings are consumed first; they never fund a fresh risk budget.
                    let imported =
                        prepared.before_tokens_micros[i].saturating_sub(owned.quantity_micros);
                    let own = consumed.saturating_sub(imported).min(owned.quantity_micros);
                    consumed_owned[i] = own;
                    let daily = if owned.day == self.loss_day {
                        owned.daily_quantity_micros
                    } else {
                        0
                    };
                    consumed_today[i] = own
                        .saturating_sub(owned.quantity_micros.saturating_sub(daily))
                        .min(daily);
                    let cost = owned
                        .cost_pico
                        .parse::<u128>()
                        .map_err(|_| "invalid owned closeout cost")?;
                    let removed = if owned.quantity_micros == 0 {
                        0
                    } else {
                        cost * u128::from(own) / u128::from(owned.quantity_micros)
                    };
                    cost_removed = cost_removed
                        .checked_add(removed)
                        .ok_or("realized cost overflow")?;
                    owned.quantity_micros -= own;
                    owned.daily_quantity_micros = daily.saturating_sub(consumed_today[i]);
                    owned.cost_pico = (cost - removed).to_string();
                }
                for positions in [&mut self.positions, &mut self.position_floor] {
                    if let Some(position) = positions.get_mut(&key) {
                        let remaining = prepared.before_tokens_micros[i] - consumed;
                        let prior = position.quantity_micros;
                        let cost = position
                            .cost_pico
                            .parse::<u128>()
                            .map_err(|_| "invalid position closeout cost")?;
                        // Keep imported cost conservative. Only an authenticated chain effect lowers quantity.
                        position.quantity_micros = remaining;
                        let remaining_owned_cost =
                            self.owned_positions.get(&key).map_or(Ok(0), |owned| {
                                owned
                                    .cost_pico
                                    .parse::<u128>()
                                    .map_err(|_| "invalid remaining owned cost")
                            })?;
                        let remaining_cost = if remaining == 0 {
                            0
                        } else if prior == 0 || remaining >= prior {
                            cost
                        } else {
                            (cost * u128::from(remaining)).div_ceil(u128::from(prior))
                        };
                        position.cost_pico = remaining_cost.max(remaining_owned_cost).to_string();
                    }
                }
            }
            let allocate_return = |amounts: [u64; 2]| -> Result<u64, String> {
                let value = match prepared.plan.operation {
                    ChainOperation::Merge { .. } => u128::from(amounts[0].min(amounts[1])),
                    ChainOperation::Redeem => {
                        if receipt.payout_denominator == 0 {
                            return Err("redemption lacks resolved payout denominator".into());
                        }
                        u128::from(amounts[0]) * u128::from(receipt.payout_numerators[0])
                            / u128::from(receipt.payout_denominator)
                            + u128::from(amounts[1]) * u128::from(receipt.payout_numerators[1])
                                / u128::from(receipt.payout_denominator)
                    }
                };
                u64::try_from(value).map_err(|_| "closeout payout overflow".into())
            };
            let returned = allocate_return(consumed_owned)?.min(receipt.collateral_delta_micros);
            let daily_returned = allocate_return(consumed_today)?.min(returned);
            self.returned_collateral_micros = self
                .returned_collateral_micros
                .checked_add(returned)
                .ok_or("returned collateral overflow")?;
            self.realized_cost_pico = (self
                .realized_cost_pico
                .parse::<u128>()
                .unwrap_or(0)
                .checked_add(cost_removed)
                .ok_or("realized cost overflow")?)
            .to_string();
            self.daily_loss_micros = self.daily_loss_micros.saturating_sub(daily_returned);
        }
        self.chain_receipts.push(receipt.clone());
        if self.chain_receipts.len() > 16 {
            self.chain_receipts.remove(0);
        }
        self.chain_pending = None;
        self.collect_terminal_diagnostics();
        Ok(())
    }
    pub(crate) fn apply_chain_expiry(
        &mut self,
        expiry: &crate::relay::RelayExpiry,
    ) -> Result<(), String> {
        let prepared = self
            .chain_pending
            .as_ref()
            .ok_or("no pending batch for expiry")?;
        let relay = prepared
            .relayer
            .as_ref()
            .ok_or("direct transaction cannot expire by wallet deadline")?;
        if !relay.dispatched
            || expiry.intent_hash != prepared.hash
            || !crate::settings::condition(&expiry.block_hash)
            || expiry.block_number < relay.prepared_block_number
            || expiry.finalized_timestamp_seconds <= relay.deadline
            || expiry.wallet_nonce != prepared.nonce
            || expiry.retained_tokens_micros != prepared.before_tokens_micros
        {
            return Err("expiry evidence differs from durable batch/nonce/token intent".into());
        }
        self.chain_expiries.push(expiry.clone());
        if self.chain_expiries.len() > 16 {
            self.chain_expiries.remove(0);
        }
        self.chain_pending = None;
        Ok(())
    }
}

pub(crate) fn validate(ledger: &Ledger, settings: &crate::LiveSettings) -> Result<(), String> {
    if ledger.owned_positions.len() > 128
        || ledger.closeout_markets.len() > 128
        || ledger.chain_receipts.len() > 16
        || ledger.chain_expiries.len() > 16
        || ledger.realized_cost_pico.parse::<u128>().unwrap_or(
            if ledger.realized_cost_pico.is_empty() {
                0
            } else {
                u128::MAX
            },
        ) > 10u128.pow(32)
        || ledger.session_gas_cost_wei.parse::<u128>().unwrap_or(
            if ledger.session_gas_cost_wei.is_empty() {
                0
            } else {
                u128::MAX
            },
        ) > 10u128.pow(20)
    {
        return Err("bounded closeout ledger exceeded".into());
    }
    let mut owned_basis = 0u128;
    for (key, owned) in &ledger.owned_positions {
        if !ledger.position_floor.contains_key(key)
            || owned.quantity_micros > 1_000_000_000_000_000
            || owned.daily_quantity_micros > owned.quantity_micros
            || owned.cost_pico.parse::<u128>().is_err()
        {
            return Err("invalid owned position structure".into());
        }
        let cost = owned
            .cost_pico
            .parse::<u128>()
            .map_err(|_| "invalid owned cost")?;
        owned_basis = owned_basis
            .checked_add(cost)
            .ok_or("owned basis overflow")?;
        let floor = ledger
            .position_floor
            .get(key)
            .ok_or("owned position floor absent")?;
        let position = ledger.positions.get(key).ok_or("owned position absent")?;
        if cost > u128::from(owned.quantity_micros) * 2_000_000
            || owned.quantity_micros > floor.quantity_micros
            || owned.quantity_micros > position.quantity_micros
            || cost
                > floor
                    .cost_pico
                    .parse::<u128>()
                    .map_err(|_| "invalid floor cost")?
            || cost
                > position
                    .cost_pico
                    .parse::<u128>()
                    .map_err(|_| "invalid position cost")?
        {
            return Err("owned cost exceeds accounting bound".into());
        }
    }
    let realized = if ledger.realized_cost_pico.is_empty() {
        0
    } else {
        ledger
            .realized_cost_pico
            .parse::<u128>()
            .map_err(|_| "invalid realized basis")?
    };
    let spent = ledger
        .session_spent_pico
        .parse::<u128>()
        .map_err(|_| "invalid lifetime spend")?;
    if owned_basis
        .checked_add(realized)
        .is_none_or(|basis| basis > spent)
    {
        return Err("owned and realized basis exceeds lifetime purchases".into());
    }
    for (condition, market) in &ledger.closeout_markets {
        if !crate::settings::condition(condition)
            || market.window_end_ms == 0
            || market.token_ids.iter().all(String::is_empty)
            || (!market.token_ids[0].is_empty() && market.token_ids[0] == market.token_ids[1])
            || market.token_ids.iter().any(|t| {
                !t.is_empty()
                    && (t.len() > 78
                        || t.starts_with('0')
                        || !t.bytes().all(|c| c.is_ascii_digit()))
            })
        {
            return Err("invalid retained closeout market".into());
        }
    }
    if let Some(pending) = &ledger.chain_pending {
        let chain = settings
            .chain
            .as_ref()
            .ok_or("pending chain intent requires its configured transport")?;
        crate::chain::validate_prepared(settings, chain, pending)?;
        if !ledger
            .closeout_markets
            .contains_key(&pending.plan.condition_id)
        {
            return Err("pending chain market absent".into());
        }
    }
    for receipt in &ledger.chain_receipts {
        if !crate::settings::condition(&receipt.transaction_hash)
            || !crate::settings::condition(&receipt.block_hash)
            || receipt.finalized_block_number < receipt.block_number
            || receipt.gas_cost_wei.parse::<u128>().is_err()
            || receipt
                .intent_hash
                .as_ref()
                .is_some_and(|h| !crate::settings::condition(h))
            || receipt
                .sponsored_gas_cost_wei
                .as_ref()
                .is_some_and(|v| v.parse::<u128>().is_err())
            || (receipt.intent_hash.is_some()
                && (!receipt.success
                    || receipt.gas_cost_wei != "0"
                    || receipt.sponsored_gas_cost_wei.is_none()))
            || (receipt.intent_hash.is_none() && receipt.sponsored_gas_cost_wei.is_some())
            || (!receipt.success
                && (receipt.collateral_delta_micros != 0
                    || receipt.consumed_tokens_micros != [0, 0]))
        {
            return Err("invalid retained finalized chain receipt".into());
        }
    }
    for expiry in &ledger.chain_expiries {
        if !crate::settings::condition(&expiry.intent_hash)
            || !crate::settings::condition(&expiry.block_hash)
            || expiry.block_number == 0
            || expiry.finalized_timestamp_seconds == 0
        {
            return Err("invalid retained relay expiry proof".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn net_budget_credits_only_verified_owned_return() {
        let ledger = Ledger {
            session_spent_pico: "4900000000000".into(),
            returned_collateral_micros: 5_000_000,
            ..Ledger::default()
        };
        assert_eq!(ledger.net_spent_pico().unwrap(), 0);
    }
    #[test]
    fn zero_fill_market_diagnostics_do_not_exhaust_floor() {
        let mut ledger = Ledger::default();
        for n in 1..=200 {
            let key = format!("0x{:064x}:{}", n, n);
            ledger.position_floor.insert(
                key,
                crate::LivePosition {
                    condition_id: format!("0x{n:064x}"),
                    token_id: n.to_string(),
                    outcome: mm2_core::Outcome::Up,
                    quantity_micros: 0,
                    cost_pico: "0".into(),
                    redeemable: false,
                    mergeable: false,
                },
            );
            ledger.collect_terminal_diagnostics();
            assert!(ledger.position_floor.is_empty());
        }
    }
}

#[cfg(test)]
#[path = "settlement_review_tests.rs"]
mod independent_review_tests;
