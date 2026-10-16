//! Public fee evidence and finalized per-fill reconciliation. Missing evidence is never zero.
use alloy::primitives::{Address, B256, U256, keccak256};
use polymarket_client_sdk_v2::types::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, str::FromStr};

pub const FEE_TTL_MS: u64 = 90_000;
pub const FEE_REFRESH_MS: u64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeePolicy {
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
    pub rate: String,
    pub exponent: u32,
    pub taker_only: bool,
    pub observed_at_ms: u64,
    pub response_sha256: String,
}
impl FeePolicy {
    pub fn validate(&self) -> Result<(), String> {
        if !crate::settings::condition(&self.condition_id)
            || self.up_token == self.down_token
            || [&self.up_token, &self.down_token]
                .iter()
                .any(|t| U256::from_str(t).is_err() || *t == "0" || t.len() > 78)
            || self.observed_at_ms == 0
            || self.response_sha256.len() != 64
            || !self.response_sha256.bytes().all(|c| c.is_ascii_hexdigit())
            || self.exponent > 8
        {
            return Err("invalid fee evidence identity or bounds".into());
        }
        let rate = Decimal::from_str(&self.rate).map_err(|_| "invalid explicit fee rate")?;
        if rate < Decimal::ZERO || rate > Decimal::ONE {
            return Err("fee rate outside supported bounds".into());
        }
        Ok(())
    }
    /// This release supports post-only makers with explicitly verified zero exchange fee.
    /// A future nonzero maker schedule blocks new orders instead of silently undercharging.
    pub fn maker_zero(&self) -> bool {
        self.validate().is_ok()
            && (self.taker_only || Decimal::from_str(&self.rate) == Ok(Decimal::ZERO))
    }
    pub fn permits(&self, condition: &str, up: &str, down: &str, now_ms: u64) -> bool {
        self.condition_id == condition
            && self.up_token == up
            && self.down_token == down
            && now_ms >= self.observed_at_ms
            && now_ms - self.observed_at_ms < FEE_TTL_MS
            && self.maker_zero()
    }
    pub fn status(&self, now_ms: u64) -> Value {
        json!({"policy":self,"age_ms":now_ms.saturating_sub(self.observed_at_ms),
            "exchange_fee_model":"verified_post_only_maker_zero",
            "actual_live_fee":null,"rebates":null,"gas_usd":null,"infrastructure_usd":null,
            "open_position_value":null,"net_pnl":null,"roi":null})
    }
}

/// Do not use SDK defaults here: absent r/e/to must not deserialize as a free market.
pub fn parse_policy(
    raw: &Value,
    condition: &str,
    up: &str,
    down: &str,
    at: u64,
) -> Result<FeePolicy, String> {
    if raw.get("c").and_then(Value::as_str) != Some(condition) {
        return Err("fee response condition mismatch".into());
    }
    let tokens = raw
        .get("t")
        .and_then(Value::as_array)
        .ok_or("fee tokens missing")?;
    if tokens.len() != 2
        || ![(up, "Up"), (down, "Down")].iter().all(|(id, outcome)| {
            tokens.iter().any(|t| {
                t.get("t").and_then(Value::as_str) == Some(*id)
                    && t.get("o").and_then(Value::as_str) == Some(*outcome)
            })
        })
    {
        return Err("fee token/outcome identity mismatch".into());
    }
    let fd = raw
        .get("fd")
        .and_then(Value::as_object)
        .ok_or("explicit fee details missing")?;
    let rate = match fd.get("r") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) if s.len() <= 32 => s.clone(),
        _ => return Err("explicit fee rate missing".into()),
    };
    let policy = FeePolicy {
        condition_id: condition.into(),
        up_token: up.into(),
        down_token: down.into(),
        rate,
        exponent: fd
            .get("e")
            .and_then(Value::as_u64)
            .and_then(|v| v.try_into().ok())
            .ok_or("fee exponent missing")?,
        taker_only: fd
            .get("to")
            .and_then(Value::as_bool)
            .ok_or("fee role missing")?,
        observed_at_ms: at,
        response_sha256: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(raw).map_err(|_| "fee serialization failed")?)
        ),
    };
    policy.validate()?;
    if !policy.maker_zero() {
        return Err("nonzero maker fee schedule is unsupported; new orders blocked".into());
    }
    Ok(policy)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFee {
    pub order_id: String,
    pub transaction_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub finalized_block_number: u64,
    pub quantity_micros: u64,
    /// V2 OrderFilled.fee, in 6-decimal collateral units; not Trade.fee_rate_bps.
    pub actual_fee_micros: u64,
    pub taker: bool,
}
impl ExecutionFee {
    pub fn key(&self) -> String {
        format!("{}:{}", self.transaction_hash, self.log_index)
    }
}

pub(crate) fn canonical_fee_receipt(
    raw: &Value,
    requested_hash: &str,
    canonical: &Value,
    finalized: u64,
    confirmations: u64,
) -> Result<bool, String> {
    let number = |v: &Value| {
        v.as_str()
            .and_then(|s| s.strip_prefix("0x"))
            .and_then(|s| u64::from_str_radix(s, 16).ok())
            .ok_or("invalid execution fee block number".to_string())
    };
    let block = number(raw.get("blockNumber").ok_or("fee block missing")?)?;
    if confirmations == 0
        || raw.get("transactionHash").and_then(Value::as_str) != Some(requested_hash)
    {
        return Err("fee receipt request identity mismatch".into());
    }
    if finalized
        < block
            .checked_add(confirmations - 1)
            .ok_or("fee confirmation overflow")?
    {
        return Ok(false);
    }
    let hash = raw
        .get("blockHash")
        .and_then(Value::as_str)
        .ok_or("fee block hash missing")?;
    B256::from_str(hash).map_err(|_| "invalid fee block hash")?;
    if canonical.get("hash").and_then(Value::as_str) != Some(hash)
        || number(
            canonical
                .get("number")
                .ok_or("canonical fee height missing")?,
        )? != block
    {
        return Err("execution fee receipt is not canonical".into());
    }
    Ok(true)
}

/// Decode only canonical finalized V2 BUY events from the pinned exchange and owner.
pub(crate) fn decode_receipt(
    raw: &Value,
    exchange: Address,
    owner: Address,
    tracked: &[crate::LiveOrder],
    finalized: u64,
) -> Result<Vec<ExecutionFee>, String> {
    let tx = raw
        .get("transactionHash")
        .and_then(Value::as_str)
        .ok_or("fee receipt hash missing")?;
    B256::from_str(tx).map_err(|_| "invalid fee transaction hash")?;
    let block_hash = raw
        .get("blockHash")
        .and_then(Value::as_str)
        .ok_or("fee block hash missing")?;
    B256::from_str(block_hash).map_err(|_| "invalid fee block hash")?;
    let quantity = |v: &Value| -> Result<u64, String> {
        let s = v
            .as_str()
            .and_then(|s| s.strip_prefix("0x"))
            .ok_or("invalid fee RPC quantity")?;
        u64::from_str_radix(s, 16).map_err(|_| "fee RPC quantity overflow".into())
    };
    let block = quantity(raw.get("blockNumber").ok_or("fee block missing")?)?;
    if quantity(raw.get("status").ok_or("fee receipt status missing")?)? != 1 || finalized < block {
        return Err("fee receipt is not successful and finalized".into());
    }
    let logs = raw
        .get("logs")
        .and_then(Value::as_array)
        .ok_or("fee receipt logs missing")?;
    if logs.len() > 4096 {
        return Err("fee receipt logs bound exceeded".into());
    }
    let filled = format!(
        "{:#x}",
        keccak256(
            "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
        )
    );
    let matched = format!(
        "{:#x}",
        keccak256("OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)")
    );
    let mut takers = std::collections::BTreeSet::new();
    for log in logs {
        if log
            .get("address")
            .and_then(Value::as_str)
            .and_then(|s| Address::from_str(s).ok())
            != Some(exchange)
        {
            continue;
        }
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .ok_or("fee event topics missing")?;
        if topics.first().and_then(Value::as_str) == Some(matched.as_str()) {
            if topics.len() != 3 {
                return Err("invalid V2 matched event".into());
            }
            takers.insert(
                topics[1]
                    .as_str()
                    .ok_or("matched hash missing")?
                    .to_ascii_lowercase(),
            );
        }
    }
    let mut result = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for log in logs {
        if log
            .get("address")
            .and_then(Value::as_str)
            .and_then(|s| Address::from_str(s).ok())
            != Some(exchange)
        {
            continue;
        }
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .ok_or("fee event topics missing")?;
        if topics.first().and_then(Value::as_str) != Some(filled.as_str()) {
            continue;
        }
        if topics.len() != 4 {
            return Err("invalid V2 fill topics".into());
        }
        let order_id = topics[1]
            .as_str()
            .ok_or("fee order hash missing")?
            .to_ascii_lowercase();
        let Some(order) = tracked.iter().find(|o| o.order_id == order_id) else {
            continue;
        };
        let maker = B256::from_str(topics[2].as_str().ok_or("fee owner missing")?)
            .map_err(|_| "invalid fee owner")?;
        if maker.as_slice()[..12] != [0; 12] || maker.as_slice()[12..] != owner.as_slice()[..] {
            return Err("fee fill owner mismatch".into());
        }
        if log.get("removed").and_then(Value::as_bool) == Some(true)
            || log.get("transactionHash").and_then(Value::as_str) != Some(tx)
            || log.get("blockHash").and_then(Value::as_str) != Some(block_hash)
            || quantity(log.get("blockNumber").ok_or("fee log block missing")?)? != block
        {
            return Err("fee log provenance mismatch".into());
        }
        let data = log
            .get("data")
            .and_then(Value::as_str)
            .and_then(|s| s.strip_prefix("0x"))
            .ok_or("fee log data missing")?;
        if data.len() != 448 || !data.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("unsupported V2 fill ABI".into());
        }
        let word = |i: usize| {
            U256::from_str_radix(&data[i * 64..(i + 1) * 64], 16)
                .map_err(|_| "invalid V2 fill word".to_string())
        };
        if word(0)? != U256::ZERO || word(1)?.to_string() != order.quote.token_id {
            return Err("fee fill token/side mismatch".into());
        }
        let qty = u64::try_from(word(3)?).map_err(|_| "fee filled quantity overflow")?;
        let collateral = u64::try_from(word(2)?).map_err(|_| "fee collateral overflow")?;
        if qty == 0
            || collateral == 0
            || qty > order.quote.quantity_micros
            || u128::from(collateral) * 1_000_000
                > (u128::from(qty) + 1) * u128::from(order.quote.price_micros) - 1
            || word(5)? != U256::ZERO
        {
            return Err("fee fill quantities contradict signed order".into());
        }
        let index = quantity(log.get("logIndex").ok_or("fee log index missing")?)?;
        if !seen.insert(index) {
            return Err("duplicate fee log index".into());
        }
        result.push(ExecutionFee {
            order_id,
            transaction_hash: tx.into(),
            log_index: index,
            block_number: block,
            block_hash: block_hash.into(),
            finalized_block_number: finalized,
            quantity_micros: qty,
            actual_fee_micros: u64::try_from(word(4)?).map_err(|_| "actual fee overflow")?,
            taker: takers.contains(&order.order_id),
        });
    }
    Ok(result)
}

pub(crate) fn mismatched(
    policies: &BTreeMap<String, FeePolicy>,
    proofs: &BTreeMap<String, ExecutionFee>,
) -> bool {
    proofs.values().any(|proof| {
        proof.actual_fee_micros != 0
            || proof.taker
            || !policies
                .get(&proof.order_id)
                .is_some_and(FeePolicy::maker_zero)
    })
}

pub(crate) fn summary(
    orders: &BTreeMap<String, crate::LiveOrder>,
    policies: &BTreeMap<String, FeePolicy>,
    proofs: &BTreeMap<String, ExecutionFee>,
) -> Value {
    let mut verified = 0u128;
    let mut actual = 0u128;
    let mismatch = mismatched(policies, proofs);
    for proof in proofs.values() {
        verified += u128::from(proof.quantity_micros);
        actual += u128::from(proof.actual_fee_micros);
    }
    let filled: u128 = orders
        .values()
        .map(|o| u128::from(o.cumulative_filled_micros))
        .sum();
    let complete = filled == verified && !mismatch;
    json!({"status":if mismatch {"MISMATCH"} else if filled==0 {"NO_FILLS"} else if complete {"MATCH"} else {"UNKNOWN_PENDING_FINALIZED_RECEIPTS"},
        "filled_quantity_micros":filled.to_string(),"verified_quantity_micros":verified.to_string(),
        "expected_exchange_fee_micros":if orders.values().all(|o|o.cumulative_filled_micros==0 || policies.contains_key(&o.order_id)) {Some("0")}else{None},
        "actual_exchange_fee_micros":if complete && filled>0 {Some(actual.to_string())}else{None},
        "observed_actual_fee_micros":actual.to_string(),"proof_count":proofs.len(),
        "net_pnl":null,"roi":null,"scope":"exchange OrderFilled fee only; gas, rebates, valuation and infrastructure remain separate"})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> FeePolicy {
        let id = format!("0x{}", "a".repeat(64));
        parse_policy(
            &json!({"c":id,"t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],
            "fd":{"r":0.07,"e":1,"to":true}}),
            &id,
            "1",
            "2",
            1000,
        )
        .unwrap()
    }
    fn order() -> crate::LiveOrder {
        crate::LiveOrder {
            order_id: format!("0x{}", "b".repeat(64)),
            quote: crate::LiveQuote {
                slot: "up_bid".into(),
                condition_id: policy().condition_id,
                market_slug: "btc-updown-5m-0".into(),
                token_id: "1".into(),
                outcome: mm2_core::Outcome::Up,
                price_micros: 500_000,
                quantity_micros: 5_000_000,
                window_start_ms: 0,
                window_end_ms: 300_000,
                tick_micros: 1000,
                min_order_micros: 5_000_000,
                fee_bps: 0,
                protocol: 2,
                neg_risk: false,
            },
            phase: crate::OrderPhase::Closed,
            cumulative_filled_micros: 5_000_000,
            created_at_ms: 1000,
            last_operation_ms: 1000,
            retry_at_ms: 1000,
        }
    }
    fn receipt(fee: u64) -> Value {
        let tx = format!("0x{}", "c".repeat(64));
        let block = format!("0x{}", "d".repeat(64));
        json!({"transactionHash":tx,"blockNumber":"0x10","blockHash":block,"status":"0x1",
            "logs":[{"address":format!("{:#x}",Address::repeat_byte(1)),"transactionHash":tx,"blockHash":block,
                "blockNumber":"0x10","logIndex":"0x2","removed":false,
                "topics":[format!("{:#x}",keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")),
                    order().order_id,format!("0x{:0>64}",hex::encode(Address::repeat_byte(2))),format!("0x{}","0".repeat(64))],
                "data":format!("0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",0,1,2_500_000,5_000_000,fee,0,0)}]})
    }
    #[test]
    fn missing_fee_fields_cannot_become_zero() {
        let p = policy();
        let raw = json!({"c":p.condition_id,"t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}});
        for key in ["r", "e", "to"] {
            let mut bad = raw.clone();
            bad["fd"].as_object_mut().unwrap().remove(key);
            assert!(parse_policy(&bad, &p.condition_id, "1", "2", 1000).is_err());
        }
        let mut bad = raw.clone();
        bad["fd"]["r"] = json!(-0.07);
        assert!(parse_policy(&bad, &p.condition_id, "1", "2", 1000).is_err());
        let mut bad = raw;
        bad["fd"]["to"] = json!(false);
        assert!(parse_policy(&bad, &p.condition_id, "1", "2", 1000).is_err());
    }
    #[test]
    fn fee_identity_and_expiry_are_exact() {
        let p = policy();
        assert!(p.permits(&p.condition_id, "1", "2", 1000));
        assert!(p.permits(&p.condition_id, "1", "2", 90_999));
        assert!(!p.permits(&p.condition_id, "1", "2", 91_000));
        assert!(!p.permits(&p.condition_id, "2", "1", 1000));
        assert!(!p.permits(&p.condition_id, "1", "2", 999));
    }
    #[test]
    fn canonical_finality_rejects_reorg_and_wrong_transaction() {
        let raw = receipt(0);
        let tx = raw["transactionHash"].as_str().unwrap();
        let canonical = json!({"number":"0x10","hash":raw["blockHash"]});
        assert!(canonical_fee_receipt(&raw, tx, &canonical, 27, 12).unwrap());
        assert!(!canonical_fee_receipt(&raw, tx, &canonical, 26, 12).unwrap());
        assert!(
            canonical_fee_receipt(&raw, &format!("0x{}", "e".repeat(64)), &canonical, 27, 12)
                .is_err()
        );
        let reorg = json!({"number":"0x10","hash":format!("0x{}","e".repeat(64))});
        assert!(canonical_fee_receipt(&raw, tx, &reorg, 27, 12).is_err());
    }
    #[test]
    fn finalized_fees_match_deduplicate_and_preserve_mismatch() {
        let mut ledger = crate::model::Ledger::default();
        let o = order();
        ledger.orders.insert(o.order_id.clone(), o.clone());
        ledger.fee_policies.insert(o.order_id.clone(), policy());
        assert!(ledger.fees_pending());
        assert!(ledger.fee_summary()["actual_exchange_fee_micros"].is_null());
        let proof = decode_receipt(
            &receipt(0),
            Address::repeat_byte(1),
            Address::repeat_byte(2),
            std::slice::from_ref(&o),
            32,
        )
        .unwrap()
        .remove(0);
        ledger.observe_fee(proof.clone()).unwrap();
        ledger.observe_fee(proof).unwrap();
        assert_eq!(ledger.execution_fees.len(), 1);
        assert!(!ledger.fees_pending());
        assert_eq!(ledger.fee_summary()["status"], "MATCH");
        let mut mismatch = crate::model::Ledger::default();
        mismatch.orders.insert(o.order_id.clone(), o.clone());
        mismatch.fee_policies.insert(o.order_id.clone(), policy());
        let proof = decode_receipt(
            &receipt(123),
            Address::repeat_byte(1),
            Address::repeat_byte(2),
            &[o],
            32,
        )
        .unwrap()
        .remove(0);
        mismatch.observe_fee(proof).unwrap();
        assert_eq!(mismatch.phase, crate::LivePhase::Faulted);
        assert_eq!(mismatch.fee_summary()["status"], "MISMATCH");
        assert_eq!(mismatch.execution_fees.len(), 1);
    }
    #[test]
    fn tampered_fee_receipts_fail_closed() {
        for change in [
            "owner", "side", "token", "removed", "tx", "quantity", "unicode", "status",
        ] {
            let mut raw = receipt(0);
            match change {
                "owner" => raw["logs"][0]["topics"][2] = json!(format!("0x{}", "0".repeat(64))),
                "side" => {
                    raw["logs"][0]["data"] = json!(format!(
                        "0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",
                        1, 1, 2_500_000, 5_000_000, 0, 0, 0
                    ))
                }
                "token" => {
                    raw["logs"][0]["data"] = json!(format!(
                        "0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",
                        0, 2, 2_500_000, 5_000_000, 0, 0, 0
                    ))
                }
                "removed" => raw["logs"][0]["removed"] = json!(true),
                "tx" => raw["logs"][0]["transactionHash"] = json!(format!("0x{}", "e".repeat(64))),
                "quantity" => {
                    raw["logs"][0]["data"] = json!(format!(
                        "0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",
                        0, 1, 2_500_000, 6_000_000, 0, 0, 0
                    ))
                }
                "unicode" => raw["logs"][0]["data"] = json!(format!("0x{}", "é".repeat(224))),
                _ => raw["status"] = json!("0x0"),
            }
            assert!(
                decode_receipt(
                    &raw,
                    Address::repeat_byte(1),
                    Address::repeat_byte(2),
                    &[order()],
                    32
                )
                .is_err(),
                "{change}"
            );
        }
        assert!(
            decode_receipt(
                &receipt(0),
                Address::repeat_byte(1),
                Address::repeat_byte(2),
                &[order()],
                15
            )
            .is_err()
        );
    }
}
