//! Read-only final outcomes for synthetic positions. No signer, wallet or broadcast path.
//! Market prices and Gamma's proposed winner never credit the paper capital ledger.
use crate::{discovery, runtime};
use alloy::{
    primitives::{Address, B256, U256, address},
    sol,
    sol_types::SolCall,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, time::Duration};
use tokio::sync::watch;

const RPC: &str = "https://polygon-bor-rpc.publicnode.com";
pub const CTF: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");
const COLLATERAL: Address = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
const MAX_BODY: usize = 262_144;
const MAX_TARGETS: usize = 64;
const PER_PASS: usize = 8;

sol! {
    interface ConditionalTokens {
        function getOutcomeSlotCount(bytes32 conditionId) external view returns (uint256);
        function getCollectionId(bytes32 parent, bytes32 conditionId, uint256 indexSet) external view returns (bytes32);
        function getPositionId(address collateralToken, bytes32 collectionId) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub condition_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
}
impl Target {
    fn validate(&self) -> Result<(), String> {
        if self.end_ms.checked_sub(self.start_ms) != Some(300_000)
            || !self.start_ms.is_multiple_of(300_000)
            || !canonical_hash(&self.condition_id)
        {
            return Err("unsupported paper resolution target".into());
        }
        Ok(())
    }
}

/// A decoded, checked finalized state observation, never a real redemption receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub target: Target,
    pub chain_id: u64,
    pub conditional_tokens: String,
    /// CTF index set 1 (Up), then 2 (Down), independently derived on chain.
    pub token_ids: [String; 2],
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp_ms: u64,
    pub observed_at_ms: u64,
    pub payout_numerators: [u64; 2],
    pub payout_denominator: u64,
    pub metadata_sha256: String,
}
fn canonical_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value[2..].bytes().any(|b| b != b'0')
}
impl Evidence {
    pub fn winner(&self) -> Result<mm2_core::Outcome, String> {
        self.target.validate()?;
        let d = self.payout_denominator;
        if self.chain_id != 137
            || self.conditional_tokens != format!("{CTF:#x}")
            || self.block_number == 0
            || !canonical_hash(&self.block_hash)
            || self.block_timestamp_ms < self.target.end_ms
            || self.observed_at_ms < self.block_timestamp_ms
            || self.metadata_sha256.len() != 64
            || !self.metadata_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || self.token_ids[0] == self.token_ids[1]
            || self.token_ids.iter().any(|s| {
                s.len() > 78
                    || !s.bytes().all(|b| b.is_ascii_digit())
                    || U256::from_str(s).is_err()
                    || s.starts_with('0')
            })
            || d == 0
        {
            return Err("invalid finalized public outcome evidence".into());
        }
        match self.payout_numerators {
            [a, 0] if a == d => Ok(mm2_core::Outcome::Up),
            [0, b] if b == d => Ok(mm2_core::Outcome::Down),
            _ => Err("unsupported non-binary CTF payout; positions retained".into()),
        }
    }
}

fn quantity(value: &Value) -> Result<u64, String> {
    let raw = value
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or("invalid RPC quantity")?;
    if raw.is_empty() || raw.len() > 16 || (raw.len() > 1 && raw.starts_with('0')) {
        return Err("RPC quantity exceeds bounds".into());
    }
    u64::from_str_radix(raw, 16).map_err(|_| "invalid RPC quantity".into())
}
fn hash(value: &Value) -> Result<String, String> {
    let raw = value.as_str().ok_or("missing canonical block hash")?;
    if !canonical_hash(raw) {
        return Err("invalid canonical block hash".into());
    }
    Ok(raw.into())
}
fn word(value: Value) -> Result<U256, String> {
    let raw = value
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or("missing ABI result")?;
    if raw.len() != 64 {
        return Err("invalid scalar ABI result".into());
    }
    let bytes = hex::decode(raw).map_err(|_| "invalid scalar ABI result")?;
    Ok(U256::from_be_slice(&bytes))
}
fn tokens(raw: &Value, target: &Target) -> Result<[String; 2], String> {
    // Reuse the strict discovery contract with only the already-closed status normalized.
    // End prices/umaResolutionStatus are deliberately ignored.
    if raw["negRisk"] != false {
        return Err("unsupported negative-risk resolution".into());
    }
    let mut identity = raw.clone();
    identity["active"] = json!(true);
    identity["closed"] = json!(false);
    identity["acceptingOrders"] = json!(true);
    let market = discovery::parse_market(&identity, target.start_ms)?;
    if market.condition_id != target.condition_id || market.window_end_ms != target.end_ms {
        return Err("resolution metadata condition/window mismatch".into());
    }
    Ok([market.up_token, market.down_token])
}

struct Client {
    http: reqwest::Client,
    rpc_url: String,
    gamma: String,
}
impl Client {
    fn public() -> Result<Self, String> {
        Ok(Self {
            http: discovery::client()?,
            rpc_url: RPC.into(),
            gamma: "https://gamma-api.polymarket.com".into(),
        })
    }
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        let mut response = self
            .http
            .post(&self.rpc_url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| "public resolution RPC unavailable")?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > MAX_BODY as u64)
        {
            return Err("public resolution RPC response rejected".into());
        }
        let mut bytes = Vec::new();
        while let Some(part) = response
            .chunk()
            .await
            .map_err(|_| "public resolution body incomplete")?
        {
            if bytes.len() + part.len() > MAX_BODY {
                return Err("public resolution body exceeds bounds".into());
            }
            bytes.extend_from_slice(&part);
        }
        let raw: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid public resolution RPC JSON")?;
        if raw["jsonrpc"] != "2.0"
            || raw["id"] != 1
            || raw.get("error").is_some()
            || raw.get("result").is_none()
        {
            return Err("public resolution RPC rejected request".into());
        }
        Ok(raw["result"].clone())
    }
    async fn call(&self, data: Vec<u8>, block: &Value) -> Result<U256, String> {
        word(
            self.rpc(
                "eth_call",
                json!([{"to":format!("{CTF:#x}"),"data":format!("0x{}",hex::encode(data))},block]),
            )
            .await?,
        )
    }
    async fn observe(&self, target: &Target) -> Result<Option<Evidence>, String> {
        target.validate()?;
        let raw = discovery::get(
            &self.http,
            &format!(
                "{}/markets/slug/btc-updown-5m-{}",
                self.gamma,
                target.start_ms / 1000
            ),
        )
        .await?;
        let token_ids = tokens(&raw, target)?;
        if quantity(&self.rpc("eth_chainId", json!([])).await?)? != 137 {
            return Err("resolution RPC chain mismatch".into());
        }
        let block = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let number = quantity(&block["number"])?;
        let block_hash = hash(&block["hash"])?;
        let timestamp = quantity(&block["timestamp"])?
            .checked_mul(1000)
            .ok_or("block time overflow")?;
        if timestamp < target.end_ms {
            return Ok(None);
        }
        // EIP-1898 pins every call to this one canonical finalized block.
        let tag = json!({"blockHash":block_hash,"requireCanonical":true});
        let condition =
            B256::from_str(&target.condition_id).map_err(|_| "invalid resolution condition")?;
        let slots = self
            .call(
                ConditionalTokens::getOutcomeSlotCountCall {
                    conditionId: condition,
                }
                .abi_encode(),
                &tag,
            )
            .await?;
        if slots != U256::from(2) {
            return Err("resolution requires existing binary CTF condition".into());
        }
        let denominator = self
            .call(
                ConditionalTokens::payoutDenominatorCall {
                    conditionId: condition,
                }
                .abi_encode(),
                &tag,
            )
            .await?;
        if denominator == U256::ZERO {
            return Ok(None);
        }
        let mut numerators = [0u64; 2];
        for i in 0..2 {
            let collection = self
                .call(
                    ConditionalTokens::getCollectionIdCall {
                        parent: B256::ZERO,
                        conditionId: condition,
                        indexSet: U256::from(i + 1),
                    }
                    .abi_encode(),
                    &tag,
                )
                .await?;
            let derived = self
                .call(
                    ConditionalTokens::getPositionIdCall {
                        collateralToken: COLLATERAL,
                        collectionId: B256::from(collection.to_be_bytes::<32>()),
                    }
                    .abi_encode(),
                    &tag,
                )
                .await?;
            if derived.to_string() != token_ids[i] {
                return Err("CTF outcome token mapping mismatch".into());
            }
            numerators[i] = u64::try_from(
                self.call(
                    ConditionalTokens::payoutNumeratorsCall {
                        conditionId: condition,
                        index: U256::from(i),
                    }
                    .abi_encode(),
                    &tag,
                )
                .await?,
            )
            .map_err(|_| "payout numerator overflow")?;
        }
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        if hash(&canonical["hash"])? != block_hash || quantity(&canonical["number"])? != number {
            return Err("resolution block changed during observation".into());
        }
        let evidence = Evidence {
            target: target.clone(),
            chain_id: 137,
            conditional_tokens: format!("{CTF:#x}"),
            token_ids,
            block_number: number,
            block_hash,
            block_timestamp_ms: timestamp,
            observed_at_ms: runtime::wall_ms(),
            payout_numerators: numerators,
            payout_denominator: u64::try_from(denominator)
                .map_err(|_| "payout denominator overflow")?,
            metadata_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(&raw).map_err(|_| "metadata encoding failed")?,
            )),
        };
        evidence.winner()?;
        Ok(Some(evidence))
    }
}

/// Bounded independent I/O; actor commits evidence before changing virtual cash.
pub async fn observe(target: &Target) -> Result<Option<Evidence>, String> {
    Client::public()?.observe(target).await
}

/// Bounded independent I/O; actor commits evidence before changing virtual cash.
pub async fn run(handle: runtime::Handle, mut shutdown: watch::Receiver<bool>) {
    let Ok(client) = Client::public() else {
        return;
    };
    let mut timer = tokio::time::interval(Duration::from_secs(30));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = 0usize;
    loop {
        tokio::select! { biased; _=shutdown.changed()=>return, _=timer.tick()=>{} }
        let pass = async {
            let targets = handle.resolution_targets().await?;
            if targets.len() > MAX_TARGETS {
                return Err("paper resolution target capacity exceeded".into());
            }
            if targets.is_empty() {
                return Ok(());
            }
            for offset in 0..PER_PASS.min(targets.len()) {
                let target = &targets[(cursor + offset) % targets.len()];
                match tokio::time::timeout(Duration::from_secs(20), client.observe(target)).await {
                    Ok(Ok(Some(evidence))) => {
                        handle.resolve_paper(evidence).await?;
                    }
                    Ok(Ok(None)) => {}
                    _ => {
                        tracing::warn!("paper outcome unverified; position and capital retained");
                    }
                }
            }
            cursor = (cursor + PER_PASS) % targets.len();
            Ok::<(), String>(())
        };
        tokio::select! { biased; _=shutdown.changed()=>return, result=pass=>{
            if result.is_err() { tracing::warn!("paper resolution admission unavailable; positions retained"); }
        }}
    }
}

#[cfg(test)]
pub(crate) mod tests;
