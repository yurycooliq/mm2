//! Direct, signer-paid Polygon closeout. Preparation never broadcasts; callers must durably
//! persist the complete prepared transaction before calling `submit`. RPC failures never create
//! a replacement transaction. Only finalized canonical receipts establish an economic outcome.
use crate::{
    LiveSettings,
    settings::{address, condition},
};
use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxEnvelope},
    eips::eip2718::{Decodable2718, Encodable2718},
    primitives::{Address, B256, Bytes, TxKind, U256, address as literal_address, keccak256},
    signers::{Signer, local::PrivateKeySigner},
    sol,
    sol_types::SolCall,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{str::FromStr, time::Duration};

#[path = "relay.rs"]
pub mod relay;

pub const STANDARD_ADAPTER: Address =
    literal_address!("0xAdA100Db00Ca00073811820692005400218FcE1f");
/// E111's mint/merge adapter is distinct from the approved public closeout adapter.
pub const EXCHANGE_OUTCOME_FACTORY: Address =
    literal_address!("0xada100874d00e3331d00f2007a9c336a65009718");
const CTF_UNDERLYING_USDCE: Address =
    literal_address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
/// These operator-run contracts can redeem a user's whole binary CTF position without
/// advancing the user's wallet nonce. Their approvals conflict with the sole-writer ledger.
pub const EXTERNAL_AUTO_REDEEMERS: [Address; 2] = [
    literal_address!("0xa1200000d0002264c9a1698e001292d00e1b00af"),
    literal_address!("0xf3cfb6a6ebfeb51876289eb235719eb1c65252b0"),
];
const MAX_RPC_BYTES: usize = 1024 * 1024;
const MAX_RAW_BYTES: usize = 16 * 1024;

sol! {
    interface Adapter {
        function COLLATERAL_TOKEN() external view returns (address);
        function CONDITIONAL_TOKENS() external view returns (address);
        function USDCE() external view returns (address);
        function mergePositions(address collateral, bytes32 parent, bytes32 conditionId, uint256[] partition, uint256 amount) external;
        function redeemPositions(address collateral, bytes32 parent, bytes32 conditionId, uint256[] indexSets) external;
    }
    interface Exchange {
        function getCollateral() external view returns (address);
        function getCtf() external view returns (address);
        function getCtfCollateral() external view returns (address);
        function getOutcomeTokenFactory() external view returns (address);
    }
    interface Token {
        function balanceOf(address owner) external view returns (uint256);
    }
    interface Ctf {
        function balanceOf(address owner, uint256 id) external view returns (uint256);
        function isApprovedForAll(address owner, address operator) external view returns (bool);
        function getOutcomeSlotCount(bytes32 conditionId) external view returns (uint256);
        function getCollectionId(bytes32 parent, bytes32 conditionId, uint256 indexSet) external view returns (bytes32);
        function getPositionId(address collateral, bytes32 collectionId) external view returns (uint256);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
    }
    struct ProxyCall { uint8 typeCode; address to; uint256 value; bytes data; }
    interface ProxyFactory {
        function getImplementation() external view returns (address);
        function proxy(ProxyCall[] calls) external payable returns (bytes[]);
    }
    interface Safe {
        function getThreshold() external view returns (uint256);
        function getOwners() external view returns (address[]);
        function nonce() external view returns (uint256);
        function getTransactionHash(address to, uint256 value, bytes data, uint8 operation, uint256 safeTxGas, uint256 baseGas, uint256 gasPrice, address gasToken, address refundReceiver, uint256 nonce) external view returns (bytes32);
        function execTransaction(address to, uint256 value, bytes data, uint8 operation, uint256 safeTxGas, uint256 baseGas, uint256 gasPrice, address gasToken, address refundReceiver, bytes signatures) external payable returns (bool);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainSettings {
    pub rpc_env: String,
    pub adapter_address: String,
    pub max_gas_limit: u64,
    pub max_fee_per_gas_wei: u64,
    pub max_priority_fee_per_gas_wei: u64,
    pub max_transaction_cost_wei: String,
    pub max_session_gas_cost_wei: String,
    pub minimum_confirmations: u64,
    pub request_timeout_ms: u64,
    pub relayer: Option<relay::RelaySettings>,
}
impl Default for ChainSettings {
    fn default() -> Self {
        Self {
            rpc_env: "MM2_POLYGON_RPC_URL".into(),
            adapter_address: format!("{STANDARD_ADAPTER:#x}"),
            max_gas_limit: 1_000_000,
            max_fee_per_gas_wei: 100_000_000_000,
            max_priority_fee_per_gas_wei: 30_000_000_000,
            max_transaction_cost_wei: "100000000000000000".into(),
            max_session_gas_cost_wei: "1000000000000000000".into(),
            minimum_confirmations: 12,
            request_timeout_ms: 3000,
            relayer: None,
        }
    }
}
impl ChainSettings {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(relayer) = &self.relayer {
            relayer.validate()?;
        }
        if self.rpc_env.is_empty()
            || self.rpc_env.len() > 128
            || !self
                .rpc_env
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            || address(&self.adapter_address)? != STANDARD_ADAPTER
            || !(100_000..=5_000_000).contains(&self.max_gas_limit)
            || !(1..=1_000_000_000_000).contains(&self.max_fee_per_gas_wei)
            || self.max_priority_fee_per_gas_wei == 0
            || self.max_priority_fee_per_gas_wei > self.max_fee_per_gas_wei
            || !(12..=256).contains(&self.minimum_confirmations)
            || !(100..=30_000).contains(&self.request_timeout_ms)
        {
            return Err("invalid finite Polygon closeout settings".into());
        }
        let cost = decimal_u128(&self.max_transaction_cost_wei)?;
        if cost == 0 || cost > 1_000_000_000_000_000_000 {
            return Err("invalid finite closeout gas cost cap".into());
        }
        let session = decimal_u128(&self.max_session_gas_cost_wei)?;
        if session < cost || session > 10_000_000_000_000_000_000 {
            return Err("invalid finite session gas cost cap".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChainOperation {
    Merge { amount_micros: u64 },
    Redeem,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainPlan {
    pub id: String,
    pub condition_id: String,
    /// CTF index-set 1, then index-set 2. Caller must resolve the market's outcome mapping.
    pub token_ids: [String; 2],
    pub operation: ChainOperation,
}
impl ChainPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty()
            || self.id.len() > 128
            || !self
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_:.".contains(&b))
            || !condition(&self.condition_id)
            || self.token_ids.iter().all(String::is_empty)
            || (!self.token_ids[0].is_empty() && self.token_ids[0] == self.token_ids[1])
        {
            return Err("invalid bounded closeout plan identity".into());
        }
        for token in &self.token_ids {
            if !token.is_empty() {
                token_id(token)?;
            }
        }
        if let ChainOperation::Merge { amount_micros } = self.operation
            && (amount_micros == 0 || amount_micros > 1_000_000_000_000_000)
        {
            return Err("invalid merge amount".into());
        }
        Ok(())
    }
}
/// Contains signed private execution material. Never place this object in UI or logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedChainTransaction {
    pub plan: ChainPlan,
    pub hash: String,
    pub raw_transaction: String,
    pub nonce: u64,
    pub safe_nonce: Option<u64>,
    pub safe_transaction_hash: Option<String>,
    pub signer_address: String,
    pub funder_address: String,
    pub before_tokens_micros: [u64; 2],
    pub before_collateral_micros: u64,
    pub expected_payout_micros: u64,
    pub payout_numerators: [u64; 2],
    pub payout_denominator: u64,
    pub gas_limit: u64,
    pub max_fee_per_gas_wei: u64,
    pub max_priority_fee_per_gas_wei: u64,
    pub maximum_gas_cost_wei: String,
    pub prepared_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relayer: Option<relay::PreparedRelay>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainReceipt {
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_hash: String,
    pub finalized_block_number: u64,
    pub success: bool,
    pub after_tokens_micros: [u64; 2],
    pub after_collateral_micros: u64,
    pub collateral_delta_micros: u64,
    pub consumed_tokens_micros: [u64; 2],
    pub payout_numerators: [u64; 2],
    pub payout_denominator: u64,
    pub gas_cost_wei: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sponsored_gas_cost_wei: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ChainObservation {
    Unknown,
    Finalized(ChainReceipt),
    Expired(relay::RelayExpiry),
}

pub struct DirectChain {
    live: LiveSettings,
    settings: ChainSettings,
    url: String,
    signer: PrivateKeySigner,
    http: reqwest::Client,
    relayer_transport: Option<relay::RelayTransport>,
}
impl DirectChain {
    pub async fn from_env(live: &LiveSettings, settings: &ChainSettings) -> Result<Self, String> {
        live.validate()?;
        settings.validate()?;
        let url = std::env::var(&settings.rpc_env)
            .map_err(|_| "Polygon RPC environment value missing")?;
        validate_rpc_url(&url, false)?;
        let key = std::env::var(&live.private_key_env)
            .map_err(|_| "authorized signer environment value missing")?;
        let signer = PrivateKeySigner::from_str(&key).map_err(|_| "invalid authorized signer")?;
        let mut client = Self::new(live.clone(), settings.clone(), url, signer, false)?;
        if live.signature_type == 3 {
            client.relayer_transport = Some(relay::RelayTransport::from_env(
                settings
                    .relayer
                    .as_ref()
                    .ok_or("DepositWallet relayer configuration missing")?,
                client.signer.address(),
                settings.request_timeout_ms,
            )?);
        }
        Ok(client)
    }
    fn new(
        live: LiveSettings,
        settings: ChainSettings,
        url: String,
        signer: PrivateKeySigner,
        test_url: bool,
    ) -> Result<Self, String> {
        live.validate()?;
        settings.validate()?;
        validate_rpc_url(&url, test_url)?;
        if signer.address() != address(&live.signer_address)? {
            return Err("closeout signer identity mismatch".into());
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(settings.request_timeout_ms))
            .build()
            .map_err(|_| "cannot create bounded Polygon RPC transport")?;
        Ok(Self {
            live,
            settings,
            url,
            signer,
            http,
            relayer_transport: None,
        })
    }
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        let mut response = self
            .http
            .post(&self.url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| "Polygon RPC request outcome unknown")?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > MAX_RPC_BYTES as u64)
        {
            return Err("Polygon RPC response rejected".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "Polygon RPC response incomplete")?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RPC_BYTES {
                return Err("Polygon RPC response capacity exceeded".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid Polygon RPC response")?;
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || value.get("id") != Some(&json!(1))
            || value.get("error").is_some()
            || value.get("result").is_none()
        {
            return Err("Polygon RPC request rejected; reconcile original identity".into());
        }
        Ok(value["result"].clone())
    }
    async fn call(&self, to: Address, data: Vec<u8>, block: &str) -> Result<Vec<u8>, String> {
        let v = self
            .rpc(
                "eth_call",
                json!([{"to":format!("{to:#x}"),"data":hex_data(&data)},block]),
            )
            .await?;
        hex_bytes(
            v.as_str().ok_or("invalid Polygon call result")?,
            MAX_RPC_BYTES,
        )
    }
    async fn uint_call(&self, to: Address, data: Vec<u8>, block: &str) -> Result<U256, String> {
        let bytes = self.call(to, data, block).await?;
        if bytes.len() != 32 {
            return Err("invalid Polygon scalar ABI result".into());
        }
        Ok(U256::from_be_slice(&bytes))
    }
    async fn address_call(
        &self,
        to: Address,
        data: Vec<u8>,
        block: &str,
    ) -> Result<Address, String> {
        let bytes = self.call(to, data, block).await?;
        if bytes.len() != 32 || bytes[..12].iter().any(|b| *b != 0) {
            return Err("invalid Polygon address ABI result".into());
        }
        Ok(Address::from_slice(&bytes[12..]))
    }
    async fn balances(&self, plan: &ChainPlan, block: &str) -> Result<([u64; 2], u64), String> {
        let ctf = polymarket_client_sdk_v2::contract_config(137, false)
            .ok_or("missing pinned CTF")?
            .conditional_tokens;
        let funder = address(&self.live.funder_address)?;
        let collateral = address(&self.live.collateral_address)?;
        let (a, b, c) = tokio::try_join!(
            self.uint_call(
                ctf,
                Ctf::balanceOfCall {
                    owner: funder,
                    id: token_id(&plan.token_ids[0])?
                }
                .abi_encode(),
                block
            ),
            self.uint_call(
                ctf,
                Ctf::balanceOfCall {
                    owner: funder,
                    id: token_id(&plan.token_ids[1])?
                }
                .abi_encode(),
                block
            ),
            self.uint_call(
                collateral,
                Token::balanceOfCall { owner: funder }.abi_encode(),
                block
            )
        )?;
        Ok(([bounded_u64(a)?, bounded_u64(b)?], bounded_u64(c)?))
    }
    async fn deployment_identity(&self, block: &str) -> Result<(Address, Address), String> {
        let exchange = address(&self.live.exchange_address)?;
        let collateral = address(&self.live.collateral_address)?;
        let ctf = polymarket_client_sdk_v2::contract_config(137, false)
            .ok_or("missing pinned CTF")?
            .conditional_tokens;
        let (ec, et, ea, eu, ac, at, underlying, fc, ft, fu) = tokio::try_join!(
            self.address_call(exchange, Exchange::getCollateralCall {}.abi_encode(), block),
            self.address_call(exchange, Exchange::getCtfCall {}.abi_encode(), block),
            self.address_call(
                exchange,
                Exchange::getOutcomeTokenFactoryCall {}.abi_encode(),
                block
            ),
            self.address_call(
                exchange,
                Exchange::getCtfCollateralCall {}.abi_encode(),
                block
            ),
            self.address_call(
                STANDARD_ADAPTER,
                Adapter::COLLATERAL_TOKENCall {}.abi_encode(),
                block
            ),
            self.address_call(
                STANDARD_ADAPTER,
                Adapter::CONDITIONAL_TOKENSCall {}.abi_encode(),
                block
            ),
            self.address_call(STANDARD_ADAPTER, Adapter::USDCECall {}.abi_encode(), block),
            self.address_call(
                EXCHANGE_OUTCOME_FACTORY,
                Adapter::COLLATERAL_TOKENCall {}.abi_encode(),
                block
            ),
            self.address_call(
                EXCHANGE_OUTCOME_FACTORY,
                Adapter::CONDITIONAL_TOKENSCall {}.abi_encode(),
                block
            ),
            self.address_call(
                EXCHANGE_OUTCOME_FACTORY,
                Adapter::USDCECall {}.abi_encode(),
                block
            )
        )?;
        if ec != collateral
            || et != ctf
            || ea != EXCHANGE_OUTCOME_FACTORY
            || eu != CTF_UNDERLYING_USDCE
            || ac != collateral
            || at != ctf
            || underlying != CTF_UNDERLYING_USDCE
            || fc != collateral
            || ft != ctf
            || fu != CTF_UNDERLYING_USDCE
        {
            return Err("Polygon exchange/adapter deployment identity mismatch".into());
        }
        let approved = self
            .uint_call(
                ctf,
                Ctf::isApprovedForAllCall {
                    owner: address(&self.live.funder_address)?,
                    operator: STANDARD_ADAPTER,
                }
                .abi_encode(),
                block,
            )
            .await?;
        if approved != U256::from(1) {
            return Err("closeout adapter ERC1155 approval missing; owner setup required".into());
        }
        Ok((ctf, underlying))
    }
    /// Read-only sole-writer guard. No approval is changed here; the owner must either
    /// revoke the competing authority or use a separately supported external-receipt path.
    pub(crate) async fn verify_no_external_redeemer(&self, block: &str) -> Result<(), String> {
        let ctf = polymarket_client_sdk_v2::contract_config(137, false)
            .ok_or("missing pinned CTF")?
            .conditional_tokens;
        let owner = address(&self.live.funder_address)?;
        let (current, legacy) = tokio::try_join!(
            self.uint_call(
                ctf,
                Ctf::isApprovedForAllCall {
                    owner,
                    operator: EXTERNAL_AUTO_REDEEMERS[0],
                }
                .abi_encode(),
                block,
            ),
            self.uint_call(
                ctf,
                Ctf::isApprovedForAllCall {
                    owner,
                    operator: EXTERNAL_AUTO_REDEEMERS[1],
                }
                .abi_encode(),
                block,
            ),
        )?;
        for (operator, approved) in EXTERNAL_AUTO_REDEEMERS.into_iter().zip([current, legacy]) {
            if approved == U256::from(1) {
                return Err(format!(
                    "external AutoRedeemer {operator:#x} remains approved; owner approval setup required for sole-writer trading"
                ));
            }
            if approved != U256::ZERO {
                return Err("external AutoRedeemer approval has invalid boolean ABI".into());
            }
        }
        Ok(())
    }
    async fn identity(&self, plan: &mut ChainPlan, block: &str) -> Result<(), String> {
        let (ctf, underlying) = self.deployment_identity(block).await?;
        let cond = B256::from_str(&plan.condition_id).map_err(|_| "invalid closeout condition")?;
        let slots = self
            .uint_call(
                ctf,
                Ctf::getOutcomeSlotCountCall { conditionId: cond }.abi_encode(),
                block,
            )
            .await?;
        if slots != U256::from(2) {
            return Err("closeout requires an existing binary CTF condition".into());
        }
        let mut derived_ids = [String::new(), String::new()];
        for (i, token) in derived_ids.iter_mut().enumerate() {
            let collection = self
                .call(
                    ctf,
                    Ctf::getCollectionIdCall {
                        parent: B256::ZERO,
                        conditionId: cond,
                        indexSet: U256::from(i + 1),
                    }
                    .abi_encode(),
                    block,
                )
                .await?;
            if collection.len() != 32 {
                return Err("invalid collection ABI result".into());
            }
            let derived = self
                .uint_call(
                    ctf,
                    Ctf::getPositionIdCall {
                        collateral: underlying,
                        collectionId: B256::from_slice(&collection),
                    }
                    .abi_encode(),
                    block,
                )
                .await?;
            *token = derived.to_string();
        }
        if plan
            .token_ids
            .iter()
            .filter(|s| !s.is_empty())
            .any(|s| !derived_ids.contains(s))
        {
            return Err("closeout token/condition/underlying identity mismatch".into());
        }
        plan.token_ids = derived_ids;
        Ok(())
    }
    /// Read-only closeout readiness must pass before a funded executor may Resume. Checks
    /// account/deployment/approval/gas even when the account currently has no outcome tokens.
    pub async fn ready(&self) -> Result<(), String> {
        let chain = quantity_u64(&self.rpc("eth_chainId", json!([])).await?)?;
        if chain != self.live.chain_id {
            return Err("Polygon RPC chain identity mismatch".into());
        }
        let head = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let number = quantity_u64(head.get("number").ok_or("chain head number missing")?)?;
        let hash = head
            .get("hash")
            .and_then(Value::as_str)
            .ok_or("chain head hash missing")?;
        parse_hash(hash)?;
        let tag = format!("0x{number:x}");
        self.deployment_identity(&tag).await?;
        self.verify_no_external_redeemer(&tag).await?;
        if self.live.signature_type == 3 {
            let nonce = self.verify_deposit_wallet(&tag).await?;
            if self
                .relay_transport()?
                .authenticated_nonce(self.signer.address())
                .await?
                != nonce
            {
                return Err("relayer nonce differs from canonical DepositWallet nonce".into());
            }
        } else {
            self.wallet_call(Vec::new(), &tag).await?;
            let from = format!("{:#x}", self.signer.address());
            let pol = quantity_u128(&self.rpc("eth_getBalance", json!([from, tag])).await?)?;
            if pol < decimal_u128(&self.settings.max_transaction_cost_wei)? {
                return Err(
                    "signer POL balance below explicit worst-case closeout transaction cap".into(),
                );
            }
        }
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(hash) {
            return Err("closeout readiness head changed canonical identity".into());
        }
        Ok(())
    }
    /// Reads authoritative chain state and signs once. It does not submit or approve anything.
    pub async fn prepare(
        &self,
        plan: &ChainPlan,
        now_ms: u64,
    ) -> Result<PreparedChainTransaction, String> {
        plan.validate()?;
        let mut plan = plan.clone();
        let chain = quantity_u64(&self.rpc("eth_chainId", json!([])).await?)?;
        if chain != self.live.chain_id {
            return Err("Polygon RPC chain identity mismatch".into());
        }
        let head = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let block_number = quantity_u64(head.get("number").ok_or("chain head number missing")?)?;
        let head_hash = head
            .get("hash")
            .and_then(Value::as_str)
            .ok_or("chain head hash missing")?;
        parse_hash(head_hash)?;
        let block = format!("0x{block_number:x}");
        self.identity(&mut plan, &block).await?;
        self.verify_no_external_redeemer(&block).await?;
        let (before_tokens_micros, before_collateral_micros) = self.balances(&plan, &block).await?;
        let (expected_payout_micros, payout_numerators, payout_denominator) = match plan.operation {
            ChainOperation::Merge { amount_micros } => {
                if before_tokens_micros.iter().any(|q| *q < amount_micros) {
                    return Err("insufficient authoritative balanced closeout tokens".into());
                }
                (amount_micros, [0, 0], 0)
            }
            ChainOperation::Redeem => {
                if before_tokens_micros == [0, 0] {
                    return Err("no authoritative tokens to redeem".into());
                }
                let ctf = polymarket_client_sdk_v2::contract_config(137, false)
                    .ok_or("missing pinned CTF")?
                    .conditional_tokens;
                let cond = B256::from_str(&plan.condition_id).map_err(|_| "invalid condition")?;
                let (denom, a, b) = tokio::try_join!(
                    self.uint_call(
                        ctf,
                        Ctf::payoutDenominatorCall { conditionId: cond }.abi_encode(),
                        &block
                    ),
                    self.uint_call(
                        ctf,
                        Ctf::payoutNumeratorsCall {
                            conditionId: cond,
                            index: U256::ZERO
                        }
                        .abi_encode(),
                        &block
                    ),
                    self.uint_call(
                        ctf,
                        Ctf::payoutNumeratorsCall {
                            conditionId: cond,
                            index: U256::from(1)
                        }
                        .abi_encode(),
                        &block
                    )
                )?;
                let d = bounded_u64(denom)?;
                let a = bounded_u64(a)?;
                let b = bounded_u64(b)?;
                if d == 0 || u128::from(a) + u128::from(b) != u128::from(d) {
                    return Err("authoritative CTF resolution unavailable or invalid".into());
                }
                let payout = u128::from(before_tokens_micros[0]) * u128::from(a) / u128::from(d)
                    + u128::from(before_tokens_micros[1]) * u128::from(b) / u128::from(d);
                (
                    u64::try_from(payout).map_err(|_| "redemption payout overflow")?,
                    [a, b],
                    d,
                )
            }
        };
        if self.live.signature_type == 3 {
            let nonce = self.verify_deposit_wallet(&block).await?;
            let mut prepared = PreparedChainTransaction {
                plan,
                hash: String::new(),
                raw_transaction: String::new(),
                nonce,
                safe_nonce: None,
                safe_transaction_hash: None,
                signer_address: format!("{:#x}", self.signer.address()),
                funder_address: self.live.funder_address.clone(),
                before_tokens_micros,
                before_collateral_micros,
                expected_payout_micros,
                payout_numerators,
                payout_denominator,
                gas_limit: 0,
                max_fee_per_gas_wei: 0,
                max_priority_fee_per_gas_wei: 0,
                maximum_gas_cost_wei: "0".into(),
                prepared_at_ms: now_ms,
                relayer: None,
            };
            let timestamp = quantity_u64(
                head.get("timestamp")
                    .ok_or("chain head timestamp missing")?,
            )?;
            let relay = self
                .prepare_relay(
                    self.relay_transport()?,
                    &prepared,
                    block_number,
                    head_hash,
                    timestamp,
                )
                .await?;
            prepared.hash = relay.batch_hash.clone();
            prepared.relayer = Some(relay);
            let canonical = self
                .rpc("eth_getBlockByNumber", json!([block, false]))
                .await?;
            if canonical.get("hash").and_then(Value::as_str) != Some(head_hash) {
                return Err("closeout preparation head changed canonical identity".into());
            }
            validate_prepared(&self.live, &self.settings, &prepared)?;
            return Ok(prepared);
        }
        let inner = inner_call(&plan, address(&self.live.collateral_address)?)?;
        let (target, data, safe_nonce, safe_transaction_hash) =
            self.wallet_call(inner, &block).await?;
        let from = format!("{:#x}", self.signer.address());
        let nonce = quantity_u64(
            &self
                .rpc("eth_getTransactionCount", json!([from, "pending"]))
                .await?,
        )?;
        let gas_estimate = quantity_u64(&self.rpc("eth_estimateGas",json!([{
            "from":from,"to":format!("{target:#x}"),"value":"0x0","data":hex_data(&data),
            "maxFeePerGas":format!("0x{:x}",self.settings.max_fee_per_gas_wei),
            "maxPriorityFeePerGas":format!("0x{:x}",self.settings.max_priority_fee_per_gas_wei)
        }])).await?)?;
        let gas_limit = gas_estimate
            .checked_add(gas_estimate / 4)
            .and_then(|n| n.checked_add(20_000))
            .ok_or("gas overflow")?;
        let maximum_gas_cost =
            u128::from(gas_limit) * u128::from(self.settings.max_fee_per_gas_wei);
        if gas_limit == 0
            || gas_limit > self.settings.max_gas_limit
            || maximum_gas_cost > decimal_u128(&self.settings.max_transaction_cost_wei)?
        {
            return Err("closeout gas estimate exceeds explicit finite cap".into());
        }
        let pol = quantity_u128(&self.rpc("eth_getBalance", json!([from, "latest"])).await?)?;
        if pol < maximum_gas_cost {
            return Err("signer POL balance insufficient for bounded closeout gas".into());
        }
        let canonical = self
            .rpc("eth_getBlockByNumber", json!([block, false]))
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(head_hash) {
            return Err("closeout preparation head changed canonical identity".into());
        }
        let tx = TxEip1559 {
            chain_id: self.live.chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas: u128::from(self.settings.max_fee_per_gas_wei),
            max_priority_fee_per_gas: u128::from(self.settings.max_priority_fee_per_gas_wei),
            to: TxKind::Call(target),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(data),
        };
        let signature = self
            .signer
            .sign_hash(&tx.signature_hash())
            .await
            .map_err(|_| "cannot sign authorized closeout")?;
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let raw = envelope.encoded_2718();
        let prepared = PreparedChainTransaction {
            plan: plan.clone(),
            hash: format!("{:#x}", keccak256(&raw)),
            raw_transaction: hex_data(&raw),
            nonce,
            safe_nonce,
            safe_transaction_hash,
            signer_address: from,
            funder_address: self.live.funder_address.clone(),
            before_tokens_micros,
            before_collateral_micros,
            expected_payout_micros,
            payout_numerators,
            payout_denominator,
            gas_limit,
            max_fee_per_gas_wei: self.settings.max_fee_per_gas_wei,
            max_priority_fee_per_gas_wei: self.settings.max_priority_fee_per_gas_wei,
            maximum_gas_cost_wei: maximum_gas_cost.to_string(),
            prepared_at_ms: now_ms,
            relayer: None,
        };
        validate_prepared(&self.live, &self.settings, &prepared)?;
        Ok(prepared)
    }
    async fn wallet_call(
        &self,
        inner: Vec<u8>,
        block: &str,
    ) -> Result<(Address, Vec<u8>, Option<u64>, Option<String>), String> {
        let funder = address(&self.live.funder_address)?;
        let signer = self.signer.address();
        match self.live.signature_type {
            0 => Ok((STANDARD_ADAPTER, inner, None, None)),
            1 => {
                let factory = polymarket_client_sdk_v2::wallet_contract_config(137)
                    .and_then(|c| c.proxy_factory)
                    .ok_or("missing canonical proxy factory")?;
                // An existing wallet is required: closeout never silently deploys a new account.
                let code = self
                    .rpc("eth_getCode", json!([format!("{funder:#x}"), block]))
                    .await?;
                let bytes = hex_bytes(code.as_str().ok_or("invalid proxy code")?, MAX_RPC_BYTES)?;
                if bytes.is_empty() {
                    return Err("canonical proxy account is not deployed".into());
                }
                let implementation = self
                    .address_call(
                        factory,
                        ProxyFactory::getImplementationCall {}.abi_encode(),
                        block,
                    )
                    .await?;
                if implementation.is_zero() {
                    return Err("canonical proxy factory implementation missing".into());
                }
                let expected_code = hex_bytes(
                    &format!(
                        "0x363d3d373d3d3d363d73{}5af43d82803e903d91602b57fd5bf3",
                        hex::encode(implementation.as_slice())
                    ),
                    MAX_RPC_BYTES,
                )?;
                if bytes != expected_code {
                    return Err(
                        "canonical proxy bytecode differs from factory implementation".into(),
                    );
                }
                // Proxy owner is the factory; inspect its documented owner slot and bind it.
                let slot = "0x734a2a5caf82146a5ddd5263d9af379f9f72724959f0567ddc9df2c40cf2cc20";
                let owner = self
                    .rpc(
                        "eth_getStorageAt",
                        json!([format!("{funder:#x}"), slot, block]),
                    )
                    .await?;
                let raw = hex_bytes(owner.as_str().ok_or("invalid proxy owner slot")?, 32)?;
                if raw.len() != 32
                    || raw[..12].iter().any(|b| *b != 0)
                    || Address::from_slice(&raw[12..]) != factory
                {
                    return Err("canonical proxy factory ownership mismatch".into());
                }
                let data = ProxyFactory::proxyCall {
                    calls: vec![ProxyCall {
                        typeCode: 1,
                        to: STANDARD_ADAPTER,
                        value: U256::ZERO,
                        data: Bytes::from(inner),
                    }],
                }
                .abi_encode();
                Ok((factory, data, None, None))
            }
            2 => {
                let threshold = self
                    .uint_call(funder, Safe::getThresholdCall {}.abi_encode(), block)
                    .await?;
                let owner_raw = self
                    .call(funder, Safe::getOwnersCall {}.abi_encode(), block)
                    .await?;
                let owners = Safe::getOwnersCall::abi_decode_returns(&owner_raw)
                    .map_err(|_| "invalid canonical Safe owners")?;
                if threshold != U256::from(1) || owners != vec![signer] {
                    return Err("closeout requires canonical one-of-one signer-owned Safe".into());
                }
                let nonce = bounded_u64(
                    self.uint_call(funder, Safe::nonceCall {}.abi_encode(), block)
                        .await?,
                )?;
                let hash = self
                    .call(
                        funder,
                        Safe::getTransactionHashCall {
                            to: STANDARD_ADAPTER,
                            value: U256::ZERO,
                            data: Bytes::from(inner.clone()),
                            operation: 0,
                            safeTxGas: U256::ZERO,
                            baseGas: U256::ZERO,
                            gasPrice: U256::ZERO,
                            gasToken: Address::ZERO,
                            refundReceiver: Address::ZERO,
                            nonce: U256::from(nonce),
                        }
                        .abi_encode(),
                        block,
                    )
                    .await?;
                if hash.len() != 32 {
                    return Err("invalid canonical Safe transaction hash".into());
                }
                let data = safe_outer(inner, signer);
                Ok((funder, data, Some(nonce), Some(hex_data(&hash))))
            }
            _ => Err("unsupported closeout account wallet".into()),
        }
    }
    /// Broadcasts only the exact already-durable raw transaction. An error remains unknown.
    pub async fn submit(&self, prepared: &PreparedChainTransaction) -> Result<(), String> {
        validate_prepared(&self.live, &self.settings, prepared)?;
        let result = self
            .rpc("eth_sendRawTransaction", json!([prepared.raw_transaction]))
            .await?;
        if result.as_str() != Some(prepared.hash.as_str()) {
            return Err(
                "closeout broadcast acknowledgement hash mismatch; original outcome unknown".into(),
            );
        }
        Ok(())
    }
    fn relay_transport(&self) -> Result<&relay::RelayTransport, String> {
        self.relayer_transport
            .as_ref()
            .ok_or_else(|| "DepositWallet requires an authenticated relayer transport".into())
    }
    pub async fn submit_ack(
        &self,
        prepared: &PreparedChainTransaction,
    ) -> Result<Option<String>, String> {
        validate_prepared(&self.live, &self.settings, prepared)?;
        if let Some(relay) = &prepared.relayer {
            self.relay_transport()?.submit_relay(relay).await.map(Some)
        } else {
            self.submit(prepared).await.map(|()| None)
        }
    }
    pub async fn observe(
        &self,
        prepared: &PreparedChainTransaction,
    ) -> Result<ChainObservation, String> {
        validate_prepared(&self.live, &self.settings, prepared)?;
        if let Some(relay) = &prepared.relayer {
            if let Some(candidate) = self
                .lookup_relay(self.relay_transport()?, prepared, relay)
                .await?
            {
                return Ok(self
                    .verified_receipt(
                        prepared,
                        &candidate.transaction_hash,
                        candidate.receipt,
                        false,
                    )
                    .await?
                    .map_or(ChainObservation::Unknown, ChainObservation::Finalized));
            }
            if let Some(expiry) = self.expired_relay(prepared, relay).await? {
                return Ok(ChainObservation::Expired(expiry));
            }
            Ok(ChainObservation::Unknown)
        } else {
            Ok(self
                .lookup(prepared)
                .await?
                .map_or(ChainObservation::Unknown, ChainObservation::Finalized))
        }
    }
    /// Exact ERC1155 amounts at one numbered block, with its canonical hash rechecked after
    /// bounded parallel reads. Data API flags/quantities are not settlement authority.
    pub async fn token_balances(&self, tokens: &[String]) -> Result<Vec<u64>, String> {
        use futures_util::{StreamExt, TryStreamExt};
        if tokens.len() > 128 {
            return Err("chain balance query capacity exceeded".into());
        }
        let block = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let number = quantity_u64(block.get("number").ok_or("chain head number missing")?)?;
        let hash = block
            .get("hash")
            .and_then(Value::as_str)
            .ok_or("chain head hash missing")?;
        parse_hash(hash)?;
        let tag = format!("0x{number:x}");
        let ctf = polymarket_client_sdk_v2::contract_config(137, false)
            .ok_or("missing pinned CTF")?
            .conditional_tokens;
        let funder = address(&self.live.funder_address)?;
        // Own the bounded work queue: this future is used through async_trait's Send
        // boundary, where a stream retaining slice lifetimes is not sufficiently general.
        let work: Vec<String> = tokens.to_vec();
        let amounts: Vec<(usize, u64)> = futures_util::stream::iter(work.into_iter().enumerate())
            .map(|(i, token)| {
                let tag = tag.clone();
                async move {
                    let id = token_id(&token)?;
                    let amount = self
                        .uint_call(
                            ctf,
                            Ctf::balanceOfCall { owner: funder, id }.abi_encode(),
                            &tag,
                        )
                        .await?;
                    Ok::<_, String>((i, bounded_u64(amount)?))
                }
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        let canonical = self
            .rpc("eth_getBlockByNumber", json!([tag, false]))
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(hash) {
            return Err("chain balance head changed canonical identity".into());
        }
        let mut result = vec![0; tokens.len()];
        for (i, amount) in amounts {
            result[i] = amount;
        }
        Ok(result)
    }
    /// Missing receipt is never proof of absence. No retry, nonce increment or replacement occurs.
    pub async fn lookup(
        &self,
        prepared: &PreparedChainTransaction,
    ) -> Result<Option<ChainReceipt>, String> {
        validate_prepared(&self.live, &self.settings, prepared)?;
        let receipt = self
            .rpc("eth_getTransactionReceipt", json!([prepared.hash]))
            .await?;
        if receipt.is_null() {
            return Ok(None);
        }
        self.verified_receipt(prepared, &prepared.hash, receipt, true)
            .await
    }
    /// Read-only finality proof for exchange fees. Does not sign or broadcast anything.
    pub(crate) async fn execution_fees(
        &self,
        hash: &str,
        tracked: &[crate::LiveOrder],
    ) -> Result<Option<Vec<crate::fees::ExecutionFee>>, String> {
        parse_hash(hash)?;
        let receipt = self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
        if receipt.is_null() {
            return Ok(None);
        }
        if receipt.get("transactionHash").and_then(Value::as_str) != Some(hash) {
            return Err("execution fee receipt hash mismatch".into());
        }
        let block = quantity_u64(
            receipt
                .get("blockNumber")
                .ok_or("fee receipt block missing")?,
        )?;
        let finalized = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let height = quantity_u64(finalized.get("number").ok_or("fee finality unavailable")?)?;
        if height
            < block
                .checked_add(self.settings.minimum_confirmations - 1)
                .ok_or("fee finality overflow")?
        {
            return Ok(None);
        }
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;
        if !crate::fees::canonical_fee_receipt(
            &receipt,
            hash,
            &canonical,
            height,
            self.settings.minimum_confirmations,
        )? {
            return Ok(None);
        }
        crate::fees::decode_receipt(
            &receipt,
            address(&self.live.exchange_address)?,
            address(&self.live.funder_address)?,
            tracked,
            height,
        )
        .map(Some)
    }
    async fn verified_receipt(
        &self,
        prepared: &PreparedChainTransaction,
        transaction_hash: &str,
        receipt: Value,
        owner_paid_gas: bool,
    ) -> Result<Option<ChainReceipt>, String> {
        parse_hash(transaction_hash)?;
        if receipt.get("transactionHash").and_then(Value::as_str) != Some(transaction_hash) {
            return Err("closeout receipt hash identity mismatch".into());
        }
        let block_number =
            quantity_u64(receipt.get("blockNumber").ok_or("receipt block missing")?)?;
        let block_hash = receipt
            .get("blockHash")
            .and_then(Value::as_str)
            .ok_or("receipt block hash missing")?
            .to_owned();
        parse_hash(&block_hash)?;
        let finalized = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let finalized_number = quantity_u64(
            finalized
                .get("number")
                .ok_or("finalized chain evidence unavailable")?,
        )?;
        let required = block_number
            .checked_add(self.settings.minimum_confirmations - 1)
            .ok_or("confirmation height overflow")?;
        if finalized_number < required {
            return Ok(None);
        }
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{block_number:x}"), false]),
            )
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(block_hash.as_str()) {
            return Err("closeout receipt block is not canonical".into());
        }
        let status = quantity_u64(receipt.get("status").ok_or("receipt status missing")?)?;
        if status > 1 {
            return Err("invalid receipt status".into());
        }
        let mut success = status == 1;
        if self.live.signature_type == 2 && success {
            success = safe_success(
                &receipt,
                address(&self.live.funder_address)?,
                prepared
                    .safe_transaction_hash
                    .as_deref()
                    .ok_or("safe transaction identity missing")?,
            )?;
        }
        let used = quantity_u64(receipt.get("gasUsed").ok_or("receipt gas missing")?)?;
        let price = quantity_u128(
            receipt
                .get("effectiveGasPrice")
                .ok_or("receipt gas price missing")?,
        )?;
        let gas_cost = u128::from(used)
            .checked_mul(price)
            .ok_or("receipt gas cost overflow")?;
        if owner_paid_gas
            && (used > prepared.gas_limit
                || price > u128::from(prepared.max_fee_per_gas_wei)
                || gas_cost > decimal_u128(&prepared.maximum_gas_cost_wei)?)
        {
            return Err("receipt gas contradicts signed gas cap".into());
        }
        let (after_tokens_micros, after_collateral_micros) = self
            .balances(&prepared.plan, &format!("0x{block_number:x}"))
            .await?;
        let expected_tokens = if success {
            match prepared.plan.operation {
                ChainOperation::Merge { amount_micros } => [
                    prepared.before_tokens_micros[0] - amount_micros,
                    prepared.before_tokens_micros[1] - amount_micros,
                ],
                ChainOperation::Redeem => [0, 0],
            }
        } else {
            prepared.before_tokens_micros
        };
        let expected_delta = if success {
            prepared.expected_payout_micros
        } else {
            0
        };
        let (token_consumption, collateral_delta) =
            receipt_deltas(&receipt, &self.live, &prepared.plan)?;
        let expected_consumption = [
            prepared.before_tokens_micros[0] - expected_tokens[0],
            prepared.before_tokens_micros[1] - expected_tokens[1],
        ];
        // Redemption sweeps the adapter's entire USDCE balance, including prior dust.
        // Exact token debits and at least the signed resolution payout are mandatory;
        // imported adapter cash must not increase the ledger's owned-capital credit.
        let payout_matches = if success && matches!(prepared.plan.operation, ChainOperation::Redeem)
        {
            collateral_delta >= expected_delta
        } else {
            collateral_delta == expected_delta
        };
        if token_consumption != expected_consumption || !payout_matches {
            return Err("finalized closeout transaction logs contradict durable economic intent; owner reconciliation required".into());
        }
        Ok(Some(ChainReceipt {
            transaction_hash: transaction_hash.to_owned(),
            block_number,
            block_hash,
            finalized_block_number: finalized_number,
            success,
            after_tokens_micros,
            after_collateral_micros,
            collateral_delta_micros: collateral_delta,
            consumed_tokens_micros: token_consumption,
            payout_numerators: prepared.payout_numerators,
            payout_denominator: prepared.payout_denominator,
            gas_cost_wei: if owner_paid_gas {
                gas_cost.to_string()
            } else {
                "0".into()
            },
            intent_hash: (!owner_paid_gas).then(|| prepared.hash.clone()),
            sponsored_gas_cost_wei: (!owner_paid_gas).then(|| gas_cost.to_string()),
        }))
    }
}

pub fn validate_prepared(
    live: &LiveSettings,
    settings: &ChainSettings,
    p: &PreparedChainTransaction,
) -> Result<(), String> {
    live.validate()?;
    settings.validate()?;
    p.plan.validate()?;
    for token in &p.plan.token_ids {
        token_id(token)?;
    }
    if live.signature_type == 3 {
        if address(&p.signer_address)? != address(&live.signer_address)?
            || address(&p.funder_address)? != address(&live.funder_address)?
            || p.prepared_at_ms == 0
        {
            return Err("durable relay owner or timestamp mismatch".into());
        }
        return relay::verify_prepared_relay(
            live,
            p,
            p.relayer.as_ref().ok_or("durable relay missing")?,
        );
    }
    if p.relayer.is_some() {
        return Err("unexpected relay for direct closeout".into());
    }
    let raw = hex_bytes(&p.raw_transaction, MAX_RAW_BYTES)?;
    if raw.is_empty()
        || format!("{:#x}", keccak256(&raw)) != p.hash
        || address(&p.signer_address)? != address(&live.signer_address)?
        || address(&p.funder_address)? != address(&live.funder_address)?
        || p.prepared_at_ms == 0
        || p.gas_limit == 0
        || p.gas_limit > settings.max_gas_limit
        || p.max_fee_per_gas_wei > settings.max_fee_per_gas_wei
        || p.max_priority_fee_per_gas_wei > settings.max_priority_fee_per_gas_wei
        || p.max_priority_fee_per_gas_wei == 0
        || p.max_priority_fee_per_gas_wei > p.max_fee_per_gas_wei
    {
        return Err("durable closeout transaction identity or bounds invalid".into());
    }
    let envelope =
        TxEnvelope::decode_2718_exact(&raw).map_err(|_| "invalid durable signed transaction")?;
    let TxEnvelope::Eip1559(signed) = envelope else {
        return Err("only bounded signed EIP1559 closeout is supported".into());
    };
    let tx = signed.tx();
    let recovered = signed
        .signature()
        .recover_address_from_prehash(&tx.signature_hash())
        .map_err(|_| "invalid durable transaction signer")?;
    let (target, data) = expected_outer(live, &p.plan)?;
    let gas_cost = u128::from(p.gas_limit) * u128::from(p.max_fee_per_gas_wei);
    if recovered != address(&live.signer_address)?
        || tx.chain_id != 137
        || tx.nonce != p.nonce
        || tx.gas_limit != p.gas_limit
        || tx.max_fee_per_gas != u128::from(p.max_fee_per_gas_wei)
        || tx.max_priority_fee_per_gas != u128::from(p.max_priority_fee_per_gas_wei)
        || tx.to != TxKind::Call(target)
        || tx.input.as_ref() != data
        || tx.value != U256::ZERO
        || !tx.access_list.0.is_empty()
        || decimal_u128(&p.maximum_gas_cost_wei)? != gas_cost
        || gas_cost > decimal_u128(&settings.max_transaction_cost_wei)?
    {
        return Err("durable signed closeout differs from exact account/plan/gas intent".into());
    }
    match p.plan.operation {
        ChainOperation::Merge { amount_micros } => {
            if p.before_tokens_micros.iter().any(|q| *q < amount_micros)
                || p.expected_payout_micros != amount_micros
                || p.payout_numerators != [0, 0]
                || p.payout_denominator != 0
            {
                return Err("invalid durable merge pre-state".into());
            }
        }
        ChainOperation::Redeem => {
            if p.before_tokens_micros == [0, 0]
                || u128::from(p.expected_payout_micros)
                    > u128::from(p.before_tokens_micros[0]) + u128::from(p.before_tokens_micros[1])
            {
                return Err("invalid durable redemption pre-state".into());
            }
            let [a, b] = p.payout_numerators;
            let d = p.payout_denominator;
            if d == 0
                || u128::from(a) + u128::from(b) != u128::from(d)
                || u128::from(p.expected_payout_micros)
                    != u128::from(p.before_tokens_micros[0]) * u128::from(a) / u128::from(d)
                        + u128::from(p.before_tokens_micros[1]) * u128::from(b) / u128::from(d)
            {
                return Err("invalid durable authoritative redemption payout".into());
            }
        }
    }
    if live.signature_type == 2 {
        if p.safe_nonce.is_none()
            || p.safe_transaction_hash
                .as_ref()
                .is_none_or(|h| parse_hash(h).is_err())
        {
            return Err("invalid durable Safe identity".into());
        }
    } else if p.safe_nonce.is_some() || p.safe_transaction_hash.is_some() {
        return Err("unexpected Safe identity for EOA/proxy".into());
    }
    Ok(())
}
fn expected_outer(live: &LiveSettings, plan: &ChainPlan) -> Result<(Address, Vec<u8>), String> {
    let inner = inner_call(plan, address(&live.collateral_address)?)?;
    match live.signature_type {
        0 => Ok((STANDARD_ADAPTER, inner)),
        1 => {
            let factory = polymarket_client_sdk_v2::wallet_contract_config(137)
                .and_then(|c| c.proxy_factory)
                .ok_or("missing pinned proxy factory")?;
            Ok((
                factory,
                ProxyFactory::proxyCall {
                    calls: vec![ProxyCall {
                        typeCode: 1,
                        to: STANDARD_ADAPTER,
                        value: U256::ZERO,
                        data: Bytes::from(inner),
                    }],
                }
                .abi_encode(),
            ))
        }
        2 => Ok((
            address(&live.funder_address)?,
            safe_outer(inner, address(&live.signer_address)?),
        )),
        _ => Err("unsupported wallet closeout".into()),
    }
}
fn inner_call(plan: &ChainPlan, collateral: Address) -> Result<Vec<u8>, String> {
    let cond = B256::from_str(&plan.condition_id).map_err(|_| "invalid condition")?;
    let partition = vec![U256::from(1), U256::from(2)];
    Ok(match plan.operation {
        ChainOperation::Merge { amount_micros } => Adapter::mergePositionsCall {
            collateral,
            parent: B256::ZERO,
            conditionId: cond,
            partition,
            amount: U256::from(amount_micros),
        }
        .abi_encode(),
        ChainOperation::Redeem => Adapter::redeemPositionsCall {
            collateral,
            parent: B256::ZERO,
            conditionId: cond,
            indexSets: partition,
        }
        .abi_encode(),
    })
}
fn safe_outer(inner: Vec<u8>, signer: Address) -> Vec<u8> {
    // Safe v=1 is an approved-by-msg.sender signature. The signed outer EOA transaction
    // binds every calldata byte and only that authenticated owner can use this path.
    let mut approved = vec![0u8; 65];
    approved[12..32].copy_from_slice(signer.as_slice());
    approved[64] = 1;
    Safe::execTransactionCall {
        to: STANDARD_ADAPTER,
        value: U256::ZERO,
        data: Bytes::from(inner),
        operation: 0,
        safeTxGas: U256::ZERO,
        baseGas: U256::ZERO,
        gasPrice: U256::ZERO,
        gasToken: Address::ZERO,
        refundReceiver: Address::ZERO,
        signatures: Bytes::from(approved),
    }
    .abi_encode()
}
fn safe_success(receipt: &Value, funder: Address, inner_hash: &str) -> Result<bool, String> {
    let success = format!("{:#x}", keccak256(b"ExecutionSuccess(bytes32,uint256)"));
    let failure = format!("{:#x}", keccak256(b"ExecutionFailure(bytes32,uint256)"));
    let expected = parse_hash(inner_hash)?;
    let logs = receipt
        .get("logs")
        .and_then(Value::as_array)
        .ok_or("Safe receipt logs missing")?;
    if logs.len() > 1024 {
        return Err("Safe receipt log bound exceeded".into());
    }
    let mut result = None;
    for log in logs {
        let Some(addr) = log.get("address").and_then(Value::as_str) else {
            return Err("invalid receipt log address".into());
        };
        if Address::from_str(addr).map_err(|_| "invalid receipt log address")? != funder {
            continue;
        }
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .ok_or("invalid Safe receipt topics")?;
        let Some(topic) = topics.first().and_then(Value::as_str) else {
            continue;
        };
        if topic != success && topic != failure {
            continue;
        }
        let data = hex_bytes(
            log.get("data")
                .and_then(Value::as_str)
                .ok_or("Safe event data missing")?,
            64,
        )?;
        if topics.len() != 1
            || data.len() != 64
            || B256::from_slice(&data[..32]) != expected
            || result.is_some()
        {
            return Err("Safe execution event contradicts exact transaction identity".into());
        }
        result = Some(topic == success);
    }
    result.ok_or_else(|| "Safe execution outcome event missing".into())
}
fn receipt_deltas(
    receipt: &Value,
    live: &LiveSettings,
    plan: &ChainPlan,
) -> Result<([u64; 2], u64), String> {
    let erc20 = keccak256(b"Transfer(address,address,uint256)");
    let single = keccak256(b"TransferSingle(address,address,address,uint256,uint256)");
    let batch = keccak256(b"TransferBatch(address,address,address,uint256[],uint256[])");
    let collateral = address(&live.collateral_address)?;
    let ctf = polymarket_client_sdk_v2::contract_config(137, false)
        .ok_or("missing pinned CTF")?
        .conditional_tokens;
    let funder = address(&live.funder_address)?;
    let ids = [token_id(&plan.token_ids[0])?, token_id(&plan.token_ids[1])?];
    let logs = receipt
        .get("logs")
        .and_then(Value::as_array)
        .ok_or("closeout receipt logs missing")?;
    if logs.len() > 1024 {
        return Err("receipt log capacity exceeded".into());
    }
    let mut collateral_net: i128 = 0;
    let mut token_net = [0i128; 2];
    for log in logs {
        let emitter = address(
            log.get("address")
                .and_then(Value::as_str)
                .ok_or("receipt emitter missing")?,
        )?;
        if emitter != collateral && emitter != ctf {
            continue;
        }
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .ok_or("receipt topics missing")?;
        let Some(topic) = topics.first().and_then(Value::as_str) else {
            continue;
        };
        let signature = parse_hash(topic)?;
        let raw = hex_bytes(
            log.get("data")
                .and_then(Value::as_str)
                .ok_or("receipt log data missing")?,
            64 * 1024,
        )?;
        if emitter == collateral && signature == erc20 {
            if topics.len() != 3 || raw.len() != 32 {
                return Err("invalid collateral transfer event".into());
            }
            let from = topic_address(&topics[1])?;
            let to = topic_address(&topics[2])?;
            let value = i128::from(bounded_u64(U256::from_be_slice(&raw))?);
            if from == funder {
                collateral_net = collateral_net
                    .checked_sub(value)
                    .ok_or("collateral log overflow")?;
            }
            if to == funder {
                collateral_net = collateral_net
                    .checked_add(value)
                    .ok_or("collateral log overflow")?;
            }
        } else if emitter == ctf && (signature == single || signature == batch) {
            if topics.len() != 4 {
                return Err("invalid CTF transfer topics".into());
            }
            let from = topic_address(&topics[2])?;
            let to = topic_address(&topics[3])?;
            let pairs = if signature == single {
                if raw.len() != 64 {
                    return Err("invalid single CTF transfer event".into());
                }
                vec![(
                    U256::from_be_slice(&raw[..32]),
                    U256::from_be_slice(&raw[32..]),
                )]
            } else {
                decode_batch(&raw)?
            };
            for (id, value) in pairs {
                if let Some(index) = ids.iter().position(|v| *v == id) {
                    let value = i128::from(bounded_u64(value)?);
                    if from == funder {
                        token_net[index] = token_net[index]
                            .checked_add(value)
                            .ok_or("CTF debit overflow")?;
                    }
                    if to == funder {
                        token_net[index] = token_net[index]
                            .checked_sub(value)
                            .ok_or("CTF debit overflow")?;
                    }
                }
            }
        }
    }
    Ok((
        [
            u64::try_from(token_net[0]).map_err(|_| "invalid CTF net debit")?,
            u64::try_from(token_net[1]).map_err(|_| "invalid CTF net debit")?,
        ],
        u64::try_from(collateral_net).map_err(|_| "invalid closeout net collateral return")?,
    ))
}
fn topic_address(value: &Value) -> Result<Address, String> {
    let topic = parse_hash(value.as_str().ok_or("invalid address topic")?)?;
    if topic.as_slice()[..12].iter().any(|b| *b != 0) {
        return Err("invalid padded address topic".into());
    }
    Ok(Address::from_slice(&topic.as_slice()[12..]))
}
fn decode_batch(raw: &[u8]) -> Result<Vec<(U256, U256)>, String> {
    if raw.len() < 128 || !raw.len().is_multiple_of(32) {
        return Err("invalid CTF batch event size".into());
    }
    let offset_a = usize::try_from(U256::from_be_slice(&raw[..32]))
        .map_err(|_| "CTF batch offset overflow")?;
    let offset_b = usize::try_from(U256::from_be_slice(&raw[32..64]))
        .map_err(|_| "CTF batch offset overflow")?;
    let array = |offset: usize| -> Result<Vec<U256>, String> {
        if offset < 64
            || !offset.is_multiple_of(32)
            || offset.checked_add(32).is_none_or(|end| end > raw.len())
        {
            return Err("invalid CTF batch offset".into());
        }
        let length = usize::try_from(U256::from_be_slice(&raw[offset..offset + 32]))
            .map_err(|_| "CTF batch length overflow")?;
        if length > 128
            || offset
                .checked_add(32 + length * 32)
                .is_none_or(|end| end > raw.len())
        {
            return Err("CTF batch capacity exceeded".into());
        }
        Ok((0..length)
            .map(|i| U256::from_be_slice(&raw[offset + 32 + i * 32..offset + 64 + i * 32]))
            .collect())
    };
    let ids = array(offset_a)?;
    let values = array(offset_b)?;
    if ids.len() != values.len() {
        return Err("CTF batch arrays differ".into());
    }
    Ok(ids.into_iter().zip(values).collect())
}
fn validate_rpc_url(url: &str, test_url: bool) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid Polygon RPC URL")?;
    let localhost = matches!(
        parsed.host_str(),
        Some("127.0.0.1") | Some("localhost") | Some("[::1]")
    );
    if url.len() > 2048
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || (parsed.scheme() != "https" && !(test_url && localhost && parsed.scheme() == "http"))
    {
        return Err("Polygon RPC requires explicit HTTPS with certificate validation".into());
    }
    Ok(())
}
fn hex_data(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}
fn hex_bytes(s: &str, max: usize) -> Result<Vec<u8>, String> {
    let raw = s.strip_prefix("0x").ok_or("invalid hexadecimal RPC data")?;
    if raw.len() > max.saturating_mul(2) || !raw.len().is_multiple_of(2) {
        return Err("hexadecimal RPC data bound exceeded".into());
    }
    hex::decode(raw).map_err(|_| "invalid hexadecimal RPC data".into())
}
fn parse_hash(s: &str) -> Result<B256, String> {
    B256::from_str(s).map_err(|_| "invalid transaction/block hash".into())
}
fn token_id(s: &str) -> Result<U256, String> {
    if s.is_empty()
        || s.len() > 78
        || !s.bytes().all(|b| b.is_ascii_digit())
        || (s.len() > 1 && s.starts_with('0'))
    {
        return Err("invalid canonical decimal CTF token".into());
    }
    let n = U256::from_str(s).map_err(|_| "CTF token overflow")?;
    if n.is_zero() {
        return Err("zero CTF token".into());
    }
    Ok(n)
}
fn decimal_u128(s: &str) -> Result<u128, String> {
    if s.is_empty()
        || s.len() > 39
        || !s.bytes().all(|b| b.is_ascii_digit())
        || (s.len() > 1 && s.starts_with('0'))
    {
        return Err("invalid exact decimal gas amount".into());
    }
    s.parse().map_err(|_| "gas amount overflow".into())
}
fn quantity_u128(v: &Value) -> Result<u128, String> {
    let s = v
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or("invalid RPC quantity")?;
    if s.is_empty() || s.len() > 32 || (s.len() > 1 && s.starts_with('0')) {
        return Err("RPC quantity bound exceeded".into());
    }
    u128::from_str_radix(s, 16).map_err(|_| String::from("invalid RPC quantity"))
}
fn quantity_u64(v: &Value) -> Result<u64, String> {
    u64::try_from(quantity_u128(v)?).map_err(|_| "RPC quantity overflow".into())
}
fn bounded_u64(n: U256) -> Result<u64, String> {
    u64::try_from(n).map_err(|_| "chain account quantity exceeds bound".into())
}

/// Public key 1 fixture only, used by coordinator recovery tests. No environment/RPC accesses.
#[cfg(test)]
pub(crate) async fn fixture_prepared(
    mut plan: ChainPlan,
    before_tokens_micros: [u64; 2],
) -> (LiveSettings, PreparedChainTransaction) {
    let signer = PrivateKeySigner::from_str(
        "0000000000000000000000000000000000000000000000000000000000000001",
    )
    .unwrap();
    let account = format!("{:#x}", signer.address());
    let config = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let live = LiveSettings {
        owner_authorized: true,
        owner_address: account.clone(),
        funder_address: account.clone(),
        signer_address: account.clone(),
        exchange_address: format!("{:#x}", config.exchange_v2.unwrap()),
        collateral_address: format!("{:#x}", config.collateral),
        allowed_conditions: vec![plan.condition_id.clone()],
        ..Default::default()
    };
    for (i, token) in plan.token_ids.iter_mut().enumerate() {
        if token.is_empty() {
            *token = (i + 1).to_string();
        }
    }
    let (target, data) = expected_outer(&live, &plan).unwrap();
    let gas_limit = 500_000;
    let fee = 100_000_000_000;
    let priority = 30_000_000_000;
    let tx = TxEip1559 {
        chain_id: 137,
        nonce: 7,
        gas_limit,
        max_fee_per_gas: fee,
        max_priority_fee_per_gas: priority,
        to: TxKind::Call(target),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(data),
    };
    let signature = signer.sign_hash(&tx.signature_hash()).await.unwrap();
    let envelope: TxEnvelope = tx.into_signed(signature).into();
    let raw = envelope.encoded_2718();
    let (expected, payout_numerators, payout_denominator) = match plan.operation {
        ChainOperation::Merge { amount_micros } => (amount_micros, [0, 0], 0),
        ChainOperation::Redeem => (before_tokens_micros[0], [1, 0], 1),
    };
    let p = PreparedChainTransaction {
        plan,
        hash: format!("{:#x}", keccak256(&raw)),
        raw_transaction: hex_data(&raw),
        nonce: 7,
        safe_nonce: None,
        safe_transaction_hash: None,
        signer_address: account.clone(),
        funder_address: account,
        before_tokens_micros,
        before_collateral_micros: 100_000_000,
        expected_payout_micros: expected,
        payout_numerators,
        payout_denominator,
        gas_limit,
        max_fee_per_gas_wei: fee as u64,
        max_priority_fee_per_gas_wei: priority as u64,
        maximum_gas_cost_wei: (u128::from(gas_limit) * fee).to_string(),
        prepared_at_ms: 1000,
        relayer: None,
    };
    validate_prepared(&live, &ChainSettings::default(), &p).unwrap();
    (live, p)
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "chain_identity_tests.rs"]
mod identity_tests;
