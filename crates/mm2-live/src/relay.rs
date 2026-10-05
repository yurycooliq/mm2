//! Owner-authorized DepositWallet batches. HTTP submission is deliberately send-once: the
//! caller persists `dispatched=true` before posting. Relayer state is only a discovery hint;
//! a finalized, canonical factory transaction and wallet events establish success.
use super::*;
use alloy::{
    primitives::Signature,
    sol_types::{SolEvent, SolStruct, SolValue},
};
use reqwest::header::{HeaderMap, HeaderValue};
use std::sync::Mutex;

pub const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
pub const DEPOSIT_FACTORY: Address = literal_address!("0x00000000000Fb5C9ADea0298D729A0CB3823Cc07");
pub const DEPOSIT_BEACON: Address = literal_address!("0x7A18EDfe055488A3128f01F563e5B479D92ffc3a");
pub const DEPOSIT_IMPLEMENTATION: Address =
    literal_address!("0xf7f27C29e60fe6325beF8dA7F93250353d2e3294");
const FACTORY_IMPLEMENTATION: Address =
    literal_address!("0x528cc05efac2b0d255e423272187efd41248abd7");
const BEACON_SLOT: &str = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50";
const IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const MAX_RELAY_BYTES: usize = 64 * 1024;

sol! {
    struct Call { address target; uint256 value; bytes data; }
    struct Batch { address wallet; uint256 nonce; uint256 deadline; Call[] calls; }
    interface DepositWallet {
        function owner() external view returns (address);
        function pendingOwner() external view returns (address);
        function factory() external view returns (address);
        function id() external view returns (bytes32);
        function nonce() external view returns (uint256);
        function paused() external view returns (uint256);
        function walletInterfaceId() external view returns (bytes32);
        function eip712Domain() external view returns (bytes1 fields, string name, string version, uint256 chainId, address verifyingContract, bytes32 salt, uint256[] extensions);
        event BatchExecuted(uint256 indexed nonce);
        event Execution(address indexed target, uint256 value, bytes data, bytes result);
    }
    interface DepositFactory {
        function predictWalletAddress(bytes32 id) external view returns (address);
        function proxy(Batch[] batches, bytes[] signatures) external;
    }
    interface Beacon { function implementation() external view returns (address); }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelaySettings {
    pub url: String,
    pub key_env: String,
    pub key_address_env: String,
    pub deadline_seconds: u64,
    pub log_chunk_blocks: u64,
    pub max_scan_pages: u64,
}
impl Default for RelaySettings {
    fn default() -> Self {
        Self {
            url: RELAYER_URL.into(),
            key_env: "MM2_POLY_RELAYER_API_KEY".into(),
            key_address_env: "MM2_POLY_RELAYER_API_KEY_ADDRESS".into(),
            deadline_seconds: 600,
            log_chunk_blocks: 50,
            max_scan_pages: 8,
        }
    }
}
impl RelaySettings {
    pub fn validate(&self) -> Result<(), String> {
        self.validate_url(false)
    }
    fn validate_url(&self, test: bool) -> Result<(), String> {
        let env_name = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        };
        if (!test && self.url != RELAYER_URL)
            || !env_name(&self.key_env)
            || !env_name(&self.key_address_env)
            || self.key_env == self.key_address_env
            || !(60..=3600).contains(&self.deadline_seconds)
            || !(10..=5000).contains(&self.log_chunk_blocks)
            || !(1..=32).contains(&self.max_scan_pages)
        {
            return Err("invalid bounded DepositWallet relayer settings".into());
        }
        validate_rpc_url(&self.url, test)
    }
}

/// Signed private execution material: persist only in the protected execution journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedRelay {
    pub batch_hash: String,
    pub deadline: u64,
    pub prepared_block_number: u64,
    pub prepared_block_hash: String,
    pub body: String,
    pub signature: String,
    pub dispatched: bool,
    pub request_id: Option<String>,
}
pub struct RelayCandidate {
    pub transaction_hash: String,
    pub receipt: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayExpiry {
    pub intent_hash: String,
    pub block_number: u64,
    pub block_hash: String,
    pub finalized_timestamp_seconds: u64,
    pub wallet_nonce: u64,
    pub retained_tokens_micros: [u64; 2],
    pub collateral_micros: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayBody {
    #[serde(rename = "type")]
    kind: String,
    from: String,
    to: String,
    nonce: String,
    signature: String,
    metadata: String,
    #[serde(rename = "depositWalletParams")]
    params: RelayParams,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayParams {
    #[serde(rename = "depositWallet")]
    wallet: String,
    deadline: String,
    calls: Vec<RelayCall>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayCall {
    target: String,
    value: String,
    data: String,
}

/// Keys remain in sensitive HTTP headers and never enter prepared objects or errors.
pub struct RelayTransport {
    settings: RelaySettings,
    owner: Address,
    http: reqwest::Client,
    cursor: Mutex<Option<(String, u64)>>,
}
impl RelayTransport {
    pub fn from_env(settings: &RelaySettings, timeout_ms: u64) -> Result<Self, String> {
        settings.validate()?;
        let key = std::env::var(&settings.key_env).map_err(|_| "owner Relayer API key missing")?;
        let owner = std::env::var(&settings.key_address_env)
            .map_err(|_| "Relayer API key owner missing")?;
        Self::new(settings.clone(), &key, address(&owner)?, timeout_ms, false)
    }
    fn new(
        settings: RelaySettings,
        key: &str,
        owner: Address,
        timeout_ms: u64,
        test: bool,
    ) -> Result<Self, String> {
        settings.validate_url(test)?;
        if key.is_empty()
            || key.len() > 1024
            || !key.bytes().all(|b| (33..=126).contains(&b))
            || owner.is_zero()
            || !(100..=30_000).contains(&timeout_ms)
        {
            return Err("invalid bounded owner Relayer API credentials".into());
        }
        let mut headers = HeaderMap::new();
        let mut key = HeaderValue::from_str(key).map_err(|_| "invalid Relayer API key header")?;
        key.set_sensitive(true);
        headers.insert("relayer_api_key", key);
        headers.insert(
            "relayer_api_key_address",
            HeaderValue::from_str(&format!("{owner:#x}"))
                .map_err(|_| "invalid Relayer API owner header")?,
        );
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_millis(timeout_ms))
            .default_headers(headers)
            .build()
            .map_err(|_| "cannot create bounded owner relayer transport")?;
        Ok(Self {
            settings,
            owner,
            http,
            cursor: Mutex::new(None),
        })
    }
    #[cfg(test)]
    pub(crate) fn test_transport(settings: RelaySettings, owner: Address) -> Result<Self, String> {
        Self::new(settings, "offline-fixture-key", owner, 1000, true)
    }
    async fn response(mut response: reqwest::Response) -> Result<Value, String> {
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > MAX_RELAY_BYTES as u64)
        {
            return Err("Relayer request rejected; original intent remains uncertain".into());
        }
        let mut bytes = Vec::new();
        while let Some(part) = response
            .chunk()
            .await
            .map_err(|_| "Relayer response incomplete; original intent remains uncertain")?
        {
            if bytes.len().saturating_add(part.len()) > MAX_RELAY_BYTES {
                return Err("Relayer response bound exceeded".into());
            }
            bytes.extend_from_slice(&part);
        }
        serde_json::from_slice(&bytes).map_err(|_| "invalid bounded Relayer response".into())
    }
    pub async fn authenticated_nonce(&self, owner: Address) -> Result<u64, String> {
        if owner != self.owner {
            return Err("Relayer API key owner differs from controlling signer".into());
        }
        let response = self
            .http
            .get(format!(
                "{}/v1/account/transactions/params",
                self.settings.url
            ))
            .query(&[
                ("address", format!("{owner:#x}")),
                ("type", "WALLET".into()),
            ])
            .send()
            .await
            .map_err(|_| "Relayer nonce request failed")?;
        let data = Self::response(response).await?;
        if address(
            data.get("address")
                .and_then(Value::as_str)
                .ok_or("Relayer nonce address missing")?,
        )? != owner
        {
            return Err("Relayer nonce owner mismatch".into());
        }
        decimal_u64(
            data.get("nonce")
                .and_then(Value::as_str)
                .ok_or("Relayer wallet nonce missing")?,
        )
    }
    /// The caller has persisted dispatched=true. Never retries and never rebuilds the batch.
    pub async fn submit_relay(&self, prepared: &PreparedRelay) -> Result<String, String> {
        if !prepared.dispatched
            || prepared.request_id.is_some()
            || prepared.body.len() > MAX_RAW_BYTES
        {
            return Err("Relayer dispatch must be durably marked once before HTTP".into());
        }
        verify_envelope(self.owner, prepared)?;
        let response = self
            .http
            .post(format!("{}/submit", self.settings.url))
            .header("content-type", "application/json")
            .body(prepared.body.clone())
            .send()
            .await
            .map_err(|_| "Relayer submission acknowledgement unknown")?;
        let data = Self::response(response).await?;
        if data.get("state").and_then(Value::as_str) != Some("STATE_NEW") {
            return Err("Relayer submit state unrecognized; retain original intent".into());
        }
        let id = data
            .get("transactionID")
            .and_then(Value::as_str)
            .ok_or("Relayer acknowledgement ID missing")?;
        validate_request_id(id)?;
        Ok(id.into())
    }
    async fn transaction_hint(&self, id: &str) -> Result<Option<String>, String> {
        validate_request_id(id)?;
        let response = self
            .http
            .get(format!(
                "{}/v1/account/transactions/{id}",
                self.settings.url
            ))
            .send()
            .await
            .map_err(|_| "Relayer transaction lookup failed")?;
        let data = Self::response(response).await?;
        if data.get("transaction_id").and_then(Value::as_str) != Some(id) {
            return Err("Relayer transaction ID mismatch".into());
        }
        match data.get("state").and_then(Value::as_str) {
            Some(
                "STATE_NEW" | "STATE_EXECUTED" | "STATE_MINED" | "STATE_CONFIRMED" | "STATE_FAILED"
                | "STATE_INVALID",
            ) => {}
            _ => return Err("Relayer transaction state unrecognized".into()),
        }
        match data.get("transaction_hash") {
            Some(Value::String(s)) if !s.is_empty() => {
                parse_hash(s)?;
                Ok(Some(s.clone()))
            }
            Some(Value::Null) | Some(Value::String(_)) => Ok(None),
            _ => Err("Relayer transaction hash field missing".into()),
        }
    }
}

impl DirectChain {
    /// Canonical native-beacon wallet only. Legacy UUPS wallets require a separately reviewed
    /// identity path; a nonempty owner code is allowed because EIP7702 preserves raw-key ECDSA.
    pub async fn verify_deposit_wallet(&self, block: &str) -> Result<u64, String> {
        if self.live.signature_type != 3 || self.live.chain_id != 137 {
            return Err("DepositWallet requires Polygon signature type 3".into());
        }
        let wallet = address(&self.live.funder_address)?;
        let owner = address(&self.live.signer_address)?;
        let owner_id = B256::from(U256::from_be_slice(owner.as_slice()).to_be_bytes::<32>());
        let (actual_owner, pending, factory, id, paused, nonce, predicted) = tokio::try_join!(
            self.address_call(wallet, DepositWallet::ownerCall {}.abi_encode(), block),
            self.address_call(
                wallet,
                DepositWallet::pendingOwnerCall {}.abi_encode(),
                block
            ),
            self.address_call(wallet, DepositWallet::factoryCall {}.abi_encode(), block),
            self.call(wallet, DepositWallet::idCall {}.abi_encode(), block),
            self.uint_call(wallet, DepositWallet::pausedCall {}.abi_encode(), block),
            self.uint_call(wallet, DepositWallet::nonceCall {}.abi_encode(), block),
            self.address_call(
                DEPOSIT_FACTORY,
                DepositFactory::predictWalletAddressCall { id: owner_id }.abi_encode(),
                block
            ),
        )?;
        if actual_owner != owner
            || !pending.is_zero()
            || factory != DEPOSIT_FACTORY
            || id.as_slice() != owner_id.as_slice()
            || !paused.is_zero()
            || predicted != wallet
        {
            return Err("DepositWallet owner/factory/ID/pause identity mismatch".into());
        }
        let beacon = storage_address(
            &self
                .rpc(
                    "eth_getStorageAt",
                    json!([format!("{wallet:#x}"), BEACON_SLOT, block]),
                )
                .await?,
        )?;
        let factory_impl = storage_address(
            &self
                .rpc(
                    "eth_getStorageAt",
                    json!([format!("{DEPOSIT_FACTORY:#x}"), IMPLEMENTATION_SLOT, block]),
                )
                .await?,
        )?;
        if beacon != DEPOSIT_BEACON || factory_impl != FACTORY_IMPLEMENTATION {
            return Err("DepositWallet beacon/factory implementation identity mismatch".into());
        }
        // Beacon resolves a per-wallet pin using msg.sender. Calling from zero is insufficient.
        let impl_result = self.rpc("eth_call", json!([{"from":format!("{wallet:#x}"),"to":format!("{beacon:#x}"),"data":hex_data(&Beacon::implementationCall {}.abi_encode())},block])).await?;
        if storage_address(&impl_result)? != DEPOSIT_IMPLEMENTATION {
            return Err("DepositWallet effective implementation is not pinned".into());
        }
        for a in [
            wallet,
            DEPOSIT_FACTORY,
            DEPOSIT_BEACON,
            DEPOSIT_IMPLEMENTATION,
            FACTORY_IMPLEMENTATION,
        ] {
            let code = self
                .rpc("eth_getCode", json!([format!("{a:#x}"), block]))
                .await?;
            if hex_bytes(
                code.as_str().ok_or("DepositWallet code missing")?,
                MAX_RPC_BYTES,
            )?
            .is_empty()
            {
                return Err("DepositWallet deployment code missing".into());
            }
        }
        let magic = self
            .call(
                wallet,
                DepositWallet::walletInterfaceIdCall {}.abi_encode(),
                block,
            )
            .await?;
        if magic.as_slice() != keccak256("Polymarket.DepositWallet").as_slice() {
            return Err("DepositWallet interface identity mismatch".into());
        }
        let raw_domain = self
            .call(
                wallet,
                DepositWallet::eip712DomainCall {}.abi_encode(),
                block,
            )
            .await?;
        let domain = DepositWallet::eip712DomainCall::abi_decode_returns(&raw_domain)
            .map_err(|_| "invalid DepositWallet EIP712 domain ABI")?;
        if domain.fields.as_slice() != [0x0f]
            || domain.name != "DepositWallet"
            || domain.version != "1"
            || domain.chainId != U256::from(137)
            || domain.verifyingContract != wallet
            || domain.salt != B256::ZERO
            || !domain.extensions.is_empty()
        {
            return Err(
                "DepositWallet EIP712 domain differs from pinned batch/order signing domain".into(),
            );
        }
        bounded_u64(nonce)
    }
    pub async fn prepare_relay(
        &self,
        transport: &RelayTransport,
        prepared: &PreparedChainTransaction,
        block_number: u64,
        block_hash: &str,
        block_timestamp: u64,
    ) -> Result<PreparedRelay, String> {
        prepared.plan.validate()?;
        validate_economics(prepared)?;
        parse_hash(block_hash)?;
        let nonce = self
            .verify_deposit_wallet(&format!("0x{block_number:x}"))
            .await?;
        if nonce != prepared.nonce
            || transport.authenticated_nonce(self.signer.address()).await? != nonce
        {
            return Err("DepositWallet chain/relayer nonce mismatch; no replacement signed".into());
        }
        let timestamp = block_timestamp.max(prepared.prepared_at_ms / 1000);
        let deadline = timestamp
            .checked_add(transport.settings.deadline_seconds)
            .ok_or("DepositWallet deadline overflow")?;
        let batch = make_batch(&self.live, prepared, deadline)?;
        let hash = batch_hash(&self.live, &batch)?;
        let signature = self
            .signer
            .sign_hash(&hash)
            .await
            .map_err(|_| "cannot sign authorized DepositWallet batch")?
            .to_string();
        let signature = if signature.starts_with("0x") {
            signature
        } else {
            format!("0x{signature}")
        };
        let body = make_body(&self.live, prepared, deadline, &signature)?;
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{block_number:x}"), false]),
            )
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(block_hash) {
            return Err("DepositWallet preparation block changed canonical identity".into());
        }
        Ok(PreparedRelay {
            batch_hash: format!("{hash:#x}"),
            deadline,
            prepared_block_number: block_number,
            prepared_block_hash: block_hash.into(),
            body,
            signature,
            dispatched: false,
            request_id: None,
        })
    }
    pub async fn lookup_relay(
        &self,
        transport: &RelayTransport,
        prepared: &PreparedChainTransaction,
        relay: &PreparedRelay,
    ) -> Result<Option<RelayCandidate>, String> {
        verify_prepared_relay(&self.live, prepared, relay)?;
        if !relay.dispatched {
            return Ok(None);
        }
        self.canonical_preparation(relay).await?;
        // Acknowledgement lookup errors do not prevent independent onchain recovery.
        if let Some(id) = &relay.request_id
            && let Ok(Some(hash)) = transport.transaction_hint(id).await
            && let Some(candidate) = self.relay_candidate(prepared, relay, &hash).await?
        {
            return Ok(Some(candidate));
        }
        let finalized = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let height = quantity_u64(
            finalized
                .get("number")
                .ok_or("relay finalized block missing")?,
        )?;
        let mut start = {
            let mut cursor = transport
                .cursor
                .lock()
                .map_err(|_| "relay scan cursor unavailable")?;
            if cursor
                .as_ref()
                .is_none_or(|(hash, _)| hash != &relay.batch_hash)
            {
                *cursor = Some((relay.batch_hash.clone(), relay.prepared_block_number));
            }
            cursor.as_ref().ok_or("relay scan cursor missing")?.1
        };
        for _ in 0..transport.settings.max_scan_pages {
            if start > height {
                break;
            }
            let end = start
                .saturating_add(transport.settings.log_chunk_blocks - 1)
                .min(height);
            let logs = self.rpc("eth_getLogs", json!([{"address":prepared.funder_address,"fromBlock":format!("0x{start:x}"),"toBlock":format!("0x{end:x}"),"topics":[format!("{:#x}",DepositWallet::BatchExecuted::SIGNATURE_HASH),format!("{:#x}",B256::from(U256::from(prepared.nonce).to_be_bytes::<32>()))]}])).await?;
            let logs = logs.as_array().ok_or("relay scan result is not an array")?;
            if logs.len() > 1 {
                return Err("relay scan returned ambiguous duplicate wallet nonce evidence".into());
            }
            if let Some(log) = logs.first() {
                validate_batch_log(log, prepared, start, end)?;
                let hash = log
                    .get("transactionHash")
                    .and_then(Value::as_str)
                    .ok_or("relay batch log transaction hash missing")?;
                // Do not advance beyond evidence whose extra confirmation margin is pending.
                if let Some(candidate) = self.relay_candidate(prepared, relay, hash).await? {
                    return Ok(Some(candidate));
                }
                return Ok(None);
            }
            start = end.checked_add(1).ok_or("relay scan height overflow")?;
            *transport
                .cursor
                .lock()
                .map_err(|_| "relay scan cursor unavailable")? =
                Some((relay.batch_hash.clone(), start));
        }
        Ok(None)
    }
    async fn canonical_preparation(&self, relay: &PreparedRelay) -> Result<(), String> {
        let block = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", relay.prepared_block_number), false]),
            )
            .await?;
        if block.get("hash").and_then(Value::as_str) != Some(relay.prepared_block_hash.as_str()) {
            return Err("relay preparation block is not canonical; retain uncertain intent".into());
        }
        Ok(())
    }
    async fn relay_candidate(
        &self,
        prepared: &PreparedChainTransaction,
        relay: &PreparedRelay,
        hash: &str,
    ) -> Result<Option<RelayCandidate>, String> {
        parse_hash(hash)?;
        let receipt = self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
        if receipt.is_null() {
            return Ok(None);
        }
        if receipt.get("transactionHash").and_then(Value::as_str) != Some(hash) {
            return Err("relay receipt transaction identity mismatch".into());
        }
        let number = quantity_u64(
            receipt
                .get("blockNumber")
                .ok_or("relay receipt block missing")?,
        )?;
        let receipt_hash = receipt
            .get("blockHash")
            .and_then(Value::as_str)
            .ok_or("relay receipt block hash missing")?;
        parse_hash(receipt_hash)?;
        let finalized = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let finalized_height = quantity_u64(
            finalized
                .get("number")
                .ok_or("relay finalized height missing")?,
        )?;
        if number < relay.prepared_block_number
            || finalized_height
                < number
                    .checked_add(self.settings.minimum_confirmations - 1)
                    .ok_or("relay finality height overflow")?
        {
            return Ok(None);
        }
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(receipt_hash) {
            return Err("relay receipt block is not canonical".into());
        }
        let status = quantity_u64(
            receipt
                .get("status")
                .ok_or("relay receipt status missing")?,
        )?;
        if status == 0 {
            return Ok(None);
        }
        if status != 1 {
            return Err("invalid relay receipt status".into());
        }
        let transaction = self.rpc("eth_getTransactionByHash", json!([hash])).await?;
        verify_factory_transaction(&self.live, prepared, relay, &transaction, &receipt)?;
        verify_wallet_events(&self.live, prepared, relay, &receipt)?;
        self.canonical_preparation(relay).await?;
        Ok(Some(RelayCandidate {
            transaction_hash: hash.into(),
            receipt,
        }))
    }
    /// Expiry is a distinct proof, never a fabricated receipt. Collateral may change through
    /// unrelated markets; same-condition token prestate and wallet nonce must remain exact.
    pub async fn expired_relay(
        &self,
        prepared: &PreparedChainTransaction,
        relay: &PreparedRelay,
    ) -> Result<Option<RelayExpiry>, String> {
        verify_prepared_relay(&self.live, prepared, relay)?;
        if !relay.dispatched {
            return Ok(None);
        }
        self.canonical_preparation(relay).await?;
        let finalized = self
            .rpc("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let number = quantity_u64(
            finalized
                .get("number")
                .ok_or("relay expiry final height missing")?,
        )?;
        let timestamp = quantity_u64(
            finalized
                .get("timestamp")
                .ok_or("relay expiry timestamp missing")?,
        )?;
        let hash = finalized
            .get("hash")
            .and_then(Value::as_str)
            .ok_or("relay expiry block hash missing")?;
        parse_hash(hash)?;
        if number < relay.prepared_block_number || timestamp <= relay.deadline {
            return Ok(None);
        }
        let tag = format!("0x{number:x}");
        let nonce = self.verify_deposit_wallet(&tag).await?;
        let (tokens, cash) = self.balances(&prepared.plan, &tag).await?;
        if nonce != prepared.nonce || tokens != prepared.before_tokens_micros {
            return Ok(None);
        }
        let canonical = self
            .rpc("eth_getBlockByNumber", json!([tag, false]))
            .await?;
        if canonical.get("hash").and_then(Value::as_str) != Some(hash) {
            return Err("relay expiry block changed canonical identity".into());
        }
        Ok(Some(RelayExpiry {
            intent_hash: relay.batch_hash.clone(),
            block_number: number,
            block_hash: hash.into(),
            finalized_timestamp_seconds: timestamp,
            wallet_nonce: nonce,
            retained_tokens_micros: tokens,
            collateral_micros: cash,
        }))
    }
}

pub fn verify_prepared_relay(
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    relay: &PreparedRelay,
) -> Result<(), String> {
    live.validate()?;
    p.plan.validate()?;
    for id in &p.plan.token_ids {
        token_id(id)?;
    }
    if live.signature_type != 3
        || live.chain_id != 137
        || address(&p.signer_address)? != address(&live.signer_address)?
        || address(&p.funder_address)? != address(&live.funder_address)?
        || !p.raw_transaction.is_empty()
        || p.safe_nonce.is_some()
        || p.safe_transaction_hash.is_some()
        || p.gas_limit != 0
        || p.max_fee_per_gas_wei != 0
        || p.max_priority_fee_per_gas_wei != 0
        || p.maximum_gas_cost_wei != "0"
        || p.prepared_at_ms == 0
        || relay.body.len() > MAX_RAW_BYTES
        || relay.prepared_block_number == 0
        || !(60..=3600).contains(&relay.deadline.saturating_sub(p.prepared_at_ms / 1000))
        || (!relay.dispatched && relay.request_id.is_some())
    {
        return Err("invalid durable gasless closeout identity/bounds".into());
    }
    parse_hash(&relay.prepared_block_hash)?;
    if let Some(id) = &relay.request_id {
        validate_request_id(id)?;
    }
    let batch = make_batch(live, p, relay.deadline)?;
    let hash = batch_hash(live, &batch)?;
    if p.hash != format!("{hash:#x}")
        || relay.batch_hash != p.hash
        || relay.body != make_body(live, p, relay.deadline, &relay.signature)?
    {
        return Err("durable relayer body differs from exact authorized intent".into());
    }
    let signature = Signature::from_str(&relay.signature)
        .map_err(|_| "invalid durable owner batch signature")?;
    if signature.normalize_s().is_some()
        || signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| "invalid owner batch recovery")?
            != address(&live.signer_address)?
    {
        return Err("durable batch signature owner mismatch".into());
    }
    if hex_bytes(&relay.signature, 65)?.len() != 65 {
        return Err("only canonical 65-byte owner batch signatures supported".into());
    }
    validate_economics(p)
}
fn validate_economics(p: &PreparedChainTransaction) -> Result<(), String> {
    match p.plan.operation {
        ChainOperation::Merge { amount_micros } => {
            if p.before_tokens_micros.iter().any(|q| *q < amount_micros)
                || p.expected_payout_micros != amount_micros
                || p.payout_numerators != [0, 0]
                || p.payout_denominator != 0
            {
                return Err("invalid durable relay merge prestate".into());
            }
        }
        ChainOperation::Redeem => {
            let [a, b] = p.payout_numerators;
            let d = p.payout_denominator;
            if p.before_tokens_micros == [0, 0]
                || d == 0
                || u128::from(a) + u128::from(b) != u128::from(d)
                || u128::from(p.expected_payout_micros)
                    != u128::from(p.before_tokens_micros[0]) * u128::from(a) / u128::from(d)
                        + u128::from(p.before_tokens_micros[1]) * u128::from(b) / u128::from(d)
            {
                return Err("invalid durable relay resolution prestate".into());
            }
        }
    }
    Ok(())
}
fn make_batch(
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    deadline: u64,
) -> Result<Batch, String> {
    Ok(Batch {
        wallet: address(&live.funder_address)?,
        nonce: U256::from(p.nonce),
        deadline: U256::from(deadline),
        calls: vec![Call {
            target: STANDARD_ADAPTER,
            value: U256::ZERO,
            data: inner_call(&p.plan, address(&live.collateral_address)?)?.into(),
        }],
    })
}
fn batch_hash(live: &LiveSettings, batch: &Batch) -> Result<B256, String> {
    Ok(batch.eip712_signing_hash(&alloy::sol_types::eip712_domain! { name: "DepositWallet", version: "1", chain_id: 137, verifying_contract: address(&live.funder_address)?, }))
}
fn verify_envelope(owner: Address, relay: &PreparedRelay) -> Result<(), String> {
    let body: RelayBody =
        serde_json::from_str(&relay.body).map_err(|_| "invalid durable Relayer body")?;
    if body.kind != "WALLET"
        || address(&body.from)? != owner
        || address(&body.to)? != DEPOSIT_FACTORY
        || body.signature != relay.signature
        || body.params.calls.len() != 1
        || body.params.deadline != relay.deadline.to_string()
        || body.metadata.len() > 132
        || !body.metadata.starts_with("mm2:")
        || serde_json::to_string(&body).map_err(|_| "invalid canonical Relayer body")? != relay.body
    {
        return Err(
            "Relayer dispatch differs from canonical complete signed owner envelope".into(),
        );
    }
    let call = &body.params.calls[0];
    let wallet = address(&body.params.wallet)?;
    let data = hex_bytes(&call.data, MAX_RAW_BYTES)?;
    if wallet.is_zero()
        || address(&call.target)? != STANDARD_ADAPTER
        || call.value != "0"
        || !(data.starts_with(&Adapter::mergePositionsCall::SELECTOR)
            || data.starts_with(&Adapter::redeemPositionsCall::SELECTOR))
    {
        return Err("Relayer dispatch call differs from bounded standard closeout".into());
    }
    let batch = Batch {
        wallet,
        nonce: U256::from(decimal_u64(&body.nonce)?),
        deadline: U256::from(relay.deadline),
        calls: vec![Call {
            target: STANDARD_ADAPTER,
            value: U256::ZERO,
            data: data.into(),
        }],
    };
    let digest = batch.eip712_signing_hash(&alloy::sol_types::eip712_domain! {name:"DepositWallet",version:"1",chain_id:137,verifying_contract:wallet,});
    let signature = Signature::from_str(&relay.signature)
        .map_err(|_| "invalid dispatch owner batch signature")?;
    if relay.batch_hash != format!("{digest:#x}")
        || signature.normalize_s().is_some()
        || hex_bytes(&relay.signature, 65)?.len() != 65
        || signature
            .recover_address_from_prehash(&digest)
            .map_err(|_| "invalid dispatch signature recovery")?
            != owner
    {
        return Err("Relayer dispatch batch hash/signature mismatch".into());
    }
    Ok(())
}
fn make_body(
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    deadline: u64,
    signature: &str,
) -> Result<String, String> {
    serde_json::to_string(&RelayBody {
        kind: "WALLET".into(),
        from: format!("{:#x}", address(&live.signer_address)?),
        to: format!("{DEPOSIT_FACTORY:#x}"),
        nonce: p.nonce.to_string(),
        signature: signature.into(),
        metadata: format!("mm2:{}", p.plan.id),
        params: RelayParams {
            wallet: format!("{:#x}", address(&live.funder_address)?),
            deadline: deadline.to_string(),
            calls: vec![RelayCall {
                target: format!("{STANDARD_ADAPTER:#x}"),
                value: "0".into(),
                data: hex_data(&inner_call(&p.plan, address(&live.collateral_address)?)?),
            }],
        },
    })
    .map_err(|_| "cannot serialize bounded DepositWallet intent".into())
}
pub(crate) fn validate_request_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("invalid bounded Relayer request ID".into());
    }
    Ok(())
}

/// Public key 1 only. No environment access, RPC, relayer requests, or economic effects.
#[cfg(test)]
pub(crate) async fn fixture_prepared(
    plan: ChainPlan,
    before: [u64; 2],
) -> (LiveSettings, PreparedChainTransaction) {
    let (mut live, mut p) = super::fixture_prepared(plan, before).await;
    let signer = PrivateKeySigner::from_str(
        "0000000000000000000000000000000000000000000000000000000000000001",
    )
    .unwrap();
    live.signature_type = 3;
    live.funder_address = format!("{:#x}", Address::repeat_byte(0x55));
    live.owner_address = live.funder_address.clone();
    live.chain = Some(ChainSettings {
        relayer: Some(RelaySettings::default()),
        ..Default::default()
    });
    p.funder_address = live.funder_address.clone();
    p.raw_transaction.clear();
    p.nonce = 7;
    p.safe_nonce = None;
    p.safe_transaction_hash = None;
    p.gas_limit = 0;
    p.max_fee_per_gas_wei = 0;
    p.max_priority_fee_per_gas_wei = 0;
    p.maximum_gas_cost_wei = "0".into();
    p.prepared_at_ms = 1_000_000;
    let deadline = 1600;
    let digest = batch_hash(&live, &make_batch(&live, &p, deadline).unwrap()).unwrap();
    let signature = signer.sign_hash(&digest).await.unwrap().to_string();
    let signature = if signature.starts_with("0x") {
        signature
    } else {
        format!("0x{signature}")
    };
    p.hash = format!("{digest:#x}");
    p.relayer = Some(PreparedRelay {
        batch_hash: p.hash.clone(),
        deadline,
        prepared_block_number: 100,
        prepared_block_hash: format!("0x{:064x}", 100),
        body: make_body(&live, &p, deadline, &signature).unwrap(),
        signature,
        dispatched: false,
        request_id: None,
    });
    (live, p)
}
fn decimal_u64(s: &str) -> Result<u64, String> {
    u64::try_from(decimal_u128(s)?).map_err(|_| "Relayer nonce exceeds u64 bound".into())
}
fn storage_address(value: &Value) -> Result<Address, String> {
    let bytes = hex_bytes(
        value
            .as_str()
            .ok_or("invalid DepositWallet storage result")?,
        32,
    )?;
    if bytes.len() != 32 || bytes[..12].iter().any(|b| *b != 0) {
        return Err("invalid DepositWallet address storage word".into());
    }
    Ok(Address::from_slice(&bytes[12..]))
}
fn validate_batch_log(
    log: &Value,
    p: &PreparedChainTransaction,
    start: u64,
    end: u64,
) -> Result<(), String> {
    let topics = log
        .get("topics")
        .and_then(Value::as_array)
        .ok_or("relay batch topics missing")?;
    let height = quantity_u64(log.get("blockNumber").ok_or("relay batch block missing")?)?;
    if address(
        log.get("address")
            .and_then(Value::as_str)
            .ok_or("relay batch emitter missing")?,
    )? != address(&p.funder_address)?
        || topics.len() != 2
        || topics[0].as_str().and_then(|s| parse_hash(s).ok())
            != Some(DepositWallet::BatchExecuted::SIGNATURE_HASH)
        || topics[1].as_str().and_then(|s| parse_hash(s).ok())
            != Some(B256::from(U256::from(p.nonce).to_be_bytes::<32>()))
        || log.get("data").and_then(Value::as_str) != Some("0x")
        || log.get("removed").and_then(Value::as_bool) != Some(false)
        || height < start
        || height > end
    {
        return Err("relay scan log differs from exact wallet nonce identity".into());
    }
    parse_hash(
        log.get("blockHash")
            .and_then(Value::as_str)
            .ok_or("relay log block hash missing")?,
    )?;
    parse_hash(
        log.get("transactionHash")
            .and_then(Value::as_str)
            .ok_or("relay log transaction hash missing")?,
    )?;
    Ok(())
}
fn verify_factory_transaction(
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    relay: &PreparedRelay,
    tx: &Value,
    receipt: &Value,
) -> Result<(), String> {
    let owner = address(&live.signer_address)?;
    let from = address(
        tx.get("from")
            .and_then(Value::as_str)
            .ok_or("relay outer sender missing")?,
    )?;
    if from.is_zero()
        || from == owner
        || tx.get("hash") != receipt.get("transactionHash")
        || tx.get("blockHash") != receipt.get("blockHash")
        || tx.get("blockNumber") != receipt.get("blockNumber")
        || address(
            tx.get("to")
                .and_then(Value::as_str)
                .ok_or("relay factory target missing")?,
        )? != DEPOSIT_FACTORY
        || address(
            receipt
                .get("to")
                .and_then(Value::as_str)
                .ok_or("relay receipt factory target missing")?,
        )? != DEPOSIT_FACTORY
        || address(
            receipt
                .get("from")
                .and_then(Value::as_str)
                .ok_or("relay receipt sender missing")?,
        )? != from
        || quantity_u128(tx.get("value").ok_or("relay outer value missing")?)? != 0
    {
        return Err("relay outer transaction identity/sponsorship mismatch".into());
    }
    let raw = hex_bytes(
        tx.get("input")
            .and_then(Value::as_str)
            .ok_or("relay factory calldata missing")?,
        MAX_RPC_BYTES,
    )?;
    let decoded = DepositFactory::proxyCall::abi_decode_validate(&raw)
        .map_err(|_| "invalid bounded factory batch ABI")?;
    if decoded.batches.is_empty()
        || decoded.batches.len() > 32
        || decoded.signatures.len() != decoded.batches.len()
        || decoded.abi_encode() != raw
    {
        return Err("invalid canonical factory batch bounds".into());
    }
    let expected = make_batch(live, p, relay.deadline)?;
    let signature = hex_bytes(&relay.signature, 65)?;
    let mut matches = 0;
    for (batch, sig) in decoded.batches.iter().zip(&decoded.signatures) {
        if batch.wallet == expected.wallet {
            if batch.abi_encode() != expected.abi_encode() || sig.as_ref() != signature {
                return Err(
                    "factory wallet batch differs from durable complete batch/signature".into(),
                );
            }
            matches += 1;
        }
    }
    if matches != 1 {
        return Err("factory transaction does not uniquely contain authorized wallet batch".into());
    }
    Ok(())
}
fn verify_wallet_events(
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    relay: &PreparedRelay,
    receipt: &Value,
) -> Result<(), String> {
    let wallet = address(&live.funder_address)?;
    let expected = make_batch(live, p, relay.deadline)?;
    let logs = receipt
        .get("logs")
        .and_then(Value::as_array)
        .ok_or("relay receipt logs missing")?;
    if logs.len() > 4096 {
        return Err("relay receipt event bound exceeded".into());
    }
    let mut execution = 0;
    let mut completed = 0;
    let mut last_execution = None;
    let mut batch_index = None;
    for (i, log) in logs.iter().enumerate() {
        let emitter = address(
            log.get("address")
                .and_then(Value::as_str)
                .ok_or("relay receipt log emitter missing")?,
        )?;
        if emitter != wallet {
            continue;
        }
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .ok_or("relay wallet event topics missing")?;
        let topic = topics
            .first()
            .and_then(Value::as_str)
            .and_then(|s| parse_hash(s).ok())
            .ok_or("relay wallet event selector invalid")?;
        let data = hex_bytes(
            log.get("data")
                .and_then(Value::as_str)
                .ok_or("relay wallet event data missing")?,
            MAX_RPC_BYTES,
        )?;
        if topic == DepositWallet::BatchExecuted::SIGNATURE_HASH {
            validate_batch_log(
                log,
                p,
                quantity_u64(
                    receipt
                        .get("blockNumber")
                        .ok_or("relay receipt height missing")?,
                )?,
                quantity_u64(
                    receipt
                        .get("blockNumber")
                        .ok_or("relay receipt height missing")?,
                )?,
            )?;
            completed += 1;
            batch_index = Some(i);
        } else if topic == DepositWallet::Execution::SIGNATURE_HASH {
            let event = DepositWallet::Execution::decode_raw_log_validate(
                topics
                    .iter()
                    .map(|v| parse_hash(v.as_str().unwrap_or("")).unwrap_or(B256::ZERO)),
                &data,
            )
            .map_err(|_| "invalid canonical wallet Execution event")?;
            if event.target != STANDARD_ADAPTER
                || event.value != U256::ZERO
                || event.data != expected.calls[0].data
                || !event.result.is_empty()
            {
                return Err("wallet execution event differs from exact closeout call".into());
            }
            execution += 1;
            last_execution = Some(i);
        } else {
            return Err("unexpected extra wallet event in closeout batch".into());
        }
        if log.get("transactionHash") != receipt.get("transactionHash")
            || log.get("blockHash") != receipt.get("blockHash")
            || log.get("blockNumber") != receipt.get("blockNumber")
            || log.get("removed").and_then(Value::as_bool) != Some(false)
        {
            return Err("wallet receipt event canonical identity mismatch".into());
        }
    }
    if execution != 1
        || completed != 1
        || last_execution.zip(batch_index).is_none_or(|(a, b)| a >= b)
    {
        return Err("wallet closeout completion event missing/duplicated/out of order".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
