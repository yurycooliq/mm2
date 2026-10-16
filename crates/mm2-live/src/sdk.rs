use crate::{
    AccountObservation, LiveOrder, LivePosition, LiveQuote, LiveSettings, OrderObservation,
    settings::address,
};
use alloy::{
    dyn_abi::Eip712Domain,
    primitives::{Address, Signature, keccak256},
    signers::local::PrivateKeySigner,
    sol_types::{SolStruct, SolValue},
};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use mm2_core::Outcome;
use polymarket_client_sdk_v2 as sdk;
use sdk::{
    auth::{Credentials, Normal, Signer, Uuid, state::Authenticated},
    clob::{
        Client, Config,
        types::{
            OrderPayload, OrderSignature, OrderStatusType, OrderType, OrderV2, Side, SignatureType,
            SignedOrder, TradeStatusType,
            request::{BalanceAllowanceRequest, OrdersRequest, TradesRequest},
            response::{ClobMarketInfoResponse, MarketResponse},
        },
        ws::{WsMessage, types::request::SubscriptionRequest},
    },
    data::{Client as DataClient, types::request::PositionsRequest},
    types::{B256, Decimal, U256},
    ws::WithCredentials,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedOrder {
    pub order_id: String,
    pub quote: LiveQuote,
    pub signed_wire: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_policy: Option<crate::fees::FeePolicy>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostResult {
    Accepted { order_id: String },
    Rejected,
}
#[derive(Clone, Debug)]
pub enum VenueNotice {
    Connected(bool),
    OrderChanged(String),
    AccountChanged,
    Fault(String),
}

/// Network seam. Tests supply an in-memory venue and never connect an authenticated account.
#[async_trait]
pub trait Venue: Send + Sync + 'static {
    async fn prepare(&self, quote: LiveQuote) -> Result<PreparedOrder, String>;
    async fn post(&self, order: PreparedOrder) -> Result<PostResult, String>;
    async fn cancel(&self, ids: Vec<String>) -> Result<Vec<String>, String>;
    async fn reconcile(&self, orders: Vec<LiveOrder>) -> Result<AccountObservation, String>;
    async fn heartbeat(&self) -> Result<(), String>;
    async fn listen(&self, events: mpsc::Sender<VenueNotice>, shutdown: watch::Receiver<bool>);
    async fn prepare_chain(
        &self,
        _: crate::ChainPlan,
    ) -> Result<crate::PreparedChainTransaction, String> {
        Err("onchain closeout is not configured".into())
    }
    async fn submit_chain(
        &self,
        _: crate::PreparedChainTransaction,
    ) -> Result<Option<String>, String> {
        Err("onchain closeout is not configured".into())
    }
    async fn lookup_chain(
        &self,
        _: crate::PreparedChainTransaction,
    ) -> Result<crate::ChainObservation, String> {
        Err("onchain closeout is not configured".into())
    }
}

/// Complete read-only query pass: bounded fanout, no partial refresh is returned on any error.
pub(crate) async fn query_active_orders<F, Fut>(
    tracked: &[LiveOrder],
    query: F,
) -> Result<Vec<OrderObservation>, String>
where
    F: Fn(String) -> Fut + Sync,
    Fut: std::future::Future<Output = Result<Option<OrderObservation>, String>> + Send,
{
    let active: Vec<_> = tracked
        .iter()
        .filter(|order| order.phase != crate::OrderPhase::Closed)
        .cloned()
        .collect();
    if active.len() > 64 {
        return Err("active reconciliation capacity exceeded".into());
    }
    let observations: Vec<Option<OrderObservation>> = futures_util::stream::iter(active)
        .map(|order| {
            let query = &query;
            async move {
                let observation = query(order.order_id.clone()).await?;
                if let Some(obs) = &observation
                    && (obs.order_id != order.order_id
                        || obs.condition_id != order.quote.condition_id
                        || obs.token_id != order.quote.token_id
                        || obs.price_micros != order.quote.price_micros
                        || obs.original_quantity_micros != order.quote.quantity_micros
                        || obs.cumulative_filled_micros > order.quote.quantity_micros)
                {
                    return Err(String::from("authoritative active query identity mismatch"));
                }
                Ok(observation)
            }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    Ok(observations.into_iter().flatten().collect())
}

pub(crate) struct SdkVenue {
    settings: LiveSettings,
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    data: DataClient,
    signed: Mutex<BTreeMap<String, (SignedOrder, u64)>>,
    heartbeat_id: Mutex<Option<Uuid>>,
    public_http: reqwest::Client,
    protocol_cache: Mutex<Option<(u64, u32)>>,
    market_cache: Mutex<BTreeMap<String, (u64, MarketResponse)>>,
    gamma_cache: Mutex<BTreeMap<String, (u64, GammaIdentity)>>,
    token_cache: Mutex<BTreeMap<String, (u64, ClobMarketInfoResponse)>>,
    fee_cache: Mutex<BTreeMap<String, (u64, crate::fees::FeePolicy)>>,
    fee_receipts: Mutex<BTreeMap<String, Vec<crate::fees::ExecutionFee>>>,
    chain: Option<crate::chain::DirectChain>,
}
const METADATA_TTL_MS: u64 = 1000;
const MAX_GAMMA_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct GammaIdentity {
    condition_id: B256,
    slug: String,
    tokens: [U256; 2],
    start_ms: u64,
    end_ms: u64,
}

fn gamma_pair(raw: &serde_json::Value, key: &str) -> Result<[String; 2], String> {
    let raw = raw
        .get(key)
        .ok_or("required Gamma token metadata missing")?;
    let decoded = if let Some(text) = raw.as_str() {
        if text.len() > 4096 {
            return Err("Gamma token metadata exceeds bound".into());
        }
        serde_json::from_str(text).map_err(|_| "invalid Gamma token metadata")?
    } else {
        raw.clone()
    };
    let pair = decoded.as_array().ok_or("invalid Gamma token pair")?;
    if pair.len() != 2 {
        return Err("Gamma token pair cardinality mismatch".into());
    }
    Ok([
        pair[0].as_str().ok_or("invalid Gamma token member")?.into(),
        pair[1].as_str().ok_or("invalid Gamma token member")?.into(),
    ])
}

fn gamma_timestamp(raw: &serde_json::Value, key: &str) -> Result<u64, String> {
    let text = raw
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or("Gamma window timestamp missing")?;
    let stamp = chrono::DateTime::parse_from_rfc3339(text)
        .map_err(|_| "invalid Gamma window timestamp")?
        .timestamp_millis();
    u64::try_from(stamp).map_err(|_| "negative Gamma window timestamp".into())
}

fn gamma_identity(raw: &serde_json::Value, quote: &LiveQuote) -> Result<GammaIdentity, String> {
    let canonical_slug = format!("btc-updown-5m-{}", quote.window_start_ms / 1000);
    let crypto = raw
        .get("cryptoMarketConfig")
        .ok_or("Gamma reference contract missing")?;
    if !quote.window_start_ms.is_multiple_of(300_000)
        || quote.window_start_ms.checked_add(300_000) != Some(quote.window_end_ms)
        || quote.market_slug != canonical_slug
        || raw.get("slug").and_then(serde_json::Value::as_str) != Some(&canonical_slug)
        || raw.get("active").and_then(serde_json::Value::as_bool) != Some(true)
        || raw.get("closed").and_then(serde_json::Value::as_bool) != Some(false)
        || raw
            .get("acceptingOrders")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || raw.get("negRisk").and_then(serde_json::Value::as_bool) != Some(false)
        || gamma_timestamp(raw, "eventStartTime")? != quote.window_start_ms
        || gamma_timestamp(raw, "endDate")? != quote.window_end_ms
        || crypto.get("asset").and_then(serde_json::Value::as_str) != Some("btc")
        || crypto.get("duration").and_then(serde_json::Value::as_str) != Some("5m")
        || crypto
            .get("twapEnabled")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || crypto
            .get("twapLookbackSeconds")
            .and_then(serde_json::Value::as_u64)
            != Some(60)
        || gamma_pair(raw, "outcomes")? != ["Up", "Down"]
    {
        return Err("Gamma identity/status/reference/five-minute window mismatch".into());
    }
    let condition_id = B256::from_str(
        raw.get("conditionId")
            .and_then(serde_json::Value::as_str)
            .ok_or("Gamma condition identity missing")?,
    )
    .map_err(|_| "invalid Gamma condition")?;
    if condition_id != B256::from_str(&quote.condition_id).map_err(|_| "invalid quote condition")? {
        return Err("Gamma condition identity differs from quote".into());
    }
    let [up, down] = gamma_pair(raw, "clobTokenIds")?;
    let token = |text: &str| -> Result<U256, String> {
        let parsed = U256::from_str(text).map_err(|_| "invalid Gamma token identifier")?;
        if parsed.is_zero() || parsed.to_string() != text {
            return Err("noncanonical Gamma token identifier".into());
        }
        Ok(parsed)
    };
    let tokens = [token(&up)?, token(&down)?];
    if tokens[0] == tokens[1] {
        return Err("Gamma outcomes share one token identifier".into());
    }
    Ok(GammaIdentity {
        condition_id,
        slug: canonical_slug,
        tokens,
        start_ms: quote.window_start_ms,
        end_ms: quote.window_end_ms,
    })
}

fn validate_clob_market(
    market: &MarketResponse,
    gamma: &GammaIdentity,
    quote: &LiveQuote,
) -> Result<(), String> {
    let token = U256::from_str(&quote.token_id).map_err(|_| "invalid token identifier")?;
    let index = if quote.outcome == Outcome::Up { 0 } else { 1 };
    if market.condition_id != Some(gamma.condition_id)
        || gamma.condition_id
            != B256::from_str(&quote.condition_id).map_err(|_| "invalid quote condition")?
        || market.market_slug != gamma.slug
        || gamma.slug != quote.market_slug
        || gamma.start_ms != quote.window_start_ms
        || gamma.end_ms != quote.window_end_ms
        || gamma.tokens[index] != token
        || !market.enable_order_book
        || !market.active
        || market.closed
        || market.archived
        || !market.accepting_orders
        || market.neg_risk
        || market.tokens.len() != 2
        || !gamma
            .tokens
            .iter()
            .zip(["Up", "Down"])
            .all(|(token, outcome)| {
                market
                    .tokens
                    .iter()
                    .any(|t| t.token_id == *token && t.outcome == outcome)
            })
    {
        return Err(
            "authoritative Gamma/CLOB market identity/window/accepting state mismatch".into(),
        );
    }
    // CLOB end_date_iso is observed as a midnight date for these intraday markets.
    // Gamma's exact window, canonical slug and both token/outcome identities establish expiry.
    Ok(())
}
fn prune_cache<T>(cache: &mut BTreeMap<String, (u64, T)>) {
    if cache.len() >= 64
        && let Some(key) = cache
            .iter()
            .min_by_key(|(_, (time, _))| *time)
            .map(|(key, _)| key.clone())
    {
        cache.remove(&key);
    }
}
pub(crate) fn maker_fee_bound(
    fee: &sdk::clob::types::response::FeeDetails,
    price_micros: u64,
) -> Result<u32, String> {
    if fee.rate.is_sign_negative() || fee.rate > Decimal::ONE || fee.exponent > 16 {
        return Err("unsupported V2 fee parameters".into());
    }
    if fee.taker_only {
        return Ok(0);
    }
    let price = decimal(price_micros);
    let mut fee_per_share = fee.rate;
    for _ in 0..fee.exponent {
        fee_per_share = fee_per_share
            .checked_mul(price * (Decimal::ONE - price))
            .ok_or("V2 fee calculation overflow")?;
    }
    let bps = (fee_per_share / price * Decimal::from(10_000))
        .ceil()
        .normalize()
        .to_string()
        .parse::<u32>()
        .map_err(|_| "invalid V2 fee bound")?;
    if bps > 10_000 {
        return Err("V2 maker fee exceeds supported conservative bound".into());
    }
    Ok(bps)
}
fn secret_env(name: &str) -> Result<String, String> {
    let value =
        std::env::var(name).map_err(|_| "required private runtime environment variable missing")?;
    if value.is_empty() || value.len() > 512 {
        return Err("invalid private runtime value".into());
    }
    Ok(value)
}
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
pub(crate) fn micro(value: Decimal) -> Result<u64, String> {
    let scaled = value * Decimal::from(1_000_000);
    if scaled.is_sign_negative() || !scaled.fract().is_zero() {
        return Err("venue decimal is not exact micro units".into());
    }
    scaled
        .normalize()
        .to_string()
        .parse()
        .map_err(|_| "venue decimal exceeds exact unit bound".into())
}
fn raw_micro(value: Decimal) -> Result<u64, String> {
    // CLOB balance/allowance responses use collateral base units, not decimal dollars.
    if value.is_sign_negative() || !value.fract().is_zero() {
        return Err("invalid collateral base units".into());
    }
    value
        .normalize()
        .to_string()
        .parse()
        .map_err(|_| "collateral exceeds unit bound".into())
}
fn decimal(value: u64) -> Decimal {
    (Decimal::from(value) / Decimal::from(1_000_000)).normalize()
}
fn map_error(_: sdk::error::Error) -> String {
    "authenticated SDK request failed; outcome requires reconciliation".into()
}

// SDK 0.7.0 ERC-7739 type strings; factory/beacon/owner acceptance belongs to chain readiness.
const ORDER_TYPE_STRING: &str = concat!(
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,",
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,",
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
);
const TYPED_DATA_SIGN_TYPE_STRING: &str = concat!(
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,",
    "address verifyingContract,bytes32 salt)",
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,",
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,",
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
);

fn poly1271_digest(order: &OrderV2, domain: &Eip712Domain) -> Result<B256, String> {
    let chain_id = domain
        .chain_id
        .ok_or("order signing chain identity is missing")?;
    let hash = keccak256(
        (
            keccak256(TYPED_DATA_SIGN_TYPE_STRING.as_bytes()),
            order.eip712_hash_struct(),
            keccak256(b"DepositWallet"),
            keccak256(b"1"),
            chain_id,
            order.signer,
            B256::ZERO,
        )
            .abi_encode(),
    );
    let mut bytes = [0_u8; 66];
    bytes[..2].copy_from_slice(&[0x19, 0x01]);
    bytes[2..34].copy_from_slice(domain.hash_struct().as_slice());
    bytes[34..].copy_from_slice(hash.as_slice());
    Ok(keccak256(bytes))
}

fn validate_sdk_signature(
    order: &OrderV2,
    signature: &OrderSignature,
    domain: &Eip712Domain,
    controlling_owner: Address,
) -> Result<(), String> {
    let (inner, digest) = match (order.signatureType, signature) {
        (0..=2, OrderSignature::Ecdsa(signature)) => {
            (*signature, order.eip712_signing_hash(domain))
        }
        (3, OrderSignature::Wrapped(wrapped)) => {
            let size = 65 + 32 + 32 + ORDER_TYPE_STRING.len() + 2;
            if order.maker != order.signer
                || order.signer.is_zero()
                || wrapped.len() != 2 + size * 2
                || !wrapped.starts_with("0x")
                || !wrapped[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err("noncanonical deposit-wallet signature or identity".into());
            }
            let bytes = hex::decode(&wrapped[2..])
                .map_err(|_| "invalid deposit-wallet signature encoding")?;
            if bytes[65..97] != domain.hash_struct().as_slice()[..]
                || bytes[97..129] != order.eip712_hash_struct().as_slice()[..]
                || bytes[129..size - 2] != *ORDER_TYPE_STRING.as_bytes()
                || bytes[size - 2..] != (ORDER_TYPE_STRING.len() as u16).to_be_bytes()
                || !matches!(bytes[64], 27 | 28)
            {
                return Err(
                    "deposit-wallet signature does not bind the exact order/domain/type".into(),
                );
            }
            let inner = Signature::from_raw(&bytes[..65])
                .map_err(|_| "invalid deposit-wallet owner signature")?;
            (inner, poly1271_digest(order, domain)?)
        }
        _ => return Err("SDK signature representation differs from its configured type".into()),
    };
    if inner.normalize_s().is_some()
        || inner
            .recover_address_from_prehash(&digest)
            .map_err(|_| "signed order owner recovery failed")?
            != controlling_owner
    {
        return Err("signed order does not bind the configured controlling owner".into());
    }
    Ok(())
}

impl SdkVenue {
    /// This constructs an authenticated type-state client from supplied L2 credentials.
    /// It does not derive/create keys and does not submit, cancel or call account endpoints.
    pub async fn from_env(settings: LiveSettings) -> Result<Arc<Self>, String> {
        settings.validate()?;
        let signer = PrivateKeySigner::from_str(&secret_env(&settings.private_key_env)?)
            .map_err(|_| "invalid private signer material")?
            .with_chain_id(Some(settings.chain_id));
        if signer.address() != address(&settings.signer_address)? {
            return Err("private signer and configured signer mismatch".into());
        }
        let credentials = Credentials::new(
            Uuid::from_str(&secret_env(&settings.api_key_env)?)
                .map_err(|_| "invalid API key format")?,
            secret_env(&settings.api_secret_env)?,
            secret_env(&settings.api_passphrase_env)?,
        );
        let signature = match settings.signature_type {
            0 => SignatureType::Eoa,
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            3 => SignatureType::Poly1271,
            _ => return Err("unsupported signature type".into()),
        };
        let client = Client::new(&settings.clob_url, Config::default()).map_err(map_error)?;
        let auth = client
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(signature);
        let auth = if settings.signature_type == 0 {
            auth
        } else {
            auth.funder(address(&settings.funder_address)?)
        };
        let client = auth.authenticate().await.map_err(map_error)?;
        let public_http = reqwest::Client::builder()
            .user_agent("mm2-public-metadata/1.0")
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(settings.request_timeout_ms))
            .https_only(true)
            .build()
            .map_err(|_| "cannot initialize public protocol client")?;
        let chain = match &settings.chain {
            Some(config) => Some(crate::chain::DirectChain::from_env(&settings, config).await?),
            None => None,
        };
        Ok(Arc::new(Self {
            settings,
            client,
            signer,
            data: DataClient::default(),
            signed: Mutex::new(BTreeMap::new()),
            heartbeat_id: Mutex::new(None),
            public_http,
            protocol_cache: Mutex::new(None),
            market_cache: Mutex::new(BTreeMap::new()),
            gamma_cache: Mutex::new(BTreeMap::new()),
            token_cache: Mutex::new(BTreeMap::new()),
            fee_cache: Mutex::new(BTreeMap::new()),
            fee_receipts: Mutex::new(BTreeMap::new()),
            chain,
        }))
    }
    async fn protocol(&self) -> Result<u32, String> {
        if let Some((time, value)) = *self.protocol_cache.lock().await
            && now_ms().saturating_sub(time) < METADATA_TTL_MS
        {
            return Ok(value);
        }
        #[derive(Deserialize)]
        struct Version {
            version: u32,
        }
        // SDK version() caches indefinitely. An independently refreshed public GET is a hard compatibility gate.
        let fresh = async {
            let response = self
                .public_http
                .get(format!("{}/version", self.settings.clob_url))
                .send()
                .await
                .map_err(|_| "public protocol refresh failed".to_owned())?
                .error_for_status()
                .map_err(|_| "public protocol rejected".to_owned())?;
            let body = response
                .bytes()
                .await
                .map_err(|_| "public protocol body failed".to_owned())?;
            if body.len() > 4096 {
                return Err("public protocol response exceeds bound".to_owned());
            }
            Ok(serde_json::from_slice::<Version>(&body)
                .map_err(|_| "invalid public protocol response".to_owned())?
                .version)
        };
        let (value, signing_version) = tokio::try_join!(fresh, async {
            self.client.version().await.map_err(map_error)
        })?;
        if value != signing_version {
            return Err(
                "signing SDK cache and deployed protocol differ; restart with supported SDK".into(),
            );
        }
        *self.protocol_cache.lock().await = Some((now_ms(), value));
        Ok(value)
    }
    async fn market(&self, condition: &str) -> Result<MarketResponse, String> {
        if let Some((time, market)) = self.market_cache.lock().await.get(condition)
            && now_ms().saturating_sub(*time) < METADATA_TTL_MS
        {
            return Ok(market.clone());
        }
        let market = self.client.market(condition).await.map_err(map_error)?;
        if market.market_slug.len() > 128
            || market.tokens.len() > 2
            || market.description.len() > 32_768
            || market.question.len() > 4096
            || market.tags.len() > 64
        {
            return Err("authoritative market metadata exceeds cache bounds".into());
        }
        let mut cache = self.market_cache.lock().await;
        if let Some((_, prior)) = cache.get(condition)
            && (prior.condition_id != market.condition_id
                || prior.market_slug != market.market_slug
                || prior.end_date_iso != market.end_date_iso
                || prior
                    .tokens
                    .iter()
                    .map(|t| (t.token_id, &t.outcome))
                    .collect::<Vec<_>>()
                    != market
                        .tokens
                        .iter()
                        .map(|t| (t.token_id, &t.outcome))
                        .collect::<Vec<_>>())
        {
            return Err("immutable market identity changed".into());
        }
        prune_cache(&mut cache);
        cache.insert(condition.to_owned(), (now_ms(), market.clone()));
        Ok(market)
    }
    async fn gamma(&self, quote: &LiveQuote) -> Result<GammaIdentity, String> {
        if let Some((time, identity)) = self.gamma_cache.lock().await.get(&quote.condition_id)
            && now_ms().saturating_sub(*time) < METADATA_TTL_MS
        {
            return Ok(identity.clone());
        }
        let mut response = self
            .public_http
            .get("https://gamma-api.polymarket.com/markets")
            .query(&[("slug", quote.market_slug.as_str()), ("limit", "2")])
            .send()
            .await
            .map_err(|_| "Gamma market refresh failed")?
            .error_for_status()
            .map_err(|_| "Gamma market query rejected")?;
        if response
            .content_length()
            .is_some_and(|size| size > MAX_GAMMA_BYTES as u64)
        {
            return Err("Gamma market body exceeds bound".into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "Gamma market body failed")?
        {
            if body.len().saturating_add(chunk.len()) > MAX_GAMMA_BYTES {
                return Err("Gamma market body exceeds bound".into());
            }
            body.extend_from_slice(&chunk);
        }
        let markets: Vec<serde_json::Value> =
            serde_json::from_slice(&body).map_err(|_| "invalid Gamma market response")?;
        if markets.len() != 1 {
            return Err("Gamma market query must establish exactly one identity".into());
        }
        let identity = gamma_identity(&markets[0], quote)?;
        let mut cache = self.gamma_cache.lock().await;
        if cache
            .get(&quote.condition_id)
            .is_some_and(|(_, prior)| prior != &identity)
        {
            return Err("immutable Gamma market identity changed".into());
        }
        prune_cache(&mut cache);
        cache.insert(quote.condition_id.clone(), (now_ms(), identity.clone()));
        Ok(identity)
    }
    async fn token_info(&self, quote: &LiveQuote) -> Result<ClobMarketInfoResponse, String> {
        if let Some((time, info)) = self.token_cache.lock().await.get(&quote.condition_id)
            && now_ms().saturating_sub(*time) < METADATA_TTL_MS
            && micro(info.min_order_size)? == quote.min_order_micros
            && micro(info.min_tick_size.as_decimal())? == quote.tick_micros
        {
            return Ok(info.clone());
        }
        // This V2 endpoint supplies authoritative minimum/tick, token identity and fee parameters in one request.
        let info = self
            .client
            .clob_market_info(&quote.condition_id)
            .await
            .map_err(map_error)?;
        if info.tokens.len() != 2 {
            return Err("V2 token metadata bound exceeded".into());
        }
        let mut cache = self.token_cache.lock().await;
        prune_cache(&mut cache);
        cache.insert(quote.condition_id.clone(), (now_ms(), info.clone()));
        Ok(info)
    }
    async fn fee_policy(
        &self,
        quote: &LiveQuote,
        gamma: &GammaIdentity,
    ) -> Result<crate::fees::FeePolicy, String> {
        if let Some((at, policy)) = self.fee_cache.lock().await.get(&quote.condition_id)
            && now_ms().saturating_sub(*at) < METADATA_TTL_MS
        {
            return Ok(policy.clone());
        }
        let mut response = self
            .public_http
            .get(format!(
                "{}/clob-markets/{}",
                self.settings.clob_url, quote.condition_id
            ))
            .send()
            .await
            .map_err(|_| "public fee refresh failed")?
            .error_for_status()
            .map_err(|_| "public fee refresh rejected")?;
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "public fee body failed")?
        {
            if body.len() + chunk.len() > 262_144 {
                return Err("public fee response exceeds bound".into());
            }
            body.extend_from_slice(&chunk);
        }
        let raw: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| "invalid public fee response")?;
        let policy = crate::fees::parse_policy(
            &raw,
            &quote.condition_id,
            &gamma.tokens[0].to_string(),
            &gamma.tokens[1].to_string(),
            now_ms(),
        )?;
        let mut cache = self.fee_cache.lock().await;
        prune_cache(&mut cache);
        cache.insert(quote.condition_id.clone(), (now_ms(), policy.clone()));
        Ok(policy)
    }
    fn observation(
        &self,
        raw: sdk::clob::types::response::OpenOrderResponse,
    ) -> Result<OrderObservation, String> {
        if raw.owner != self.client.credentials().key()
            || raw.maker_address != address(&self.settings.funder_address)?
            || raw.side != Side::Buy
        {
            return Err("authenticated order account/side mismatch".into());
        }
        let original = micro(raw.original_size)?;
        let filled = micro(raw.size_matched)?;
        let terminal = matches!(raw.status, OrderStatusType::Canceled)
            || (matches!(raw.status, OrderStatusType::Matched) && filled == original);
        if matches!(raw.status, OrderStatusType::Unknown(_)) {
            return Err("unknown authenticated order status".into());
        }
        Ok(OrderObservation {
            order_id: raw.id,
            condition_id: format!("{:#x}", raw.market),
            token_id: raw.asset_id.to_string(),
            price_micros: micro(raw.price)?,
            original_quantity_micros: original,
            cumulative_filled_micros: filled,
            terminal,
        })
    }
}
#[async_trait]
impl Venue for SdkVenue {
    async fn prepare(&self, mut quote: LiveQuote) -> Result<PreparedOrder, String> {
        if self.chain.is_none() {
            return Err("finalized execution fee verification RPC is not configured".into());
        }
        quote.validate(&self.settings, now_ms())?;
        let token = U256::from_str(&quote.token_id).map_err(|_| "invalid token identifier")?;
        if token.to_string() != quote.token_id {
            return Err("noncanonical token identifier".into());
        }
        let condition =
            B256::from_str(&quote.condition_id).map_err(|_| "invalid condition identifier")?;
        let (version, market, info, gamma) = tokio::try_join!(
            self.protocol(),
            self.market(&quote.condition_id),
            self.token_info(&quote),
            self.gamma(&quote)
        )?;
        if version != u32::from(self.settings.protocol) {
            return Err("deployed CLOB protocol differs from pinned V2".into());
        }
        let expected_outcome = match quote.outcome {
            Outcome::Up => "Up",
            Outcome::Down => "Down",
        };
        validate_clob_market(&market, &gamma, &quote)?;
        let fee_policy = self.fee_policy(&quote, &gamma).await?;
        if info.condition_id != condition
            || info.neg_risk
            || !info
                .tokens
                .iter()
                .flatten()
                .any(|t| t.token_id == token && t.outcome == expected_outcome)
            || micro(info.min_order_size)? != quote.min_order_micros
            || micro(info.min_tick_size.as_decimal())? != quote.tick_micros
        {
            return Err("authoritative token/minimum/tick mismatch".into());
        }
        let fee = maker_fee_bound(
            info.fee_details
                .as_ref()
                .ok_or("authoritative V2 fee parameters missing")?,
            quote.price_micros,
        )?;
        if fee > quote.fee_bps {
            return Err("actual fee exceeds approved quote fee bound".into());
        }
        quote.fee_bps = fee;
        self.client.set_tick_size(token, info.min_tick_size);
        self.client.set_neg_risk(token, false);
        let builder = self
            .client
            .limit_order()
            .token_id(token)
            .price(decimal(quote.price_micros))
            .size(decimal(quote.quantity_micros))
            .side(Side::Buy)
            .post_only(true);
        // GTD's security buffer is not a strategy freeze: use GTC in the final minute.
        let order = if quote.window_end_ms > now_ms().saturating_add(65_000) {
            let expiry = chrono::DateTime::from_timestamp_millis(quote.window_end_ms as i64)
                .ok_or("invalid GTD expiry")?;
            builder
                .order_type(OrderType::GTD)
                .expiration(expiry)
                .build()
                .await
                .map_err(map_error)?
        } else {
            builder
                .order_type(OrderType::GTC)
                .build()
                .await
                .map_err(map_error)?
        };
        let signed = self
            .client
            .sign(&self.signer, order)
            .await
            .map_err(map_error)?;
        let payload = match &signed.payload {
            OrderPayload::V2(payload) => payload,
            _ => return Err("SDK signed unsupported protocol".into()),
        };
        let expected_signer = if self.settings.signature_type == 3 {
            address(&self.settings.funder_address)?
        } else {
            self.signer.address()
        };
        let commitment = u128::from(quote.price_micros) * u128::from(quote.quantity_micros);
        if payload.order.maker != address(&self.settings.funder_address)?
            || payload.order.signer != expected_signer
            || payload.order.tokenId != token
            || payload.order.side != 0
            || payload.order.signatureType != self.settings.signature_type
            || payload.order.builder != B256::ZERO
            || !commitment.is_multiple_of(1_000_000)
            || payload.order.makerAmount != U256::from(commitment / 1_000_000)
            || payload.order.takerAmount != U256::from(quote.quantity_micros)
            || signed.owner != self.client.credentials().key()
            || signed.post_only != Some(true)
        {
            return Err("signed order differs from validated intent".into());
        }
        let domain = Eip712Domain {
            name: Some("Polymarket CTF Exchange".into()),
            version: Some("2".into()),
            chain_id: Some(U256::from(self.settings.chain_id)),
            verifying_contract: Some(address(&self.settings.exchange_address)?),
            ..Default::default()
        };
        validate_sdk_signature(
            &payload.order,
            &signed.signature,
            &domain,
            self.signer.address(),
        )?;
        let order_id = format!("{:#x}", payload.order.eip712_signing_hash(&domain));
        let signed_wire =
            serde_json::to_value(&signed).map_err(|_| "cannot encode signed order intent")?;
        let mut cache = self.signed.lock().await;
        let now = now_ms();
        cache.retain(|_, (_, expires)| *expires > now);
        if cache.len() >= 64 {
            return Err("bounded signer cache exhausted".into());
        }
        cache.insert(
            order_id.clone(),
            (signed, now.saturating_add(self.settings.quote_ttl_ms)),
        );
        Ok(PreparedOrder {
            order_id,
            quote,
            signed_wire,
            fee_policy: Some(fee_policy),
        })
    }
    async fn post(&self, prepared: PreparedOrder) -> Result<PostResult, String> {
        let (signed, expiry) = self
            .signed
            .lock()
            .await
            .remove(&prepared.order_id)
            .ok_or("signed order cache identity unavailable")?;
        if expiry <= now_ms() {
            return Ok(PostResult::Rejected);
        }
        let response = self.client.post_order(signed).await.map_err(map_error)?;
        if response.success {
            if response.order_id != prepared.order_id {
                return Err("post acknowledgement order hash mismatch".into());
            }
            Ok(PostResult::Accepted {
                order_id: response.order_id,
            })
        } else {
            Ok(PostResult::Rejected)
        }
    }
    async fn cancel(&self, ids: Vec<String>) -> Result<Vec<String>, String> {
        if ids.is_empty() || ids.len() > 64 {
            return Err("invalid bounded cancellation batch".into());
        }
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let response = self.client.cancel_orders(&refs).await.map_err(map_error)?;
        if response.canceled.iter().any(|id| !ids.contains(id)) {
            return Err("cancel result contains foreign identity".into());
        }
        // A cancel acknowledgement is only a reason to query. It does not release reservations.
        Ok(response.canceled)
    }
    async fn reconcile(&self, tracked: Vec<LiveOrder>) -> Result<AccountObservation, String> {
        if let Some(chain) = &self.chain {
            chain.ready().await?;
        }
        if self.protocol().await? != 2
            || self
                .client
                .check_geoblock()
                .await
                .map_err(map_error)?
                .blocked
        {
            return Err("host/protocol eligibility is unverified or blocked".into());
        }
        let server = self.client.server_time().await.map_err(map_error)?;
        if (server * 1000).abs_diff(now_ms() as i64) > 2000 {
            return Err("server clock differs by more than approved bound".into());
        }
        if self
            .client
            .closed_only_mode()
            .await
            .map_err(map_error)?
            .closed_only
        {
            return Err("account is restricted to close-only".into());
        }
        let account = self
            .client
            .balance_allowance(BalanceAllowanceRequest::default())
            .await
            .map_err(map_error)?;
        let exchange = address(&self.settings.exchange_address)?;
        let allowance = U256::from_str(
            account
                .allowances
                .get(&exchange)
                .ok_or("allowance for pinned exchange is missing")?,
        )
        .map_err(|_| "invalid exchange allowance")?;
        let collateral = raw_micro(account.balance)?;
        let allowance = u64::try_from(allowance).unwrap_or(u64::MAX);
        let mut result = AccountObservation {
            collateral_micros: collateral,
            allowance_micros: allowance,
            ..Default::default()
        };
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        let mut fee_transactions = BTreeSet::new();
        for page_no in 0..8 {
            let page = self
                .client
                .orders(&OrdersRequest::default(), cursor)
                .await
                .map_err(map_error)?;
            for raw in page.data {
                if result.orders.len() + result.foreign_order_ids.len() >= 256 {
                    return Err("account open order bound exceeded".into());
                }
                if tracked.iter().any(|o| o.order_id == raw.id) {
                    result.orders.push(self.observation(raw)?);
                } else {
                    result.foreign_order_ids.push(raw.id);
                }
            }
            if page.next_cursor == "LTE=" || page.next_cursor.is_empty() {
                break;
            }
            if page_no == 7 || !seen.insert(page.next_cursor.clone()) {
                return Err("open order pagination incomplete".into());
            }
            cursor = Some(page.next_cursor);
        }
        let observations = query_active_orders(&tracked, |id| async move {
            match self.client.order(&id).await {
                Ok(raw) => self.observation(raw).map(Some),
                Err(error)
                    if error
                        .downcast_ref::<sdk::error::Status>()
                        .is_some_and(|e| e.status_code.as_u16() == 404) =>
                {
                    // Missing order plus absent open list cannot prove a timed-out post was rejected.
                    Ok(None)
                }
                Err(error) => Err(map_error(error)),
            }
        })
        .await?;
        for observation in observations {
            result.orders.retain(|o| o.order_id != observation.order_id);
            result.orders.push(observation);
        }
        // Trade queries corroborate cumulative observations and surface failed settlement.
        let earliest = tracked
            .iter()
            .map(|o| o.created_at_ms / 1000)
            .min()
            .unwrap_or(now_ms() / 1000);
        let request = TradesRequest::builder()
            .after(earliest.saturating_sub(60) as i64)
            .build();
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        for page_no in 0..8 {
            let page = self
                .client
                .trades(&request, cursor)
                .await
                .map_err(map_error)?;
            for trade in page.data {
                let relevant = tracked.iter().any(|o| {
                    o.order_id == trade.taker_order_id
                        || trade.maker_orders.iter().any(|m| m.order_id == o.order_id)
                });
                if relevant
                    && matches!(
                        trade.status,
                        TradeStatusType::Failed | TradeStatusType::Unknown(_)
                    )
                {
                    return Err("tracked execution settlement failed or has unknown status".into());
                }
                if relevant
                    && trade.maker_orders.iter().any(|m| {
                        result.orders.iter().any(|o| {
                            o.order_id == m.order_id
                                && micro(m.matched_amount)
                                    .is_ok_and(|q| q > o.cumulative_filled_micros)
                        })
                    })
                {
                    return Err("trade exceeds authoritative cumulative order observation".into());
                }
                if relevant && trade.transaction_hash != B256::ZERO {
                    fee_transactions.insert(format!("{:#x}", trade.transaction_hash));
                    if fee_transactions.len() > 256 {
                        return Err("execution fee transaction bound exceeded".into());
                    }
                }
            }
            if page.next_cursor == "LTE=" || page.next_cursor.is_empty() {
                break;
            }
            if page_no == 7 || !seen.insert(page.next_cursor.clone()) {
                return Err("trade pagination incomplete".into());
            }
            cursor = Some(page.next_cursor);
        }
        if let Some(chain) = &self.chain {
            // Finalized proofs are immutable; cache them to avoid repeated RPC work.
            let mut uncached = 0;
            for hash in fee_transactions {
                let cached = self.fee_receipts.lock().await.get(&hash).cloned();
                let proofs = if let Some(proofs) = cached {
                    Some(proofs)
                } else if uncached < 8 {
                    uncached += 1;
                    chain.execution_fees(&hash, &tracked).await?
                } else {
                    None
                };
                if let Some(proofs) = proofs {
                    let mut cache = self.fee_receipts.lock().await;
                    if cache.len() >= 256 && !cache.contains_key(&hash) {
                        return Err("finalized fee cache capacity exhausted".into());
                    }
                    cache.insert(hash, proofs.clone());
                    result.execution_fees.extend(proofs);
                    if result.execution_fees.len() > 1024 {
                        return Err("execution fee proof bound exceeded".into());
                    }
                }
            }
        }
        for offset in (0..512).step_by(128) {
            let req = PositionsRequest::builder()
                .user(address(&self.settings.funder_address)?)
                .size_threshold(Decimal::ZERO)
                .limit(128)
                .map_err(|_| "position request bound")?
                .offset(offset)
                .map_err(|_| "position offset bound")?
                .build();
            let positions = self.data.positions(&req).await.map_err(map_error)?;
            let count = positions.len();
            for p in positions {
                if p.proxy_wallet != address(&self.settings.funder_address)?
                    || result.positions.len() >= 128
                {
                    return Err("position account or capacity mismatch".into());
                }
                let outcome = match p.outcome.as_str() {
                    "Up" => Outcome::Up,
                    "Down" => Outcome::Down,
                    _ => {
                        return Err(
                            "unsupported existing account outcome requires owner closeout".into(),
                        );
                    }
                };
                if !self
                    .settings
                    .allowed_conditions
                    .contains(&format!("{:#x}", p.condition_id))
                    && !self
                        .settings
                        .allowed_market_prefixes
                        .iter()
                        .any(|prefix| p.slug.starts_with(prefix))
                {
                    return Err("existing account positions outside approved market family".into());
                }
                let price_ceil = (p.avg_price * Decimal::from(1_000_000))
                    .ceil()
                    .normalize()
                    .to_string()
                    .parse::<u64>()
                    .map_err(|_| "invalid bounded portfolio average price")?;
                if price_ceil > 1_000_000 {
                    return Err("binary outcome average price exceeds unit payout".into());
                }
                result.positions.push(LivePosition {
                    condition_id: format!("{:#x}", p.condition_id),
                    token_id: p.asset.to_string(),
                    outcome,
                    quantity_micros: micro(p.size)?,
                    cost_pico: (u128::from(price_ceil) * u128::from(micro(p.size)?)).to_string(),
                    redeemable: p.redeemable,
                    mergeable: p.mergeable,
                });
            }
            if count < 128 {
                break;
            }
            if offset == 384 {
                return Err("position pagination incomplete".into());
            }
        }
        if let Some(chain) = &self.chain {
            let tokens: Vec<_> = result
                .positions
                .iter()
                .map(|p| p.token_id.clone())
                .collect();
            let quantities = chain.token_balances(&tokens).await?;
            for (position, quantity) in result.positions.iter_mut().zip(quantities) {
                let prior = position.quantity_micros;
                position.quantity_micros = quantity;
                let cost = position
                    .cost_pico
                    .parse::<u128>()
                    .map_err(|_| "invalid position cost")?;
                if prior > 0 {
                    position.cost_pico = (cost * u128::from(quantity))
                        .div_ceil(u128::from(prior))
                        .to_string();
                }
            }
            result.positions.retain(|p| p.quantity_micros > 0);
        }
        Ok(result)
    }
    async fn heartbeat(&self) -> Result<(), String> {
        let mut id = self.heartbeat_id.lock().await;
        let response = self.client.post_heartbeat(*id).await.map_err(map_error)?;
        if response.error.is_some() {
            return Err("venue heartbeat rejected".into());
        }
        *id = Some(response.heartbeat_id);
        Ok(())
    }
    async fn prepare_chain(
        &self,
        plan: crate::ChainPlan,
    ) -> Result<crate::PreparedChainTransaction, String> {
        self.chain
            .as_ref()
            .ok_or("onchain closeout is not configured")?
            .prepare(&plan, now_ms())
            .await
    }
    async fn submit_chain(
        &self,
        prepared: crate::PreparedChainTransaction,
    ) -> Result<Option<String>, String> {
        self.chain
            .as_ref()
            .ok_or("onchain closeout is not configured")?
            .submit_ack(&prepared)
            .await
    }
    async fn lookup_chain(
        &self,
        prepared: crate::PreparedChainTransaction,
    ) -> Result<crate::ChainObservation, String> {
        self.chain
            .as_ref()
            .ok_or("onchain closeout is not configured")?
            .observe(&prepared)
            .await
    }
    async fn listen(&self, events: mpsc::Sender<VenueNotice>, shutdown: watch::Receiver<bool>) {
        let subscription =
            match SubscriptionRequest::user(vec![]).as_authenticated(self.client.credentials()) {
                Ok(subscription) => subscription,
                Err(_) => {
                    let _ = events.try_send(VenueNotice::Fault(
                        "user subscription encoding failed".into(),
                    ));
                    return;
                }
            };
        // SDK 0.7.0's all-account subscription owns reconnect tasks after Client::drop.
        // Own the bounded socket directly; retain SDK request/message schemas and verified TLS.
        listen_user_socket(
            "wss://ws-subscriptions-clob.polymarket.com/ws/user",
            subscription,
            events,
            shutdown,
            UserStreamTiming::production(self.settings.request_timeout_ms),
        )
        .await;
    }
}

#[cfg(test)]
#[path = "sdk_type3_tests.rs"]
mod type3_tests;

#[derive(Clone, Copy)]
pub(crate) struct UserStreamTiming {
    pub request: Duration,
    pub retry: Duration,
    pub ping: Duration,
    pub pong_timeout: Duration,
}
impl UserStreamTiming {
    fn production(request_timeout_ms: u64) -> Self {
        Self {
            request: Duration::from_millis(request_timeout_ms),
            retry: Duration::from_secs(2),
            ping: Duration::from_secs(5),
            pong_timeout: Duration::from_secs(15),
        }
    }
}
async fn user_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        let stopping = *shutdown.borrow();
        if stopping || shutdown.changed().await.is_err() {
            return;
        }
    }
}
pub(crate) fn user_notices(text: &str) -> Result<Vec<VenueNotice>, String> {
    if text.len() > 65_536 {
        return Err("bounded private message size exceeded".into());
    }
    let raw: serde_json::Value =
        serde_json::from_str(text).map_err(|_| "invalid private message JSON")?;
    let values = match raw {
        serde_json::Value::Array(values) => values,
        value => vec![value],
    };
    if values.len() > 128 {
        return Err("bounded private message batch exceeded".into());
    }
    values
        .into_iter()
        .map(|value| {
            match serde_json::from_value::<WsMessage>(value)
                .map_err(|_| "unsupported private message schema")?
            {
                WsMessage::Order(order) => Ok(VenueNotice::OrderChanged(order.id)),
                WsMessage::Trade(_) => Ok(VenueNotice::AccountChanged),
                _ => Err("unexpected public message on private channel".into()),
            }
        })
        .collect()
}
pub(crate) async fn listen_user_socket(
    endpoint: &str,
    subscription: String,
    events: mpsc::Sender<VenueNotice>,
    mut shutdown: watch::Receiver<bool>,
    timing: UserStreamTiming,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(65_536))
            .max_frame_size(Some(65_536))
            .max_write_buffer_size(262_144);
        let connection = tokio::select! {
            biased;
            _ = user_shutdown(&mut shutdown) => return,
            connection = tokio::time::timeout(timing.request, connect_async_with_config(endpoint, Some(config), false)) => connection,
        };
        if let Ok(Ok((mut socket, _))) = connection {
            let subscribed = tokio::select! {
                biased;
                _ = user_shutdown(&mut shutdown) => false,
                sent = tokio::time::timeout(timing.request, socket.send(Message::Text(subscription.clone().into()))) => matches!(sent, Ok(Ok(()))),
            };
            if subscribed {
                if events.try_send(VenueNotice::Connected(true)).is_err() {
                    let _ = tokio::time::timeout(timing.request, socket.close(None)).await;
                    return;
                }
                let mut ping = tokio::time::interval(timing.ping);
                ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut watchdog = tokio::time::interval(Duration::from_millis(50));
                watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut last_pong = std::time::Instant::now();
                let mut fatal = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = user_shutdown(&mut shutdown) => break,
                        _ = watchdog.tick() => {
                            if last_pong.elapsed() > timing.pong_timeout { break; }
                        },
                        _ = ping.tick() => {
                            if !matches!(tokio::time::timeout(timing.request, socket.send(Message::Text("PING".into()))).await, Ok(Ok(()))) { break; }
                        },
                        item = socket.next() => {
                            match item {
                                Some(Ok(Message::Text(text))) if text.as_str() == "PONG" => { last_pong = std::time::Instant::now(); },
                                Some(Ok(Message::Text(text))) => {
                                    let notices = match user_notices(text.as_str()) {
                                        Ok(notices) => notices,
                                        Err(error) => {
                                            let _ = tokio::time::timeout(timing.request, events.send(VenueNotice::Fault(error))).await;
                                            fatal = true; break;
                                        }
                                    };
                                    for notice in notices {
                                        // Events trigger authoritative REST queries; they never free money directly.
                                        if events.try_send(notice).is_err() {
                                            let _ = tokio::time::timeout(timing.request, events.send(VenueNotice::Fault("bounded user stream ingress overflow".into()))).await;
                                            fatal = true; break;
                                        }
                                    }
                                    if fatal { break; }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if !matches!(tokio::time::timeout(timing.request, socket.send(Message::Pong(payload))).await, Ok(Ok(()))) { break; }
                                },
                                Some(Ok(Message::Pong(_))) => {},
                                _ => break,
                            }
                        },
                    }
                }
                let _ = tokio::time::timeout(timing.request, socket.close(None)).await;
                if !matches!(
                    tokio::time::timeout(
                        timing.request,
                        events.send(VenueNotice::Connected(false))
                    )
                    .await,
                    Ok(Ok(()))
                ) || fatal
                {
                    return;
                }
            } else {
                let _ = tokio::time::timeout(timing.request, socket.close(None)).await;
            }
        }
        tokio::select! {
            biased;
            _ = user_shutdown(&mut shutdown) => return,
            _ = tokio::time::sleep(timing.retry) => {},
        }
    }
}
