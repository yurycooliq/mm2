use crate::{
    AccountObservation, LiveOrder, LivePosition, LiveQuote, LiveSettings, OrderObservation,
    settings::address,
};
use alloy::{dyn_abi::Eip712Domain, signers::local::PrivateKeySigner, sol_types::SolStruct};
use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use mm2_core::Outcome;
use polymarket_client_sdk_v2 as sdk;
use sdk::{
    auth::{Credentials, Normal, Signer, Uuid, state::Authenticated},
    clob::{
        Client, Config,
        types::{
            OrderPayload, OrderStatusType, OrderType, Side, SignatureType, SignedOrder,
            TradeStatusType,
            request::{BalanceAllowanceRequest, OrdersRequest, TradesRequest},
            response::{ClobMarketInfoResponse, MarketResponse},
        },
        ws::{ChannelType, Client as WsClient, WsMessage},
    },
    data::{Client as DataClient, types::request::PositionsRequest},
    types::{B256, Decimal, U256},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc, watch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedOrder {
    pub order_id: String,
    pub quote: LiveQuote,
    pub signed_wire: serde_json::Value,
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
    token_cache: Mutex<BTreeMap<String, (u64, ClobMarketInfoResponse)>>,
}
const METADATA_TTL_MS: u64 = 1000;
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
    if fee.taker_only {
        return Ok(0);
    }
    if fee.rate.is_sign_negative() || fee.rate > Decimal::ONE || fee.exponent > 16 {
        return Err("unsupported V2 fee parameters".into());
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
            .timeout(Duration::from_millis(settings.request_timeout_ms))
            .https_only(true)
            .build()
            .map_err(|_| "cannot initialize public protocol client")?;
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
            token_cache: Mutex::new(BTreeMap::new()),
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
        quote.validate(&self.settings, now_ms())?;
        let token = U256::from_str(&quote.token_id).map_err(|_| "invalid token identifier")?;
        if token.to_string() != quote.token_id {
            return Err("noncanonical token identifier".into());
        }
        let condition =
            B256::from_str(&quote.condition_id).map_err(|_| "invalid condition identifier")?;
        let (version, market, info) = tokio::try_join!(
            self.protocol(),
            self.market(&quote.condition_id),
            self.token_info(&quote)
        )?;
        if version != u32::from(self.settings.protocol) {
            return Err("deployed CLOB protocol differs from pinned V2".into());
        }
        let expected_outcome = match quote.outcome {
            Outcome::Up => "Up",
            Outcome::Down => "Down",
        };
        if market.condition_id != Some(condition)
            || market.market_slug != quote.market_slug
            || !market.enable_order_book
            || !market.active
            || market.closed
            || market.archived
            || !market.accepting_orders
            || market.neg_risk
            || market.tokens.len() != 2
            || !market
                .tokens
                .iter()
                .any(|t| t.token_id == token && t.outcome == expected_outcome)
            || market.end_date_iso.map(|d| d.timestamp_millis() as u64) != Some(quote.window_end_ms)
        {
            return Err("authoritative market identity/window/accepting state mismatch".into());
        }
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
        if payload.order.maker != address(&self.settings.funder_address)?
            || payload.order.signer != self.signer.address()
            || payload.order.tokenId != token
            || payload.order.side != 0
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
            }
            if page.next_cursor == "LTE=" || page.next_cursor.is_empty() {
                break;
            }
            if page_no == 7 || !seen.insert(page.next_cursor.clone()) {
                return Err("trade pagination incomplete".into());
            }
            cursor = Some(page.next_cursor);
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
    async fn listen(&self, events: mpsc::Sender<VenueNotice>, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let ws = match WsClient::default()
                .authenticate(self.client.credentials().clone(), self.client.address())
            {
                Ok(client) => client,
                Err(_) => {
                    let _ = events
                        .send(VenueNotice::Fault(
                            "user stream initialization failed".into(),
                        ))
                        .await;
                    return;
                }
            };
            // Empty markets subscribes to this authenticated user's complete account.
            let stream = match ws.subscribe_user_events(vec![]) {
                Ok(stream) => stream,
                Err(_) => {
                    let _ = events
                        .send(VenueNotice::Fault("user subscription failed".into()))
                        .await;
                    return;
                }
            };
            let mut stream = Box::pin(stream);
            let mut pulse = tokio::time::interval(Duration::from_millis(250));
            let mut connected = false;
            loop {
                tokio::select! {
                    _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
                    _ = pulse.tick() => {
                        let next = ws.is_connected(ChannelType::User);
                        if next != connected { connected = next; if events.try_send(VenueNotice::Connected(next)).is_err() { break; } }
                    }
                    item = stream.next() => {
                        let notice = match item {
                            Some(Ok(WsMessage::Order(order))) => VenueNotice::OrderChanged(order.id),
                            Some(Ok(WsMessage::Trade(_))) => VenueNotice::AccountChanged,
                            Some(Ok(_)) => continue,
                            Some(Err(_)) | None => { let _ = events.send(VenueNotice::Connected(false)).await; break; }
                        };
                        // Private events are reconciliation triggers, not authority to free funds.
                        if events.try_send(notice).is_err() { let _ = events.send(VenueNotice::Fault("bounded user stream ingress overflow".into())).await; break; }
                    }
                }
            }
            let _ = ws.unsubscribe_user_events(&[]);
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}
