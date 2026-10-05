pub use crate::domain::parse_micro;
use crate::domain::{
    BookSide, Change, FeedEvent, FeedSource, Level, Market, Outcome, valid_tick_micro,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig},
};

pub const CLOB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const BINANCE_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";
const MAX_REST_BYTES: usize = 262_144;
pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
const MAX_MEMBERS: usize = 4096;
const MAX_DEPTH: usize = 10_000;
const UINT256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

fn validate_decimal(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 128 {
        return Err("invalid decimal length".into());
    }
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|digit| digit.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
        || fraction.is_some_and(|part| {
            part.is_empty() || !part.bytes().all(|digit| digit.is_ascii_digit())
        })
    {
        return Err("decimal must be an unsigned fixed string".into());
    }
    Ok(())
}

pub fn validate_token(value: &str) -> Result<(), String> {
    if value.is_empty()
        || !value.bytes().all(|digit| digit.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
        || value.len() > UINT256_MAX.len()
        || value.len() == UINT256_MAX.len() && value > UINT256_MAX
    {
        return Err("invalid canonical uint256 token".into());
    }
    Ok(())
}

fn condition(value: &str) -> Result<String, String> {
    if value.len() != 66
        || !value.starts_with("0x")
        || !value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err("invalid condition identity".into());
    }
    Ok(value.to_ascii_lowercase())
}

fn object(value: &Value) -> Result<&Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| "feed member must be an object".into())
}

fn string<'a>(value: &'a Map<String, Value>, name: &str) -> Result<&'a str, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string {name}"))
}

fn unsigned_string(value: &str) -> Result<u64, String> {
    if value.is_empty()
        || !value.bytes().all(|digit| digit.is_ascii_digit())
        || value.len() > 1 && value.starts_with('0')
    {
        return Err("invalid unsigned integer string".into());
    }
    value
        .parse()
        .map_err(|_| "unsigned integer overflow".into())
}

fn clob_clock(value: &Map<String, Value>) -> Result<u64, String> {
    let timestamp = unsigned_string(string(value, "timestamp")?)?;
    if timestamp == 0 {
        return Err("missing positive exchange clock".into());
    }
    Ok(timestamp)
}

fn micro_field(value: &Map<String, Value>, name: &str) -> Result<u64, String> {
    parse_micro(string(value, name)?)
}

fn price_field(value: &Map<String, Value>, name: &str) -> Result<u64, String> {
    let price = micro_field(value, name)?;
    if price > 1_000_000 {
        return Err("CLOB price outside unit interval".into());
    }
    Ok(price)
}

fn token_outcome(token: &str, identity: &str, market: &Market) -> Result<Option<Outcome>, String> {
    validate_token(token)?;
    let outcome = if token == market.up_token {
        Some(Outcome::Up)
    } else if token == market.down_token {
        Some(Outcome::Down)
    } else {
        None
    };
    if identity == market.condition_id && outcome.is_none()
        || identity != market.condition_id && outcome.is_some()
    {
        return Err("condition and token mapping disagree".into());
    }
    Ok(outcome)
}

fn levels(value: &Map<String, Value>, name: &str) -> Result<Vec<Level>, String> {
    let entries = value
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing depth {name}"))?;
    if entries.len() > MAX_DEPTH {
        return Err("snapshot depth exceeds member limit".into());
    }
    let mut prices = HashSet::new();
    let mut normalized = Vec::with_capacity(entries.len());
    for entry in entries {
        let entry = object(entry)?;
        let price_micro = price_field(entry, "price")?;
        let quantity_micro = micro_field(entry, "size")?;
        if quantity_micro == 0 || !prices.insert(price_micro) {
            return Err("snapshot zero size or duplicate level".into());
        }
        normalized.push(Level {
            price_micro,
            quantity_micro,
        });
    }
    Ok(normalized)
}

/// A recovery snapshot carries the venue's current tick. Gamma's tick at initial
/// discovery cannot prove the tick after a missed WebSocket transition.
pub fn parse_rest_book(
    raw: &Value,
    market: &Market,
    outcome: Outcome,
    observed_mono_ms: u64,
) -> Result<Vec<FeedEvent>, String> {
    let raw = object(raw)?;
    let identity = condition(string(raw, "market")?)?;
    if token_outcome(string(raw, "asset_id")?, &identity, market)? != Some(outcome) {
        return Err("REST book is not the requested outcome".into());
    }
    let exchange_ms = clob_clock(raw)?;
    let tick_micro = micro_field(raw, "tick_size")?;
    if !valid_tick_micro(tick_micro) {
        return Err("unsupported current REST tick".into());
    }
    let bids = levels(raw, "bids")?;
    let asks = levels(raw, "asks")?;
    if bids
        .iter()
        .chain(&asks)
        .any(|level| level.price_micro % tick_micro != 0)
    {
        return Err("REST book price violates current tick".into());
    }
    if let (Some(bid), Some(ask)) = (
        bids.iter().map(|l| l.price_micro).max(),
        asks.iter().map(|l| l.price_micro).min(),
    ) && bid >= ask
    {
        return Err("REST book is crossed".into());
    }
    if let Some(value) = raw.get("neg_risk")
        && !value.is_boolean()
    {
        return Err("invalid REST neg_risk".into());
    }
    if let Some(value) = raw.get("hash")
        && value.as_str().is_none_or(str::is_empty)
    {
        return Err("invalid REST book hash".into());
    }
    if raw.contains_key("min_order_size") && micro_field(raw, "min_order_size")? == 0 {
        return Err("invalid REST minimum size".into());
    }
    if raw.contains_key("last_trade_price") {
        price_field(raw, "last_trade_price")?;
    }
    Ok(vec![
        FeedEvent::Tick {
            condition_id: identity.clone(),
            outcome,
            exchange_ms,
            observed_mono_ms,
            old_tick_micro: None,
            new_tick_micro: tick_micro,
        },
        FeedEvent::Snapshot {
            condition_id: identity,
            outcome,
            exchange_ms,
            observed_mono_ms,
            bids,
            asks,
        },
    ])
}

struct Recovery {
    baseline: [u64; 2],
    initialized: [bool; 2],
}
impl Recovery {
    fn new(baseline: [u64; 2]) -> Self {
        Self {
            baseline,
            initialized: [false; 2],
        }
    }
    #[cfg(test)]
    fn passthrough() -> Self {
        Self {
            baseline: [0; 2],
            initialized: [true; 2],
        }
    }
    fn ready(&self) -> bool {
        self.initialized.iter().all(|ready| *ready)
    }
    fn filter(&mut self, events: Vec<FeedEvent>) -> Vec<FeedEvent> {
        let index = |outcome: Outcome| if outcome == Outcome::Up { 0 } else { 1 };
        events
            .into_iter()
            .filter_map(|event| match event {
                FeedEvent::Snapshot {
                    outcome,
                    exchange_ms,
                    ..
                } => {
                    let index = index(outcome);
                    if !self.initialized[index] && exchange_ms <= self.baseline[index] {
                        return None;
                    }
                    self.initialized[index] = true;
                    Some(event)
                }
                FeedEvent::Tick {
                    outcome,
                    exchange_ms,
                    ..
                } => {
                    let index = index(outcome);
                    if !self.initialized[index] && exchange_ms <= self.baseline[index] {
                        return None;
                    }
                    Some(event)
                }
                FeedEvent::Delta {
                    condition_id,
                    exchange_ms,
                    observed_mono_ms,
                    changes,
                } => {
                    // All members were validated first. A delta cannot initialize a book.
                    let changes: Vec<_> = changes
                        .into_iter()
                        .filter(|change| {
                            let index = index(change.outcome);
                            self.initialized[index]
                        })
                        .collect();
                    if changes.is_empty() {
                        None
                    } else {
                        Some(FeedEvent::Delta {
                            condition_id,
                            exchange_ms,
                            observed_mono_ms,
                            changes,
                        })
                    }
                }
                event => Some(event),
            })
            .collect()
    }
}

async fn rest_recovery(
    market: &Market,
    start: Instant,
) -> Result<(Vec<FeedEvent>, [u64; 2]), SessionResult> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| SessionResult::Fatal("cannot build public book client".into()))?;
    let mut events = Vec::new();
    let mut baseline = [0; 2];
    for (index, outcome, token) in [
        (0, Outcome::Up, &market.up_token),
        (1, Outcome::Down, &market.down_token),
    ] {
        let mut response = client
            .get(CLOB_BOOK_URL)
            .query(&[("token_id", token)])
            .send()
            .await
            .map_err(|_| SessionResult::Transport("public REST book transport failed"))?;
        if !response.status().is_success() {
            return Err(SessionResult::Transport(
                "public REST book HTTP unavailable",
            ));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_REST_BYTES as u64)
        {
            return Err(SessionResult::Fatal(
                "public REST book exceeds 256 KiB".into(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| SessionResult::Transport("public REST book body failed"))?
        {
            if bytes.len() + chunk.len() > MAX_REST_BYTES {
                return Err(SessionResult::Fatal(
                    "public REST book exceeds 256 KiB".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let raw: Value = serde_json::from_slice(&bytes)
            .map_err(|_| SessionResult::Fatal("REST book schema is not JSON".into()))?;
        let observed_mono_ms = start
            .elapsed()
            .as_millis()
            .try_into()
            .map_err(|_| SessionResult::Fatal("monotonic clock overflow".into()))?;
        let normalized = parse_rest_book(&raw, market, outcome, observed_mono_ms)
            .map_err(SessionResult::Fatal)?;
        baseline[index] = match &normalized[0] {
            FeedEvent::Tick { exchange_ms, .. } => *exchange_ms,
            _ => unreachable!(),
        };
        // REST establishes only the tick and cutoff. Publishing its book then applying
        // equal-ms buffered WS events would choose a causality order we cannot prove.
        events.extend(
            normalized
                .into_iter()
                .filter(|event| matches!(event, FeedEvent::Tick { .. })),
        );
    }
    Ok((events, baseline))
}

fn side(value: &Map<String, Value>) -> Result<BookSide, String> {
    match string(value, "side")? {
        "BUY" => Ok(BookSide::Bid),
        "SELL" => Ok(BookSide::Ask),
        _ => Err("invalid book side".into()),
    }
}

fn clob_member(
    value: &Value,
    market: &Market,
    observed_mono_ms: u64,
) -> Result<Vec<FeedEvent>, String> {
    let value = object(value)?;
    let event_type = string(value, "event_type")?;
    let identity = condition(string(value, "market")?)?;
    let exchange_ms = if matches!(event_type, "new_market" | "market_resolved") {
        value
            .get("timestamp")
            .map(|_| clob_clock(value))
            .transpose()?
            .unwrap_or(0)
    } else {
        clob_clock(value)?
    };
    match event_type {
        "book" => {
            let outcome = token_outcome(string(value, "asset_id")?, &identity, market)?;
            let bids = levels(value, "bids")?;
            let asks = levels(value, "asks")?;
            Ok(outcome
                .map(|outcome| {
                    vec![FeedEvent::Snapshot {
                        condition_id: identity,
                        outcome,
                        exchange_ms,
                        observed_mono_ms,
                        bids,
                        asks,
                    }]
                })
                .unwrap_or_default())
        }
        "price_change" => {
            let entries = value
                .get("price_changes")
                .and_then(Value::as_array)
                .ok_or("missing price changes")?;
            if entries.is_empty() || entries.len() > MAX_MEMBERS {
                return Err("invalid price-change member count".into());
            }
            let mut changes = Vec::with_capacity(entries.len());
            for entry in entries {
                let entry = object(entry)?;
                let outcome = token_outcome(string(entry, "asset_id")?, &identity, market)?;
                let price_micro = price_field(entry, "price")?;
                let quantity_micro = micro_field(entry, "size")?;
                let side = side(entry)?;
                for name in ["best_bid", "best_ask"] {
                    if entry.contains_key(name) {
                        price_field(entry, name)?;
                    }
                }
                if let Some(outcome) = outcome {
                    changes.push(Change {
                        outcome,
                        side,
                        level: Level {
                            price_micro,
                            quantity_micro,
                        },
                    });
                }
            }
            if changes.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![FeedEvent::Delta {
                    condition_id: identity,
                    exchange_ms,
                    observed_mono_ms,
                    changes,
                }])
            }
        }
        "tick_size_change" => {
            let outcome = token_outcome(string(value, "asset_id")?, &identity, market)?;
            let old_tick_micro = value
                .get("old_tick_size")
                .map(|_| micro_field(value, "old_tick_size"))
                .transpose()?;
            let new_tick_micro = micro_field(value, "new_tick_size")?;
            if !valid_tick_micro(new_tick_micro)
                || old_tick_micro.is_some_and(|tick| !valid_tick_micro(tick))
            {
                return Err("unsupported CLOB tick size".into());
            }
            Ok(outcome
                .map(|outcome| {
                    vec![FeedEvent::Tick {
                        condition_id: identity,
                        outcome,
                        exchange_ms,
                        observed_mono_ms,
                        old_tick_micro,
                        new_tick_micro,
                    }]
                })
                .unwrap_or_default())
        }
        "last_trade_price" => {
            let outcome = token_outcome(string(value, "asset_id")?, &identity, market)?;
            let price_micro = price_field(value, "price")?;
            let quantity_micro = micro_field(value, "size")?;
            if quantity_micro == 0 {
                return Err("zero trade quantity".into());
            }
            let trade_side = side(value)?;
            if value.contains_key("fee_rate_bps") {
                unsigned_string(string(value, "fee_rate_bps")?)?;
            }
            // No native unique trade ID is supplied. Equal witnesses are deduplicated
            // conservatively in the paper lane; they never update the real account.
            use sha2::{Digest, Sha256};
            let fingerprint = hex::encode(Sha256::digest(
                serde_json::to_vec(value).map_err(|_| "cannot fingerprint public trade")?,
            ));
            Ok(outcome
                .filter(|_| trade_side == BookSide::Ask)
                .map_or_else(Vec::new, |outcome| {
                    vec![FeedEvent::Trade {
                        condition_id: identity,
                        outcome,
                        exchange_ms,
                        observed_mono_ms,
                        price_micro,
                        quantity_micro,
                        fingerprint,
                    }]
                }))
        }
        "best_bid_ask" => {
            token_outcome(string(value, "asset_id")?, &identity, market)?;
            let bid = price_field(value, "best_bid")?;
            let ask = price_field(value, "best_ask")?;
            let spread = price_field(value, "spread")?;
            if ask.checked_sub(bid) != Some(spread) {
                return Err("invalid public BBO spread".into());
            }
            Ok(Vec::new())
        }
        "market_resolved" | "new_market" => {
            let assets = value
                .get("assets_ids")
                .and_then(Value::as_array)
                .ok_or("missing lifecycle assets")?;
            if assets.is_empty() || assets.len() > MAX_MEMBERS {
                return Err("invalid lifecycle asset count".into());
            }
            let mut selected = HashSet::new();
            for asset in assets {
                let token = asset.as_str().ok_or("invalid lifecycle token")?;
                token_outcome(token, &identity, market)?;
                if !selected.insert(token) {
                    return Err("duplicate lifecycle token".into());
                }
            }
            if identity != market.condition_id {
                return Ok(Vec::new());
            }
            if assets.len() != 2 {
                return Err("invalid binary lifecycle assets".into());
            }
            if event_type == "new_market" {
                if string(value, "slug")? != market.slug {
                    return Err("selected lifecycle slug changed".into());
                }
                let outcomes = value
                    .get("outcomes")
                    .and_then(Value::as_array)
                    .ok_or("missing lifecycle outcomes")?;
                if outcomes.len() != assets.len() {
                    return Err("invalid lifecycle outcome count".into());
                }
                for (asset, label) in assets.iter().zip(outcomes) {
                    let token = asset.as_str().ok_or("invalid lifecycle token")?;
                    let expected = if token == market.up_token {
                        "Up"
                    } else {
                        "Down"
                    };
                    if label.as_str() != Some(expected) {
                        return Err("selected lifecycle outcome mapping changed".into());
                    }
                }
            } else {
                let winner = string(value, "winning_asset_id")?;
                token_outcome(winner, &identity, market)?;
                let expected = if winner == market.up_token {
                    "Up"
                } else {
                    "Down"
                };
                if string(value, "winning_outcome")? != expected {
                    return Err("selected winner mapping changed".into());
                }
                return Ok(vec![FeedEvent::Disconnect {
                    source: FeedSource::Clob,
                    reason: "selected market resolved".into(),
                }]);
            }
            Ok(Vec::new())
        }
        _ => Err("unsupported CLOB event type".into()),
    }
}

/// The complete native frame is validated before any batch is returned to the queue.
pub fn parse_clob(
    text: &str,
    market: &Market,
    observed_mono_ms: u64,
) -> Result<Vec<FeedEvent>, String> {
    if text.len() > MAX_MESSAGE_BYTES {
        return Err("CLOB message exceeds byte limit".into());
    }
    if text == "PONG" {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(text).map_err(|_| "invalid CLOB JSON")?;
    let mut events = Vec::new();
    if let Some(entries) = value.as_array() {
        if entries.len() > MAX_MEMBERS {
            return Err("CLOB frame exceeds member limit".into());
        }
        for entry in entries {
            events.extend(clob_member(entry, market, observed_mono_ms)?);
        }
    } else {
        events = clob_member(&value, market, observed_mono_ms)?;
    }
    Ok(events)
}

pub fn parse_binance(text: &str, observed_mono_ms: u64) -> Result<Vec<FeedEvent>, String> {
    if text.len() > MAX_MESSAGE_BYTES {
        return Err("Binance message exceeds byte limit".into());
    }
    let value: Value = serde_json::from_str(text).map_err(|_| "invalid Binance JSON")?;
    let value = object(&value)?;
    if string(value, "e")? != "trade" || string(value, "s")? != "BTCUSDT" {
        return Err("unexpected Binance stream identity".into());
    }
    let exchange_ms = value
        .get("T")
        .and_then(Value::as_u64)
        .filter(|time| *time > 0)
        .ok_or("invalid Binance trade clock")?;
    value
        .get("E")
        .and_then(Value::as_u64)
        .filter(|time| *time > 0)
        .ok_or("invalid Binance event clock")?;
    value
        .get("t")
        .and_then(Value::as_u64)
        .ok_or("invalid Binance trade identity")?;
    value
        .get("m")
        .and_then(Value::as_bool)
        .ok_or("invalid Binance maker flag")?;
    let quantity = string(value, "q")?;
    validate_decimal(quantity)?;
    if !quantity.bytes().any(|digit| (b'1'..=b'9').contains(&digit)) {
        return Err("zero Binance quantity".into());
    }
    let price_micro = micro_field(value, "p")?;
    if price_micro == 0 {
        return Err("zero Binance price".into());
    }
    // BTCUSDT spot trades are a diagnostic reference, not Chainlink or a settlement TWAP.
    Ok(vec![FeedEvent::Reference {
        exchange_ms,
        observed_mono_ms,
        price_micro,
    }])
}

fn latch_fault(fault: &watch::Sender<Option<String>>, reason: String) {
    fault.send_if_modified(|state| {
        if state.is_none() {
            *state = Some(reason);
            true
        } else {
            false
        }
    });
}

/// Reserve the whole normalized frame so overflow cannot queue a partial frame.
pub fn enqueue_frame(
    tx: &mpsc::Sender<FeedEvent>,
    events: Vec<FeedEvent>,
    fault: &watch::Sender<Option<String>>,
) -> Result<(), String> {
    if events.is_empty() {
        return Ok(());
    }
    let permits = tx.try_reserve_many(events.len()).map_err(|error| {
        let reason = match error {
            mpsc::error::TrySendError::Full(_) => "feed queue overflow",
            mpsc::error::TrySendError::Closed(_) => "feed consumer closed",
        }
        .to_string();
        latch_fault(fault, reason.clone());
        reason
    })?;
    for (permit, event) in permits.zip(events) {
        permit.send(event);
    }
    Ok(())
}

#[derive(Clone)]
struct Settings {
    connect: Duration,
    receive: Duration,
    send: Duration,
    ping: Duration,
    retry: Duration,
    max_retry: Duration,
    bootstrap: Duration,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            receive: Duration::from_secs(30),
            send: Duration::from_secs(5),
            ping: Duration::from_secs(10),
            retry: Duration::from_millis(500),
            max_retry: Duration::from_secs(30),
            bootstrap: Duration::from_secs(15),
        }
    }
}
#[derive(Clone, Copy)]
enum TickRefresh {
    Public,
    #[cfg(test)]
    Skip,
    #[cfg(test)]
    Fixture([u64; 2]),
}
struct Endpoint {
    source: FeedSource,
    url: String,
    settings: Settings,
    tick_refresh: TickRefresh,
}

enum SessionResult {
    Shutdown,
    Transport(&'static str),
    Fatal(String),
}

async fn wait_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        let stopping = *shutdown.borrow();
        if stopping {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn session(
    endpoint: &Endpoint,
    market: &Market,
    start: Instant,
    tx: &mpsc::Sender<FeedEvent>,
    fault: &watch::Sender<Option<String>>,
    shutdown: &mut watch::Receiver<bool>,
) -> SessionResult {
    let source = endpoint.source;
    let settings = &endpoint.settings;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES))
        .max_write_buffer_size(MAX_MESSAGE_BYTES);
    let connection = tokio::select! {
        _ = wait_shutdown(shutdown) => return SessionResult::Shutdown,
        connection = tokio::time::timeout(settings.connect, connect_async_with_config(&endpoint.url, Some(config), false)) => connection,
    };
    let (mut socket, _) = match connection {
        Ok(Ok(socket)) => socket,
        Ok(Err(_)) => return SessionResult::Transport("public WebSocket connection failed"),
        Err(_) => return SessionResult::Transport("public WebSocket connect timeout"),
    };
    if source == FeedSource::Clob {
        let subscription = json!({"assets_ids": [&market.up_token, &market.down_token], "type": "market", "custom_feature_enabled": true});
        if !matches!(
            tokio::time::timeout(
                settings.send,
                socket.send(Message::Text(subscription.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return SessionResult::Transport("public subscription send failed");
        }
    }
    let mut recovery_state = Recovery::new([0; 2]);
    if source == FeedSource::Clob && matches!(endpoint.tick_refresh, TickRefresh::Public) {
        // Subscribe before REST recovery so updates produced during HTTP lookup remain
        // buffered on the bounded socket. Recovery provides an outcome-specific cutoff.
        let recovery = rest_recovery(market, start);
        tokio::pin!(recovery);
        let mut heartbeat = tokio::time::interval(settings.ping);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let recovered = loop {
            tokio::select! {
                biased;
                _ = wait_shutdown(shutdown) => {
                    let _ = tokio::time::timeout(settings.send, socket.close(None)).await;
                    return SessionResult::Shutdown;
                },
                result = &mut recovery => break result,
                _ = heartbeat.tick() => {
                    if !matches!(tokio::time::timeout(settings.send, socket.send(Message::Text("PING".into()))).await, Ok(Ok(()))) {
                        return SessionResult::Transport("public recovery heartbeat send failed");
                    }
                },
            }
        };
        let (events, recovered_baseline) = match recovered {
            Ok(value) => value,
            Err(reason) => return reason,
        };
        recovery_state = Recovery::new(recovered_baseline);
        if let Err(error) = enqueue_frame(tx, events, fault) {
            return SessionResult::Fatal(error);
        }
    }
    #[cfg(test)]
    match endpoint.tick_refresh {
        TickRefresh::Skip => recovery_state = Recovery::passthrough(),
        TickRefresh::Fixture(baseline) => recovery_state = Recovery::new(baseline),
        TickRefresh::Public => {}
    }
    if source == FeedSource::Binance {
        recovery_state.initialized = [true; 2];
    }
    let bootstrap_deadline = tokio::time::sleep(settings.bootstrap);
    tokio::pin!(bootstrap_deadline);
    let idle = tokio::time::sleep(settings.receive);
    tokio::pin!(idle);
    let mut ping = tokio::time::interval(settings.ping);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = wait_shutdown(shutdown) => {
                let _ = tokio::time::timeout(settings.send, socket.close(None)).await;
                return SessionResult::Shutdown;
            },
            _ = &mut idle => return SessionResult::Transport("public WebSocket receive timeout"),
            _ = &mut bootstrap_deadline, if !recovery_state.ready() => return SessionResult::Transport("fresh both-outcome snapshot bootstrap timeout"),
            _ = ping.tick() => {
                let message = if source == FeedSource::Clob { Message::Text("PING".into()) } else { Message::Ping(Vec::new().into()) };
                if !matches!(tokio::time::timeout(settings.send, socket.send(message)).await, Ok(Ok(()))) { return SessionResult::Transport("public heartbeat send failed"); }
            },
            received = socket.next() => {
                let message = match received {
                    Some(Ok(message)) => message,
                    Some(Err(WebSocketError::Capacity(_))) => return SessionResult::Fatal("public WebSocket exceeds capacity limit".into()),
                    Some(Err(WebSocketError::Protocol(_))) => return SessionResult::Fatal("invalid public WebSocket protocol".into()),
                    Some(Err(_)) => return SessionResult::Transport("public WebSocket read failed"),
                    None => return SessionResult::Transport("public WebSocket ended"),
                };
                idle.as_mut().reset(tokio::time::Instant::now() + settings.receive);
                match message {
                    Message::Text(text) => {
                        let observed_mono_ms = match u64::try_from(start.elapsed().as_millis()) { Ok(value) => value, Err(_) => return SessionResult::Fatal("monotonic clock overflow".into()) };
                        let events = if source == FeedSource::Clob { parse_clob(&text, market, observed_mono_ms) } else { parse_binance(&text, observed_mono_ms) };
                        let events = match events { Ok(events) => events, Err(error) => return SessionResult::Fatal(error) };
                        let events = if source == FeedSource::Clob { recovery_state.filter(events) } else { events };
                        if let Err(error) = enqueue_frame(tx, events, fault) { return SessionResult::Fatal(error); }
                    },
                    Message::Ping(payload) => {
                        if !matches!(tokio::time::timeout(settings.send, socket.send(Message::Pong(payload))).await, Ok(Ok(()))) { return SessionResult::Transport("public pong send failed"); }
                    },
                    Message::Pong(_) => {},
                    Message::Close(_) => return SessionResult::Transport("public WebSocket remote close"),
                    _ => return SessionResult::Fatal("unsupported public WebSocket frame".into()),
                }
            }
        }
    }
}

async fn run_feed(
    endpoint: Endpoint,
    market: Market,
    start: Instant,
    tx: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let source = endpoint.source;
    let mut retry = endpoint.settings.retry;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let result = session(&endpoint, &market, start, &tx, &fault, &mut shutdown).await;
        if matches!(result, SessionResult::Shutdown) {
            return Ok(());
        }
        let fatal = matches!(result, SessionResult::Fatal(_));
        let reason = match result {
            SessionResult::Fatal(error) => error,
            SessionResult::Transport(reason) => reason.into(),
            SessionResult::Shutdown => unreachable!(),
        };
        enqueue_frame(
            &tx,
            vec![FeedEvent::Disconnect {
                source,
                reason: reason.clone(),
            }],
            &fault,
        )?;
        if fatal {
            latch_fault(&fault, reason.clone());
            return Err(reason);
        }
        tokio::select! {
            _ = wait_shutdown(&mut shutdown) => return Ok(()),
            _ = tokio::time::sleep(retry) => {},
        }
        retry = retry.saturating_mul(2).min(endpoint.settings.max_retry);
    }
}

pub async fn run_public(
    market: Market,
    start: Instant,
    tx: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let validate = || -> Result<(), String> {
        validate_token(&market.up_token)?;
        validate_token(&market.down_token)?;
        if market.up_token == market.down_token
            || condition(&market.condition_id)? != market.condition_id
        {
            return Err("invalid pinned public market identity".into());
        }
        Ok(())
    };
    if let Err(error) = validate() {
        latch_fault(&fault, error.clone());
        return Err(error);
    }
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(run_feed(
        Endpoint {
            source: FeedSource::Clob,
            url: CLOB_URL.into(),
            settings: Settings::default(),
            tick_refresh: TickRefresh::Public,
        },
        market.clone(),
        start,
        tx.clone(),
        fault.clone(),
        shutdown.clone(),
    ));
    tasks.spawn(run_feed(
        Endpoint {
            source: FeedSource::Binance,
            url: BINANCE_URL.into(),
            settings: Settings::default(),
            tick_refresh: TickRefresh::Public,
        },
        market,
        start,
        tx,
        fault.clone(),
        shutdown,
    ));
    while let Some(result) = tasks.join_next().await {
        let failure = match result {
            Ok(Ok(())) => continue,
            Ok(Err(reason)) => reason,
            Err(_) => "public feed task failed".into(),
        };
        latch_fault(&fault, failure.clone());
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Err(failure);
    }
    Ok(())
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use crate::domain::{Action, Core, CoreConfig, StopReason};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    fn market() -> Market {
        Market {
            slug: "btc-updown-5m-1790856000".into(),
            condition_id: format!("0x{}", "11".repeat(32)),
            up_token: "123".into(),
            down_token: "456".into(),
            window_start_ms: 1790856000000,
            window_end_ms: 1790856300000,
            tick_micro: 1000,
        }
    }

    fn snapshot(market: &Market) -> Value {
        json!({"event_type":"book","market":market.condition_id,"asset_id":market.up_token,
            "timestamp":"1790856000123","bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"2"}]})
    }

    fn fast_settings() -> Settings {
        Settings {
            connect: Duration::from_millis(500),
            receive: Duration::from_secs(1),
            send: Duration::from_millis(100),
            ping: Duration::from_millis(20),
            retry: Duration::from_millis(10),
            max_retry: Duration::from_millis(40),
            bootstrap: Duration::from_secs(1),
        }
    }

    fn rest_book(market: &Market, tick: &str, timestamp: &str) -> Value {
        json!({"market":market.condition_id,"asset_id":market.up_token,"timestamp":timestamp,
            "tick_size":tick,"bids":[{"price":"0.40","size":"100"}],
            "asks":[{"price":"0.60","size":"100"}],"neg_risk":false,"min_order_size":"5","hash":"native-book-hash"})
    }

    #[test]
    fn initialized_recovery_forwards_regressions_to_core_instead_of_preserving_stale_depth() {
        for regressed in ["delta", "snapshot", "tick"] {
            let mut selected = market();
            selected.tick_micro = 10_000;
            let mut core = Core::new(CoreConfig {
                initial_collateral_micro: 1_000_000_000,
                max_imbalance_micro: 10_000_000,
                lot_micro: 5_000_000,
                half_width_micro: 10_000,
                max_feed_age_ms: 1000,
            })
            .unwrap();
            core.set_market(selected.clone()).unwrap();
            core.control(Action::Resume).unwrap();
            let base = selected.window_start_ms;
            let mut recovery = Recovery::new([base + 100; 2]);
            let book = |token: &str, clock: u64| {
                json!({"event_type":"book","market":selected.condition_id,"asset_id":token,
                "timestamp":clock.to_string(),"bids":[{"price":"0.49","size":"10"}],"asks":[{"price":"0.51","size":"10"}]})
            };
            for (token, observed) in [(&selected.up_token, 10), (&selected.down_token, 11)] {
                for event in recovery.filter(
                    parse_clob(&book(token, base + 101).to_string(), &selected, observed).unwrap(),
                ) {
                    core.apply(event).unwrap();
                }
            }
            assert_eq!(core.evaluate(base + 201, 11).intents.len(), 2);
            let tick = json!({"event_type":"tick_size_change","market":selected.condition_id,"asset_id":selected.up_token,
                "timestamp":(base+200).to_string(),"old_tick_size":"0.01","new_tick_size":"0.001"});
            for event in recovery.filter(parse_clob(&tick.to_string(), &selected, 12).unwrap()) {
                core.apply(event).unwrap();
            }
            let adverse = match regressed {
                "delta" => {
                    json!({"event_type":"price_change","market":selected.condition_id,"timestamp":(base+150).to_string(),
                    "price_changes":[{"asset_id":selected.up_token,"price":"0.49","size":"0","side":"BUY"}]})
                }
                "snapshot" => book(&selected.up_token, base + 150),
                _ => {
                    json!({"event_type":"tick_size_change","market":selected.condition_id,"asset_id":selected.up_token,
                    "timestamp":(base+150).to_string(),"old_tick_size":"0.001","new_tick_size":"0.01"})
                }
            };
            let events = recovery.filter(parse_clob(&adverse.to_string(), &selected, 13).unwrap());
            assert_eq!(
                events.len(),
                1,
                "post-bootstrap {regressed} must reach the domain validator"
            );
            assert!(core.apply(events.into_iter().next().unwrap()).is_err());
            let decision = core.evaluate(base + 201, 14);
            assert!(decision.intents.is_empty());
            assert!(decision.stop_reasons.contains(&StopReason::InvalidInput));
        }
    }

    #[test]
    fn recovery_uses_rest_current_tick_instead_of_initial_gamma_tick() {
        let selected = market();
        assert_eq!(selected.tick_micro, 1000);
        let recovered = parse_rest_book(
            &rest_book(&selected, "0.01", "1790856000200"),
            &selected,
            Outcome::Up,
            77,
        )
        .unwrap();
        assert!(matches!(
            &recovered[0],
            FeedEvent::Tick {
                old_tick_micro: None,
                new_tick_micro: 10_000,
                exchange_ms: 1790856000200,
                observed_mono_ms: 77,
                ..
            }
        ));
        assert!(matches!(
            &recovered[1],
            FeedEvent::Snapshot {
                outcome: Outcome::Up,
                ..
            }
        ));
        let mut wrong = rest_book(&selected, "0.01", "1790856000200");
        wrong["asset_id"] = "456".into();
        assert!(parse_rest_book(&wrong, &selected, Outcome::Up, 77).is_err());
        wrong["asset_id"] = "123".into();
        wrong["tick_size"] = 0.01.into();
        assert!(parse_rest_book(&wrong, &selected, Outcome::Up, 77).is_err());
        wrong["tick_size"] = "0.03".into();
        assert!(parse_rest_book(&wrong, &selected, Outcome::Up, 77).is_err());
    }

    #[test]
    fn recovery_cutoffs_are_per_outcome_and_native_batches_are_validated_first() {
        let selected = market();
        let mut recovery = Recovery::new([200, 300]);
        let mut full = snapshot(&selected);
        full["timestamp"] = "200".into();
        assert!(
            recovery
                .filter(parse_clob(&full.to_string(), &selected, 7).unwrap())
                .is_empty()
        );
        assert!(!recovery.ready());
        full["timestamp"] = "201".into();
        assert_eq!(
            recovery
                .filter(parse_clob(&full.to_string(), &selected, 7).unwrap())
                .len(),
            1
        );
        assert!(!recovery.ready());
        let native = json!({"event_type":"price_change","market":selected.condition_id,"timestamp":"250",
            "price_changes":[{"asset_id":"123","price":"0.4","size":"1","side":"BUY"},
                {"asset_id":"456","price":"0.4","size":"2","side":"BUY"}]});
        let events = recovery.filter(parse_clob(&native.to_string(), &selected, 7).unwrap());
        assert!(
            matches!(&events[0], FeedEvent::Delta { changes, .. } if changes.len() == 1 && changes[0].outcome == Outcome::Up)
        );
        full["asset_id"] = "456".into();
        full["timestamp"] = "301".into();
        assert_eq!(
            recovery
                .filter(parse_clob(&full.to_string(), &selected, 8).unwrap())
                .len(),
            1
        );
        assert!(recovery.ready());
        // Equal REST/WS clocks never initialize a book; only native later snapshots do.
        let mut invalid_old = native;
        invalid_old["price_changes"][0]["size"] = 1.into();
        assert!(parse_clob(&invalid_old.to_string(), &selected, 7).is_err());
        let recovered = parse_rest_book(
            &rest_book(&selected, "0.01", "199"),
            &selected,
            Outcome::Up,
            77,
        )
        .unwrap();
        // Once initialized, chronology is validated by Core rather than suppressed here.
        assert_eq!(recovery.filter(recovered).len(), 2);
    }

    #[tokio::test]
    async fn continuous_deltas_and_pongs_cannot_extend_the_bootstrap_deadline() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server_market = selected.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            let delta = json!({"event_type":"price_change","market":server_market.condition_id,"timestamp":"150",
                "price_changes":[{"asset_id":"123","price":"0.4","size":"1","side":"BUY"}]}).to_string();
            let mut cadence = tokio::time::interval(Duration::from_millis(5));
            loop {
                tokio::select! {
                    _ = cadence.tick() => {
                        if socket.send(Message::Text(delta.clone().into())).await.is_err() { break; }
                        if socket.send(Message::Text("PONG".into())).await.is_err() { break; }
                    },
                    message = socket.next() => {
                        if matches!(message, None | Some(Err(_)) | Some(Ok(Message::Close(_)))) { break; }
                    },
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, _state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let mut settings = fast_settings();
        settings.bootstrap = Duration::from_millis(80);
        let client = tokio::spawn(async move {
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings,
                    tick_refresh: TickRefresh::Fixture([100, 100]),
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            )
            .await
        });
        let event = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, FeedEvent::Disconnect { source:FeedSource::Clob, reason } if reason.contains("bootstrap timeout"))
        );
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(500), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_millis(500), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_websocket_disconnect_reconnects_and_resubscribes_until_shutdown() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server_market = selected.clone();
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let subscription: Value = serde_json::from_str(&subscription).unwrap();
                assert_eq!(subscription["type"], "market");
                assert_eq!(
                    subscription["assets_ids"],
                    json!([server_market.up_token, server_market.down_token])
                );
                socket
                    .send(Message::Text(snapshot(&server_market).to_string().into()))
                    .await
                    .unwrap();
                if connection == 0 {
                    let _ = socket.close(None).await;
                } else {
                    while let Some(Ok(message)) = socket.next().await {
                        if matches!(message, Message::Close(_)) {
                            break;
                        }
                        if matches!(&message, Message::Text(text) if text.as_str() == "PING") {
                            socket.send(Message::Text("PONG".into())).await.unwrap();
                        }
                    }
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let client = tokio::spawn(async move {
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings: fast_settings(),
                    tick_refresh: TickRefresh::Skip,
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            )
            .await
        });
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let reset = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let resumed = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, FeedEvent::Snapshot { .. }));
        assert!(matches!(
            reset,
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        assert!(matches!(resumed, FeedEvent::Snapshot { .. }));
        assert!(state.borrow().is_none());
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(500), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_millis(500), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_websocket_overflow_faults_without_emitting_first_snapshot() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server_market = selected.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            let first = snapshot(&server_market);
            let mut second = first.clone();
            second["asset_id"] = json!(server_market.down_token);
            socket
                .send(Message::Text(json!([first, second]).to_string().into()))
                .await
                .unwrap();
        });
        let (tx, mut rx) = mpsc::channel(1);
        let (fault, state) = watch::channel(None);
        let (_shutdown, control) = watch::channel(false);
        let client = tokio::spawn(async move {
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings: fast_settings(),
                    tick_refresh: TickRefresh::Skip,
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(2), client)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert_eq!(state.borrow().as_deref(), Some("feed queue overflow"));
        assert!(matches!(
            rx.try_recv().unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        assert!(rx.try_recv().is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn outgoing_pings_cannot_extend_the_receive_deadline() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            while socket.next().await.is_some() {}
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, _state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let mut settings = fast_settings();
        settings.receive = Duration::from_millis(80);
        settings.ping = Duration::from_millis(10);
        let client = tokio::spawn(async move {
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings,
                    tick_refresh: TickRefresh::Skip,
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            )
            .await
        });
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(400), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(500), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_millis(500), server)
            .await
            .unwrap()
            .unwrap();
    }
}
