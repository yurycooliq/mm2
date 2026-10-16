pub use crate::domain::parse_micro;
use crate::domain::{
    BookSide, Change, FeedEvent, FeedSource, Level, Market, Outcome, valid_tick_micro,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message, error::ProtocolError, protocol::WebSocketConfig,
    },
};

pub const CLOB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const BINANCE_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const CLOB_BOOK_URL: &str = "https://clob.polymarket.com/book";
const MAX_REST_BYTES: usize = 262_144;
pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
const MAX_MEMBERS: usize = 4096;
// Admission retains one fully validated frame and streams ordered chunks through
// the existing event-count bound. A valid wire frame need not fit in the queue.
const ADMISSION_CHUNK: usize = 16;
const MAX_DEPTH: usize = 10_000;
// Binance is a latest spot-price diagnostic, not an execution tape or TWAP.
const REFERENCE_INTERVAL: Duration = Duration::from_millis(50);
const UINT256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const UNCLEAN_WEBSOCKET_CLOSE: &str = "public WebSocket closed without closing handshake";
// Append-only fixed categories: never format protocol variants or retain their
// headers, payloads, URLs or nested error messages in secondary diagnostics.
const PROTOCOL_FAILURES: [(&str, &str); 26] = [
    (
        "invalid public WebSocket protocol: wrong_http_method",
        "websocket_protocol_wrong_http_method",
    ),
    (
        "invalid public WebSocket protocol: wrong_http_version",
        "websocket_protocol_wrong_http_version",
    ),
    (
        "invalid public WebSocket protocol: missing_connection_upgrade",
        "websocket_protocol_missing_connection_upgrade",
    ),
    (
        "invalid public WebSocket protocol: missing_websocket_upgrade",
        "websocket_protocol_missing_websocket_upgrade",
    ),
    (
        "invalid public WebSocket protocol: missing_websocket_version",
        "websocket_protocol_missing_websocket_version",
    ),
    (
        "invalid public WebSocket protocol: missing_websocket_key",
        "websocket_protocol_missing_websocket_key",
    ),
    (
        "invalid public WebSocket protocol: accept_key_mismatch",
        "websocket_protocol_accept_key_mismatch",
    ),
    (
        "invalid public WebSocket protocol: subprotocol",
        "websocket_protocol_subprotocol",
    ),
    (
        "invalid public WebSocket protocol: junk_after_request",
        "websocket_protocol_junk_after_request",
    ),
    (
        "invalid public WebSocket protocol: custom_response_successful",
        "websocket_protocol_custom_response_successful",
    ),
    (
        "invalid public WebSocket protocol: invalid_header",
        "websocket_protocol_invalid_header",
    ),
    (
        "invalid public WebSocket protocol: handshake_incomplete",
        "websocket_protocol_handshake_incomplete",
    ),
    (
        "invalid public WebSocket protocol: http_parse",
        "websocket_protocol_http_parse",
    ),
    (
        "invalid public WebSocket protocol: send_after_closing",
        "websocket_protocol_send_after_closing",
    ),
    (
        "invalid public WebSocket protocol: received_after_closing",
        "websocket_protocol_received_after_closing",
    ),
    (
        "invalid public WebSocket protocol: nonzero_reserved_bits",
        "websocket_protocol_nonzero_reserved_bits",
    ),
    (
        "invalid public WebSocket protocol: unmasked_client_frame",
        "websocket_protocol_unmasked_client_frame",
    ),
    (
        "invalid public WebSocket protocol: masked_server_frame",
        "websocket_protocol_masked_server_frame",
    ),
    (
        "invalid public WebSocket protocol: fragmented_control_frame",
        "websocket_protocol_fragmented_control_frame",
    ),
    (
        "invalid public WebSocket protocol: control_frame_too_big",
        "websocket_protocol_control_frame_too_big",
    ),
    (
        "invalid public WebSocket protocol: unknown_control_frame",
        "websocket_protocol_unknown_control_frame",
    ),
    (
        "invalid public WebSocket protocol: unknown_data_frame",
        "websocket_protocol_unknown_data_frame",
    ),
    (
        "invalid public WebSocket protocol: unexpected_continue_frame",
        "websocket_protocol_unexpected_continue_frame",
    ),
    (
        "invalid public WebSocket protocol: expected_fragment",
        "websocket_protocol_expected_fragment",
    ),
    (
        "invalid public WebSocket protocol: invalid_opcode",
        "websocket_protocol_invalid_opcode",
    ),
    (
        "invalid public WebSocket protocol: invalid_close_sequence",
        "websocket_protocol_invalid_close_sequence",
    ),
];

fn websocket_protocol_reason(error: ProtocolError) -> &'static str {
    let index = match error {
        ProtocolError::WrongHttpMethod => 0,
        ProtocolError::WrongHttpVersion => 1,
        ProtocolError::MissingConnectionUpgradeHeader => 2,
        ProtocolError::MissingUpgradeWebSocketHeader => 3,
        ProtocolError::MissingSecWebSocketVersionHeader => 4,
        ProtocolError::MissingSecWebSocketKey => 5,
        ProtocolError::SecWebSocketAcceptKeyMismatch => 6,
        ProtocolError::SecWebSocketSubProtocolError(_) => 7,
        ProtocolError::JunkAfterRequest => 8,
        ProtocolError::CustomResponseSuccessful => 9,
        ProtocolError::InvalidHeader(_) => 10,
        ProtocolError::HandshakeIncomplete => 11,
        ProtocolError::HttparseError(_) => 12,
        ProtocolError::SendAfterClosing => 13,
        ProtocolError::ReceivedAfterClosing => 14,
        ProtocolError::NonZeroReservedBits => 15,
        ProtocolError::UnmaskedFrameFromClient => 16,
        ProtocolError::MaskedFrameFromServer => 17,
        ProtocolError::FragmentedControlFrame => 18,
        ProtocolError::ControlFrameTooBig => 19,
        ProtocolError::UnknownControlFrameType(_) => 20,
        ProtocolError::UnknownDataFrameType(_) => 21,
        ProtocolError::UnexpectedContinueFrame => 22,
        ProtocolError::ExpectedFragment(_) => 23,
        ProtocolError::ResetWithoutClosingHandshake => return UNCLEAN_WEBSOCKET_CLOSE,
        ProtocolError::InvalidOpcode(_) => 24,
        ProtocolError::InvalidCloseSequence => 25,
    };
    PROTOCOL_FAILURES[index].0
}

/// Optional diagnostics never enter the financial/feed queue or require a network/disk write.
/// Fixed categories are used rather than response bodies, URLs or exception text.
#[derive(Default)]
pub struct FeedDiagnostics {
    sources: [SourceDiagnostics; 2],
}

#[derive(Default)]
struct SourceDiagnostics {
    attempts: AtomicU64,
    transport_disconnects: AtomicU64,
    bootstrap_timeouts: AtomicU64,
    backoff_resets: AtomicU64,
    retry_delay_ms: AtomicU64,
    snapshots_seen: [AtomicU64; 2],
    snapshots_discarded_before_cutoff: [AtomicU64; 2],
    stale_snapshots_discarded: [AtomicU64; 2],
    book_exchange_watermark_ms: [AtomicU64; 2],
    bootstrap_cutoff_exchange_ms: [AtomicU64; 2],
    phase: AtomicU8,
    last_failure: AtomicU64,
    reference_received: AtomicU64,
    reference_coalesced: AtomicU64,
    reference_delivered: AtomicU64,
    reference_queue_full: AtomicU64,
    reference_discarded: AtomicU64,
    reference_pending: AtomicU64,
    ingress_frames: AtomicU64,
    ingress_events: AtomicU64,
    ingress_enqueued: AtomicU64,
    ingress_waits: AtomicU64,
    ingress_wait_ns: AtomicU64,
    ingress_timeouts: AtomicU64,
    ingress_pending_events: AtomicU64,
    ingress_discarded_events: AtomicU64,
    ingress_max_frame_events: AtomicU64,
    ingress_queue_high_watermark: AtomicU64,
}

#[derive(Clone, Serialize)]
pub struct FeedDiagnosticsSnapshot {
    pub clob: SourceDiagnosticsSnapshot,
    pub binance: SourceDiagnosticsSnapshot,
}

#[derive(Clone, Serialize)]
pub struct SourceDiagnosticsSnapshot {
    attempts: u64,
    transport_disconnects: u64,
    bootstrap_timeouts: u64,
    backoff_resets: u64,
    retry_delay_ms: u64,
    snapshots_seen: [u64; 2],
    snapshots_discarded_before_cutoff: [u64; 2],
    stale_snapshots_discarded: [u64; 2],
    book_exchange_watermark_ms: [u64; 2],
    bootstrap_cutoff_exchange_ms: [u64; 2],
    phase: &'static str,
    last_failure: Option<&'static str>,
    last_failure_fatal: bool,
    last_failure_at_mono_ms: Option<u64>,
    reference_received: u64,
    reference_coalesced: u64,
    reference_delivered: u64,
    reference_queue_full: u64,
    reference_discarded: u64,
    reference_pending: u64,
    ingress_frames: u64,
    ingress_events: u64,
    ingress_enqueued: u64,
    ingress_waits: u64,
    ingress_wait_ns: u64,
    ingress_timeouts: u64,
    ingress_pending_events: u64,
    ingress_discarded_events: u64,
    ingress_max_frame_events: u64,
    ingress_queue_high_watermark: u64,
    reference_publish_interval_ms: Option<u64>,
}

fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(1))
    });
}

impl FeedDiagnostics {
    fn source(&self, source: FeedSource) -> &SourceDiagnostics {
        &self.sources[if source == FeedSource::Clob { 0 } else { 1 }]
    }

    pub fn snapshot(&self) -> FeedDiagnosticsSnapshot {
        let source = |source: FeedSource| {
            let value = self.source(source);
            let failure = value.last_failure.load(Ordering::Relaxed);
            let failure_class = (failure & 255) as u8;
            SourceDiagnosticsSnapshot {
                attempts: value.attempts.load(Ordering::Relaxed),
                transport_disconnects: value.transport_disconnects.load(Ordering::Relaxed),
                bootstrap_timeouts: value.bootstrap_timeouts.load(Ordering::Relaxed),
                backoff_resets: value.backoff_resets.load(Ordering::Relaxed),
                retry_delay_ms: value.retry_delay_ms.load(Ordering::Relaxed),
                snapshots_seen: std::array::from_fn(|i| {
                    value.snapshots_seen[i].load(Ordering::Relaxed)
                }),
                snapshots_discarded_before_cutoff: std::array::from_fn(|i| {
                    value.snapshots_discarded_before_cutoff[i].load(Ordering::Relaxed)
                }),
                stale_snapshots_discarded: std::array::from_fn(|i| {
                    value.stale_snapshots_discarded[i].load(Ordering::Relaxed)
                }),
                book_exchange_watermark_ms: std::array::from_fn(|i| {
                    value.book_exchange_watermark_ms[i].load(Ordering::Relaxed)
                }),
                bootstrap_cutoff_exchange_ms: std::array::from_fn(|i| {
                    value.bootstrap_cutoff_exchange_ms[i].load(Ordering::Relaxed)
                }),
                phase: match value.phase.load(Ordering::Relaxed) {
                    1 => "connecting",
                    2 => "rest_recovery",
                    3 => "awaiting_native_snapshot",
                    4 => "streaming",
                    5 => "retrying",
                    6 => "faulted",
                    7 => "stopped",
                    _ => "starting",
                },
                last_failure: failure_name(failure_class),
                last_failure_fatal: failure & 256 != 0,
                last_failure_at_mono_ms: (failure_class != 0).then_some(failure >> 9),
                reference_received: value.reference_received.load(Ordering::Relaxed),
                reference_coalesced: value.reference_coalesced.load(Ordering::Relaxed),
                reference_delivered: value.reference_delivered.load(Ordering::Relaxed),
                reference_queue_full: value.reference_queue_full.load(Ordering::Relaxed),
                reference_discarded: value.reference_discarded.load(Ordering::Relaxed),
                reference_pending: value.reference_pending.load(Ordering::Relaxed),
                ingress_frames: value.ingress_frames.load(Ordering::Relaxed),
                ingress_events: value.ingress_events.load(Ordering::Relaxed),
                ingress_enqueued: value.ingress_enqueued.load(Ordering::Relaxed),
                ingress_waits: value.ingress_waits.load(Ordering::Relaxed),
                ingress_wait_ns: value.ingress_wait_ns.load(Ordering::Relaxed),
                ingress_timeouts: value.ingress_timeouts.load(Ordering::Relaxed),
                ingress_pending_events: value.ingress_pending_events.load(Ordering::Relaxed),
                ingress_discarded_events: value.ingress_discarded_events.load(Ordering::Relaxed),
                ingress_max_frame_events: value.ingress_max_frame_events.load(Ordering::Relaxed),
                ingress_queue_high_watermark: value
                    .ingress_queue_high_watermark
                    .load(Ordering::Relaxed),
                reference_publish_interval_ms: (source == FeedSource::Binance)
                    .then_some(REFERENCE_INTERVAL.as_millis() as u64),
            }
        };
        FeedDiagnosticsSnapshot {
            clob: source(FeedSource::Clob),
            binance: source(FeedSource::Binance),
        }
    }
}

fn failure_code(reason: &str, fatal: bool) -> u8 {
    if let Some(index) = PROTOCOL_FAILURES.iter().position(|entry| entry.0 == reason) {
        return 20 + index as u8;
    }
    match reason {
        UNCLEAN_WEBSOCKET_CLOSE => 18,
        "fresh both-outcome snapshot bootstrap timeout" => 1,
        "public WebSocket connection failed" | "public WebSocket connect timeout" => 2,
        "public REST book transport failed"
        | "public REST book HTTP unavailable"
        | "public REST book body failed" => 3,
        "public WebSocket receive timeout" => 4,
        "public WebSocket remote close"
        | "public WebSocket ended"
        | "public WebSocket read failed" => 5,
        "public heartbeat send failed"
        | "public recovery heartbeat send failed"
        | "public pong send failed"
        | "public subscription send failed" => 6,
        "feed queue overflow" => 16,
        "feed consumer closed" => 17,
        "feed backpressure deadline exceeded" => 46,
        "public feed frame exceeds event limit" => 8,
        "public WebSocket exceeds capacity limit" | "public REST book exceeds 256 KiB" => 8,
        "invalid public WebSocket protocol" | "unsupported public WebSocket frame" => 9,
        reason if reason.contains("tick") => 12,
        reason
            if reason.contains("clock")
                || reason.contains("timestamp")
                || reason.contains("unsigned integer") =>
        {
            13
        }
        reason
            if reason.contains("condition")
                || reason.contains("token")
                || reason.contains("requested outcome") =>
        {
            14
        }
        reason
            if reason.contains("depth")
                || reason.contains("snapshot")
                || reason.contains("crossed")
                || reason.contains("book level") =>
        {
            15
        }
        _ if fatal => 10,
        _ => 11,
    }
}

fn failure_name(code: u8) -> Option<&'static str> {
    match code {
        0 => None,
        1 => Some("native_snapshot_bootstrap_timeout"),
        2 => Some("connect_unavailable"),
        3 => Some("rest_book_unavailable"),
        4 => Some("receive_timeout"),
        5 => Some("transport_disconnected"),
        6 => Some("transport_send_failed"),
        7 => Some("feed_queue_unavailable"),
        8 => Some("input_capacity_exceeded"),
        9 => Some("invalid_websocket_protocol"),
        10 => Some("invalid_public_input"),
        12 => Some("invalid_tick_input"),
        13 => Some("invalid_exchange_clock"),
        14 => Some("invalid_market_identity"),
        15 => Some("invalid_book_input"),
        16 => Some("feed_queue_overflow"),
        17 => Some("feed_consumer_closed"),
        18 => Some("websocket_unclean_disconnect"),
        46 => Some("feed_backpressure_timeout"),
        20..=45 => PROTOCOL_FAILURES
            .get(usize::from(code - 20))
            .map(|entry| entry.1),
        _ => Some("transport_unavailable"),
    }
}

pub(crate) fn input_failure_class(reason: &str) -> &'static str {
    failure_name(failure_code(reason, true)).unwrap_or("invalid_public_input")
}

/// Fixed public categories only: a rejected message or arbitrary error must not
/// enter process logs. Keep enough detail to distinguish parser failures.
pub fn input_failure_detail(reason: &str) -> &'static str {
    match reason {
        "invalid decimal length" | "decimal must be an unsigned fixed string" => "invalid_decimal",
        "CLOB price outside unit interval" => "price_outside_unit_interval",
        "invalid public BBO spread" => "inconsistent_bbo_spread",
        "crossed or oversized batch result" => "crossed_or_oversized_delta",
        "crossed or oversized book" => "crossed_or_oversized_snapshot",
        "delta before snapshot or invalid price" => "delta_without_snapshot_or_invalid_price",
        "invalid or incompatible tick transition" => "incompatible_tick_transition",
        "native snapshot recovery deadline exceeded" => "native_snapshot_timeout",
        "CLOB receive clock regressed" => "clob_receive_clock_regressed",
        "CLOB exchange clock regressed" => "clob_exchange_clock_regressed",
        "zero trade quantity" => "zero_trade_quantity",
        "unsupported CLOB event type" => "unsupported_clob_event_type",
        "invalid CLOB JSON" => "invalid_clob_json",
        "missing price changes" | "invalid price-change member count" => {
            "invalid_price_change_members"
        }
        "feed member must be an object" => "non_object_member",
        "missing string price" => "missing_price_string",
        "missing string size" => "missing_size_string",
        "missing string best_bid" => "missing_best_bid_string",
        "missing string best_ask" => "missing_best_ask_string",
        "missing string spread" => "missing_spread_string",
        "missing string fee_rate_bps" => "missing_fee_string",
        "invalid optional decimal field" => "invalid_optional_decimal_type",
        "invalid book side" => "invalid_order_side",
        "missing string timestamp" | "invalid CLOB timestamp" => "invalid_clob_timestamp",
        "missing lifecycle assets" | "invalid lifecycle asset count" => "invalid_lifecycle_assets",
        "invalid lifecycle token" | "duplicate lifecycle token" => "invalid_lifecycle_token",
        "feed backpressure deadline exceeded" => "feed_backpressure_timeout",
        "feed consumer closed" => "feed_consumer_closed",
        _ => "unclassified_feed_failure",
    }
}

#[cfg(test)]
mod clock_detail_tests {
    #[test]
    fn clock_failures_have_distinct_fixed_public_details() {
        assert_eq!(
            super::input_failure_class("CLOB receive clock regressed"),
            "invalid_exchange_clock"
        );
        assert_eq!(
            super::input_failure_detail("CLOB receive clock regressed"),
            "clob_receive_clock_regressed"
        );
        assert_eq!(
            super::input_failure_detail("CLOB exchange clock regressed"),
            "clob_exchange_clock_regressed"
        );
        assert_eq!(
            super::input_failure_detail("provider payload must not be echoed"),
            "unclassified_feed_failure"
        );
    }
}

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

/// Native CLOB optional decimals use empty strings (and nullable SDK fields).
/// Absence is metadata absence, never a fabricated zero price or quantity.
fn optional_decimal<'a>(
    value: &'a Map<String, Value>,
    name: &str,
) -> Result<Option<&'a str>, String> {
    match value.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.is_empty() => Ok(None),
        Some(Value::String(text)) => {
            validate_decimal(text)?;
            Ok(Some(text))
        }
        _ => Err("invalid optional decimal field".into()),
    }
}

fn optional_micro_field(value: &Map<String, Value>, name: &str) -> Result<Option<u64>, String> {
    optional_decimal(value, name)?.map(parse_micro).transpose()
}

fn optional_price_field(value: &Map<String, Value>, name: &str) -> Result<Option<u64>, String> {
    let price = optional_micro_field(value, name)?;
    if price.is_some_and(|price| price > 1_000_000) {
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

/// The native frame filter used by public sockets. Replay may reuse it only
/// with an explicitly known bootstrap baseline; zero means no REST seed proof.
pub struct Recovery {
    baseline: [u64; 2],
    initialized: [bool; 2],
    latest_exchange: [u64; 2],
    latest_receive: u64,
}
impl Recovery {
    pub fn new(baseline: [u64; 2]) -> Self {
        Self {
            baseline,
            initialized: [false; 2],
            latest_exchange: baseline,
            latest_receive: 0,
        }
    }
    #[cfg(test)]
    fn passthrough() -> Self {
        Self {
            baseline: [0; 2],
            initialized: [true; 2],
            latest_exchange: [0; 2],
            latest_receive: 0,
        }
    }
    pub fn ready(&self) -> bool {
        self.initialized.iter().all(|ready| *ready)
    }
    /// Read-only research evidence; never resets or advances native filter state.
    pub fn exchange_watermarks(&self) -> [u64; 2] {
        self.latest_exchange
    }
    fn advance_watermark(
        &mut self,
        index: usize,
        exchange_ms: u64,
        diagnostics: Option<&SourceDiagnostics>,
    ) {
        self.latest_exchange[index] = self.latest_exchange[index].max(exchange_ms);
        if let Some(diagnostics) = diagnostics {
            diagnostics.book_exchange_watermark_ms[index]
                .store(self.latest_exchange[index], Ordering::Relaxed);
        }
    }
    pub fn filter(&mut self, events: Vec<FeedEvent>) -> Vec<FeedEvent> {
        self.filter_observed(events, None)
    }
    fn filter_observed(
        &mut self,
        events: Vec<FeedEvent>,
        diagnostics: Option<&SourceDiagnostics>,
    ) -> Vec<FeedEvent> {
        let index = |outcome: Outcome| if outcome == Outcome::Up { 0 } else { 1 };
        events
            .into_iter()
            .filter_map(|event| match event {
                FeedEvent::Snapshot {
                    outcome,
                    exchange_ms,
                    observed_mono_ms,
                    ref bids,
                    ref asks,
                    ..
                } => {
                    let index = index(outcome);
                    if let Some(diagnostics) = diagnostics {
                        increment(&diagnostics.snapshots_seen[index]);
                    }
                    let before_bootstrap =
                        !self.initialized[index] && exchange_ms <= self.baseline[index];
                    let obsolete = exchange_ms < self.latest_exchange[index];
                    if (before_bootstrap || obsolete)
                        && observed_mono_ms >= self.latest_receive
                        && crate::domain::snapshot_shape_valid(bids, asks)
                    {
                        if let Some(diagnostics) = diagnostics {
                            increment(if before_bootstrap {
                                &diagnostics.snapshots_discarded_before_cutoff[index]
                            } else {
                                &diagnostics.stale_snapshots_discarded[index]
                            });
                        }
                        return None;
                    }
                    self.initialized[index] = true;
                    self.latest_receive = self.latest_receive.max(observed_mono_ms);
                    self.advance_watermark(index, exchange_ms, diagnostics);
                    Some(event)
                }
                FeedEvent::Tick {
                    outcome,
                    exchange_ms,
                    observed_mono_ms,
                    ..
                } => {
                    let index = index(outcome);
                    if !self.initialized[index] && exchange_ms <= self.baseline[index] {
                        return None;
                    }
                    self.latest_receive = self.latest_receive.max(observed_mono_ms);
                    self.advance_watermark(index, exchange_ms, diagnostics);
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
                        self.latest_receive = self.latest_receive.max(observed_mono_ms);
                        // Incremental updates and tick transitions remain strict:
                        // older deltas/ticks are forwarded to Core, never discarded.
                        // Advancing this floor also keeps a snapshot older than a
                        // split/crossed delta from pretending to complete recovery.
                        for change in &changes {
                            self.advance_watermark(index(change.outcome), exchange_ms, diagnostics);
                        }
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
    let side = string(value, "side")?;
    if side.eq_ignore_ascii_case("BUY") {
        Ok(BookSide::Bid)
    } else if side.eq_ignore_ascii_case("SELL") {
        Ok(BookSide::Ask)
    } else {
        Err("invalid book side".into())
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
    let informational = matches!(
        event_type,
        "best_bid_ask" | "new_market" | "market_resolved"
    );
    let exchange_ms = if informational {
        value
            .get("timestamp")
            .filter(|value| !value.is_null())
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
                    optional_price_field(entry, name)?;
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
            let old_tick_micro = optional_micro_field(value, "old_tick_size")?;
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
            let quantity_micro = optional_micro_field(value, "size")?;
            if quantity_micro == Some(0) {
                return Err("zero trade quantity".into());
            }
            let trade_side = side(value)?;
            optional_decimal(value, "fee_rate_bps")?;
            let Some(quantity_micro) = quantity_micro else {
                // A price-only trade notification is not evidence of executable
                // volume. Do not invent a size for the paper execution lane.
                return Ok(Vec::new());
            };
            // No native unique trade ID is supplied. Equal witnesses are deduplicated
            // conservatively in the paper lane; they never update the real account.
            use sha2::{Digest, Sha256};
            let fingerprint = hex::encode(Sha256::digest(
                serde_json::to_vec(&(
                    &identity,
                    string(value, "asset_id")?,
                    exchange_ms,
                    price_micro,
                    quantity_micro,
                    trade_side == BookSide::Ask,
                ))
                .map_err(|_| "cannot fingerprint public trade")?,
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
            let bid = optional_price_field(value, "best_bid")?;
            let ask = optional_price_field(value, "best_ask")?;
            let spread = optional_price_field(value, "spread")?;
            if let (Some(bid), Some(ask), Some(spread)) = (bid, ask, spread)
                && ask.checked_sub(bid) != Some(spread)
            {
                return Err("invalid public BBO spread".into());
            }
            Ok(Vec::new())
        }
        "market_resolved" | "new_market" => {
            // Lifecycle metadata is not a book update or settlement receipt. Missing
            // optional assets cannot change token mappings or create execution volume.
            if value.get("assets_ids").is_none_or(Value::is_null) {
                if identity != market.condition_id || event_type == "new_market" {
                    if identity == market.condition_id
                        && value.get("slug").is_some()
                        && string(value, "slug")? != market.slug
                    {
                        return Err("selected lifecycle slug changed".into());
                    }
                    return Ok(Vec::new());
                }
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

/// Parse/validate the entire frame before calling this function. Admission waits
/// for bounded capacity, preserving every event and its original clocks/order.
/// Shutdown may discard the remaining generation; a stalled consumer faults via
/// the independent watch channel even when no Disconnect fits in the queue.
pub async fn enqueue_frame(
    tx: &mpsc::Sender<FeedEvent>,
    events: Vec<FeedEvent>,
    fault: &watch::Sender<Option<String>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, String> {
    let result = enqueue_ordered(tx, events, Duration::from_secs(5), shutdown, None).await;
    if let Err(reason) = &result {
        latch_fault(fault, reason.clone());
    }
    result
}

struct Admission<'a> {
    diagnostics: Option<&'a SourceDiagnostics>,
    pending: usize,
    waiting: Option<Instant>,
}

fn add(counter: &AtomicU64, count: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(count))
    });
}

impl Admission<'_> {
    fn end_wait(&mut self) {
        if let Some(began) = self.waiting.take()
            && let Some(d) = self.diagnostics
        {
            add(
                &d.ingress_wait_ns,
                began.elapsed().as_nanos().try_into().unwrap_or(u64::MAX),
            );
        }
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.end_wait();
        if let Some(d) = self.diagnostics {
            add(&d.ingress_discarded_events, self.pending as u64);
            d.ingress_pending_events.store(0, Ordering::Relaxed);
        }
    }
}

async fn enqueue_ordered(
    tx: &mpsc::Sender<FeedEvent>,
    events: Vec<FeedEvent>,
    deadline: Duration,
    shutdown: &mut watch::Receiver<bool>,
    diagnostics: Option<&SourceDiagnostics>,
) -> Result<bool, String> {
    if events.len() > MAX_MEMBERS {
        return Err("public feed frame exceeds event limit".into());
    }
    let mut admission = Admission {
        diagnostics,
        pending: events.len(),
        waiting: None,
    };
    if let Some(d) = diagnostics {
        increment(&d.ingress_frames);
        add(&d.ingress_events, admission.pending as u64);
        d.ingress_max_frame_events
            .fetch_max(admission.pending as u64, Ordering::Relaxed);
        d.ingress_pending_events
            .store(admission.pending as u64, Ordering::Relaxed);
    }
    let deadline = tokio::time::Instant::now() + deadline;
    let mut events = events.into_iter();
    while admission.pending != 0 {
        if *shutdown.borrow() {
            return Ok(false);
        }
        if tokio::time::Instant::now() >= deadline {
            if let Some(d) = diagnostics {
                increment(&d.ingress_timeouts);
            }
            return Err("feed backpressure deadline exceeded".into());
        }
        let count = admission
            .pending
            .min(tx.max_capacity())
            .min(ADMISSION_CHUNK);
        let permits = match tx.try_reserve_many(count) {
            Ok(permits) => permits,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err("feed consumer closed".into()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(d) = diagnostics {
                    increment(&d.ingress_waits);
                }
                admission.waiting = Some(Instant::now());
                let reserved = tokio::select! {
                    biased;
                    _ = wait_shutdown(shutdown) => return Ok(false),
                    result = tokio::time::timeout_at(deadline, tx.reserve_many(count)) => result,
                };
                admission.end_wait();
                match reserved {
                    Ok(Ok(permits)) => permits,
                    Ok(Err(_)) => return Err("feed consumer closed".into()),
                    Err(_) => {
                        if let Some(d) = diagnostics {
                            increment(&d.ingress_timeouts);
                        }
                        return Err("feed backpressure deadline exceeded".into());
                    }
                }
            }
        };
        for permit in permits {
            permit.send(events.next().expect("bounded frame admission"));
        }
        admission.pending -= count;
        if let Some(d) = diagnostics {
            add(&d.ingress_enqueued, count as u64);
            d.ingress_pending_events
                .store(admission.pending as u64, Ordering::Relaxed);
            d.ingress_queue_high_watermark.fetch_max(
                (tx.max_capacity() - tx.capacity()) as u64,
                Ordering::Relaxed,
            );
        }
    }
    Ok(true)
}

/// The primary lane backpressures the source. Optional paper overflow faults
/// only that lane. A full primary queue must never hide shutdown or a fault.
pub async fn relay(
    mut input: mpsc::Receiver<FeedEvent>,
    primary: mpsc::Sender<FeedEvent>,
    paper: Option<(mpsc::Sender<FeedEvent>, watch::Sender<Option<String>>)>,
    mut shutdown: watch::Receiver<bool>,
    fault: watch::Sender<Option<String>>,
) -> Result<(), String> {
    let mut fault_rx = fault.subscribe();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if let Some(reason) = fault_rx.borrow().clone() {
            return Err(reason);
        }
        let event = tokio::select! {
            biased;
            _ = wait_shutdown(&mut shutdown) => return Ok(()),
            _ = fault_rx.changed() => continue,
            event = input.recv() => match event { Some(event) => event, None => return Ok(()) },
        };
        let paper_event = paper.as_ref().map(|_| event.clone());
        let admitted = tokio::select! {
            biased;
            _ = fault_rx.changed() => continue,
            result = enqueue_ordered(&primary, vec![event], Duration::from_secs(5), &mut shutdown, None) => result,
        };
        match admitted {
            Ok(false) => return Ok(()),
            Err(reason) => {
                latch_fault(&fault, reason.clone());
                return Err(reason);
            }
            Ok(true) => {}
        }
        if let (Some((sender, paper_fault)), Some(event)) = (&paper, paper_event)
            && sender.try_send(event).is_err()
        {
            latch_fault(
                paper_fault,
                "parallel simulation lost a feed event; rebuild its source state".into(),
            );
        }
    }
}

/// One pending diagnostic price per socket. Never coalesce CLOB books/trades,
/// stamp a queued price with a new receive clock, or suppress invalid input.
struct ReferenceBuffer<'a> {
    pending: Option<FeedEvent>,
    last_clock: Option<(u64, u64)>,
    diagnostics: Option<&'a SourceDiagnostics>,
}

impl<'a> ReferenceBuffer<'a> {
    fn new(diagnostics: Option<&'a SourceDiagnostics>) -> Self {
        Self {
            pending: None,
            last_clock: None,
            diagnostics,
        }
    }

    fn observe(&mut self, event: FeedEvent) -> Result<(), String> {
        let FeedEvent::Reference {
            exchange_ms,
            observed_mono_ms,
            price_micro,
        } = &event
        else {
            return Err("unexpected Binance reference event".into());
        };
        if *price_micro == 0
            || self.last_clock.is_some_and(|(exchange, observed)| {
                *exchange_ms < exchange || *observed_mono_ms < observed
            })
        {
            return Err("invalid or regressed Binance reference input".into());
        }
        self.last_clock = Some((*exchange_ms, *observed_mono_ms));
        let replaced = self.pending.replace(event).is_some();
        if let Some(diagnostics) = self.diagnostics {
            increment(&diagnostics.reference_received);
            if replaced {
                increment(&diagnostics.reference_coalesced);
            }
            diagnostics.reference_pending.store(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn flush(&mut self, tx: &mpsc::Sender<FeedEvent>) -> Result<(), String> {
        if self.pending.is_none() {
            return Ok(());
        }
        let permit = match tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(diagnostics) = self.diagnostics {
                    increment(&diagnostics.reference_queue_full);
                }
                return Ok(());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err("feed consumer closed".into()),
        };
        permit.send(self.pending.take().expect("pending reference reserved"));
        if let Some(diagnostics) = self.diagnostics {
            increment(&diagnostics.reference_delivered);
            diagnostics.reference_pending.store(0, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Drop for ReferenceBuffer<'_> {
    fn drop(&mut self) {
        // A previous socket's pending price cannot appear after its Disconnect.
        if self.pending.is_some()
            && let Some(diagnostics) = self.diagnostics
        {
            increment(&diagnostics.reference_discarded);
            diagnostics.reference_pending.store(0, Ordering::Relaxed);
        }
    }
}

async fn enqueue_reference_disconnect(
    tx: &mpsc::Sender<FeedEvent>,
    event: FeedEvent,
    deadline: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, String> {
    // Both sources' Disconnects follow already admitted data, with bounded wait.
    enqueue_ordered(tx, vec![event], deadline, shutdown, None).await
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
    stable: Duration,
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
            stable: Duration::from_secs(60),
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
    diagnostics: Option<Arc<FeedDiagnostics>>,
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
    _fault: &watch::Sender<Option<String>>,
    shutdown: &mut watch::Receiver<bool>,
    ready_since: &mut Option<Instant>,
) -> SessionResult {
    let source = endpoint.source;
    let settings = &endpoint.settings;
    let diagnostics = endpoint
        .diagnostics
        .as_ref()
        .map(|value| value.source(source));
    if let Some(diagnostics) = diagnostics {
        increment(&diagnostics.attempts);
        diagnostics.phase.store(1, Ordering::Relaxed);
        for cutoff in &diagnostics.bootstrap_cutoff_exchange_ms {
            cutoff.store(0, Ordering::Relaxed);
        }
        for watermark in &diagnostics.book_exchange_watermark_ms {
            watermark.store(0, Ordering::Relaxed);
        }
    }
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
        if let Some(diagnostics) = diagnostics {
            diagnostics.phase.store(2, Ordering::Relaxed);
        }
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
        if let Some(diagnostics) = diagnostics {
            for (index, value) in recovered_baseline.into_iter().enumerate() {
                diagnostics.bootstrap_cutoff_exchange_ms[index].store(value, Ordering::Relaxed);
                diagnostics.book_exchange_watermark_ms[index].store(value, Ordering::Relaxed);
            }
        }
        match enqueue_ordered(tx, events, settings.send, shutdown, diagnostics).await {
            Ok(true) => {}
            Ok(false) => return SessionResult::Shutdown,
            Err(error) => return SessionResult::Fatal(error),
        }
    }
    #[cfg(test)]
    match endpoint.tick_refresh {
        TickRefresh::Skip => recovery_state = Recovery::passthrough(),
        TickRefresh::Fixture(baseline) => {
            recovery_state = Recovery::new(baseline);
            if let Some(diagnostics) = diagnostics {
                for (index, value) in baseline.into_iter().enumerate() {
                    diagnostics.bootstrap_cutoff_exchange_ms[index].store(value, Ordering::Relaxed);
                    diagnostics.book_exchange_watermark_ms[index].store(value, Ordering::Relaxed);
                }
            }
        }
        TickRefresh::Public => {}
    }
    if source == FeedSource::Binance {
        recovery_state.initialized = [true; 2];
    }
    if let Some(diagnostics) = diagnostics {
        diagnostics.phase.store(
            if recovery_state.ready() { 4 } else { 3 },
            Ordering::Relaxed,
        );
    }
    let bootstrap_deadline = tokio::time::sleep(settings.bootstrap);
    tokio::pin!(bootstrap_deadline);
    let idle = tokio::time::sleep(settings.receive);
    tokio::pin!(idle);
    let mut ping = tokio::time::interval(settings.ping);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reference_tick = tokio::time::interval(REFERENCE_INTERVAL);
    reference_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reference = ReferenceBuffer::new(diagnostics);
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
            _ = reference_tick.tick(), if source == FeedSource::Binance => {
                if let Err(error) = reference.flush(tx) { return SessionResult::Fatal(error); }
            },
            received = socket.next() => {
                let message = match received {
                    Some(Ok(message)) => message,
                    Some(Err(WebSocketError::Capacity(_))) => return SessionResult::Fatal("public WebSocket exceeds capacity limit".into()),
                    Some(Err(WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => return SessionResult::Transport(UNCLEAN_WEBSOCKET_CLOSE),
                    Some(Err(WebSocketError::Protocol(error))) => return SessionResult::Fatal(websocket_protocol_reason(error).into()),
                    Some(Err(_)) => return SessionResult::Transport("public WebSocket read failed"),
                    None => return SessionResult::Transport("public WebSocket ended"),
                };
                idle.as_mut().reset(tokio::time::Instant::now() + settings.receive);
                match message {
                    Message::Text(text) => {
                        let observed_mono_ms = match u64::try_from(start.elapsed().as_millis()) { Ok(value) => value, Err(_) => return SessionResult::Fatal("monotonic clock overflow".into()) };
                        let events = if source == FeedSource::Clob { parse_clob(&text, market, observed_mono_ms) } else { parse_binance(&text, observed_mono_ms) };
                        let events = match events { Ok(events) => events, Err(error) => return SessionResult::Fatal(error) };
                        let events = if source == FeedSource::Clob { recovery_state.filter_observed(events, diagnostics) } else { events };
                        if recovery_state.ready() && events.iter().any(|event| matches!(event, FeedEvent::Snapshot { .. } | FeedEvent::Delta { .. } | FeedEvent::Reference { .. })) {
                            ready_since.get_or_insert_with(Instant::now);
                        }
                        if recovery_state.ready() && let Some(diagnostics) = diagnostics { diagnostics.phase.store(4, Ordering::Relaxed); }
                        if source == FeedSource::Binance {
                            for event in events {
                                if let Err(error) = reference.observe(event) { return SessionResult::Fatal(error); }
                            }
                        } else {
                            match enqueue_ordered(tx, events, settings.send, shutdown, diagnostics).await {
                                Ok(true) => {},
                                Ok(false) => return SessionResult::Shutdown,
                                Err(error) => return SessionResult::Fatal(error),
                            }
                        }
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

fn record_failure(
    endpoint: &Endpoint,
    start: Instant,
    reason: &str,
    fatal: bool,
    retry: Duration,
    reset_backoff: bool,
) {
    let Some(diagnostics) = &endpoint.diagnostics else {
        return;
    };
    let diagnostics = diagnostics.source(endpoint.source);
    let code = failure_code(reason, fatal);
    let at = u64::try_from(start.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .min(u64::MAX >> 9);
    diagnostics.last_failure.store(
        (at << 9) | u64::from(code) | if fatal { 256 } else { 0 },
        Ordering::Relaxed,
    );
    diagnostics
        .phase
        .store(if fatal { 6 } else { 5 }, Ordering::Relaxed);
    if !fatal {
        increment(&diagnostics.transport_disconnects);
    }
    if code == 1 {
        increment(&diagnostics.bootstrap_timeouts);
    }
    if reset_backoff {
        increment(&diagnostics.backoff_resets);
    }
    diagnostics.retry_delay_ms.store(
        if fatal {
            0
        } else {
            u64::try_from(retry.as_millis()).unwrap_or(u64::MAX)
        },
        Ordering::Relaxed,
    );
    tracing::warn!(
        source = if endpoint.source == FeedSource::Clob {
            "clob"
        } else {
            "binance"
        },
        code = failure_name(code).unwrap_or("unclassified"),
        detail = input_failure_detail(reason),
        fatal,
        "public feed session ended"
    );
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
            if let Some(diagnostics) = &endpoint.diagnostics {
                diagnostics.source(source).phase.store(7, Ordering::Relaxed);
            }
            return Ok(());
        }
        let mut ready_since = None;
        let result = session(
            &endpoint,
            &market,
            start,
            &tx,
            &fault,
            &mut shutdown,
            &mut ready_since,
        )
        .await;
        if matches!(result, SessionResult::Shutdown) {
            if let Some(diagnostics) = &endpoint.diagnostics {
                diagnostics.source(source).phase.store(7, Ordering::Relaxed);
            }
            return Ok(());
        }
        let fatal = matches!(result, SessionResult::Fatal(_));
        let reason = match result {
            SessionResult::Fatal(error) => error,
            SessionResult::Transport(reason) => reason.into(),
            SessionResult::Shutdown => unreachable!(),
        };
        let reset_backoff = !fatal
            && ready_since
                .is_some_and(|ready: Instant| ready.elapsed() >= endpoint.settings.stable);
        retry = reconnect_delay(retry, &endpoint.settings, reset_backoff);
        record_failure(&endpoint, start, &reason, fatal, retry, reset_backoff);
        if fatal {
            // A secondary Disconnect failure must not replace the session's
            // first fatal cause in the independent fault channel.
            latch_fault(&fault, reason.clone());
        }
        let disconnect = FeedEvent::Disconnect {
            source,
            reason: reason.clone(),
        };
        let queued =
            enqueue_reference_disconnect(&tx, disconnect, endpoint.settings.send, &mut shutdown)
                .await;
        if matches!(queued, Ok(false)) {
            if let Some(diagnostics) = &endpoint.diagnostics {
                diagnostics.source(source).phase.store(7, Ordering::Relaxed);
            }
            return Ok(());
        }
        if let Err(queue_error) = queued {
            latch_fault(&fault, queue_error.clone());
            // Even a recoverable transport loss becomes fatal if its disconnect
            // cannot reach the actor. Preserve source/code/fatal diagnostics for
            // that secondary queue failure rather than exporting a retry phase.
            if !fatal || queue_error != reason {
                record_failure(&endpoint, start, &queue_error, true, Duration::ZERO, false);
            }
            return Err(queue_error);
        }
        if fatal {
            return Err(reason);
        }
        tokio::select! {
            _ = wait_shutdown(&mut shutdown) => {
                if let Some(diagnostics) = &endpoint.diagnostics { diagnostics.source(source).phase.store(7, Ordering::Relaxed); }
                return Ok(());
            },
            _ = tokio::time::sleep(retry) => {},
        }
        retry = retry.saturating_mul(2).min(endpoint.settings.max_retry);
    }
}

fn reconnect_delay(previous: Duration, settings: &Settings, stable_stream: bool) -> Duration {
    if stable_stream {
        settings.retry
    } else {
        previous
    }
}

pub async fn run_public(
    market: Market,
    start: Instant,
    tx: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    run_public_with_diagnostics(market, start, tx, fault, shutdown, None).await
}

pub async fn run_public_with_diagnostics(
    market: Market,
    start: Instant,
    tx: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    shutdown: watch::Receiver<bool>,
    diagnostics: Option<Arc<FeedDiagnostics>>,
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
            diagnostics: diagnostics.clone(),
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
            diagnostics,
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
    use tokio::io::AsyncWriteExt;
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
            stable: Duration::from_millis(50),
        }
    }

    fn reference(exchange_ms: u64, observed_mono_ms: u64) -> FeedEvent {
        FeedEvent::Reference {
            exchange_ms,
            observed_mono_ms,
            price_micro: 80_000_000_000,
        }
    }

    #[tokio::test]
    async fn primary_relay_cancels_a_blocked_send_on_shutdown() {
        let (input, incoming) = mpsc::channel(1);
        let (primary, _held) = mpsc::channel(1);
        primary.try_send(reference(0, 0)).unwrap();
        input.try_send(reference(1, 2)).unwrap();
        let (stop, shutdown) = watch::channel(false);
        let (fault, observed) = watch::channel(None);
        let task = tokio::spawn(relay(incoming, primary, None, shutdown, fault));
        tokio::time::sleep(Duration::from_millis(10)).await;
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(observed.borrow().is_none());
    }

    #[tokio::test]
    async fn primary_relay_observes_fault_while_consumer_is_full() {
        let (input, incoming) = mpsc::channel(1);
        let (primary, _held) = mpsc::channel(1);
        primary.try_send(reference(0, 0)).unwrap();
        input.try_send(reference(1, 2)).unwrap();
        let (_stop, shutdown) = watch::channel(false);
        let (fault, observed) = watch::channel(None);
        let task = tokio::spawn(relay(incoming, primary, None, shutdown, fault.clone()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        fault.send_replace(Some("prior durability fault".into()));
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
            "prior durability fault"
        );
        assert_eq!(observed.borrow().as_deref(), Some("prior durability fault"));
    }

    #[tokio::test]
    async fn websocket_frame_larger_than_queue_delivers_all_clob_events_in_order() {
        let selected = market();
        let server_market = selected.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            let frames: Vec<_> = (0..1024)
                .map(|index| {
                    let mut value = snapshot(&server_market);
                    value["asset_id"] = if index % 2 == 0 {
                        server_market.up_token.as_str().into()
                    } else {
                        server_market.down_token.as_str().into()
                    };
                    value["timestamp"] = (1790856000123_u64 + index).to_string().into();
                    value
                })
                .collect();
            socket
                .send(Message::Text(
                    serde_json::to_string(&frames).unwrap().into(),
                ))
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if matches!(&message, Message::Close(_)) {
                    break;
                }
                if matches!(&message, Message::Text(text) if text.as_str() == "PING")
                    && socket.send(Message::Text("PONG".into())).await.is_err()
                {
                    break;
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, observed_fault) = watch::channel(None);
        let (stop, shutdown) = watch::channel(false);
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let mut settings = fast_settings();
        settings.send = Duration::from_secs(3);
        settings.receive = Duration::from_secs(5);
        let client = tokio::spawn(run_feed(
            Endpoint {
                source: FeedSource::Clob,
                url,
                settings,
                tick_refresh: TickRefresh::Skip,
                diagnostics: Some(diagnostics.clone()),
            },
            selected,
            Instant::now(),
            tx,
            fault,
            shutdown,
        ));
        // Force the parser to encounter backpressure, then drain a frame 64x the queue.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let mut receive_clock = None;
        for index in 0..1024 {
            let event = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            match event {
                FeedEvent::Snapshot {
                    outcome,
                    exchange_ms,
                    observed_mono_ms,
                    ..
                } => {
                    assert_eq!(
                        outcome,
                        if index % 2 == 0 {
                            Outcome::Up
                        } else {
                            Outcome::Down
                        }
                    );
                    assert_eq!(exchange_ms, 1790856000123 + index);
                    assert_eq!(
                        *receive_clock.get_or_insert(observed_mono_ms),
                        observed_mono_ms
                    );
                }
                other => panic!("unexpected event {other:?}"),
            }
        }
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), client)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(observed_fault.borrow().is_none());
        let sampled = diagnostics.snapshot().clob;
        assert_eq!(sampled.ingress_enqueued, 1024);
        assert_eq!(sampled.ingress_discarded_events, 0);
        assert_eq!(sampled.ingress_timeouts, 0);
        assert!(sampled.ingress_waits > 0);
    }

    #[tokio::test]
    async fn paper_relay_overflow_is_isolated_from_primary_fifo() {
        let (input, incoming) = mpsc::channel(2);
        let (primary, mut output) = mpsc::channel(2);
        let (paper, _held) = mpsc::channel(1);
        paper.try_send(reference(0, 0)).unwrap();
        let (paper_fault, paper_observed) = watch::channel(None);
        let (_stop, shutdown) = watch::channel(false);
        let (fault, observed) = watch::channel(None);
        input.try_send(reference(1, 2)).unwrap();
        input.try_send(reference(2, 3)).unwrap();
        drop(input);
        relay(
            incoming,
            primary,
            Some((paper, paper_fault)),
            shutdown,
            fault,
        )
        .await
        .unwrap();
        assert_eq!(output.recv().await.unwrap(), reference(1, 2));
        assert_eq!(output.recv().await.unwrap(), reference(2, 3));
        assert!(paper_observed.borrow().is_some());
        assert!(observed.borrow().is_none());
    }

    #[test]
    fn reference_burst_keeps_one_latest_price_without_displacing_execution_events() {
        let diagnostics = FeedDiagnostics::default();
        let mut pending = ReferenceBuffer::new(Some(diagnostics.source(FeedSource::Binance)));
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(FeedEvent::Disconnect {
            source: FeedSource::Clob,
            reason: "fixture".into(),
        })
        .unwrap();
        for time in 1..=50_000 {
            pending.observe(reference(time, time + 3)).unwrap();
            pending.flush(&tx).unwrap();
        }
        let sampled = diagnostics.snapshot().binance;
        assert_eq!(sampled.reference_received, 50_000);
        assert_eq!(sampled.reference_coalesced, 49_999);
        assert_eq!(sampled.reference_pending, 1);
        assert_eq!(sampled.reference_delivered, 0);
        assert_eq!(sampled.reference_queue_full, 50_000);
        assert!(matches!(
            rx.try_recv().unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        pending.flush(&tx).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            FeedEvent::Reference {
                exchange_ms: 50_000,
                observed_mono_ms: 50_003,
                ..
            }
        ));
        assert!(rx.try_recv().is_err());
        let sampled = diagnostics.snapshot().binance;
        assert_eq!(sampled.reference_delivered, 1);
        assert_eq!(sampled.reference_pending, 0);
        assert_eq!(sampled.reference_discarded, 0);
    }

    #[test]
    fn reference_coalescing_cannot_hide_regressed_clocks_or_invalid_event_types() {
        let mut pending = ReferenceBuffer::new(None);
        pending.observe(reference(10, 20)).unwrap();
        assert!(pending.observe(reference(9, 21)).is_err());
        assert!(pending.observe(reference(11, 19)).is_err());
        assert!(
            pending
                .observe(FeedEvent::Disconnect {
                    source: FeedSource::Binance,
                    reason: "fixture".into()
                })
                .is_err()
        );
        let (tx, mut rx) = mpsc::channel(1);
        pending.flush(&tx).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            FeedEvent::Reference {
                exchange_ms: 10,
                observed_mono_ms: 20,
                ..
            }
        ));
        // Same-exchange-time trades retain their actual receive ordering.
        pending.observe(reference(10, 21)).unwrap();
    }

    #[test]
    fn reference_socket_end_discards_pending_price_and_closed_consumer_is_fatal() {
        let diagnostics = FeedDiagnostics::default();
        {
            let mut pending = ReferenceBuffer::new(Some(diagnostics.source(FeedSource::Binance)));
            pending.observe(reference(1, 2)).unwrap();
            let (tx, rx) = mpsc::channel(1);
            drop(rx);
            assert_eq!(pending.flush(&tx).unwrap_err(), "feed consumer closed");
        }
        let sampled = diagnostics.snapshot().binance;
        assert_eq!(sampled.reference_discarded, 1);
        assert_eq!(sampled.reference_pending, 0);
        assert_eq!(sampled.reference_delivered, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn reference_disconnect_waits_for_capacity_and_preserves_order() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(reference(10, 20)).await.unwrap();
        let (_stop, mut shutdown) = watch::channel(false);
        let task = tokio::spawn(async move {
            enqueue_reference_disconnect(
                &tx,
                FeedEvent::Disconnect {
                    source: FeedSource::Binance,
                    reason: "fixture".into(),
                },
                Duration::from_secs(1),
                &mut shutdown,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert!(matches!(
            rx.recv().await.unwrap(),
            FeedEvent::Reference {
                exchange_ms: 10,
                ..
            }
        ));
        assert!(task.await.unwrap().unwrap());
        assert!(matches!(
            rx.recv().await.unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Binance,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn reference_disconnect_has_a_finite_deadline_and_shutdown_priority() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(reference(10, 20)).await.unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        let event = || FeedEvent::Disconnect {
            source: FeedSource::Binance,
            reason: "fixture".into(),
        };
        assert_eq!(
            enqueue_reference_disconnect(&tx, event(), Duration::from_secs(1), &mut shutdown)
                .await
                .unwrap_err(),
            "feed backpressure deadline exceeded"
        );
        stop.send_replace(true);
        assert!(
            !enqueue_reference_disconnect(&tx, event(), Duration::from_secs(1), &mut shutdown)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn local_binance_burst_survives_full_execution_queue_and_delivers_latest_price() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            for time in 1..=4096 {
                let trade = json!({"e":"trade","s":"BTCUSDT","T":time,"E":time,"t":time,"m":false,"q":"0.01","p":"80000"});
                socket
                    .send(Message::Text(trade.to_string().into()))
                    .await
                    .unwrap();
            }
            while let Some(Ok(message)) = socket.next().await {
                match message {
                    Message::Ping(value) => {
                        if socket.send(Message::Pong(value)).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(FeedEvent::Disconnect {
            source: FeedSource::Clob,
            reason: "held execution event".into(),
        })
        .await
        .unwrap();
        let (fault, observed_fault) = watch::channel(None);
        let (stop, shutdown) = watch::channel(false);
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let client = tokio::spawn(run_feed(
            Endpoint {
                source: FeedSource::Binance,
                url,
                settings: Settings {
                    receive: Duration::from_secs(10),
                    ..fast_settings()
                },
                tick_refresh: TickRefresh::Skip,
                diagnostics: Some(diagnostics),
            },
            market(),
            Instant::now(),
            tx,
            fault,
            shutdown,
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let sample = observed.snapshot().binance;
                if sample.reference_received == 4096 && sample.reference_queue_full > 0 {
                    break;
                }
                assert!(!client.is_finished());
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert!(observed_fault.borrow().is_none());
        assert!(matches!(
            rx.recv().await.unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        let latest = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            latest,
            FeedEvent::Reference {
                exchange_ms: 4096,
                price_micro: 80_000_000_000,
                ..
            }
        ));
        let sample = observed.snapshot().binance;
        assert_eq!(sample.reference_received, 4096);
        assert_eq!(sample.reference_coalesced, 4095);
        assert_eq!(sample.reference_delivered, 1);
        assert_eq!(sample.reference_pending, 0);
        assert_eq!(sample.transport_disconnects, 0);
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), client)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(observed_fault.borrow().is_none());
    }

    #[test]
    fn r14_interleaved_native_snapshot_does_not_rewind_books_or_drop_trade_witnesses() {
        for (latest, stale, receive, prior_receive) in [
            (
                1_791_944_333_584_u64,
                1_791_944_333_571_u64,
                13_782_271,
                13_782_271,
            ),
            (1_791_944_350_054, 1_791_944_350_048, 13_798_740, 13_798_740),
            (1_791_944_352_894, 1_791_944_352_878, 13_801_586, 13_801_579),
        ] {
            // These clocks are the retained R14 contexts. Level/trade values are
            // constructed fixtures: the original rejected frames were not saved.
            let mut selected = market();
            selected.window_start_ms = latest - 1000;
            selected.window_end_ms = latest + 60_000;
            let mut core = crate::domain::Core::new(CoreConfig {
                initial_collateral_micro: 1_000_000_000,
                max_imbalance_micro: 10_000_000,
                lot_micro: 5_000_000,
                half_width_micro: 10_000,
                max_feed_age_ms: 1000,
            })
            .unwrap();
            core.set_market(selected.clone()).unwrap();
            let mut recovery = Recovery::new([0; 2]);
            for token in [&selected.up_token, &selected.down_token] {
                let mut book = snapshot(&selected);
                book["asset_id"] = token.as_str().into();
                book["timestamp"] = (latest - 1).to_string().into();
                for event in recovery
                    .filter(parse_clob(&book.to_string(), &selected, prior_receive).unwrap())
                {
                    core.apply(event).unwrap();
                }
            }
            let delta = json!({"event_type":"price_change", "market":selected.condition_id,
                "timestamp":latest.to_string(), "price_changes":[
                    {"asset_id":selected.up_token,"price":"0.49","size":"11","side":"BUY"},
                    {"asset_id":selected.down_token,"price":"0.49","size":"11","side":"BUY"}]});
            for event in
                recovery.filter(parse_clob(&delta.to_string(), &selected, prior_receive).unwrap())
            {
                core.apply(event).unwrap();
            }
            let before = core.snapshot().up_book;
            let mut old_book = snapshot(&selected);
            old_book["timestamp"] = stale.to_string().into();
            let trade = json!({"event_type":"last_trade_price","market":selected.condition_id,
                "asset_id":selected.up_token,"timestamp":latest.to_string(),"price":"0.49",
                "size":"1","side":"SELL","fee_rate_bps":"0"});
            let parsed =
                parse_clob(&json!([old_book, trade]).to_string(), &selected, receive).unwrap();
            let mut strict = core.clone();
            assert_eq!(
                strict.apply(parsed[0].clone()).unwrap_err(),
                "CLOB exchange clock regressed"
            );
            let accepted = recovery.filter(parsed);
            assert_eq!(
                accepted.len(),
                1,
                "an obsolete native snapshot must not reach the strict core"
            );
            assert!(matches!(&accepted[0], FeedEvent::Trade { .. }));
            for event in accepted {
                core.apply(event).unwrap();
            }
            let after = core.snapshot().up_book;
            assert_eq!(after.exchange_ms, before.exchange_ms);
            assert_eq!(after.observed_mono_ms, before.observed_mono_ms);
            assert_eq!(after.best_bid_micro, before.best_bid_micro);
            assert_eq!(after.best_ask_micro, before.best_ask_micro);
            assert!(core.snapshot().last_error.is_none());
        }
    }

    #[test]
    fn diagnostics_count_discarded_snapshots_without_initializing_or_exporting_payloads() {
        let selected = market();
        let diagnostics = FeedDiagnostics::default();
        let mut recovery = Recovery::new([200, 300]);
        let mut full = snapshot(&selected);
        full["timestamp"] = "200".into();
        assert!(
            recovery
                .filter_observed(
                    parse_clob(&full.to_string(), &selected, 7).unwrap(),
                    Some(diagnostics.source(FeedSource::Clob))
                )
                .is_empty()
        );
        assert!(!recovery.ready());
        let sample = diagnostics.snapshot();
        assert_eq!(sample.clob.snapshots_seen, [1, 0]);
        assert_eq!(sample.clob.snapshots_discarded_before_cutoff, [1, 0]);
        assert_eq!(
            failure_name(failure_code(
                "private response https://example.invalid/secret",
                true
            )),
            Some("invalid_public_input")
        );
        let exported = serde_json::to_string(&sample).unwrap();
        assert!(!exported.contains(&selected.condition_id));
        assert!(!exported.contains("example.invalid"));
        assert!(!exported.contains("private response"));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_and_closed_consumers_have_distinct_codes_and_keep_first_fault() {
        let event = || FeedEvent::Disconnect {
            source: FeedSource::Binance,
            reason: "test frame".into(),
        };
        let (tx, mut rx) = mpsc::channel(1);
        let (fault, observed) = watch::channel(None);
        let (_stop, mut shutdown) = watch::channel(false);
        let reason = enqueue_frame(&tx, vec![event(), event()], &fault, &mut shutdown)
            .await
            .unwrap_err();
        assert_eq!(reason, "feed backpressure deadline exceeded");
        assert_eq!(input_failure_class(&reason), "feed_backpressure_timeout");
        assert_eq!(
            observed.borrow().as_deref(),
            Some("feed backpressure deadline exceeded")
        );
        assert_eq!(rx.try_recv().unwrap(), event());
        assert!(rx.try_recv().is_err());
        drop(rx);
        let (fault, observed) = watch::channel(None);
        let reason = enqueue_frame(&tx, vec![event()], &fault, &mut shutdown)
            .await
            .unwrap_err();
        assert_eq!(reason, "feed consumer closed");
        assert_eq!(input_failure_class(&reason), "feed_consumer_closed");
        assert_eq!(observed.borrow().as_deref(), Some("feed consumer closed"));
        assert_eq!(failure_name(7), Some("feed_queue_unavailable"));
        assert_eq!(failure_name(16), Some("feed_queue_overflow")); // historical receipts
    }

    #[tokio::test]
    async fn valid_frame_larger_than_queue_preserves_every_event_and_original_clock() {
        let (tx, mut rx) = mpsc::channel(16);
        let (_stop, mut shutdown) = watch::channel(false);
        let diagnostics = FeedDiagnostics::default();
        let source = diagnostics.source(FeedSource::Clob);
        let consumer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            for i in 0..MAX_MEMBERS as u64 {
                assert_eq!(rx.recv().await.unwrap(), reference(i, i + 7));
                assert!(rx.len() <= 16);
            }
            assert!(rx.try_recv().is_err());
        });
        let events = (0..MAX_MEMBERS as u64)
            .map(|i| reference(i, i + 7))
            .collect();
        assert!(
            enqueue_ordered(
                &tx,
                events,
                Duration::from_secs(2),
                &mut shutdown,
                Some(source)
            )
            .await
            .unwrap()
        );
        consumer.await.unwrap();
        let sample = diagnostics.snapshot().clob;
        assert_eq!(sample.ingress_events, MAX_MEMBERS as u64);
        assert_eq!(sample.ingress_enqueued, MAX_MEMBERS as u64);
        assert_eq!(sample.ingress_discarded_events, 0);
        assert_eq!(sample.ingress_pending_events, 0);
        assert_eq!(sample.ingress_timeouts, 0);
        assert!(sample.ingress_waits > 0);
        assert_eq!(sample.ingress_queue_high_watermark, 16);
    }

    #[tokio::test(start_paused = true)]
    async fn partial_admission_deadline_is_bounded_and_discarded_suffix_is_explicit() {
        let (tx, mut rx) = mpsc::channel(1);
        let (_stop, mut shutdown) = watch::channel(false);
        let diagnostics = FeedDiagnostics::default();
        let result = enqueue_ordered(
            &tx,
            vec![reference(1, 2), reference(2, 3)],
            Duration::from_secs(1),
            &mut shutdown,
            Some(diagnostics.source(FeedSource::Clob)),
        )
        .await;
        assert_eq!(result.unwrap_err(), "feed backpressure deadline exceeded");
        assert_eq!(rx.try_recv().unwrap(), reference(1, 2));
        assert!(rx.try_recv().is_err());
        let sample = diagnostics.snapshot().clob;
        assert_eq!(sample.ingress_events, 2);
        assert_eq!(sample.ingress_enqueued, 1);
        assert_eq!(sample.ingress_discarded_events, 1);
        assert_eq!(sample.ingress_pending_events, 0);
        assert_eq!(sample.ingress_timeouts, 1);
    }

    #[tokio::test]
    async fn cancelling_backpressured_frame_discards_only_its_pending_suffix() {
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(reference(0, 0)).unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        let diagnostics = FeedDiagnostics::default();
        let admission = enqueue_ordered(
            &tx,
            vec![reference(1, 2), reference(2, 3)],
            Duration::from_secs(5),
            &mut shutdown,
            Some(diagnostics.source(FeedSource::Clob)),
        );
        tokio::pin!(admission);
        tokio::select! {
            result = &mut admission => panic!("unexpected admission: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }
        assert_eq!(diagnostics.snapshot().clob.ingress_pending_events, 2);
        stop.send_replace(true);
        assert!(
            !tokio::time::timeout(Duration::from_millis(100), admission)
                .await
                .unwrap()
                .unwrap()
        );
        assert_eq!(diagnostics.snapshot().clob.ingress_discarded_events, 2);
        assert_eq!(diagnostics.snapshot().clob.ingress_pending_events, 0);
    }

    async fn disconnect_queue_failure(
        closed: bool,
        prior_fault: Option<&str>,
        invalid_input: bool,
        unclean_close: bool,
    ) {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            if invalid_input {
                socket
                    .send(Message::Text("{invalid fixture JSON".into()))
                    .await
                    .unwrap();
            } else if unclean_close {
                socket.get_mut().shutdown().await.unwrap();
                tokio::time::sleep(Duration::from_millis(30)).await;
                drop(socket);
            } else {
                socket.close(None).await.unwrap();
            }
        });
        let (tx, rx) = mpsc::channel(1);
        if !closed {
            tx.try_send(FeedEvent::Reference {
                exchange_ms: 1,
                observed_mono_ms: 1,
                price_micro: 1,
            })
            .unwrap();
        }
        let retained_receiver = if closed {
            drop(rx);
            None
        } else {
            Some(rx)
        };
        let (fault, state) = watch::channel(prior_fault.map(str::to_owned));
        let (_shutdown, control) = watch::channel(false);
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let reason = tokio::time::timeout(
            Duration::from_secs(2),
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings: fast_settings(),
                    tick_refresh: TickRefresh::Skip,
                    diagnostics: Some(diagnostics.clone()),
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        let expected = if closed {
            "feed consumer closed"
        } else {
            "feed backpressure deadline exceeded"
        };
        assert_eq!(reason, expected);
        let first = if invalid_input {
            "invalid CLOB JSON"
        } else {
            expected
        };
        assert_eq!(
            state.borrow().as_deref(),
            Some(prior_fault.unwrap_or(first))
        );
        let sample = diagnostics.snapshot();
        assert_eq!(
            sample.clob.last_failure,
            Some(input_failure_class(expected))
        );
        assert!(sample.clob.last_failure_fatal);
        assert_eq!(sample.clob.phase, "faulted");
        assert_eq!(sample.clob.retry_delay_ms, 0);
        assert_eq!(
            sample.clob.transport_disconnects,
            if invalid_input { 0 } else { 1 }
        );
        assert_eq!(sample.binance.last_failure, None);
        if let Some(mut rx) = retained_receiver {
            assert!(matches!(
                rx.try_recv().unwrap(),
                FeedEvent::Reference { .. }
            ));
            assert!(rx.try_recv().is_err());
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_queue_full_is_a_fatal_source_failure_without_replacing_prior_fault() {
        disconnect_queue_failure(false, None, false, false).await;
        disconnect_queue_failure(false, Some("prior durability fault"), false, false).await;
    }

    #[tokio::test]
    async fn disconnect_queue_closed_is_a_fatal_source_failure_without_replacing_prior_fault() {
        disconnect_queue_failure(true, None, false, false).await;
        disconnect_queue_failure(true, Some("prior durability fault"), false, false).await;
    }

    #[tokio::test]
    async fn disconnect_queue_failure_preserves_the_sessions_first_fatal_input_cause() {
        disconnect_queue_failure(false, None, true, false).await;
        disconnect_queue_failure(true, None, true, false).await;
    }

    #[tokio::test]
    async fn unclean_disconnect_admission_failure_remains_fatal_and_preserves_first_fault() {
        for closed in [false, true] {
            disconnect_queue_failure(closed, None, false, true).await;
            disconnect_queue_failure(closed, Some("prior durability fault"), false, true).await;
        }
    }

    #[tokio::test]
    async fn stable_native_stream_resets_backoff_after_consecutive_failed_connections() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server_market = selected.clone();
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let server_diagnostics = diagnostics.clone();
        let server = tokio::spawn(async move {
            for connection in 0..4 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let _ = socket.next().await.unwrap().unwrap();
                if connection < 2 {
                    let _ = socket.close(None).await;
                    continue;
                }
                for token in [&server_market.up_token, &server_market.down_token] {
                    let mut full = snapshot(&server_market);
                    full["asset_id"] = token.as_str().into();
                    full["timestamp"] = "201".into();
                    socket
                        .send(Message::Text(full.to_string().into()))
                        .await
                        .unwrap();
                }
                if connection == 2 {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while server_diagnostics.snapshot().clob.phase != "streaming" {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = socket.close(None).await;
                    continue;
                }
                while let Some(Ok(message)) = socket.next().await {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    if matches!(&message, Message::Text(text) if text.as_str() == "PING")
                        && socket.send(Message::Text("PONG".into())).await.is_err()
                    {
                        break;
                    }
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let client = tokio::spawn(run_feed(
            Endpoint {
                source: FeedSource::Clob,
                url,
                settings: fast_settings(),
                tick_refresh: TickRefresh::Fixture([100; 2]),
                diagnostics: Some(diagnostics),
            },
            selected,
            Instant::now(),
            tx,
            fault,
            control,
        ));
        let mut snapshots = 0;
        let mut disconnects = 0;
        for _ in 0..7 {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            match event {
                FeedEvent::Snapshot { .. } => snapshots += 1,
                FeedEvent::Disconnect { .. } => disconnects += 1,
                _ => panic!("unexpected fixture event"),
            }
        }
        assert_eq!(snapshots, 4);
        assert_eq!(disconnects, 3);
        let sample = observed.snapshot();
        assert_eq!(sample.clob.attempts, 4);
        assert_eq!(sample.clob.backoff_resets, 1);
        assert_eq!(sample.clob.retry_delay_ms, 10);
        assert_eq!(sample.clob.bootstrap_timeouts, 0);
        assert!(state.borrow().is_none());
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn discarded_initial_snapshot_times_out_reconnects_and_requires_both_native_books() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server_market = selected.clone();
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let _ = socket.next().await.unwrap().unwrap();
                for token in [&server_market.up_token, &server_market.down_token] {
                    let mut full = snapshot(&server_market);
                    full["asset_id"] = token.as_str().into();
                    full["timestamp"] = if connection == 0 { "100" } else { "201" }.into();
                    socket
                        .send(Message::Text(full.to_string().into()))
                        .await
                        .unwrap();
                }
                while let Some(Ok(message)) = socket.next().await {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    if matches!(&message, Message::Text(text) if text.as_str() == "PING") {
                        let delta = json!({"event_type":"price_change","market":server_market.condition_id,"timestamp":"150","price_changes":[{"asset_id":server_market.up_token,"price":"0.4","size":"1","side":"BUY"}]});
                        let response = if connection == 0 {
                            delta.to_string()
                        } else {
                            "PONG".into()
                        };
                        if socket.send(Message::Text(response.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let mut settings = fast_settings();
        settings.bootstrap = Duration::from_millis(100);
        let client = tokio::spawn(run_feed(
            Endpoint {
                source: FeedSource::Clob,
                url,
                settings,
                tick_refresh: TickRefresh::Fixture([100; 2]),
                diagnostics: Some(diagnostics),
            },
            selected,
            Instant::now(),
            tx,
            fault,
            control,
        ));
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(first, FeedEvent::Disconnect { reason, .. } if reason == "fresh both-outcome snapshot bootstrap timeout")
        );
        let up = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let down = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            up,
            FeedEvent::Snapshot {
                outcome: Outcome::Up,
                exchange_ms: 201,
                ..
            }
        ));
        assert!(matches!(
            down,
            FeedEvent::Snapshot {
                outcome: Outcome::Down,
                exchange_ms: 201,
                ..
            }
        ));
        let sample = observed.snapshot();
        assert_eq!(sample.clob.attempts, 2);
        assert_eq!(sample.clob.bootstrap_timeouts, 1);
        assert_eq!(sample.clob.transport_disconnects, 1);
        assert_eq!(sample.clob.snapshots_seen, [2, 2]);
        assert_eq!(sample.clob.snapshots_discarded_before_cutoff, [1, 1]);
        assert_eq!(sample.clob.phase, "streaming");
        assert_eq!(
            sample.clob.last_failure,
            Some("native_snapshot_bootstrap_timeout")
        );
        assert!(!sample.clob.last_failure_fatal);
        assert!(state.borrow().is_none());
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    fn rest_book(market: &Market, tick: &str, timestamp: &str) -> Value {
        json!({"market":market.condition_id,"asset_id":market.up_token,"timestamp":timestamp,
            "tick_size":tick,"bids":[{"price":"0.40","size":"100"}],
            "asks":[{"price":"0.60","size":"100"}],"neg_risk":false,"min_order_size":"5","hash":"native-book-hash"})
    }

    #[test]
    fn initialized_recovery_keeps_incremental_and_tick_regressions_strict() {
        for regressed in ["delta", "tick"] {
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
        // Old full depth is redundant; tick transitions still reach the strict Core.
        let filtered = recovery.filter(recovered);
        assert_eq!(filtered.len(), 1);
        assert!(matches!(filtered[0], FeedEvent::Tick { .. }));
    }

    #[test]
    fn stale_snapshots_are_scoped_to_one_outcome_and_do_not_extend_freshness() {
        let mut selected = market();
        let base = selected.window_start_ms;
        selected.window_end_ms = base + 60_000;
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
        let diagnostics = FeedDiagnostics::default();
        let source = diagnostics.source(FeedSource::Clob);
        let mut recovery = Recovery::new([base; 2]);
        let mut full = snapshot(&selected);
        full["timestamp"] = (base + 100).to_string().into();
        for token in [&selected.up_token, &selected.down_token] {
            full["asset_id"] = token.as_str().into();
            for event in recovery.filter_observed(
                parse_clob(&full.to_string(), &selected, 100).unwrap(),
                Some(source),
            ) {
                core.apply(event).unwrap();
            }
        }
        let delta = json!({"event_type":"price_change", "market":selected.condition_id,
            "timestamp":(base + 200).to_string(), "price_changes":[
                {"asset_id":selected.up_token,"price":"0.49","size":"11","side":"BUY"}]});
        for event in recovery.filter_observed(
            parse_clob(&delta.to_string(), &selected, 200).unwrap(),
            Some(source),
        ) {
            core.apply(event).unwrap();
        }
        // Down's clock is independent of Up's newer delta.
        full["asset_id"] = selected.down_token.as_str().into();
        full["timestamp"] = (base + 150).to_string().into();
        let down = recovery.filter_observed(
            parse_clob(&full.to_string(), &selected, 250).unwrap(),
            Some(source),
        );
        assert_eq!(down.len(), 1);
        for event in down {
            core.apply(event).unwrap();
        }
        full["asset_id"] = selected.up_token.as_str().into();
        assert!(
            recovery
                .filter_observed(
                    parse_clob(&full.to_string(), &selected, 1201).unwrap(),
                    Some(source),
                )
                .is_empty()
        );
        let decision = core.evaluate(base + 1201, 1201);
        assert!(decision.intents.is_empty());
        assert!(
            decision
                .stop_reasons
                .contains(&StopReason::StaleBook(Outcome::Up))
        );
        assert_eq!(core.snapshot().up_book.observed_mono_ms, Some(200));
        assert_eq!(core.snapshot().down_book.exchange_ms, Some(base + 150));
        let observed = diagnostics.snapshot();
        assert_eq!(observed.clob.stale_snapshots_discarded, [1, 0]);
        assert_eq!(
            observed.clob.book_exchange_watermark_ms,
            [base + 200, base + 150]
        );
    }

    #[test]
    fn obsolete_snapshot_cannot_hide_invalid_depth_identity_or_receive_clock() {
        let selected = market();
        let mut full = snapshot(&selected);
        full["timestamp"] = "200".into();
        for invalid in [
            "crossed",
            "duplicate",
            "price",
            "numeric",
            "identity",
            "receive",
        ] {
            let mut recovery = Recovery::new([0; 2]);
            let mut core = Core::new(CoreConfig {
                initial_collateral_micro: 1_000_000_000,
                max_imbalance_micro: 10_000_000,
                lot_micro: 5_000_000,
                half_width_micro: 10_000,
                max_feed_age_ms: 1000,
            })
            .unwrap();
            core.set_market(selected.clone()).unwrap();
            for event in recovery.filter(parse_clob(&full.to_string(), &selected, 100).unwrap()) {
                core.apply(event).unwrap();
            }
            let mut old = full.clone();
            old["timestamp"] = "199".into();
            match invalid {
                "crossed" => old["bids"][0]["price"] = "0.7".into(),
                "duplicate" => {
                    let level = old["bids"][0].clone();
                    old["bids"].as_array_mut().unwrap().push(level);
                }
                "price" => old["asks"][0]["price"] = "1".into(),
                "numeric" => old["asks"][0]["price"] = 1.into(),
                "identity" => old["asset_id"] = "999".into(),
                _ => {}
            }
            let parsed = parse_clob(
                &old.to_string(),
                &selected,
                if invalid == "receive" { 99 } else { 101 },
            );
            if matches!(invalid, "duplicate" | "numeric" | "identity") {
                assert!(
                    parsed.is_err(),
                    "invalid {invalid} must fail atomic parsing"
                );
            } else {
                let events = recovery.filter(parsed.unwrap());
                assert_eq!(
                    events.len(),
                    1,
                    "invalid {invalid} must reach the strict Core"
                );
                assert!(core.apply(events.into_iter().next().unwrap()).is_err());
            }
        }
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
                    diagnostics: None,
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
                    diagnostics: None,
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
    async fn unclean_websocket_close_reconnects_and_requires_both_fresh_books() {
        let selected = market();
        let base = selected.window_start_ms;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let server_market = selected.clone();
        let server_diagnostics = diagnostics.clone();
        let (allow_down, wait_down) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut wait_down = Some(wait_down);
            for connection in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let subscription: Value = serde_json::from_str(&subscription).unwrap();
                assert_eq!(
                    subscription["assets_ids"],
                    json!([server_market.up_token, server_market.down_token])
                );
                for (index, token) in [&server_market.up_token, &server_market.down_token]
                    .into_iter()
                    .enumerate()
                {
                    if connection == 1 && index == 1 {
                        wait_down.take().unwrap().await.unwrap();
                    }
                    let mut full = snapshot(&server_market);
                    full["asset_id"] = token.as_str().into();
                    full["timestamp"] = (base + 201 + connection * 100).to_string().into();
                    socket
                        .send(Message::Text(full.to_string().into()))
                        .await
                        .unwrap();
                }
                if connection == 0 {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while server_diagnostics.snapshot().clob.phase != "streaming" {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    // A TCP EOF without a WebSocket Close is a routine lost
                    // transport, not permission to reuse either cached book.
                    // Keep the read half alive briefly: bare drop with unread
                    // heartbeat bytes can produce TCP RST instead of EOF.
                    socket.get_mut().shutdown().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    drop(socket);
                } else {
                    while let Some(Ok(message)) = socket.next().await {
                        if matches!(message, Message::Close(_)) {
                            break;
                        }
                        if matches!(&message, Message::Text(text) if text.as_str() == "PING")
                            && socket.send(Message::Text("PONG".into())).await.is_err()
                        {
                            break;
                        }
                    }
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(16);
        let (fault, fault_state) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let start = Instant::now();
        let client = tokio::spawn(run_feed(
            Endpoint {
                source: FeedSource::Clob,
                url,
                settings: fast_settings(),
                tick_refresh: TickRefresh::Fixture([base + 100; 2]),
                diagnostics: Some(diagnostics),
            },
            selected.clone(),
            start,
            tx,
            fault,
            control,
        ));
        let mut core = Core::new(CoreConfig {
            initial_collateral_micro: 1_000_000_000,
            max_imbalance_micro: 10_000_000,
            lot_micro: 5_000_000,
            half_width_micro: 10_000,
            max_feed_age_ms: 1000,
        })
        .unwrap();
        core.set_market(selected).unwrap();
        core.control(Action::Resume).unwrap();
        for _ in 0..2 {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(&event, FeedEvent::Snapshot { .. }));
            core.apply(event).unwrap();
        }
        assert_eq!(
            core.evaluate(base + 400, start.elapsed().as_millis() as u64)
                .intents
                .len(),
            2
        );
        let reset = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let reason = match &reset {
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                reason,
            } => reason,
            _ => panic!("disconnect event required"),
        };
        assert_eq!(reason, "public WebSocket closed without closing handshake");
        core.apply(reset).unwrap();
        let decision = core.evaluate(base + 400, start.elapsed().as_millis() as u64);
        assert!(decision.intents.is_empty());
        assert!(
            decision
                .stop_reasons
                .contains(&StopReason::ClobDisconnected)
        );
        assert!(
            decision
                .stop_reasons
                .contains(&StopReason::MissingBook(Outcome::Up))
        );
        assert!(
            decision
                .stop_reasons
                .contains(&StopReason::MissingBook(Outcome::Down))
        );
        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &first,
            FeedEvent::Snapshot {
                outcome: Outcome::Up,
                ..
            }
        ));
        core.apply(first).unwrap();
        let decision = core.evaluate(base + 400, start.elapsed().as_millis() as u64);
        assert!(decision.intents.is_empty());
        assert!(
            decision
                .stop_reasons
                .contains(&StopReason::MissingBook(Outcome::Down))
        );
        assert_eq!(observed.snapshot().clob.phase, "awaiting_native_snapshot");
        assert_eq!(
            observed.snapshot().clob.last_failure,
            Some("websocket_unclean_disconnect")
        );
        assert!(!observed.snapshot().clob.last_failure_fatal);
        assert!(fault_state.borrow().is_none());
        allow_down.send(()).unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &second,
            FeedEvent::Snapshot {
                outcome: Outcome::Down,
                ..
            }
        ));
        core.apply(second).unwrap();
        assert_eq!(
            core.evaluate(base + 400, start.elapsed().as_millis() as u64)
                .intents
                .len(),
            2
        );
        assert_eq!(observed.snapshot().clob.phase, "streaming");
        assert_eq!(observed.snapshot().clob.attempts, 2);
        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), client)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn invalid_websocket_opcode_remains_fatal_and_exports_a_static_subcode() {
        let selected = market();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            // FIN + reserved opcode 3, with a zero-length unmasked payload.
            socket.get_mut().write_all(&[0x83, 0]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let (tx, mut rx) = mpsc::channel(4);
        let (fault, fault_state) = watch::channel(None);
        let (_shutdown, control) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url: url.clone(),
                    settings: fast_settings(),
                    tick_refresh: TickRefresh::Skip,
                    diagnostics: Some(diagnostics),
                },
                selected,
                Instant::now(),
                tx,
                fault,
                control,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            Some("invalid public WebSocket protocol: invalid_opcode")
        );
        assert_eq!(
            fault_state.borrow().as_deref(),
            Some("invalid public WebSocket protocol: invalid_opcode")
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            FeedEvent::Disconnect {
                source: FeedSource::Clob,
                ..
            }
        ));
        let sample = observed.snapshot();
        assert_eq!(
            sample.clob.last_failure,
            Some("websocket_protocol_invalid_opcode")
        );
        assert!(sample.clob.last_failure_fatal);
        assert_eq!(sample.clob.phase, "faulted");
        assert_eq!(sample.clob.attempts, 1);
        let exported = serde_json::to_string(&sample).unwrap();
        assert!(!exported.contains("ws://"));
        assert!(!exported.contains(&url));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_websocket_stall_faults_after_admitted_prefix_without_silent_loss() {
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
        let diagnostics = Arc::new(FeedDiagnostics::default());
        let observed = diagnostics.clone();
        let client = tokio::spawn(async move {
            run_feed(
                Endpoint {
                    source: FeedSource::Clob,
                    url,
                    settings: fast_settings(),
                    tick_refresh: TickRefresh::Skip,
                    diagnostics: Some(diagnostics),
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
        assert_eq!(
            state.borrow().as_deref(),
            Some("feed backpressure deadline exceeded")
        );
        let sample = observed.snapshot();
        assert_eq!(sample.clob.last_failure, Some("feed_backpressure_timeout"));
        assert!(sample.clob.last_failure_fatal);
        assert_eq!(sample.clob.phase, "faulted");
        assert_eq!(sample.clob.attempts, 1);
        assert_eq!(sample.clob.retry_delay_ms, 0);
        assert_eq!(sample.clob.backoff_resets, 0);
        assert!(sample.clob.ingress_timeouts >= 1);
        assert!(sample.clob.ingress_discarded_events >= 1);
        assert_eq!(sample.clob.ingress_pending_events, 0);
        assert!(matches!(
            rx.try_recv().unwrap(),
            FeedEvent::Snapshot {
                outcome: Outcome::Up,
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
                    diagnostics: None,
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
