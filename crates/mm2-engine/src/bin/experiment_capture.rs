//! Bounded public input capture. No authenticated, wallet, order or relayer endpoints.
use futures_util::{SinkExt, StreamExt};
use mm2_engine::experiments as tape;
use mm2_engine::{discovery, domain::Market, feeds, paper_resolution};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message,
        error::{CapacityError, ProtocolError},
        protocol::{CloseFrame, WebSocketConfig},
    },
};

const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const STABLE_SESSION: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CaptureProfile {
    Default,
    NoInitialDumpV1,
    TransportDiagnosticV1,
}
impl CaptureProfile {
    fn parse(arguments: &[String]) -> Result<Self, &'static str> {
        match arguments.len() {
            5 => Ok(Self::Default),
            6 if arguments[5] == "--capture-profile=no-initial-dump-v1" => {
                Ok(Self::NoInitialDumpV1)
            }
            6 if arguments[5] == "--capture-profile=transport-diagnostic-v1" => {
                Ok(Self::TransportDiagnosticV1)
            }
            _ => Err(
                "usage: experiment_capture NEW_TAPE.jsonl SECONDS MAX_BYTES MAX_FRAMES [--capture-profile=no-initial-dump-v1|--capture-profile=transport-diagnostic-v1]",
            ),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::NoInitialDumpV1 => "no-initial-dump-v1",
            Self::TransportDiagnosticV1 => "transport-diagnostic-v1",
        }
    }
    fn source(self) -> &'static str {
        match self {
            Self::Default => "native_public_ws_capture",
            Self::NoInitialDumpV1 => "native_public_ws_capture_no_initial_dump_v1",
            Self::TransportDiagnosticV1 => "native_public_ws_capture_transport_diagnostic_v1",
        }
    }
    fn prewarm_ms(self) -> u64 {
        match self {
            Self::Default => 20_000,
            Self::NoInitialDumpV1 | Self::TransportDiagnosticV1 => 60_000,
        }
    }
    fn subscription(self, market: &Market) -> Value {
        let mut request = json!({"assets_ids":[market.up_token,market.down_token],"type":"market","custom_feature_enabled":true});
        if self != Self::Default {
            // The documented initial dump is independent of subsequent full
            // books. Preserve every received frame and let strict native
            // recovery decide whether real books ever initialize the market.
            request["initial_dump"] = json!(false);
        }
        request
    }
    fn wanted_starts(self, now: u64) -> Vec<u64> {
        let current = now / 300_000 * 300_000;
        let mut wanted = vec![current];
        if 300_000 - now % 300_000 <= self.prewarm_ms()
            && let Some(next) = current.checked_add(300_000)
        {
            wanted.push(next);
        }
        wanted
    }
    fn proof(self) -> Value {
        json!({"capture_profile":self.name(),"source":self.source(),"clob_initial_dump":self == Self::Default,"prewarm_ms":self.prewarm_ms()})
    }
}

#[derive(Default)]
struct RetryBackoff {
    retry_later_streak: u8,
}
impl RetryBackoff {
    fn next_delay(&mut self, close_code: Option<u16>, stable: bool, jitter_ms: u64) -> Duration {
        if stable {
            self.retry_later_streak = 0;
        }
        if close_code == Some(1013) {
            let seconds = [5, 10, 20, 30][usize::from(self.retry_later_streak.min(3))];
            self.retry_later_streak = self.retry_later_streak.saturating_add(1).min(4);
            return Duration::from_millis((seconds * 1000 + jitter_ms.min(1000)).min(30_000));
        }
        // A short transport failure does not erase the recent 1013 history.
        Duration::from_millis(500)
    }
}

// Fixed categories only. Never format a nested transport error, URL, header,
// peer close reason, or payload into secondary diagnostics. The explicit
// diagnostic profile separately retains bounded close-reason bytes as hex.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ErrorClass {
    name: &'static str,
    size: Option<usize>,
    limit: Option<usize>,
}
impl ErrorClass {
    fn named(name: &'static str) -> Self {
        Self {
            name,
            size: None,
            limit: None,
        }
    }
}
fn error_class(error: &WebSocketError) -> ErrorClass {
    match error {
        WebSocketError::Capacity(CapacityError::MessageTooLong { size, max_size }) => ErrorClass {
            name: "capacity_message_too_long",
            size: Some(*size),
            limit: Some(*max_size),
        },
        WebSocketError::Capacity(_) => ErrorClass::named("capacity_other"),
        WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
            ErrorClass::named("protocol_reset_without_close")
        }
        WebSocketError::Protocol(_) => ErrorClass::named("protocol_other"),
        WebSocketError::Tls(_) => ErrorClass::named("tls"),
        WebSocketError::Io(error) => ErrorClass::named(match error.kind() {
            std::io::ErrorKind::ConnectionReset => "io_connection_reset",
            std::io::ErrorKind::ConnectionAborted => "io_connection_aborted",
            std::io::ErrorKind::BrokenPipe => "io_broken_pipe",
            std::io::ErrorKind::TimedOut => "io_timed_out",
            std::io::ErrorKind::WouldBlock => "io_would_block",
            std::io::ErrorKind::UnexpectedEof => "io_unexpected_eof",
            std::io::ErrorKind::ConnectionRefused => "io_connection_refused",
            std::io::ErrorKind::NotConnected => "io_not_connected",
            _ => "io_other",
        }),
        WebSocketError::ConnectionClosed => ErrorClass::named("connection_closed"),
        WebSocketError::AlreadyClosed => ErrorClass::named("already_closed"),
        WebSocketError::Http(_) => ErrorClass::named("http_handshake"),
        WebSocketError::HttpFormat(_) => ErrorClass::named("http_format"),
        WebSocketError::Url(_) => ErrorClass::named("url"),
        _ => ErrorClass::named("other_websocket_error"),
    }
}
async fn write_bounded(
    write: impl Future<Output = Result<(), WebSocketError>>,
) -> Result<(), ErrorClass> {
    match tokio::time::timeout(SOCKET_WRITE_TIMEOUT, write).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error_class(&error)),
        Err(_) => Err(ErrorClass::named("write_timeout")),
    }
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

// RFC6455 limits a Close control payload to125bytes, including its2byte code.
// Retain bytes, not a rendered peer string: it may contain terminal controls,
// separators or misleading diagnostic text. Never interpolate it directly.
const MAX_CLOSE_REASON_BYTES: usize = 123;
struct PeerCloseEvidence {
    reason_hex: String,
    retained_reason_sha256: String,
    retained_reason_bytes: usize,
    original_reason_bytes: usize,
    observed_wall_ms: u64,
    flush_result: &'static str,
    flush_started_session_ms: u64,
    flush_finished_session_ms: u64,
    flush_us: u64,
}
impl PeerCloseEvidence {
    fn new(frame: Option<&CloseFrame>, observed_wall_ms: u64) -> Self {
        let reason = frame.map_or("", |frame| frame.reason.as_str());
        let mut end = reason.len().min(MAX_CLOSE_REASON_BYTES);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        let retained = &reason.as_bytes()[..end];
        Self {
            reason_hex: hex::encode(retained),
            retained_reason_sha256: format!("{:x}", Sha256::digest(retained)),
            retained_reason_bytes: end,
            original_reason_bytes: reason.len(),
            observed_wall_ms,
            flush_result: "not_attempted",
            flush_started_session_ms: 0,
            flush_finished_session_ms: 0,
            flush_us: 0,
        }
    }
}

struct TransportTrace {
    started: tokio::time::Instant,
    message_payload_bytes: u64,
    text_frames: u64,
    bucket_second: u64,
    bucket_text_frames: u64,
    peak_text_frames_in_1s_bucket: u64,
    completed_read_wait_us_max: u64,
    between_completed_read_attempts_us_max: u64,
    last_read_completed: Option<tokio::time::Instant>,
    record_lock_wait_us_max: u64,
    record_append_us_max: u64,
    record_busy_us_total: u64,
    application_ping_attempts: u64,
    application_pings_flushed: u64,
    application_ping_flush_us_max: u64,
    application_pongs_received: u64,
    last_application_ping: Option<tokio::time::Instant>,
    last_application_pong: Option<tokio::time::Instant>,
    last_pong_since_last_ping_ms: Option<u64>,
    native_pings_received: u64,
    native_pongs_flushed: u64,
    native_pong_flush_us_max: u64,
    close: Option<PeerCloseEvidence>,
    retained_gap_reason_sha256: Option<String>,
}
impl TransportTrace {
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            message_payload_bytes: 0,
            text_frames: 0,
            bucket_second: 0,
            bucket_text_frames: 0,
            peak_text_frames_in_1s_bucket: 0,
            completed_read_wait_us_max: 0,
            between_completed_read_attempts_us_max: 0,
            last_read_completed: None,
            record_lock_wait_us_max: 0,
            record_append_us_max: 0,
            record_busy_us_total: 0,
            application_ping_attempts: 0,
            application_pings_flushed: 0,
            application_ping_flush_us_max: 0,
            application_pongs_received: 0,
            last_application_ping: None,
            last_application_pong: None,
            last_pong_since_last_ping_ms: None,
            native_pings_received: 0,
            native_pongs_flushed: 0,
            native_pong_flush_us_max: 0,
            close: None,
            retained_gap_reason_sha256: None,
        }
    }
    fn observe_read(
        &mut self,
        message: Option<&Message>,
        started: tokio::time::Instant,
        completed: tokio::time::Instant,
    ) {
        self.completed_read_wait_us_max = self
            .completed_read_wait_us_max
            .max(micros(completed.saturating_duration_since(started)));
        if let Some(previous) = self.last_read_completed {
            self.between_completed_read_attempts_us_max = self
                .between_completed_read_attempts_us_max
                .max(micros(started.saturating_duration_since(previous)));
        }
        self.last_read_completed = Some(completed);
        let Some(message) = message else { return };
        self.message_payload_bytes = self
            .message_payload_bytes
            .saturating_add(message.len().try_into().unwrap_or(u64::MAX));
        if let Message::Text(text) = message {
            self.text_frames = self.text_frames.saturating_add(1);
            // These are aligned, nonoverlapping1second buckets, not an exact
            // rolling window. No per-frame history or financial parsing.
            let second = completed.saturating_duration_since(self.started).as_secs();
            if second != self.bucket_second {
                self.bucket_second = second;
                self.bucket_text_frames = 0;
            }
            self.bucket_text_frames = self.bucket_text_frames.saturating_add(1);
            self.peak_text_frames_in_1s_bucket = self
                .peak_text_frames_in_1s_bucket
                .max(self.bucket_text_frames);
            if text.as_str() == "PONG" {
                self.application_pongs_received = self.application_pongs_received.saturating_add(1);
                self.last_application_pong = Some(completed);
                // PONG contains no correlation ID. This elapsed observation
                // is not proof of remote RTT or acknowledgement identity.
                self.last_pong_since_last_ping_ms = self
                    .last_application_ping
                    .map(|sent| millis(completed.saturating_duration_since(sent)));
            }
        } else if matches!(message, Message::Ping(_)) {
            self.native_pings_received = self.native_pings_received.saturating_add(1);
        }
    }
    fn summary(&self) -> Value {
        let now = tokio::time::Instant::now();
        let mut value = json!({
            "transport_diag_v":1,
            "message_payload_bytes":self.message_payload_bytes,
            "diagnostic_text_frames":self.text_frames,
            "peak_text_frames_in_1s_bucket":self.peak_text_frames_in_1s_bucket,
            "completed_read_wait_us_max":self.completed_read_wait_us_max,
            "between_completed_read_attempts_us_max":self.between_completed_read_attempts_us_max,
            "record_lock_wait_us_max":self.record_lock_wait_us_max,
            "record_append_us_max":self.record_append_us_max,
            "record_busy_us_total":self.record_busy_us_total,
            "application_ping_attempts":self.application_ping_attempts,
            "application_pings_flushed":self.application_pings_flushed,
            "application_ping_flush_us_max":self.application_ping_flush_us_max,
            "application_pongs_received":self.application_pongs_received,
            "last_application_ping_age_ms":self.last_application_ping.map(|at|millis(now.saturating_duration_since(at))),
            "last_application_pong_age_ms":self.last_application_pong.map(|at|millis(now.saturating_duration_since(at))),
            "last_pong_since_last_ping_ms":self.last_pong_since_last_ping_ms,
            "native_pings_received":self.native_pings_received,
            "native_pongs_flushed":self.native_pongs_flushed,
            "native_pong_flush_us_max":self.native_pong_flush_us_max,
        });
        if let Some(close) = &self.close {
            value["peer_reason_hex"] = json!(close.reason_hex);
            value["peer_reason_retained_sha256"] = json!(close.retained_reason_sha256);
            value["peer_reason_retained_bytes"] = json!(close.retained_reason_bytes);
            value["peer_reason_original_bytes"] = json!(close.original_reason_bytes);
            value["peer_reason_truncated"] =
                json!(close.retained_reason_bytes != close.original_reason_bytes);
            value["peer_close_observed_wall_ms"] = json!(close.observed_wall_ms);
            value["peer_close_flush_result"] = json!(close.flush_result);
            value["peer_close_flush_started_session_ms"] = json!(close.flush_started_session_ms);
            value["peer_close_flush_finished_session_ms"] = json!(close.flush_finished_session_ms);
            value["peer_close_flush_us"] = json!(close.flush_us);
        }
        if let Some(hash) = &self.retained_gap_reason_sha256 {
            value["retained_gap_reason_sha256"] = json!(hash);
        }
        value
    }
    fn reason_suffix(&self) -> String {
        self.summary()
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                let value = if value.is_null() {
                    "none".into()
                } else {
                    value
                        .as_str()
                        .map_or_else(|| value.to_string(), str::to_owned)
                };
                format!("{key}={value}")
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

async fn flush_peer_close<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    frame: Option<&CloseFrame>,
    trace: &mut TransportTrace,
    deadline: tokio::time::Instant,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let started = tokio::time::Instant::now();
    let session_started = trace.started;
    let evidence = trace
        .close
        .get_or_insert_with(|| PeerCloseEvidence::new(frame, wall()));
    evidence.flush_started_session_ms = millis(started.saturating_duration_since(session_started));
    // read() already queued the exact close ACK. Flush it once; do not send a
    // second close, reconnect, extend the original bound or clear the Gap.
    evidence.flush_result = if started >= deadline {
        "deadline_before_flush"
    } else {
        match tokio::time::timeout_at(deadline.min(started + SOCKET_WRITE_TIMEOUT), ws.flush())
            .await
        {
            Ok(Ok(())) => "ok",
            Ok(Err(error)) => error_class(&error).name,
            Err(_) if tokio::time::Instant::now() >= deadline => "deadline_during_flush",
            Err(_) => "write_timeout",
        }
    };
    let finished = tokio::time::Instant::now();
    evidence.flush_finished_session_ms =
        millis(finished.saturating_duration_since(session_started));
    evidence.flush_us = micros(finished.saturating_duration_since(started));
}

struct PeerCloseGap<'a> {
    capture: &'a Shared,
    feed: &'a str,
    slug: Option<&'a str>,
    generation: &'a mut u64,
    reason: String,
    delay: Duration,
}
async fn retain_and_flush_peer_close<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    frame: Option<&CloseFrame>,
    trace: &mut TransportTrace,
    deadline: tokio::time::Instant,
    gap: PeerCloseGap<'_>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let closed_generation = *gap.generation;
    let mut evidence = PeerCloseEvidence::new(frame, wall());
    evidence.flush_result = "pending_or_unknown";
    trace.close = Some(evidence);
    let reason = format!(
        "{};retry_delay_ms={};{}",
        gap.reason,
        millis(gap.delay),
        trace.reason_suffix()
    );
    let reason_sha256 = format!("{:x}", Sha256::digest(reason.as_bytes()));
    // There must be no await before retaining an already observed failure.
    // Capture main aborts receivers at its deadline; a blocked close ACK must
    // not turn that real failure into an apparently gap-free final tape.
    let recorded = emit(
        gap.capture,
        tape::Input::Gap {
            feed: gap.feed.into(),
            market_slug: gap.slug.map(str::to_owned),
            reason,
        },
    );
    *gap.generation += 1;
    trace.retained_gap_reason_sha256 = Some(reason_sha256);
    println!(
        "{}",
        json!({"event":"capture_peer_close_observed","feed":gap.feed,"market_slug":gap.slug,
        "generation":closed_generation,"gap_recorded":recorded,"retry_delay_ms":millis(gap.delay),
        "measurements":trace.summary(),"remote_root_cause":"UNKNOWN","orders_submitted":0})
    );
    flush_peer_close(ws, frame, trace, deadline).await;
    // If aborted during flush, the retained Gap and first stdout witness keep
    // pending_or_unknown. A completed ACK result is separate, hash-bound evidence.
    println!(
        "{}",
        json!({"event":"capture_peer_close_ack_result","feed":gap.feed,"market_slug":gap.slug,
        "generation":closed_generation,"measurements":trace.summary(),"remote_root_cause":"UNKNOWN","orders_submitted":0})
    );
}
struct SocketTrace {
    started: Instant,
    connected_at: Option<Instant>,
    last_text: Option<Instant>,
    text_frames: u64,
    max_record_write_ms: u64,
    max_heartbeat_late_ms: u64,
    transport: Option<TransportTrace>,
}
impl SocketTrace {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            connected_at: None,
            last_text: None,
            text_frames: 0,
            max_record_write_ms: 0,
            max_heartbeat_late_ms: 0,
            transport: None,
        }
    }
    fn for_profile(profile: CaptureProfile) -> Self {
        let mut trace = Self::new();
        if profile == CaptureProfile::TransportDiagnosticV1 {
            trace.transport = Some(TransportTrace::new());
        }
        trace
    }
    fn diagnostic_output(&self, feed: &str, slug: Option<&str>, generation: u64, end: &str) {
        if let Some(transport) = &self.transport {
            println!(
                "{}",
                json!({"event":"capture_transport_session_diagnostic","feed":feed,"market_slug":slug,"generation":generation,"end_category":end,"measurements":transport.summary(),"remote_root_cause":"UNKNOWN","orders_submitted":0})
            );
        }
    }
    fn stable(&self) -> bool {
        self.text_frames > 0
            && self
                .connected_at
                .is_some_and(|at| at.elapsed() >= STABLE_SESSION)
    }
    fn reason(
        &self,
        stage: &'static str,
        class: ErrorClass,
        generation: u64,
        close_code: Option<u16>,
    ) -> String {
        // All interpolated strings are locally fixed categories; numbers have
        // finite machine widths. This format is bounded below 512 ASCII bytes.
        format!(
            "socket_{stage};class={};generation={generation};session_ms={};last_text_age_ms={};text_frames={};record_write_ms_max={};heartbeat_late_ms_max={};close_code={};size={};limit={}",
            class.name,
            millis(self.started.elapsed()),
            self.last_text
                .map(|last| millis(last.elapsed()))
                .map_or_else(|| "none".into(), |n| n.to_string()),
            self.text_frames,
            self.max_record_write_ms,
            self.max_heartbeat_late_ms,
            close_code.map_or_else(|| "none".into(), |n| n.to_string()),
            class.size.map_or_else(|| "none".into(), |n| n.to_string()),
            class.limit.map_or_else(|| "none".into(), |n| n.to_string())
        )
    }
}

fn wall() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
struct Capture {
    writer: tape::Writer,
    start: Instant,
    deadline: Instant,
    frames: u64,
    gaps: u64,
    max_frames: u64,
    stop: Option<String>,
}
impl Capture {
    fn emit(&mut self, input: tape::Input) -> bool {
        let frame = matches!(input, tape::Input::Frame { .. });
        // A receive task may wait behind another writer. Check admission while
        // owning the writer lock; retain late Gap evidence and the final footer.
        if self.stop.is_some() || (frame && Instant::now() >= self.deadline) {
            return false;
        }
        let gap = matches!(input, tape::Input::Gap { .. });
        if frame && self.frames >= self.max_frames {
            self.stop = Some("frame_budget".into());
            return false;
        }
        if let Err(e) = self.writer.append(
            wall(),
            self.start.elapsed().as_millis() as u64,
            input,
            false,
        ) {
            self.stop = Some(e);
            return false;
        }
        self.frames += u64::from(frame);
        self.gaps += u64::from(gap);
        true
    }
    fn stopped(&self) -> bool {
        self.stop.is_some() || Instant::now() >= self.deadline
    }
}
type Shared = Arc<Mutex<Capture>>;
fn emit(c: &Shared, input: tape::Input) -> bool {
    c.lock().unwrap().emit(input)
}
fn emit_frame(c: &Shared, input: tape::Input, deadline: tokio::time::Instant) -> bool {
    let mut capture = c.lock().unwrap();
    if tokio::time::Instant::now() >= deadline {
        return false;
    }
    capture.emit(input)
}
fn emit_frame_observed(
    c: &Shared,
    input: tape::Input,
    deadline: tokio::time::Instant,
    trace: &mut TransportTrace,
) -> bool {
    let waiting = tokio::time::Instant::now();
    let mut capture = c.lock().unwrap();
    let acquired = tokio::time::Instant::now();
    trace.record_lock_wait_us_max = trace
        .record_lock_wait_us_max
        .max(micros(acquired.saturating_duration_since(waiting)));
    if acquired >= deadline {
        return false;
    }
    let result = capture.emit(input);
    let elapsed = micros(tokio::time::Instant::now().saturating_duration_since(acquired));
    trace.record_append_us_max = trace.record_append_us_max.max(elapsed);
    trace.record_busy_us_total = trace.record_busy_us_total.saturating_add(elapsed);
    result
}
async fn get(client: &reqwest::Client, url: &str) -> Result<Value, String> {
    let mut r = client
        .get(url)
        .send()
        .await
        .map_err(|_| "public GET transport failed")?;
    if !r.status().is_success() {
        return Err(format!("public GET HTTP {}", r.status().as_u16()));
    }
    if r.content_length().is_some_and(|n| n > 262144) {
        return Err("metadata byte budget".into());
    }
    let mut body = Vec::new();
    while let Some(b) = r.chunk().await.map_err(|_| "public GET body failed")? {
        if body.len() + b.len() > 262144 {
            return Err("metadata byte budget".into());
        }
        body.extend_from_slice(&b);
    }
    serde_json::from_slice(&body).map_err(|_| "public GET invalid JSON".into())
}
async fn gap_and_retry(
    c: &Shared,
    feed: &str,
    slug: Option<String>,
    reason: String,
    generation: &mut u64,
    delay: Duration,
    deadline: tokio::time::Instant,
) -> bool {
    emit(
        c,
        tape::Input::Gap {
            feed: feed.into(),
            market_slug: slug,
            reason,
        },
    );
    *generation += 1;
    wait_retry(c, delay, deadline).await
}
async fn wait_retry(c: &Shared, delay: Duration, deadline: tokio::time::Instant) -> bool {
    let retry_at = tokio::time::Instant::now() + delay;
    loop {
        let now = tokio::time::Instant::now();
        if c.lock().unwrap().stopped() || now >= deadline {
            return false;
        }
        if now >= retry_at {
            return true;
        }
        tokio::time::sleep_until(retry_at.min(deadline).min(now + Duration::from_millis(250)))
            .await;
    }
}
fn socket_deadline(c: &Shared, until_wall_ms: u64) -> tokio::time::Instant {
    let capture_deadline = c.lock().unwrap().deadline;
    let remaining = capture_deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_millis(until_wall_ms.saturating_sub(wall())));
    (tokio::time::Instant::now() + remaining).min(tokio::time::Instant::from_std(capture_deadline))
}
async fn socket(c: Shared, market: Option<Market>, profile: CaptureProfile) -> Result<(), String> {
    let feed = if market.is_some() { "clob" } else { "binance" };
    let slug = market.as_ref().map(|m| m.slug.clone());
    let mut generation = 0;
    let mut backoff = RetryBackoff::default();
    let until = market.as_ref().map_or(u64::MAX, |m| m.window_end_ms + 2000);
    // A wall-clock correction cannot extend this connection's original bound.
    let deadline = socket_deadline(&c, until);
    loop {
        if c.lock().unwrap().stopped() || wall() >= until || tokio::time::Instant::now() >= deadline
        {
            return Ok(());
        }
        let url = if market.is_some() {
            feeds::CLOB_URL
        } else {
            feeds::BINANCE_URL
        };
        let config = WebSocketConfig::default()
            .max_message_size(Some(feeds::MAX_MESSAGE_BYTES))
            .max_frame_size(Some(feeds::MAX_MESSAGE_BYTES));
        let mut trace = SocketTrace::for_profile(profile);
        let mut close_code = None;
        let mut already_recorded_gap_delay = None;
        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Ok(()),
            result = tokio::time::timeout(
                Duration::from_secs(5),
                connect_async_with_config(url, Some(config), false),
            ) => result,
        };
        let reason = match result {
            Ok(Ok((mut ws, _))) => 'connected: {
                trace.connected_at = Some(Instant::now());
                if c.lock().unwrap().stopped()
                    || wall() >= until
                    || tokio::time::Instant::now() >= deadline
                {
                    let _ = write_bounded(ws.close(None)).await;
                    trace.diagnostic_output(
                        feed,
                        slug.as_deref(),
                        generation,
                        "local_stop_after_connect",
                    );
                    return Ok(());
                }
                if let Some(m) = &market {
                    let subscription = profile.subscription(m);
                    if let Err(class) =
                        write_bounded(ws.send(Message::Text(subscription.to_string().into()))).await
                    {
                        break 'connected trace.reason(
                            "subscription_send",
                            class,
                            generation,
                            None,
                        );
                    }
                }
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                heartbeat.tick().await;
                loop {
                    if c.lock().unwrap().stopped()
                        || wall() >= until
                        || tokio::time::Instant::now() >= deadline
                    {
                        let _ = write_bounded(ws.close(None)).await;
                        trace.diagnostic_output(feed, slug.as_deref(), generation, "local_stop");
                        return Ok(());
                    }
                    tokio::select! {
                        biased;
                        _=tokio::time::sleep_until(deadline)=>{
                            let _=write_bounded(ws.close(None)).await;
                            trace.diagnostic_output(feed,slug.as_deref(),generation,"socket_deadline");
                            return Ok(());
                        },
                        scheduled=heartbeat.tick()=>{
                            trace.max_heartbeat_late_ms=trace.max_heartbeat_late_ms.max(millis(tokio::time::Instant::now().saturating_duration_since(scheduled)));
                            if market.is_some() {
                                let observed=trace.transport.as_ref().map(|_|tokio::time::Instant::now());
                                if let Some(transport)=&mut trace.transport {
                                    transport.application_ping_attempts=transport.application_ping_attempts.saturating_add(1);
                                }
                                let result=write_bounded(ws.send(Message::Text("PING".into()))).await;
                                if let (Some(transport),Some(started))=(&mut trace.transport,observed) {
                                    let completed=tokio::time::Instant::now();
                                    transport.application_ping_flush_us_max=transport.application_ping_flush_us_max.max(micros(completed.saturating_duration_since(started)));
                                    if result.is_ok() {
                                        transport.application_pings_flushed=transport.application_pings_flushed.saturating_add(1);
                                        transport.last_application_ping=Some(completed);
                                    }
                                }
                                if let Err(class)=result {break trace.reason("heartbeat_send",class,generation,None);}
                            }
                        },
                        received=async {
                            let started=(profile==CaptureProfile::TransportDiagnosticV1).then(tokio::time::Instant::now);
                            let message=ws.next().await;
                            let timing=started.map(|started|(started,tokio::time::Instant::now()));
                            (message,timing)
                        }=>{
                            let (message,timing)=received;
                            if let (Some(transport),Some((started,completed)))=(&mut trace.transport,timing) {
                                transport.observe_read(message.as_ref().and_then(|value|value.as_ref().ok()),started,completed);
                            }
                            match message {
                            Some(Ok(Message::Text(text)))=>{
                                if c.lock().unwrap().stopped() || wall() >= until || tokio::time::Instant::now() >= deadline {
                                    trace.diagnostic_output(feed,slug.as_deref(),generation,"text_after_cutoff");return Ok(());
                                }
                                let received=Instant::now();trace.last_text=Some(received);trace.text_frames+=1;
                                let input=tape::Input::Frame{feed:feed.into(),market_slug:slug.clone(),generation,text:text.to_string()};
                                let retained=if let Some(transport)=&mut trace.transport {
                                    emit_frame_observed(&c,input,deadline,transport)
                                } else {emit_frame(&c,input,deadline)};
                                if !retained {
                                    trace.diagnostic_output(feed,slug.as_deref(),generation,"record_admission_stopped");return Ok(());
                                }
                                trace.max_record_write_ms=trace.max_record_write_ms.max(millis(received.elapsed()));
                            },
                            Some(Ok(Message::Ping(payload)))=>{
                                let observed=trace.transport.as_ref().map(|_|tokio::time::Instant::now());
                                let result=write_bounded(ws.send(Message::Pong(payload))).await;
                                if let (Some(transport),Some(started))=(&mut trace.transport,observed) {
                                    transport.native_pong_flush_us_max=transport.native_pong_flush_us_max.max(micros(started.elapsed()));
                                    if result.is_ok() {transport.native_pongs_flushed=transport.native_pongs_flushed.saturating_add(1);}
                                }
                                if let Err(class)=result {
                                    break trace.reason("pong_send",class,generation,None);
                                }
                            },
                            Some(Ok(Message::Close(frame)))=>{
                                close_code=frame.as_ref().map(|frame|u16::from(frame.code));
                                let reason=trace.reason("peer_close",ErrorClass::named("peer_close"),generation,close_code);
                                let stable=trace.stable();
                                if let Some(transport)=&mut trace.transport {
                                    let jitter_ms=(uuid::Uuid::new_v4().as_u128()%1001) as u64;
                                    let delay=backoff.next_delay(close_code,stable,jitter_ms);
                                    already_recorded_gap_delay=Some(delay);
                                    retain_and_flush_peer_close(&mut ws,frame.as_ref(),transport,deadline,
                                        PeerCloseGap{capture:&c,feed,slug:slug.as_deref(),generation:&mut generation,reason:reason.clone(),delay}).await;
                                }
                                break reason;
                            },
                            Some(Err(error))=>break trace.reason("read",error_class(&error),generation,None),
                            None=>break trace.reason("read",ErrorClass::named("stream_ended"),generation,None),
                            _=>{},
                        }},
                        _=tokio::time::sleep(Duration::from_millis(250))=>{},
                    }
                }
            }
            Ok(Err(error)) => trace.reason("connect", error_class(&error), generation, None),
            Err(_) => trace.reason(
                "connect",
                ErrorClass::named("connect_timeout"),
                generation,
                None,
            ),
        };
        if let Some(delay) = already_recorded_gap_delay {
            if !wait_retry(&c, delay, deadline).await {
                return Ok(());
            }
            continue;
        }
        let jitter_ms = (uuid::Uuid::new_v4().as_u128() % 1001) as u64;
        let delay = backoff.next_delay(close_code, trace.stable(), jitter_ms);
        let mut reason = format!("{reason};retry_delay_ms={}", millis(delay));
        if let Some(transport) = &trace.transport {
            reason.push(';');
            reason.push_str(&transport.reason_suffix());
            trace.diagnostic_output(feed, slug.as_deref(), generation, "transport_gap");
        }
        // Subscription-send failures also consume a generation and back off;
        // a following frame can never masquerade as the failed connection.
        if !gap_and_retry(
            &c,
            feed,
            slug.clone(),
            reason,
            &mut generation,
            delay,
            deadline,
        )
        .await
        {
            return Ok(());
        }
    }
}
async fn fees(c: Shared, client: reqwest::Client, market: Market) {
    while !c.lock().unwrap().stopped() && wall() < market.window_end_ms + 2000 {
        let result = async {
            let version = get(&client, "https://clob.polymarket.com/version").await?;
            let raw = get(
                &client,
                &format!(
                    "https://clob.polymarket.com/clob-markets/{}",
                    market.condition_id
                ),
            )
            .await?;
            Ok::<_, String>((version, raw))
        }
        .await;
        match result {
            Ok((version, raw)) => {
                emit(
                    &c,
                    tape::Input::Fee {
                        condition_id: market.condition_id.clone(),
                        raw,
                        version,
                    },
                );
            }
            Err(_) => {
                emit(
                    &c,
                    tape::Input::Gap {
                        feed: "fee".into(),
                        market_slug: Some(market.slug.clone()),
                        reason: "fee_observation_failed".into(),
                    },
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    mm2_engine::tls::initialize()?;
    let a: Vec<_> = std::env::args().collect();
    let profile = CaptureProfile::parse(&a)?;
    let seconds: u64 = a[2].parse()?;
    let max_bytes: u64 = a[3].parse()?;
    let max_frames: u64 = a[4].parse()?;
    if !(1..=3600).contains(&seconds)
        || !(65536..=4_294_967_296).contains(&max_bytes)
        || !(1..=4_000_000).contains(&max_frames)
    {
        return Err("capture bounds invalid".into());
    }
    let start = Instant::now();
    let mut writer = tape::Writer::new(Path::new(&a[1]), max_bytes)?;
    writer.append(
        wall(),
        0,
        tape::Input::Header {
            schema: 1,
            source: profile.source().into(),
            started_wall_ms: wall(),
            max_bytes,
            max_frames,
        },
        false,
    )?;
    let c = Arc::new(Mutex::new(Capture {
        writer,
        start,
        deadline: start + Duration::from_secs(seconds),
        frames: 0,
        gaps: 0,
        max_frames,
        stop: None,
    }));
    let client = discovery::client()?;
    let mut tasks = tokio::task::JoinSet::new();
    if profile != CaptureProfile::Default {
        println!(
            "{}",
            json!({"event":"capture_profile_selected","profile":profile.proof(),"network":"public_read_only","orders_submitted":0})
        );
    }
    tasks.spawn(socket(c.clone(), None, profile));
    let mut markets = BTreeMap::<u64, Market>::new();
    let mut resolved = BTreeSet::new();
    let mut last_outcome = Instant::now() - Duration::from_secs(31);
    loop {
        if c.lock().unwrap().stopped() {
            break;
        }
        let now = wall();
        for start_ms in profile.wanted_starts(now) {
            if markets.contains_key(&start_ms) {
                continue;
            }
            if markets.len() >= 16 {
                c.lock().unwrap().stop = Some("market_budget".into());
                break;
            }
            let slug = format!("btc-updown-5m-{}", start_ms / 1000);
            let raw = get(
                &client,
                &format!("https://gamma-api.polymarket.com/markets/slug/{slug}"),
            )
            .await;
            match raw.and_then(|raw| discovery::parse_market(&raw, start_ms).map(|m| (m, raw))) {
                Ok((m, metadata)) => {
                    emit(
                        &c,
                        tape::Input::Market {
                            market: tape::MarketWire::from(&m),
                            metadata,
                        },
                    );
                    markets.insert(start_ms, m.clone());
                    tasks.spawn(socket(c.clone(), Some(m.clone()), profile));
                    let cc = c.clone();
                    let http = client.clone();
                    tasks.spawn(async move {
                        fees(cc, http, m).await;
                        Ok(())
                    });
                }
                Err(_) => {
                    emit(
                        &c,
                        tape::Input::Gap {
                            feed: "metadata".into(),
                            market_slug: Some(slug),
                            reason: "market_metadata_unavailable".into(),
                        },
                    );
                }
            }
        }
        if last_outcome.elapsed() >= Duration::from_secs(30) {
            last_outcome = Instant::now();
            let pending: Vec<_> = markets
                .values()
                .filter(|m| m.window_end_ms < now && !resolved.contains(&m.condition_id))
                .cloned()
                .collect();
            for m in pending {
                let target = paper_resolution::Target {
                    condition_id: m.condition_id.clone(),
                    start_ms: m.window_start_ms,
                    end_ms: m.window_end_ms,
                };
                if let Ok(Some(evidence)) = paper_resolution::observe(&target).await {
                    emit(&c, tape::Input::Outcome { evidence });
                    resolved.insert(m.condition_id.clone());
                }
            }
        }
        while let Some(result) = tasks.try_join_next() {
            if !matches!(result, Ok(Ok(()))) {
                emit(
                    &c,
                    tape::Input::Gap {
                        feed: "collector".into(),
                        market_slug: None,
                        reason: "collector_task_failed".into(),
                    },
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let mut c = c.lock().unwrap();
    let reason = c.stop.clone().unwrap_or_else(|| "deadline".into());
    let frames = c.frames;
    let gaps = c.gaps;
    let elapsed = c.start.elapsed().as_millis() as u64;
    // Capture completion is not proof that every venue event was delivered; no CLOB sequence exists.
    c.writer.append(
        wall(),
        elapsed,
        tape::Input::End {
            reason: reason.clone(),
            frames,
            gaps,
            complete: reason == "deadline" && gaps == 0,
        },
        true,
    )?;
    c.writer.finish()?;
    let mut result = json!({"tape":a[1],"frames":frames,"gaps":gaps,"stop_reason":reason,"network":"public_read_only","orders_submitted":0,"capture_completeness":"unproven_no_clob_sequence","score":"requires_replay_quality_and_final_outcomes"});
    if profile != CaptureProfile::Default {
        result["profile"] = profile.proof();
    }
    println!("{result}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

    fn profile_market() -> Market {
        Market {
            slug: "controlled_profile".into(),
            condition_id: "condition".into(),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: 300_000,
            window_end_ms: 600_000,
            tick_micro: 10_000,
        }
    }

    #[test]
    fn capture_profile_cli_is_explicit_unique_and_keeps_default_wire_and_source() {
        let base = ["capture", "new.jsonl", "35", "65536", "1000"];
        let mut arguments: Vec<_> = base.into_iter().map(str::to_owned).collect();
        assert_eq!(
            CaptureProfile::parse(&arguments),
            Ok(CaptureProfile::Default)
        );
        assert_eq!(
            CaptureProfile::Default.subscription(&profile_market()),
            json!({"assets_ids":["1","2"],"type":"market","custom_feature_enabled":true})
        );
        assert_eq!(CaptureProfile::Default.source(), "native_public_ws_capture");
        arguments.push("--capture-profile=no-initial-dump-v1".into());
        let profile = CaptureProfile::parse(&arguments).unwrap();
        assert_eq!(profile, CaptureProfile::NoInitialDumpV1);
        assert_eq!(
            profile.proof(),
            json!({"capture_profile":"no-initial-dump-v1","source":"native_public_ws_capture_no_initial_dump_v1","clob_initial_dump":false,"prewarm_ms":60_000})
        );
        arguments.push("--capture-profile=no-initial-dump-v1".into());
        assert!(CaptureProfile::parse(&arguments).is_err());
        arguments.pop();
        for unknown in [
            "--capture-profile=unknown",
            "--capture-profile=default",
            "--no-initial-dump",
            "--capture-profile=no-initial-dump-v1=extra",
        ] {
            arguments[5] = unknown.into();
            assert!(CaptureProfile::parse(&arguments).is_err());
        }
        assert!(CaptureProfile::parse(&arguments[..4]).is_err());
    }

    #[test]
    fn opt_in_prewarm_opens_only_current_and_next_at_exact_sixty_second_boundary() {
        let profile = CaptureProfile::NoInitialDumpV1;
        assert_eq!(profile.wanted_starts(239_999), vec![0]);
        assert_eq!(profile.wanted_starts(240_000), vec![0, 300_000]);
        assert_eq!(profile.wanted_starts(299_999), vec![0, 300_000]);
        assert_eq!(profile.wanted_starts(300_000), vec![300_000]);
        assert_eq!(CaptureProfile::Default.wanted_starts(279_999), vec![0]);
        assert_eq!(
            CaptureProfile::Default.wanted_starts(280_000),
            vec![0, 300_000]
        );
        assert_eq!(profile.wanted_starts(u64::MAX).len(), 1);
    }

    #[test]
    fn transport_diagnostic_profile_is_separate_and_keeps_old_profiles_unobserved() {
        let mut arguments = ["capture", "new.jsonl", "35", "65536", "1000"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        arguments.push("--capture-profile=transport-diagnostic-v1".into());
        let diagnostic = CaptureProfile::parse(&arguments).unwrap();
        assert_eq!(diagnostic, CaptureProfile::TransportDiagnosticV1);
        assert_eq!(
            diagnostic.proof(),
            json!({"capture_profile":"transport-diagnostic-v1","source":"native_public_ws_capture_transport_diagnostic_v1","clob_initial_dump":false,"prewarm_ms":60_000})
        );
        assert_eq!(
            diagnostic.subscription(&profile_market()),
            CaptureProfile::NoInitialDumpV1.subscription(&profile_market())
        );
        assert_ne!(
            diagnostic.source(),
            CaptureProfile::NoInitialDumpV1.source()
        );
        for old in [CaptureProfile::Default, CaptureProfile::NoInitialDumpV1] {
            assert!(SocketTrace::for_profile(old).transport.is_none());
        }
        arguments.push("--capture-profile=no-initial-dump-v1".into());
        assert!(CaptureProfile::parse(&arguments).is_err());
    }

    #[test]
    fn close_reason_evidence_is_bounded_utf8_hex_and_never_rendered_as_peer_text() {
        let reason = format!("{}é\n;forged_gap=true", "x".repeat(122));
        let frame = CloseFrame {
            code: 1013.into(),
            reason: reason.clone().into(),
        };
        let evidence = PeerCloseEvidence::new(Some(&frame), 17);
        let retained = hex::decode(&evidence.reason_hex).unwrap();
        assert_eq!(retained, vec![b'x'; 122]);
        assert!(std::str::from_utf8(&retained).is_ok());
        assert_eq!(evidence.retained_reason_bytes, 122);
        assert_eq!(evidence.original_reason_bytes, reason.len());
        assert_eq!(
            evidence.retained_reason_sha256,
            format!("{:x}", Sha256::digest(&retained))
        );
        let mut transport = TransportTrace::new();
        transport.close = Some(evidence);
        let suffix = transport.reason_suffix();
        assert!(suffix.is_ascii());
        assert!(!suffix.contains("forged_gap"));
        assert!(!suffix.contains('\n'));
        assert!(suffix.contains("peer_reason_truncated=true"));
        assert!(suffix.len() < 2048);
        let none = PeerCloseEvidence::new(None, 0);
        assert!(none.reason_hex.is_empty());
        assert_eq!(none.retained_reason_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostic_wire_preserves_one_pong_and_flushes_exact_peer_close_ack_and_gap() {
        let (client_io, peer_io) = tokio::io::duplex(4096);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Server, None).await;
        let mut trace = TransportTrace::new();
        let native = Message::Text("{\"untouched\":true}".into());
        let ping = Message::Ping(vec![7, 9, 0, 255].into());
        let close = CloseFrame {
            code: 1013.into(),
            reason: "retry later\n;untrusted=true".into(),
        };
        peer.send(native.clone()).await.unwrap();
        peer.send(ping.clone()).await.unwrap();
        peer.send(Message::Close(Some(close.clone())))
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let received = client.next().await.unwrap().unwrap();
        assert_eq!(received, native);
        trace.observe_read(Some(&received), started, tokio::time::Instant::now());
        let received = client.next().await.unwrap().unwrap();
        assert_eq!(received, ping);
        trace.observe_read(Some(&received), started, tokio::time::Instant::now());
        let Message::Ping(payload) = received else {
            unreachable!()
        };
        write_bounded(client.send(Message::Pong(payload)))
            .await
            .unwrap();
        assert_eq!(
            peer.next().await.unwrap().unwrap(),
            Message::Pong(vec![7, 9, 0, 255].into())
        );
        let Message::Close(received_close) = client.next().await.unwrap().unwrap() else {
            panic!("peer close expected");
        };
        assert_eq!(received_close, Some(close.clone()));
        // Returning Close queues the ACK but does not flush it. This fails if
        // there was a duplicate manual/automatic Pong before the close ACK.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), peer.next())
                .await
                .is_err()
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let (_dir, capture) = retry_capture();
        let mut generation = 3;
        retain_and_flush_peer_close(
            &mut client,
            received_close.as_ref(),
            &mut trace,
            deadline,
            PeerCloseGap {
                capture: &capture,
                feed: "clob",
                slug: Some("controlled"),
                generation: &mut generation,
                reason: "socket_peer_close;class=peer_close;generation=3;close_code=1013".into(),
                delay: Duration::ZERO,
            },
        )
        .await;
        assert_eq!(
            peer.next().await.unwrap().unwrap(),
            Message::Close(Some(close.clone()))
        );
        let evidence = trace.close.as_ref().unwrap();
        assert_eq!(evidence.flush_result, "ok");
        assert_eq!(
            hex::decode(&evidence.reason_hex).unwrap(),
            close.reason.as_bytes()
        );
        let reason = trace.reason_suffix();
        assert!(!reason.contains("untrusted=true"));
        assert!(trace.retained_gap_reason_sha256.is_some());
        assert_eq!(generation, 4);
        assert_eq!(capture.lock().unwrap().gaps, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn aborting_stalled_peer_close_ack_preserves_already_observed_gap_in_final_chain() {
        let (client_io, peer_io) = tokio::io::duplex(1);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Server, None).await;
        let frame = CloseFrame {
            code: 1013.into(),
            reason: "overload\n;peer_claim=true".into(),
        };
        let (sent, received) = tokio::join!(peer.send(Message::Close(Some(frame))), client.next());
        sent.unwrap();
        let Message::Close(frame) = received.unwrap().unwrap() else {
            panic!("peer close expected")
        };
        let (dir, capture) = retry_capture();
        let owned_capture = capture.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(40);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let mut generation = 7;
            let mut trace = TransportTrace::new();
            retain_and_flush_peer_close(
                &mut client,
                frame.as_ref(),
                &mut trace,
                deadline,
                PeerCloseGap {
                    capture: &owned_capture,
                    feed: "clob",
                    slug: Some("controlled"),
                    generation: &mut generation,
                    reason: "socket_peer_close;class=peer_close;generation=7;close_code=1013"
                        .into(),
                    delay: Duration::from_millis(5007),
                },
            )
            .await;
            generation
        });
        while capture.lock().unwrap().gaps == 0 {
            tokio::task::yield_now().await;
        }
        // The peer stops reading, so the real tiny transport leaves ACK flush
        // pending. Use the same abort_all boundary as capture main, then seal
        // the final tape. The observed1013 must already be in its hash chain.
        tasks.abort_all();
        assert!(tasks.join_next().await.unwrap().unwrap_err().is_cancelled());
        tokio::time::advance(Duration::from_millis(40)).await;
        {
            let mut state = capture.lock().unwrap();
            assert_eq!(state.gaps, 1);
            let elapsed = millis(state.start.elapsed());
            state
                .writer
                .append(
                    wall(),
                    elapsed,
                    tape::Input::End {
                        reason: "deadline".into(),
                        frames: 0,
                        gaps: 1,
                        complete: false,
                    },
                    true,
                )
                .unwrap();
            state.writer.finish().unwrap();
        }
        let mut gaps = Vec::new();
        tape::visit(&dir.path().join("retry.jsonl"), 65_536, |record| {
            if let tape::Input::Gap { reason, .. } = record.input {
                gaps.push(reason);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("generation=7;close_code=1013;retry_delay_ms=5007;"));
        assert!(gaps[0].contains("peer_close_flush_result=pending_or_unknown"));
        assert!(
            gaps[0].contains("peer_reason_hex=6f7665726c6f61640a3b706565725f636c61696d3d74727565")
        );
        assert!(!gaps[0].contains("peer_claim=true"));
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostic_peer_close_ack_never_outlives_deadline_or_polls_after_it() {
        for remaining in [Duration::ZERO, Duration::from_millis(40)] {
            // Read the server close concurrently because only one wire byte
            // fits. Then stop reading ACK bytes so its flush must time out.
            let (client_io, peer_io) = tokio::io::duplex(1);
            let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
            let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Server, None).await;
            let frame = CloseFrame {
                code: 1013.into(),
                reason: "busy".into(),
            };
            let (sent, received) =
                tokio::join!(peer.send(Message::Close(Some(frame))), client.next());
            sent.unwrap();
            let Message::Close(frame) = received.unwrap().unwrap() else {
                panic!("peer close expected")
            };
            let mut trace = TransportTrace::new();
            let started = tokio::time::Instant::now();
            flush_peer_close(&mut client, frame.as_ref(), &mut trace, started + remaining).await;
            assert_eq!(started.elapsed(), remaining);
            let evidence = trace.close.as_ref().unwrap();
            assert_eq!(
                evidence.flush_result,
                if remaining.is_zero() {
                    "deadline_before_flush"
                } else {
                    "deadline_during_flush"
                }
            );
            assert_eq!(evidence.flush_us, micros(remaining));
            drop(client);
            let mut bytes = Vec::new();
            peer.get_mut().read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes.len(), usize::from(!remaining.is_zero()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn diagnostic_burst_and_pong_observations_keep_fixed_bucket_and_unmatched_semantics() {
        let mut trace = TransportTrace::new();
        let started = tokio::time::Instant::now();
        for _ in 0..3 {
            trace.observe_read(Some(&Message::Text("x".into())), started, started);
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let now = tokio::time::Instant::now();
        trace.last_application_ping = Some(now);
        trace.observe_read(Some(&Message::Text("PONG".into())), now, now);
        trace.observe_read(Some(&Message::Text("yy".into())), now, now);
        let summary = trace.summary();
        assert_eq!(summary["peak_text_frames_in_1s_bucket"], 3);
        assert_eq!(summary["diagnostic_text_frames"], 5);
        assert_eq!(summary["message_payload_bytes"], 9);
        assert_eq!(summary["application_pongs_received"], 1);
        assert_eq!(summary["last_pong_since_last_ping_ms"], 0);
        assert_eq!(summary["application_pings_flushed"], 0);
        assert_eq!(summary["between_completed_read_attempts_us_max"], 1_000_000);
    }

    #[tokio::test]
    async fn opt_in_subscription_wire_disables_only_initial_dump_and_preserves_later_books() {
        let market = profile_market();
        let subscription = CaptureProfile::NoInitialDumpV1.subscription(&market);
        let (client_io, peer_io) = tokio::io::duplex(16_384);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Server, None).await;
        let native_frames = [
            json!({"event_type":"price_change","market":"condition","timestamp":"300001","price_changes":[{"asset_id":"1","side":"BUY","price":"0.4","size":"5"}]}).to_string(),
            json!({"event_type":"book","market":"condition","asset_id":"1","timestamp":"300002","bids":[{"price":"0.4","size":"5"}],"asks":[{"price":"0.6","size":"5"}]}).to_string(),
            json!({"event_type":"book","market":"condition","asset_id":"2","timestamp":"300003","bids":[{"price":"0.4","size":"7"}],"asks":[{"price":"0.6","size":"7"}]}).to_string(),
        ];
        let (dir, capture) = retry_capture();
        let capture_side = async {
            write_bounded(client.send(Message::Text(subscription.to_string().into())))
                .await
                .unwrap();
            for expected in &native_frames {
                let Message::Text(text) = client.next().await.unwrap().unwrap() else {
                    panic!("native public text expected");
                };
                assert_eq!(text.as_str(), expected);
                assert!(emit_frame(
                    &capture,
                    tape::Input::Frame {
                        feed: "clob".into(),
                        market_slug: Some(market.slug.clone()),
                        generation: 7,
                        text: text.to_string(),
                    },
                    tokio::time::Instant::now() + Duration::from_secs(10),
                ));
            }
        };
        let public_peer = async {
            let Message::Text(text) = peer.next().await.unwrap().unwrap() else {
                panic!("subscription text expected");
            };
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(
                request,
                json!({"assets_ids":["1","2"],"type":"market","custom_feature_enabled":true,"initial_dump":false})
            );
            for text in &native_frames {
                peer.send(Message::Text(text.clone().into())).await.unwrap();
            }
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(capture_side, public_peer);
        })
        .await
        .unwrap();
        {
            let mut state = capture.lock().unwrap();
            assert_eq!(state.frames, 3);
            assert_eq!(state.gaps, 0);
            let elapsed = millis(state.start.elapsed());
            state
                .writer
                .append(
                    wall(),
                    elapsed,
                    tape::Input::End {
                        reason: "controlled".into(),
                        frames: 3,
                        gaps: 0,
                        complete: false,
                    },
                    true,
                )
                .unwrap();
            state.writer.finish().unwrap();
        }
        let mut retained = Vec::new();
        tape::visit(&dir.path().join("retry.jsonl"), 65_536, |record| {
            if let tape::Input::Frame {
                feed,
                market_slug,
                generation,
                text,
            } = record.input
            {
                assert_eq!(feed, "clob");
                assert_eq!(market_slug.as_deref(), Some(market.slug.as_str()));
                assert_eq!(generation, 7);
                retained.push(text);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(retained, native_frames);
    }

    fn retry_capture() -> (tempfile::TempDir, Shared) {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = tape::Writer::new(&dir.path().join("retry.jsonl"), 65536).unwrap();
        writer
            .append(
                wall(),
                0,
                tape::Input::Header {
                    schema: 1,
                    source: "controlled_retry_test".into(),
                    started_wall_ms: wall(),
                    max_bytes: 65536,
                    max_frames: 10,
                },
                false,
            )
            .unwrap();
        let start = Instant::now();
        let c = Arc::new(Mutex::new(Capture {
            writer,
            start,
            deadline: start + Duration::from_secs(3600),
            frames: 0,
            gaps: 0,
            max_frames: 10,
            stop: None,
        }));
        (dir, c)
    }

    #[tokio::test(start_paused = true)]
    async fn locked_frame_admission_checks_capture_and_socket_cutoffs_but_retains_late_gaps() {
        let frame = || tape::Input::Frame {
            feed: "clob".into(),
            market_slug: Some("controlled".into()),
            generation: 0,
            text: "PONG".into(),
        };
        let (_dir, expired_capture) = retry_capture();
        expired_capture.lock().unwrap().deadline = Instant::now() - Duration::from_millis(1);
        let future_socket = tokio::time::Instant::now() + Duration::from_secs(10);
        assert!(!emit_frame(&expired_capture, frame(), future_socket));
        assert!(!emit(&expired_capture, frame()));
        assert!(emit(
            &expired_capture,
            tape::Input::Gap {
                feed: "clob".into(),
                market_slug: Some("controlled".into()),
                reason: "controlled_late_gap".into(),
            }
        ));
        assert_eq!(expired_capture.lock().unwrap().frames, 0);
        assert_eq!(expired_capture.lock().unwrap().gaps, 1);

        let (_dir, active_capture) = retry_capture();
        let socket_cutoff = tokio::time::Instant::now() + Duration::from_millis(50);
        assert!(emit_frame(&active_capture, frame(), socket_cutoff));
        tokio::time::advance(Duration::from_millis(50)).await;
        // The capture still has ample time; the independent market/socket
        // cutoff alone rejects a task that reaches the writer lock too late.
        assert!(!active_capture.lock().unwrap().stopped());
        assert!(!emit_frame(&active_capture, frame(), socket_cutoff));
        assert_eq!(active_capture.lock().unwrap().frames, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_retry_later_closes_wait_with_jitter_cap_and_keep_every_gap() {
        let (dir, c) = retry_capture();
        let mut backoff = RetryBackoff::default();
        let mut generation = 0;
        for expected_ms in [5007, 10007, 20007, 30000, 30000] {
            let delay = backoff.next_delay(Some(1013), false, 7);
            assert_eq!(millis(delay), expected_ms);
            let started = tokio::time::Instant::now();
            let reason = format!(
                "socket_peer_close;class=peer_close;generation={generation};close_code=1013;retry_delay_ms={expected_ms}"
            );
            assert!(
                gap_and_retry(
                    &c,
                    "clob",
                    Some("controlled".into()),
                    reason,
                    &mut generation,
                    delay,
                    started + Duration::from_secs(60)
                )
                .await
            );
            assert_eq!(started.elapsed(), delay);
        }
        assert_eq!(generation, 5);
        {
            let mut capture = c.lock().unwrap();
            assert_eq!(capture.gaps, 5);
            let elapsed = millis(capture.start.elapsed());
            capture
                .writer
                .append(
                    wall(),
                    elapsed,
                    tape::Input::End {
                        reason: "test".into(),
                        frames: 0,
                        gaps: 5,
                        complete: false,
                    },
                    true,
                )
                .unwrap();
            capture.writer.finish().unwrap();
        }
        let mut seen = 0;
        tape::visit(&dir.path().join("retry.jsonl"), 65536, |record| {
            if let tape::Input::Gap { reason, .. } = record.input {
                assert!(reason.contains(&format!("generation={seen};")));
                assert!(reason.contains("close_code=1013"));
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_stable_observed_session_resets_retry_later_history() {
        let (_dir, c) = retry_capture();
        let mut backoff = RetryBackoff::default();
        assert_eq!(
            backoff.next_delay(Some(1013), false, 0),
            Duration::from_secs(5)
        );
        // A different short failure uses its old generic delay but cannot erase
        // the remembered service-requested pause.
        assert_eq!(
            backoff.next_delay(None, false, 999),
            Duration::from_millis(500)
        );
        let mut trace = SocketTrace::new();
        trace.connected_at = Some(Instant::now());
        trace.text_frames = 1;
        assert!(!trace.stable());
        assert_eq!(
            backoff.next_delay(Some(1013), trace.stable(), 0),
            Duration::from_secs(10)
        );
        trace.connected_at = Some(Instant::now() - Duration::from_secs(31));
        trace.text_frames = 0;
        assert!(!trace.stable());
        assert_eq!(
            backoff.next_delay(Some(1013), trace.stable(), 0),
            Duration::from_secs(20)
        );
        trace.text_frames = 1;
        assert!(trace.stable());
        let delay = backoff.next_delay(Some(1013), trace.stable(), 1001);
        assert_eq!(delay, Duration::from_secs(6));
        let started = tokio::time::Instant::now();
        assert!(wait_retry(&c, delay, started + Duration::from_secs(60)).await);
        assert_eq!(started.elapsed(), delay);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_wait_clips_to_deadline_and_observes_stop_without_reconnecting() {
        let (_dir, c) = retry_capture();
        let started = tokio::time::Instant::now();
        assert!(
            !wait_retry(
                &c,
                Duration::from_secs(30),
                started + Duration::from_millis(800)
            )
            .await
        );
        assert_eq!(started.elapsed(), Duration::from_millis(800));
        let started = tokio::time::Instant::now();
        assert!(!wait_retry(&c, Duration::from_secs(30), started).await);
        assert_eq!(started.elapsed(), Duration::ZERO);
        let started = tokio::time::Instant::now();
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c.lock().unwrap().stop = Some("controlled_stop".into());
        };
        let waiter = wait_retry(
            &c,
            Duration::from_secs(30),
            started + Duration::from_secs(60),
        );
        let (retry, ()) = tokio::join!(waiter, stopper);
        assert!(!retry);
        assert_eq!(started.elapsed(), Duration::from_millis(250));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_websocket_send_and_close_are_bounded_before_discarding_connection() {
        // A real WebSocket over a tiny in-memory transport. The peer never
        // reads, so both application writes and close flushes must time out.
        for closing in [false, true] {
            let (io, mut peer) = tokio::io::duplex(1);
            let mut ws = WebSocketStream::from_raw_socket(io, Role::Client, None).await;
            let started = tokio::time::Instant::now();
            let class = if closing {
                write_bounded(ws.close(None)).await.unwrap_err()
            } else {
                write_bounded(ws.send(Message::Text("x".repeat(4096).into())))
                    .await
                    .unwrap_err()
            };
            assert_eq!(class.name, "write_timeout");
            assert_eq!(started.elapsed(), SOCKET_WRITE_TIMEOUT);
            // A timed-out partially written connection must be discarded,
            // never reused for a later subscription or heartbeat.
            drop(ws);
            let mut retained = Vec::new();
            peer.read_to_end(&mut retained).await.unwrap();
            assert_eq!(retained.len(), 1);
        }
    }

    #[tokio::test]
    async fn bounded_control_writes_preserve_native_text_and_ping_pong_order() {
        let (client_io, peer_io) = tokio::io::duplex(1024);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer_io, Role::Server, None).await;
        let capture_side = async {
            write_bounded(client.send(Message::Text("PING".into())))
                .await
                .unwrap();
            assert_eq!(
                client.next().await.unwrap().unwrap(),
                Message::Text("before".into())
            );
            let Message::Ping(payload) = client.next().await.unwrap().unwrap() else {
                panic!("expected native control Ping");
            };
            write_bounded(client.send(Message::Pong(payload)))
                .await
                .unwrap();
            assert_eq!(
                client.next().await.unwrap().unwrap(),
                Message::Text("after".into())
            );
            write_bounded(client.close(None)).await.unwrap();
        };
        let public_peer = async {
            assert_eq!(
                peer.next().await.unwrap().unwrap(),
                Message::Text("PING".into())
            );
            for message in [
                Message::Text("before".into()),
                Message::Ping(vec![7, 9].into()),
                Message::Text("after".into()),
            ] {
                peer.send(message).await.unwrap();
            }
            assert_eq!(
                peer.next().await.unwrap().unwrap(),
                Message::Pong(vec![7, 9].into())
            );
            assert!(matches!(
                peer.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(capture_side, public_peer);
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn subscription_failure_records_one_gap_then_advances_generation_with_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retry.jsonl");
        let start = Instant::now();
        let mut writer = tape::Writer::new(&path, 65536).unwrap();
        writer
            .append(
                wall(),
                0,
                tape::Input::Header {
                    schema: 1,
                    source: "controlled_retry_test".into(),
                    started_wall_ms: wall(),
                    max_bytes: 65536,
                    max_frames: 10,
                },
                false,
            )
            .unwrap();
        let c = Arc::new(Mutex::new(Capture {
            writer,
            start,
            deadline: start + Duration::from_secs(60),
            frames: 0,
            gaps: 0,
            max_frames: 10,
            stop: None,
        }));
        let mut generation = 7;
        let before = tokio::time::Instant::now();
        let reason = SocketTrace::new().reason(
            "subscription_send",
            ErrorClass::named("io_broken_pipe"),
            generation,
            None,
        );
        gap_and_retry(
            &c,
            "clob",
            Some("controlled".into()),
            reason,
            &mut generation,
            Duration::from_millis(500),
            before + Duration::from_secs(60),
        )
        .await;
        assert_eq!(generation, 8);
        assert!(before.elapsed() >= Duration::from_millis(500));
        assert!(emit(
            &c,
            tape::Input::Frame {
                feed: "clob".into(),
                market_slug: Some("controlled".into()),
                generation,
                text: "PONG".into()
            }
        ));
        {
            let mut capture = c.lock().unwrap();
            assert_eq!(capture.gaps, 1);
            let elapsed = millis(capture.start.elapsed());
            let frames = capture.frames;
            let gaps = capture.gaps;
            capture
                .writer
                .append(
                    wall(),
                    elapsed,
                    tape::Input::End {
                        reason: "test".into(),
                        frames,
                        gaps,
                        complete: false,
                    },
                    true,
                )
                .unwrap();
            capture.writer.finish().unwrap();
        }
        let mut gaps = 0;
        let mut next = None;
        tape::visit(&path, 65536, |record| {
            match record.input {
                tape::Input::Gap { reason, .. } => {
                    gaps += 1;
                    assert!(
                        reason.contains("subscription_send") && reason.contains("generation=7")
                    );
                }
                tape::Input::Frame { generation, .. } => next = Some(generation),
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        assert_eq!((gaps, next), (1, Some(8)));
    }
    #[test]
    fn failure_diagnostics_distinguish_capacity_reset_and_private_io_errors_without_leaking_text() {
        let capacity = WebSocketError::Capacity(CapacityError::MessageTooLong {
            size: 1_048_577,
            max_size: 1_048_576,
        });
        let class = error_class(&capacity);
        assert_eq!(class.name, "capacity_message_too_long");
        assert_eq!(
            (class.size, class.limit),
            (Some(1_048_577), Some(1_048_576))
        );
        let reset = WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake);
        assert_eq!(error_class(&reset).name, "protocol_reset_without_close");
        let io = WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "private-token=NEVER-LOG-THIS",
        ));
        let reason = SocketTrace::new().reason("heartbeat_send", error_class(&io), 17, None);
        assert!(reason.contains("heartbeat_send;class=io_connection_reset;generation=17"));
        assert!(!reason.contains("NEVER-LOG-THIS"));
        assert!(!reason.contains("private-token"));
        assert!(reason.is_ascii() && reason.len() < 512);
    }
    #[test]
    fn numeric_connection_diagnostics_remain_bounded_and_keep_peer_close_code() {
        let trace = SocketTrace {
            started: Instant::now(),
            connected_at: Some(Instant::now()),
            last_text: Some(Instant::now()),
            text_frames: u64::MAX,
            max_record_write_ms: u64::MAX,
            max_heartbeat_late_ms: u64::MAX,
            transport: None,
        };
        let reason = trace.reason(
            "peer_close",
            ErrorClass {
                name: "capacity_message_too_long",
                size: Some(usize::MAX),
                limit: Some(usize::MAX),
            },
            u64::MAX,
            Some(u16::MAX),
        );
        assert!(reason.contains("close_code=65535"));
        assert!(reason.contains("record_write_ms_max="));
        assert!(reason.contains("heartbeat_late_ms_max="));
        assert!(reason.is_ascii() && reason.len() < 512);
    }
}
