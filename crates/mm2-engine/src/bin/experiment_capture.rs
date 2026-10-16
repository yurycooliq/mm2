//! Bounded public input capture. No authenticated, wallet, order or relayer endpoints.
use futures_util::{SinkExt, StreamExt};
use mm2_engine::experiments as tape;
use mm2_engine::{discovery, domain::Market, feeds, paper_resolution};
use serde_json::{Value, json};
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
        protocol::WebSocketConfig,
    },
};

const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const STABLE_SESSION: Duration = Duration::from_secs(30);

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
// peer close reason, or payload into secondary diagnostics.
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
struct SocketTrace {
    started: Instant,
    connected_at: Option<Instant>,
    last_text: Option<Instant>,
    text_frames: u64,
    max_record_write_ms: u64,
    max_heartbeat_late_ms: u64,
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
        if self.stop.is_some() {
            return false;
        }
        let frame = matches!(input, tape::Input::Frame { .. });
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
async fn socket(c: Shared, market: Option<Market>) -> Result<(), String> {
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
        let mut trace = SocketTrace::new();
        let mut close_code = None;
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
                    return Ok(());
                }
                if let Some(m) = &market {
                    let subscription = json!({"assets_ids":[m.up_token,m.down_token],"type":"market","custom_feature_enabled":true});
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
                        return Ok(());
                    }
                    tokio::select! {
                        biased;
                        _=tokio::time::sleep_until(deadline)=>{
                            let _=write_bounded(ws.close(None)).await;
                            return Ok(());
                        },
                        scheduled=heartbeat.tick()=>{
                            trace.max_heartbeat_late_ms=trace.max_heartbeat_late_ms.max(millis(tokio::time::Instant::now().saturating_duration_since(scheduled)));
                            if market.is_some() && let Err(class)=write_bounded(ws.send(Message::Text("PING".into()))).await {
                                break trace.reason("heartbeat_send",class,generation,None);
                            }
                        },
                        message=ws.next()=>{match message {
                            Some(Ok(Message::Text(text)))=>{
                                if c.lock().unwrap().stopped() || wall() >= until || tokio::time::Instant::now() >= deadline {return Ok(());}
                                let received=Instant::now();trace.last_text=Some(received);trace.text_frames+=1;
                                if !emit(&c,tape::Input::Frame{feed:feed.into(),market_slug:slug.clone(),generation,text:text.to_string()}){return Ok(());}
                                trace.max_record_write_ms=trace.max_record_write_ms.max(millis(received.elapsed()));
                            },
                            Some(Ok(Message::Ping(payload)))=>{
                                if let Err(class)=write_bounded(ws.send(Message::Pong(payload))).await {
                                    break trace.reason("pong_send",class,generation,None);
                                }
                            },
                            Some(Ok(Message::Close(frame)))=>{
                                close_code=frame.map(|frame|u16::from(frame.code));
                                break trace.reason("peer_close",ErrorClass::named("peer_close"),generation,close_code);
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
        let jitter_ms = (uuid::Uuid::new_v4().as_u128() % 1001) as u64;
        let delay = backoff.next_delay(close_code, trace.stable(), jitter_ms);
        let reason = format!("{reason};retry_delay_ms={}", millis(delay));
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
    if a.len() != 5 {
        return Err("usage: experiment_capture NEW_TAPE.jsonl SECONDS MAX_BYTES MAX_FRAMES".into());
    }
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
            source: "native_public_ws_capture".into(),
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
    tasks.spawn(socket(c.clone(), None));
    let mut markets = BTreeMap::<u64, Market>::new();
    let mut resolved = BTreeSet::new();
    let mut last_outcome = Instant::now() - Duration::from_secs(31);
    loop {
        if c.lock().unwrap().stopped() {
            break;
        }
        let now = wall();
        let current = now / 300000 * 300000;
        let mut wanted = vec![current];
        if current + 300000 - now <= 20000 {
            wanted.push(current + 300000);
        }
        for start_ms in wanted {
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
                    tasks.spawn(socket(c.clone(), Some(m.clone())));
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
    println!(
        "{}",
        json!({"tape":a[1],"frames":frames,"gaps":gaps,"stop_reason":reason,"network":"public_read_only","orders_submitted":0,"capture_completeness":"unproven_no_clob_sequence","score":"requires_replay_quality_and_final_outcomes"})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

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
