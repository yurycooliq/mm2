//! Bounded public input capture. No authenticated, wallet, order or relayer endpoints.
use futures_util::{SinkExt, StreamExt};
use mm2_engine::experiments as tape;
use mm2_engine::{discovery, domain::Market, feeds, paper_resolution};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
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
fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
struct SocketTrace {
    started: Instant,
    last_text: Option<Instant>,
    text_frames: u64,
    max_record_write_ms: u64,
    max_heartbeat_late_ms: u64,
}
impl SocketTrace {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            last_text: None,
            text_frames: 0,
            max_record_write_ms: 0,
            max_heartbeat_late_ms: 0,
        }
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
) {
    emit(
        c,
        tape::Input::Gap {
            feed: feed.into(),
            market_slug: slug,
            reason,
        },
    );
    *generation += 1;
    tokio::time::sleep(Duration::from_millis(500)).await;
}
async fn socket(c: Shared, market: Option<Market>) -> Result<(), String> {
    let feed = if market.is_some() { "clob" } else { "binance" };
    let slug = market.as_ref().map(|m| m.slug.clone());
    let mut generation = 0;
    let until = market.as_ref().map_or(u64::MAX, |m| m.window_end_ms + 2000);
    loop {
        if c.lock().unwrap().stopped() || wall() >= until {
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
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            connect_async_with_config(url, Some(config), false),
        )
        .await;
        let reason = match result {
            Ok(Ok((mut ws, _))) => 'connected: {
                if let Some(m) = &market {
                    let subscription = json!({"assets_ids":[m.up_token,m.down_token],"type":"market","custom_feature_enabled":true});
                    if let Err(error) = ws
                        .send(Message::Text(subscription.to_string().into()))
                        .await
                    {
                        break 'connected trace.reason(
                            "subscription_send",
                            error_class(&error),
                            generation,
                            None,
                        );
                    }
                }
                let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
                heartbeat.tick().await;
                loop {
                    if c.lock().unwrap().stopped() || wall() >= until {
                        let _ = ws.close(None).await;
                        return Ok(());
                    }
                    tokio::select! {
                        _=tokio::time::sleep(Duration::from_millis(250))=>{},
                        scheduled=heartbeat.tick()=>{
                            trace.max_heartbeat_late_ms=trace.max_heartbeat_late_ms.max(millis(tokio::time::Instant::now().saturating_duration_since(scheduled)));
                            if market.is_some() && let Err(error)=ws.send(Message::Text("PING".into())).await {
                                break trace.reason("heartbeat_send",error_class(&error),generation,None);
                            }
                        },
                        message=ws.next()=>{match message {
                            Some(Ok(Message::Text(text)))=>{
                                let received=Instant::now();trace.last_text=Some(received);trace.text_frames+=1;
                                if !emit(&c,tape::Input::Frame{feed:feed.into(),market_slug:slug.clone(),generation,text:text.to_string()}){return Ok(());}
                                trace.max_record_write_ms=trace.max_record_write_ms.max(millis(received.elapsed()));
                            },
                            Some(Ok(Message::Ping(payload)))=>{
                                if let Err(error)=ws.send(Message::Pong(payload)).await {
                                    break trace.reason("pong_send",error_class(&error),generation,None);
                                }
                            },
                            Some(Ok(Message::Close(frame)))=>break trace.reason("peer_close",ErrorClass::named("peer_close"),generation,frame.map(|frame|u16::from(frame.code))),
                            Some(Err(error))=>break trace.reason("read",error_class(&error),generation,None),
                            None=>break trace.reason("read",ErrorClass::named("stream_ended"),generation,None),
                            _=>{},
                        }}
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
        // Subscription-send failures also consume a generation and back off;
        // a following frame can never masquerade as the failed connection.
        gap_and_retry(&c, feed, slug.clone(), reason, &mut generation).await;
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
