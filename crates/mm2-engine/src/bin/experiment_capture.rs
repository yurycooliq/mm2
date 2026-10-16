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
    tungstenite::{Message, protocol::WebSocketConfig},
};

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
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            connect_async_with_config(url, Some(config), false),
        )
        .await;
        if let Ok(Ok((mut ws, _))) = result {
            if let Some(m) = &market {
                let subscription = json!({"assets_ids":[m.up_token,m.down_token],"type":"market","custom_feature_enabled":true});
                if ws
                    .send(Message::Text(subscription.to_string().into()))
                    .await
                    .is_err()
                {
                    emit(
                        &c,
                        tape::Input::Gap {
                            feed: feed.into(),
                            market_slug: slug.clone(),
                            reason: "subscription_failed".into(),
                        },
                    );
                    continue;
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
                    _=heartbeat.tick()=>{if market.is_some() && ws.send(Message::Text("PING".into())).await.is_err(){break;}},
                    msg=ws.next()=>{match msg {
                        Some(Ok(Message::Text(text)))=>{if !emit(&c,tape::Input::Frame{feed:feed.into(),market_slug:slug.clone(),generation,text:text.to_string()}){return Ok(());}},
                        Some(Ok(Message::Ping(p)))=>{if ws.send(Message::Pong(p)).await.is_err(){break;}},
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }}
                }
            }
            emit(
                &c,
                tape::Input::Gap {
                    feed: feed.into(),
                    market_slug: slug.clone(),
                    reason: "socket_disconnected".into(),
                },
            );
        } else {
            emit(
                &c,
                tape::Input::Gap {
                    feed: feed.into(),
                    market_slug: slug.clone(),
                    reason: "socket_connect_failed".into(),
                },
            );
        }
        generation += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
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
