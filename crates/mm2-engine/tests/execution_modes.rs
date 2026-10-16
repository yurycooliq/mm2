//! Offline integration. Every account endpoint and execution effect is an in-memory Venue.
use async_trait::async_trait;
use mm2_engine::{
    config::{Config, FeedSource, Limits},
    domain::{FeedEvent, Level, Market, Outcome},
    journal::Command,
    runtime::{self, Actor, Handle},
};
use mm2_live::{
    AccountObservation, LiveHandle, LiveOrder, LivePhase, LivePosition, LiveQuote, LiveRiskLimits,
    LiveRuntime, LiveSettings, OrderObservation, PostResult, PreparedOrder, Venue, VenueNotice,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
};

#[derive(Default)]
struct OfflineVenue {
    serial: AtomicU64,
    posts: AtomicU64,
    orders: Mutex<BTreeMap<String, OrderObservation>>,
    reconciliation_error: Mutex<Option<String>>,
}
#[async_trait]
impl Venue for OfflineVenue {
    async fn prepare(&self, quote: LiveQuote) -> Result<PreparedOrder, String> {
        let id = self.serial.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(PreparedOrder {
            order_id: format!("0x{id:064x}"),
            quote,
            signed_wire: serde_json::json!({"offline":true}),
            fee_policy: None,
        })
    }
    async fn post(&self, order: PreparedOrder) -> Result<PostResult, String> {
        self.posts.fetch_add(1, Ordering::Relaxed);
        self.orders.lock().await.insert(
            order.order_id.clone(),
            OrderObservation {
                order_id: order.order_id.clone(),
                condition_id: order.quote.condition_id,
                token_id: order.quote.token_id,
                price_micros: order.quote.price_micros,
                original_quantity_micros: order.quote.quantity_micros,
                cumulative_filled_micros: 0,
                terminal: false,
            },
        );
        Ok(PostResult::Accepted {
            order_id: order.order_id,
        })
    }
    async fn cancel(&self, ids: Vec<String>) -> Result<Vec<String>, String> {
        let mut orders = self.orders.lock().await;
        for id in &ids {
            if let Some(order) = orders.get_mut(id) {
                order.terminal = true;
            }
        }
        Ok(ids)
    }
    async fn reconcile(&self, _: Vec<LiveOrder>) -> Result<AccountObservation, String> {
        if let Some(reason) = self.reconciliation_error.lock().await.as_ref() {
            return Err(reason.clone());
        }
        Ok(AccountObservation {
            collateral_micros: 100_000_000,
            allowance_micros: 100_000_000,
            orders: self.orders.lock().await.values().cloned().collect(),
            positions: vec![LivePosition {
                condition_id: condition(),
                token_id: "1".into(),
                outcome: mm2_core::Outcome::Up,
                quantity_micros: 5_000_000,
                cost_pico: "2500000000000".into(),
                redeemable: false,
                mergeable: false,
            }],
            foreign_order_ids: vec![],
            execution_fees: vec![],
        })
    }
    async fn heartbeat(&self) -> Result<(), String> {
        Ok(())
    }
    async fn listen(&self, events: mpsc::Sender<VenueNotice>, mut stop: watch::Receiver<bool>) {
        let _ = events.send(VenueNotice::Connected(true)).await;
        while stop.changed().await.is_ok() {
            if *stop.borrow() {
                return;
            }
        }
    }
}
fn condition() -> String {
    format!("0x{}", "a".repeat(64))
}
fn settings() -> LiveSettings {
    let address = "0x1111111111111111111111111111111111111111";
    LiveSettings {
        owner_authorized: true,
        owner_address: address.into(),
        signer_address: address.into(),
        funder_address: address.into(),
        exchange_address: "0xE111180000d2663C0091e4f400237545B87B996B".into(),
        collateral_address: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB".into(),
        allowed_conditions: vec![condition()],
        risk: LiveRiskLimits {
            max_budget_micros: 100_000_000,
            max_gross_micros: 100_000_000,
            max_imbalance_micros: 25_000_000,
            max_order_micros: 10_000_000,
            max_orders: 8,
            max_markets: 8,
            daily_loss_limit_micros: 100_000_000,
        },
        quote_ttl_ms: 5000,
        request_timeout_ms: 500,
        heartbeat_interval_ms: 500,
        ..Default::default()
    }
}
fn config(path: &Path) -> Config {
    Config {
        mode: "both".into(),
        feed_source: FeedSource::Public,
        listen: "127.0.0.1:8088".parse().unwrap(),
        allow_remote_http: false,
        state_dir: path.into(),
        live_state_dir: Some(path.join("account-live")),
        journal_max_bytes: 8_388_608,
        journal: None,
        strategy: Default::default(),
        paper_policy: None,
        live: Some(settings()),
        command_capacity: 8,
        feed_capacity: 16,
        limits: Limits {
            initial_collateral_usd: "1000".into(),
            max_imbalance_shares: "25".into(),
            lot_shares: "5".into(),
            half_width: "0.045".into(),
            max_feed_age_ms: 1000,
        },
    }
}
fn control(id: &str, version: u64, action: &str) -> Command {
    Command::Control {
        id: id.into(),
        expected_version: version,
        action: action.into(),
    }
}
async fn wait(
    label: &str,
    mut predicate: impl FnMut() -> bool,
    diagnostic: impl Fn() -> serde_json::Value,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("{label}: {error}; public_snapshot={}", diagnostic()));
}
struct Service {
    handle: Handle,
    live: LiveHandle,
    paper: Handle,
    venue: Arc<OfflineVenue>,
    paper_fault: watch::Sender<Option<String>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    paper_task: Option<JoinHandle<()>>,
    delayed_paper: Option<Actor>,
    feeder: Option<JoinHandle<()>>,
    feeds: mpsc::Sender<FeedEvent>,
    paper_feeds: mpsc::Sender<FeedEvent>,
    start: Instant,
    _primary_fault: watch::Sender<Option<String>>,
}
fn public_snapshot(service: &Service) -> serde_json::Value {
    serde_json::json!({"primary":service.handle.state.borrow().as_ref(),"paper":service.paper.state.borrow().as_ref(),
        "live":service.live.snapshot().as_ref(),"mock_posts":service.venue.posts.load(Ordering::Relaxed)})
}
async fn launch(
    path: &Path,
    run_paper: bool,
    extra_paper_control: bool,
    venue: Arc<OfflineVenue>,
) -> Service {
    let config = config(path);
    let start = Instant::now();
    let (feeds, rx) = mpsc::channel(16);
    let (fault, fault_rx) = watch::channel(None);
    let (stop, shutdown) = watch::channel(false);
    let (mut handle, mut actor) =
        runtime::prepare(&config, start, rx, fault_rx, shutdown.clone()).unwrap();
    let mut paper_config = config.clone();
    paper_config.mode = "simulation".into();
    paper_config.live = None;
    paper_config.live_state_dir = None;
    paper_config.state_dir = path.join("paper");
    let (paper_feeds, paper_rx) = mpsc::channel(16);
    let (paper_fault, paper_fault_rx) = watch::channel(None);
    let (paper, paper_actor) =
        runtime::prepare(&paper_config, start, paper_rx, paper_fault_rx, shutdown).unwrap();
    let (paper_task, delayed_paper) = if run_paper {
        (Some(tokio::spawn(paper_actor.run())), None)
    } else {
        (None, Some(paper_actor))
    };
    if extra_paper_control {
        paper
            .command(control("paper-only", 0, "pause"))
            .await
            .unwrap();
    }
    handle.attach_paper(paper.clone(), paper_fault.clone());
    actor.attach_paper(paper.clone(), paper_fault.clone());
    let live =
        LiveRuntime::start_with_venue(settings(), config.live_state_dir.unwrap(), venue.clone())
            .await
            .unwrap();
    wait(
        "live startup readiness",
        || live.resume_ready(),
        || serde_json::to_value(live.snapshot().as_ref()).unwrap(),
    )
    .await;
    actor.attach_live(live.clone());
    Service {
        handle,
        live,
        paper,
        venue,
        paper_fault,
        stop,
        task: tokio::spawn(actor.run()),
        paper_task,
        delayed_paper,
        feeder: None,
        feeds,
        paper_feeds,
        start,
        _primary_fault: fault,
    }
}
async fn begin_feeds(service: &mut Service) {
    let now = runtime::wall_ms();
    let market = Market {
        slug: "fixture-market".into(),
        condition_id: condition(),
        up_token: "1".into(),
        down_token: "2".into(),
        window_start_ms: now - 1000,
        window_end_ms: now + 60_000,
        tick_micro: 10_000,
    };
    service.handle.select_market(market).await.unwrap();
    service.handle.fee_policy(Some(
        mm2_live::fees::parse_policy(
            &serde_json::json!({"c":condition(),
        "t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}}),
            &condition(),
            "1",
            "2",
            now,
        )
        .unwrap(),
    ));
    let feeds = service.feeds.clone();
    let paper_feeds = service.paper_feeds.clone();
    let start = service.start;
    service.feeder = Some(tokio::spawn(async move {
        loop {
            let wall = runtime::wall_ms();
            let observed = runtime::mono_ms(start);
            for outcome in [Outcome::Up, Outcome::Down] {
                let event = FeedEvent::Snapshot {
                    condition_id: condition(),
                    outcome,
                    exchange_ms: wall,
                    observed_mono_ms: observed,
                    bids: vec![Level {
                        price_micro: 490_000,
                        quantity_micro: 50_000_000,
                    }],
                    asks: vec![Level {
                        price_micro: 510_000,
                        quantity_micro: 50_000_000,
                    }],
                };
                let _ = paper_feeds.try_send(event.clone());
                if feeds.send(event).await.is_err() {
                    return;
                }
            }
            let reference = FeedEvent::Reference {
                exchange_ms: wall,
                observed_mono_ms: observed,
                price_micro: 100_000_000_000,
            };
            let _ = paper_feeds.try_send(reference.clone());
            if feeds.send(reference).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }));
}
async fn halt(mut service: Service) {
    service.stop.send_replace(true);
    if let Some(feeder) = service.feeder.take() {
        feeder.abort();
        let _ = feeder.await;
    }
    tokio::time::timeout(Duration::from_secs(15), service.task)
        .await
        .unwrap()
        .unwrap();
    if let Some(task) = service.paper_task.take() {
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
    }
    drop(service.delayed_paper.take());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_books_remain_ready_when_funded_eligibility_blocks_resume() {
    for reason in [
        "host/protocol eligibility is unverified or blocked",
        "geoblock endpoint transport unavailable",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let venue = Arc::new(OfflineVenue::default());
        let mut service = launch(dir.path(), true, false, venue.clone()).await;
        begin_feeds(&mut service).await;
        wait(
            "fresh public books in both independent lanes",
            || {
                service.handle.state.borrow()["feed_ready"] == true
                    && service.paper.state.borrow()["feed_ready"] == true
            },
            || public_snapshot(&service),
        )
        .await;
        *venue.reconciliation_error.lock().await = Some(reason.into());
        wait(
            "funded reconciliation fail-closed with public books available",
            || {
                !service.live.snapshot().account_reconciled
                    && service.handle.state.borrow()["live_eligible"] == false
            },
            || public_snapshot(&service),
        )
        .await;
        assert_eq!(service.handle.state.borrow()["feed_ready"], true);
        assert_eq!(service.paper.state.borrow()["feed_ready"], true);
        assert!(!service.live.resume_ready());
        assert_eq!(service.live.snapshot().phase, LivePhase::Paused);
        let denial = service
            .handle
            .command(control("blocked-funded-resume", 0, "resume"))
            .await
            .unwrap_err();
        assert_eq!(denial.kind, "conflict");
        assert!(denial.message.contains("live account"));
        assert_eq!(service.handle.state.borrow()["command_version"], 0);
        assert_eq!(service.handle.state.borrow()["mode"], "paused");
        assert_eq!(venue.posts.load(Ordering::Relaxed), 0);
        halt(service).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mirrored_cas_adapts_to_paper_version_and_secondary_fault_does_not_stop_live() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(OfflineVenue::default());
    let mut service = launch(dir.path(), true, true, venue).await;
    begin_feeds(&mut service).await;
    service
        .handle
        .command(control("owner-resume", 0, "resume"))
        .await
        .unwrap();
    wait(
        "paper mirrored Resume version",
        || service.paper.state.borrow()["command_version"] == 2,
        || public_snapshot(&service),
    )
    .await;
    wait(
        "primary post before secondary fault",
        || service.venue.posts.load(Ordering::Relaxed) > 0,
        || public_snapshot(&service),
    )
    .await;
    service
        .paper_fault
        .send_replace(Some("offline secondary failure".into()));
    wait(
        "secondary fault publication",
        || service.paper.state.borrow()["fault"].as_str().is_some(),
        || public_snapshot(&service),
    )
    .await;
    wait(
        "primary secondary-error publication",
        || {
            service.handle.state.borrow()["simulation_error"]
                .as_str()
                .is_some()
        },
        || public_snapshot(&service),
    )
    .await;
    assert_eq!(service.live.snapshot().phase, LivePhase::Running);
    assert_eq!(service.live.snapshot().collateral_micros, 100_000_000);
    assert_eq!(
        service.live.snapshot().positions[0].quantity_micros,
        5_000_000
    );
    assert_eq!(
        service.handle.state.borrow()["fault"],
        serde_json::Value::Null
    );
    service
        .handle
        .command(control("owner-pause", 1, "pause"))
        .await
        .unwrap();
    wait(
        "owner Pause terminal reconciliation",
        || service.live.snapshot().active_order_count == 0,
        || public_snapshot(&service),
    )
    .await;
    halt(service).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_control_and_quotes_do_not_wait_for_a_suspended_paper_actor() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(OfflineVenue::default());
    let mut service = launch(dir.path(), false, false, venue).await;
    begin_feeds(&mut service).await;
    // The unpolled paper actor cannot acknowledge anything. Use the owner's
    // bounded command contract; durable disk writes have no extra two-second SLA.
    let receipt = service
        .handle
        .command(control("resume-slow-paper", 0, "resume"))
        .await
        .unwrap();
    assert_eq!(receipt["receipt"]["applied_version"], 1);
    wait(
        "primary post with suspended paper",
        || service.venue.posts.load(Ordering::Relaxed) > 0,
        || public_snapshot(&service),
    )
    .await;
    assert_eq!(service.paper.state.borrow()["command_version"], 0);
    assert_eq!(service.live.snapshot().collateral_micros, 100_000_000);
    halt(service).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_parent_resume_never_reactivates_recovered_live_or_paper() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(OfflineVenue::default());
    // Leave the first secondary mirror queued, then recover its ledger without that Resume receipt.
    // Historical parent retries must not activate a secondary that missed the original command.
    let first = launch(dir.path(), false, false, venue.clone()).await;
    first
        .handle
        .command(control("original-resume", 0, "resume"))
        .await
        .unwrap();
    halt(first).await;
    let second = launch(dir.path(), true, false, venue).await;
    let replay = second
        .handle
        .command(control("original-resume", 0, "resume"))
        .await
        .unwrap();
    assert_eq!(replay["replayed"], true);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(second.live.snapshot().phase, LivePhase::Paused);
    assert_eq!(second.handle.state.borrow()["mode"], "paused");
    assert_eq!(second.paper.state.borrow()["mode"], "paused");
    halt(second).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn funded_profiles_cannot_relabel_initial_capital_as_a_live_risk_change() {
    for mode in ["trading", "both"] {
        let dir = tempfile::tempdir().unwrap();
        let mut profile = config(dir.path());
        profile.mode = mode.into();
        let (_feeds, input) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (stop, shutdown) = watch::channel(false);
        let (handle, actor) =
            runtime::prepare(&profile, Instant::now(), input, fault, shutdown).unwrap();
        let task = tokio::spawn(actor.run());
        let mut changed = profile.limits.clone();
        changed.initial_collateral_usd = "1".into();
        let error = handle
            .command(Command::Limits {
                id: format!("bad-capital-{mode}"),
                expected_version: 0,
                limits: changed,
            })
            .await
            .unwrap_err();
        assert!(error.message.contains("fixed at startup"));
        assert_eq!(handle.state.borrow()["command_version"], 0);
        let mut quote_settings = profile.limits.clone();
        quote_settings.half_width = "0.04".into();
        let result = handle
            .command(Command::Limits {
                id: format!("quotes-{mode}"),
                expected_version: 0,
                limits: quote_settings,
            })
            .await
            .unwrap();
        assert_eq!(result["receipt"]["applied_version"], 1);
        assert_eq!(
            handle.state.borrow()["limits"]["initial_collateral_usd"],
            "1000"
        );
        stop.send_replace(true);
        task.await.unwrap();
    }
}
