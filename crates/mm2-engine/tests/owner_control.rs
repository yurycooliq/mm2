use mm2_engine::{
    config::{Config, FeedSource, Limits},
    domain::{FeedEvent, Level, Market, Outcome},
    journal::Command,
    runtime::{self, Handle},
};
use std::time::{Duration, Instant};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

struct Service {
    handle: Handle,
    feeds: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    start: Instant,
}
fn config(path: &std::path::Path) -> Config {
    Config {
        mode: "shadow".into(),
        feed_source: FeedSource::Demo,
        listen: "127.0.0.1:8088".parse().unwrap(),
        allow_remote_http: false,
        state_dir: path.into(),
        live_state_dir: None,
        journal_max_bytes: 1_048_576,
        journal: None,
        strategy: Default::default(),
        live: None,
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
fn launch(path: &std::path::Path) -> Service {
    launch_source(path, FeedSource::Demo)
}
fn launch_source(path: &std::path::Path, source: FeedSource) -> Service {
    let (feeds, rx) = mpsc::channel(16);
    let (fault, fault_rx) = watch::channel(None);
    let (stop, shutdown) = watch::channel(false);
    let start = Instant::now();
    let mut config = config(path);
    config.feed_source = source;
    if source == FeedSource::Demo {
        config.mode = "emulator".into();
    }
    let (handle, actor) = runtime::prepare(&config, start, rx, fault_rx, shutdown).unwrap();
    Service {
        handle,
        feeds,
        fault,
        stop,
        task: tokio::spawn(actor.run()),
        start,
    }
}
async fn halt(service: Service) {
    service.stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), service.task)
        .await
        .unwrap()
        .unwrap();
}
fn control(id: &str, version: u64, action: &str) -> Command {
    Command::Control {
        id: id.into(),
        expected_version: version,
        action: action.into(),
    }
}
async fn market(service: &Service) -> Market {
    let now = runtime::wall_ms();
    let market = Market {
        slug: "fixture-market".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        up_token: "1".into(),
        down_token: "2".into(),
        window_start_ms: now - 1000,
        window_end_ms: now + 60_000,
        tick_micro: 10_000,
    };
    service.handle.select_market(market.clone()).await.unwrap();
    market
}
async fn snapshots(service: &Service, market: &Market, observed: u64) {
    for outcome in [Outcome::Up, Outcome::Down] {
        service
            .feeds
            .send(FeedEvent::Snapshot {
                condition_id: market.condition_id.clone(),
                outcome,
                exchange_ms: runtime::wall_ms(),
                observed_mono_ms: observed,
                bids: vec![Level {
                    price_micro: 490_000,
                    quantity_micro: 20_000_000,
                }],
                asks: vec![Level {
                    price_micro: 510_000,
                    quantity_micro: 20_000_000,
                }],
            })
            .await
            .unwrap();
    }
}
async fn wait(service: &Service, field: &str, value: serde_json::Value) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut state = service.handle.state.clone();
        loop {
            if state.borrow_and_update()[field] == value {
                break;
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_retries_have_one_durable_effect_and_conflicting_identity_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch(dir.path());
    let command = control("same", 0, "resume");
    let (first, second) = tokio::join!(
        service.handle.command(command.clone()),
        service.handle.command(command)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first["receipt"], second["receipt"]);
    assert_ne!(first["replayed"], second["replayed"]);
    assert_eq!(service.handle.state.borrow()["command_version"], 1);
    assert_eq!(
        service
            .handle
            .command(control("same", 0, "kill"))
            .await
            .unwrap_err()
            .kind,
        "conflict"
    );
    assert_eq!(
        service
            .handle
            .command(control("stale", 0, "pause"))
            .await
            .unwrap_err()
            .kind,
        "conflict"
    );
    halt(service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_preserves_receipts_and_limits_but_requires_resume_and_retains_kill() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch(dir.path());
    service
        .handle
        .command(control("resume", 0, "resume"))
        .await
        .unwrap();
    let mut limits = config(dir.path()).limits;
    limits.lot_shares = "6.0000000".into();
    service
        .handle
        .command(Command::Limits {
            id: "limits".into(),
            expected_version: 1,
            limits,
        })
        .await
        .unwrap();
    halt(service).await;
    let service = launch(dir.path());
    assert_eq!(service.handle.state.borrow()["mode"], "paused");
    assert_eq!(service.handle.state.borrow()["limits"]["lot_shares"], "6");
    let retry = service
        .handle
        .command(control("resume", 0, "resume"))
        .await
        .unwrap();
    assert_eq!(retry["receipt"]["mode_at_ack"], "shadow");
    assert_eq!(retry["state"]["mode"], "paused");
    assert_eq!(retry["state"]["command_version"], 2);
    service
        .handle
        .command(control("kill", 2, "kill"))
        .await
        .unwrap();
    halt(service).await;
    let service = launch(dir.path());
    assert_eq!(service.handle.state.borrow()["mode"], "killed");
    assert_eq!(
        service
            .handle
            .command(control("bad-resume", 3, "resume"))
            .await
            .unwrap_err()
            .kind,
        "conflict"
    );
    service
        .handle
        .command(control("reset", 3, "reset"))
        .await
        .unwrap();
    assert_eq!(service.handle.state.borrow()["mode"], "paused");
    halt(service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_rejects_pre_reset_observations_and_fault_cannot_be_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch(dir.path());
    let market = market(&service).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let old = runtime::mono_ms(service.start);
    snapshots(&service, &market, old).await;
    service
        .handle
        .command(control("resume", 0, "resume"))
        .await
        .unwrap();
    wait(&service, "shadow_ready", true.into()).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    service
        .handle
        .command(control("reset", 1, "reset"))
        .await
        .unwrap();
    service
        .handle
        .command(control("again", 2, "resume"))
        .await
        .unwrap();
    snapshots(&service, &market, old).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(service.handle.state.borrow()["feed_ready"], false);
    assert_eq!(
        service.handle.state.borrow()["shadow_intents"],
        serde_json::json!([])
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    snapshots(&service, &market, runtime::mono_ms(service.start)).await;
    wait(&service, "shadow_ready", true.into()).await;
    service
        .fault
        .send_replace(Some("fixture queue overflow".into()));
    wait(&service, "api_ready", false.into()).await;
    assert_eq!(service.handle.state.borrow()["mode"], "paused");
    assert_eq!(
        service.handle.state.borrow()["shadow_intents"],
        serde_json::json!([])
    );
    assert_eq!(
        service
            .handle
            .command(control("fault-resume", 3, "resume"))
            .await
            .unwrap_err()
            .kind,
        "unavailable"
    );
    halt(service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dependency_backoff_is_visible_and_recovery_does_not_create_orders() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch(dir.path());
    service
        .handle
        .source_status(
            "backoff",
            Some("public-source IP is blocked or eligibility is unknown".into()),
            Some(5000),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let state = service.handle.state.borrow().clone();
    assert_eq!(state["source_status"]["phase"], "backoff");
    assert_eq!(state["source_status"]["retry_in_ms"], 5000);
    assert!(
        state["source_status"]["message"]
            .as_str()
            .unwrap()
            .contains("blocked")
    );
    assert_eq!(state["live"], serde_json::Value::Null);
    assert_eq!(state["live_eligible"], false);
    assert!(state["wallet_balance_usd"].is_null());
    halt(service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_reset_waits_for_joined_source_refresh_even_when_old_socket_receipts_are_new() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch_source(dir.path(), FeedSource::Public);
    let selected = market(&service).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    snapshots(&service, &selected, runtime::mono_ms(service.start)).await;
    service
        .handle
        .command(control("resume", 0, "resume"))
        .await
        .unwrap();
    wait(&service, "shadow_ready", true.into()).await;
    service
        .handle
        .command(control("reset", 1, "reset"))
        .await
        .unwrap();
    service
        .handle
        .command(control("again", 2, "resume"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // The old socket is still delivering after the receive-time barrier.
    snapshots(&service, &selected, runtime::mono_ms(service.start)).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        service.handle.state.borrow()["awaiting_source_refresh"],
        true
    );
    assert_eq!(service.handle.state.borrow()["shadow_ready"], false);
    // Only the source coordinator's joined-generation acknowledgement clears this fence.
    service
        .handle
        .select_market(selected.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    snapshots(&service, &selected, runtime::mono_ms(service.start)).await;
    wait(&service, "shadow_ready", true.into()).await;
    halt(service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emulator_fills_are_durable_and_pause_keeps_reservations_until_cancel_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let service = launch(dir.path());
    assert_eq!(service.handle.state.borrow()["execution_mode"], "emulator");
    assert_eq!(service.handle.state.borrow()["mode"], "paused");
    let selected = market(&service).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    service
        .handle
        .command(control("paper-resume", 0, "resume"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            snapshots(&service, &selected, runtime::mono_ms(service.start)).await;
            if service.handle.state.borrow()["simulation"]["stats"]["fill_updates"]
                .as_u64()
                .unwrap_or(0)
                > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let paused = service
        .handle
        .command(control("paper-pause", 1, "pause"))
        .await
        .unwrap();
    assert!(
        paused["state"]["simulation"]["account"]["reserved_micros_ceil"]
            .as_u64()
            .unwrap()
            > 0
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service.handle.state.borrow()["simulation"]["account"]["reserved_micros_ceil"] == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let settled = service.handle.state.borrow().clone();
    let up = settled["simulation"]["account"]["up_inventory_micros"].clone();
    let down = settled["simulation"]["account"]["down_inventory_micros"].clone();
    assert!(up.as_u64().unwrap() + down.as_u64().unwrap() > 0);
    halt(service).await;
    let recovered = launch(dir.path());
    let state = recovered.handle.state.borrow().clone();
    assert_eq!(state["mode"], "paused");
    assert_eq!(state["simulation"]["account"]["up_inventory_micros"], up);
    assert_eq!(
        state["simulation"]["account"]["down_inventory_micros"],
        down
    );
    assert_eq!(state["simulation"]["account"]["reserved_micros_ceil"], 0);
    assert_eq!(state["live"], serde_json::Value::Null);
    assert_eq!(state["live_eligible"], false);
    let mut other = selected.clone();
    other.condition_id = format!("0x{}", "b".repeat(64));
    recovered.handle.select_market(other).await.unwrap();
    let rolled = recovered.handle.state.borrow().clone();
    assert_eq!(
        rolled["paper_portfolio"]["positions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(rolled["simulation"]["account"]["up_inventory_micros"], 0);
    assert_eq!(rolled["simulation"]["account"]["down_inventory_micros"], 0);
    let before = rolled["journal_bytes"].as_u64().unwrap();
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert_eq!(
        recovered.handle.state.borrow()["journal_bytes"],
        before,
        "paused idle ticks do not write execution records"
    );
    halt(recovered).await;
}
