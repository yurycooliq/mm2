use crate::*;
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify, mpsc, watch};

fn settings() -> LiveSettings {
    let account = "0x1111111111111111111111111111111111111111";
    let contract = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    LiveSettings {
        owner_authorized: true,
        owner_address: account.into(),
        signer_address: account.into(),
        funder_address: account.into(),
        exchange_address: format!("{:#x}", contract.exchange_v2.unwrap()),
        collateral_address: format!("{:#x}", contract.collateral),
        allowed_conditions: vec![format!("0x{}", "a".repeat(64))],
        risk: LiveRiskLimits {
            max_budget_micros: 100_000_000,
            max_gross_micros: 100_000_000,
            max_imbalance_micros: 10_000_000,
            max_order_micros: 5_000_000,
            max_orders: 8,
            max_markets: 8,
            daily_loss_limit_micros: 100_000_000,
        },
        request_timeout_ms: 100,
        heartbeat_interval_ms: 500,
        quote_ttl_ms: 5000,
        ..Default::default()
    }
}
fn quote() -> LiveQuote {
    let now = crate::sdk::now_ms();
    LiveQuote {
        slot: "up:lot".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        market_slug: "fixture".into(),
        token_id: "1".into(),
        outcome: mm2_core::Outcome::Up,
        price_micros: 490_000,
        quantity_micros: 5_000_000,
        window_start_ms: now.saturating_sub(10_000),
        window_end_ms: now + 200_000,
        tick_micros: 1000,
        min_order_micros: 5_000_000,
        fee_bps: 0,
        protocol: 2,
        neg_risk: false,
    }
}
#[derive(Default)]
struct MockVenue {
    serial: AtomicU64,
    posts: AtomicU64,
    cancels: AtomicU64,
    post_stall: AtomicBool,
    missing: AtomicBool,
    fill_on_post: AtomicU64,
    seen: Mutex<BTreeMap<String, OrderObservation>>,
    release: Notify,
    state_dir: Option<std::path::PathBuf>,
    durable_before_post: AtomicBool,
    foreign: AtomicBool,
    cancel_unconfirmed: AtomicBool,
    accepted_lost_ack: AtomicBool,
    normalize_fee: AtomicBool,
    account_position_micros: AtomicU64,
    notice_sender: Mutex<Option<mpsc::Sender<VenueNotice>>>,
}
#[async_trait]
impl Venue for MockVenue {
    async fn prepare(&self, mut quote: LiveQuote) -> Result<PreparedOrder, String> {
        if self.normalize_fee.load(Ordering::Relaxed) {
            quote.fee_bps = 0;
        }
        let n = self.serial.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(PreparedOrder {
            order_id: format!("0x{n:064x}"),
            quote,
            signed_wire: serde_json::json!({"test_only":true,"salt":n}),
        })
    }
    async fn post(&self, order: PreparedOrder) -> Result<PostResult, String> {
        self.posts.fetch_add(1, Ordering::Relaxed);
        if let Some(path) = &self.state_dir {
            let persisted = std::fs::read_dir(path)
                .unwrap()
                .filter_map(|entry| {
                    let entry = entry.unwrap();
                    (entry.path().extension().is_some_and(|e| e == "jsonl"))
                        .then(|| std::fs::read_to_string(entry.path()).unwrap())
                })
                .any(|body| {
                    body.contains(&order.order_id) && body.contains("signed_submission_intent")
                });
            self.durable_before_post.store(persisted, Ordering::Relaxed);
            assert!(persisted, "post preceded durable signed intent");
        }
        if self.missing.load(Ordering::Relaxed) {
            return Err("lost post acknowledgement and absent query".into());
        }
        self.seen.lock().await.insert(
            order.order_id.clone(),
            OrderObservation {
                order_id: order.order_id.clone(),
                condition_id: order.quote.condition_id,
                token_id: order.quote.token_id,
                price_micros: order.quote.price_micros,
                original_quantity_micros: order.quote.quantity_micros,
                cumulative_filled_micros: self.fill_on_post.load(Ordering::Relaxed),
                terminal: false,
            },
        );
        if self.post_stall.load(Ordering::Relaxed) {
            self.release.notified().await;
        }
        if self.accepted_lost_ack.load(Ordering::Relaxed) {
            return Err("accepted order acknowledgement lost".into());
        }
        Ok(PostResult::Accepted {
            order_id: order.order_id,
        })
    }
    async fn cancel(&self, ids: Vec<String>) -> Result<Vec<String>, String> {
        self.cancels.fetch_add(1, Ordering::Relaxed);
        let mut orders = self.seen.lock().await;
        let mut cancelled = vec![];
        for id in ids {
            if let Some(order) = orders.get_mut(&id) {
                if !self.cancel_unconfirmed.load(Ordering::Relaxed) {
                    order.terminal = true;
                }
                cancelled.push(id);
            }
        }
        Ok(cancelled)
    }
    async fn reconcile(&self, _: Vec<LiveOrder>) -> Result<AccountObservation, String> {
        Ok(AccountObservation {
            collateral_micros: 100_000_000,
            allowance_micros: u64::MAX,
            positions: if self.account_position_micros.load(Ordering::Relaxed) > 0 {
                vec![LivePosition {
                    condition_id: format!("0x{}", "a".repeat(64)),
                    token_id: "1".into(),
                    outcome: mm2_core::Outcome::Up,
                    quantity_micros: self.account_position_micros.load(Ordering::Relaxed),
                    cost_pico: (u128::from(self.account_position_micros.load(Ordering::Relaxed))
                        * 490_000)
                        .to_string(),
                    redeemable: false,
                    mergeable: false,
                }]
            } else {
                vec![]
            },
            orders: self.seen.lock().await.values().cloned().collect(),
            foreign_order_ids: if self.foreign.load(Ordering::Relaxed) {
                vec![format!("0x{}", "f".repeat(64))]
            } else {
                vec![]
            },
        })
    }
    async fn heartbeat(&self) -> Result<(), String> {
        Ok(())
    }
    async fn listen(&self, events: mpsc::Sender<VenueNotice>, mut shutdown: watch::Receiver<bool>) {
        *self.notice_sender.lock().await = Some(events.clone());
        events.send(VenueNotice::Connected(true)).await.unwrap();
        while shutdown.changed().await.is_ok() {
            if *shutdown.borrow() {
                break;
            }
        }
    }
}
async fn wait_for(handle: &LiveHandle, predicate: impl Fn(&LiveSnapshot) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if predicate(&handle.snapshot()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn unconfigured_live_and_wrong_deployment_funder_are_rejected() {
    assert!(LiveSettings::default().validate().is_err());
    let mut config = settings();
    config.validate().unwrap();
    config.protocol = 3;
    assert!(config.validate().is_err());
    config = settings();
    config.exchange_address = config.owner_address.clone();
    assert!(config.validate().is_err());
    config = settings();
    config.signature_type = 2;
    assert!(config.validate().is_err());
    config = settings();
    config.clob_url = "http://localhost:9".into();
    assert!(config.validate().is_err());
}
#[test]
fn final_minute_is_valid_gtc_and_market_family_is_exact() {
    let mut config = settings();
    let mut q = quote();
    let now = crate::sdk::now_ms();
    q.window_end_ms = now + 20_000;
    q.validate(&config, now).unwrap();
    config.allowed_conditions.clear();
    config.allowed_market_prefixes = vec!["btc-updown-5m-".into()];
    let start = now / 300_000 * 300_000;
    q.window_start_ms = start;
    q.window_end_ms = start + 300_000;
    q.market_slug = format!("btc-updown-5m-{}", start / 1000);
    q.validate(&config, start + 1000).unwrap();
    q.market_slug = format!("eth-updown-5m-{}", start / 1000);
    assert!(q.validate(&config, start + 1000).is_err());
}
#[test]
fn exact_decimal_units_do_not_fail_on_trailing_scale_or_accept_fractional_micros() {
    use polymarket_client_sdk_v2::types::Decimal;
    use std::str::FromStr;
    assert_eq!(
        crate::sdk::micro(Decimal::from_str("0.490000").unwrap()).unwrap(),
        490_000
    );
    assert!(crate::sdk::micro(Decimal::from_str("0.0000001").unwrap()).is_err());
}
#[test]
fn v2_maker_fee_uses_fd_curve_and_taker_only_is_free_for_post_only() {
    let maker: polymarket_client_sdk_v2::clob::types::response::FeeDetails =
        serde_json::from_str(r#"{"r":"0.25","e":2,"to":false}"#).unwrap();
    assert_eq!(crate::sdk::maker_fee_bound(&maker, 500_000).unwrap(), 313);
    let taker_only = serde_json::from_str(r#"{"r":"0.25","e":2,"to":true}"#).unwrap();
    assert_eq!(
        crate::sdk::maker_fee_bound(&taker_only, 500_000).unwrap(),
        0
    );
    let invalid = serde_json::from_str(r#"{"r":"0.25","e":50,"to":false}"#).unwrap();
    assert!(crate::sdk::maker_fee_bound(&invalid, 500_000).is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_quotes_and_account_reconciliation_do_not_grow_durable_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().into(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |state| state.can_resume).await;
    handle
        .command("steady-resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    let q = quote();
    handle
        .try_submit_quotes(vec![q.clone()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |state| {
        state
            .orders
            .iter()
            .any(|order| order.phase == OrderPhase::Live)
    })
    .await;
    // Allow the first authoritative reconciliation to converge its phase before measuring.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let bytes = handle.snapshot().journal_bytes;
    let sequence = handle.snapshot().durable_sequence;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        handle
            .try_submit_quotes(vec![q.clone()], crate::sdk::now_ms())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(handle.snapshot().journal_bytes, bytes);
    assert_eq!(handle.snapshot().durable_sequence, sequence);
    assert_eq!(venue.posts.load(Ordering::Relaxed), 1);
    handle.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ahead_portfolio_does_not_double_count_fills_and_preexisting_baseline_survives_lag() {
    for baseline in [0, 5_000_000] {
        let dir = tempfile::tempdir().unwrap();
        let venue = Arc::new(MockVenue::default());
        venue
            .account_position_micros
            .store(baseline, Ordering::Relaxed);
        let handle = LiveRuntime::start_with_venue(settings(), dir.path().into(), venue.clone())
            .await
            .unwrap();
        wait_for(&handle, |state| state.can_resume).await;
        handle
            .command("portfolio-resume".into(), ControlAction::Resume)
            .await
            .unwrap();
        let q = quote();
        handle
            .try_submit_quotes(vec![q], crate::sdk::now_ms())
            .unwrap();
        wait_for(&handle, |state| {
            state
                .orders
                .iter()
                .any(|order| order.phase == OrderPhase::Live)
        })
        .await;
        let id = handle.snapshot().orders[0].order_id.clone();
        let filled = 2_000_000;
        // Data API can lead the cumulative order endpoint on an empty baseline.
        if baseline == 0 {
            venue
                .account_position_micros
                .store(filled, Ordering::Relaxed);
            venue
                .notice_sender
                .lock()
                .await
                .as_ref()
                .unwrap()
                .send(VenueNotice::AccountChanged)
                .await
                .unwrap();
            wait_for(&handle, |state| {
                state
                    .positions
                    .first()
                    .is_some_and(|p| p.quantity_micros == filled)
            })
            .await;
        }
        venue
            .seen
            .lock()
            .await
            .get_mut(&id)
            .unwrap()
            .cumulative_filled_micros = filled;
        venue
            .notice_sender
            .lock()
            .await
            .as_ref()
            .unwrap()
            .send(VenueNotice::AccountChanged)
            .await
            .unwrap();
        wait_for(&handle, |state| {
            state.orders[0].cumulative_filled_micros == filled
        })
        .await;
        assert_eq!(
            handle.snapshot().positions[0].quantity_micros,
            baseline + filled
        );
        // On the nonzero baseline the account snapshot still lags at 5; the known floor is 7.
        handle.shutdown().await.unwrap();
        let recovered = LiveRuntime::start_with_venue(settings(), dir.path().into(), venue)
            .await
            .unwrap();
        wait_for(&recovered, |state| state.can_resume).await;
        assert_eq!(
            recovered.snapshot().positions[0].quantity_micros,
            baseline + filled
        );
        recovered.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalized_maker_fee_can_admit_both_sides_within_approved_daily_cap() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.normalize_fee.store(true, Ordering::Relaxed);
    let mut config = settings();
    config.risk.daily_loss_limit_micros = 5_000_000;
    config.risk.max_imbalance_micros = 5_000_000;
    let handle = LiveRuntime::start_with_venue(config, dir.path().into(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |state| state.can_resume).await;
    handle
        .command("normalized-resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    let mut up = quote();
    up.fee_bps = 10_000;
    let mut down = up.clone();
    down.slot = "down:lot".into();
    down.token_id = "2".into();
    down.outcome = mm2_core::Outcome::Down;
    handle
        .try_submit_quotes(vec![up, down], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |state| {
        state
            .orders
            .iter()
            .filter(|order| order.phase == OrderPhase::Live)
            .count()
            == 2
    })
    .await;
    assert_eq!(venue.posts.load(Ordering::Relaxed), 2);
    assert_eq!(handle.snapshot().reserved_pico, "4900000000000");
    handle.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn impossible_overweight_slot_does_not_starve_the_admissible_hedge() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.normalize_fee.store(true, Ordering::Relaxed);
    venue
        .account_position_micros
        .store(5_000_000, Ordering::Relaxed);
    let mut config = settings();
    config.risk.max_imbalance_micros = 5_000_000;
    config.risk.daily_loss_limit_micros = 5_000_000;
    let handle = LiveRuntime::start_with_venue(config, dir.path().into(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |state| state.can_resume).await;
    handle
        .command("hedge-resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    let mut up = quote();
    up.slot = "0-overweight-up".into();
    up.fee_bps = 10_000;
    let mut down = up.clone();
    down.slot = "1-hedge-down".into();
    down.token_id = "2".into();
    down.outcome = mm2_core::Outcome::Down;
    handle
        .try_submit_quotes(vec![up, down], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |state| {
        state
            .orders
            .iter()
            .any(|order| order.phase == OrderPhase::Live)
    })
    .await;
    assert_eq!(venue.posts.load(Ordering::Relaxed), 1);
    assert_eq!(
        handle.snapshot().orders[0].quote.outcome,
        mm2_core::Outcome::Down
    );
    handle.shutdown().await.unwrap();
}
fn query_fixture() -> Vec<LiveOrder> {
    (1..=16)
        .map(|id| LiveOrder {
            order_id: format!("0x{id:064x}"),
            quote: quote(),
            phase: OrderPhase::UnknownCancel,
            cumulative_filled_micros: 0,
            created_at_ms: 0,
            last_operation_ms: 0,
            retry_at_ms: 0,
        })
        .collect()
}
fn query_observation(order: &LiveOrder) -> OrderObservation {
    OrderObservation {
        order_id: order.order_id.clone(),
        condition_id: order.quote.condition_id.clone(),
        token_id: order.quote.token_id.clone(),
        price_micros: order.quote.price_micros,
        original_quantity_micros: order.quote.quantity_micros,
        cumulative_filled_micros: 0,
        terminal: false,
    }
}
async fn wait_counter(counter: &AtomicU64, value: u64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::SeqCst) < value {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sixteen_delayed_active_queries_use_two_waves_of_eight_and_one_complete_result() {
    let orders = query_fixture();
    let expected: Arc<Vec<_>> = Arc::new(orders.iter().map(query_observation).collect());
    let started = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));
    let (release, allowed) = watch::channel(0u64);
    let (s, c, active, high) = (
        started.clone(),
        completed.clone(),
        in_flight.clone(),
        peak.clone(),
    );
    let task = tokio::spawn(async move {
        crate::sdk::query_active_orders(&orders, |id| {
            let (s, c, active, high, expected) = (
                s.clone(),
                c.clone(),
                active.clone(),
                high.clone(),
                expected.clone(),
            );
            let mut allowed = allowed.clone();
            async move {
                let wave = s.fetch_add(1, Ordering::SeqCst) / 8 + 1;
                high.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                while *allowed.borrow() < wave {
                    allowed
                        .changed()
                        .await
                        .map_err(|_| "offline delay gate stopped".to_owned())?;
                }
                active.fetch_sub(1, Ordering::SeqCst);
                c.fetch_add(1, Ordering::SeqCst);
                Ok(Some(
                    expected
                        .iter()
                        .find(|obs| obs.order_id == id)
                        .unwrap()
                        .clone(),
                ))
            }
        })
        .await
    });
    wait_counter(&started, 8).await;
    assert_eq!(started.load(Ordering::SeqCst), 8);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    release.send_replace(1);
    wait_counter(&started, 16).await;
    assert_eq!(completed.load(Ordering::SeqCst), 8);
    assert!(!task.is_finished());
    release.send_replace(2);
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.len(), 16);
    assert_eq!(peak.load(Ordering::SeqCst), 8);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_active_query_discards_partial_success_and_missing_orders_remain_unknown() {
    let orders = query_fixture();
    let observations: Arc<Vec<_>> = Arc::new(orders.iter().map(query_observation).collect());
    let completed = AtomicU64::new(0);
    let result = crate::sdk::query_active_orders(&orders, |id| {
        let observations = observations.clone();
        let completed = &completed;
        async move {
            if id.ends_with('2') {
                tokio::time::sleep(Duration::from_millis(20)).await;
                return Err("offline authoritative query failed".into());
            }
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(Some(
                observations
                    .iter()
                    .find(|obs| obs.order_id == id)
                    .unwrap()
                    .clone(),
            ))
        }
    })
    .await;
    assert!(completed.load(Ordering::SeqCst) > 0);
    assert!(result.is_err());
    let observations: Arc<Vec<_>> = Arc::new(orders.iter().map(query_observation).collect());
    let missing = crate::sdk::query_active_orders(&orders, |id| {
        let observations = observations.clone();
        async move {
            if id.ends_with('1') {
                return Ok(None);
            }
            Ok(Some(
                observations
                    .iter()
                    .find(|obs| obs.order_id == id)
                    .unwrap()
                    .clone(),
            ))
        }
    })
    .await
    .unwrap();
    assert_eq!(missing.len(), 15);
    assert_eq!(orders[0].phase, OrderPhase::UnknownCancel);
    assert!(orders[0].reserve_pico() > 0);
    let wrong = crate::sdk::query_active_orders(&orders, |_| async {
        Ok(Some(query_observation(&query_fixture()[1])))
    })
    .await;
    assert!(wrong.is_err());
}
#[test]
fn terminal_regression_and_late_extra_fill_do_not_free_unknown_reservations() {
    use crate::model::Ledger;
    let q = quote();
    let id = format!("0x{}", "1".repeat(64));
    let mut ledger = Ledger::default();
    ledger.orders.insert(
        id.clone(),
        LiveOrder {
            order_id: id.clone(),
            quote: q.clone(),
            phase: OrderPhase::UnknownCancel,
            cumulative_filled_micros: 0,
            created_at_ms: 0,
            last_operation_ms: 0,
            retry_at_ms: 0,
        },
    );
    let mut obs = OrderObservation {
        order_id: id.clone(),
        condition_id: q.condition_id,
        token_id: q.token_id,
        price_micros: q.price_micros,
        original_quantity_micros: q.quantity_micros,
        cumulative_filled_micros: 2_000_000,
        terminal: false,
    };
    ledger.observe(&obs).unwrap();
    assert_eq!(ledger.orders[&id].phase, OrderPhase::UnknownCancel);
    assert_eq!(ledger.reserved_pico(), 1_470_000_000_000);
    obs.terminal = true;
    obs.cumulative_filled_micros = 1_000_000;
    assert!(ledger.observe(&obs).is_err());
    obs.cumulative_filled_micros = 2_000_000;
    ledger.observe(&obs).unwrap();
    assert_eq!(ledger.reserved_pico(), 0);
    obs.cumulative_filled_micros = 3_000_000;
    assert!(ledger.observe(&obs).is_err());
}
#[test]
fn hash_consistent_corrupt_quote_is_rejected_but_expired_unknown_survives_tighter_policy() {
    let id = format!("0x{}", "1".repeat(64));
    let config = settings();
    let mut ledger = crate::model::Ledger::default();
    let mut q = quote();
    q.window_start_ms = 1000;
    q.window_end_ms = 301_000;
    ledger.orders.insert(
        id.clone(),
        LiveOrder {
            order_id: id.clone(),
            quote: q,
            phase: OrderPhase::UnknownCancel,
            cumulative_filled_micros: 0,
            created_at_ms: 2000,
            last_operation_ms: 2000,
            retry_at_ms: 0,
        },
    );
    ledger
        .signed_intents
        .insert(id.clone(), serde_json::json!({"offline":true}));
    ledger.position_floor.insert(
        format!("{}:1", ledger.orders[&id].quote.condition_id),
        LivePosition {
            condition_id: ledger.orders[&id].quote.condition_id.clone(),
            token_id: "1".into(),
            outcome: mm2_core::Outcome::Up,
            quantity_micros: 0,
            cost_pico: "0".into(),
            redeemable: false,
            mergeable: false,
        },
    );
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = crate::journal::Journal::open(dir.path(), &config).unwrap();
    journal.commit("expired_unknown_fixture", &ledger).unwrap();
    drop(journal);
    let mut tighter = config.clone();
    tighter.risk.max_order_micros = 1_000_000;
    let (journal, restored) = crate::journal::Journal::open(dir.path(), &tighter).unwrap();
    assert_eq!(restored.orders[&id].phase, OrderPhase::UnknownCancel);
    assert!(restored.reserved_pico() > 0);
    drop(journal);
    let bad_dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = crate::journal::Journal::open(bad_dir.path(), &config).unwrap();
    ledger.orders.get_mut(&id).unwrap().quote.price_micros = u64::MAX;
    // Writer computes the valid chain hash: replay must enforce semantics as well as hashes.
    journal
        .commit("hash_consistent_invalid_quote", &ledger)
        .unwrap();
    drop(journal);
    assert!(crate::journal::Journal::open(bad_dir.path(), &config).is_err());
}
fn user_stream_timing() -> crate::sdk::UserStreamTiming {
    crate::sdk::UserStreamTiming {
        request: Duration::from_millis(250),
        retry: Duration::from_millis(50),
        ping: Duration::from_millis(30),
        pong_timeout: Duration::from_millis(120),
    }
}
fn fixture_user_subscription() -> String {
    use polymarket_client_sdk_v2::{
        auth::{Credentials, Uuid},
        clob::ws::types::request::SubscriptionRequest,
        ws::WithCredentials,
    };
    SubscriptionRequest::user(vec![])
        .as_authenticated(&Credentials::new(
            Uuid::from_u128(1),
            "fixture-only-secret".into(),
            "fixture-only-pass".into(),
        ))
        .unwrap()
}
#[test]
fn private_messages_are_bounded_and_use_sdk_user_schemas() {
    let order = serde_json::json!({"event_type":"order","id":"fixture-order","market":format!("0x{}","a".repeat(64)),"asset_id":"1","side":"BUY","price":"0.49"});
    let notices = crate::sdk::user_notices(&order.to_string()).unwrap();
    assert!(matches!(&notices[0], VenueNotice::OrderChanged(id) if id == "fixture-order"));
    let batch = vec![order.clone(); 128];
    assert_eq!(
        crate::sdk::user_notices(&serde_json::to_string(&batch).unwrap())
            .unwrap()
            .len(),
        128
    );
    assert!(crate::sdk::user_notices(&serde_json::to_string(&vec![order; 129]).unwrap()).is_err());
    assert!(crate::sdk::user_notices("{invalid").is_err());
    assert!(crate::sdk::user_notices("{\"event_type\":\"unexpected\"}").is_err());
    assert!(crate::sdk::user_notices(&" ".repeat(65_537)).is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_socket_shutdown_closes_the_owned_connection_and_does_not_reconnect() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (events, mut notices) = mpsc::channel(16);
    let (stop, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
        let payload: serde_json::Value = serde_json::from_str(subscription.as_str()).unwrap();
        assert_eq!(payload["type"], "user");
        assert_eq!(payload["markets"], serde_json::json!([]));
        assert_eq!(payload["auth"]["secret"], "fixture-only-secret");
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) if text.as_str() == "PING" => {
                    socket.send(Message::Text("PONG".into())).await.unwrap()
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            }
        }
        let _ = socket.close(None).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "shutdown must not open another connection"
        );
    });
    let client = tokio::spawn(async move {
        crate::sdk::listen_user_socket(
            &endpoint,
            fixture_user_subscription(),
            events,
            shutdown,
            user_stream_timing(),
        )
        .await
    });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), notices.recv())
            .await
            .unwrap(),
        Some(VenueNotice::Connected(true))
    ));
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), client)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_socket_silent_peer_disconnects_then_reconnects_with_a_new_subscription() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (events, mut notices) = mpsc::channel(16);
    let (stop, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let payload: serde_json::Value = serde_json::from_str(subscription.as_str()).unwrap();
            assert_eq!(payload["type"], "user");
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) if attempt == 1 && text.as_str() == "PING" => {
                        socket.send(Message::Text("PONG".into())).await.unwrap()
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
            let _ = socket.close(None).await;
        }
    });
    let client = tokio::spawn(async move {
        crate::sdk::listen_user_socket(
            &endpoint,
            fixture_user_subscription(),
            events,
            shutdown,
            user_stream_timing(),
        )
        .await
    });
    for expected in [true, false, true] {
        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(2), notices.recv()).await.unwrap(), Some(VenueNotice::Connected(actual)) if actual == expected)
        );
    }
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), client)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
#[test]
fn live_ledger_detects_partial_records_and_fences_second_executor() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let config = settings();
    let (mut journal, ledger) = crate::journal::Journal::open(dir.path(), &config).unwrap();
    journal.commit("fixture", &ledger).unwrap();
    assert!(crate::journal::Journal::open(dir.path(), &config).is_err());
    drop(journal);
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .unwrap();
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(b"{broken").unwrap();
    assert!(crate::journal::Journal::open(dir.path(), &config).is_err());
}
#[test]
fn failed_fsync_poison_prevents_duplicate_sequence_until_checked_replay() {
    let dir = tempfile::tempdir().unwrap();
    let config = settings();
    let (mut journal, mut ledger) = crate::journal::Journal::open(dir.path(), &config).unwrap();
    journal.commit("initial", &ledger).unwrap();
    let initial_sequence = journal.sequence;
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .unwrap();
    let initial_bytes = std::fs::metadata(&path).unwrap().len();
    ledger.phase = LivePhase::Running;
    journal.fail_next_sync();
    assert!(journal.commit("owner_resume_unknown", &ledger).is_err());
    assert!(journal.is_poisoned());
    assert_eq!(journal.sequence, initial_sequence);
    let unknown_bytes = std::fs::metadata(&path).unwrap().len();
    assert!(
        unknown_bytes > initial_bytes,
        "fsync failure can follow a complete write"
    );
    ledger.phase = LivePhase::Paused;
    assert!(
        journal
            .commit("owner_reset_must_not_reuse_sequence", &ledger)
            .is_err()
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), unknown_bytes);
    drop(journal);
    // Only startup replay may resolve a complete record whose acknowledgement was lost.
    let (recovered, restored) = crate::journal::Journal::open(dir.path(), &config).unwrap();
    assert!(!recovered.is_poisoned());
    assert_eq!(recovered.sequence, initial_sequence + 1);
    assert_eq!(restored.phase, LivePhase::Running);
}
#[tokio::test]
async fn signed_intent_is_durable_before_post_pause_preserves_fills_and_duplicate_control() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue {
        state_dir: Some(dir.path().to_owned()),
        ..Default::default()
    });
    venue.fill_on_post.store(2_000_000, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    let receipt = handle
        .command("resume-1".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| {
        !s.orders.is_empty() && s.orders[0].cumulative_filled_micros == 2_000_000
    })
    .await;
    assert!(venue.durable_before_post.load(Ordering::Relaxed));
    assert_eq!(
        handle
            .command("resume-1".into(), ControlAction::Resume)
            .await
            .unwrap(),
        receipt
    );
    assert!(
        handle
            .command("resume-1".into(), ControlAction::Kill)
            .await
            .is_err()
    );
    handle
        .command("pause-1".into(), ControlAction::Pause)
        .await
        .unwrap();
    wait_for(&handle, |s| s.reserved_pico == "0").await;
    assert_eq!(handle.snapshot().positions[0].quantity_micros, 2_000_000);
    assert_eq!(handle.snapshot().session_spent_pico, "980000000000");
    handle.shutdown().await.unwrap();
    let restarted = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&restarted, |s| s.account_reconciled).await;
    assert_eq!(restarted.snapshot().phase, LivePhase::Paused);
    restarted
        .command("resume-1".into(), ControlAction::Resume)
        .await
        .unwrap();
    assert_eq!(
        restarted.snapshot().phase,
        LivePhase::Paused,
        "old receipt must not resume recovered runtime"
    );
    assert_eq!(venue.posts.load(Ordering::Relaxed), 1);
    restarted.shutdown().await.unwrap();
}
#[tokio::test]
async fn slow_post_does_not_occupy_cancel_capacity_and_cancel_query_owns_terminal_fill() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = settings();
    config.request_timeout_ms = 2000;
    let venue = Arc::new(MockVenue::default());
    venue.post_stall.store(true, Ordering::Relaxed);
    venue.fill_on_post.store(2_000_000, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(config, dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| !s.orders.is_empty()).await;
    handle
        .command("pause".into(), ControlAction::Pause)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(1300), async {
        while venue.cancels.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    wait_for(&handle, |s| {
        s.reserved_pico == "0" && s.orders[0].cumulative_filled_micros == 2_000_000
    })
    .await;
    venue.release.notify_one();
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn absent_unknown_post_keeps_full_reserve_and_cannot_be_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.missing.store(true, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| {
        s.orders
            .first()
            .is_some_and(|o| matches!(o.phase, OrderPhase::UnknownPost | OrderPhase::UnknownCancel))
    })
    .await;
    assert_eq!(handle.snapshot().reserved_pico, "2450000000000");
    handle
        .command("kill".into(), ControlAction::Kill)
        .await
        .unwrap();
    assert!(
        handle
            .command("resume-again".into(), ControlAction::Resume)
            .await
            .is_err()
    );
    assert_eq!(venue.posts.load(Ordering::Relaxed), 1);
    handle.shutdown().await.unwrap();
    let restarted = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    assert_eq!(restarted.snapshot().phase, LivePhase::Killed);
    assert_eq!(restarted.snapshot().reserved_pico, "2450000000000");
    assert_eq!(
        venue.posts.load(Ordering::Relaxed),
        1,
        "startup must never resend an unknown post"
    );
    // Tokio test runtime cancellation simulates abrupt process loss; the durable intent remains.
}
#[tokio::test]
async fn foreign_account_orders_block_initial_resume() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.foreign.store(true, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue)
        .await
        .unwrap();
    wait_for(&handle, |s| s.account_reconciled).await;
    assert!(!handle.snapshot().can_resume);
    assert!(
        handle
            .command("resume".into(), ControlAction::Resume)
            .await
            .is_err()
    );
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancel_ack_alone_retains_partial_fill_reservation_until_authoritative_query() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.fill_on_post.store(2_000_000, Ordering::Relaxed);
    venue.cancel_unconfirmed.store(true, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| {
        s.orders
            .first()
            .is_some_and(|o| o.cumulative_filled_micros == 2_000_000)
    })
    .await;
    handle
        .command("pause".into(), ControlAction::Pause)
        .await
        .unwrap();
    wait_for(&handle, |s| s.orders[0].phase == OrderPhase::UnknownCancel).await;
    assert_eq!(handle.snapshot().reserved_pico, "1470000000000");
    assert!(!handle.snapshot().can_resume);
    venue.cancel_unconfirmed.store(false, Ordering::Relaxed);
    wait_for(&handle, |s| s.reserved_pico == "0").await;
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn stream_disconnect_cancels_and_reconnect_requires_a_new_owner_resume() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| !s.orders.is_empty()).await;
    venue
        .notice_sender
        .lock()
        .await
        .as_ref()
        .unwrap()
        .send(VenueNotice::Connected(false))
        .await
        .unwrap();
    wait_for(&handle, |s| {
        s.phase == LivePhase::Paused && s.reserved_pico == "0"
    })
    .await;
    assert!(
        handle
            .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
            .is_err()
    );
    venue
        .notice_sender
        .lock()
        .await
        .as_ref()
        .unwrap()
        .send(VenueNotice::Connected(true))
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    assert_eq!(handle.snapshot().phase, LivePhase::Paused);
    handle
        .command("fresh-resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    assert_eq!(handle.snapshot().phase, LivePhase::Running);
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn stale_quote_generation_cancels_without_console_and_possible_fill_caps_do_not_net() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    let mut config = settings();
    config.quote_ttl_ms = 250;
    config.risk.max_imbalance_micros = 5_000_000;
    let handle = LiveRuntime::start_with_venue(config, dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    let up = quote();
    let mut other_up = up.clone();
    other_up.slot = "up:extra".into();
    let mut down = up.clone();
    down.slot = "down:lot".into();
    down.outcome = mm2_core::Outcome::Down;
    down.token_id = "2".into();
    handle
        .try_submit_quotes(vec![up, other_up, down], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| !s.orders.is_empty()).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    wait_for(&handle, |s| s.reserved_pico == "0").await;
    assert!(
        venue.posts.load(Ordering::Relaxed) <= 2,
        "opposite-side commitment must not enable the second up quote"
    );
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn accepted_lost_ack_reconciles_same_id_and_does_not_resend_or_forget_fill() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    venue.accepted_lost_ack.store(true, Ordering::Relaxed);
    venue.fill_on_post.store(2_000_000, Ordering::Relaxed);
    let handle = LiveRuntime::start_with_venue(settings(), dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    handle
        .try_submit_quotes(vec![quote()], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| {
        s.orders.first().is_some_and(|o| {
            o.cumulative_filled_micros == 2_000_000 && o.phase == OrderPhase::Closed
        })
    })
    .await;
    assert_eq!(venue.posts.load(Ordering::Relaxed), 1);
    assert_eq!(handle.snapshot().reserved_pico, "0");
    assert_eq!(handle.snapshot().positions[0].quantity_micros, 2_000_000);
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn opposite_quotes_never_net_away_a_second_same_outcome_possible_fill() {
    let dir = tempfile::tempdir().unwrap();
    let venue = Arc::new(MockVenue::default());
    let mut config = settings();
    config.risk.max_imbalance_micros = 5_000_000;
    let handle = LiveRuntime::start_with_venue(config, dir.path().to_owned(), venue.clone())
        .await
        .unwrap();
    wait_for(&handle, |s| s.can_resume).await;
    handle
        .command("resume".into(), ControlAction::Resume)
        .await
        .unwrap();
    let up = quote();
    let mut other_up = up.clone();
    other_up.slot = "up:extra".into();
    let mut down = up.clone();
    down.slot = "down:lot".into();
    down.outcome = mm2_core::Outcome::Down;
    down.token_id = "2".into();
    handle
        .try_submit_quotes(vec![up, other_up, down], crate::sdk::now_ms())
        .unwrap();
    wait_for(&handle, |s| {
        s.orders
            .iter()
            .filter(|o| o.phase == OrderPhase::Live)
            .count()
            == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(venue.posts.load(Ordering::Relaxed), 2);
    handle
        .command("pause".into(), ControlAction::Pause)
        .await
        .unwrap();
    wait_for(&handle, |s| s.reserved_pico == "0").await;
    handle.shutdown().await.unwrap();
}
#[tokio::test]
async fn sdk_offline_signature_hash_and_wire_bind_the_configured_exchange_and_exact_units() {
    use alloy::{
        dyn_abi::Eip712Domain,
        signers::{Signer, local::PrivateKeySigner},
        sol_types::SolStruct,
    };
    use polymarket_client_sdk_v2::{
        auth::Uuid,
        clob::types::{OrderPayload, OrderPayloadV2, OrderType, SignedOrder},
        types::{Address, U256},
    };
    use std::str::FromStr;
    // Public, intentionally insecure fixture key 1; never an account credential or network signer.
    let signer = PrivateKeySigner::from_str(&format!("0x{:064x}", 1)).unwrap();
    let contract = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let mut payload = OrderPayloadV2::default();
    payload.order.salt = U256::from(1);
    payload.order.maker = signer.address();
    payload.order.signer = signer.address();
    payload.order.tokenId = U256::from(123);
    payload.order.makerAmount = U256::from(2_450_000);
    payload.order.takerAmount = U256::from(5_000_000);
    payload.order.side = 0;
    payload.order.signatureType = 0;
    payload.order.timestamp = U256::from(1_770_000_000_000u64);
    let domain = Eip712Domain {
        name: Some("Polymarket CTF Exchange".into()),
        version: Some("2".into()),
        chain_id: Some(U256::from(137)),
        verifying_contract: contract.exchange_v2,
        ..Default::default()
    };
    let id = payload.order.eip712_signing_hash(&domain);
    let mut wrong = domain.clone();
    wrong.verifying_contract = Some(Address::ZERO);
    assert_ne!(id, payload.order.eip712_signing_hash(&wrong));
    let signature = signer.sign_hash(&id).await.unwrap();
    assert_eq!(
        signature.recover_address_from_prehash(&id).unwrap(),
        signer.address()
    );
    let order = SignedOrder::builder()
        .payload(OrderPayload::V2(payload))
        .signature(signature)
        .order_type(OrderType::GTC)
        .owner(Uuid::nil())
        .post_only(true)
        .defer_exec(false)
        .build();
    let wire = serde_json::to_value(order).unwrap();
    assert_eq!(wire["order"]["makerAmount"], "2450000");
    assert_eq!(wire["order"]["takerAmount"], "5000000");
    assert_eq!(wire["order"]["side"], "BUY");
    assert_eq!(wire["postOnly"], true);
    assert_eq!(wire["orderType"], "GTC");
}
