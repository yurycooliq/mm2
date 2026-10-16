//! Offline actor regression: feed ticks reach the durable paper execution contract.
use super::TestClock;
use crate::{
    config::{Config, FeedSource, Limits},
    domain::{FeedEvent, Level, Market, Outcome},
    journal::{Command, Journal, JournalOptions},
    runtime::{self, Handle},
    simulation::{self, Operation},
};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

fn config(directory: &Path) -> Config {
    Config {
        mode: "simulation".into(),
        feed_source: FeedSource::Public,
        listen: "127.0.0.1:8088".parse().unwrap(),
        allow_remote_http: false,
        state_dir: directory.into(),
        live_state_dir: None,
        journal_max_bytes: 8_388_608,
        journal: None,
        strategy: Default::default(),
        paper_policy: None,
        live: None,
        command_capacity: 8,
        feed_capacity: 32,
        limits: Limits {
            initial_collateral_usd: "100".into(),
            max_imbalance_shares: "10".into(),
            lot_shares: "5".into(),
            half_width: "0.01".into(),
            max_feed_age_ms: 1000,
        },
    }
}

async fn live_orders(handle: &Handle, tick: u64) -> mm2_emulator::SessionSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = handle.state.borrow().clone();
            assert!(state["fault"].is_null(), "actor fault: {}", state["fault"]);
            assert!(
                state["simulation_error"].is_null(),
                "paper rejected a legal quote: {}",
                state["simulation_error"]
            );
            let snapshot: mm2_emulator::SessionSnapshot =
                serde_json::from_value(state["simulation"].clone()).unwrap();
            if snapshot.orders.len() == 2
                && snapshot.orders.iter().all(|order| {
                    order.phase == mm2_core::OrderPhase::Live
                        && order.quote.price_micros.is_multiple_of(tick)
                })
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        let state = handle.state.borrow();
        let details = serde_json::json!({
            "mode":state["mode"],"stop_reasons":state["stop_reasons"],
            "strategy_gate":state["strategy_gate"],"fault":state["fault"],
            "last_input_error":state["last_input_error"],
            "awaiting_source_refresh":state["awaiting_source_refresh"],
            "awaiting_native_snapshots":state["awaiting_native_snapshots"],
            "source_status":state["source_status"],"books":state["books"],
            "reference":state["reference"],"orders":state["simulation"]["orders"]
        });
        panic!("paper actor did not publish two legal live orders within five seconds: {details}");
    })
}

#[tokio::test(flavor = "current_thread")]
async fn actual_feed_tick_change_cancels_and_drains_before_durable_requote() {
    let _clock = TestClock::fixed(1_800_000_000_000, 0);
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let start = Instant::now();
    let (feeds, feed_rx) = mpsc::channel(config.feed_capacity);
    let (_fault, fault_rx) = watch::channel(None);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (handle, actor) = runtime::prepare(&config, start, feed_rx, fault_rx, shutdown_rx).unwrap();
    let actor_task = tokio::spawn(actor.run());
    let condition = format!("0x{}", "a".repeat(64));
    let wall = runtime::wall_ms();
    handle
        .select_market(Market {
            slug: "offline-tick-fixture".into(),
            condition_id: condition.clone(),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: wall - 1000,
            window_end_ms: wall + 60_000,
            tick_micro: 100,
        })
        .await
        .unwrap();

    let (next_tick, mut tick_rx) = watch::channel(100u64);
    handle.fee_policy(Some(
        mm2_live::fees::parse_policy(
            &serde_json::json!({"c":condition,
        "t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}}),
            &condition,
            "1",
            "2",
            wall,
        )
        .unwrap(),
    ));
    // A single bounded producer preserves Tick -> Snapshot ordering for each outcome.
    let feeder = tokio::spawn(async move {
        let mut current_tick = 100;
        for _ in 0..500 {
            TestClock::advance_current(20);
            let target_tick = *tick_rx.borrow_and_update();
            let wall = runtime::wall_ms();
            let mono = runtime::mono_ms(start);
            if feeds
                .send(FeedEvent::Reference {
                    exchange_ms: wall,
                    observed_mono_ms: mono,
                    price_micro: 80_000_000_000,
                })
                .await
                .is_err()
            {
                return;
            }
            if target_tick != current_tick {
                for outcome in [Outcome::Up, Outcome::Down] {
                    if feeds
                        .send(FeedEvent::Tick {
                            condition_id: condition.clone(),
                            outcome,
                            exchange_ms: wall,
                            observed_mono_ms: mono,
                            old_tick_micro: Some(current_tick),
                            new_tick_micro: target_tick,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                current_tick = target_tick;
            }
            for (outcome, bid, ask) in if current_tick == 100 {
                [
                    (Outcome::Up, 490_100, 510_100),
                    (Outcome::Down, 489_900, 509_900),
                ]
            } else {
                [
                    (Outcome::Up, 492_500, 512_500),
                    (Outcome::Down, 487_500, 507_500),
                ]
            } {
                if feeds
                    .send(FeedEvent::Snapshot {
                        condition_id: condition.clone(),
                        outcome,
                        exchange_ms: wall,
                        observed_mono_ms: mono,
                        bids: vec![Level {
                            price_micro: bid,
                            quantity_micro: 100_000_000,
                        }],
                        asks: vec![Level {
                            price_micro: ask,
                            quantity_micro: 100_000_000,
                        }],
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    handle
        .command(Command::Control {
            id: "paper-tick-resume".into(),
            expected_version: 0,
            action: "resume".into(),
        })
        .await
        .unwrap();
    let first = live_orders(&handle, 100).await;
    assert!(
        first
            .orders
            .iter()
            .any(|order| !order.quote.price_micros.is_multiple_of(1000)),
        "fixture must exercise prices rejected by the previous hardcoded 1000 grid"
    );
    let old_ids: Vec<_> = first.orders.iter().map(|order| order.id).collect();

    next_tick.send_replace(2500);
    let second = live_orders(&handle, 2500).await;
    assert!(
        second
            .orders
            .iter()
            .all(|order| !old_ids.contains(&order.id))
    );
    assert!(
        second
            .orders
            .iter()
            .any(|order| !order.quote.price_micros.is_multiple_of(1000))
    );
    assert!(second.venue_stats.cancelled_orders >= old_ids.len() as u64);
    assert_eq!(second.venue_stats.rejected_posts, 0);
    assert_eq!(second.account.spent_pico, "0");

    shutdown.send_replace(true);
    feeder.abort();
    let _ = feeder.await;
    tokio::time::timeout(Duration::from_secs(5), actor_task)
        .await
        .unwrap()
        .unwrap();

    // Replay the actual actor inputs, checking the moment rules changed rather than
    // merely trusting the final UI snapshot or cancellation counters.
    let journal = Journal::open_streaming(
        directory.path(),
        JournalOptions::default(),
        config.limits.canonical().unwrap(),
    )
    .unwrap();
    let mut replay = simulation::new(journal.bootstrap()).unwrap();
    let mut rules = Vec::new();
    let mut cancelled_old_grid = false;
    let mut quotes_on_new_grid = false;
    journal
        .visit_execution(|entries| {
            for entry in entries {
                let operation: Operation = serde_json::from_value(entry.clone()).unwrap();
                match &operation {
                    Operation::CancelAll { .. } if replay.limits().tick_micros == 100 => {
                        cancelled_old_grid = true;
                    }
                    Operation::VenueRules { tick_micros, .. } => {
                        let before = replay.snapshot();
                        assert!(before.orders.is_empty());
                        assert!(before.venue_orders.is_empty());
                        assert_eq!(before.pending_events, 0);
                        assert_eq!(before.account.reserved_pico, "0");
                        if *tick_micros == 2500 {
                            assert!(cancelled_old_grid);
                        }
                        rules.push(*tick_micros);
                    }
                    Operation::Quotes { quotes, .. } => {
                        assert!(quotes.iter().all(|quote| {
                            quote
                                .price_micros
                                .is_multiple_of(replay.limits().tick_micros)
                        }));
                        if replay.limits().tick_micros == 2500 && !quotes.is_empty() {
                            quotes_on_new_grid = true;
                        }
                    }
                    _ => {}
                }
                simulation::apply(&mut replay, &operation)?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(rules, vec![100, 2500]);
    assert!(quotes_on_new_grid);
    replay.validate().unwrap();
}
