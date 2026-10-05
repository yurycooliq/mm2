use crate::{
    config, discovery,
    domain::{FeedEvent, Level, Market, Outcome},
    feeds,
    runtime::{self, Handle},
};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const WINDOW_MS: u64 = 300_000;
const FEED_STOP_TIMEOUT: Duration = Duration::from_secs(6);

fn latch_fault(fault: &watch::Sender<Option<String>>, reason: &str) {
    fault.send_if_modified(|current| {
        if current.is_some() {
            return false;
        }
        *current = Some(reason.to_owned());
        true
    });
}

async fn wait_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        let stopping = *shutdown.borrow_and_update();
        if stopping || shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_fault(fault: &mut watch::Receiver<Option<String>>) -> String {
    loop {
        let current = fault.borrow_and_update().clone();
        if let Some(reason) = current {
            return reason;
        }
        if fault.changed().await.is_err() {
            return "source fault channel closed".into();
        }
    }
}

/// Dropping a cancelled metadata request also drops its network future. Shutdown and
/// the independently latched fault always have priority over ready source work.
async fn interruptible<F, T>(
    future: F,
    shutdown: &mut watch::Receiver<bool>,
    fault: &mut watch::Receiver<Option<String>>,
) -> Result<Option<T>, String>
where
    F: Future<Output = Result<T, String>>,
{
    tokio::select! {
        biased;
        _ = wait_shutdown(shutdown) => Ok(None),
        reason = wait_fault(fault) => Err(reason),
        result = future => result.map(Some),
    }
}

fn demo_market(now_wall_ms: u64) -> Result<Market, String> {
    let window_start_ms = now_wall_ms / WINDOW_MS * WINDOW_MS;
    let window_end_ms = window_start_ms
        .checked_add(WINDOW_MS)
        .ok_or("demo window overflow")?;
    let token = window_start_ms
        .checked_mul(2)
        .ok_or("demo token overflow")?;
    Ok(Market {
        slug: format!("demo-btc5m-{}", window_start_ms / 1000),
        condition_id: format!("0xd3{window_start_ms:062x}"),
        up_token: token
            .checked_add(1)
            .ok_or("demo token overflow")?
            .to_string(),
        down_token: token
            .checked_add(2)
            .ok_or("demo token overflow")?
            .to_string(),
        window_start_ms,
        window_end_ms,
        tick_micro: 1_000,
    })
}

fn demo_frame(market: &Market, wall_ms: u64, observed_mono_ms: u64) -> Vec<FeedEvent> {
    [Outcome::Up, Outcome::Down]
        .into_iter()
        .map(|outcome| FeedEvent::Snapshot {
            condition_id: market.condition_id.clone(),
            outcome,
            exchange_ms: wall_ms,
            observed_mono_ms,
            bids: vec![Level {
                price_micro: 490_000,
                quantity_micro: 100_000_000,
            }],
            asks: vec![Level {
                price_micro: 510_000,
                quantity_micro: 100_000_000,
            }],
        })
        .collect()
}

async fn run_demo(
    start: Instant,
    handle: &Handle,
    tx: &mpsc::Sender<FeedEvent>,
    fault_tx: &watch::Sender<Option<String>>,
    shutdown: &mut watch::Receiver<bool>,
    fault: &mut watch::Receiver<Option<String>>,
) -> Result<(), String> {
    let mut market = demo_market(runtime::wall_ms())?;
    if interruptible(handle.select_market(market.clone()), shutdown, fault)
        .await?
        .is_none()
    {
        return Ok(());
    }
    let _ = handle.source_status(
        "demo",
        Some("synthetic books for deterministic exchange emulation; fills and balances are simulated".into()),
        None,
    );
    let mut cadence = tokio::time::interval(Duration::from_millis(250));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if interruptible(
            async {
                cadence.tick().await;
                Ok(())
            },
            shutdown,
            fault,
        )
        .await?
        .is_none()
        {
            return Ok(());
        }
        let mut now = runtime::wall_ms();
        if now < market.window_start_ms || now >= market.window_end_ms {
            market = demo_market(now)?;
            if interruptible(handle.select_market(market.clone()), shutdown, fault)
                .await?
                .is_none()
            {
                return Ok(());
            }
            now = runtime::wall_ms();
        }
        feeds::enqueue_frame(
            tx,
            demo_frame(&market, now, runtime::mono_ms(start)),
            fault_tx,
        )?;
    }
}

/// A source future can itself be cancelled. Its owned task must then be aborted
/// rather than detached; normal shutdown first gives both sockets time to join.
struct OwnedFeeds {
    task: Option<JoinHandle<Result<(), String>>>,
    shutdown: watch::Sender<bool>,
}
impl OwnedFeeds {
    async fn stop(mut self, timeout: Duration) -> Result<(), String> {
        self.shutdown.send_replace(true);
        let task = self.task.as_mut().ok_or("feed task already joined")?;
        match tokio::time::timeout(timeout, &mut *task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("public feed task failed during shutdown".into()),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("public feeds exceeded shutdown deadline".into())
            }
        }
    }
}
impl Drop for OwnedFeeds {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

enum Transition {
    Shutdown,
    Rollover,
    Refresh,
    Fault(String),
    Joined(Result<Result<(), String>, tokio::task::JoinError>),
}

fn reset_epoch(value: &serde_json::Value) -> Result<u64, String> {
    value
        .get("reset_epoch")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "actor reset epoch is missing".into())
}

fn refresh_requested(value: &serde_json::Value, initial_epoch: u64) -> Result<bool, String> {
    let pending = value
        .get("awaiting_source_refresh")
        .and_then(serde_json::Value::as_bool)
        .ok_or("actor source refresh barrier is missing")?;
    Ok(pending || reset_epoch(value)? != initial_epoch)
}

async fn watch_window(
    feeds: &mut OwnedFeeds,
    window_end_ms: u64,
    shutdown: &mut watch::Receiver<bool>,
    fault: &mut watch::Receiver<Option<String>>,
    state: &mut watch::Receiver<Arc<serde_json::Value>>,
    initial_epoch: u64,
) -> Transition {
    let mut clock = tokio::time::interval(Duration::from_millis(250));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // Reset can race the SelectMarket acknowledgement and initial epoch capture.
        match refresh_requested(&state.borrow_and_update(), initial_epoch) {
            Ok(true) => return Transition::Refresh,
            Ok(false) => {}
            Err(reason) => return Transition::Fault(reason),
        }
        tokio::select! {
            biased;
            _ = wait_shutdown(shutdown) => return Transition::Shutdown,
            reason = wait_fault(fault) => return Transition::Fault(reason),
            result = feeds.task.as_mut().expect("owned feed task exists") => return Transition::Joined(result),
            _ = clock.tick() => {
                if runtime::wall_ms() >= window_end_ms { return Transition::Rollover; }
            },
            changed = state.changed() => {
                if changed.is_err() { return Transition::Fault("actor state channel closed".into()); }
                let current = refresh_requested(&state.borrow_and_update(), initial_epoch);
                match current {
                    Ok(true) => return Transition::Refresh,
                    Ok(false) => {},
                    Err(reason) => return Transition::Fault(reason),
                }
            },
        }
    }
}

async fn run_public(
    start: Instant,
    handle: &Handle,
    tx: &mpsc::Sender<FeedEvent>,
    fault_tx: &watch::Sender<Option<String>>,
    shutdown: &mut watch::Receiver<bool>,
    fault: &mut watch::Receiver<Option<String>>,
) -> Result<(), String> {
    let client = discovery::client()?;
    let mut retry_seconds = 5u64;
    loop {
        let _ = handle.source_status("discovering", None, None);
        let discovery = async {
            tokio::time::timeout(Duration::from_secs(25), async {
                discovery::current_btc(&client, runtime::wall_ms())
                    .await
                    .map_err(|reason| format!("BTC5m discovery: {reason}"))
            })
            .await
            .map_err(|_| "public discovery timed out".to_owned())?
        };
        let market = match interruptible(discovery, shutdown, fault).await {
            Ok(Some(market)) => market,
            Ok(None) => return Ok(()),
            Err(reason) if fault.borrow().is_some() => return Err(reason),
            Err(reason) => {
                let _ = handle.source_status("retrying", Some(reason), Some(retry_seconds * 1000));
                tracing::warn!("current BTC5m metadata unavailable; source retry scheduled");
                if interruptible(
                    async {
                        tokio::time::sleep(Duration::from_secs(retry_seconds)).await;
                        Ok(())
                    },
                    shutdown,
                    fault,
                )
                .await?
                .is_none()
                {
                    return Ok(());
                }
                retry_seconds = (retry_seconds * 2).min(30);
                continue;
            }
        };
        let now = runtime::wall_ms();
        if now < market.window_start_ms || now >= market.window_end_ms {
            let _ = handle.source_status(
                "retrying",
                Some("discovered market window is no longer current".into()),
                Some(5000),
            );
            if interruptible(
                async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(())
                },
                shutdown,
                fault,
            )
            .await?
            .is_none()
            {
                return Ok(());
            }
            continue;
        }
        if interruptible(handle.select_market(market.clone()), shutdown, fault)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let mut state = handle.state.clone();
        let initial_epoch = reset_epoch(&state.borrow_and_update())?;
        // The actor has acknowledged the new identity before any new-market frame is emitted.
        let (feed_shutdown, feed_shutdown_rx) = watch::channel(false);
        let mut owned = OwnedFeeds {
            task: Some(tokio::spawn(feeds::run_public(
                market.clone(),
                start,
                tx.clone(),
                fault_tx.clone(),
                feed_shutdown_rx,
            ))),
            shutdown: feed_shutdown,
        };
        let _ = handle.source_status(
            "streaming",
            Some("public CLOB books and Binance reference; exact TWAP unavailable".into()),
            None,
        );
        retry_seconds = 5;
        match watch_window(
            &mut owned,
            market.window_end_ms,
            shutdown,
            fault,
            &mut state,
            initial_epoch,
        )
        .await
        {
            Transition::Shutdown => {
                owned.stop(FEED_STOP_TIMEOUT).await?;
                return Ok(());
            }
            Transition::Rollover => owned.stop(FEED_STOP_TIMEOUT).await?,
            Transition::Refresh => {
                let _ = handle.source_status(
                    "refreshing",
                    Some("durable Reset requires fresh source snapshots".into()),
                    None,
                );
                owned.stop(FEED_STOP_TIMEOUT).await?;
            }
            Transition::Fault(reason) => {
                let _ = owned.stop(FEED_STOP_TIMEOUT).await;
                return Err(reason);
            }
            Transition::Joined(result) => {
                // A completed JoinHandle must not be polled a second time.
                owned.task.take();
                return Err(match result {
                    Ok(Err(reason)) => reason,
                    Ok(Ok(())) => "public feeds stopped unexpectedly".into(),
                    Err(_) => "public feed task failed".into(),
                });
            }
        }
    }
}

pub async fn run(
    source: config::FeedSource,
    start: Instant,
    handle: Handle,
    tx: mpsc::Sender<FeedEvent>,
    fault: watch::Sender<Option<String>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut fault_rx = fault.subscribe();
    let result = match source {
        config::FeedSource::Demo => {
            run_demo(start, &handle, &tx, &fault, &mut shutdown, &mut fault_rx).await
        }
        config::FeedSource::Public => {
            run_public(start, &handle, &tx, &fault, &mut shutdown, &mut fault_rx).await
        }
    };
    if let Err(reason) = &result {
        latch_fault(&fault, reason);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Core, CoreConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn demo_identity_is_distinct_per_window_and_valid() {
        let first = demo_market(1_790_712_601_234).unwrap();
        let second = demo_market(first.window_end_ms).unwrap();
        assert!(first.slug.starts_with("demo-"));
        assert_ne!(first.condition_id, second.condition_id);
        assert_ne!(first.up_token, first.down_token);
        assert_ne!(first.up_token, second.up_token);
        let mut core = Core::new(CoreConfig {
            initial_collateral_micro: 1_000_000_000,
            max_imbalance_micro: 25_000_000,
            lot_micro: 5_000_000,
            half_width_micro: 45_000,
            max_feed_age_ms: 1000,
        })
        .unwrap();
        core.set_market(first).unwrap();
    }

    #[test]
    fn demo_has_both_books_and_independent_clocks_without_reference() {
        let market = demo_market(1_790_712_601_234).unwrap();
        let events = demo_frame(&market, 1_790_712_601_234, 17);
        assert_eq!(events.len(), 2);
        for (event, expected) in events.iter().zip([Outcome::Up, Outcome::Down]) {
            match event {
                FeedEvent::Snapshot {
                    outcome,
                    exchange_ms,
                    observed_mono_ms,
                    bids,
                    asks,
                    ..
                } => {
                    assert_eq!(*outcome, expected);
                    assert_eq!(*exchange_ms, 1_790_712_601_234);
                    assert_eq!(*observed_mono_ms, 17);
                    assert_eq!(bids[0].quantity_micro, 100_000_000);
                    assert!(bids[0].price_micro < asks[0].price_micro);
                }
                _ => panic!("demo emitted a non-book event"),
            }
        }
    }

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn hung_owned_feed_is_aborted_and_joined_by_deadline() {
        let dropped = Arc::new(AtomicBool::new(false));
        let child = dropped.clone();
        let (shutdown, _) = watch::channel(false);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = Dropped(child);
            let _ = started.send(());
            std::future::pending::<Result<(), String>>().await
        });
        ready.await.unwrap();
        let owned = OwnedFeeds {
            task: Some(task),
            shutdown,
        };
        assert!(
            owned
                .stop(Duration::from_millis(20))
                .await
                .unwrap_err()
                .contains("deadline")
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancellation_does_not_detach_owned_feeds() {
        let dropped = Arc::new(AtomicBool::new(false));
        let child = dropped.clone();
        let (shutdown, _) = watch::channel(false);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = Dropped(child);
            let _ = started.send(());
            std::future::pending::<Result<(), String>>().await
        });
        ready.await.unwrap();
        drop(OwnedFeeds {
            task: Some(task),
            shutdown,
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn global_fault_preempts_ready_source_work() {
        let (_shutdown, mut shutdown_rx) = watch::channel(false);
        let (_fault, mut fault_rx) = watch::channel(Some("actor fatal".to_owned()));
        assert_eq!(
            interruptible(async { Ok(7u64) }, &mut shutdown_rx, &mut fault_rx)
                .await
                .unwrap_err(),
            "actor fatal"
        );
    }

    #[tokio::test]
    async fn already_pending_reset_refreshes_without_another_state_notification() {
        let dropped = Arc::new(AtomicBool::new(false));
        let child = dropped.clone();
        let (feed_shutdown, mut feed_control) = watch::channel(false);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = Dropped(child);
            let _ = started.send(());
            wait_shutdown(&mut feed_control).await;
            Ok(())
        });
        ready.await.unwrap();
        let mut owned = OwnedFeeds {
            task: Some(task),
            shutdown: feed_shutdown,
        };
        let (_shutdown, mut shutdown_rx) = watch::channel(false);
        let (_fault, mut fault_rx) = watch::channel(None);
        // Reset happened before the coordinator captured epoch=2. There is no
        // subsequent publication to wake state.changed().
        let (_state, mut state_rx) = watch::channel(Arc::new(serde_json::json!({
            "reset_epoch": 2,
            "awaiting_source_refresh": true,
        })));
        let transition = tokio::time::timeout(
            Duration::from_millis(100),
            watch_window(
                &mut owned,
                u64::MAX,
                &mut shutdown_rx,
                &mut fault_rx,
                &mut state_rx,
                2,
            ),
        )
        .await
        .expect("initial Reset must not wait for another state change");
        assert!(matches!(transition, Transition::Refresh));
        assert!(!dropped.load(Ordering::SeqCst));
        owned.stop(Duration::from_millis(100)).await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn reset_epoch_requires_the_actor_published_integer() {
        assert_eq!(
            reset_epoch(&serde_json::json!({"reset_epoch":2})).unwrap(),
            2
        );
        assert!(reset_epoch(&serde_json::json!({"reset_epoch":"2"})).is_err());
        assert!(reset_epoch(&serde_json::json!({})).is_err());
        assert!(
            refresh_requested(
                &serde_json::json!({"reset_epoch":2,"awaiting_source_refresh":true}),
                2
            )
            .unwrap()
        );
        assert!(
            !refresh_requested(
                &serde_json::json!({"reset_epoch":2,"awaiting_source_refresh":false}),
                2
            )
            .unwrap()
        );
        assert!(
            refresh_requested(
                &serde_json::json!({"reset_epoch":3,"awaiting_source_refresh":false}),
                2
            )
            .unwrap()
        );
    }
}
