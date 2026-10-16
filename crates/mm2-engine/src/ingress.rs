//! Pure public-event admission shared by the actor and observed-input replay.
//! Socket/REST recovery and source-generation ownership remain with their callers.
use crate::domain::{Core, FeedEvent, Outcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    Event,
    DeltaSkippedAwaitingSnapshots,
    SnapshotRecoveryStarted,
    SnapshotRecoveryCompleted,
}

pub fn apply_with_native_snapshots(
    core: &mut Core,
    event: FeedEvent,
    snapshots: &mut Option<[bool; 2]>,
) -> Result<Applied, String> {
    let delta = matches!(&event, FeedEvent::Delta { .. });
    if delta && snapshots.is_some() {
        return Ok(Applied::DeltaSkippedAwaitingSnapshots);
    }
    let snapshot = match &event {
        FeedEvent::Snapshot { outcome, .. } => Some(usize::from(*outcome == Outcome::Down)),
        FeedEvent::Disconnect { .. } => {
            *snapshots = None;
            None
        }
        _ => None,
    };
    if let Err(error) = core.apply(event) {
        if delta && error == "crossed or oversized batch result" {
            *snapshots = Some([false; 2]);
            return Ok(Applied::SnapshotRecoveryStarted);
        }
        return Err(error);
    }
    if let (Some(outcome), Some(seen)) = (snapshot, snapshots.as_mut()) {
        seen[outcome] = true;
        if seen.iter().all(|seen| *seen) {
            *snapshots = None;
            return Ok(Applied::SnapshotRecoveryCompleted);
        }
    }
    Ok(Applied::Event)
}

/// Validate before consuming any trade witness from this bounded batch.
/// A recoverable split delta preserves later trades/full books; a genuine fault
/// cuts the suffix exactly where the production actor will stop processing it.
pub fn valid_prefix(core: &Core, batch: &[FeedEvent], mut snapshots: Option<[bool; 2]>) -> usize {
    let mut checked = core.clone();
    batch
        .iter()
        .position(|event| {
            apply_with_native_snapshots(&mut checked, event.clone(), &mut snapshots).is_err()
        })
        .unwrap_or(batch.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Action, BookSide, Change, CoreConfig, Level, Market};
    fn setup() -> (Core, String) {
        let id = format!("0x{}", "a".repeat(64));
        let mut core = Core::new(CoreConfig {
            initial_collateral_micro: 1_000_000_000,
            max_imbalance_micro: 10_000_000,
            lot_micro: 5_000_000,
            half_width_micro: 10_000,
            max_feed_age_ms: 1000,
        })
        .unwrap();
        core.set_market(Market {
            slug: "native-ingress-test".into(),
            condition_id: id.clone(),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: 1000,
            window_end_ms: 301000,
            tick_micro: 10000,
        })
        .unwrap();
        core.control(Action::Resume).unwrap();
        for outcome in [Outcome::Up, Outcome::Down] {
            core.apply(snapshot(&id, outcome, 1001, 1)).unwrap();
        }
        (core, id)
    }
    fn snapshot(id: &str, outcome: Outcome, exchange: u64, received: u64) -> FeedEvent {
        FeedEvent::Snapshot {
            condition_id: id.into(),
            outcome,
            exchange_ms: exchange,
            observed_mono_ms: received,
            bids: vec![Level {
                price_micro: 490000,
                quantity_micro: 10_000_000,
            }],
            asks: vec![Level {
                price_micro: 510000,
                quantity_micro: 10_000_000,
            }],
        }
    }
    fn crossed(id: &str) -> FeedEvent {
        FeedEvent::Delta {
            condition_id: id.into(),
            exchange_ms: 1002,
            observed_mono_ms: 2,
            changes: vec![Change {
                outcome: Outcome::Up,
                side: BookSide::Bid,
                level: Level {
                    price_micro: 520000,
                    quantity_micro: 5_000_000,
                },
            }],
        }
    }
    #[test]
    fn split_delta_never_reinitializes_a_book_and_requires_both_native_snapshots() {
        let (mut core, id) = setup();
        let mut seen = None;
        assert_eq!(
            apply_with_native_snapshots(&mut core, crossed(&id), &mut seen).unwrap(),
            Applied::SnapshotRecoveryStarted
        );
        assert!(!core.snapshot().up_book.initialized);
        assert_eq!(
            apply_with_native_snapshots(&mut core, crossed(&id), &mut seen).unwrap(),
            Applied::DeltaSkippedAwaitingSnapshots
        );
        assert_eq!(
            apply_with_native_snapshots(&mut core, snapshot(&id, Outcome::Up, 1003, 3), &mut seen)
                .unwrap(),
            Applied::Event
        );
        assert_eq!(seen, Some([true, false]));
        assert!(!core.evaluate(1003, 3).stop_reasons.is_empty());
        assert_eq!(
            apply_with_native_snapshots(
                &mut core,
                snapshot(&id, Outcome::Down, 1004, 4),
                &mut seen
            )
            .unwrap(),
            Applied::SnapshotRecoveryCompleted
        );
        assert_eq!(seen, None);
        assert!(core.evaluate(1004, 4).stop_reasons.is_empty());
    }
    #[test]
    fn recoverable_split_keeps_trade_prefix_but_invalid_snapshot_cuts_the_suffix() {
        let (core, id) = setup();
        let trade = FeedEvent::Trade {
            condition_id: id.clone(),
            outcome: Outcome::Up,
            exchange_ms: 1003,
            observed_mono_ms: 3,
            price_micro: 490000,
            quantity_micro: 5_000_000,
            fingerprint: "a".repeat(64),
        };
        let mut invalid = snapshot(&id, Outcome::Down, 1005, 5);
        if let FeedEvent::Snapshot { bids, .. } = &mut invalid {
            bids[0].price_micro = 520000;
        }
        let batch = vec![
            crossed(&id),
            trade.clone(),
            snapshot(&id, Outcome::Up, 1004, 4),
            invalid,
            trade,
        ];
        assert_eq!(valid_prefix(&core, &batch, None), 3);
        assert_eq!(valid_prefix(&core, &batch[..3], None), 3);
    }
    #[test]
    fn native_watermark_drops_only_valid_obsolete_full_books_and_keeps_trade_witnesses() {
        let (mut core, id) = setup();
        let mut recovery = crate::feeds::Recovery::new([0; 2]);
        let seed = vec![
            snapshot(&id, Outcome::Up, 1001, 1),
            snapshot(&id, Outcome::Down, 1001, 1),
        ];
        assert_eq!(recovery.filter(seed).len(), 2);
        let update = FeedEvent::Delta {
            condition_id: id.clone(),
            exchange_ms: 1004,
            observed_mono_ms: 4,
            changes: vec![Change {
                outcome: Outcome::Up,
                side: BookSide::Bid,
                level: Level {
                    price_micro: 480000,
                    quantity_micro: 5_000_000,
                },
            }],
        };
        let mut seen = None;
        for event in recovery.filter(vec![update]) {
            apply_with_native_snapshots(&mut core, event, &mut seen).unwrap();
        }
        let trade = FeedEvent::Trade {
            condition_id: id.clone(),
            outcome: Outcome::Up,
            exchange_ms: 1005,
            observed_mono_ms: 5,
            price_micro: 490000,
            quantity_micro: 5_000_000,
            fingerprint: "b".repeat(64),
        };
        let filtered = recovery.filter(vec![snapshot(&id, Outcome::Up, 1002, 5), trade.clone()]);
        assert_eq!(filtered, vec![trade.clone()]);
        assert_eq!(valid_prefix(&core, &filtered, None), 1);
        assert_eq!(core.snapshot().up_book.exchange_ms, Some(1004));
        let mut malformed = snapshot(&id, Outcome::Up, 1002, 6);
        if let FeedEvent::Snapshot { bids, .. } = &mut malformed {
            bids[0].price_micro = 520000;
        }
        let invalid = recovery.filter(vec![malformed, trade]);
        assert_eq!(
            invalid.len(),
            2,
            "an invalid stale payload must never be hidden by watermark filtering"
        );
        assert_eq!(valid_prefix(&core, &invalid, None), 0);
    }
}
