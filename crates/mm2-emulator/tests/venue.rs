use mm2_emulator::*;
fn q(slot: &str, price: u64, shares: u64) -> DesiredQuote {
    DesiredQuote {
        slot: slot.into(),
        outcome: Outcome::Up,
        price_micros: price,
        quantity_micros: shares * SCALE,
    }
}
fn book(size: u64) -> Book {
    Book {
        bid: Some(500_000),
        ask: Some(600_000),
        bid_levels: vec![Level {
            price_micros: 500_000,
            quantity_micros: size * SCALE,
        }],
    }
}
fn session(latency: LatencyConfig) -> PaperSession {
    let mut s = PaperSession::new(RiskLimits::default(), latency).unwrap();
    s.set_book(0, Outcome::Up, book(0)).unwrap();
    s
}
fn zero() -> LatencyConfig {
    LatencyConfig {
        post_ms: 0,
        cancel_ms: 0,
        ack_ms: 0,
        fill_ms: 0,
        timeout_ms: 500,
    }
}
#[test]
fn shared_external_queue_and_own_priority_conserve_each_trade() {
    let mut s = session(zero());
    s.set_book(0, Outcome::Up, book(3)).unwrap();
    s.quote(0, vec![q("a", 500_000, 5), q("b", 500_000, 5)])
        .unwrap();
    s.advance(0).unwrap();
    let r = s.trade(1, Outcome::Up, 500_000, 5 * SCALE).unwrap();
    assert_eq!(r.external_consumed_micros, 3 * SCALE);
    assert_eq!(r.own_filled_micros, 2 * SCALE);
    s.advance(1).unwrap();
    let snap = s.snapshot();
    assert_eq!(snap.account.up_inventory_micros, 2 * SCALE);
    assert_eq!(snap.venue_orders[1].external_queue_ahead_micros, 0);
    let r = s.trade(2, Outcome::Up, 500_000, 5 * SCALE).unwrap();
    assert_eq!(r.own_filled_micros, 5 * SCALE);
    s.advance(2).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 7 * SCALE);
}
#[test]
fn better_external_levels_consume_volume_before_own_order() {
    let mut s = session(zero());
    s.set_book(
        0,
        Outcome::Up,
        Book {
            bid: Some(550_000),
            ask: Some(600_000),
            bid_levels: vec![
                Level {
                    price_micros: 550_000,
                    quantity_micros: 4 * SCALE,
                },
                Level {
                    price_micros: 500_000,
                    quantity_micros: SCALE,
                },
            ],
        },
    )
    .unwrap();
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(0).unwrap();
    let r = s.trade(1, Outcome::Up, 500_000, 7 * SCALE).unwrap();
    assert_eq!(r.external_consumed_micros, 5 * SCALE);
    assert_eq!(r.own_filled_micros, 2 * SCALE);
}
#[test]
fn arrival_book_crossing_rejects_post_only() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.set_book(
        10,
        Outcome::Up,
        Book {
            bid: Some(480_000),
            ask: Some(490_000),
            bid_levels: vec![],
        },
    )
    .unwrap();
    s.advance(50).unwrap();
    assert_eq!(s.snapshot().venue_stats.rejected_posts, 1);
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().account.reserved_pico, "0");
}
#[test]
fn cancel_arrival_fill_race_preserves_unreported_spend() {
    let mut s = session(LatencyConfig {
        post_ms: 10,
        cancel_ms: 20,
        ack_ms: 5,
        fill_ms: 100,
        timeout_ms: 500,
    });
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(15).unwrap();
    s.cancel_all(16).unwrap();
    s.trade(20, Outcome::Up, 500_000, 2 * SCALE).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 0);
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 2_500_000);
    s.advance(41).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 2 * SCALE);
    assert_eq!(s.snapshot().account.spent_micros_ceil, SCALE);
    assert!(s.snapshot().orders.is_empty());
    s.advance(130).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 2 * SCALE);
}
#[test]
fn cancel_before_post_and_coalesced_replacement_cannot_fill_early() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    s.quote(41, vec![q("a", 490_000, 5)]).unwrap();
    s.quote(42, vec![q("a", 480_000, 5)]).unwrap();
    s.advance(75).unwrap();
    assert_eq!(s.snapshot().venue_stats.accepted_posts, 1);
    s.advance(101).unwrap();
    assert_eq!(s.snapshot().venue_stats.accepted_posts, 2);
    assert_eq!(s.snapshot().venue_orders[0].quote.price_micros, 480_000);
    assert_eq!(s.snapshot().venue_orders[0].id, 2);
}
#[test]
fn lost_post_ack_reconciles_same_identity_without_duplicate_post() {
    let mut s = session(LatencyConfig::default());
    s.inject(Fault::DropNextPostAck).unwrap();
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(501).unwrap();
    assert_eq!(
        s.snapshot().orders[0].phase,
        mm2_core::OrderPhase::UnknownPost
    );
    s.advance(515).unwrap();
    assert_eq!(s.snapshot().orders[0].id, 1);
    assert_eq!(s.snapshot().orders[0].phase, mm2_core::OrderPhase::Live);
    assert_eq!(s.snapshot().stats.submitted_posts, 1);
}
#[test]
fn timeout_before_transport_arrival_never_releases_reservation() {
    let mut s = session(LatencyConfig {
        post_ms: 100,
        ack_ms: 0,
        timeout_ms: 10,
        ..LatencyConfig::default()
    });
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.cancel_all(11).unwrap();
    s.advance(50).unwrap();
    assert_eq!(s.snapshot().orders.len(), 1);
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 2_500_000);
    assert_eq!(s.snapshot().stats.submitted_posts, 1);
    s.advance(150).unwrap();
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().venue_stats.accepted_posts, 1);
}
#[test]
fn lost_cancel_ack_reserves_until_authoritative_query() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    s.inject(Fault::DropNextCancelAck).unwrap();
    s.cancel_all(41).unwrap();
    s.advance(100).unwrap();
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 2_500_000);
    s.advance(556).unwrap();
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().account.reserved_pico, "0");
}
#[test]
fn rate_limited_post_retries_with_new_id_after_confirmed_rejection() {
    let mut s = session(LatencyConfig::default());
    s.inject(Fault::RejectNextPost429 {
        retry_after_ms: 100,
    })
    .unwrap();
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    assert_eq!(s.snapshot().account.reserved_pico, "0");
    s.advance(180).unwrap();
    assert_eq!(s.snapshot().venue_orders[0].id, 2);
    assert_eq!(s.snapshot().stats.rate_limits, 1);
    assert_eq!(s.snapshot().stats.submitted_posts, 2);
}
#[test]
fn rate_limited_cancel_keeps_reserve_and_retries() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    s.inject(Fault::RejectNextCancel429 {
        retry_after_ms: 100,
    })
    .unwrap();
    s.cancel_all(41).unwrap();
    s.advance(100).unwrap();
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 2_500_000);
    s.advance(220).unwrap();
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().stats.rate_limits, 1);
}
#[test]
fn duplicate_fill_notification_does_not_duplicate_inventory() {
    let mut s = session(zero());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(0).unwrap();
    s.inject(Fault::DuplicateNextNotice).unwrap();
    s.trade(1, Outcome::Up, 500_000, 2 * SCALE).unwrap();
    s.advance(2).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 2 * SCALE);
    assert_eq!(s.snapshot().venue_stats.duplicate_notifications, 1);
}
#[test]
fn restart_recovers_delayed_actions_fills_queue_and_order_identity() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    s.trade(41, Outcome::Up, 500_000, 2 * SCALE).unwrap();
    s.quote(42, vec![q("a", 490_000, 5)]).unwrap();
    let bytes = serde_json::to_vec(&s).unwrap();
    let mut recovered: PaperSession = serde_json::from_slice(&bytes).unwrap();
    recovered.validate().unwrap();
    s.advance(200).unwrap();
    recovered.advance(200).unwrap();
    assert_eq!(s.snapshot(), recovered.snapshot());
}
#[test]
fn back_of_queue_cancellation_and_new_external_depth_do_not_grant_free_priority() {
    let mut s = session(zero());
    s.set_book(0, Outcome::Up, book(3)).unwrap();
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(0).unwrap();
    s.set_book(1, Outcome::Up, book(8)).unwrap();
    s.set_book(2, Outcome::Up, book(6)).unwrap();
    assert_eq!(
        s.snapshot().venue_orders[0].external_queue_ahead_micros,
        3 * SCALE
    );
    let r = s.trade(3, Outcome::Up, 500_000, 4 * SCALE).unwrap();
    assert_eq!(r.own_filled_micros, SCALE);
}
#[test]
fn event_time_and_snapshot_bounds_fail_closed() {
    let mut s = session(zero());
    s.advance(20).unwrap();
    assert!(s.advance(19).is_err());
    let bytes = serde_json::to_value(&s).unwrap();
    let mut altered = bytes.clone();
    altered["version"] = serde_json::json!(99);
    let altered: PaperSession = serde_json::from_value(altered).unwrap();
    assert!(altered.validate().is_err());
}

#[test]
fn disconnected_post_cannot_be_retried_until_absence_is_reconciled() {
    let mut s = session(LatencyConfig::default());
    s.inject(Fault::Disconnect { until_ms: 2_000 }).unwrap();
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(1_500).unwrap();
    assert_eq!(s.snapshot().stats.submitted_posts, 1);
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 2_500_000);
    s.cancel_all(1_501).unwrap();
    s.advance(3_000).unwrap();
    assert_eq!(s.snapshot().stats.submitted_posts, 1);
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().account.reserved_pico, "0");
}

#[test]
fn disconnected_cancel_keeps_possible_fill_and_recovers_cumulative_inventory() {
    let mut s = session(LatencyConfig::default());
    s.quote(0, vec![q("a", 500_000, 5)]).unwrap();
    s.advance(40).unwrap();
    s.inject(Fault::Disconnect { until_ms: 2_000 }).unwrap();
    s.cancel_all(41).unwrap();
    s.trade(80, Outcome::Up, 500_000, 2 * SCALE).unwrap();
    s.advance(1_500).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 2 * SCALE);
    assert_eq!(s.snapshot().account.reserved_micros_ceil, 1_500_000);
    s.advance(3_000).unwrap();
    assert!(s.snapshot().orders.is_empty());
    assert_eq!(s.snapshot().account.up_inventory_micros, 2 * SCALE);
    assert_eq!(s.snapshot().account.reserved_pico, "0");
}

#[test]
fn a_long_idle_time_jump_does_not_invent_actions() {
    let mut s = session(LatencyConfig::default());
    let sequence = s.snapshot().audit_sequence;
    s.advance(7 * 86_400_000).unwrap();
    assert_eq!(s.snapshot().audit_sequence, sequence);
    assert_eq!(s.snapshot().pending_events, 0);
}
