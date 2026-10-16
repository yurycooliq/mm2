use mm2_emulator::*;

fn session(queue_shares: u64) -> PaperSession {
    let mut s = PaperSession::new(
        RiskLimits::default(),
        LatencyConfig {
            post_ms: 0,
            cancel_ms: 0,
            ack_ms: 0,
            fill_ms: 0,
            timeout_ms: 500,
        },
    )
    .unwrap();
    s.defer_automatic_posts();
    s.set_book(
        0,
        Outcome::Up,
        Book {
            bid: Some(500_000),
            ask: Some(600_000),
            bid_levels: vec![Level {
                price_micros: 500_000,
                quantity_micros: queue_shares * SCALE,
            }],
        },
    )
    .unwrap();
    s
}
fn quote(slot: &str) -> DesiredQuote {
    DesiredQuote {
        slot: slot.into(),
        outcome: Outcome::Up,
        price_micros: 500_000,
        quantity_micros: 5 * SCALE,
    }
}
#[test]
fn delayed_print_cannot_fill_new_order_or_advance_its_queue() {
    let mut s = session(3);
    s.quote(100, vec![quote("up")]).unwrap();
    s.advance(100).unwrap();
    assert_eq!(s.snapshot().venue_orders[0].posted_at_ms, Some(100));
    let excluded = s
        .trade_at_price_causal(200, 90, Outcome::Up, 500_000, 4 * SCALE)
        .unwrap();
    assert_eq!(excluded.own_filled_micros, 0);
    assert_eq!(excluded.external_consumed_micros, 0);
    assert_eq!(excluded.unconsumed_micros, 4 * SCALE);
    assert_eq!(excluded.causality_excluded_order_count, 1);
    assert_eq!(
        s.snapshot().venue_orders[0].external_queue_ahead_micros,
        3 * SCALE
    );
    let eligible = s
        .trade_at_price_causal(201, 101, Outcome::Up, 500_000, 4 * SCALE)
        .unwrap();
    assert_eq!(eligible.external_consumed_micros, 3 * SCALE);
    assert_eq!(eligible.own_filled_micros, SCALE);
    s.advance(201).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, SCALE);
    assert_eq!(s.snapshot().venue_stats.causality_excluded_prints, 1);
    s.validate().unwrap();
}
#[test]
fn equal_exchange_acceptance_clock_and_legacy_unknown_timing_fail_closed() {
    let mut s = session(0);
    s.quote(10, vec![quote("up")]).unwrap();
    s.advance(10).unwrap();
    assert_eq!(
        s.trade_at_price_causal(20, 10, Outcome::Up, 500_000, 5 * SCALE)
            .unwrap()
            .own_filled_micros,
        0
    );
    let id = s.snapshot().venue_orders[0].id.to_string();
    let mut value = serde_json::to_value(&s).unwrap();
    value["venue"][id.as_str()]
        .as_object_mut()
        .unwrap()
        .remove("posted_at_ms");
    let mut legacy: PaperSession = serde_json::from_value(value).unwrap();
    assert_eq!(
        legacy
            .trade_at_price_causal(21, 15, Outcome::Up, 500_000, 5 * SCALE)
            .unwrap()
            .own_filled_micros,
        0
    );
    // Historical V2 fill rules remain independently replayable.
    assert_eq!(
        legacy
            .trade_at_price(22, Outcome::Up, 500_000, 5 * SCALE)
            .unwrap()
            .own_filled_micros,
        5 * SCALE
    );
    legacy.advance(22).unwrap();
    legacy.validate().unwrap();
}
#[test]
fn mixed_old_and_new_orders_skip_whole_print_without_free_priority() {
    let mut s = session(0);
    let old = quote("old");
    s.quote(0, vec![old.clone()]).unwrap();
    s.advance(0).unwrap();
    s.quote(10, vec![old, quote("new")]).unwrap();
    s.advance(10).unwrap();
    let result = s
        .trade_at_price_causal(20, 5, Outcome::Up, 500_000, 10 * SCALE)
        .unwrap();
    assert_eq!(result.causality_excluded_order_count, 1);
    assert_eq!(result.own_filled_micros, 0);
    assert_eq!(result.unconsumed_micros, 10 * SCALE);
    assert!(
        s.trade_at_price_causal(21, 22, Outcome::Up, 500_000, SCALE)
            .is_err()
    );
    s.validate().unwrap();
}
