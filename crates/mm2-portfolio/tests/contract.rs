use mm2_core::{Outcome, SCALE};
use mm2_portfolio::{
    Fill, Market, MergeReceipt, Portfolio, Provenance, Reservation, ResolutionReceipt, RiskPolicy,
};

fn portfolio() -> Portfolio {
    let mut p = Portfolio::new(100 * SCALE, RiskPolicy::default()).unwrap();
    p.register_market(Market {
        id: "btc-5m-0".into(),
        start_ms: 0,
        end_ms: 300_000,
    })
    .unwrap();
    p
}
fn fill(id: &str, outcome: Outcome, price: u64, qty: u64, fee: u128) -> Fill {
    Fill {
        id: id.into(),
        market_id: "btc-5m-0".into(),
        order_id: None,
        outcome,
        price_micros: price,
        quantity_micros: qty,
        fee_pico: fee,
    }
}
fn resolution(outcome: Outcome) -> ResolutionReceipt {
    ResolutionReceipt {
        id: "resolution-0".into(),
        market_id: "btc-5m-0".into(),
        winning_outcome: outcome,
        fee_pico: 0,
        at_ms: 300_001,
        finality_confirmed: true,
        provenance: Provenance::Synthetic,
    }
}

#[test]
fn distinct_markets_never_transfer_inventory_or_replenish_cash() {
    let mut p = portfolio();
    p.record_execution_totals("btc-5m-0", 2_000_000_000_003, 5 * SCALE, 0, 3)
        .unwrap();
    p.register_market(Market {
        id: "btc-5m-300".into(),
        start_ms: 300_000,
        end_ms: 600_000,
    })
    .unwrap();
    assert_eq!(p.market("btc-5m-0").unwrap().up.quantity_micros, 5 * SCALE);
    assert_eq!(p.market("btc-5m-300").unwrap().up.quantity_micros, 0);
    assert_eq!(p.capital_for_new_market().unwrap(), 97_999_999);
    assert_eq!(p.snapshot().cash_pico, "97999999999997");
    let before = p.clone();
    assert!(
        !p.record_execution_totals("btc-5m-0", 2_000_000_000_003, 5 * SCALE, 0, 3)
            .unwrap()
    );
    assert_eq!(p, before);
    assert!(
        p.record_execution_totals("btc-5m-0", 2_000_000_000_002, 5 * SCALE, 0, 3)
            .is_err()
    );
    assert_eq!(p, before);
}

#[test]
fn exact_fee_inclusive_fills_and_idempotent_receipt_collision() {
    let mut p = portfolio();
    let f = fill("fill-1", Outcome::Up, 333_333, 7, 11);
    assert!(p.apply_fill(f.clone(), 1).unwrap());
    let before = p.clone();
    assert!(!p.apply_fill(f.clone(), 0).unwrap()); // Duplicate does not move clock backwards.
    assert_eq!(p, before);
    let mut collision = f;
    collision.fee_pico = 12;
    assert!(p.apply_fill(collision, 2).is_err());
    assert_eq!(p.snapshot().spent_pico, (333_333_u128 * 7 + 11).to_string());
    p.validate().unwrap();
}

#[test]
fn cancelling_unknown_orders_retain_commitment_until_terminal() {
    let mut p = portfolio();
    let reservation = Reservation {
        order_id: "order-1".into(),
        market_id: "btc-5m-0".into(),
        outcome: Outcome::Up,
        price_micros: 500_000,
        quantity_micros: 10 * SCALE,
        max_fee_pico: 100,
    };
    assert!(p.reserve(reservation.clone()).unwrap());
    assert!(!p.reserve(reservation.clone()).unwrap());
    assert_eq!(p.available_cash_micros(), 94_999_999);
    let mut f = fill("partial", Outcome::Up, 490_000, 3 * SCALE, 30);
    f.order_id = Some("order-1".into());
    p.apply_fill(f, 10).unwrap();
    assert_eq!(
        p.snapshot().reserved_pico,
        (500_000_u128 * 7_000_000 + 70).to_string()
    );
    assert!(p.redeem(resolution(Outcome::Up), true).is_err());
    p.release_reservation("order-1").unwrap();
    assert!(p.reserve(reservation).is_err());
    assert!(p.redeem(resolution(Outcome::Up), true).unwrap());
    p.validate().unwrap();
}

#[test]
fn partial_merge_and_redemption_preserve_cost_dust_and_true_collateral() {
    let mut p = portfolio();
    p.apply_fill(fill("up", Outcome::Up, 400_001, 15, 2), 1)
        .unwrap();
    p.apply_fill(fill("down", Outcome::Down, 500_003, 10, 1), 2)
        .unwrap();
    let merge = MergeReceipt {
        id: "merge".into(),
        market_id: "btc-5m-0".into(),
        quantity_micros: 7,
        fee_pico: 1,
        at_ms: 3,
        provenance: Provenance::Synthetic,
    };
    assert!(p.merge_complete_sets(merge.clone(), true).unwrap());
    let before = p.clone();
    assert!(!p.merge_complete_sets(merge, true).unwrap());
    assert_eq!(p, before);
    assert!(p.redeem(resolution(Outcome::Up), true).unwrap());
    assert_eq!(p.snapshot().returned_pico, "15000000");
    assert_eq!(
        p.snapshot().realized_pnl_pico,
        (15_000_000_i128 - (400_001_i128 * 15 + 500_003_i128 * 10 + 4)).to_string()
    );
    assert_eq!(p.market("btc-5m-0").unwrap().up.cost_pico, 0);
    assert_eq!(p.market("btc-5m-0").unwrap().down.cost_pico, 0);
    p.validate().unwrap();
}

#[test]
fn resolution_must_be_final_closed_and_approved_provenance() {
    let mut p = portfolio();
    p.apply_fill(fill("up", Outcome::Up, 500_000, 5 * SCALE, 0), 1)
        .unwrap();
    let before = p.clone();
    assert!(p.redeem(resolution(Outcome::Up), false).is_err());
    let mut r = resolution(Outcome::Up);
    r.finality_confirmed = false;
    assert!(p.redeem(r, true).is_err());
    let mut r = resolution(Outcome::Up);
    r.at_ms = 299_999;
    assert!(p.redeem(r, true).is_err());
    assert_eq!(p, before);
    let r = resolution(Outcome::Up);
    assert!(p.redeem(r.clone(), true).unwrap());
    assert!(!p.redeem(r, true).unwrap());
    assert!(p.redeem(resolution(Outcome::Down), true).is_err());
}

#[test]
fn loss_budget_latches_and_next_day_never_automatically_rearms() {
    let policy = RiskPolicy {
        max_daily_loss_micros: SCALE,
        ..RiskPolicy::default()
    };
    let mut p = Portfolio::new(100 * SCALE, policy).unwrap();
    p.register_market(Market {
        id: "btc-5m-0".into(),
        start_ms: 0,
        end_ms: 300_000,
    })
    .unwrap();
    p.apply_fill(fill("loser", Outcome::Down, 500_000, 5 * SCALE, 0), 1)
        .unwrap();
    p.redeem(resolution(Outcome::Up), true).unwrap();
    assert_eq!(p.snapshot().daily_loss_pico, "2500000000000");
    assert!(p.capital_for_new_market().is_err());
    assert!(p.snapshot().halted.is_some());
    let restored: Portfolio = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(p, restored);
}

#[test]
fn aggregate_cost_checks_and_exposure_admission_are_atomic() {
    let mut p = portfolio();
    let before = p.clone();
    assert!(
        p.record_execution_totals("btc-5m-0", 10_000_000, 1, 0, 0)
            .is_err()
    );
    assert_eq!(p, before);
    assert!(p.record_execution_totals("btc-5m-0", 1, 0, 0, 1).is_err());
    assert_eq!(p, before);
    let too_big = Reservation {
        order_id: "huge".into(),
        market_id: "btc-5m-0".into(),
        outcome: Outcome::Up,
        price_micros: 500_000,
        quantity_micros: 201 * SCALE,
        max_fee_pico: 0,
    };
    assert!(p.reserve(too_big).is_err());
    assert_eq!(p, before);
}

#[test]
fn fragmented_outcomes_and_merges_conserve_exact_cash_over_many_markets() {
    let policy = RiskPolicy {
        max_daily_loss_micros: 100 * SCALE,
        max_drawdown_micros: 100 * SCALE,
        ..RiskPolicy::default()
    };
    let mut p = Portfolio::new(100 * SCALE, policy).unwrap();
    for window in 0..288_u64 {
        let id = format!("btc-5m-{window}");
        p.register_market(Market {
            id: id.clone(),
            start_ms: window * 300_000,
            end_ms: (window + 1) * 300_000,
        })
        .unwrap();
        for part in 0..10_u64 {
            for outcome in [Outcome::Up, Outcome::Down] {
                p.apply_fill(
                    Fill {
                        id: format!("fill-{window}-{part}-{outcome:?}"),
                        market_id: id.clone(),
                        order_id: None,
                        outcome,
                        price_micros: 499_999,
                        quantity_micros: 1_000_001,
                        fee_pico: 1,
                    },
                    window * 300_000 + part,
                )
                .unwrap();
            }
        }
        p.merge_complete_sets(
            MergeReceipt {
                id: format!("merge-{window}"),
                market_id: id.clone(),
                quantity_micros: 10_000_010,
                fee_pico: 1,
                at_ms: window * 300_000 + 20,
                provenance: Provenance::Synthetic,
            },
            true,
        )
        .unwrap();
        p.redeem(
            ResolutionReceipt {
                id: format!("resolve-{window}"),
                market_id: id,
                winning_outcome: Outcome::Up,
                fee_pico: 0,
                at_ms: (window + 1) * 300_000,
                finality_confirmed: true,
                provenance: Provenance::Synthetic,
            },
            true,
        )
        .unwrap();
        p.validate().unwrap();
    }
    let snapshot = p.snapshot();
    let initial: u128 = snapshot.initial_cash_pico.parse().unwrap();
    let returned: u128 = snapshot.returned_pico.parse().unwrap();
    let spent: u128 = snapshot.spent_pico.parse().unwrap();
    let cash: u128 = snapshot.cash_pico.parse().unwrap();
    assert_eq!(initial + returned, cash + spent);
    assert!(
        snapshot
            .positions
            .iter()
            .all(|m| m.up.quantity_micros == 0 && m.down.quantity_micros == 0)
    );
    assert!(cash > initial); // Synthetic arithmetic assertion, no economic evidence.
}

#[test]
fn no_inventory_windows_do_not_consume_unresolved_capacity() {
    let mut p = portfolio();
    for window in 1..288_u64 {
        p.register_market(Market {
            id: format!("empty-{window}"),
            start_ms: window * 300_000,
            end_ms: (window + 1) * 300_000,
        })
        .unwrap();
    }
    assert_eq!(p.capital_for_new_market().unwrap(), 100 * SCALE);
    let summary = p.compact_snapshot(3);
    assert_eq!(summary.position_count, 288);
    assert_eq!(summary.positions_truncated, 285);
    assert_eq!(summary.held_market_count, Some(0));
    assert_eq!(summary.positions.len(), 3);
    assert_eq!(summary.positions[0].market.id, "empty-287");
    assert_eq!(summary.cash_pico, p.snapshot().cash_pico);
    assert!(p.compact_snapshot(0).positions.is_empty());
    assert_eq!(p.compact_snapshot(0).held_market_count, Some(0));
}

#[test]
fn compact_telemetry_counts_all_held_markets_including_omitted_inventory() {
    let mut p = portfolio();
    p.apply_fill(fill("held-up", Outcome::Up, 490_000, SCALE, 0), 1)
        .unwrap();
    p.apply_fill(fill("held-down", Outcome::Down, 510_000, SCALE, 0), 2)
        .unwrap();
    for window in 1..70_u64 {
        p.register_market(Market {
            id: format!("empty-{window}"),
            start_ms: window * 300_000,
            end_ms: (window + 1) * 300_000,
        })
        .unwrap();
    }
    let compact = p.compact_snapshot(64);
    assert_eq!(compact.position_count, 70);
    assert_eq!(compact.positions_truncated, 6);
    assert_eq!(compact.held_market_count, Some(1)); // One market, two outcomes.
    assert!(compact.positions.iter().all(|position| {
        position.up.quantity_micros == 0 && position.down.quantity_micros == 0
    }));
    assert_eq!(p.snapshot().held_market_count, Some(1));

    let mut legacy = serde_json::to_value(&compact).unwrap();
    legacy.as_object_mut().unwrap().remove("held_market_count");
    let decoded: mm2_portfolio::Snapshot = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.held_market_count, None);
}

#[test]
fn malicious_checkpoint_monetary_overflow_is_rejected_before_arithmetic() {
    let mut encoded = serde_json::to_value(portfolio()).unwrap();
    encoded["returned_pico"] = serde_json::json!(u128::MAX.to_string());
    let restored: Portfolio = serde_json::from_value(encoded).unwrap();
    assert!(restored.validate().is_err());
}
