use mm2_core::*;
fn q(slot: &str, outcome: Outcome, price: u64, size: u64) -> DesiredQuote {
    DesiredQuote {
        slot: slot.into(),
        outcome,
        price_micros: price,
        quantity_micros: size * SCALE,
    }
}
fn posted(actions: Vec<Action>) -> Order {
    match actions
        .into_iter()
        .find(|a| matches!(a, Action::Post { .. }))
        .unwrap()
    {
        Action::Post { order } => order,
        _ => unreachable!(),
    }
}
#[test]
fn exact_outcome_costs_include_partial_and_cancel_race_fills_once() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let order = posted(
        core.quote(0, vec![q("up", Outcome::Up, 730_000, 5)])
            .unwrap(),
    );
    let partial = Observation {
        order_id: order.id,
        cumulative_filled_micros: SCALE,
        terminal: false,
    };
    core.observe(1, partial.clone()).unwrap();
    core.observe(2, partial).unwrap();
    core.cancel_all(3).unwrap();
    core.observe(
        4,
        Observation {
            order_id: order.id,
            cumulative_filled_micros: 2 * SCALE,
            terminal: true,
        },
    )
    .unwrap();
    let order = posted(
        core.quote(5, vec![q("down", Outcome::Down, 210_000, 5)])
            .unwrap(),
    );
    core.observe(
        6,
        Observation {
            order_id: order.id,
            cumulative_filled_micros: 5 * SCALE,
            terminal: true,
        },
    )
    .unwrap();
    let account = core.snapshot().account;
    assert_eq!(
        account.outcome_spent_pico,
        Some(["1460000000000".into(), "1050000000000".into()])
    );
    assert_eq!(account.spent_pico, "2510000000000");
    core.validate().unwrap();
    let restored: Core = serde_json::from_str(&serde_json::to_string(&core).unwrap()).unwrap();
    assert_eq!(restored.snapshot(), core.snapshot());
}

#[test]
fn legacy_checkpoint_side_costs_stay_unknown_and_corrupt_new_totals_are_rejected() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let order = posted(
        core.quote(0, vec![q("up", Outcome::Up, 400_000, 5)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: order.id,
            cumulative_filled_micros: 5 * SCALE,
            terminal: true,
        },
    )
    .unwrap();
    let mut value = serde_json::to_value(&core).unwrap();
    value.as_object_mut().unwrap().remove("outcome_spent_pico");
    let legacy: Core = serde_json::from_value(value.clone()).unwrap();
    legacy.validate().unwrap();
    assert_eq!(legacy.snapshot().account.spent_pico, "2000000000000");
    assert_eq!(legacy.snapshot().account.outcome_spent_pico, None);
    value["outcome_spent_pico"] = serde_json::json!([1, 0]);
    assert!(
        serde_json::from_value::<Core>(value)
            .unwrap()
            .validate()
            .is_err()
    );
}
#[test]
fn cancelling_and_unknown_orders_keep_cash_and_directional_reservations() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 500_000, 20)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 0,
            terminal: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.cancel_all(2).unwrap(),
        vec![Action::Cancel { order_id: o.id }]
    );
    assert_eq!(core.snapshot().account.reserved_micros_ceil, 10 * SCALE);
    assert_eq!(
        core.timeout(502, o.id, Operation::Cancel).unwrap(),
        vec![Action::Query { order_id: o.id }]
    );
    assert_eq!(
        core.snapshot().account.worst_up_imbalance_micros,
        20 * SCALE as i64
    );
    assert!(
        core.quote(503, vec![q("other", Outcome::Up, 500_000, 5)])
            .unwrap()
            .is_empty()
    );
    core.observe(
        504,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 2 * SCALE,
            terminal: true,
        },
    )
    .unwrap();
    assert_eq!(core.snapshot().account.up_inventory_micros, 2 * SCALE);
    assert_eq!(core.snapshot().account.spent_micros_ceil, SCALE);
}
#[test]
fn opposite_posts_never_cancel_one_anothers_possible_fill_risk() {
    let mut core = Core::new(RiskLimits {
        max_imbalance_micros: 5 * SCALE,
        ..RiskLimits::default()
    })
    .unwrap();
    let actions = core
        .quote(
            0,
            vec![
                q("up", Outcome::Up, 500_000, 5),
                q("down", Outcome::Down, 500_000, 5),
            ],
        )
        .unwrap();
    assert_eq!(actions.len(), 2);
    assert_eq!(
        core.snapshot().account.worst_up_imbalance_micros,
        5 * SCALE as i64
    );
    assert_eq!(
        core.snapshot().account.worst_down_imbalance_micros,
        5 * SCALE as i64
    );
    assert!(
        core.quote(
            1,
            vec![
                q("up", Outcome::Up, 500_000, 5),
                q("down", Outcome::Down, 500_000, 5),
                q("extra", Outcome::Up, 500_000, 5)
            ]
        )
        .unwrap()
        .is_empty()
    );
}
#[test]
fn delayed_cancel_terminal_includes_missed_fill_and_duplicates_do_not_double_spend() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 330_000, 5)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 0,
            terminal: false,
        },
    )
    .unwrap();
    core.cancel_all(2).unwrap();
    core.observe(
        3,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 1_000_001,
            terminal: true,
        },
    )
    .unwrap();
    core.observe(
        4,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 1_000_001,
            terminal: false,
        },
    )
    .unwrap();
    assert_eq!(core.snapshot().account.spent_pico, "330000330000");
    assert_eq!(core.snapshot().account.up_inventory_micros, 1_000_001);
    assert_eq!(core.snapshot().account.reserved_pico, "0");
}
#[test]
fn replacement_waits_for_terminal_confirmation_and_coalesces_latest_quote() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 500_000, 5)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 0,
            terminal: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.quote(2, vec![q("up", Outcome::Up, 490_000, 5)])
            .unwrap(),
        vec![Action::Cancel { order_id: o.id }]
    );
    assert!(
        core.quote(3, vec![q("up", Outcome::Up, 480_000, 5)])
            .unwrap()
            .is_empty()
    );
    let next = posted(
        core.observe(
            4,
            Observation {
                order_id: o.id,
                cumulative_filled_micros: SCALE,
                terminal: true,
            },
        )
        .unwrap(),
    );
    assert_eq!(next.quote.price_micros, 480_000);
    assert!(next.id > o.id);
}
#[test]
fn clipped_order_retains_request_identity_without_automatic_churn() {
    let mut core = Core::new(RiskLimits {
        initial_cash_micros: 3 * SCALE,
        ..RiskLimits::default()
    })
    .unwrap();
    let requested = q("up", Outcome::Up, 500_000, 20);
    let o = posted(core.quote(0, vec![requested.clone()]).unwrap());
    assert_eq!(o.quote.quantity_micros, 6 * SCALE);
    assert!(
        core.observe(
            1,
            Observation {
                order_id: o.id,
                cumulative_filled_micros: 0,
                terminal: false
            }
        )
        .unwrap()
        .is_empty()
    );
    assert!(core.quote(2, vec![requested]).unwrap().is_empty());
}
#[test]
fn invalid_quotes_are_atomic_and_persisted_core_validates() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 500_000, 5)])
            .unwrap(),
    );
    assert!(
        core.quote(1, vec![q("up", Outcome::Up, 501_000, 5)])
            .is_err()
    );
    assert_eq!(core.snapshot().orders[0].id, o.id);
    let bytes = serde_json::to_vec(&core).unwrap();
    let restored: Core = serde_json::from_slice(&bytes).unwrap();
    restored.validate().unwrap();
    assert_eq!(core.snapshot(), restored.snapshot());
}
#[test]
fn exact_fragment_cost_conserves_budget() {
    let mut core = Core::new(RiskLimits {
        initial_cash_micros: 2 * SCALE,
        ..RiskLimits::default()
    })
    .unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 330_000, 5)])
            .unwrap(),
    );
    for quantity in [1, 2, 3, 4, 5_000_000] {
        core.observe(
            quantity,
            Observation {
                order_id: o.id,
                cumulative_filled_micros: quantity,
                terminal: quantity == 5_000_000,
            },
        )
        .unwrap();
    }
    let a = core.snapshot().account;
    assert_eq!(a.spent_pico, "1650000000000");
    assert_eq!(a.available_pico, "350000000000");
}
#[test]
fn risk_limits_reduction_cancels_without_resetting_account() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let o = posted(
        core.quote(0, vec![q("up", Outcome::Up, 500_000, 20)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: o.id,
            cumulative_filled_micros: 0,
            terminal: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.set_risk_limits(
            RiskLimits {
                max_imbalance_micros: 5 * SCALE,
                ..RiskLimits::default()
            },
            2
        )
        .unwrap(),
        vec![Action::Cancel { order_id: o.id }]
    );
    assert_eq!(core.snapshot().account.reserved_micros_ceil, 10 * SCALE);
}

#[test]
fn contradictory_late_fill_is_rejected_after_terminal_reconciliation() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let order = posted(
        core.quote(0, vec![q("up", Outcome::Up, 500_000, 5)])
            .unwrap(),
    );
    core.observe(
        1,
        Observation {
            order_id: order.id,
            cumulative_filled_micros: 0,
            terminal: true,
        },
    )
    .unwrap();
    assert!(
        core.observe(
            2,
            Observation {
                order_id: order.id,
                cumulative_filled_micros: SCALE,
                terminal: false
            }
        )
        .is_err()
    );
    assert_eq!(core.snapshot().account.up_inventory_micros, 0);
}
