use mm2_core::{DesiredQuote, Outcome, SCALE};
use mm2_emulator::{Book, LatencyConfig, Level, PaperSession};
use mm2_engine::simulation::{self, Operation as Op};
use serde_json::{Value, json};

fn limits() -> mm2_engine::config::Limits {
    mm2_engine::config::Limits {
        initial_collateral_usd: "1000".into(),
        max_imbalance_shares: "10".into(),
        lot_shares: "5".into(),
        half_width: "0.01".into(),
        max_feed_age_ms: 1000,
    }
}
fn quote(outcome: Outcome, price_micros: u64) -> DesiredQuote {
    DesiredQuote {
        slot: format!("{outcome:?}:lot"),
        outcome,
        price_micros,
        quantity_micros: 5 * SCALE,
    }
}
fn paper_session() -> PaperSession {
    PaperSession::new(
        simulation::risk(&limits()).unwrap(),
        LatencyConfig::default(),
    )
    .unwrap()
}
fn book(depth: u64) -> Book {
    Book {
        bid: Some(490_000),
        ask: Some(510_000),
        bid_levels: vec![Level {
            price_micros: 490_000,
            quantity_micros: depth,
        }],
    }
}

#[test]
fn exact_cost_version_blocks_the_reproduced_excess_risk_and_replays() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/r10-risk-basis-regression.json"
    ))
    .unwrap();
    let tape = fixture["operations"].as_array().unwrap();
    let mut legacy = simulation::new(&limits()).unwrap();
    simulation::replay(&mut legacy, tape).unwrap();
    let mut legacy_snapshot = serde_json::to_value(legacy.portfolio_snapshot()).unwrap();
    // The historical fixture predates this additional accounting dimension.
    // Check it explicitly, then compare every original field without rewriting
    // the dated evidence or dropping any of its financial assertions.
    for (position, expected_loss) in legacy_snapshot["positions"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(["2600000000000", "2600000000000", "0", "0"])
    {
        assert_eq!(position["market_realized_gross_loss_pico"], expected_loss);
        position
            .as_object_mut()
            .unwrap()
            .remove("market_realized_gross_loss_pico");
    }
    assert_eq!(legacy_snapshot, fixture["legacy_after_rollover"]);
    assert!(simulation::apply(&mut legacy, &Op::ExactAccounting {}).is_err());
    let mut decision: mm2_strategy::Decision =
        serde_json::from_value(fixture["strategy_decision"].clone()).unwrap();
    assert!(!legacy.quote_risk_permitted(&decision.quotes).unwrap());
    legacy
        .guard_strategy_quotes(&mm2_strategy::Rules::default(), &mut decision)
        .unwrap();
    assert!(decision.quotes.is_empty());
    assert_eq!(decision.reason, mm2_strategy::Gate::InventoryCost);

    let mut exact_tape = vec![serde_json::to_value(Op::ExactAccounting {}).unwrap()];
    exact_tape.extend(tape.iter().cloned());
    let mut exact = simulation::new(&limits()).unwrap();
    simulation::replay(&mut exact, &exact_tape).unwrap();
    assert_eq!(exact.portfolio_snapshot().open_cost_pico, "800000000000");
    assert_eq!(exact.portfolio_snapshot().daily_loss_pico, "5200000000000");
    assert_eq!(
        exact.portfolio_snapshot().cash_pico,
        legacy.portfolio_snapshot().cash_pico
    );
    let mut decision: mm2_strategy::Decision =
        serde_json::from_value(fixture["strategy_decision"].clone()).unwrap();
    assert!(!exact.quote_risk_permitted(&decision.quotes).unwrap());
    exact
        .guard_strategy_quotes(&mm2_strategy::Rules::default(), &mut decision)
        .unwrap();
    assert!(decision.quotes.is_empty());
    assert_eq!(decision.reason, mm2_strategy::Gate::LossBudget);
    let mut replay = simulation::new(&limits()).unwrap();
    simulation::replay(&mut replay, &exact_tape).unwrap();
    assert!(simulation::same_state_without_clock(&exact, &replay).unwrap());
    let recovered: simulation::MultiMarketPaper =
        serde_json::from_value(serde_json::to_value(&exact).unwrap()).unwrap();
    recovered.validate().unwrap();
}

#[test]
fn exact_side_import_is_atomic_and_cannot_reinterpret_legacy_cost() {
    use mm2_portfolio::{Market, Portfolio, RiskPolicy};
    let mut p = Portfolio::new(1000 * SCALE, RiskPolicy::default()).unwrap();
    p.register_market(Market {
        id: "exact".into(),
        start_ms: 0,
        end_ms: 300_000,
    })
    .unwrap();
    p.record_execution_outcome_totals(
        "exact",
        [4_000_000_000_000, 500_000_000_000],
        [5 * SCALE; 2],
    )
    .unwrap();
    assert_eq!(p.market("exact").unwrap().up.cost_pico, 4_000_000_000_000);
    let before = p.clone();
    assert!(
        p.record_execution_outcome_totals(
            "exact",
            [3_000_000_000_000, 1_500_000_000_000],
            [5 * SCALE; 2]
        )
        .is_err()
    );
    assert_eq!(p, before);
    assert!(
        p.record_execution_totals("exact", 4_500_000_000_000, 5 * SCALE, 5 * SCALE, 0)
            .is_err()
    );
    assert_eq!(p, before);
    let mut legacy = Portfolio::new(1000 * SCALE, RiskPolicy::default()).unwrap();
    legacy
        .register_market(Market {
            id: "legacy".into(),
            start_ms: 0,
            end_ms: 300_000,
        })
        .unwrap();
    legacy
        .record_execution_totals("legacy", 4_500_000_000_000, 5 * SCALE, 5 * SCALE, 0)
        .unwrap();
    let old = legacy.clone();
    assert!(
        legacy
            .record_execution_outcome_totals(
                "legacy",
                [4_000_000_000_000, 500_000_000_000],
                [5 * SCALE; 2]
            )
            .is_err()
    );
    assert_eq!(legacy, old);
}

#[test]
fn public_print_does_not_invent_a_multi_level_sweep() {
    let mut s = paper_session();
    s.set_book_conservative(0, Outcome::Up, book(SCALE))
        .unwrap();
    s.quote(0, vec![quote(Outcome::Up, 490_000)]).unwrap();
    s.advance(50).unwrap();
    let lower = s
        .trade_at_price(50, Outcome::Up, 480_000, 20 * SCALE)
        .unwrap();
    assert_eq!(lower.own_filled_micros, 0);
    let same = s
        .trade_at_price(60, Outcome::Up, 490_000, 6 * SCALE)
        .unwrap();
    assert_eq!(same.own_filled_micros, 5 * SCALE);
    assert_eq!(same.external_consumed_micros, SCALE);
}

#[test]
fn boundary_public_prints_neither_fill_passive_orders_nor_fault_execution() {
    let mut s = paper_session();
    s.set_book_conservative(0, Outcome::Up, book(SCALE))
        .unwrap();
    s.quote(0, vec![quote(Outcome::Up, 490_000)]).unwrap();
    s.advance(50).unwrap();
    for price in [0, SCALE] {
        let result = s
            .trade_at_price(60, Outcome::Up, price, 10 * SCALE)
            .unwrap();
        assert_eq!(result.own_filled_micros, 0);
        assert_eq!(result.unconsumed_micros, 10 * SCALE);
    }
    assert_eq!(s.snapshot().account.spent_pico, "0");
    assert!(s.trade_at_price(60, Outcome::Up, SCALE + 1, SCALE).is_err());
    assert!(s.trade_at_price(60, Outcome::Up, 0, 0).is_err());
    assert!(s.snapshot().fault.is_none());
}

#[test]
fn observed_depth_seeds_queue_and_cancellation_does_not_grant_priority() {
    for (depth, expected_fill) in [(SCALE, 5 * SCALE), (1000 * SCALE, 0)] {
        let mut s = paper_session();
        s.set_book_conservative(0, Outcome::Up, book(depth))
            .unwrap();
        s.quote(0, vec![quote(Outcome::Up, 490_000)]).unwrap();
        s.advance(50).unwrap();
        assert_eq!(
            s.snapshot().venue_orders[0].external_queue_ahead_micros,
            depth
        );
        s.set_book_conservative(50, Outcome::Up, book(0)).unwrap();
        assert_eq!(
            s.snapshot().venue_orders[0].external_queue_ahead_micros,
            depth
        );
        assert_eq!(
            s.trade_at_price(60, Outcome::Up, 490_000, 10 * SCALE)
                .unwrap()
                .own_filled_micros,
            expected_fill
        );
    }
}

#[test]
fn a_confirmed_fill_cannot_automatically_repeat_a_lot_before_new_admission() {
    let mut s = simulation::new(&limits()).unwrap();
    simulation::apply(&mut s, &Op::ExactAccounting {}).unwrap();
    simulation::apply(
        &mut s,
        &Op::Market {
            condition_id: format!("0x{}", "a".repeat(64)),
            start_ms: Some(0),
            end_ms: Some(300_000),
        },
    )
    .unwrap();
    simulation::apply(
        &mut s,
        &Op::DepthBook {
            now_ms: 0,
            outcome: "up".into(),
            bid: 490_000,
            ask: 510_000,
            bid_levels: vec![],
        },
    )
    .unwrap();
    let desired = vec![quote(Outcome::Up, 490_000)];
    simulation::apply(
        &mut s,
        &Op::Quotes {
            now_ms: 0,
            quotes: simulation::from_core_quotes(&desired),
        },
    )
    .unwrap();
    simulation::apply(&mut s, &Op::Advance { now_ms: 50 }).unwrap();
    simulation::apply(
        &mut s,
        &Op::ObservedTradeV2 {
            now_ms: 60,
            outcome: "up".into(),
            price_micros: 490_000,
            quantity_micros: 5 * SCALE,
            fingerprint: "a".repeat(64),
        },
    )
    .unwrap();
    simulation::apply(&mut s, &Op::Advance { now_ms: 1000 }).unwrap();
    assert_eq!(s.snapshot().account.up_inventory_micros, 5 * SCALE);
    assert_eq!(s.snapshot().stats.submitted_posts, 1);
    assert!(s.snapshot().orders.is_empty());
    s.validate().unwrap();
    simulation::apply(
        &mut s,
        &Op::Quotes {
            now_ms: 1000,
            quotes: simulation::from_core_quotes(&desired),
        },
    )
    .unwrap();
    assert_eq!(s.snapshot().stats.submitted_posts, 2);
}

#[test]
fn dust_retains_only_an_existing_legal_complement_without_reposting() {
    let mut s = simulation::new(&limits()).unwrap();
    simulation::apply(&mut s, &Op::ExactAccounting {}).unwrap();
    simulation::apply(
        &mut s,
        &Op::Market {
            condition_id: format!("0x{}", "a".repeat(64)),
            start_ms: Some(0),
            end_ms: Some(300_000),
        },
    )
    .unwrap();
    for side in ["up", "down"] {
        simulation::apply(
            &mut s,
            &Op::DepthBook {
                now_ms: 0,
                outcome: side.into(),
                bid: 490_000,
                ask: 510_000,
                bid_levels: vec![],
            },
        )
        .unwrap();
    }
    simulation::apply(
        &mut s,
        &Op::Quotes {
            now_ms: 0,
            quotes: simulation::from_core_quotes(&[
                quote(Outcome::Up, 490_000),
                quote(Outcome::Down, 490_000),
            ]),
        },
    )
    .unwrap();
    simulation::apply(&mut s, &Op::Advance { now_ms: 50 }).unwrap();
    simulation::apply(
        &mut s,
        &Op::ObservedTradeV2 {
            now_ms: 60,
            outcome: "up".into(),
            price_micros: 490_000,
            quantity_micros: 10_000,
            fingerprint: "a".repeat(64),
        },
    )
    .unwrap();
    simulation::apply(&mut s, &Op::Advance { now_ms: 100 }).unwrap();
    let original = s
        .snapshot()
        .orders
        .into_iter()
        .find(|o| o.quote.outcome == Outcome::Down)
        .unwrap();
    let decision = || mm2_strategy::Decision {
        quotes: vec![],
        reason: mm2_strategy::Gate::InventoryDust,
        center_micros: None,
        half_width_micros: None,
    };
    let mut retained = decision();
    s.guard_strategy_quotes(&mm2_strategy::Rules::default(), &mut retained)
        .unwrap();
    assert_eq!(retained.quotes.len(), 1);
    assert_eq!(retained.quotes[0], original.quote);
    simulation::apply(
        &mut s,
        &Op::Quotes {
            now_ms: 100,
            quotes: simulation::from_core_quotes(&retained.quotes),
        },
    )
    .unwrap();
    assert_eq!(s.snapshot().stats.submitted_posts, 2);
    assert_eq!(
        s.snapshot()
            .orders
            .into_iter()
            .find(|o| o.id == original.id)
            .unwrap()
            .phase,
        mm2_core::OrderPhase::Live
    );
    simulation::apply(&mut s, &Op::CancelAll { now_ms: 110 }).unwrap();
    simulation::apply(&mut s, &Op::Advance { now_ms: 1000 }).unwrap();
    let mut no_order = decision();
    s.guard_strategy_quotes(&mm2_strategy::Rules::default(), &mut no_order)
        .unwrap();
    assert!(no_order.quotes.is_empty());
    assert_eq!(no_order.reason, mm2_strategy::Gate::InventoryDust);
}

#[test]
fn informational_nulls_normalized_sides_and_required_frame_atomicity() {
    use mm2_engine::domain::Market;
    let m = Market {
        slug: "btc-updown-5m-audit".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        up_token: "1".into(),
        down_token: "2".into(),
        window_start_ms: 0,
        window_end_ms: 300_000,
        tick_micro: 10_000,
    };
    for stamp in [None, Some(Value::Null)] {
        let mut bbo = json!({"event_type":"best_bid_ask","market":m.condition_id,"asset_id":"1","best_bid":"0.49","best_ask":"0.51","spread":"0.02"});
        if let Some(stamp) = stamp {
            bbo["timestamp"] = stamp;
        }
        assert!(
            mm2_engine::feeds::parse_clob(&bbo.to_string(), &m, 100)
                .unwrap()
                .is_empty()
        );
    }
    let foreign = json!({"event_type":"new_market","market":format!("0x{}","b".repeat(64)),"assets_ids":null,"timestamp":null});
    assert!(
        mm2_engine::feeds::parse_clob(&foreign.to_string(), &m, 100)
            .unwrap()
            .is_empty()
    );
    let mut trade = json!({"event_type":"last_trade_price","market":m.condition_id,"asset_id":"1","timestamp":"1000","price":"0.49","size":"1","side":"sell"});
    let lower = mm2_engine::feeds::parse_clob(&trade.to_string(), &m, 100).unwrap();
    trade["side"] = "SELL".into();
    assert_eq!(
        lower,
        mm2_engine::feeds::parse_clob(&trade.to_string(), &m, 100).unwrap()
    );
    trade["timestamp"] = Value::Null;
    assert!(mm2_engine::feeds::parse_clob(&trade.to_string(), &m, 100).is_err());
    let frame = json!([foreign, trade]);
    assert!(mm2_engine::feeds::parse_clob(&frame.to_string(), &m, 100).is_err());
}
