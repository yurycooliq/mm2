use mm2_engine::domain::*;

fn config() -> CoreConfig {
    CoreConfig {
        initial_collateral_micro: 100 * MICRO,
        max_imbalance_micro: 10 * MICRO,
        lot_micro: 5 * MICRO,
        half_width_micro: 10_000,
        max_feed_age_ms: 1000,
    }
}
fn market() -> Market {
    Market {
        slug: "btc-updown-5m-test".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        up_token: "1".into(),
        down_token: "2".into(),
        window_start_ms: 1000,
        window_end_ms: 301_000,
        tick_micro: 10_000,
    }
}
fn book(outcome: Outcome, exchange_ms: u64, observed_mono_ms: u64) -> FeedEvent {
    FeedEvent::Snapshot {
        condition_id: market().condition_id,
        outcome,
        exchange_ms,
        observed_mono_ms,
        bids: vec![Level {
            price_micro: 490_000,
            quantity_micro: 20 * MICRO,
        }],
        asks: vec![Level {
            price_micro: 510_000,
            quantity_micro: 20 * MICRO,
        }],
    }
}
fn ready() -> Core {
    let mut core = Core::new(config()).unwrap();
    core.set_market(market()).unwrap();
    core.apply(book(Outcome::Up, 1000, 100)).unwrap();
    core.apply(book(Outcome::Down, 1000, 100)).unwrap();
    core
}

#[test]
fn decimal_construction_is_exact_checked_and_shared() {
    assert_eq!(parse_micro("0.490000000"), Ok(490_000));
    assert_eq!(parse_micro("5.000001"), Ok(5_000_001));
    assert_eq!(parse_micro("18446744073709.551615"), Ok(u64::MAX));
    for invalid in [
        "",
        "-1",
        "+1",
        "1e-6",
        ".1",
        "1.",
        " 1",
        "1.0000001",
        "18446744073709.551616",
    ] {
        assert!(parse_micro(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn starts_paused_resume_only_proposes_and_pause_clears_reservation() {
    let mut core = ready();
    assert!(
        core.evaluate(1000, 100)
            .stop_reasons
            .contains(&StopReason::Paused)
    );
    core.control(Action::Resume).unwrap();
    let decision = core.evaluate(1000, 100);
    assert_eq!(decision.mode, Mode::Shadow);
    assert_eq!(decision.intents.len(), 2);
    assert_eq!(decision.hypothetical_reservation_micro, 4_800_000);
    assert_eq!(decision.worst_case_imbalance_micro, 5 * MICRO);
    assert!(decision.experimental_book_only);
    assert!(!decision.exact_twap_available);
    core.control(Action::Pause).unwrap();
    assert!(core.snapshot().intents.is_empty());
    assert_eq!(core.snapshot().hypothetical_reservation_micro, 0);
}

#[test]
fn both_sides_share_a_finite_budget_and_round_reservations_up() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    let mut cfg = config();
    cfg.lot_micro += 1;
    cfg.initial_collateral_micro = 4_800_002;
    core.set_config(cfg.clone()).unwrap();
    let decision = core.evaluate(1000, 100);
    assert_eq!(decision.intents.len(), 2);
    assert_eq!(decision.hypothetical_reservation_micro, 4_800_002);
    cfg.initial_collateral_micro = 4_800_001;
    core.set_config(cfg).unwrap();
    let decision = core.evaluate(1000, 100);
    assert!(decision.stop_reasons.contains(&StopReason::BudgetExceeded));
    assert!(decision.intents.is_empty());
    assert_eq!(decision.hypothetical_reservation_micro, 0);
}

#[test]
fn paired_proposals_do_not_net_possible_one_sided_inventory_to_zero() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    let mut cfg = config();
    cfg.max_imbalance_micro = 5 * MICRO - 1;
    core.set_config(cfg).unwrap();
    let decision = core.evaluate(1000, 100);
    assert!(
        decision
            .stop_reasons
            .contains(&StopReason::ImbalanceExceeded)
    );
    assert!(decision.intents.is_empty());
}

#[test]
fn stale_or_future_clocks_never_preserve_old_proposals() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    assert_eq!(core.evaluate(1000, 100).intents.len(), 2);
    let stale = core.evaluate(2001, 1101);
    assert!(
        stale
            .stop_reasons
            .contains(&StopReason::StaleBook(Outcome::Up))
    );
    assert!(core.snapshot().intents.is_empty());
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    assert!(
        core.evaluate(1000, 99)
            .stop_reasons
            .contains(&StopReason::InvalidClock)
    );
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    core.evaluate(1000, 100);
    assert!(
        core.evaluate(1000, 99)
            .stop_reasons
            .contains(&StopReason::InvalidClock)
    );
    assert!(!core.snapshot().up_book.initialized);
}

#[test]
fn one_outcome_update_cannot_certify_the_other_outcome_or_old_exchange_data() {
    let mut core = Core::new(config()).unwrap();
    core.set_market(market()).unwrap();
    core.control(Action::Resume).unwrap();
    core.apply(book(Outcome::Up, 1000, 100)).unwrap();
    assert!(
        core.evaluate(1000, 100)
            .stop_reasons
            .contains(&StopReason::MissingBook(Outcome::Down))
    );
    core.apply(book(Outcome::Down, 1000, 100)).unwrap();
    core.apply(book(Outcome::Up, 2001, 1101)).unwrap();
    let decision = core.evaluate(2001, 1101);
    assert!(
        decision
            .stop_reasons
            .contains(&StopReason::StaleBook(Outcome::Down))
    );
    assert!(decision.intents.is_empty());
    core.apply(book(Outcome::Down, 1000, 1102)).unwrap();
    let decision = core.evaluate(2002, 1102);
    assert!(
        decision
            .stop_reasons
            .contains(&StopReason::StaleBook(Outcome::Down))
    );
    assert!(decision.intents.is_empty());
}

#[test]
fn arrival_and_exchange_regressions_invalidate_books_and_atomic_batch_failures_do_not_leak() {
    for event in [book(Outcome::Up, 1001, 99), book(Outcome::Up, 999, 101)] {
        let mut core = ready();
        core.control(Action::Resume).unwrap();
        core.evaluate(1000, 100);
        assert!(core.apply(event).is_err());
        assert!(!core.snapshot().up_book.initialized);
        assert!(core.snapshot().intents.is_empty());
    }
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    let invalid = FeedEvent::Delta {
        condition_id: market().condition_id,
        exchange_ms: 1001,
        observed_mono_ms: 101,
        changes: vec![
            Change {
                outcome: Outcome::Up,
                side: BookSide::Bid,
                level: Level {
                    price_micro: 480_000,
                    quantity_micro: MICRO,
                },
            },
            Change {
                outcome: Outcome::Down,
                side: BookSide::Bid,
                level: Level {
                    price_micro: MICRO,
                    quantity_micro: MICRO,
                },
            },
        ],
    };
    assert!(core.apply(invalid).is_err());
    assert!(!core.snapshot().down_book.initialized);
    assert!(
        core.evaluate(1001, 101)
            .stop_reasons
            .contains(&StopReason::InvalidInput)
    );
}

#[test]
fn atomic_batch_can_replace_touch_without_an_intermediate_crossing() {
    let mut core = ready();
    core.apply(FeedEvent::Delta {
        condition_id: market().condition_id,
        exchange_ms: 1001,
        observed_mono_ms: 101,
        changes: vec![
            Change {
                outcome: Outcome::Up,
                side: BookSide::Bid,
                level: Level {
                    price_micro: 520_000,
                    quantity_micro: MICRO,
                },
            },
            Change {
                outcome: Outcome::Up,
                side: BookSide::Ask,
                level: Level {
                    price_micro: 510_000,
                    quantity_micro: 0,
                },
            },
            Change {
                outcome: Outcome::Up,
                side: BookSide::Ask,
                level: Level {
                    price_micro: 530_000,
                    quantity_micro: MICRO,
                },
            },
        ],
    })
    .unwrap();
    assert_eq!(core.snapshot().up_book.best_bid_micro, Some(520_000));
}

#[test]
fn ticks_are_independent_and_incompatible_transitions_fail_closed() {
    for tick in [100_000, 10_000, 5_000, 2_500, 1_000, 100] {
        assert!(valid_tick_micro(tick));
        let mut metadata = market();
        metadata.tick_micro = tick;
        assert!(Core::new(config()).unwrap().set_market(metadata).is_ok());
    }
    for tick in [0, 1, 2000, 20_000, MICRO, u64::MAX] {
        assert!(!valid_tick_micro(tick));
    }
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    core.apply(FeedEvent::Tick {
        condition_id: market().condition_id,
        outcome: Outcome::Up,
        exchange_ms: 1001,
        observed_mono_ms: 101,
        old_tick_micro: Some(10_000),
        new_tick_micro: 1000,
    })
    .unwrap();
    assert_eq!(core.snapshot().up_book.tick_micro, 1000);
    assert_eq!(core.snapshot().down_book.tick_micro, 10_000);
    assert!(
        core.evaluate(1000, 100)
            .stop_reasons
            .contains(&StopReason::InvalidClock)
    );
    assert!(
        core.apply(FeedEvent::Tick {
            condition_id: market().condition_id,
            outcome: Outcome::Up,
            exchange_ms: 1002,
            observed_mono_ms: 102,
            old_tick_micro: Some(10_000),
            new_tick_micro: 100
        })
        .is_err()
    );
    assert!(core.snapshot().intents.is_empty());
    assert!(!core.snapshot().up_book.initialized);
}

#[test]
fn source_disconnects_and_rollover_clear_only_the_intended_data_and_never_create_fills() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    core.apply(FeedEvent::Reference {
        price_micro: 60_000 * MICRO,
        exchange_ms: 1000,
        observed_mono_ms: 100,
    })
    .unwrap();
    core.evaluate(1000, 100);
    core.apply(FeedEvent::Disconnect {
        source: FeedSource::Binance,
        reason: "network".into(),
    })
    .unwrap();
    assert!(core.snapshot().reference.is_none());
    assert!(core.snapshot().up_book.initialized);
    assert!(core.snapshot().intents.is_empty());
    core.apply(FeedEvent::Disconnect {
        source: FeedSource::Clob,
        reason: "network".into(),
    })
    .unwrap();
    assert!(!core.snapshot().up_book.initialized);
    assert!(!core.snapshot().down_book.initialized);
    let mut next = market();
    next.condition_id = format!("0x{}", "b".repeat(64));
    next.window_start_ms = 301_000;
    next.window_end_ms = 601_000;
    core.set_market(next).unwrap();
    assert_eq!(core.snapshot().hypothetical_reservation_micro, 0);
    assert!(core.apply(book(Outcome::Up, 301_000, 1000)).is_err());
    assert!(core.snapshot().intents.is_empty());
    assert_eq!(core.snapshot().config.initial_collateral_micro, 100 * MICRO);
}

#[test]
fn kill_latch_survives_pause_config_market_and_feed_changes_until_explicit_reset() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    core.evaluate(1000, 100);
    core.control(Action::Kill).unwrap();
    core.control(Action::Pause).unwrap();
    core.set_config(config()).unwrap();
    core.set_market(market()).unwrap();
    core.apply(book(Outcome::Up, 1001, 101)).unwrap();
    assert!(core.control(Action::Resume).is_err());
    assert_eq!(core.snapshot().mode, Mode::Killed);
    assert!(core.snapshot().intents.is_empty());
    core.control(Action::Reset).unwrap();
    assert_eq!(core.snapshot().mode, Mode::Paused);
    assert!(!core.snapshot().up_book.initialized);
    core.control(Action::Resume).unwrap();
    assert!(core.evaluate(1001, 101).intents.is_empty());
}

#[test]
fn invalid_config_is_rejected_and_pauses_instead_of_silently_reusing_old_proposals() {
    let mut core = ready();
    core.control(Action::Resume).unwrap();
    core.evaluate(1000, 100);
    let mut invalid = config();
    invalid.max_feed_age_ms = 0;
    assert!(core.set_config(invalid.clone()).is_err());
    assert_eq!(core.snapshot().mode, Mode::Paused);
    assert!(Core::new(invalid).is_err());
    assert!(core.snapshot().intents.is_empty());
}
