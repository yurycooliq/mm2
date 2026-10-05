use mm2_core::{Outcome, SCALE};
use mm2_strategy::{Bbo, Gate, Input, Rules, Signals, Widening, decide, half_width, session_lot};

fn input() -> Input {
    Input {
        now_ms: 54_000_000,
        market_start_ms: 53_900_000,
        market_end_ms: 54_300_000,
        up: Bbo {
            bid_micros: Some(600_000),
            ask_micros: Some(610_000),
            received_ms: 54_000_000,
        },
        down: Bbo {
            bid_micros: Some(390_000),
            ask_micros: Some(400_000),
            received_ms: 54_000_000,
        },
        inventory_up_micros: 0,
        inventory_down_micros: 0,
        reserved_up_micros: 0,
        reserved_down_micros: 0,
        available_cash_micros: 1_000 * SCALE,
        tick_micros: 10_000,
        min_order_micros: 5 * SCALE,
        signals: Signals::default(),
    }
}

#[test]
fn source_v1_prices_and_whole_inventory_hedge() {
    let rules = Rules {
        lot_full_micros: 502 * SCALE,
        shrink_ppm: 950_000,
        max_imbalance_micros: 2_000 * SCALE,
        ..Rules::default()
    };
    let mut i = input();
    i.inventory_down_micros = 322_780_000;
    let d = decide(&rules, &i).unwrap();
    assert_eq!(d.reason, Gate::Quote);
    assert_eq!(d.quotes[0].slot, "up:hedge");
    assert_eq!(d.quotes[0].quantity_micros, 322_780_000);
    assert_eq!(d.quotes[0].price_micros, 550_000);
    assert_eq!(d.quotes[1].price_micros, 550_000);
    assert_eq!(d.quotes[2].price_micros, 360_000);
    assert_eq!(d.quotes[1].quantity_micros, 502 * SCALE);
}

#[test]
fn utc_schedule_has_exact_boundaries_and_odd_lot_rounding() {
    // Unix epoch Thursday; Saturday starts after 2 days.
    assert_eq!(session_lot(12 * 3_600_000, 502 * SCALE), 502 * SCALE);
    assert_eq!(session_lot(23 * 3_600_000 - 1, 502 * SCALE), 502 * SCALE);
    assert_eq!(session_lot(23 * 3_600_000, 502 * SCALE), 251 * SCALE);
    assert_eq!(
        session_lot(2 * 86_400_000 + 15 * 3_600_000, 11 * SCALE),
        6 * SCALE
    );
}

#[test]
fn stale_missing_crossed_hollow_and_close_books_cancel_every_slot() {
    let r = Rules {
        stub_price_micros: Some(10_000),
        ..Rules::default()
    };
    for (mut i, expected) in [
        (input(), Gate::StaleBook),
        (input(), Gate::InvalidBook),
        (input(), Gate::HollowBook),
        (input(), Gate::CloseGuard),
    ] {
        match expected {
            Gate::StaleBook => i.down.received_ms -= 1_001,
            Gate::InvalidBook => i.down.ask_micros = None,
            Gate::HollowBook => i.up.bid_micros = Some(300_000),
            Gate::CloseGuard => i.now_ms = i.market_end_ms - r.close_guard_ms,
            _ => unreachable!(),
        }
        let d = decide(&r, &i).unwrap();
        assert_eq!(d.reason, expected);
        assert!(d.quotes.is_empty());
    }
    let mut i = input();
    i.up.bid_micros = i.up.ask_micros;
    assert_eq!(decide(&r, &i).unwrap().reason, Gate::InvalidBook);
    i = input();
    i.down.received_ms += 1;
    assert_eq!(decide(&r, &i).unwrap().reason, Gate::StaleBook);
}

#[test]
fn reconstruction_requires_all_point_in_time_signals() {
    let r = Rules::reconstructed_btc5m();
    let mut i = input();
    assert_eq!(decide(&r, &i).unwrap().reason, Gate::MissingSignal);
    i.signals = Signals {
        mid_vol_micros: Some(30_000),
        model_up_micros: Some(600_000),
        imbalance_ppm: Some(0),
        flow_ppm: Some(0),
        received_ms: i.now_ms,
    };
    assert_eq!(half_width(&r, &i).unwrap(), 45_000);
    i.market_end_ms = i.now_ms + 150_000;
    assert_eq!(half_width(&r, &i).unwrap(), 60_000);
    i.market_end_ms = i.now_ms + 60_000;
    i.signals.mid_vol_micros = Some(25_000);
    assert_eq!(half_width(&r, &i).unwrap(), 79_057);
    i.signals.received_ms -= 1_001;
    assert_eq!(decide(&r, &i).unwrap().reason, Gate::StaleSignal);
}

#[test]
fn actual_down_ask_is_used_for_post_only_price() {
    let r = Rules {
        half_width_micros: 0,
        shrink_ppm: SCALE,
        ..Rules::default()
    };
    let mut i = input();
    i.down = Bbo {
        bid_micros: Some(310_000),
        ask_micros: Some(320_000),
        received_ms: i.now_ms,
    };
    let d = decide(&r, &i).unwrap();
    assert!(d.quotes.iter().all(|q| q.price_micros
        < if q.outcome == Outcome::Up {
            610_000
        } else {
            320_000
        }));
    assert_eq!(
        d.quotes
            .iter()
            .find(|q| q.outcome == Outcome::Down)
            .unwrap()
            .price_micros,
        310_000
    );
}

#[test]
fn adverse_grid_respects_cash_ticks_and_independent_worst_fill_caps() {
    let r = Rules {
        lot_full_micros: 11 * SCALE,
        stub_price_micros: Some(10_000),
        ..Rules::default()
    };
    for tick in [10_000, 5_000, 2_500, 1_000, 100] {
        for up in [0, 5 * SCALE, 19 * SCALE, 50 * SCALE] {
            for down in [0, 5 * SCALE, 19 * SCALE, 50 * SCALE] {
                for cash in [0, SCALE, 5 * SCALE, 20 * SCALE, 100 * SCALE] {
                    let mut i = input();
                    i.inventory_up_micros = up;
                    i.inventory_down_micros = down;
                    i.available_cash_micros = cash;
                    i.reserved_up_micros = 3 * SCALE;
                    i.reserved_down_micros = 2 * SCALE;
                    i.tick_micros = tick;
                    let d = decide(&r, &i).unwrap();
                    assert!(d.quotes.len() <= 6);
                    let cost: u128 = d
                        .quotes
                        .iter()
                        .map(|q| u128::from(q.price_micros) * u128::from(q.quantity_micros))
                        .sum();
                    assert!(cost <= u128::from(cash) * u128::from(SCALE));
                    for q in &d.quotes {
                        assert!(q.quantity_micros >= i.min_order_micros);
                        assert_eq!(q.price_micros % tick, 0);
                    }
                    for (outcome, base, reserved) in [
                        (
                            Outcome::Up,
                            i128::from(up) - i128::from(down),
                            i.reserved_up_micros,
                        ),
                        (
                            Outcome::Down,
                            i128::from(down) - i128::from(up),
                            i.reserved_down_micros,
                        ),
                    ] {
                        let added: u64 = d
                            .quotes
                            .iter()
                            .filter(|q| q.outcome == outcome)
                            .map(|q| q.quantity_micros)
                            .sum();
                        if added > 0 {
                            assert!(
                                base + i128::from(reserved) + i128::from(added)
                                    <= i128::from(r.max_imbalance_micros)
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn invalid_quantities_never_overflow_and_rules_are_frozen_serializable() {
    let mut i = input();
    i.inventory_up_micros = u64::MAX;
    assert!(decide(&Rules::default(), &i).is_err());
    let r = Rules::reconstructed_btc5m();
    assert_eq!(
        serde_json::from_str::<Rules>(&serde_json::to_string(&r).unwrap()).unwrap(),
        r
    );
    let r = Rules {
        widening: Widening::VolTime {
            k_ppm: 100 * SCALE,
            tau_ref_ms: 86_400_000,
        },
        ..Rules::default()
    };
    i = input();
    i.signals.mid_vol_micros = Some(SCALE);
    assert!(half_width(&r, &i).unwrap() > SCALE);
    assert!(decide(&r, &i).unwrap().quotes.is_empty());
}
