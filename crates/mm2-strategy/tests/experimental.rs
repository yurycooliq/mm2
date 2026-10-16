use mm2_core::{Outcome, SCALE};
use mm2_strategy::{
    Bbo, Gate, Input, PaperPolicy, Rules, Signals, decide_experimental, inventory_loss_bound,
};

fn rules() -> Rules {
    Rules {
        lot_full_micros: 5 * SCALE,
        half_width_micros: 10_000,
        max_imbalance_micros: 10 * SCALE,
        ..Rules::default()
    }
}
fn input(owned: u64, complement_bid: u64) -> Input {
    Input {
        now_ms: 10_000,
        market_start_ms: 0,
        market_end_ms: 300_000,
        up: Bbo {
            bid_micros: Some(990_000 - complement_bid),
            ask_micros: Some(SCALE - complement_bid),
            received_ms: 10_000,
        },
        down: Bbo {
            bid_micros: Some(complement_bid),
            ask_micros: Some(complement_bid + 10_000),
            received_ms: 10_000,
        },
        inventory_up_micros: owned,
        inventory_down_micros: 0,
        reserved_up_micros: 0,
        reserved_down_micros: 0,
        available_cash_micros: 1000 * SCALE,
        tick_micros: 10_000,
        min_order_micros: 5 * SCALE,
        signals: Signals::default(),
    }
}

#[test]
fn paired_entry_requires_both_equal_legal_lots_and_stops_late() {
    let policy = PaperPolicy::default();
    let mut i = input(0, 490_000);
    let d = decide_experimental(&rules(), &i, Some([0, 0]), &policy, 0).unwrap();
    assert_eq!(d.quotes.len(), 2);
    assert_eq!(d.quotes[0].quantity_micros, d.quotes[1].quantity_micros);
    assert!(d.quotes.iter().map(|q| q.price_micros).sum::<u64>() <= 980_000);
    i.available_cash_micros = 3 * SCALE;
    let d = decide_experimental(&rules(), &i, Some([0, 0]), &policy, 0).unwrap();
    assert_eq!(d.reason, Gate::PairedEntry);
    assert!(d.quotes.is_empty());
    i.now_ms = 240_000;
    i.up.received_ms = i.now_ms;
    i.down.received_ms = i.now_ms;
    assert_eq!(
        decide_experimental(&rules(), &i, Some([0, 0]), &policy, 0)
            .unwrap()
            .reason,
        Gate::CloseGuard
    );
}

#[test]
fn normal_hedge_remains_cost_capped_until_repair_age_and_close_trigger() {
    let policy = PaperPolicy::default();
    let mut i = input(5 * SCALE, 550_000);
    let costs = [2_400_000_000_000, 0]; // 0.48 paid; ordinary complement <=0.50
    let d = decide_experimental(&rules(), &i, Some(costs), &policy, 14_999).unwrap();
    assert_eq!(d.quotes[0].slot, "down:hedge");
    assert_eq!(d.quotes[0].price_micros, 500_000);
    let d = decide_experimental(&rules(), &i, Some(costs), &policy, 15_000).unwrap();
    assert_eq!(d.quotes[0].slot, "down:repair");
    assert_eq!(d.quotes[0].price_micros, 550_000); // exactly $0.15 locked loss
    i.now_ms = 270_000;
    i.up.received_ms = i.now_ms;
    i.down.received_ms = i.now_ms;
    assert_eq!(
        decide_experimental(&rules(), &i, Some(costs), &policy, 0)
            .unwrap()
            .quotes[0]
            .slot,
        "down:repair"
    );
}

#[test]
fn r17_expensive_repairs_are_rejected_without_deleting_owned_inventory() {
    for paid in [190_000, 360_000] {
        let i = input(5 * SCALE, 970_000);
        let costs = [u128::from(paid) * u128::from(5 * SCALE), 0];
        let d = decide_experimental(&rules(), &i, Some(costs), &PaperPolicy::default(), 200_000)
            .unwrap();
        assert_eq!(d.quotes[0].slot, "down:hedge");
        assert!(d.quotes[0].price_micros <= SCALE - paid - 20_000);
    }
}

#[test]
fn dust_overbuy_is_legal_symmetric_and_every_partial_fill_stays_bounded() {
    for side in [Outcome::Up, Outcome::Down] {
        let mut i = input(4 * SCALE, 770_000);
        let mut costs = [880_000_000_000, 0];
        if side == Outcome::Down {
            std::mem::swap(&mut i.up, &mut i.down);
            std::mem::swap(&mut i.inventory_up_micros, &mut i.inventory_down_micros);
            costs.swap(0, 1);
        }
        let d = decide_experimental(&rules(), &i, Some(costs), &PaperPolicy::default(), 15_000)
            .unwrap();
        assert_eq!(d.quotes.len(), 1);
        assert!(d.quotes[0].slot.ends_with(":repair"));
        assert_eq!(d.quotes[0].quantity_micros, 5 * SCALE);
        let before = inventory_loss_bound([i.inventory_up_micros, i.inventory_down_micros], costs);
        for step in 0..=100 {
            let fill = d.quotes[0].quantity_micros * step / 100;
            let mut qs = [i.inventory_up_micros, i.inventory_down_micros];
            let mut cs = costs;
            let lagging = usize::from(d.quotes[0].outcome == Outcome::Down);
            qs[lagging] += fill;
            cs[lagging] += u128::from(fill) * u128::from(d.quotes[0].price_micros);
            assert!(inventory_loss_bound(qs, cs) <= before);
        }
    }
}

#[test]
fn missing_costs_and_unreviewed_directional_policy_fail_closed() {
    let i = input(5 * SCALE, 550_000);
    assert!(
        decide_experimental(&rules(), &i, None, &PaperPolicy::default(), 15_000)
            .unwrap()
            .quotes
            .is_empty()
    );
    let policy = PaperPolicy {
        paired_entry_only: false,
        ..PaperPolicy::default()
    };
    assert!(
        decide_experimental(&rules(), &i, Some([2_400_000_000_000, 0]), &policy, 15_000).is_err()
    );
}
