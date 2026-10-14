use mm2_core::{Outcome, SCALE};
use mm2_strategy::{
    Bbo, Input, Rules, Signals, decide_inventory_aware, decide_inventory_repair,
    inventory_loss_bound,
};
fn input(q: u64, price: u64) -> Input {
    Input {
        now_ms: 10_000,
        market_start_ms: 0,
        market_end_ms: 300_000,
        up: Bbo {
            bid_micros: Some(990_000 - price),
            ask_micros: Some(1_000_000 - price),
            received_ms: 10_000,
        },
        down: Bbo {
            bid_micros: Some(price),
            ask_micros: Some(price + 10_000),
            received_ms: 10_000,
        },
        inventory_up_micros: q,
        inventory_down_micros: 0,
        reserved_up_micros: 0,
        reserved_down_micros: 0,
        available_cash_micros: 1000 * SCALE,
        tick_micros: 10_000,
        min_order_micros: 5 * SCALE,
        signals: Signals::default(),
    }
}
fn rules() -> Rules {
    Rules {
        lot_full_micros: 5 * SCALE,
        half_width_micros: 10_000,
        max_imbalance_micros: 10 * SCALE,
        ..Rules::default()
    }
}
#[test]
fn locked_loss_hedge_reduces_risk_without_claiming_profit() {
    let i = input(5 * SCALE, 800_000);
    let costs = [2_400_000_000_000, 0];
    let old = decide_inventory_aware(&rules(), &i, Some(costs)).unwrap();
    assert!(old.quotes.iter().all(|q| q.price_micros <= 520_000));
    let new = decide_inventory_repair(&rules(), &i, Some(costs)).unwrap();
    assert_eq!(new.quotes.len(), 1);
    assert_eq!(new.quotes[0].slot, "down:repair");
    assert_eq!(new.quotes[0].price_micros, 800_000);
    assert_eq!(
        inventory_loss_bound([5 * SCALE, 5 * SCALE], [costs[0], 4_000_000_000_000]),
        1_400_000_000_000
    );
}
#[test]
fn legal_minimum_dust_repair_is_symmetric_and_rejects_worsening_overbuy() {
    for owned in [Outcome::Up, Outcome::Down] {
        let mut i = input(4 * SCALE, 770_000);
        let mut costs = [880_000_000_000, 0];
        if owned == Outcome::Down {
            std::mem::swap(&mut i.up, &mut i.down);
            std::mem::swap(&mut i.inventory_up_micros, &mut i.inventory_down_micros);
            costs.swap(0, 1);
        }
        let d = decide_inventory_repair(&rules(), &i, Some(costs)).unwrap();
        assert_eq!(d.quotes.len(), 1);
        assert_eq!(d.quotes[0].quantity_micros, 5 * SCALE);
        assert_ne!(d.quotes[0].outcome, owned);
    }
    let i = input(SCALE, 820_000);
    assert!(
        decide_inventory_repair(&rules(), &i, Some([170_000_000_000, 0]))
            .unwrap()
            .quotes
            .is_empty()
    );
}
#[test]
fn full_and_every_partial_repair_fill_preserve_the_original_gross_risk_bound() {
    for shares in [1_u64, 2, 3, 4, 5, 6, 9] {
        for paid in [50_000_u64, 210_000, 480_000, 800_000] {
            for bid in [20_000_u64, 170_000, 390_000, 770_000, 970_000] {
                let q = shares * SCALE;
                let costs = [u128::from(q) * u128::from(paid), 0];
                let i = input(q, bid);
                let d = decide_inventory_repair(&rules(), &i, Some(costs)).unwrap();
                for quote in d.quotes.iter().filter(|q| q.slot.ends_with(":repair")) {
                    assert!(quote.quantity_micros >= 5 * SCALE);
                    let before = inventory_loss_bound([q, 0], costs);
                    for step in 0..=20 {
                        let fill = quote.quantity_micros * step / 20;
                        assert!(
                            inventory_loss_bound(
                                [q, fill],
                                [costs[0], u128::from(fill) * u128::from(quote.price_micros)]
                            ) <= before
                        );
                    }
                }
            }
        }
    }
}
