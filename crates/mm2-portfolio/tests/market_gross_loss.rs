use mm2_core::SCALE;
use mm2_portfolio::{Market, MergeReceipt, Portfolio, Provenance, RiskPolicy};

#[test]
fn lifetime_market_loss_survives_profit_merge_reentry_day_and_duplicate_receipt() {
    let mut p = Portfolio::new(1000 * SCALE, RiskPolicy::default()).unwrap();
    p.register_market(Market {
        id: "condition".into(),
        start_ms: 0,
        end_ms: 300_000,
    })
    .unwrap();
    p.record_execution_outcome_totals(
        "condition",
        [3_000_000_000_000, 2_500_000_000_000],
        [5 * SCALE, 5 * SCALE],
    )
    .unwrap();
    let receipt = MergeReceipt {
        id: "loss".into(),
        market_id: "condition".into(),
        quantity_micros: 5 * SCALE,
        fee_pico: 0,
        at_ms: 1,
        provenance: Provenance::Synthetic,
    };
    p.merge_complete_sets(receipt.clone(), true).unwrap();
    assert!(!p.merge_complete_sets(receipt, true).unwrap());
    assert_eq!(
        p.market("condition")
            .unwrap()
            .realized_gross_loss_pico()
            .unwrap(),
        Some(500_000_000_000)
    );
    assert_eq!(p.market("condition").unwrap().up.cost_pico, 0);
    p.record_execution_outcome_totals(
        "condition",
        [4_000_000_000_000, 3_500_000_000_000],
        [10 * SCALE, 10 * SCALE],
    )
    .unwrap();
    p.merge_complete_sets(
        MergeReceipt {
            id: "profit".into(),
            market_id: "condition".into(),
            quantity_micros: 5 * SCALE,
            fee_pico: 0,
            at_ms: 2,
            provenance: Provenance::Synthetic,
        },
        true,
    )
    .unwrap();
    p.advance_risk_window(86_400_000).unwrap();
    assert_eq!(p.snapshot().daily_loss_pico, "0");
    assert_eq!(
        p.market("condition")
            .unwrap()
            .realized_gross_loss_pico()
            .unwrap(),
        Some(500_000_000_000)
    );
    let recovered: Portfolio = serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
    assert_eq!(
        recovered
            .market("condition")
            .unwrap()
            .realized_gross_loss_pico()
            .unwrap(),
        Some(500_000_000_000)
    );
    recovered.validate().unwrap();
}

#[test]
fn legacy_spent_market_keeps_unknown_lifetime_loss() {
    let mut p = Portfolio::new(1000 * SCALE, RiskPolicy::default()).unwrap();
    p.register_market(Market {
        id: "condition".into(),
        start_ms: 0,
        end_ms: 300_000,
    })
    .unwrap();
    p.record_execution_outcome_totals("condition", [2_000_000_000_000, 0], [5 * SCALE, 0])
        .unwrap();
    let mut serialized = serde_json::to_value(&p).unwrap();
    serialized["markets"]["condition"]
        .as_object_mut()
        .unwrap()
        .remove("market_realized_gross_loss_pico");
    let recovered: Portfolio = serde_json::from_value(serialized).unwrap();
    assert_eq!(
        recovered
            .market("condition")
            .unwrap()
            .realized_gross_loss_pico()
            .unwrap(),
        None
    );
    recovered.validate().unwrap();
}
