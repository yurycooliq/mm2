use mm2_core::{Outcome, SCALE};
use mm2_portfolio::{Market, Portfolio, Provenance, ResolutionReceipt, RiskPolicy};

const DAY: u64 = 86_400_000;

fn loss_portfolio(loss_micros: u64) -> Portfolio {
    let mut portfolio = Portfolio::new(100 * SCALE, RiskPolicy::default()).unwrap();
    portfolio
        .register_market(Market {
            id: "old".into(),
            start_ms: 0,
            end_ms: 300_000,
        })
        .unwrap();
    portfolio
        .register_market(Market {
            id: "next".into(),
            start_ms: DAY,
            end_ms: DAY + 300_000,
        })
        .unwrap();
    portfolio
        .record_execution_totals(
            "old",
            u128::from(loss_micros) * u128::from(SCALE),
            20 * SCALE,
            0,
            0,
        )
        .unwrap();
    portfolio.redeem(receipt("old", DAY - 1), true).unwrap();
    portfolio
}

fn receipt(market: &str, at_ms: u64) -> ResolutionReceipt {
    ResolutionReceipt {
        id: format!("close:{market}"),
        market_id: market.into(),
        winning_outcome: Outcome::Down,
        fee_pico: 0,
        at_ms,
        finality_confirmed: true,
        provenance: Provenance::Synthetic,
    }
}

#[test]
fn midnight_without_fills_reopens_only_the_daily_window() {
    let mut portfolio = loss_portfolio(9 * SCALE);
    assert!(
        !portfolio
            .permits_execution_risk("next", [5 * SCALE, 0], SCALE / 2)
            .unwrap()
    );
    let before = portfolio.snapshot();
    assert!(portfolio.risk_window_due(DAY));
    portfolio.advance_risk_window(DAY).unwrap();
    assert!(!portfolio.risk_window_due(DAY + 1000));
    assert!(
        portfolio
            .permits_execution_risk("next", [5 * SCALE, 0], SCALE / 2)
            .unwrap()
    );
    let after = portfolio.snapshot();
    assert_eq!(after.daily_loss_pico, "0");
    assert_eq!(after.risk_day_utc, Some(1));
    let mut expected = before;
    expected.daily_loss_pico = "0".into();
    expected.risk_day_utc = Some(1);
    expected.risk_clock_ms = DAY;
    assert_eq!(after, expected);
}

#[test]
fn midnight_preserves_old_inventory_and_drawdown_risk() {
    let mut portfolio = loss_portfolio(9 * SCALE);
    portfolio
        .register_market(Market {
            id: "unresolved".into(),
            start_ms: 0,
            end_ms: 300_000,
        })
        .unwrap();
    portfolio
        .record_execution_totals(
            "unresolved",
            8 * u128::from(SCALE) * u128::from(SCALE),
            20 * SCALE,
            0,
            0,
        )
        .unwrap();
    let before = portfolio.snapshot();
    portfolio.advance_risk_window(DAY).unwrap();
    assert_eq!(portfolio.snapshot().drawdown_pico, before.drawdown_pico);
    assert_eq!(portfolio.snapshot().open_cost_pico, before.open_cost_pico);
    assert!(
        !portfolio
            .permits_execution_risk("next", [5 * SCALE, 0], SCALE / 2)
            .unwrap()
    );
}

#[test]
fn midnight_never_clears_a_latched_loss_halt() {
    let mut portfolio = loss_portfolio(10 * SCALE);
    let halt = portfolio.snapshot().halted;
    assert!(halt.is_some());
    portfolio.advance_risk_window(DAY).unwrap();
    assert_eq!(portfolio.snapshot().halted, halt);
    assert!(
        !portfolio
            .permits_execution_risk("next", [5 * SCALE, 0], SCALE / 2)
            .unwrap()
    );
}

#[test]
fn late_old_market_close_is_charged_when_processed_and_does_not_rewind_day() {
    let mut portfolio = Portfolio::new(100 * SCALE, RiskPolicy::default()).unwrap();
    portfolio
        .register_market(Market {
            id: "old".into(),
            start_ms: 0,
            end_ms: 300_000,
        })
        .unwrap();
    portfolio
        .record_execution_totals(
            "old",
            2 * u128::from(SCALE) * u128::from(SCALE),
            5 * SCALE,
            0,
            0,
        )
        .unwrap();
    portfolio.advance_risk_window(DAY).unwrap();
    let close = receipt("old", DAY + 10);
    assert!(portfolio.redeem(close.clone(), true).unwrap());
    assert_eq!(portfolio.snapshot().risk_day_utc, Some(1));
    assert_eq!(portfolio.snapshot().daily_loss_pico, "2000000000000");
    let once = portfolio.snapshot();
    assert!(!portfolio.redeem(close, true).unwrap());
    assert_eq!(portfolio.snapshot(), once);
}

#[test]
fn backward_risk_window_is_rejected_without_mutation() {
    let mut portfolio = loss_portfolio(9 * SCALE);
    portfolio.advance_risk_window(DAY + 10).unwrap();
    let before = portfolio.clone();
    assert!(portfolio.advance_risk_window(DAY - 1).is_err());
    assert_eq!(portfolio, before);
}
