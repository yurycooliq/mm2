//! Independent monetary regression cases. Public key 1 and synthetic receipts only.
use super::*;
use crate::{ChainSettings, LiveOrder, LivePosition, LiveQuote, OrderObservation, OrderPhase};

const SHARE: u64 = 1_000_000;
const USD: u128 = 1_000_000_000_000;
fn condition() -> String {
    format!("0x{}", "a".repeat(64))
}
fn plan(operation: ChainOperation) -> ChainPlan {
    ChainPlan {
        id: "independent-closeout".into(),
        condition_id: condition(),
        token_ids: ["11".into(), "22".into()],
        operation,
    }
}
fn position(token: &str, outcome: mm2_core::Outcome, qty: u64, cost: u128) -> LivePosition {
    LivePosition {
        condition_id: condition(),
        token_id: token.into(),
        outcome,
        quantity_micros: qty,
        cost_pico: cost.to_string(),
        redeemable: false,
        mergeable: true,
    }
}
fn insert_position(
    ledger: &mut Ledger,
    token: &str,
    outcome: mm2_core::Outcome,
    qty: u64,
    cost: u128,
) {
    let p = position(token, outcome, qty, cost);
    let key = format!("{}:{token}", condition());
    ledger.positions.insert(key.clone(), p.clone());
    ledger.position_floor.insert(key, p);
}
fn insert_owned(ledger: &mut Ledger, token: &str, qty: u64, cost: u128, day: u64, daily_qty: u64) {
    ledger.owned_positions.insert(
        format!("{}:{token}", condition()),
        OwnedPosition {
            quantity_micros: qty,
            cost_pico: cost.to_string(),
            day,
            daily_quantity_micros: daily_qty,
        },
    );
}
fn ledger(prepared: PreparedChainTransaction) -> Ledger {
    let mut ledger = Ledger {
        initial_collateral_micros: Some(100 * SHARE),
        collateral_micros: 100 * SHARE,
        allowance_micros: 100 * SHARE,
        loss_day: 2,
        ..Ledger::default()
    };
    ledger.closeout_markets.insert(
        condition(),
        SettlementMarket {
            token_ids: prepared.plan.token_ids.clone(),
            window_end_ms: 999,
        },
    );
    ledger.chain_pending = Some(prepared);
    ledger
}
fn receipt(prepared: &PreparedChainTransaction, success: bool) -> ChainReceipt {
    let consumed = if success {
        match prepared.plan.operation {
            ChainOperation::Merge { amount_micros } => [amount_micros; 2],
            ChainOperation::Redeem => prepared.before_tokens_micros,
        }
    } else {
        [0; 2]
    };
    let payout = if success {
        prepared.expected_payout_micros
    } else {
        0
    };
    ChainReceipt {
        transaction_hash: prepared.hash.clone(),
        block_number: 100,
        block_hash: format!("0x{}", "b".repeat(64)),
        finalized_block_number: 111,
        success,
        after_tokens_micros: [
            prepared.before_tokens_micros[0] - consumed[0],
            prepared.before_tokens_micros[1] - consumed[1],
        ],
        after_collateral_micros: prepared.before_collateral_micros + payout,
        collateral_delta_micros: payout,
        consumed_tokens_micros: consumed,
        payout_numerators: prepared.payout_numerators,
        payout_denominator: prepared.payout_denominator,
        gas_cost_wei: "100".into(),
        intent_hash: None,
        sponsored_gas_cost_wei: None,
    }
}

#[tokio::test]
async fn imported_first_merge_never_undervalues_remaining_owned_cost() {
    let (_, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [10 * SHARE, 5 * SHARE],
    )
    .await;
    let r = receipt(&prepared, true);
    let mut state = ledger(prepared);
    // Up: imported 5 @ .2 + owned 5 @ .8. Down is entirely owned.
    insert_position(&mut state, "11", mm2_core::Outcome::Up, 10 * SHARE, 5 * USD);
    insert_position(&mut state, "22", mm2_core::Outcome::Down, 5 * SHARE, USD);
    insert_owned(&mut state, "11", 5 * SHARE, 4 * USD, 2, 5 * SHARE);
    insert_owned(&mut state, "22", 5 * SHARE, USD, 2, 5 * SHARE);
    state.session_spent_pico = (5 * USD).to_string();
    state.daily_loss_micros = 5 * SHARE;
    state.apply_chain_receipt(&r).unwrap();
    let up = format!("{}:11", condition());
    assert_eq!(state.owned_positions[&up].quantity_micros, 5 * SHARE);
    assert_eq!(state.owned_positions[&up].cost_pico, (4 * USD).to_string());
    assert!(
        state.positions[&up].cost_pico.parse::<u128>().unwrap() >= 4 * USD,
        "gross exposure must include all remaining known owned cost"
    );
    assert!(state.position_floor[&up].cost_pico.parse::<u128>().unwrap() >= 4 * USD);
    assert_eq!(
        state.returned_collateral_micros, 0,
        "imported counterpart does not create a fresh owned budget"
    );
}

#[tokio::test]
async fn fractional_redemption_credits_owned_returns_per_leg_without_imported_rounding() {
    let (mut settings, mut prepared) =
        crate::chain::fixture_prepared(plan(ChainOperation::Redeem), [3, 3]).await;
    settings.chain = Some(ChainSettings::default());
    prepared.payout_numerators = [1, 1];
    prepared.payout_denominator = 2;
    prepared.expected_payout_micros = 2;
    crate::chain::validate_prepared(&settings, settings.chain.as_ref().unwrap(), &prepared)
        .unwrap();
    let r = receipt(&prepared, true);
    let mut state = ledger(prepared);
    insert_position(&mut state, "11", mm2_core::Outcome::Up, 3, 1_500_000);
    insert_position(&mut state, "22", mm2_core::Outcome::Down, 3, 1_500_000);
    insert_owned(&mut state, "11", 1, 500_000, 2, 1);
    insert_owned(&mut state, "22", 1, 500_000, 2, 1);
    state.session_spent_pico = "1000000".into();
    state.daily_loss_micros = 1;
    state.apply_chain_receipt(&r).unwrap();
    assert_eq!(state.returned_collateral_micros, 0);
    assert_eq!(state.daily_loss_micros, 1);
    assert_eq!(state.realized_cost_pico, "1000000");
    assert!(state.owned_positions.is_empty());
}

#[tokio::test]
async fn fully_imported_redemption_releases_positions_without_recycling_owner_budget() {
    let (_, prepared) =
        crate::chain::fixture_prepared(plan(ChainOperation::Redeem), [5 * SHARE, 0]).await;
    let r = receipt(&prepared, true);
    let mut state = ledger(prepared);
    insert_position(&mut state, "11", mm2_core::Outcome::Up, 5 * SHARE, 2 * USD);
    state.apply_chain_receipt(&r).unwrap();
    assert_eq!(state.returned_collateral_micros, 0);
    assert_eq!(state.realized_cost_pico, "0");
    assert!(state.positions.is_empty());
    assert_eq!(state.session_gas_cost_wei, "100");
}

#[tokio::test]
async fn owned_merge_returns_capital_once_and_cannot_reapply_a_receipt() {
    let (_, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [5 * SHARE; 2],
    )
    .await;
    let r = receipt(&prepared, true);
    let mut state = ledger(prepared);
    for (token, outcome) in [
        ("11", mm2_core::Outcome::Up),
        ("22", mm2_core::Outcome::Down),
    ] {
        insert_position(&mut state, token, outcome, 5 * SHARE, 245 * USD / 100);
        insert_owned(&mut state, token, 5 * SHARE, 245 * USD / 100, 2, 5 * SHARE);
    }
    state.session_spent_pico = (49 * USD / 10).to_string();
    state.daily_loss_micros = 49 * SHARE / 10;
    state.apply_chain_receipt(&r).unwrap();
    assert_eq!(state.returned_collateral_micros, 5 * SHARE);
    assert_eq!(state.realized_cost_pico, (49 * USD / 10).to_string());
    assert_eq!(state.net_spent_pico().unwrap(), 0);
    assert_eq!(state.daily_loss_micros, 0);
    let after = serde_json::to_value(&state).unwrap();
    assert!(state.apply_chain_receipt(&r).is_err());
    assert_eq!(serde_json::to_value(&state).unwrap(), after);
}

#[tokio::test]
async fn failed_finalized_transaction_charges_only_gas_and_keeps_all_owned_basis() {
    let (_, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [5 * SHARE; 2],
    )
    .await;
    let r = receipt(&prepared, false);
    let mut state = ledger(prepared);
    for (token, outcome) in [
        ("11", mm2_core::Outcome::Up),
        ("22", mm2_core::Outcome::Down),
    ] {
        insert_position(&mut state, token, outcome, 5 * SHARE, 245 * USD / 100);
        insert_owned(&mut state, token, 5 * SHARE, 245 * USD / 100, 2, 5 * SHARE);
    }
    state.session_spent_pico = (49 * USD / 10).to_string();
    state.daily_loss_micros = 49 * SHARE / 10;
    state.apply_chain_receipt(&r).unwrap();
    assert_eq!(state.returned_collateral_micros, 0);
    assert_eq!(state.realized_cost_pico, "");
    assert_eq!(state.daily_loss_micros, 49 * SHARE / 10);
    assert_eq!(state.session_gas_cost_wei, "100");
    assert!(state.chain_pending.is_none());
    assert_eq!(state.owned_positions.len(), 2);
    assert!(state.owned_positions.values().all(|p| p.quantity_micros == 5 * SHARE && p.cost_pico == (245 * USD / 100).to_string()));
}

#[tokio::test]
async fn prior_day_units_close_before_current_day_units_without_crediting_todays_loss() {
    let (_, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [10 * SHARE; 2],
    )
    .await;
    let r = receipt(&prepared, true);
    let mut state = ledger(prepared);
    for (token, outcome) in [
        ("11", mm2_core::Outcome::Up),
        ("22", mm2_core::Outcome::Down),
    ] {
        insert_position(&mut state, token, outcome, 10 * SHARE, 49 * USD / 10);
    }
    insert_owned(&mut state, "11", 10 * SHARE, 49 * USD / 10, 2, 5 * SHARE);
    insert_owned(&mut state, "22", 10 * SHARE, 49 * USD / 10, 2, 10 * SHARE);
    state.session_spent_pico = (98 * USD / 10).to_string();
    state.daily_loss_micros = 735 * SHARE / 100;
    state.apply_chain_receipt(&r).unwrap();
    assert_eq!(state.returned_collateral_micros, 5 * SHARE);
    assert_eq!(
        state.daily_loss_micros,
        735 * SHARE / 100,
        "a pair with a prior-day leg does not credit today's purchase loss bound"
    );
    assert_eq!(
        state.owned_positions[&format!("{}:11", condition())].daily_quantity_micros,
        5 * SHARE
    );
    assert_eq!(
        state.owned_positions[&format!("{}:22", condition())].daily_quantity_micros,
        5 * SHARE
    );
}

#[test]
fn first_observed_fill_on_new_day_preserves_lifetime_quantity_and_restarts_daily_quantity() {
    let mut state = Ledger {
        loss_day: 2,
        session_spent_pico: (245 * USD / 100).to_string(),
        ..Ledger::default()
    };
    insert_position(
        &mut state,
        "11",
        mm2_core::Outcome::Up,
        5 * SHARE,
        245 * USD / 100,
    );
    insert_owned(&mut state, "11", 5 * SHARE, 245 * USD / 100, 1, 5 * SHARE);
    let id = format!("0x{}", "c".repeat(64));
    let q = LiveQuote {
        slot: "up:new-day".into(),
        condition_id: condition(),
        market_slug: "fixture".into(),
        token_id: "11".into(),
        outcome: mm2_core::Outcome::Up,
        price_micros: 490_000,
        quantity_micros: 5 * SHARE,
        window_start_ms: 1000,
        window_end_ms: 301_000,
        tick_micros: 1000,
        min_order_micros: 5 * SHARE,
        fee_bps: 0,
        protocol: 2,
        neg_risk: false,
    };
    state.orders.insert(
        id.clone(),
        LiveOrder {
            order_id: id.clone(),
            quote: q,
            phase: OrderPhase::Live,
            cumulative_filled_micros: 0,
            created_at_ms: 1000,
            last_operation_ms: 1000,
            retry_at_ms: 0,
        },
    );
    state
        .observe(&OrderObservation {
            order_id: id,
            condition_id: condition(),
            token_id: "11".into(),
            price_micros: 490_000,
            original_quantity_micros: 5 * SHARE,
            cumulative_filled_micros: 2 * SHARE,
            terminal: false,
        })
        .unwrap();
    let owned = &state.owned_positions[&format!("{}:11", condition())];
    assert_eq!(owned.quantity_micros, 7 * SHARE);
    assert_eq!(owned.day, 2);
    assert_eq!(owned.daily_quantity_micros, 2 * SHARE);
    assert_eq!(owned.cost_pico, (343 * USD / 100).to_string());
}

#[tokio::test]
async fn replay_validation_rejects_invalid_owned_daily_quantity_and_chainless_pending_intent() {
    let (mut settings, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [5 * SHARE; 2],
    )
    .await;
    settings.chain = Some(ChainSettings::default());
    let mut state = ledger(prepared);
    insert_position(
        &mut state,
        "11",
        mm2_core::Outcome::Up,
        5 * SHARE,
        245 * USD / 100,
    );
    insert_owned(&mut state, "11", 5 * SHARE, 245 * USD / 100, 2, 5 * SHARE);
    state.session_spent_pico = (245 * USD / 100).to_string();
    crate::settlement::validate(&state, &settings).unwrap();
    state
        .owned_positions
        .get_mut(&format!("{}:11", condition()))
        .unwrap()
        .daily_quantity_micros = 6 * SHARE;
    assert!(crate::settlement::validate(&state, &settings).is_err());
    state
        .owned_positions
        .get_mut(&format!("{}:11", condition()))
        .unwrap()
        .daily_quantity_micros = 5 * SHARE;
    let up = format!("{}:11", condition());
    state.owned_positions.get_mut(&up).unwrap().quantity_micros = 6 * SHARE;
    assert!(
        crate::settlement::validate(&state, &settings).is_err(),
        "owned units cannot exceed the durable position floor"
    );
    state.owned_positions.get_mut(&up).unwrap().quantity_micros = 5 * SHARE;
    state.session_spent_pico = (3 * USD).to_string();
    state.owned_positions.get_mut(&up).unwrap().cost_pico = (245 * USD / 100 + 1).to_string();
    assert!(
        crate::settlement::validate(&state, &settings).is_err(),
        "gross floor must cover the remaining owned basis"
    );
    state.position_floor.get_mut(&up).unwrap().cost_pico = (11 * USD).to_string();
    state.positions.get_mut(&up).unwrap().cost_pico = (11 * USD).to_string();
    state.owned_positions.get_mut(&up).unwrap().cost_pico = (11 * USD).to_string();
    state.session_spent_pico = (11 * USD).to_string();
    assert!(
        crate::settlement::validate(&state, &settings).is_err(),
        "owned basis cannot exceed all shares at maxprice plus maximum fee"
    );
    state.position_floor.get_mut(&up).unwrap().cost_pico = (245 * USD / 100).to_string();
    state.positions.get_mut(&up).unwrap().cost_pico = (245 * USD / 100).to_string();
    state.owned_positions.get_mut(&up).unwrap().cost_pico = (245 * USD / 100).to_string();
    state.session_spent_pico = (245 * USD / 100).to_string();
    state.realized_cost_pico = USD.to_string();
    assert!(
        crate::settlement::validate(&state, &settings).is_err(),
        "open and realized owned basis cannot exceed total authenticated spend"
    );
    state.realized_cost_pico.clear();
    state
        .closeout_markets
        .get_mut(&condition())
        .unwrap()
        .token_ids = ["11".into(), "11".into()];
    assert!(crate::settlement::validate(&state, &settings).is_err());
    state
        .closeout_markets
        .get_mut(&condition())
        .unwrap()
        .token_ids = ["11".into(), "22".into()];
    settings.chain = None;
    assert!(crate::settlement::validate(&state, &settings).is_err());
}

#[test]
fn closeout_round_robin_makes_progress_past_an_unresolved_first_market() {
    let mut state = Ledger::default();
    let conditions: Vec<_> = [10, 20, 30]
        .into_iter()
        .map(|n| format!("0x{n:064x}"))
        .collect();
    for (n, condition_id) in conditions.iter().enumerate() {
        state.closeout_markets.insert(
            condition_id.clone(),
            SettlementMarket {
                token_ids: ["11".into(), "22".into()],
                window_end_ms: 999,
            },
        );
        for (token, outcome) in [
            ("11", mm2_core::Outcome::Up),
            ("22", mm2_core::Outcome::Down),
        ] {
            if token == "22" && n != 1 {
                continue;
            }
            state.positions.insert(
                format!("{condition_id}:{token}"),
                LivePosition {
                    condition_id: condition_id.clone(),
                    token_id: token.into(),
                    outcome,
                    quantity_micros: 5 * SHARE,
                    cost_pico: (245 * USD / 100).to_string(),
                    redeemable: false,
                    mergeable: n == 1,
                },
            );
        }
    }
    let first = state.closeout_candidate(1000, None).unwrap();
    assert_eq!(first.condition_id, conditions[1]);
    assert!(matches!(first.operation, ChainOperation::Merge { .. }));
    let next = state
        .closeout_candidate(1000, Some(&first.condition_id))
        .unwrap();
    assert_eq!(next.condition_id, conditions[0]);
    let after_unresolved = state
        .closeout_candidate(1000, Some(&next.condition_id))
        .unwrap();
    assert_eq!(
        after_unresolved.condition_id, conditions[2],
        "an unresolved first market cannot occupy every subsequent attempt"
    );
    assert_eq!(
        state
            .closeout_candidate(1000, Some(&after_unresolved.condition_id))
            .unwrap()
            .condition_id,
        conditions[1]
    );
}

#[tokio::test]
async fn unseen_onchain_holdings_require_a_fresh_account_pass_before_signer_submission() {
    let (_, prepared) = crate::chain::fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5 * SHARE,
        }),
        [10 * SHARE, 5 * SHARE],
    )
    .await;
    let mut state = Ledger::default();
    state.closeout_markets.insert(
        condition(),
        SettlementMarket {
            token_ids: ["11".into(), "22".into()],
            window_end_ms: 999,
        },
    );
    insert_position(
        &mut state,
        "11",
        mm2_core::Outcome::Up,
        5 * SHARE,
        245 * USD / 100,
    );
    insert_position(
        &mut state,
        "22",
        mm2_core::Outcome::Down,
        5 * SHARE,
        245 * USD / 100,
    );
    assert!(state.admit_chain(&prepared).is_err());
    state
        .positions
        .get_mut(&format!("{}:11", condition()))
        .unwrap()
        .quantity_micros = 10 * SHARE;
    state.admit_chain(&prepared).unwrap();
}
