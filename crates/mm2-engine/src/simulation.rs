//! Replayable execution inputs. The hash journal records a complete transaction before
//! the candidate emulator becomes visible. Console samples are never this ledger.
use crate::{config::Limits, domain};
use mm2_core::{DesiredQuote, Outcome, RiskLimits};
use mm2_emulator::{Book, LatencyConfig, Level, PaperSession};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct MultiMarketPaper {
    session: PaperSession,
    portfolio: mm2_portfolio::Portfolio,
    current_market: Option<String>,
    public_trades: VecDeque<String>,
}
impl Deref for MultiMarketPaper {
    type Target = PaperSession;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}
impl DerefMut for MultiMarketPaper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}
impl MultiMarketPaper {
    pub fn portfolio_snapshot(&self) -> mm2_portfolio::Snapshot {
        self.portfolio.snapshot()
    }
    pub fn telemetry_portfolio_snapshot(&self) -> mm2_portfolio::Snapshot {
        self.portfolio.compact_snapshot(64)
    }
    pub fn portfolio_halt(&self) -> Option<&str> {
        self.portfolio.halt_reason()
    }
    pub fn validate(&self) -> Result<(), String> {
        self.session.validate()?;
        self.portfolio.validate()?;
        if let Some(id) = &self.current_market {
            let position = self
                .portfolio
                .market(id)
                .ok_or("missing current portfolio market")?;
            let account = self.session.snapshot().account;
            if position.execution_spent_pico
                != account
                    .spent_pico
                    .parse::<u128>()
                    .map_err(|_| "invalid simulated spend")?
                || position.execution_up_micros != account.up_inventory_micros
                || position.execution_down_micros != account.down_inventory_micros
            {
                return Err("current paper session disagrees with durable portfolio totals".into());
            }
        }
        Ok(())
    }
    fn sync_portfolio(&mut self) -> Result<(), String> {
        if let Some(market) = &self.current_market {
            let account = self.session.snapshot().account;
            self.portfolio.record_execution_totals(
                market,
                account
                    .spent_pico
                    .parse()
                    .map_err(|_| "invalid exact simulated spend")?,
                account.up_inventory_micros,
                account.down_inventory_micros,
                0,
            )?;
        }
        Ok(())
    }
    fn bind_market(
        &mut self,
        id: &str,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<(), String> {
        if self.current_market.as_deref() == Some(id) {
            let market = &self
                .portfolio
                .market(id)
                .ok_or("missing current market")?
                .market;
            if start_ms.is_some_and(|start| start != market.start_ms)
                || end_ms.is_some_and(|end| end != market.end_ms)
            {
                return Err("market window identity cannot change".into());
            }
            return Ok(());
        }
        self.sync_portfolio()?;
        if let Some(previous) = &self.current_market {
            let snapshot = self.session.snapshot();
            if snapshot.pending_events != 0 || snapshot.account.reserved_micros_ceil != 0 {
                return Err(
                    "market rollover requires authoritative terminal simulated orders".into(),
                );
            }
            let quantity = snapshot
                .account
                .up_inventory_micros
                .min(snapshot.account.down_inventory_micros);
            if quantity > 0 {
                self.portfolio.merge_complete_sets(
                    mm2_portfolio::MergeReceipt {
                        id: format!("paper-merge:{previous}"),
                        market_id: previous.clone(),
                        quantity_micros: quantity,
                        fee_pico: 0,
                        at_ms: self
                            .portfolio
                            .market(previous)
                            .ok_or("missing retained paper market")?
                            .market
                            .end_ms,
                        provenance: mm2_portfolio::Provenance::Synthetic,
                    },
                    true,
                )?;
            }
            let mut limits = self.session.limits().clone();
            limits.initial_cash_micros = self.portfolio.capital_for_new_market()?;
            if limits.initial_cash_micros == 0 {
                return Err("finite paper portfolio collateral exhausted".into());
            }
            let now = self.session.now_ms();
            self.session = PaperSession::new(limits, LatencyConfig::default())?;
            self.session.advance(now)?;
        }
        self.portfolio.register_market(mm2_portfolio::Market {
            id: id.into(),
            start_ms: start_ms.unwrap_or(0),
            end_ms: end_ms.unwrap_or(86_400_000),
        })?;
        self.current_market = Some(id.into());
        self.public_trades.clear();
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Market {
        condition_id: String,
        #[serde(default)]
        start_ms: Option<u64>,
        #[serde(default)]
        end_ms: Option<u64>,
    },
    Advance {
        now_ms: u64,
    },
    Quotes {
        now_ms: u64,
        quotes: Vec<Quote>,
    },
    CancelAll {
        now_ms: u64,
    },
    Book {
        now_ms: u64,
        outcome: String,
        bid: u64,
        ask: u64,
        ahead_micros: u64,
    },
    Trade {
        now_ms: u64,
        outcome: String,
        worst_price_micros: u64,
        quantity_micros: u64,
    },
    ObservedTrade {
        now_ms: u64,
        outcome: String,
        price_micros: u64,
        quantity_micros: u64,
        fingerprint: String,
    },
    VenueRules {
        now_ms: u64,
        tick_micros: u64,
        min_order_micros: u64,
    },
    /// Explicit synthetic settlement input. Public feeds never generate this operation.
    DemoResolution {
        condition_id: String,
        winning_outcome: String,
        at_ms: u64,
    },
    Limits {
        now_ms: u64,
        limits: Limits,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quote {
    pub slot: String,
    pub outcome: String,
    pub price_micros: u64,
    pub quantity_micros: u64,
}

fn outcome(name: &str) -> Result<Outcome, String> {
    match name {
        "up" => Ok(Outcome::Up),
        "down" => Ok(Outcome::Down),
        _ => Err("invalid simulation outcome".into()),
    }
}

pub fn risk(limits: &Limits) -> Result<RiskLimits, String> {
    let config = limits.core()?;
    Ok(RiskLimits {
        initial_cash_micros: config.initial_collateral_micro,
        max_imbalance_micros: config.max_imbalance_micro,
        min_order_micros: 5_000_000,
        tick_micros: 1_000,
        max_orders: 16,
    })
}

pub fn new(limits: &Limits) -> Result<MultiMarketPaper, String> {
    let initial = limits.core()?.initial_collateral_micro;
    let portfolio = mm2_portfolio::Portfolio::new(
        initial,
        mm2_portfolio::RiskPolicy {
            max_total_exposure_micros: initial,
            max_market_exposure_micros: initial,
            ..Default::default()
        },
    )?;
    Ok(MultiMarketPaper {
        session: PaperSession::new(risk(limits)?, LatencyConfig::default())?,
        portfolio,
        current_market: None,
        public_trades: VecDeque::new(),
    })
}

pub fn apply(session: &mut MultiMarketPaper, operation: &Operation) -> Result<(), String> {
    let result = match operation {
        Operation::Market {
            condition_id,
            start_ms,
            end_ms,
        } => {
            if condition_id.len() == 66
                && condition_id.starts_with("0x")
                && condition_id[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                session.bind_market(condition_id, *start_ms, *end_ms)
            } else {
                Err("invalid durable simulation market identity".into())
            }
        }
        Operation::Advance { now_ms } => {
            session.advance(*now_ms).map_err(|error| error.to_string())
        }
        Operation::CancelAll { now_ms } => session
            .cancel_all(*now_ms)
            .map_err(|error| error.to_string()),
        Operation::Quotes { now_ms, quotes } => {
            if !quotes.is_empty() && session.portfolio_halt().is_some() {
                return Err("paper portfolio loss/drawdown halt blocks new quotes".into());
            }
            session
                .quote(
                    *now_ms,
                    quotes
                        .iter()
                        .map(|quote| {
                            Ok(DesiredQuote {
                                slot: quote.slot.clone(),
                                outcome: outcome(&quote.outcome)?,
                                price_micros: quote.price_micros,
                                quantity_micros: quote.quantity_micros,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                )
                .map_err(|error| error.to_string())
        }
        Operation::VenueRules {
            now_ms,
            tick_micros,
            min_order_micros,
        } => {
            let mut limits = session.limits().clone();
            limits.tick_micros = *tick_micros;
            limits.min_order_micros = *min_order_micros;
            session
                .set_risk_limits(*now_ms, limits)
                .map_err(|error| error.to_string())
        }
        Operation::Book {
            now_ms,
            outcome: name,
            bid,
            ask,
            ahead_micros,
        } => session
            .set_book(
                *now_ms,
                outcome(name)?,
                Book {
                    bid: Some(*bid),
                    ask: Some(*ask),
                    bid_levels: vec![Level {
                        price_micros: *bid,
                        quantity_micros: *ahead_micros,
                    }],
                },
            )
            .map_err(|error| error.to_string()),
        Operation::Trade {
            now_ms,
            outcome: name,
            worst_price_micros,
            quantity_micros,
        } => session
            .trade(
                *now_ms,
                outcome(name)?,
                *worst_price_micros,
                *quantity_micros,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Operation::ObservedTrade {
            now_ms,
            outcome: name,
            price_micros,
            quantity_micros,
            fingerprint,
        } => {
            if fingerprint.len() != 64 || !fingerprint.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err("invalid observed trade fingerprint".into());
            }
            if session.public_trades.contains(fingerprint) {
                return Ok(());
            }
            session.trade(*now_ms, outcome(name)?, *price_micros, *quantity_micros)?;
            if session.public_trades.len() == 8192 {
                session.public_trades.pop_front();
            }
            session.public_trades.push_back(fingerprint.clone());
            Ok(())
        }
        Operation::DemoResolution {
            condition_id,
            winning_outcome,
            at_ms,
        } => {
            if session.current_market.as_deref() == Some(condition_id) {
                return Err("cannot resolve the current paper execution market".into());
            }
            session
                .portfolio
                .redeem(
                    mm2_portfolio::ResolutionReceipt {
                        id: format!("demo-resolution:{condition_id}"),
                        market_id: condition_id.clone(),
                        winning_outcome: outcome(winning_outcome)?,
                        fee_pico: 0,
                        at_ms: *at_ms,
                        finality_confirmed: true,
                        provenance: mm2_portfolio::Provenance::Synthetic,
                    },
                    true,
                )
                .map(|_| ())
        }
        Operation::Limits { now_ms, limits } => {
            let mut risk = risk(limits)?;
            if u128::from(limits.core()?.initial_collateral_micro)
                != session
                    .portfolio_snapshot()
                    .initial_cash_pico
                    .parse::<u128>()
                    .map_err(|_| "corrupt portfolio initial cash")?
                    / 1_000_000
            {
                return Err("initial portfolio collateral cannot change".into());
            }
            risk.initial_cash_micros = session.limits().initial_cash_micros;
            risk.tick_micros = session.limits().tick_micros;
            risk.min_order_micros = session.limits().min_order_micros;
            session
                .set_risk_limits(*now_ms, risk)
                .map_err(|error| error.to_string())
        }
    };
    result?;
    session.sync_portfolio()
}

pub fn replay(session: &mut MultiMarketPaper, inputs: &[serde_json::Value]) -> Result<(), String> {
    for input in inputs {
        let operation: Operation = serde_json::from_value(input.clone())
            .map_err(|_| "invalid durable simulation operation")?;
        apply(session, &operation)?;
    }
    session.validate().map_err(|error| error.to_string())
}

pub fn values(inputs: &[Operation]) -> Result<Vec<serde_json::Value>, String> {
    inputs
        .iter()
        .map(|input| {
            serde_json::to_value(input).map_err(|_| "simulation input encoding failed".into())
        })
        .collect()
}

pub fn desired(decision: &domain::Decision) -> Vec<Quote> {
    decision
        .intents
        .iter()
        .map(|intent| Quote {
            slot: match intent.outcome {
                domain::Outcome::Up => "up_bid",
                domain::Outcome::Down => "down_bid",
            }
            .into(),
            outcome: match intent.outcome {
                domain::Outcome::Up => "up",
                domain::Outcome::Down => "down",
            }
            .into(),
            price_micros: intent.price_micro,
            quantity_micros: intent.quantity_micro,
        })
        .collect()
}

pub fn from_core_quotes(quotes: &[DesiredQuote]) -> Vec<Quote> {
    quotes
        .iter()
        .map(|quote| Quote {
            slot: quote.slot.clone(),
            outcome: if quote.outcome == Outcome::Up {
                "up"
            } else {
                "down"
            }
            .into(),
            price_micros: quote.price_micros,
            quantity_micros: quote.quantity_micros,
        })
        .collect()
}

/// Clock-only advances have no causal effect. Any pending event, reservation,
/// account, venue, desired order, sequence or audit change remains compared.
pub fn same_state_without_clock(
    left: &MultiMarketPaper,
    right: &MultiMarketPaper,
) -> Result<bool, String> {
    fn state(session: &MultiMarketPaper) -> Result<serde_json::Value, String> {
        let mut value =
            serde_json::to_value(session).map_err(|_| "simulation comparison encoding failed")?;
        value
            .get_mut("session")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("simulation comparison schema failed")?
            .remove("now_ms");
        Ok(value)
    }
    Ok(state(left)? == state(right)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Limits {
        Limits {
            initial_collateral_usd: "100".into(),
            max_imbalance_shares: "10".into(),
            lot_shares: "5".into(),
            half_width: "0.01".into(),
            max_feed_age_ms: 1000,
        }
    }
    fn id(c: char) -> String {
        format!("0x{}", c.to_string().repeat(64))
    }
    fn executed_window() -> Vec<Operation> {
        vec![
            Operation::Market {
                condition_id: id('a'),
                start_ms: Some(0),
                end_ms: Some(300_000),
            },
            Operation::Book {
                now_ms: 0,
                outcome: "up".into(),
                bid: 400_000,
                ask: 500_000,
                ahead_micros: 0,
            },
            Operation::Book {
                now_ms: 0,
                outcome: "down".into(),
                bid: 400_000,
                ask: 500_000,
                ahead_micros: 0,
            },
            Operation::Quotes {
                now_ms: 0,
                quotes: vec![
                    Quote {
                        slot: "up".into(),
                        outcome: "up".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000,
                    },
                    Quote {
                        slot: "down".into(),
                        outcome: "down".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000,
                    },
                ],
            },
            Operation::Advance { now_ms: 50 },
            Operation::ObservedTrade {
                now_ms: 60,
                outcome: "up".into(),
                price_micros: 400_000,
                quantity_micros: 5_000_000,
                fingerprint: "a".repeat(64),
            },
            Operation::Advance { now_ms: 100 },
            Operation::CancelAll { now_ms: 100 },
            Operation::Advance { now_ms: 1000 },
        ]
    }
    #[test]
    fn rollover_preserves_unpaired_positions_and_finite_cash_and_replays_exactly() {
        let mut session = new(&limits()).unwrap();
        let mut operations = executed_window();
        operations.push(Operation::Market {
            condition_id: id('b'),
            start_ms: Some(300_000),
            end_ms: Some(600_000),
        });
        for operation in &operations {
            apply(&mut session, operation).unwrap();
        }
        session.validate().unwrap();
        let portfolio = session.portfolio_snapshot();
        assert_eq!(portfolio.cash_pico, "98000000000000");
        assert_eq!(session.snapshot().account.initial_cash_micros, 98_000_000);
        let previous = portfolio
            .positions
            .iter()
            .find(|p| p.market.id == id('a'))
            .unwrap();
        assert_eq!(previous.up.quantity_micros, 5_000_000);
        assert!(previous.resolution.is_none());
        let mut recovered = new(&limits()).unwrap();
        replay(&mut recovered, &values(&operations).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            serde_json::to_value(recovered).unwrap()
        );
    }
    #[test]
    fn observed_trade_duplicate_is_idempotent_and_changed_market_bounds_reject() {
        let mut session = new(&limits()).unwrap();
        let operations = executed_window();
        for operation in &operations {
            apply(&mut session, operation).unwrap();
        }
        let previous = serde_json::to_value(&session).unwrap();
        apply(&mut session, &operations[5]).unwrap();
        assert_eq!(serde_json::to_value(&session).unwrap(), previous);
        assert!(
            apply(
                &mut session,
                &Operation::Market {
                    condition_id: id('a'),
                    start_ms: Some(1),
                    end_ms: Some(300_000)
                }
            )
            .is_err()
        );
    }
    #[test]
    fn explicit_demo_resolution_releases_only_known_synthetic_payout_and_is_idempotent() {
        let mut session = new(&limits()).unwrap();
        for op in executed_window() {
            apply(&mut session, &op).unwrap();
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        let resolution = Operation::DemoResolution {
            condition_id: id('a'),
            winning_outcome: "up".into(),
            at_ms: 300_000,
        };
        apply(&mut session, &resolution).unwrap();
        apply(&mut session, &resolution).unwrap();
        assert_eq!(session.portfolio_snapshot().cash_pico, "103000000000000");
        session.validate().unwrap();
        assert!(
            apply(
                &mut session,
                &Operation::DemoResolution {
                    condition_id: id('b'),
                    winning_outcome: "up".into(),
                    at_ms: 600_000
                }
            )
            .is_err()
        );
    }
    #[test]
    fn wrapper_validation_detects_corrupt_imported_execution_totals() {
        let mut session = new(&limits()).unwrap();
        for operation in executed_window() {
            apply(&mut session, &operation).unwrap();
        }
        let mut value = serde_json::to_value(session).unwrap();
        value["portfolio"]["markets"][id('a')]["execution_up_micros"] =
            serde_json::json!(6_000_000);
        let corrupt: MultiMarketPaper = serde_json::from_value(value).unwrap();
        assert!(corrupt.validate().is_err());
    }
    #[test]
    fn venue_grid_changes_require_terminal_orders_and_replay_sub_millitick_quotes() {
        let mut session = new(&limits()).unwrap();
        let mut tape = executed_window();
        for op in &tape {
            apply(&mut session, op).unwrap();
        }
        let fine = Operation::VenueRules {
            now_ms: 1000,
            tick_micros: 100,
            min_order_micros: 5_000_000,
        };
        apply(&mut session, &fine).unwrap();
        tape.push(fine);
        let quote = Operation::Quotes {
            now_ms: 1000,
            quotes: vec![Quote {
                slot: "fine".into(),
                outcome: "up".into(),
                price_micros: 400_100,
                quantity_micros: 5_000_000,
            }],
        };
        apply(&mut session, &quote).unwrap();
        tape.push(quote);
        let coarse = Operation::VenueRules {
            now_ms: 1000,
            tick_micros: 2500,
            min_order_micros: 5_000_000,
        };
        assert!(apply(&mut session, &coarse).is_err());
        for op in [
            Operation::CancelAll { now_ms: 1000 },
            Operation::Advance { now_ms: 2000 },
            Operation::VenueRules {
                now_ms: 2000,
                tick_micros: 2500,
                min_order_micros: 5_000_000,
            },
            Operation::Quotes {
                now_ms: 2000,
                quotes: vec![Quote {
                    slot: "coarse".into(),
                    outcome: "up".into(),
                    price_micros: 402_500,
                    quantity_micros: 5_000_000,
                }],
            },
        ] {
            apply(&mut session, &op).unwrap();
            tape.push(op);
        }
        let mut recovered = new(&limits()).unwrap();
        replay(&mut recovered, &values(&tape).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            serde_json::to_value(&recovered).unwrap()
        );
    }
    #[test]
    fn settlement_loss_halt_blocks_quotes_in_already_funded_next_window() {
        let mut session = new(&limits()).unwrap();
        session.portfolio = mm2_portfolio::Portfolio::new(
            100_000_000,
            mm2_portfolio::RiskPolicy {
                max_daily_loss_micros: 1_000_000,
                max_drawdown_micros: 1_000_000,
                ..Default::default()
            },
        )
        .unwrap();
        for op in executed_window() {
            apply(&mut session, &op).unwrap();
        }
        apply(
            &mut session,
            &Operation::Market {
                condition_id: id('b'),
                start_ms: Some(300_000),
                end_ms: Some(600_000),
            },
        )
        .unwrap();
        apply(
            &mut session,
            &Operation::DemoResolution {
                condition_id: id('a'),
                winning_outcome: "down".into(),
                at_ms: 300_000,
            },
        )
        .unwrap();
        assert!(session.portfolio_halt().is_some());
        let prior = serde_json::to_value(&session).unwrap();
        assert!(
            apply(
                &mut session,
                &Operation::Quotes {
                    now_ms: 1000,
                    quotes: vec![Quote {
                        slot: "forbidden".into(),
                        outcome: "up".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000
                    }]
                }
            )
            .is_err()
        );
        assert_eq!(prior, serde_json::to_value(&session).unwrap());
        apply(&mut session, &Operation::CancelAll { now_ms: 1000 }).unwrap();
        session.validate().unwrap();
    }
}
