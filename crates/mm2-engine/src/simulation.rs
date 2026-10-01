//! Replayable execution inputs. The hash journal records a complete transaction before
//! the candidate emulator becomes visible. Console samples are never this ledger.
use crate::{config::Limits, domain};
use mm2_core::{DesiredQuote, Outcome, RiskLimits};
use mm2_emulator::{Book, LatencyConfig, Level, PaperSession};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Market {
        condition_id: String,
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

pub fn new(limits: &Limits) -> Result<PaperSession, String> {
    PaperSession::new(risk(limits)?, LatencyConfig::default()).map_err(|error| error.to_string())
}

pub fn apply(session: &mut PaperSession, operation: &Operation) -> Result<(), String> {
    match operation {
        Operation::Market { condition_id } => {
            if condition_id.len() == 66
                && condition_id.starts_with("0x")
                && condition_id[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                Ok(())
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
        Operation::Quotes { now_ms, quotes } => session
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
            .map_err(|error| error.to_string()),
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
        Operation::Limits { now_ms, limits } => session
            .set_risk_limits(*now_ms, risk(limits)?)
            .map_err(|error| error.to_string()),
    }
}

pub fn replay(session: &mut PaperSession, inputs: &[serde_json::Value]) -> Result<(), String> {
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

/// Clock-only advances have no causal effect. Any pending event, reservation,
/// account, venue, desired order, sequence or audit change remains compared.
pub fn same_state_without_clock(left: &PaperSession, right: &PaperSession) -> Result<bool, String> {
    fn state(session: &PaperSession) -> Result<serde_json::Value, String> {
        let mut value =
            serde_json::to_value(session).map_err(|_| "simulation comparison encoding failed")?;
        value
            .as_object_mut()
            .ok_or("simulation comparison schema failed")?
            .remove("now_ms");
        Ok(value)
    }
    Ok(state(left)? == state(right)?)
}
