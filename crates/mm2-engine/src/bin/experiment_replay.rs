//! Counterfactual strategies consume full common public inputs, never filtered historical fills.
use mm2_emulator::LatencyConfig;
use mm2_engine::experiments as tape;
use mm2_engine::{
    config::Limits,
    domain::{self, Action, Core, FeedEvent, FeedSource, Market, StopReason},
    feeds, ingress,
    paper_guard::ReferenceGuard,
    simulation::{self, MultiMarketPaper, Operation},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    name: String,
    rules: mm2_strategy::Rules,
    policy: Option<mm2_strategy::PaperPolicy>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema: u32,
    frozen_at_wall_ms: u64,
    limits: Limits,
    candidates: Vec<Candidate>,
    latency_per_leg_ms: Vec<u64>,
    decision_interval_ms: u64,
    training_windows: usize,
    #[serde(default = "default_embargo")]
    embargo_windows: usize,
    min_holdout_windows: usize,
    infrastructure_usd_per_day: String,
    extra_cost_bps: u64,
    max_input_bytes: u64,
    #[serde(default, skip_serializing_if = "QualityProtocol::is_strict")]
    quality_protocol: QualityProtocol,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QualityProtocol {
    #[default]
    StrictV1,
    RecoveredNativeV2,
}
impl QualityProtocol {
    fn is_strict(&self) -> bool {
        *self == Self::StrictV1
    }
}
fn default_embargo() -> usize {
    1
}
const MAX_INGRESS_WITNESSES: usize = 32;

#[derive(Serialize)]
struct FaultOrigin {
    record_sequence: u64,
    record_hash: String,
    frame_sha256: String,
    source_feed: String,
    source_generation: u64,
    record_wall_ms: u64,
    record_mono_ms: u64,
}
#[derive(Default)]
struct IngressDiagnostics {
    total_faults: u64,
    witnesses: Vec<Value>,
}
impl IngressDiagnostics {
    fn has_room(&self) -> bool {
        self.witnesses.len() < MAX_INGRESS_WITNESSES
    }
    fn record(&mut self, origin: &FaultOrigin, metadata: Value, error: &str) {
        self.total_faults += 1;
        if self.has_room() {
            self.witnesses.push(
                json!({"origin":origin,"error_class":native_error_class(error),"context":metadata}),
            );
        }
    }
    fn report(self) -> Value {
        json!({"mode":"diagnostic_only_not_for_selection","max_witnesses":MAX_INGRESS_WITNESSES,
            "total_faults":self.total_faults,"omitted_witnesses":self.total_faults-self.witnesses.len() as u64,
            "witnesses":self.witnesses,"raw_payloads_or_error_text_retained":false,
            "pre_error_summary_scope":"Known preflight fault or predicted Reference zero/regression only; no ordinary-frame snapshots.",
            "watermark_scope":"Core accepted book/tick/receive clocks; private feed-filter internals are not inferred."})
    }
}
struct DiagnosticContext<'a> {
    log: &'a mut IngressDiagnostics,
    origin: &'a FaultOrigin,
    admitted_event_index: usize,
    preflight_fault: bool,
}
fn native_error_class(error: &str) -> &'static str {
    match error {
        "event market identity mismatch" => "event_market_identity_mismatch",
        "CLOB receive clock regressed" => "clob_receive_clock_regressed",
        "CLOB exchange clock regressed" => "clob_exchange_clock_regressed",
        "invalid or regressed reference input" => "invalid_or_regressed_reference_input",
        "book depth exceeds bound" => "book_depth_exceeds_bound",
        "invalid or duplicate book level" => "invalid_or_duplicate_book_level",
        "crossed or oversized book" => "crossed_or_oversized_book",
        "empty or oversized delta batch" => "empty_or_oversized_delta_batch",
        "delta before snapshot or invalid price" => "delta_before_snapshot_or_invalid_price",
        "crossed or oversized batch result" => "crossed_or_oversized_batch_result",
        "invalid or incompatible tick transition" => "invalid_or_incompatible_tick_transition",
        _ => "unclassified_native_error",
    }
}
fn book_summary(book: &domain::BookSnapshot) -> Value {
    json!({"initialized":book.initialized,"tick_micro":book.tick_micro,
        "best_bid_micro":book.best_bid_micro,"best_ask_micro":book.best_ask_micro,
        "exchange_ms":book.exchange_ms,"observed_mono_ms":book.observed_mono_ms})
}
fn event_summary(event: &FeedEvent) -> Value {
    let outcome = |outcome: domain::Outcome| {
        if outcome == domain::Outcome::Up {
            "up"
        } else {
            "down"
        }
    };
    match event {
        FeedEvent::Snapshot {
            outcome: side,
            exchange_ms,
            observed_mono_ms,
            bids,
            asks,
            ..
        } => {
            json!({"kind":"snapshot","outcome":outcome(*side),"exchange_ms":exchange_ms,"observed_mono_ms":observed_mono_ms,
                "bid_levels":bids.len(),"ask_levels":asks.len(),"best_bid_micro":bids.iter().map(|level|level.price_micro).max(),
                "best_ask_micro":asks.iter().map(|level|level.price_micro).min()})
        }
        FeedEvent::Delta {
            exchange_ms,
            observed_mono_ms,
            changes,
            ..
        } => {
            json!({"kind":"delta","exchange_ms":exchange_ms,"observed_mono_ms":observed_mono_ms,"changes":changes.len(),
                "invalid_price_changes":changes.iter().filter(|change|change.level.price_micro==0 || change.level.price_micro>=1_000_000).count(),
                "first_changes":changes.iter().take(4).map(|change|json!({"outcome":outcome(change.outcome),
                    "side":if change.side==domain::BookSide::Bid {"bid"} else {"ask"},
                    "price_micro":change.level.price_micro,"quantity_micro":change.level.quantity_micro})).collect::<Vec<_>>()})
        }
        FeedEvent::Tick {
            outcome: side,
            exchange_ms,
            observed_mono_ms,
            old_tick_micro,
            new_tick_micro,
            ..
        } => {
            json!({"kind":"tick","outcome":outcome(*side),"exchange_ms":exchange_ms,"observed_mono_ms":observed_mono_ms,
                "old_tick_micro":old_tick_micro,"new_tick_micro":new_tick_micro})
        }
        FeedEvent::Reference {
            exchange_ms,
            observed_mono_ms,
            price_micro,
        } => {
            json!({"kind":"reference","exchange_ms":exchange_ms,"observed_mono_ms":observed_mono_ms,"price_micro":price_micro})
        }
        FeedEvent::Trade {
            outcome: side,
            exchange_ms,
            observed_mono_ms,
            price_micro,
            quantity_micro,
            ..
        } => {
            json!({"kind":"trade","outcome":outcome(*side),"exchange_ms":exchange_ms,"observed_mono_ms":observed_mono_ms,
                "price_micro":price_micro,"quantity_micro":quantity_micro})
        }
        FeedEvent::Disconnect { source, .. } => {
            json!({"kind":"disconnect","source":if *source==FeedSource::Clob {"clob"} else {"binance"}})
        }
    }
}
struct MarketState {
    market: Market,
    core: Core,
    fee: Option<mm2_live::fees::FeePolicy>,
    first_snap: [Option<u64>; 2],
    gaps: u64,
    invalid: u64,
    winner: Option<mm2_core::Outcome>,
    outcome_observed_at: Option<u64>,
    frames: feeds::Recovery,
    native_snapshots: Option<(u64, [bool; 2])>,
    source_generation: Option<u64>,
    awaiting_generation: bool,
    recoverable_splits: u64,
    completed_snapshot_recoveries: u64,
    discarded_native_events: u64,
    split_invalid_inputs: u64,
    completed_quality_recoveries: u64,
    recovery_started_for_quality: bool,
    max_recovery_ms: u64,
    parse_errors: u64,
    fee_errors: u64,
    fatal_ingress_errors: u64,
    recovery_timeouts: u64,
    diagnostic_reference: Option<(u64, u64, u64)>,
}
impl MarketState {
    fn fatal_invalid_inputs(&self) -> u64 {
        self.invalid.saturating_sub(self.split_invalid_inputs)
    }
    fn unresolved_quality_recoveries(&self) -> u64 {
        self.split_invalid_inputs
            .saturating_sub(self.completed_quality_recoveries)
    }
    fn quality_eligible(&self, protocol: QualityProtocol) -> bool {
        let warm = self
            .first_snap
            .iter()
            .all(|t| t.is_some_and(|at| at <= self.market.window_start_ms));
        let inputs_valid = match protocol {
            QualityProtocol::StrictV1 => self.invalid == 0,
            QualityProtocol::RecoveredNativeV2 => {
                self.fatal_invalid_inputs() == 0 && self.unresolved_quality_recoveries() == 0
            }
        };
        warm && self.gaps == 0 && inputs_valid && self.winner.is_some()
    }
    fn recovered_quality_evidence(&self) -> Value {
        json!({
            "fatal_invalid_inputs": self.fatal_invalid_inputs(),
            "parse_errors": self.parse_errors,
            "fee_policy_errors": self.fee_errors,
            "fatal_ingress_errors": self.fatal_ingress_errors,
            "native_recovery_timeouts": self.recovery_timeouts,
            "split_recoveries_started_before_close": self.split_invalid_inputs,
            "split_recoveries_verified_before_close": self.completed_quality_recoveries,
            "unresolved_native_recoveries": self.unresolved_quality_recoveries(),
        })
    }
    fn clear_native(&mut self, awaiting_generation: bool) {
        self.frames = feeds::Recovery::new([0; 2]);
        self.native_snapshots = None;
        self.recovery_started_for_quality = false;
        self.awaiting_generation = awaiting_generation;
        let _ = self.core.apply(FeedEvent::Disconnect {
            source: FeedSource::Clob,
            reason: "recorded native source recovery".into(),
        });
    }
    fn expire_native(&mut self, wall: u64, mono: u64, max_age: u64) {
        if self
            .native_snapshots
            .is_some_and(|(started, _)| mono.saturating_sub(started) >= max_age)
        {
            if wall < self.market.window_end_ms {
                self.invalid += 1;
                self.recovery_timeouts += 1;
            }
            // This recorder cannot invent the actor's REST refresh/new socket.
            self.clear_native(true);
        }
    }
    fn apply_public(&mut self, event: FeedEvent, wall: u64, mono: u64) -> bool {
        self.apply_public_diagnostic(event, wall, mono, None)
    }
    fn apply_public_diagnostic(
        &mut self,
        event: FeedEvent,
        wall: u64,
        mono: u64,
        diagnostic: Option<DiagnosticContext<'_>>,
    ) -> bool {
        let prior = self.native_snapshots;
        let next_reference = if diagnostic.is_some() || self.diagnostic_reference.is_some() {
            match &event {
                FeedEvent::Reference {
                    exchange_ms,
                    observed_mono_ms,
                    price_micro,
                } => Some(Some((*exchange_ms, *observed_mono_ms, *price_micro))),
                FeedEvent::Disconnect {
                    source: FeedSource::Binance,
                    ..
                } => Some(None),
                _ => None,
            }
        } else {
            None
        };
        let reference_fault = diagnostic.as_ref().is_some_and(|d| d.log.has_room())
            && next_reference
                .flatten()
                .is_some_and(|(exchange, received, price)| {
                    price == 0
                        || self.diagnostic_reference.is_some_and(
                            |(old_exchange, old_received, _)| {
                                exchange < old_exchange || received < old_received
                            },
                        )
                });
        let input_kind = match &event {
            FeedEvent::Snapshot { .. } => "snapshot",
            FeedEvent::Delta { .. } => "delta",
            FeedEvent::Tick { .. } => "tick",
            FeedEvent::Reference { .. } => "reference",
            FeedEvent::Trade { .. } => "trade",
            FeedEvent::Disconnect { .. } => "disconnect",
        };
        let pre_error = diagnostic.as_ref().filter(|d|d.log.has_room() && (d.preflight_fault || reference_fault)).map(|d| {
            let snapshot = self.core.snapshot();
            json!({"admitted_event_index":d.admitted_event_index,"event":event_summary(&event),"pre_error_summary_available":true,
                "accepted_clock_watermarks":self.core.feed_clock_context(&event),
                "pre_error_books":{"up":book_summary(&snapshot.up_book),"down":book_summary(&snapshot.down_book)},
                "pre_error_reference":snapshot.reference.as_ref().map(|r|json!({"exchange_ms":r.exchange_ms,"observed_mono_ms":r.observed_mono_ms,"price_micro":r.price_micro})),
                "native_recovery_started_mono_ms":prior.map(|(at,_)|at),"native_recovery_snapshots_seen":prior.map(|(_,seen)|seen),
                "awaiting_observed_source_generation":self.awaiting_generation,
                "active_clob_source_generation":self.source_generation,
                "market_window_start_ms":self.market.window_start_ms,"market_window_end_ms":self.market.window_end_ms,
                "market_slug":self.market.slug,"before_market_close":wall<self.market.window_end_ms})
        });
        let mut seen = prior.map(|(_, seen)| seen);
        let snapshot = if let FeedEvent::Snapshot { outcome, .. } = &event {
            Some(usize::from(*outcome == domain::Outcome::Down))
        } else {
            None
        };
        match ingress::apply_with_native_snapshots(&mut self.core, event, &mut seen) {
            Ok(ingress::Applied::SnapshotRecoveryStarted) => {
                self.recoverable_splits += 1;
                // Preserve the existing strict experiment quality gate even
                // when the actor can restore its books without reconnecting.
                if wall < self.market.window_end_ms {
                    self.invalid += 1;
                    self.split_invalid_inputs += 1;
                }
                self.recovery_started_for_quality = wall < self.market.window_end_ms;
                self.native_snapshots = Some((mono, [false; 2]));
                true
            }
            Ok(applied) => {
                if let Some(reference) = next_reference {
                    self.diagnostic_reference = reference;
                }
                if applied == ingress::Applied::SnapshotRecoveryCompleted {
                    self.completed_snapshot_recoveries += 1;
                    if self.recovery_started_for_quality
                        && wall < self.market.window_end_ms
                        && prior.is_some_and(|(started, _)| {
                            mono.checked_sub(started)
                                .is_some_and(|elapsed| elapsed < self.max_recovery_ms)
                        })
                    {
                        self.completed_quality_recoveries += 1;
                    }
                    self.recovery_started_for_quality = false;
                }
                self.native_snapshots =
                    seen.map(|seen| (prior.expect("existing native recovery").0, seen));
                if let Some(index) = snapshot {
                    self.first_snap[index].get_or_insert(wall);
                }
                true
            }
            Err(error) => {
                if next_reference.is_some() {
                    self.diagnostic_reference = None;
                }
                if let Some(diagnostic) = diagnostic {
                    if diagnostic.log.has_room() {
                        diagnostic.log.record(diagnostic.origin, pre_error.unwrap_or_else(||json!({
                        "admitted_event_index":diagnostic.admitted_event_index,"pre_error_summary_available":false,
                        "event_kind":input_kind,
                        "market_slug":self.market.slug,"before_market_close":wall<self.market.window_end_ms,
                        "active_clob_source_generation":self.source_generation})), &error);
                    } else {
                        diagnostic.log.total_faults += 1;
                    }
                }
                if wall < self.market.window_end_ms {
                    self.invalid += 1;
                    self.fatal_ingress_errors += 1;
                }
                self.clear_native(true);
                false
            }
        }
    }
}
struct Run {
    candidate: Candidate,
    latency: u64,
    paper: MultiMarketPaper,
    current: Option<String>,
    guard: ReferenceGuard,
    inventory_since: Option<(mm2_core::Outcome, u64)>,
    gates: BTreeMap<String, u64>,
    errors: Vec<String>,
    orders_rejected: u64,
    fill_events: u64,
    partial_fill_events: u64,
    max_daily_gross_loss: u128,
    max_drawdown: u128,
    error_times: Vec<u64>,
    now_wall: u64,
    skipped_delayed_prints: u64,
}
impl Run {
    fn fail(&mut self, message: String) {
        if self.errors.len() < 8 {
            self.errors.push(message);
            self.error_times.push(self.now_wall);
        }
    }
    fn apply(&mut self, op: Operation) -> bool {
        let before = self.paper.progress_mark();
        match simulation::apply(&mut self.paper, &op) {
            Ok(()) => {
                let after = self.paper.replay_progress_since(before);
                self.skipped_delayed_prints += after.execution.causality_excluded_prints;
                self.fill_events += after.execution.fill_events;
                self.partial_fill_events += after.execution.partial_fill_events;
                self.max_daily_gross_loss = self.max_daily_gross_loss.max(after.daily_loss_pico);
                self.max_drawdown = self.max_drawdown.max(after.drawdown_pico);
                true
            }
            Err(e) => {
                self.fail(e);
                false
            }
        }
    }
    fn tick(
        &mut self,
        wall: u64,
        mono: u64,
        states: &mut BTreeMap<String, MarketState>,
        interval: u64,
    ) {
        self.now_wall = wall;
        self.apply(Operation::Advance { now_ms: mono });
        if self.paper.risk_window_due(wall) {
            self.apply(Operation::RiskWindow { at_ms: wall });
        }
        let target = states
            .iter()
            .find(|(_, s)| s.market.window_start_ms <= wall && wall < s.market.window_end_ms)
            .map(|(slug, _)| slug.clone());
        if target != self.current {
            self.apply(Operation::CancelAll { now_ms: mono });
            let snapshot = self.paper.snapshot();
            if snapshot.pending_events > 0 || snapshot.account.reserved_micros_ceil > 0 {
                return;
            }
            if let Some(slug) = &target {
                let m = &states[slug].market;
                if !self.apply(Operation::Market {
                    condition_id: m.condition_id.clone(),
                    start_ms: Some(m.window_start_ms),
                    end_ms: Some(m.window_end_ms),
                }) {
                    return;
                }
                self.current = Some(slug.clone());
                self.inventory_since = None;
            } else {
                return;
            }
        }
        let Some(slug) = &self.current else {
            return;
        };
        let Some(state) = states.get_mut(slug) else {
            return;
        };
        if wall >= state.market.window_end_ms
            || self.paper.portfolio_halt().is_some()
            || !self.errors.is_empty()
            || state.native_snapshots.is_some()
            || state.awaiting_generation
        {
            self.apply(Operation::CancelAll { now_ms: mono });
            return;
        }
        let decision = state.core.evaluate(wall, mono);
        if decision
            .stop_reasons
            .iter()
            .any(|r| !matches!(r, StopReason::NoLegalQuote(_) | StopReason::BudgetExceeded))
        {
            self.apply(Operation::CancelAll { now_ms: mono });
            return;
        }
        let snap = state.core.snapshot();
        let Some(fee) = state.fee.as_ref().filter(|f| {
            f.permits(
                &state.market.condition_id,
                &state.market.up_token,
                &state.market.down_token,
                wall,
            )
        }) else {
            self.apply(Operation::CancelAll { now_ms: mono });
            return;
        };
        self.apply(Operation::FeeRules {
            policy: fee.clone(),
        });
        let account = self.paper.account_snapshot();
        let balanced = account.up_inventory_micros == account.down_inventory_micros;
        if balanced {
            self.inventory_since = None;
        } else {
            let side = if account.up_inventory_micros > account.down_inventory_micros {
                mm2_core::Outcome::Up
            } else {
                mm2_core::Outcome::Down
            };
            if self.inventory_since.is_none_or(|(old, _)| old != side) {
                self.inventory_since = Some((side, mono));
            }
        }
        if let Some(r) = snap.reference {
            self.guard
                .observe(r.exchange_ms, r.observed_mono_ms, r.price_micro);
        }
        let bbo = |b: &domain::BookSnapshot| mm2_strategy::Bbo {
            bid_micros: b.best_bid_micro,
            ask_micros: b.best_ask_micro,
            received_ms: wall.saturating_sub(mono.saturating_sub(b.observed_mono_ms.unwrap_or(0))),
        };
        let tick = common_tick(snap.up_book.tick_micro, snap.down_book.tick_micro);
        if self.paper.limits().tick_micros != tick {
            let terminal = self.paper.snapshot();
            if terminal.pending_events > 0 || terminal.account.reserved_micros_ceil > 0 {
                self.apply(Operation::CancelAll { now_ms: mono });
                return;
            }
            self.apply(Operation::VenueRules {
                now_ms: mono,
                tick_micros: tick,
                min_order_micros: 5_000_000,
            });
        }
        let input = mm2_strategy::Input {
            now_ms: wall,
            market_start_ms: state.market.window_start_ms,
            market_end_ms: state.market.window_end_ms,
            up: bbo(&snap.up_book),
            down: bbo(&snap.down_book),
            inventory_up_micros: account.up_inventory_micros,
            inventory_down_micros: account.down_inventory_micros,
            reserved_up_micros: 0,
            reserved_down_micros: 0,
            available_cash_micros: account
                .initial_cash_micros
                .saturating_sub(account.spent_micros_ceil),
            tick_micros: tick,
            min_order_micros: 5_000_000,
            signals: Default::default(),
        };
        let costs = account
            .outcome_spent_pico
            .and_then(|p| Some([p[0].parse().ok()?, p[1].parse().ok()?]));
        let rules = &self.candidate.rules;
        let proposal = if let Some(policy) = &self.candidate.policy {
            mm2_strategy::decide_experimental(
                rules,
                &input,
                costs,
                policy,
                self.inventory_since
                    .map_or(0, |(_, at)| mono.saturating_sub(at)),
            )
        } else {
            mm2_strategy::decide_inventory_repair(rules, &input, costs)
        };
        let mut proposal = match proposal {
            Ok(p) => p,
            Err(e) => {
                self.fail(e);
                return;
            }
        };
        if balanced && proposal.reason == mm2_strategy::Gate::Quote {
            match if snap.binance_connected {
                self.guard.blocked(mono, wall)
            } else {
                None
            } {
                None => {
                    proposal.quotes.clear();
                    proposal.reason = mm2_strategy::Gate::MissingSignal;
                }
                Some(Some(blocked)) => {
                    if self.candidate.policy.is_some() {
                        proposal.quotes.clear();
                    } else {
                        proposal.quotes.retain(|q| q.outcome != blocked);
                    }
                    if proposal.quotes.is_empty() {
                        proposal.reason = mm2_strategy::Gate::DirectionalGuard;
                    }
                }
                Some(None) => {}
            }
        }
        let admitted = if let Some(policy) = &self.candidate.policy {
            self.paper
                .guard_experimental_strategy_quotes(rules, &mut proposal, policy)
        } else {
            self.paper
                .guard_public_strategy_quotes(rules, &mut proposal)
        };
        if let Err(e) = admitted {
            self.fail(e);
            return;
        }
        *self
            .gates
            .entry(format!("{:?}", proposal.reason))
            .or_default() += 1;
        let active = self.paper.snapshot();
        for (name, outcome, book) in [
            ("up", domain::Outcome::Up, &snap.up_book),
            ("down", domain::Outcome::Down, &snap.down_book),
        ] {
            if let (Some(bid), Some(ask)) = (book.best_bid_micro, book.best_ask_micro) {
                let mut prices: Vec<_> = proposal
                    .quotes
                    .iter()
                    .filter(|q| {
                        (q.outcome == mm2_core::Outcome::Up) == (outcome == domain::Outcome::Up)
                    })
                    .map(|q| q.price_micros)
                    .collect();
                prices.push(bid);
                prices.extend(
                    active
                        .orders
                        .iter()
                        .filter(|o| {
                            (o.quote.outcome == mm2_core::Outcome::Up)
                                == (outcome == domain::Outcome::Up)
                        })
                        .map(|o| o.quote.price_micros),
                );
                let bid_levels = state
                    .core
                    .bid_depth_at(outcome, &prices)
                    .into_iter()
                    .map(|l| mm2_emulator::Level {
                        price_micros: l.price_micro,
                        quantity_micros: l.quantity_micro,
                    })
                    .collect();
                self.apply(Operation::DepthBook {
                    now_ms: mono,
                    outcome: name.into(),
                    bid,
                    ask,
                    bid_levels,
                });
            }
        }
        let quotes = proposal
            .quotes
            .iter()
            .map(|q| simulation::Quote {
                slot: q.slot.clone(),
                outcome: if q.outcome == mm2_core::Outcome::Up {
                    "up".into()
                } else {
                    "down".into()
                },
                price_micros: q.price_micros,
                quantity_micros: q.quantity_micros,
            })
            .collect();
        if simulation::apply(
            &mut self.paper,
            &Operation::Quotes {
                now_ms: mono,
                quotes,
            },
        )
        .is_err()
        {
            self.orders_rejected += 1;
            self.apply(Operation::CancelAll { now_ms: mono });
        }
        let _ = interval;
    }
    fn trade(&mut self, wall: u64, mono: u64, slug: &str, event: &FeedEvent, max_age: u64) {
        self.now_wall = wall;
        if self.current.as_deref() != Some(slug) {
            return;
        }
        if let FeedEvent::Trade {
            outcome,
            exchange_ms,
            price_micro,
            quantity_micro,
            fingerprint,
            ..
        } = event
        {
            if *exchange_ms > wall || wall - *exchange_ms > max_age {
                return;
            }
            self.apply(Operation::Advance { now_ms: mono });
            let Some(exchange_event_mono_ms) = mono.checked_sub(wall - *exchange_ms) else {
                self.skipped_delayed_prints += 1;
                return;
            };
            self.apply(Operation::ObservedTradeV3 {
                now_ms: mono,
                exchange_event_mono_ms,
                exchange_ms: *exchange_ms,
                received_wall_ms: wall,
                outcome: if *outcome == domain::Outcome::Up {
                    "up".into()
                } else {
                    "down".into()
                },
                price_micros: *price_micro,
                quantity_micros: *quantity_micro,
                fingerprint: fingerprint.clone(),
            });
        }
    }
}
fn common_tick(up: u64, down: u64) -> u64 {
    if up == 0 || down == 0 {
        return 0;
    }
    let (mut a, mut b) = (up, down);
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    (up / a).checked_mul(down).unwrap_or(0)
}
fn select_training(rows: &[Value], latencies: &[u64]) -> Vec<Value> {
    latencies.iter().map(|latency|{
        let chosen=rows.iter().filter(|r|r["latency_per_leg_ms"].as_u64()==Some(*latency) && r["training_selection_eligible"]==true).max_by_key(|r|r["training_net_pnl_pico"].as_str().unwrap().parse::<i128>().unwrap());
        json!({"latency_per_leg_ms":latency,"selection_uses":"training_only","candidate":chosen.map(|r|r["candidate"].clone()),"training_net_pnl_pico":chosen.map(|r|r["training_net_pnl_pico"].clone()),"chosen_holdout_net_pnl_pico":chosen.filter(|r|r["holdout_eligible"]==true).map(|r|r["holdout_net_pnl_pico"].clone()),"status":if chosen.is_some(){"MODEL_CANDIDATE_SELECTED_NO_LIVE_PROMOTION"}else{"NO_POSITIVE_TRAINING_CANDIDATE"}})
    }).collect()
}
fn evaluate(tape_path: &Path, bytes: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    evaluate_mode(tape_path, bytes, false)
}
fn evaluate_mode(
    tape_path: &Path,
    bytes: &[u8],
    diagnostic_ingress: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    if bytes.len() > 65536 {
        return Err("plan exceeds 64KiB".into());
    }
    let plan: Plan = serde_json::from_slice(bytes)?;
    let raw_plan: Value = serde_json::from_slice(bytes)?;
    if plan.schema != 1
        || plan.candidates.is_empty()
        || plan.candidates.len() > 16
        || plan.latency_per_leg_ms.is_empty()
        || plan.latency_per_leg_ms.len() > 4
        || plan.latency_per_leg_ms.iter().any(|n| *n > 1000)
        || !(10..=1000).contains(&plan.decision_interval_ms)
        || plan.training_windows == 0
        || plan.min_holdout_windows == 0
        || plan.extra_cost_bps > 100
        || plan.max_input_bytes > 4_294_967_296
    {
        return Err("invalid bounded experiment plan".into());
    }
    let mut names = BTreeSet::new();
    let mut runs = Vec::new();
    for (candidate_index, candidate) in plan.candidates.iter().enumerate() {
        if candidate.name.is_empty() || !names.insert(candidate.name.clone()) {
            return Err("candidate identity collision".into());
        }
        let mut candidate = candidate.clone();
        let mut effective_limits = plan.limits.clone();
        let raw_rules = &raw_plan["candidates"][candidate_index]["rules"];
        let decimal = |value: u64| format!("{}.{:06}", value / 1_000_000, value % 1_000_000);
        if raw_rules.get("half_width_micros").is_some() {
            effective_limits.half_width = decimal(candidate.rules.half_width_micros);
        }
        if raw_rules.get("lot_full_micros").is_some() {
            effective_limits.lot_shares = decimal(candidate.rules.lot_full_micros);
        }
        if raw_rules.get("max_imbalance_micros").is_some() {
            effective_limits.max_imbalance_shares = decimal(candidate.rules.max_imbalance_micros);
        }
        if raw_rules.get("max_feed_age_ms").is_some() {
            effective_limits.max_feed_age_ms = candidate.rules.max_feed_age_ms;
        }
        let configured = effective_limits.core()?;
        candidate.rules.lot_full_micros = configured.lot_micro;
        candidate.rules.half_width_micros = configured.half_width_micro;
        candidate.rules.max_imbalance_micros = configured.max_imbalance_micro;
        candidate.rules.max_feed_age_ms = configured.max_feed_age_ms;
        candidate.rules.validate()?;
        if let Some(p) = &candidate.policy {
            p.validate()?;
        }
        for &latency in &plan.latency_per_leg_ms {
            let delays = LatencyConfig {
                post_ms: latency,
                cancel_ms: latency,
                ack_ms: latency,
                fill_ms: latency,
                timeout_ms: (4 * latency + 1000).max(500),
            };
            let mut paper = simulation::new_with_latency(&effective_limits, delays)?;
            simulation::apply(&mut paper, &Operation::ExactAccounting {})?;
            runs.push(Run {
                candidate: candidate.clone(),
                latency,
                paper,
                current: None,
                guard: ReferenceGuard::default(),
                inventory_since: None,
                gates: BTreeMap::new(),
                errors: Vec::new(),
                orders_rejected: 0,
                fill_events: 0,
                partial_fill_events: 0,
                max_daily_gross_loss: 0,
                max_drawdown: 0,
                error_times: Vec::new(),
                now_wall: 0,
                skipped_delayed_prints: 0,
            });
        }
    }
    let mut states = BTreeMap::<String, MarketState>::new();
    let mut next_tick = 0;
    let mut previous_wall = 0;
    let mut previous_mono = 0;
    let mut header_wall = 0;
    let mut capture_end = 0;
    let mut all_gaps = 0;
    let mut end_complete = false;
    let mut clock_steps = 0;
    let mut clock_step_times = Vec::new();
    let mut deferred = Vec::new();
    let mut diagnostics = diagnostic_ingress.then(IngressDiagnostics::default);
    let receipt = tape::visit(tape_path, plan.max_input_bytes, |record| {
        if previous_wall > 0
            && record
                .wall_ms
                .abs_diff(previous_wall + record.mono_ms.saturating_sub(previous_mono))
                > 100
        {
            clock_steps += 1;
            clock_step_times.push(record.wall_ms);
        }
        while next_tick < record.mono_ms {
            let wall = previous_wall + next_tick.saturating_sub(previous_mono);
            for state in states.values_mut() {
                state.expire_native(wall, next_tick, plan.limits.max_feed_age_ms);
            }
            for run in &mut runs {
                run.tick(wall, next_tick, &mut states, plan.decision_interval_ms);
            }
            next_tick += plan.decision_interval_ms;
        }
        previous_wall = record.wall_ms;
        previous_mono = record.mono_ms;
        match record.input {
            tape::Input::Header {
                started_wall_ms, ..
            } => {
                header_wall = started_wall_ms;
            }
            tape::Input::Market { market, metadata } => {
                if states.len() >= 16
                    || states.contains_key(&market.slug)
                    || states
                        .values()
                        .any(|s| s.market.condition_id == market.condition_id)
                {
                    return Err("market replay budget".into());
                }
                let m = mm2_engine::discovery::parse_market(&metadata, market.window_start_ms)?;
                if serde_json::to_value(tape::MarketWire::from(&m)).unwrap()
                    != serde_json::to_value(&market).unwrap()
                {
                    return Err("market metadata mismatch".into());
                }
                let mut core = Core::new(plan.limits.core()?)?;
                core.set_market(m.clone())?;
                core.control(Action::Resume)?;
                states.insert(
                    m.slug.clone(),
                    MarketState {
                        market: m,
                        core,
                        fee: None,
                        first_snap: [None, None],
                        gaps: 0,
                        invalid: 0,
                        winner: None,
                        outcome_observed_at: None,
                        frames: feeds::Recovery::new([0; 2]),
                        native_snapshots: None,
                        source_generation: None,
                        awaiting_generation: false,
                        recoverable_splits: 0,
                        completed_snapshot_recoveries: 0,
                        discarded_native_events: 0,
                        split_invalid_inputs: 0,
                        completed_quality_recoveries: 0,
                        recovery_started_for_quality: false,
                        max_recovery_ms: plan.limits.max_feed_age_ms,
                        parse_errors: 0,
                        fee_errors: 0,
                        fatal_ingress_errors: 0,
                        recovery_timeouts: 0,
                        diagnostic_reference: None,
                    },
                );
            }
            tape::Input::Fee {
                condition_id,
                raw,
                version,
            } => {
                for state in states
                    .values_mut()
                    .filter(|s| s.market.condition_id == condition_id)
                {
                    state.fee = if version["version"].as_u64() == Some(2) {
                        mm2_live::fees::parse_policy(
                            &raw,
                            &condition_id,
                            &state.market.up_token,
                            &state.market.down_token,
                            record.wall_ms,
                        )
                        .ok()
                    } else {
                        None
                    };
                    if state.fee.is_none() && record.wall_ms < state.market.window_end_ms {
                        state.invalid += 1;
                        state.fee_errors += 1;
                    }
                }
            }
            tape::Input::Frame {
                feed,
                market_slug,
                text,
                generation,
            } => {
                let origin = diagnostics.as_ref().map(|_| FaultOrigin {
                    record_sequence: record.sequence,
                    record_hash: record.hash.clone(),
                    frame_sha256: format!("{:x}", sha2::Sha256::digest(text.as_bytes())),
                    source_feed: feed.clone(),
                    source_generation: generation,
                    record_wall_ms: record.wall_ms,
                    record_mono_ms: record.mono_ms,
                });
                if feed == "binance" {
                    match feeds::parse_binance(&text, record.mono_ms) {
                        Ok(events) => {
                            for state in states
                                .values_mut()
                                .filter(|s| record.wall_ms < s.market.window_end_ms)
                            {
                                for (index, e) in events.iter().enumerate() {
                                    let context = diagnostics.as_mut().zip(origin.as_ref()).map(
                                        |(log, origin)| DiagnosticContext {
                                            log,
                                            origin,
                                            admitted_event_index: index,
                                            preflight_fault: false,
                                        },
                                    );
                                    state.apply_public_diagnostic(
                                        e.clone(),
                                        record.wall_ms,
                                        record.mono_ms,
                                        context,
                                    );
                                }
                            }
                        }
                        Err(_) => {
                            all_gaps += 1;
                            for state in states
                                .values_mut()
                                .filter(|s| record.wall_ms < s.market.window_end_ms)
                            {
                                state.gaps += 1;
                                state.parse_errors += 1;
                                state.diagnostic_reference = None;
                                let _ = state.core.apply(FeedEvent::Disconnect {
                                    source: FeedSource::Binance,
                                    reason: "invalid_reference".into(),
                                });
                            }
                        }
                    }
                } else if feed == "clob" {
                    let slug = market_slug.ok_or("missing CLOB market identity")?;
                    let state = states.get_mut(&slug).ok_or("frame before metadata")?;
                    if state.source_generation != Some(generation) {
                        if state.source_generation.is_some() {
                            state.clear_native(false);
                        }
                        state.source_generation = Some(generation);
                        state.awaiting_generation = false;
                    }
                    if !state.awaiting_generation {
                        match feeds::parse_clob(&text, &state.market, record.mono_ms) {
                            Ok(parsed) => {
                                let count = parsed.len();
                                let events = state.frames.filter(parsed);
                                state.discarded_native_events += (count - events.len()) as u64;
                                // The actor consumes at most sixteen admitted events per turn.
                                // Cross-frame queue coalescing remains unobserved on this tape.
                                'frame: for (batch_index, batch) in events.chunks(16).enumerate() {
                                    let valid = ingress::valid_prefix(
                                        &state.core,
                                        batch,
                                        state.native_snapshots.map(|(_, seen)| seen),
                                    );
                                    for event in &batch[..valid] {
                                        for run in &mut runs {
                                            run.trade(
                                                record.wall_ms,
                                                record.mono_ms,
                                                &slug,
                                                event,
                                                plan.limits.max_feed_age_ms,
                                            );
                                        }
                                    }
                                    for (index, event) in batch.iter().enumerate() {
                                        let context = diagnostics
                                            .as_mut()
                                            .zip(origin.as_ref())
                                            .map(|(log, origin)| DiagnosticContext {
                                                log,
                                                origin,
                                                admitted_event_index: batch_index * 16 + index,
                                                preflight_fault: index == valid,
                                            });
                                        if !state.apply_public_diagnostic(
                                            event.clone(),
                                            record.wall_ms,
                                            record.mono_ms,
                                            context,
                                        ) {
                                            break 'frame;
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                if record.wall_ms < state.market.window_end_ms {
                                    state.invalid += 1;
                                    state.parse_errors += 1;
                                }
                                state.clear_native(true);
                            }
                        }
                    }
                } else {
                    return Err("unknown tape feed".into());
                }
            }
            tape::Input::Gap {
                feed, market_slug, ..
            } => {
                all_gaps += 1;
                for (slug, state) in &mut states {
                    if market_slug.as_ref().is_none_or(|s| s == slug)
                        && record.wall_ms < state.market.window_end_ms
                    {
                        state.gaps += 1;
                        if feed == "fee" {
                            state.fee = None;
                        }
                        if feed == "binance" {
                            state.apply_public(
                                FeedEvent::Disconnect {
                                    source: FeedSource::Binance,
                                    reason: "recorded_capture_gap".into(),
                                },
                                record.wall_ms,
                                record.mono_ms,
                            );
                        } else {
                            state.clear_native(feed == "clob" || feed == "collector");
                        }
                    }
                }
            }
            tape::Input::Outcome { evidence } => {
                if evidence.observed_at_ms > record.wall_ms {
                    return Err("future outcome evidence in tape".into());
                }
                let winner = evidence.winner()?;
                if let Some(s) = states
                    .values_mut()
                    .find(|s| s.market.condition_id == evidence.target.condition_id)
                {
                    if [s.market.up_token.clone(), s.market.down_token.clone()]
                        != evidence.token_ids
                        || evidence.target.start_ms != s.market.window_start_ms
                        || evidence.target.end_ms != s.market.window_end_ms
                        || s.winner.is_some_and(|old| old != winner)
                    {
                        return Err("outcome tokens mismatch".into());
                    }
                    s.winner = Some(winner);
                    s.outcome_observed_at = Some(evidence.observed_at_ms);
                }
                if let Some(prior) =
                    deferred
                        .iter()
                        .find(|prior: &&mm2_engine::paper_resolution::Evidence| {
                            prior.target.condition_id == evidence.target.condition_id
                        })
                {
                    if *prior != evidence {
                        return Err("conflicting duplicate outcome evidence".into());
                    }
                } else {
                    if deferred.len() >= 16
                        || !states
                            .values()
                            .any(|state| state.market.condition_id == evidence.target.condition_id)
                    {
                        return Err("outcome identity or evidence budget".into());
                    }
                    deferred.push(evidence);
                }
            }
            tape::Input::End { complete, .. } => {
                capture_end = record.wall_ms;
                end_complete = complete;
            }
        }
        for run in &mut runs {
            for evidence in &deferred {
                if run
                    .current
                    .as_ref()
                    .and_then(|slug| states.get(slug))
                    .is_some_and(|s| s.market.condition_id != evidence.target.condition_id)
                    && run
                        .paper
                        .has_unresolved_position(&evidence.target.condition_id)
                {
                    run.apply(Operation::PublicResolution {
                        evidence: evidence.clone(),
                        at_ms: record.wall_ms.max(evidence.observed_at_ms),
                    });
                }
            }
        }
        Ok(())
    })?;
    let mut windows: Vec<_> = states
        .values()
        .filter(|s| {
            s.market.window_start_ms >= header_wall && s.market.window_end_ms <= capture_end
        })
        .collect();
    windows.sort_by_key(|s| s.market.window_start_ms);
    let split = windows
        .get(plan.training_windows + plan.embargo_windows)
        .map(|s| s.market.window_start_ms);
    let infra_per_day = domain::parse_micro(&plan.infrastructure_usd_per_day)? as i128 * 1_000_000;
    let mut rows = Vec::new();
    for run in &mut runs {
        run.apply(Operation::CancelAll {
            now_ms: previous_mono,
        });
        run.apply(Operation::Advance {
            now_ms: previous_mono + 10_000,
        });
        run.paper.validate()?;
        let p = run.paper.portfolio_snapshot();
        let mut train = 0_i128;
        let mut holdout = 0_i128;
        let mut train_count = 0;
        let mut holdout_count = 0;
        let mut per_market = Vec::new();
        let mut eligible = true;
        let mut training_quality = true;
        let mut training_filled_markets = 0;
        let mut holdout_filled_markets = 0;
        for (index, state) in windows.iter().enumerate() {
            let position = p
                .positions
                .iter()
                .find(|p| p.market.id == state.market.condition_id);
            let warm = state
                .first_snap
                .iter()
                .all(|t| t.is_some_and(|at| at <= state.market.window_start_ms));
            let quality = state.quality_eligible(plan.quality_protocol);
            if index < plan.training_windows
                || index >= plan.training_windows + plan.embargo_windows
            {
                eligible &= quality;
            }
            if index < plan.training_windows {
                training_quality &= quality
                    && split.is_some_and(|cutoff| {
                        state.outcome_observed_at.is_some_and(|at| at <= cutoff)
                    });
            }
            let pnl = position.and_then(|p| {
                state.winner.map(|winner| {
                    let payout = if winner == mm2_core::Outcome::Up {
                        p.execution_up_micros
                    } else {
                        p.execution_down_micros
                    };
                    payout as i128 * 1_000_000 - p.execution_spent_pico as i128
                })
            });
            let costs = position.map_or(0, |p| {
                p.execution_spent_pico as i128 * plan.extra_cost_bps as i128 / 10_000
            }) + infra_per_day * 300 / 86400;
            let net = pnl.map(|pnl| pnl - costs);
            let part = if index < plan.training_windows {
                "training"
            } else if index >= plan.training_windows + plan.embargo_windows {
                "holdout"
            } else {
                "embargo"
            };
            if quality && let Some(net) = net {
                if index < plan.training_windows {
                    train += net;
                    train_count += 1;
                    if position.is_some_and(|p| p.execution_up_micros + p.execution_down_micros > 0)
                    {
                        training_filled_markets += 1;
                    }
                } else if index >= plan.training_windows + plan.embargo_windows {
                    holdout += net;
                    holdout_count += 1;
                    if position.is_some_and(|p| p.execution_up_micros + p.execution_down_micros > 0)
                    {
                        holdout_filled_markets += 1;
                    }
                }
            }
            let mut market_report = json!({"slug":state.market.slug,"partition":part,"quality_eligible":quality,"preopen_snapshots":warm,"gaps":state.gaps,"invalid_inputs":state.invalid,"recoverable_split_deltas":state.recoverable_splits,"completed_native_snapshot_recoveries":state.completed_snapshot_recoveries,"native_events_discarded_by_production_filter":state.discarded_native_events,"awaiting_observed_source_generation":state.awaiting_generation,"final_outcome_observed":state.winner.is_some(),"model_gross_pnl_pico":pnl.map(|v|v.to_string()),"assumed_extra_cost_pico":costs.to_string(),"model_net_pnl_pico":net.map(|v|v.to_string())});
            if plan.quality_protocol == QualityProtocol::RecoveredNativeV2 {
                market_report["recovered_native_quality"] = state.recovered_quality_evidence();
            }
            per_market.push(market_report);
        }
        let holdout_eligible = eligible
            && end_complete
            && all_gaps == 0
            && plan.frozen_at_wall_ms <= header_wall
            && train_count == plan.training_windows
            && holdout_count >= plan.min_holdout_windows
            && run.errors.is_empty()
            && training_filled_markets >= 2
            && holdout_filled_markets >= 2
            && clock_steps == 0;
        let full_settled_gross: i128 = p
            .positions
            .iter()
            .filter_map(|p| {
                states
                    .values()
                    .find(|s| s.market.condition_id == p.market.id)
                    .and_then(|s| s.winner)
                    .map(|winner| {
                        let quantity = if winner == mm2_core::Outcome::Up {
                            p.execution_up_micros
                        } else {
                            p.execution_down_micros
                        };
                        quantity as i128 * 1_000_000 - p.execution_spent_pico as i128
                    })
            })
            .sum();
        let unknown_spent: u128 = p
            .positions
            .iter()
            .filter(|p| {
                states
                    .values()
                    .find(|s| s.market.condition_id == p.market.id)
                    .is_none_or(|s| s.winner.is_none())
            })
            .map(|p| p.execution_spent_pico)
            .sum();
        let full_assumed_cost = p
            .positions
            .iter()
            .map(|p| p.execution_spent_pico as i128 * plan.extra_cost_bps as i128 / 10000)
            .sum::<i128>()
            + infra_per_day * (capture_end.saturating_sub(header_wall)) as i128 / 86_400_000;
        let scored_gross: i128 = per_market
            .iter()
            .filter(|m| m["partition"] != "embargo")
            .filter_map(|m| m["model_gross_pnl_pico"].as_str()?.parse::<i128>().ok())
            .sum();
        rows.push(json!({"candidate":run.candidate.name,"latency_per_leg_ms":run.latency,"training_settled_windows":train_count,"holdout_settled_windows":holdout_count,"training_filled_markets":training_filled_markets,"holdout_filled_markets":holdout_filled_markets,"training_net_pnl_pico":if train_count>0{Some(train.to_string())}else{None},"holdout_net_pnl_pico":if holdout_count>0{Some(holdout.to_string())}else{None},"training_selection_eligible":training_quality && plan.frozen_at_wall_ms<=header_wall && train_count==plan.training_windows && training_filled_markets>=2 && train>0 && !run.error_times.iter().any(|at|split.is_some_and(|cutoff|*at<cutoff)) && !clock_step_times.iter().any(|at|split.is_some_and(|cutoff|*at<cutoff)),"effective_rules":run.candidate.rules,"skipped_delayed_prints":run.skipped_delayed_prints,"holdout_eligible":holdout_eligible,"status":if holdout_eligible{"APPROXIMATE_PUBLIC_INPUT_MODEL"}else{"INCOMPLETE_NOT_ELIGIBLE"},"full_run_settled_gross_pnl_pico":full_settled_gross.to_string(),"full_run_net_pnl_pico":if unknown_spent==0{Some((full_settled_gross-full_assumed_cost).to_string())}else{None},"unknown_terminal_spent_pico":unknown_spent.to_string(),"excluded_window_settled_gross_pnl_pico":(full_settled_gross-scored_gross).to_string(),"assumed_full_run_cost_pico":full_assumed_cost.to_string(),"fill_events":run.fill_events,"partial_fill_events":run.partial_fill_events,"filled_quantity_micros":p.positions.iter().map(|p|p.execution_up_micros+p.execution_down_micros).sum::<u64>(),"max_daily_gross_loss_pico":run.max_daily_gross_loss.to_string(),"max_drawdown_pico":run.max_drawdown.to_string(),"errors":run.errors,"quote_rejections":run.orders_rejected,"gates":run.gates,"portfolio":p,"markets":per_market}));
    }
    let selection = select_training(&rows, &plan.latency_per_leg_ms);
    let report = json!({"schema":1,"network":"none","orders_submitted":0,"plan_sha256":format!("{:x}",sha2::Sha256::digest(bytes)),"declared_plan_time_precedes_capture":plan.frozen_at_wall_ms<=header_wall,"tape_receipt":receipt,"capture_final_complete_flag":end_complete,"capture_gaps":all_gaps,"clock_steps":clock_steps,"eligible_window_candidates":windows.len(),"training_windows_required":plan.training_windows,"embargo_windows":plan.embargo_windows,"execution_model_revision":"native_public_print_causal_v3_shared_native_ingress_v1","holdout_windows_required":plan.min_holdout_windows,"holdout_start_wall_ms":split,"training_only_selection":selection,"rows":rows,"limitations":["Synthetic execution on observed public input, never real PnL or promised ROI.","No documented CLOB sequence proves capture completeness; raw receive ordering belongs to this host.","Fixed per-leg latency stresses are assumptions, not measured order round trips.","Conservative native L2 queue approximation uses prints at their reported price, no full sweep proof.","Equal timestamps retain original captured frame order, independent candidates have separate queues and finite capital.","Infrastructure and extra execution costs are explicit plan assumptions; rebates zero; actual gas unknown.","Whole first partial and last incomplete market windows excluded from scored totals; their trades still consume capital, full-run losses and unknown terminal cost remain visible.","No capital reset at training/holdout boundary; realized settled market PnL is attributed by market start.","Native frame filtering and split-snapshot admission share production code; this tape lacks REST bootstrap responses/cutoffs, engine-triggered refresh receipts and actor cross-frame batching. Replay waits for an observed generation after genuine faults/timeouts rather than inventing recovery.","Replay tick scheduler and raw-frame chunking approximate actor scheduling; exact production parity remains unproven.","Declared freeze timestamp alone does not prove plan immutability; require externally retained pre-capture plan hash and launch binding."]});
    let report = if plan.quality_protocol == QualityProtocol::RecoveredNativeV2 {
        let mut recovered_report = report;
        recovered_report["quality_protocol"] = json!("recovered_native_v2");
        recovered_report["quality_protocol_definition"] = json!({
            "legacy_raw_invalid_counts_preserved": true,
            "recoverable_split_is_not_itself_a_fatal_input": true,
            "both_native_full_snapshots_required": true,
            "completion_strictly_before_market_close": true,
            "completion_age_strictly_below_configured_max_feed_age": true,
            "all_genuine_capture_gaps_still_reject": true,
            "timeout_fault_or_unresolved_recovery_still_rejects": true,
            "requires_new_preregistered_capture_and_plan": true,
        });
        recovered_report["limitations"].as_array_mut().expect("report limitations").push(json!(
            "Recovered-native v2 changes evidence classification only. Both valid native snapshots must actually restore books in time; quote, cancellation, fill and risk semantics are unchanged. It does not authorize rescoring sealed old evidence for selection."
        ));
        recovered_report
    } else {
        report
    };
    let report = if let Some(diagnostics) = diagnostics {
        let mut report = report;
        report["ingress_diagnostics"] = diagnostics.report();
        report["training_only_selection"] = json!([]);
        report["selection_or_promotion_authorized"] = json!(false);
        for row in report["rows"].as_array_mut().expect("report rows") {
            row["training_selection_eligible"] = json!(false);
            row["holdout_eligible"] = json!(false);
            row["status"] = json!("DIAGNOSTIC_ONLY_NOT_FOR_SELECTION");
        }
        report
    } else {
        report
    };
    Ok(report)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let diagnostic_ingress = args.len() == 5 && args[4] == "--diagnostic-ingress";
    if args.len() != 4 && !diagnostic_ingress {
        return Err("usage: experiment_replay TAPE.jsonl FROZEN_PLAN.json NEW_REPORT.json [--diagnostic-ingress]".into());
    }
    let mut bytes = Vec::new();
    File::open(&args[2])?.take(65537).read_to_end(&mut bytes)?;
    let report = if diagnostic_ingress {
        evaluate_mode(Path::new(&args[1]), &bytes, true)?
    } else {
        evaluate(Path::new(&args[1]), &bytes)?
    };
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[3])?;
    serde_json::to_writer_pretty(&mut out, &report)?;
    out.write_all(b"\n")?;
    out.sync_all()?;
    println!(
        "{}",
        json!({"report":args[3],"runs":report["rows"].as_array().unwrap().len(),"windows":report["eligible_window_candidates"],"orders_submitted":0})
    );
    Ok(())
}

use sha2::Digest;
#[cfg(test)]
mod tests {
    use super::*;
    fn quality_snapshot(
        condition: &str,
        outcome: domain::Outcome,
        wall: u64,
        mono: u64,
    ) -> FeedEvent {
        FeedEvent::Snapshot {
            condition_id: condition.into(),
            outcome,
            exchange_ms: wall,
            observed_mono_ms: mono,
            bids: vec![domain::Level {
                price_micro: 490_000,
                quantity_micro: 10_000_000,
            }],
            asks: vec![domain::Level {
                price_micro: 510_000,
                quantity_micro: 10_000_000,
            }],
        }
    }
    fn quality_state() -> MarketState {
        let market = Market {
            slug: "synthetic-quality-test".into(),
            condition_id: format!("0x{}", "a".repeat(64)),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: 1000,
            window_end_ms: 301000,
            tick_micro: 10000,
        };
        let limits = Limits {
            initial_collateral_usd: "1000".into(),
            max_imbalance_shares: "10".into(),
            lot_shares: "5".into(),
            half_width: "0.01".into(),
            max_feed_age_ms: 1000,
        };
        let mut core = Core::new(limits.core().unwrap()).unwrap();
        core.set_market(market.clone()).unwrap();
        core.control(Action::Resume).unwrap();
        core.apply(FeedEvent::Reference {
            exchange_ms: 999,
            observed_mono_ms: 1,
            price_micro: 80_000_000_000,
        })
        .unwrap();
        let mut state = MarketState {
            market,
            core,
            fee: None,
            first_snap: [None, None],
            gaps: 0,
            invalid: 0,
            winner: Some(mm2_core::Outcome::Up),
            outcome_observed_at: Some(301001),
            frames: feeds::Recovery::new([0; 2]),
            native_snapshots: None,
            source_generation: Some(0),
            awaiting_generation: false,
            recoverable_splits: 0,
            completed_snapshot_recoveries: 0,
            discarded_native_events: 0,
            split_invalid_inputs: 0,
            completed_quality_recoveries: 0,
            recovery_started_for_quality: false,
            max_recovery_ms: 1000,
            parse_errors: 0,
            fee_errors: 0,
            fatal_ingress_errors: 0,
            recovery_timeouts: 0,
            diagnostic_reference: None,
        };
        for outcome in [domain::Outcome::Up, domain::Outcome::Down] {
            let event = quality_snapshot(&state.market.condition_id, outcome, 999, 1);
            assert!(state.apply_public(event, 999, 1));
        }
        state
    }
    fn quality_split(state: &MarketState, wall: u64, mono: u64) -> FeedEvent {
        FeedEvent::Delta {
            condition_id: state.market.condition_id.clone(),
            exchange_ms: wall,
            observed_mono_ms: mono,
            changes: vec![domain::Change {
                outcome: domain::Outcome::Up,
                side: domain::BookSide::Bid,
                level: domain::Level {
                    price_micro: 520_000,
                    quantity_micro: 5_000_000,
                },
            }],
        }
    }
    fn complete_quality_recovery(state: &mut MarketState, wall: u64, mono: u64) {
        for (offset, outcome) in [domain::Outcome::Up, domain::Outcome::Down]
            .into_iter()
            .enumerate()
        {
            let event = quality_snapshot(
                &state.market.condition_id,
                outcome,
                wall + offset as u64,
                mono + offset as u64,
            );
            assert!(state.apply_public(event, wall + offset as u64, mono + offset as u64));
        }
    }
    fn diagnostic_origin(feed: &str) -> FaultOrigin {
        FaultOrigin {
            record_sequence: 7,
            record_hash: "a".repeat(64),
            frame_sha256: format!("{:x}", sha2::Sha256::digest(b"public fixture")),
            source_feed: feed.into(),
            source_generation: 2,
            record_wall_ms: 1002,
            record_mono_ms: 4,
        }
    }
    #[test]
    fn ingress_diagnostic_keeps_pre_error_book_clocks_without_changing_failure() {
        let mut observed = quality_state();
        let mut ordinary = quality_state();
        let event = quality_snapshot(&observed.market.condition_id, domain::Outcome::Up, 998, 2);
        assert_eq!(
            ingress::valid_prefix(&observed.core, std::slice::from_ref(&event), None),
            0
        );
        let origin = diagnostic_origin("clob");
        let mut log = IngressDiagnostics::default();
        assert!(!observed.apply_public_diagnostic(
            event.clone(),
            1002,
            2,
            Some(DiagnosticContext {
                log: &mut log,
                origin: &origin,
                admitted_event_index: 0,
                preflight_fault: true,
            })
        ));
        assert!(!ordinary.apply_public(event, 1002, 2));
        assert_eq!(observed.core.snapshot(), ordinary.core.snapshot());
        assert_eq!(observed.fatal_ingress_errors, ordinary.fatal_ingress_errors);
        assert_eq!(observed.awaiting_generation, ordinary.awaiting_generation);
        assert_eq!(
            log.witnesses[0]["error_class"],
            "clob_exchange_clock_regressed"
        );
        assert_eq!(
            log.witnesses[0]["context"]["pre_error_books"]["up"]["exchange_ms"],
            999
        );
        assert_eq!(
            log.witnesses[0]["context"]["accepted_clock_watermarks"]["previous_receive_ms"],
            1
        );
        assert_eq!(log.witnesses[0]["origin"]["record_sequence"], 7);
        assert_eq!(log.witnesses[0]["origin"]["record_hash"], "a".repeat(64));
        assert_eq!(
            log.witnesses[0]["origin"]["frame_sha256"],
            format!("{:x}", sha2::Sha256::digest(b"public fixture"))
        );
    }
    #[test]
    fn ingress_diagnostic_reference_watermark_resets_with_disconnect_and_actual_failure() {
        let mut state = quality_state();
        let mut log = IngressDiagnostics::default();
        let origin = diagnostic_origin("binance");
        let reference = |exchange, received, price| FeedEvent::Reference {
            exchange_ms: exchange,
            observed_mono_ms: received,
            price_micro: price,
        };
        assert!(state.apply_public_diagnostic(
            reference(1001, 3, 80_000_000_000),
            1001,
            3,
            Some(DiagnosticContext {
                log: &mut log,
                origin: &origin,
                admitted_event_index: 0,
                preflight_fault: false,
            })
        ));
        assert!(log.witnesses.is_empty());
        assert!(state.apply_public(
            FeedEvent::Disconnect {
                source: FeedSource::Binance,
                reason: "controlled".into()
            },
            1002,
            4
        ));
        assert!(state.diagnostic_reference.is_none());
        assert!(state.core.snapshot().reference.is_none());
        // An earlier exchange clock after a real disconnect is accepted by Core;
        // the diagnostic mirror must not invent the old watermark or a fault.
        assert!(state.apply_public_diagnostic(
            reference(1000, 5, 80_000_000_000),
            1003,
            5,
            Some(DiagnosticContext {
                log: &mut log,
                origin: &origin,
                admitted_event_index: 0,
                preflight_fault: false,
            })
        ));
        assert!(log.witnesses.is_empty());
        assert!(!state.apply_public_diagnostic(
            reference(999, 6, 80_000_000_000),
            1004,
            6,
            Some(DiagnosticContext {
                log: &mut log,
                origin: &origin,
                admitted_event_index: 0,
                preflight_fault: false,
            })
        ));
        assert_eq!(
            log.witnesses[0]["error_class"],
            "invalid_or_regressed_reference_input"
        );
        assert_eq!(log.witnesses[0]["origin"]["source_feed"], "binance");
        assert_eq!(
            log.witnesses[0]["context"]["pre_error_reference"]["exchange_ms"],
            1000
        );
        assert_eq!(log.witnesses[0]["context"]["event"]["exchange_ms"], 999);
        assert!(state.diagnostic_reference.is_none());
        assert!(state.core.snapshot().reference.is_none());
        assert!(state.apply_public_diagnostic(
            reference(998, 7, 80_000_000_000),
            1005,
            7,
            Some(DiagnosticContext {
                log: &mut log,
                origin: &origin,
                admitted_event_index: 0,
                preflight_fault: false,
            })
        ));
        assert_eq!(log.witnesses.len(), 1);
        let mut default = quality_state();
        assert!(default.apply_public(reference(1001, 3, 80_000_000_000), 1001, 3));
        assert!(
            default.diagnostic_reference.is_none(),
            "default has no diagnostic cache maintenance"
        );
    }
    #[test]
    fn ingress_diagnostic_caps_witnesses_and_suppresses_unrecognized_error_text() {
        let origin = diagnostic_origin("binance");
        let mut log = IngressDiagnostics::default();
        for _ in 0..37 {
            log.record(
                &origin,
                json!({"pre_error_summary_available":false}),
                "private-token=NEVER-RETAIN",
            );
        }
        let report = log.report();
        assert_eq!(report["total_faults"], 37);
        assert_eq!(report["omitted_witnesses"], 5);
        assert_eq!(report["witnesses"].as_array().unwrap().len(), 32);
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains("private-token") && !encoded.contains("NEVER-RETAIN"));
        assert!(
            report["witnesses"]
                .as_array()
                .unwrap()
                .iter()
                .all(|w| w["error_class"] == "unclassified_native_error")
        );
    }
    #[test]
    fn recovered_quality_requires_both_actual_valid_snapshots_and_preserves_strict_counts() {
        let mut state = quality_state();
        assert!(state.quality_eligible(QualityProtocol::StrictV1));
        assert!(state.quality_eligible(QualityProtocol::RecoveredNativeV2));
        assert!(state.apply_public(quality_split(&state, 1001, 3), 1001, 3));
        assert!(!state.quality_eligible(QualityProtocol::RecoveredNativeV2));
        assert!(!state.core.evaluate(1001, 3).stop_reasons.is_empty());
        let event = quality_snapshot(&state.market.condition_id, domain::Outcome::Up, 1002, 4);
        assert!(state.apply_public(event, 1002, 4));
        assert!(!state.quality_eligible(QualityProtocol::RecoveredNativeV2));
        assert!(!state.core.evaluate(1002, 4).stop_reasons.is_empty());
        let event = quality_snapshot(&state.market.condition_id, domain::Outcome::Down, 1003, 5);
        assert!(state.apply_public(event, 1003, 5));
        assert!(state.core.evaluate(1003, 5).stop_reasons.is_empty());
        assert_eq!(state.invalid, 1);
        assert_eq!(state.recoverable_splits, 1);
        assert_eq!(state.completed_snapshot_recoveries, 1);
        assert_eq!(state.fatal_invalid_inputs(), 0);
        assert_eq!(state.unresolved_quality_recoveries(), 0);
        assert!(!state.quality_eligible(QualityProtocol::StrictV1));
        assert!(state.quality_eligible(QualityProtocol::RecoveredNativeV2));
        state.gaps = 1;
        assert!(!state.quality_eligible(QualityProtocol::RecoveredNativeV2));
    }
    #[test]
    fn recovery_timeout_exact_age_and_after_close_never_receive_quality_credit() {
        let mut expired = quality_state();
        assert!(expired.apply_public(quality_split(&expired, 1001, 3), 1001, 3));
        expired.expire_native(2001, 1003, 1000);
        assert_eq!(expired.recovery_timeouts, 1);
        assert_eq!(expired.fatal_invalid_inputs(), 1);
        assert_eq!(expired.unresolved_quality_recoveries(), 1);
        assert!(expired.awaiting_generation);
        assert!(!expired.quality_eligible(QualityProtocol::RecoveredNativeV2));
        let mut exact = quality_state();
        assert!(exact.apply_public(quality_split(&exact, 1001, 3), 1001, 3));
        // No timeout tick between records: equality to max_age still rejects.
        complete_quality_recovery(&mut exact, 2000, 1002);
        assert_eq!(exact.completed_snapshot_recoveries, 1);
        assert_eq!(exact.completed_quality_recoveries, 0);
        assert!(!exact.quality_eligible(QualityProtocol::RecoveredNativeV2));
        let mut closed = quality_state();
        assert!(closed.apply_public(quality_split(&closed, 300999, 3), 300999, 3));
        complete_quality_recovery(&mut closed, 301000, 4);
        assert_eq!(closed.completed_snapshot_recoveries, 1);
        assert_eq!(closed.completed_quality_recoveries, 0);
        assert!(!closed.quality_eligible(QualityProtocol::RecoveredNativeV2));
    }
    #[test]
    fn malformed_snapshot_and_generation_reset_cannot_forgive_an_old_recovery() {
        let mut invalid = quality_state();
        assert!(invalid.apply_public(quality_split(&invalid, 1001, 3), 1001, 3));
        let mut bad = quality_snapshot(&invalid.market.condition_id, domain::Outcome::Up, 1002, 4);
        if let FeedEvent::Snapshot { bids, .. } = &mut bad {
            bids[0].price_micro = 520_000;
        }
        assert!(!invalid.apply_public(bad, 1002, 4));
        assert_eq!(invalid.fatal_ingress_errors, 1);
        assert!(!invalid.quality_eligible(QualityProtocol::RecoveredNativeV2));
        for gap in [false, true] {
            let mut reset = quality_state();
            assert!(reset.apply_public(quality_split(&reset, 1001, 3), 1001, 3));
            reset.gaps = u64::from(gap);
            reset.clear_native(false);
            reset.source_generation = Some(1);
            complete_quality_recovery(&mut reset, 1100, 100);
            assert!(reset.apply_public(quality_split(&reset, 1102, 102), 1102, 102));
            complete_quality_recovery(&mut reset, 1103, 103);
            assert_eq!(reset.completed_quality_recoveries, 1);
            assert_eq!(reset.split_invalid_inputs, 2);
            assert_eq!(reset.unresolved_quality_recoveries(), 1);
            assert!(!reset.quality_eligible(QualityProtocol::RecoveredNativeV2));
            assert_eq!(reset.gaps, u64::from(gap));
        }
    }
    #[test]
    fn holdout_fault_cannot_replace_training_winner_and_negative_training_never_selects() {
        let rows = vec![
            json!({"candidate":"training_best","latency_per_leg_ms":50,"training_selection_eligible":true,"training_net_pnl_pico":"500","holdout_eligible":false,"holdout_net_pnl_pico":null,"errors":["holdout_fault"]}),
            json!({"candidate":"training_second","latency_per_leg_ms":50,"training_selection_eligible":true,"training_net_pnl_pico":"100","holdout_eligible":true,"holdout_net_pnl_pico":"900"}),
        ];
        let selected = select_training(&rows, &[50]);
        assert_eq!(selected[0]["candidate"], "training_best");
        assert!(selected[0]["chosen_holdout_net_pnl_pico"].is_null());
        let negative = vec![
            json!({"candidate":"losing","latency_per_leg_ms":50,"training_selection_eligible":false,"training_net_pnl_pico":"-500"}),
        ];
        let selected = select_training(&negative, &[50]);
        assert!(selected[0]["candidate"].is_null());
        assert_eq!(selected[0]["status"], "NO_POSITIVE_TRAINING_CANDIDATE");
    }
    #[test]
    fn full_native_input_fixture_replays_independent_candidates_without_fake_terminal_pnl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixture.jsonl");
        let start = 1790712600000_u64;
        let origin = start - 2000;
        let condition = format!("0x{}", "a".repeat(64));
        let metadata = json!({"slug":"btc-updown-5m-1790712600","active":true,"closed":false,"acceptingOrders":true,"eventStartTime":"2026-09-29T20:10:00Z","endDate":"2026-09-29T20:15:00Z","conditionId":condition,"clobTokenIds":"[\"1\",\"2\"]","outcomes":"[\"Up\",\"Down\"]","orderPriceMinTickSize":0.01,"cryptoMarketConfig":{"asset":"btc","duration":"5m","twapEnabled":true,"twapLookbackSeconds":60}});
        let market = mm2_engine::discovery::parse_market(&metadata, start).unwrap();
        let mut writer = tape::Writer::new(&path, 4_000_000).unwrap();
        writer
            .append(
                origin,
                0,
                tape::Input::Header {
                    schema: 1,
                    source: "synthetic_controlled_test".into(),
                    started_wall_ms: origin,
                    max_bytes: 4_000_000,
                    max_frames: 10000,
                },
                false,
            )
            .unwrap();
        writer
            .append(
                origin,
                0,
                tape::Input::Market {
                    market: tape::MarketWire::from(&market),
                    metadata,
                },
                false,
            )
            .unwrap();
        let fee = json!({"c":condition,"t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}});
        let mut frames = 0;
        for elapsed in (1..302001).step_by(500) {
            let at = origin + elapsed;
            if elapsed % 30000 == 1 {
                writer
                    .append(
                        at,
                        elapsed,
                        tape::Input::Fee {
                            condition_id: condition.clone(),
                            raw: fee.clone(),
                            version: json!({"version":2}),
                        },
                        false,
                    )
                    .unwrap();
            }
            let reference = json!({"e":"trade","s":"BTCUSDT","T":at,"E":at,"t":elapsed,"m":true,"q":"1","p":"100000"});
            writer
                .append(
                    at,
                    elapsed,
                    tape::Input::Frame {
                        feed: "binance".into(),
                        market_slug: None,
                        generation: 0,
                        text: reference.to_string(),
                    },
                    false,
                )
                .unwrap();
            frames += 1;
            let books:Vec<_>=["1","2"].iter().map(|token|json!({"event_type":"book","market":condition,"asset_id":token,"timestamp":at.to_string(),"bids":[{"price":"0.49","size":"1"}],"asks":[{"price":"0.51","size":"100"}]})).collect();
            writer
                .append(
                    at,
                    elapsed,
                    tape::Input::Frame {
                        feed: "clob".into(),
                        market_slug: Some(market.slug.clone()),
                        generation: 0,
                        text: serde_json::to_string(&books).unwrap(),
                    },
                    false,
                )
                .unwrap();
            frames += 1;
            if elapsed > 4000 && elapsed % 5000 == 1 {
                let prints:Vec<_>=["1","2"].iter().map(|token|json!({"event_type":"last_trade_price","market":condition,"asset_id":token,"timestamp":at.to_string(),"side":"SELL","price":"0.49","size":"2","fee_rate_bps":"0"})).collect();
                writer
                    .append(
                        at,
                        elapsed,
                        tape::Input::Frame {
                            feed: "clob".into(),
                            market_slug: Some(market.slug.clone()),
                            generation: 0,
                            text: serde_json::to_string(&prints).unwrap(),
                        },
                        false,
                    )
                    .unwrap();
                frames += 1;
            }
        }
        writer
            .append(
                origin + 302500,
                302500,
                tape::Input::End {
                    reason: "fixture".into(),
                    frames,
                    gaps: 0,
                    complete: true,
                },
                true,
            )
            .unwrap();
        writer.finish().unwrap();
        let plan = Plan {
            schema: 1,
            frozen_at_wall_ms: origin - 1,
            limits: Limits {
                initial_collateral_usd: "1000".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
            candidates: vec![
                Candidate {
                    name: "baseline".into(),
                    rules: Default::default(),
                    policy: None,
                },
                Candidate {
                    name: "paired".into(),
                    rules: Default::default(),
                    policy: Some(Default::default()),
                },
            ],
            latency_per_leg_ms: vec![50, 250],
            decision_interval_ms: 100,
            training_windows: 1,
            embargo_windows: 1,
            min_holdout_windows: 1,
            infrastructure_usd_per_day: "1".into(),
            extra_cost_bps: 1,
            max_input_bytes: 4_000_000,
            quality_protocol: QualityProtocol::StrictV1,
        };
        // Serialize the real schema with empty rules, as the frozen CLI plan does.
        let mut raw = serde_json::to_value(plan).unwrap();
        assert!(
            raw.get("quality_protocol").is_none(),
            "strict_v1 preserves old plan serialization"
        );
        let decoded: Plan = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(decoded.quality_protocol, QualityProtocol::StrictV1);
        raw["quality_protocol"] = json!("unrecognized_quality_protocol");
        assert!(serde_json::from_value::<Plan>(raw.clone()).is_err());
        raw.as_object_mut().unwrap().remove("quality_protocol");
        for c in raw["candidates"].as_array_mut().unwrap() {
            c["rules"] = json!({});
        }
        let plan_bytes = serde_json::to_vec(&raw).unwrap();
        let report = evaluate(&path, &plan_bytes).unwrap();
        let mut diagnostic_report = evaluate_mode(&path, &plan_bytes, true).unwrap();
        assert_eq!(
            diagnostic_report["ingress_diagnostics"]["mode"],
            "diagnostic_only_not_for_selection"
        );
        assert_eq!(
            diagnostic_report["selection_or_promotion_authorized"],
            false
        );
        assert_eq!(diagnostic_report["training_only_selection"], json!([]));
        for (ordinary, diagnostic) in report["rows"]
            .as_array()
            .unwrap()
            .iter()
            .zip(diagnostic_report["rows"].as_array_mut().unwrap())
        {
            assert_eq!(diagnostic["training_selection_eligible"], false);
            assert_eq!(diagnostic["holdout_eligible"], false);
            assert_eq!(diagnostic["status"], "DIAGNOSTIC_ONLY_NOT_FOR_SELECTION");
            for field in ["training_selection_eligible", "holdout_eligible", "status"] {
                diagnostic[field] = ordinary[field].clone();
            }
        }
        diagnostic_report
            .as_object_mut()
            .unwrap()
            .remove("ingress_diagnostics");
        diagnostic_report
            .as_object_mut()
            .unwrap()
            .remove("selection_or_promotion_authorized");
        diagnostic_report["training_only_selection"] = report["training_only_selection"].clone();
        assert_eq!(
            serde_json::to_vec(&diagnostic_report).unwrap(),
            serde_json::to_vec(&report).unwrap(),
            "diagnostic mode must preserve every non-diagnostic/non-selection report byte"
        );
        let rows = report["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(report["orders_submitted"], 0);
        assert!(
            rows.iter()
                .all(|r| r["errors"].as_array().unwrap().is_empty()),
            "{report}"
        );
        assert!(rows.iter().any(|r| r["fill_events"].as_u64().unwrap() > 0));
        assert!(
            rows.iter()
                .all(|r| r["holdout_eligible"] == false && r["full_run_net_pnl_pico"].is_null())
        );
        assert!(
            rows.iter()
                .all(|r| r["portfolio"]["initial_cash_pico"] == "1000000000000000")
        );
    }
}
