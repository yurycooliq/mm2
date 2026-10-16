//! Counterfactual strategies consume full common public inputs, never filtered historical fills.
use mm2_emulator::LatencyConfig;
use mm2_engine::experiments as tape;
use mm2_engine::{
    config::Limits,
    domain::{self, Action, Core, FeedEvent, FeedSource, Market, StopReason},
    feeds,
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
}
fn default_embargo() -> usize {
    1
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
        let before = self.paper.snapshot();
        match simulation::apply(&mut self.paper, &op) {
            Ok(()) => {
                let after = self.paper.snapshot();
                self.skipped_delayed_prints += after
                    .venue_stats
                    .causality_excluded_prints
                    .saturating_sub(before.venue_stats.causality_excluded_prints);
                if after.audit_sequence >= before.audit_sequence {
                    for event in after.recent_audit.iter().filter(|event| {
                        event.sequence > before.audit_sequence && event.kind == "venue_fill"
                    }) {
                        self.fill_events += 1;
                        if after.venue_orders.iter().any(|order| {
                            Some(order.id) == event.order_id && order.remaining_micros > 0
                        }) {
                            self.partial_fill_events += 1;
                        }
                    }
                }
                let portfolio = self.paper.portfolio_snapshot();
                self.max_daily_gross_loss = self
                    .max_daily_gross_loss
                    .max(portfolio.daily_loss_pico.parse().unwrap_or(u128::MAX));
                self.max_drawdown = self
                    .max_drawdown
                    .max(portfolio.drawdown_pico.parse().unwrap_or(u128::MAX));
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
        let account = self.paper.snapshot().account;
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
                    }
                }
            }
            tape::Input::Frame {
                feed,
                market_slug,
                text,
                ..
            } => {
                if feed == "binance" {
                    match feeds::parse_binance(&text, record.mono_ms) {
                        Ok(events) => {
                            for state in states
                                .values_mut()
                                .filter(|s| record.wall_ms < s.market.window_end_ms)
                            {
                                for e in &events {
                                    if state.core.apply(e.clone()).is_err()
                                        && record.wall_ms < state.market.window_end_ms
                                    {
                                        state.invalid += 1;
                                    }
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
                    match feeds::parse_clob(&text, &state.market, record.mono_ms) {
                        Ok(events) => {
                            for event in events {
                                if let FeedEvent::Snapshot { outcome, .. } = &event {
                                    state.first_snap
                                        [usize::from(*outcome == domain::Outcome::Down)]
                                    .get_or_insert(record.wall_ms);
                                }
                                for run in &mut runs {
                                    run.trade(
                                        record.wall_ms,
                                        record.mono_ms,
                                        &slug,
                                        &event,
                                        plan.limits.max_feed_age_ms,
                                    );
                                }
                                if state.core.apply(event).is_err()
                                    && record.wall_ms < state.market.window_end_ms
                                {
                                    state.invalid += 1;
                                }
                            }
                        }
                        Err(_) => {
                            if record.wall_ms < state.market.window_end_ms {
                                state.invalid += 1;
                            }
                            let _ = state.core.apply(FeedEvent::Disconnect {
                                source: FeedSource::Clob,
                                reason: "invalid_frame".into(),
                            });
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
                        let source = if feed == "binance" {
                            FeedSource::Binance
                        } else {
                            FeedSource::Clob
                        };
                        let _ = state.core.apply(FeedEvent::Disconnect {
                            source,
                            reason: "recorded_capture_gap".into(),
                        });
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
                    && run.paper.portfolio_snapshot().positions.iter().any(|p| {
                        p.market.id == evidence.target.condition_id && p.resolution.is_none()
                    })
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
            let quality = warm && state.gaps == 0 && state.invalid == 0 && state.winner.is_some();
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
            per_market.push(json!({"slug":state.market.slug,"partition":part,"quality_eligible":quality,"preopen_snapshots":warm,"gaps":state.gaps,"invalid_inputs":state.invalid,"final_outcome_observed":state.winner.is_some(),"model_gross_pnl_pico":pnl.map(|v|v.to_string()),"assumed_extra_cost_pico":costs.to_string(),"model_net_pnl_pico":net.map(|v|v.to_string())}));
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
    let report = json!({"schema":1,"network":"none","orders_submitted":0,"plan_sha256":format!("{:x}",sha2::Sha256::digest(bytes)),"declared_plan_time_precedes_capture":plan.frozen_at_wall_ms<=header_wall,"tape_receipt":receipt,"capture_final_complete_flag":end_complete,"capture_gaps":all_gaps,"clock_steps":clock_steps,"eligible_window_candidates":windows.len(),"training_windows_required":plan.training_windows,"embargo_windows":plan.embargo_windows,"execution_model_revision":"native_public_print_causal_v3_all_candidates","holdout_windows_required":plan.min_holdout_windows,"holdout_start_wall_ms":split,"training_only_selection":selection,"rows":rows,"limitations":["Synthetic execution on observed public input, never real PnL or promised ROI.","No documented CLOB sequence proves capture completeness; raw receive ordering belongs to this host.","Fixed per-leg latency stresses are assumptions, not measured order round trips.","Conservative native L2 queue approximation uses prints at their reported price, no full sweep proof.","Equal timestamps retain original captured frame order, independent candidates have separate queues and finite capital.","Infrastructure and extra execution costs are explicit plan assumptions; rebates zero; actual gas unknown.","Whole first partial and last incomplete market windows excluded from scored totals; their trades still consume capital, full-run losses and unknown terminal cost remain visible.","No capital reset at training/holdout boundary; realized settled market PnL is attributed by market start.","Replay tick scheduler approximates runtime scheduling; production parity requires separate controlled comparison.","Declared freeze timestamp alone does not prove plan immutability; require externally retained pre-capture plan hash and launch binding."]});
    Ok(report)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: experiment_replay TAPE.jsonl FROZEN_PLAN.json NEW_REPORT.json".into());
    }
    let mut bytes = Vec::new();
    File::open(&args[2])?.take(65537).read_to_end(&mut bytes)?;
    let report = evaluate(Path::new(&args[1]), &bytes)?;
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
        };
        // Serialize the real schema with empty rules, as the frozen CLI plan does.
        let mut raw = serde_json::to_value(plan).unwrap();
        for c in raw["candidates"].as_array_mut().unwrap() {
            c["rules"] = json!({});
        }
        let report = evaluate(&path, &serde_json::to_vec(&raw).unwrap()).unwrap();
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
