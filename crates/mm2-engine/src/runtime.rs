use crate::{
    config::{Config, Limits, decimal},
    domain::{Action, BookSnapshot, Core, Decision, FeedEvent, Market, Mode, Outcome, StopReason},
    journal::{Command, Journal, Receipt},
    simulation::{self, Operation as SimulationOperation},
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, watch};

pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().try_into().unwrap_or(u64::MAX))
}
pub fn mono_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}
fn valid_feed_prefix(core: &Core, batch: &[FeedEvent]) -> usize {
    let mut checked = core.clone();
    batch
        .iter()
        .position(|event| checked.apply(event.clone()).is_err())
        .unwrap_or(batch.len())
}
fn valid_feed_prefix_with_recovery(
    core: &Core,
    batch: &[FeedEvent],
    mut native_snapshots: Option<[bool; 2]>,
) -> usize {
    let mut checked = core.clone();
    for (index, event) in batch.iter().enumerate() {
        if native_snapshots.is_some() && matches!(event, FeedEvent::Delta { .. }) {
            continue;
        }
        if let Err(error) = checked.apply(event.clone()) {
            if matches!(event, FeedEvent::Delta { .. })
                && error == "crossed or oversized batch result"
            {
                native_snapshots = Some([false; 2]);
                continue;
            }
            return index;
        }
        match event {
            FeedEvent::Snapshot { outcome, .. } => {
                if let Some(seen) = &mut native_snapshots {
                    seen[usize::from(*outcome == crate::domain::Outcome::Down)] = true;
                    if seen.iter().all(|seen| *seen) {
                        native_snapshots = None;
                    }
                }
            }
            FeedEvent::Disconnect { .. } => native_snapshots = None,
            _ => {}
        }
    }
    batch.len()
}
pub fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Paused => "paused",
        Mode::Shadow => "shadow",
        Mode::Killed => "killed",
    }
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub kind: &'static str,
    pub message: String,
}
impl Failure {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}
type Reply = Result<Value, Failure>;
// One actor turn, not a lossy market-data merge. Commands/faults are checked
// again between bounded turns; all trade inputs retain FIFO order in one sync.
const FEED_BATCH_LIMIT: usize = 16;
enum Operation {
    Command(Command, oneshot::Sender<Reply>),
    Mirror(Command),
    Market(Market, oneshot::Sender<Result<(), String>>),
    ResolutionTargets(oneshot::Sender<Vec<crate::paper_resolution::Target>>),
    PublicResolution(
        crate::paper_resolution::Evidence,
        oneshot::Sender<Result<(), String>>,
    ),
    SourceStatus(&'static str, Option<String>, Option<u64>),
}

#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Operation>,
    pub state: watch::Receiver<Arc<Value>>,
    paper: Option<Box<Handle>>,
    paper_fault: Option<watch::Sender<Option<String>>>,
    pub(crate) feed_diagnostics: Arc<crate::feeds::FeedDiagnostics>,
    fee_policy: watch::Sender<Option<mm2_live::fees::FeePolicy>>,
}
impl Handle {
    pub fn simulation_handle(&self) -> Option<Handle> {
        if self.state.borrow()["execution_mode"] == "simulation" {
            Some(self.clone())
        } else {
            self.paper.as_deref().cloned()
        }
    }
    pub async fn resolution_targets(&self) -> Result<Vec<crate::paper_resolution::Target>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Operation::ResolutionTargets(tx))
            .map_err(|_| "resolution target queue unavailable")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "resolution target request timed out")?
            .map_err(|_| "resolution actor unavailable".into())
    }
    pub async fn resolve_paper(
        &self,
        evidence: crate::paper_resolution::Evidence,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Operation::PublicResolution(evidence, tx))
            .map_err(|_| "paper resolution queue unavailable")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "paper resolution acknowledgement unknown")?
            .map_err(|_| "paper resolution actor unavailable")?
    }
    pub fn fee_policy(&self, policy: Option<mm2_live::fees::FeePolicy>) {
        let policy = policy.filter(|p| p.validate().is_ok() && p.maker_zero());
        self.fee_policy.send_replace(policy.clone());
        if let Some(paper) = &self.paper {
            paper.fee_policy(policy);
        }
    }
    pub fn source_status(
        &self,
        phase: &'static str,
        message: Option<String>,
        retry_in_ms: Option<u64>,
    ) -> Result<(), String> {
        self.tx
            .try_send(Operation::SourceStatus(phase, message, retry_in_ms))
            .map_err(|_| "source status queue full or closed".into())
    }
    pub async fn command(&self, command: Command) -> Reply {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Operation::Command(command, tx))
            .map_err(|_| Failure::new("unavailable", "control queue is full or closed"))?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| {
                Failure::new(
                    "outcome_unknown",
                    "acknowledgement timed out; retry the same command id and payload",
                )
            })?
            .map_err(|_| {
                Failure::new(
                    "outcome_unknown",
                    "actor stopped; reconcile the same command id after restart",
                )
            })?
    }
    pub async fn select_market(&self, market: Market) -> Result<(), String> {
        self.fee_policy(None);
        if let Some(paper) = &self.paper {
            let (reply, _) = oneshot::channel();
            if paper
                .tx
                .try_send(Operation::Market(market.clone(), reply))
                .is_err()
                && let Some(fault) = &self.paper_fault
            {
                fault.send_replace(Some("parallel simulation missed market selection".into()));
            }
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Operation::Market(market, tx))
            .map_err(|_| "market control queue full or closed")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "market selection timed out")?
            .map_err(|_| "market actor stopped")?
    }

    pub fn attach_paper(&mut self, paper: Handle, fault: watch::Sender<Option<String>>) {
        self.paper = Some(Box::new(paper));
        self.paper_fault = Some(fault);
    }
}

pub struct Actor {
    core: Core,
    limits: Limits,
    journal: Option<Journal>,
    version: u64,
    journal_bytes: u64,
    commands: mpsc::Receiver<Operation>,
    feeds: mpsc::Receiver<FeedEvent>,
    fault: watch::Receiver<Option<String>>,
    shutdown: watch::Receiver<bool>,
    state: watch::Sender<Arc<Value>>,
    start: Instant,
    boot_id: String,
    source: &'static str,
    sequence: u64,
    fatal: Option<String>,
    committing: bool,
    closing: bool,
    feed_events: u64,
    feed_diagnostics: Arc<crate::feeds::FeedDiagnostics>,
    decision_count: u64,
    durations_ns: VecDeque<u64>,
    last_decision: Decision,
    condition: Option<String>,
    reset_epoch: u64,
    observation_barrier: Option<u64>,
    source_status: Value,
    fee_policy: watch::Receiver<Option<mm2_live::fees::FeePolicy>>,
    awaiting_source_refresh: bool,
    simulation: Option<simulation::MultiMarketPaper>,
    simulation_origin_ms: u64,
    simulation_last_trade_ms: u64,
    simulation_error: Option<String>,
    last_publication: Instant,
    coalesced_publications: u64,
    publication_duration_ns: u64,
    simulation_books: [(u64, u64); 2],
    simulation_depth: [Vec<mm2_emulator::Level>; 2],
    source_refresh_failures: VecDeque<Instant>,
    native_snapshot_recovery: Option<(Instant, [bool; 2])>,
    execution_noop_ticks: u64,
    execution_periodic_overruns: u64,
    execution_feed_batches: u64,
    execution_trade_commits: u64,
    feed_batch_limit: usize,
    publication_gate: Arc<tokio::sync::Semaphore>,
    publication_task: Option<tokio::task::JoinHandle<()>>,
    simulation_condition: Option<String>,
    execution_mode: String,
    strategy: mm2_strategy::Rules,
    desired_quotes: Vec<mm2_core::DesiredQuote>,
    strategy_gate: Option<mm2_strategy::Gate>,
    reference_guard: crate::paper_guard::ReferenceGuard,
    live: Option<mm2_live::LiveHandle>,
    live_quotes: Vec<mm2_core::DesiredQuote>,
    paper: Option<Handle>,
    paper_error: Option<String>,
    paper_fault: Option<watch::Sender<Option<String>>>,
    #[cfg(test)]
    timer_work_delay: Duration,
}

struct Publication {
    snapshot: crate::domain::CoreSnapshot,
    limits: Limits,
    last_decision: Decision,
    simulation: Option<mm2_emulator::SessionSnapshot>,
    simulation_error: Option<String>,
    boot_id: String,
    sequence: u64,
    source: &'static str,
    version: u64,
    reset_epoch: u64,
    source_status: Value,
    fee_check: Value,
    awaiting_source_refresh: bool,
    awaiting_native_snapshots: bool,
    fatal: Option<String>,
    committing: bool,
    closing: bool,
    feed_events: u64,
    feed_diagnostics: crate::feeds::FeedDiagnosticsSnapshot,
    decision_count: u64,
    durations_ns: VecDeque<u64>,
    now: u64,
    mono: u64,
    journal_bytes: u64,
    coalesced_publications: u64,
    publication_duration_ns: u64,
    execution_noop_ticks: u64,
    execution_periodic_overruns: u64,
    execution_feed_batches: u64,
    execution_trade_commits: u64,
    feed_batch_limit: usize,
    simulation_condition: Option<String>,
    execution_mode: String,
    desired_quotes: Vec<mm2_core::DesiredQuote>,
    strategy_gate: Option<mm2_strategy::Gate>,
    live: Option<Arc<mm2_live::LiveSnapshot>>,
    journal_metrics: Option<crate::journal::JournalMetrics>,
    paper_portfolio: Option<mm2_portfolio::Snapshot>,
    paper_resolution: Option<Value>,
    paper: Option<Arc<Value>>,
    paper_error: Option<String>,
}

pub fn prepare(
    config: &Config,
    start: Instant,
    feeds: mpsc::Receiver<FeedEvent>,
    fault: watch::Receiver<Option<String>>,
    shutdown: watch::Receiver<bool>,
) -> Result<(Handle, Actor), String> {
    config.validate()?;
    let options = config
        .journal
        .clone()
        .unwrap_or(crate::journal::JournalOptions {
            segment_max_bytes: config.journal_max_bytes,
            total_max_bytes: config
                .journal_max_bytes
                .saturating_mul(16)
                .min(1_073_741_824),
            ..Default::default()
        });
    // A trade input is bounded below 512 encoded bytes, including maximum u64
    // digits and its 64-byte fingerprint. Reserve another 512 for the envelope.
    // Small supported segments must still accept individual trade records.
    let feed_batch_limit = if config.simulates() && config.mode != "both" {
        ((options.segment_max_bytes.saturating_sub(512) / 512) as usize).clamp(1, FEED_BATCH_LIMIT)
    } else {
        // Funded/shadow actors do not own this emulator journal. Preserve their
        // one-event control scheduling; the separate paper actor may batch.
        1
    };
    let mut journal =
        Journal::open_streaming(&config.state_dir, options, config.limits.canonical()?)?;
    config.bind_profile()?;
    let mut limits = journal.bootstrap().clone();
    let mut core = Core::new(limits.core()?)?;
    // Only complete durable records are recovered; a response may have been lost after sync.
    // Feed state and proposals are always rebuilt.
    for (command, _) in journal.history() {
        apply_command(&mut core, &mut limits, command)?;
    }
    core.control(Action::Pause)?; // Pause preserves the durable Kill latch.
    let mut simulation = if config.simulates() && config.mode != "both" {
        Some(simulation::new(journal.bootstrap())?)
    } else {
        None
    };
    if simulation.is_none() && journal.metrics().execution_operations != 0 {
        return Err(
            "state directory contains emulator history; use its emulator configuration".into(),
        );
    }
    let mut simulation_origin_ms = 0;
    let mut simulation_condition = None;
    if let Some(session) = &mut simulation {
        journal.visit_execution(|entries| {
            simulation::replay(session, entries)?;
            for entry in entries {
                if entry["kind"] == "market" {
                    simulation_condition = entry["condition_id"].as_str().map(str::to_owned);
                }
            }
            Ok(())
        })?;
        let before_recovery = session.clone();
        let now = session.snapshot().now_ms;
        // Venue state is local and deterministic. Recover in-flight events, cancel all,
        // then settle the simulated deadlines before allowing another explicit Resume.
        let recovery = vec![
            SimulationOperation::CancelAll { now_ms: now },
            SimulationOperation::Advance {
                now_ms: now.saturating_add(10_000),
            },
        ];
        for operation in &recovery {
            simulation::apply(session, operation)?;
        }
        if !simulation::same_state_without_clock(&before_recovery, session)? {
            journal.commit_execution(simulation::values(&recovery)?)?;
        }
        simulation_origin_ms = session.snapshot().now_ms;
    }
    let last_decision = core.evaluate(wall_ms(), mono_ms(start));
    journal.release_replay_buffer();
    let (tx, commands) = mpsc::channel(config.command_capacity);
    let (state, state_rx) = watch::channel(Arc::new(json!({"api_ready":false})));
    let (fee_tx, fee_rx) = watch::channel(None);
    let feed_diagnostics = Arc::new(crate::feeds::FeedDiagnostics::default());
    let mut actor = Actor {
        core,
        limits,
        version: journal.version(),
        journal_bytes: journal.bytes(),
        journal: Some(journal),
        commands,
        feeds,
        fault,
        shutdown,
        state,
        start,
        boot_id: uuid::Uuid::new_v4().to_string(),
        source: match config.feed_source {
            crate::config::FeedSource::Demo => "demo",
            crate::config::FeedSource::Public => "public",
        },
        sequence: 0,
        fatal: None,
        committing: false,
        closing: false,
        feed_events: 0,
        feed_diagnostics: feed_diagnostics.clone(),
        decision_count: 0,
        durations_ns: VecDeque::with_capacity(1024),
        last_decision,
        condition: None,
        reset_epoch: 0,
        observation_barrier: None,
        source_status: json!({"phase":"starting","at_ms":wall_ms()}),
        fee_policy: fee_rx,
        awaiting_source_refresh: false,
        simulation,
        simulation_origin_ms,
        simulation_last_trade_ms: simulation_origin_ms,
        simulation_error: None,
        last_publication: Instant::now(),
        coalesced_publications: 0,
        publication_duration_ns: 0,
        simulation_books: [(0, 0); 2],
        simulation_depth: [Vec::new(), Vec::new()],
        source_refresh_failures: VecDeque::new(),
        native_snapshot_recovery: None,
        execution_noop_ticks: 0,
        execution_periodic_overruns: 0,
        execution_feed_batches: 0,
        execution_trade_commits: 0,
        feed_batch_limit,
        publication_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        publication_task: None,
        simulation_condition,
        execution_mode: config.mode.clone(),
        strategy: config.strategy.clone(),
        desired_quotes: Vec::new(),
        strategy_gate: None,
        reference_guard: Default::default(),
        live: None,
        live_quotes: Vec::new(),
        paper: None,
        paper_error: None,
        paper_fault: None,
        #[cfg(test)]
        timer_work_delay: Duration::ZERO,
    };
    actor.publish();
    Ok((
        Handle {
            tx,
            state: state_rx,
            paper: None,
            paper_fault: None,
            feed_diagnostics,
            fee_policy: fee_tx,
        },
        actor,
    ))
}

fn apply_command(core: &mut Core, limits: &mut Limits, command: &Command) -> Result<(), String> {
    match command {
        Command::Control { action, .. } => core.control(match action.as_str() {
            "pause" => Action::Pause,
            "resume" => Action::Resume,
            "kill" => Action::Kill,
            "reset" => Action::Reset,
            _ => return Err("unsupported control action".into()),
        }),
        Command::Limits { limits: next, .. } => {
            core.set_config(next.core()?)?;
            *limits = next.clone();
            Ok(())
        }
    }
}

impl Actor {
    fn fee_ready(&self, now: u64) -> bool {
        self.source != "public"
            || self.core.snapshot().market.as_ref().is_some_and(|market| {
                self.fee_policy.borrow().as_ref().is_some_and(|policy| {
                    policy.permits(
                        &market.condition_id,
                        &market.up_token,
                        &market.down_token,
                        now,
                    )
                })
            })
    }
    fn fee_check(&self, now: u64) -> Value {
        let ready = self.fee_ready(now);
        let mut result = self.fee_policy.borrow().as_ref().map_or_else(||json!({
            "policy":null,"net_pnl":null,"roi":null,"actual_live_fee":null,
            "gas_usd":null,"rebates":null,"infrastructure_usd":null,"open_position_value":null
        }),|p|p.status(now));
        result["ready"] = json!(ready);
        result["status"] = json!(if self.source != "public" {
            "SYNTHETIC_DEMO"
        } else if ready {
            "VERIFIED_MAKER_ZERO"
        } else {
            "BLOCKED_MISSING_STALE_OR_UNSUPPORTED"
        });
        result
    }
    fn mirror_paper(&mut self, command: Command) {
        if let Some(paper) = &self.paper
            && paper.tx.try_send(Operation::Mirror(command)).is_err()
        {
            self.paper_error = Some(
                "parallel simulation control queue unavailable; inspect its independent state"
                    .into(),
            );
            if let Some(fault) = &self.paper_fault {
                fault.send_replace(self.paper_error.clone());
            }
        }
    }
    pub fn attach_paper(&mut self, paper: Handle, fault: watch::Sender<Option<String>>) {
        self.paper = Some(paper);
        self.paper_fault = Some(fault);
        self.publish();
    }
    pub fn attach_live(&mut self, live: mm2_live::LiveHandle) {
        self.live = Some(live);
        self.publish();
    }

    fn evaluate(&mut self) {
        self.evaluate_with_paper(None);
    }
    fn evaluate_with_paper(&mut self, advanced: Option<&simulation::MultiMarketPaper>) {
        let began = Instant::now();
        let now = wall_ms();
        let mono = mono_ms(self.start);
        let paper = advanced.or(self.simulation.as_ref());
        self.last_decision = self.core.evaluate(now, mono);
        self.desired_quotes.clear();
        self.live_quotes.clear();
        self.strategy_gate = None;
        self.last_decision.stop_reasons.retain(|reason| {
            !matches!(
                reason,
                StopReason::NoLegalQuote(_) | StopReason::BudgetExceeded
            )
        });
        let paper_halted = paper.is_some_and(|paper| paper.portfolio_halt().is_some());
        if paper_halted {
            self.strategy_gate = Some(mm2_strategy::Gate::InsufficientCapital);
        }
        if self.last_decision.stop_reasons.is_empty() && !paper_halted {
            let snapshot = self.core.snapshot();
            if let Some(reference) = &snapshot.reference {
                self.reference_guard.observe(
                    reference.exchange_ms,
                    reference.observed_mono_ms,
                    reference.price_micro,
                );
            }
            if let Some(market) = &snapshot.market {
                let account = paper.map(|s| s.snapshot().account);
                let mut rules = self.strategy.clone();
                rules.half_width_micros = snapshot.config.half_width_micro;
                rules.lot_full_micros = snapshot.config.lot_micro;
                rules.max_imbalance_micros = snapshot.config.max_imbalance_micro;
                rules.max_feed_age_ms = snapshot.config.max_feed_age_ms;
                let bbo = |book: &BookSnapshot| mm2_strategy::Bbo {
                    bid_micros: book.best_bid_micro,
                    ask_micros: book.best_ask_micro,
                    received_ms: now
                        .saturating_sub(mono.saturating_sub(book.observed_mono_ms.unwrap_or(0))),
                };
                let input = mm2_strategy::Input {
                    now_ms: now,
                    market_start_ms: market.window_start_ms,
                    market_end_ms: market.window_end_ms,
                    up: bbo(&snapshot.up_book),
                    down: bbo(&snapshot.down_book),
                    inventory_up_micros: account.as_ref().map_or(0, |a| a.up_inventory_micros),
                    inventory_down_micros: account.as_ref().map_or(0, |a| a.down_inventory_micros),
                    // These are desired replacements. The execution ledger separately
                    // retains all old possible fills until terminal reconciliation.
                    reserved_up_micros: 0,
                    reserved_down_micros: 0,
                    available_cash_micros: account
                        .as_ref()
                        .map_or(snapshot.config.initial_collateral_micro, |a| {
                            a.initial_cash_micros.saturating_sub(a.spent_micros_ceil)
                        }),
                    tick_micros: common_tick(
                        snapshot.up_book.tick_micro,
                        snapshot.down_book.tick_micro,
                    ),
                    min_order_micros: 5_000_000,
                    signals: mm2_strategy::Signals::default(),
                };
                let decision = if self.execution_mode == "simulation"
                    && let Some(account) = &account
                {
                    let costs = account
                        .outcome_spent_pico
                        .as_ref()
                        .and_then(|costs| Some([costs[0].parse().ok()?, costs[1].parse().ok()?]));
                    if self.source == "public" {
                        mm2_strategy::decide_inventory_repair(&rules, &input, costs)
                    } else {
                        mm2_strategy::decide_inventory_aware(&rules, &input, costs)
                    }
                } else {
                    mm2_strategy::decide(&rules, &input)
                };
                let decision = decision.and_then(|mut decision| {
                    if self.execution_mode == "simulation"
                        && self.source == "public"
                        && input.inventory_up_micros == input.inventory_down_micros
                        && decision.reason == mm2_strategy::Gate::Quote
                    {
                        match if snapshot.binance_connected {
                            self.reference_guard.blocked(mono, now)
                        } else {
                            None
                        } {
                            None => {
                                decision.quotes.clear();
                                decision.reason = mm2_strategy::Gate::MissingSignal;
                            }
                            Some(Some(outcome)) => {
                                decision.quotes.retain(|q| q.outcome != outcome);
                                if decision.quotes.is_empty() {
                                    decision.reason = mm2_strategy::Gate::DirectionalGuard;
                                }
                            }
                            Some(None) => {}
                        }
                    }
                    if self.execution_mode == "simulation"
                        && let Some(paper) = paper
                    {
                        if self.source == "public" {
                            paper.guard_public_strategy_quotes(&rules, &mut decision)?;
                        } else {
                            paper.guard_strategy_quotes(&rules, &mut decision)?;
                        }
                    }
                    Ok(decision)
                });
                match decision {
                    Ok(decision) => {
                        self.strategy_gate = Some(decision.reason);
                        self.desired_quotes = decision.quotes;
                    }
                    Err(_) => self
                        .last_decision
                        .stop_reasons
                        .push(StopReason::InvalidInput),
                }
                if let Some(live) = &self.live {
                    let account = live.snapshot();
                    let inventory = |outcome| {
                        account
                            .positions
                            .iter()
                            .filter(|p| {
                                p.condition_id == market.condition_id && p.outcome == outcome
                            })
                            .map(|p| p.quantity_micros)
                            .sum()
                    };
                    let input = mm2_strategy::Input {
                        inventory_up_micros: inventory(mm2_core::Outcome::Up),
                        inventory_down_micros: inventory(mm2_core::Outcome::Down),
                        // Existing reservations are enforced by the live gateway.
                        available_cash_micros: account.collateral_micros,
                        ..input
                    };
                    if let Ok(decision) = mm2_strategy::decide(&rules, &input) {
                        self.strategy_gate = Some(decision.reason);
                        self.live_quotes = decision.quotes;
                    }
                    self.desired_quotes = self.live_quotes.clone();
                }
            }
        }
        if !self.fee_ready(now) {
            self.desired_quotes.clear();
            self.live_quotes.clear();
        }
        self.last_decision.intents = self
            .desired_quotes
            .iter()
            .map(|q| crate::domain::Intent {
                outcome: if q.outcome == mm2_core::Outcome::Up {
                    Outcome::Up
                } else {
                    Outcome::Down
                },
                price_micro: q.price_micros,
                quantity_micro: q.quantity_micros,
            })
            .collect();
        self.last_decision.hypothetical_reservation_micro = self
            .desired_quotes
            .iter()
            .map(|q| {
                (u128::from(q.price_micros) * u128::from(q.quantity_micros)).div_ceil(1_000_000)
                    as u64
            })
            .sum();
        self.decision_count = self.decision_count.saturating_add(1);
        if self.durations_ns.len() == 1024 {
            self.durations_ns.pop_front();
        }
        self.durations_ns
            .push_back(began.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
    }
    fn latch(&mut self, reason: String) {
        if self.fatal.is_none() {
            tracing::error!(
                code = runtime_failure_class(&reason),
                "engine fault latched; owner effects paused"
            );
            self.fatal = Some(reason.chars().take(256).collect());
        }
        let _ = self.core.control(Action::Pause);
        self.evaluate();
        self.publish();
    }

    async fn live_control(&self, command: &Command) -> Result<(), String> {
        let Some(live) = &self.live else {
            return Ok(());
        };
        let action = match command {
            Command::Control { action, .. } => match action.as_str() {
                "pause" => mm2_live::ControlAction::Pause,
                "resume" => mm2_live::ControlAction::Resume,
                "kill" => mm2_live::ControlAction::Kill,
                "reset" => mm2_live::ControlAction::Reset,
                _ => return Err("invalid live action".into()),
            },
            // Limit changes cancel quotes and require a fresh explicit Resume.
            Command::Limits { .. } => mm2_live::ControlAction::Pause,
        };
        live.command(command.id().to_owned(), action)
            .await
            .map(|_| ())
    }

    fn drive_live(&mut self) {
        let Some(live) = self.live.clone() else {
            return;
        };
        let phase = live.snapshot().phase;
        if self.core.snapshot().mode == Mode::Shadow && phase != mm2_live::LivePhase::Running {
            let _ = self.core.control(Action::Pause);
            self.evaluate();
        }
        let snapshot = self.core.snapshot();
        let active = snapshot.mode == Mode::Shadow
            && live.snapshot().phase == mm2_live::LivePhase::Running
            && self.fatal.is_none()
            && !self.closing
            && !self.awaiting_source_refresh
            && self.native_snapshot_recovery.is_none();
        let quotes = if active {
            snapshot.market.as_ref().map_or_else(Vec::new, |market| {
                self.live_quotes
                    .iter()
                    .map(|quote| mm2_live::LiveQuote {
                        slot: quote.slot.clone(),
                        condition_id: market.condition_id.clone(),
                        market_slug: market.slug.clone(),
                        window_start_ms: market.window_start_ms,
                        token_id: if quote.outcome == mm2_core::Outcome::Up {
                            &market.up_token
                        } else {
                            &market.down_token
                        }
                        .clone(),
                        outcome: quote.outcome,
                        price_micros: quote.price_micros,
                        quantity_micros: quote.quantity_micros / 10_000 * 10_000,
                        window_end_ms: market.window_end_ms,
                        tick_micros: if quote.outcome == mm2_core::Outcome::Up {
                            snapshot.up_book.tick_micro
                        } else {
                            snapshot.down_book.tick_micro
                        },
                        min_order_micros: 5_000_000,
                        fee_bps: 10_000,
                        protocol: 2,
                        neg_risk: false,
                    })
                    .collect()
            })
        } else {
            Vec::new()
        };
        if let Err(reason) = live.try_submit_quotes(quotes, wall_ms()) {
            self.latch(reason);
        }
    }
    fn publication(&mut self) -> Publication {
        let began = Instant::now();
        self.last_publication = began;
        self.sequence += 1;
        let publication = Publication {
            snapshot: self.core.snapshot(),
            limits: self.limits.clone(),
            last_decision: self.last_decision.clone(),
            simulation: self.simulation.as_ref().map(|session| session.snapshot()),
            simulation_error: self.simulation_error.clone(),
            boot_id: self.boot_id.clone(),
            sequence: self.sequence,
            source: self.source,
            version: self.version,
            reset_epoch: self.reset_epoch,
            source_status: self.source_status.clone(),
            fee_check: self.fee_check(wall_ms()),
            awaiting_source_refresh: self.awaiting_source_refresh,
            awaiting_native_snapshots: self.native_snapshot_recovery.is_some(),
            fatal: self.fatal.clone(),
            committing: self.committing,
            closing: self.closing,
            feed_events: self.feed_events,
            feed_diagnostics: self.feed_diagnostics.snapshot(),
            decision_count: self.decision_count,
            durations_ns: self.durations_ns.clone(),
            now: wall_ms(),
            mono: mono_ms(self.start),
            journal_bytes: self.journal_bytes,
            coalesced_publications: self.coalesced_publications,
            publication_duration_ns: self.publication_duration_ns,
            execution_noop_ticks: self.execution_noop_ticks,
            execution_periodic_overruns: self.execution_periodic_overruns,
            execution_feed_batches: self.execution_feed_batches,
            execution_trade_commits: self.execution_trade_commits,
            feed_batch_limit: self.feed_batch_limit,
            simulation_condition: self.simulation_condition.clone(),
            execution_mode: self.execution_mode.clone(),
            desired_quotes: self.desired_quotes.clone(),
            strategy_gate: self.strategy_gate,
            live: self.live.as_ref().map(|handle| handle.snapshot()),
            journal_metrics: self.journal.as_ref().map(|journal| journal.metrics()),
            paper_portfolio: self
                .simulation
                .as_ref()
                .map(|session| session.telemetry_portfolio_snapshot()),
            paper_resolution: self
                .simulation
                .as_ref()
                .map(|paper| paper.resolution_status()),
            paper: self
                .paper
                .as_ref()
                .map(|paper| paper.state.borrow().clone()),
            paper_error: self.paper_error.clone(),
        };
        self.publication_duration_ns = began.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);
        publication
    }
    fn publish(&mut self) {
        // Commands/faults need an immediate current receipt. Periodic telemetry uses
        // the bounded worker below and cannot retain an actor slot while rendering.
        let publication = self.publication();
        self.state.send_replace(publication.render());
    }
    fn publish_if_due(&mut self) {
        if self.last_publication.elapsed() < Duration::from_millis(250) {
            self.coalesced_publications = self.coalesced_publications.saturating_add(1);
            return;
        }
        let Ok(permit) = self.publication_gate.clone().try_acquire_owned() else {
            self.coalesced_publications = self.coalesced_publications.saturating_add(1);
            return;
        };
        let publication = self.publication();
        let sequence = publication.sequence;
        let state = self.state.clone();
        self.publication_task = Some(tokio::task::spawn_blocking(move || {
            let rendered = publication.render();
            state.send_if_modified(|current| {
                if current["sequence"].as_u64().unwrap_or(0) >= sequence {
                    return false;
                }
                *current = rendered.clone();
                true
            });
            drop(permit);
        }));
    }
    async fn simulation_step(&mut self, settling: bool) {
        let Some(session) = self.simulation.clone() else {
            return;
        };
        let mut active =
            self.core.snapshot().mode == Mode::Shadow && self.fatal.is_none() && !self.closing;
        if let Some(reason) = session.portfolio_halt() {
            let _ = self.core.control(Action::Pause);
            self.simulation_error = Some(format!("paper portfolio halted: {reason}"));
            self.evaluate();
            active = false;
        }
        let snapshot = session.snapshot();
        if !active
            && snapshot.pending_events == 0
            && snapshot.account.reserved_micros_ceil == 0
            && !settling
        {
            return;
        }
        let mut candidate = session.clone();
        let now = self
            .simulation_origin_ms
            .saturating_add(mono_ms(self.start))
            .max(session.snapshot().now_ms);
        let mut operations = Vec::new();
        operations.push(SimulationOperation::Advance { now_ms: now });
        if !active || settling {
            operations.push(SimulationOperation::CancelAll { now_ms: now });
        }
        for operation in &operations {
            if let Err(error) = simulation::apply(&mut candidate, operation) {
                self.latch(format!("simulation invariant: {error}"));
                return;
            }
        }
        // Execution time may run ahead while draining cancels at rollover. Risk
        // days use separately recorded UTC, never that synthetic future clock.
        let risk_at = wall_ms().max(candidate.risk_clock_ms());
        if active && !settling && candidate.risk_window_due(risk_at) {
            let operation = SimulationOperation::RiskWindow { at_ms: risk_at };
            if let Err(error) = simulation::apply(&mut candidate, &operation) {
                self.latch(format!("simulation risk window invariant: {error}"));
                return;
            }
            operations.push(operation);
        }
        if active && !settling {
            let books = self.core.snapshot();
            let tick = common_tick(books.up_book.tick_micro, books.down_book.tick_micro);
            if tick != candidate.limits().tick_micros
                || candidate.limits().min_order_micros != 5_000_000
            {
                let terminal = candidate.snapshot();
                let operation = if tick > 0
                    && terminal.pending_events == 0
                    && terminal.account.reserved_micros_ceil == 0
                {
                    SimulationOperation::VenueRules {
                        now_ms: now,
                        tick_micros: tick,
                        min_order_micros: 5_000_000,
                    }
                } else {
                    // Changing a grid never forgets possible fills of old orders.
                    active = false;
                    SimulationOperation::CancelAll { now_ms: now }
                };
                if let Err(error) = simulation::apply(&mut candidate, &operation) {
                    self.latch(format!("simulation venue rules invariant: {error}"));
                    return;
                }
                operations.push(operation);
            }
        }
        if active && !settling && self.source == "public" {
            let policy = self.fee_policy.borrow().clone();
            if let Some(policy) = policy.filter(|_| self.fee_ready(wall_ms())) {
                if candidate.fee_policy.as_ref() != Some(&policy) {
                    let operation = SimulationOperation::FeeRules { policy };
                    if let Err(error) = simulation::apply(&mut candidate, &operation) {
                        self.latch(format!("simulation fee evidence invariant: {error}"));
                        return;
                    }
                    operations.push(operation);
                }
            } else {
                // Unknown rules cancel possible fills before any new post.
                let operation = SimulationOperation::CancelAll { now_ms: now };
                if let Err(error) = simulation::apply(&mut candidate, &operation) {
                    self.latch(format!("simulation fee cancel invariant: {error}"));
                    return;
                }
                operations.push(operation);
                active = false;
            }
        }
        if active && !settling {
            // Advance has confirmed fills/cancels and changed inventory, costs
            // and possible fills. Only this candidate may authorize the quote.
            self.evaluate_with_paper(Some(&candidate));
            let current = candidate.snapshot();
            let minimum_cost = self
                .last_decision
                .intents
                .iter()
                .map(|intent| u128::from(intent.price_micro) * 5_000_000u128 / 1_000_000u128)
                .min();
            if current.pending_events == 0
                && current.orders.is_empty()
                && minimum_cost
                    .is_some_and(|cost| cost > u128::from(current.account.available_micros_floor))
            {
                let _ = self.core.control(Action::Pause);
                self.evaluate_with_paper(Some(&candidate));
                self.simulation_error =
                    Some("finite simulated collateral exhausted; session is paused".into());
                active = false;
            }
        }
        if active && !settling {
            let snapshot = self.core.snapshot();
            let current = candidate.snapshot();
            let new_quote = self.desired_quotes.iter().any(|quote| {
                !current.orders.iter().any(|order| {
                    order.quote.slot == quote.slot
                        && order.quote.outcome == quote.outcome
                        && order.quote.price_micros == quote.price_micros
                        && order.requested_quantity_micros == quote.quantity_micros
                })
            });
            let needs_depth = new_quote
                || current
                    .orders
                    .iter()
                    .any(|order| order.phase == mm2_core::OrderPhase::Posting);
            for (index, (name, outcome, book)) in [
                ("up", mm2_core::Outcome::Up, &snapshot.up_book),
                ("down", mm2_core::Outcome::Down, &snapshot.down_book),
            ]
            .into_iter()
            .enumerate()
            {
                if let (Some(bid), Some(ask)) = (book.best_bid_micro, book.best_ask_micro)
                    && candidate.exact_execution_costs()
                    && needs_depth
                {
                    let prices: Vec<_> = std::iter::once(bid)
                        .chain(
                            self.desired_quotes
                                .iter()
                                .filter(|q| q.outcome == outcome)
                                .map(|q| q.price_micros),
                        )
                        .chain(
                            current
                                .orders
                                .iter()
                                .filter(|o| o.quote.outcome == outcome)
                                .map(|o| o.quote.price_micros),
                        )
                        .collect();
                    let depth: Vec<_> = self
                        .core
                        .bid_depth_at(
                            if index == 0 {
                                crate::domain::Outcome::Up
                            } else {
                                crate::domain::Outcome::Down
                            },
                            &prices,
                        )
                        .into_iter()
                        .map(|level| mm2_emulator::Level {
                            price_micros: level.price_micro,
                            quantity_micros: level.quantity_micro,
                        })
                        .collect();
                    if new_quote
                        || self.simulation_books[index] != (bid, ask)
                        || self.simulation_depth[index] != depth
                    {
                        let operation = SimulationOperation::DepthBook {
                            now_ms: now,
                            outcome: name.into(),
                            bid,
                            ask,
                            bid_levels: depth,
                        };
                        if let Err(error) = simulation::apply(&mut candidate, &operation) {
                            self.latch(format!("simulation depth invariant: {error}"));
                            return;
                        }
                        operations.push(operation);
                    }
                }
            }
            let quote = SimulationOperation::Quotes {
                now_ms: now,
                quotes: simulation::from_core_quotes(&self.desired_quotes),
            };
            if new_quote
                && self.source == "public"
                && current.account.up_inventory_micros == current.account.down_inventory_micros
                && !self.desired_quotes.is_empty()
            {
                let Some((
                    anchor_exchange_ms,
                    anchor_price_micro,
                    latest_exchange_ms,
                    latest_price_micro,
                )) = self.reference_guard.evidence()
                else {
                    self.latch("missing public paper entry signal witness".into());
                    return;
                };
                let evidence = SimulationOperation::PaperEntrySignal {
                    anchor_exchange_ms,
                    anchor_price_micro,
                    latest_exchange_ms,
                    latest_price_micro,
                };
                if let Err(error) = simulation::apply(&mut candidate, &evidence) {
                    self.latch(error);
                    return;
                }
                operations.push(evidence);
            }
            let mut quoted = candidate.clone();
            match simulation::apply(&mut quoted, &quote) {
                Ok(()) => {
                    candidate = quoted;
                    operations.push(quote);
                    self.simulation_error = None;
                }
                Err(error) => {
                    self.simulation_error = Some(error.chars().take(256).collect());
                }
            }
            if self.source == "demo" && now.saturating_sub(self.simulation_last_trade_ms) >= 500 {
                let outcome = if (now / 500).is_multiple_of(2) {
                    "up"
                } else {
                    "down"
                };
                let trade = SimulationOperation::Trade {
                    now_ms: now,
                    outcome: outcome.into(),
                    worst_price_micros: 1,
                    quantity_micros: 102_000_000,
                };
                if let Err(error) = simulation::apply(&mut candidate, &trade) {
                    self.latch(format!("synthetic trade invariant: {error}"));
                    return;
                }
                operations.push(trade);
                self.simulation_last_trade_ms = now;
            }
        }
        if settling {
            let advance = SimulationOperation::Advance {
                now_ms: now.saturating_add(10_000),
            };
            if let Err(error) = simulation::apply(&mut candidate, &advance) {
                self.latch(format!("simulation shutdown invariant: {error}"));
                return;
            }
            operations.push(advance);
        }
        match simulation::same_state_without_clock(&session, &candidate) {
            Ok(true) => {
                self.execution_noop_ticks = self.execution_noop_ticks.saturating_add(1);
                self.simulation = Some(candidate);
                return;
            }
            Ok(false) => {}
            Err(error) => {
                self.latch(error);
                return;
            }
        }
        let values = match simulation::values(&operations) {
            Ok(values) => values,
            Err(error) => {
                self.latch(error);
                return;
            }
        };
        let Some(mut journal) = self.journal.take() else {
            self.latch("execution journal unavailable".into());
            return;
        };
        let result = tokio::task::spawn_blocking(move || {
            let result = journal.commit_execution(values);
            (journal, result)
        })
        .await;
        match result {
            Ok((journal, result)) => {
                self.journal_bytes = journal.bytes();
                self.journal = Some(journal);
                if let Err(error) = result {
                    self.latch(error);
                    return;
                }
                if operations
                    .iter()
                    .any(|operation| matches!(operation, SimulationOperation::Trade { .. }))
                {
                    self.simulation_last_trade_ms = now;
                }
                self.simulation = Some(candidate);
                for operation in &operations {
                    if let SimulationOperation::Book {
                        outcome, bid, ask, ..
                    } = operation
                    {
                        self.simulation_books[usize::from(outcome == "down")] = (*bid, *ask);
                    }
                    if let SimulationOperation::DepthBook {
                        outcome,
                        bid,
                        ask,
                        bid_levels,
                        ..
                    } = operation
                    {
                        let index = usize::from(outcome == "down");
                        self.simulation_books[index] = (*bid, *ask);
                        self.simulation_depth[index] = bid_levels.clone();
                    }
                }
            }
            Err(_) => {
                self.latch("execution journal writer stopped; reconcile after restart".into())
            }
        }
    }
    async fn bind_simulation_market(&mut self, condition: &str) -> Result<(), String> {
        let Some(session) = &self.simulation else {
            return Ok(());
        };
        if self.simulation_condition.as_deref() == Some(condition) {
            return Ok(());
        }
        let mut candidate = session.clone();
        let now = candidate.now_ms();
        let market = self.core.snapshot().market;
        let mut operations = Vec::new();
        if session.can_enable_exact_costs() {
            operations.push(SimulationOperation::ExactAccounting {});
        }
        if self.simulation_condition.is_some() {
            operations.push(SimulationOperation::CancelAll { now_ms: now });
            operations.push(SimulationOperation::Advance {
                now_ms: now.saturating_add(10_000),
            });
        }
        operations.push(SimulationOperation::Market {
            condition_id: condition.into(),
            start_ms: market.as_ref().map(|market| market.window_start_ms),
            end_ms: market.as_ref().map(|market| market.window_end_ms),
        });
        if self.source == "demo"
            && let Some(previous) = &self.simulation_condition
        {
            let previous_market = session
                .portfolio_snapshot()
                .positions
                .into_iter()
                .find(|position| position.market.id == *previous)
                .ok_or("missing demo settlement market")?
                .market;
            operations.push(SimulationOperation::DemoResolution {
                condition_id: previous.clone(),
                at_ms: previous_market
                    .end_ms
                    .max(wall_ms())
                    .max(candidate.risk_clock_ms()),
                winning_outcome: if (previous_market.start_ms / 300_000).is_multiple_of(2) {
                    "up"
                } else {
                    "down"
                }
                .into(),
            });
        }
        for operation in &operations {
            simulation::apply(&mut candidate, operation)?;
        }
        let operations = simulation::values(&operations)?;
        let Some(mut journal) = self.journal.take() else {
            return Err("execution journal unavailable".into());
        };
        let result = tokio::task::spawn_blocking(move || {
            let result = journal.commit_execution(operations);
            (journal, result)
        })
        .await;
        match result {
            Ok((journal, result)) => {
                self.journal_bytes = journal.bytes();
                self.journal = Some(journal);
                result?;
                self.simulation_condition = Some(condition.into());
                self.simulation_origin_ms = candidate.now_ms().saturating_sub(mono_ms(self.start));
                self.simulation_last_trade_ms = candidate.now_ms();
                self.simulation = Some(candidate);
                self.simulation_books = [(0, 0); 2];
                self.simulation_depth = [Vec::new(), Vec::new()];
                Ok(())
            }
            Err(_) => Err("market identity journal writer stopped".into()),
        }
    }
    async fn admit_public_resolution(
        &mut self,
        evidence: crate::paper_resolution::Evidence,
    ) -> Result<(), String> {
        if self.source != "public" || self.execution_mode != "simulation" {
            return Err("public outcome admission requires the independent simulation lane".into());
        }
        let Some(session) = &self.simulation else {
            return Err("no paper ledger".into());
        };
        let mut candidate = session.clone();
        let operation = SimulationOperation::PublicResolution {
            at_ms: wall_ms()
                .max(evidence.observed_at_ms)
                .max(candidate.risk_clock_ms()),
            evidence,
        };
        simulation::apply(&mut candidate, &operation)?;
        candidate.validate()?;
        if simulation::same_state_without_clock(session, &candidate)? {
            return Ok(());
        }
        let values = simulation::values(&[operation])?;
        let Some(mut journal) = self.journal.take() else {
            return Err("paper journal unavailable".into());
        };
        match tokio::task::spawn_blocking(move || {
            let result = journal.commit_execution(values);
            (journal, result)
        })
        .await
        {
            Ok((journal, result)) => {
                self.journal_bytes = journal.bytes();
                self.journal = Some(journal);
                if let Err(error) = result {
                    self.latch(error.clone());
                    return Err(error);
                }
                self.simulation = Some(candidate);
                self.evaluate();
                // A newly realized loss must cancel existing possible fills immediately.
                self.simulation_step(false).await;
                self.publish();
                Ok(())
            }
            Err(_) => {
                self.latch("paper resolution journal writer stopped".into());
                Err("paper resolution journal writer stopped".into())
            }
        }
    }
    async fn select_market(&mut self, market: Market) -> Result<(), String> {
        // A failed paper rollover is an inspectable paused actor, not a dead source/API.
        if self.fatal.is_some() && self.simulation.is_some() {
            return Ok(());
        }
        let mut candidate = self.core.clone();
        candidate.set_market(market.clone())?;
        let previous = std::mem::replace(&mut self.core, candidate);
        if let Err(error) = self.bind_simulation_market(&market.condition_id).await {
            self.core = previous;
            self.simulation_error = Some(error.clone());
            self.latch(error.clone());
            self.simulation_step(true).await;
            self.publish();
            return if self.simulation.is_some() {
                Ok(())
            } else {
                Err(error)
            };
        }
        self.condition = Some(market.condition_id);
        self.observation_barrier = Some(mono_ms(self.start));
        self.awaiting_source_refresh = false;
        self.native_snapshot_recovery = None;
        self.evaluate();
        self.publish();
        Ok(())
    }
    fn apply_feed_event(&mut self, event: FeedEvent) -> bool {
        self.feed_events = self.feed_events.saturating_add(1);
        let delta = matches!(&event, FeedEvent::Delta { .. });
        if delta && self.native_snapshot_recovery.is_some() {
            // A split price update must not initialize a cleared book. Wait for
            // independently validated full native snapshots; trades still flow.
            return true;
        }
        let snapshot_outcome = match &event {
            FeedEvent::Snapshot { outcome, .. } => {
                Some(usize::from(*outcome == crate::domain::Outcome::Down))
            }
            FeedEvent::Disconnect { .. } => {
                self.native_snapshot_recovery = None;
                None
            }
            _ => None,
        };
        let clock_context = self.core.feed_clock_context(&event);
        if let Err(error) = self.core.apply(event)
            && self.source == "public"
        {
            if error == "CLOB receive clock regressed" || error == "CLOB exchange clock regressed" {
                tracing::warn!(
                    validation_detail = crate::feeds::input_failure_detail(&error),
                    clock_context = ?clock_context,
                    "public clock validation failed"
                );
            }
            if delta && error == "crossed or oversized batch result" {
                // Public updates can add a new best price before removing the
                // opposite old price in a separate frame. Never trade that
                // intermediate book, or infer an atomic cross-frame ordering.
                self.native_snapshot_recovery = Some((Instant::now(), [false; 2]));
                // This is the same FIFO source generation. A processing-time
                // barrier would discard already queued recovery snapshots and
                // trade witnesses, although they arrived after the split delta.
                self.simulation_books = [(0, 0); 2];
                self.simulation_depth = [Vec::new(), Vec::new()];
                tracing::warn!(
                    validation_detail = "crossed_or_oversized_delta",
                    "public book waiting for native snapshots"
                );
                self.source_status = json!({"phase":"resynchronizing","message":"waiting for validated full native books","validation_detail":"crossed_or_oversized_delta","at_ms":wall_ms()});
                self.evaluate();
                self.drive_live();
                self.publish();
                // Keep the rest of this bounded batch: it may already contain
                // both native snapshots. Admission remains blocked until then.
                return true;
            }
            self.refresh_after_validation_failure(&error);
            return false;
        }
        if let (Some(outcome), Some((_, seen))) =
            (snapshot_outcome, &mut self.native_snapshot_recovery)
        {
            seen[outcome] = true;
            if seen.iter().all(|seen| *seen) {
                self.native_snapshot_recovery = None;
                tracing::info!(
                    "native book snapshots restored; freshness admission still required"
                );
                self.source_status = json!({"phase":"streaming","message":"validated native books restored; freshness admission still required","at_ms":wall_ms()});
            }
        }
        self.evaluate();
        self.drive_live();
        true
    }
    fn expire_native_snapshot_recovery(&mut self) {
        let maximum = Duration::from_millis(self.core.snapshot().config.max_feed_age_ms);
        if self
            .native_snapshot_recovery
            .is_some_and(|(started, _)| started.elapsed() >= maximum)
        {
            self.native_snapshot_recovery = None;
            self.refresh_after_validation_failure("native snapshot recovery deadline exceeded");
        }
    }
    fn refresh_after_validation_failure(&mut self, error: &str) {
        self.native_snapshot_recovery = None;
        // Join the old source generation before accepting a new snapshot. This
        // is an internal refresh, not an owner Reset/Resume or a risk reset.
        let _ = self.core.apply(FeedEvent::Disconnect {
            source: crate::domain::FeedSource::Clob,
            reason: "book validation requires source refresh".into(),
        });
        let now = Instant::now();
        while self
            .source_refresh_failures
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(60))
        {
            self.source_refresh_failures.pop_front();
        }
        self.source_refresh_failures.push_back(now);
        self.awaiting_source_refresh = true;
        self.observation_barrier = Some(mono_ms(self.start));
        self.simulation_books = [(0, 0); 2];
        self.simulation_depth = [Vec::new(), Vec::new()];
        let class = crate::feeds::input_failure_class(error);
        let detail = crate::feeds::input_failure_detail(error);
        tracing::warn!(
            validation_class = class,
            validation_detail = detail,
            "public input validation requested source refresh"
        );
        self.source_status = json!({"phase":"refreshing","message":"book validation requires fresh source snapshots","validation_class":class,"validation_detail":detail,"at_ms":wall_ms()});
        if self.source_refresh_failures.len() >= 3 {
            self.latch("repeated public input validation failure; inspection required".into());
        }
        self.evaluate();
        self.drive_live();
        self.publish();
    }
    async fn observed_trades(&mut self, events: &[FeedEvent]) {
        let Some(session) = &self.simulation else {
            return;
        };
        let wall = wall_ms();
        let mono = mono_ms(self.start);
        let mut candidate = None;
        let mut operations = Vec::new();
        for event in events {
            let FeedEvent::Trade {
                outcome,
                price_micro,
                quantity_micro,
                fingerprint,
                exchange_ms,
                observed_mono_ms,
                ..
            } = event
            else {
                continue;
            };
            if *exchange_ms > wall
                || wall - *exchange_ms > self.limits.max_feed_age_ms
                || *observed_mono_ms > mono
                || mono - *observed_mono_ms > self.limits.max_feed_age_ms
            {
                continue;
            }
            let candidate = candidate.get_or_insert_with(|| session.clone());
            let operation = if candidate.exact_execution_costs() {
                SimulationOperation::ObservedTradeV2 {
                    now_ms: self
                        .simulation_origin_ms
                        .saturating_add(mono)
                        .max(candidate.now_ms()),
                    outcome: outcome_name(*outcome).into(),
                    price_micros: *price_micro,
                    quantity_micros: *quantity_micro,
                    fingerprint: fingerprint.clone(),
                }
            } else {
                SimulationOperation::ObservedTrade {
                    now_ms: self
                        .simulation_origin_ms
                        .saturating_add(mono)
                        .max(candidate.now_ms()),
                    outcome: outcome_name(*outcome).into(),
                    price_micros: *price_micro,
                    quantity_micros: *quantity_micro,
                    fingerprint: fingerprint.clone(),
                }
            };
            let duplicate = candidate.has_public_trade(fingerprint);
            if let Err(error) = simulation::apply(candidate, &operation) {
                self.latch(error);
                return;
            }
            if !duplicate {
                operations.push(operation);
            }
        }
        if operations.is_empty() {
            return;
        }
        let candidate = candidate.expect("nonempty validated trade batch");
        let Ok(values) = simulation::values(&operations) else {
            self.latch("paper trade encoding failed".into());
            return;
        };
        let Some(mut journal) = self.journal.take() else {
            self.latch("paper journal unavailable".into());
            return;
        };
        match tokio::task::spawn_blocking(move || {
            let result = journal.commit_execution(values);
            (journal, result)
        })
        .await
        {
            Ok((journal, result)) => {
                self.journal_bytes = journal.bytes();
                self.journal = Some(journal);
                if let Err(error) = result {
                    self.latch(error);
                } else {
                    self.simulation = Some(candidate);
                    self.execution_trade_commits = self.execution_trade_commits.saturating_add(1);
                }
            }
            Err(_) => self.latch("paper trade journal worker stopped".into()),
        }
    }
    async fn command(&mut self, mut command: Command) -> Reply {
        command.validate().map_err(|e| Failure::new("invalid", e))?;
        if let Command::Limits { limits, .. } = &mut command {
            *limits = limits.canonical().map_err(|e| Failure::new("invalid", e))?;
            if limits.initial_collateral_usd != self.limits.initial_collateral_usd {
                return Err(Failure::new(
                    "conflict",
                    "initial collateral is fixed at startup; live capital limits are configured in the deployment profile",
                ));
            }
        }
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| Failure::new("unavailable", "journal unavailable; restart required"))?;
        if let Some(receipt) = journal
            .existing(&command)
            .map_err(|e| Failure::new("conflict", e))?
        {
            let historical_resume = matches!(&command, Command::Control {action,..} if action=="resume")
                && self.core.snapshot().mode != Mode::Shadow;
            // A recovered lane may never have received its queued mirror before a
            // crash. Replaying an old receipt must not create a fresh activation.
            if !historical_resume {
                self.live_control(&command).await.map_err(|_|Failure::new("outcome_unknown","durable owner command exists; live acknowledgement unavailable; retry same identity"))?;
                self.mirror_paper(command.clone());
            }
            return Ok(
                json!({"receipt":receipt,"state":self.state.borrow().as_ref(),"replayed":true}),
            );
        }
        let stopping_command =
            matches!(&command,Command::Control {action,..} if action=="pause" || action=="kill");
        if (self.fatal.is_some() && !stopping_command) || self.closing {
            return Err(Failure::new(
                "unavailable",
                "fault latched; restart required",
            ));
        }
        if command.expected_version() != self.version {
            return Err(Failure::new(
                "conflict",
                "expected_version differs from current command_version",
            ));
        }
        if matches!(&command,Command::Control { action,.. } if action=="resume")
            && self
                .simulation
                .as_ref()
                .is_some_and(|paper| paper.portfolio_halt().is_some())
        {
            return Err(Failure::new(
                "conflict",
                "paper portfolio loss/drawdown halt blocks resume",
            ));
        }
        if matches!(&command,Command::Control { action,.. } if action=="resume")
            && self
                .live
                .as_ref()
                .is_some_and(|live| !live.snapshot().can_resume)
        {
            return Err(Failure::new(
                "conflict",
                "live account, stream or heartbeat is not ready; inspect live state",
            ));
        }
        if matches!(&command,Command::Control {action,..} if action=="resume")
            && self.simulation_condition.is_some()
            && self.condition.is_some()
            && self.simulation_condition != self.condition
        {
            return Err(Failure::new(
                "conflict",
                "emulator inventory belongs to another contract; resume is blocked",
            ));
        }
        let mut candidate = self.core.clone();
        let mut limits = self.limits.clone();
        apply_command(&mut candidate, &mut limits, &command)
            .map_err(|e| Failure::new("conflict", e))?;
        let mut simulation_candidate = self.simulation.clone();
        let mut simulation_operations = Vec::new();
        if let Some(session) = &mut simulation_candidate {
            let now = self
                .simulation_origin_ms
                .saturating_add(mono_ms(self.start))
                .max(session.snapshot().now_ms);
            let operation = match &command {
                Command::Control { action, .. }
                    if matches!(action.as_str(), "pause" | "kill" | "reset") =>
                {
                    Some(SimulationOperation::CancelAll { now_ms: now })
                }
                Command::Limits { limits, .. } => Some(SimulationOperation::Limits {
                    now_ms: now,
                    limits: limits.clone(),
                }),
                _ => None,
            };
            if let Some(operation) = operation {
                simulation::apply(session, &operation)
                    .map_err(|error| Failure::new("conflict", error))?;
                simulation_operations.push(operation);
            }
        }
        let execution_values = simulation::values(&simulation_operations)
            .map_err(|error| Failure::new("invalid", error))?;
        let resets = matches!(&command,Command::Control { action,.. } if action=="reset");
        let pauses_live = self.live.is_some() && matches!(&command, Command::Limits { .. });
        let live_command = command.clone();
        let receipt = Receipt {
            command_id: command.id().into(),
            applied_version: self.version + 1,
            mode_at_ack: mode_name(candidate.snapshot().mode).into(),
        };
        // Disk latency cannot expose a new effect before durability, or keep publishing old proposals.
        self.committing = true;
        self.publish();
        let mut journal = self
            .journal
            .take()
            .expect("journal ownership checked above");
        let durable_receipt = receipt.clone();
        let result = tokio::task::spawn_blocking(move || {
            let result = journal.commit_with_execution(command, durable_receipt, execution_values);
            (journal, result)
        })
        .await;
        self.committing = false;
        let (journal, result) = match result {
            Ok(result) => result,
            Err(_) => {
                self.latch("journal writer stopped; reconcile on restart".into());
                return Err(Failure::new(
                    "outcome_unknown",
                    "journal writer stopped; reconcile on restart",
                ));
            }
        };
        self.version = journal.version();
        self.journal_bytes = journal.bytes();
        self.journal = Some(journal);
        if let Err(error) = result {
            self.latch(error);
            return Err(Failure::new(
                "outcome_unknown",
                "durable write failed; reconcile the same command id after restart",
            ));
        }
        self.core = candidate;
        self.limits = limits;
        self.simulation = simulation_candidate;
        if self.live_control(&live_command).await.is_err() {
            self.latch("durable owner command has an unknown live acknowledgement; retry same command identity".into());
            return Err(Failure::new(
                "outcome_unknown",
                "durable owner command has an unknown live acknowledgement; retry same identity",
            ));
        }
        self.mirror_paper(live_command);
        if pauses_live {
            let _ = self.core.control(Action::Pause);
        }
        if resets {
            self.native_snapshot_recovery = None;
            self.reset_epoch += 1;
            self.observation_barrier = Some(mono_ms(self.start));
            // Public Reset requires the source owner to join its old socket and acknowledge
            // a new market setup. New receive timestamps alone do not prove a new generation.
            self.awaiting_source_refresh = self.source == "public";
            while self.feeds.try_recv().is_ok() {}
        }
        // A shutdown or feed overflow may have occurred while the disk worker was running.
        if *self.shutdown.borrow() {
            self.closing = true;
            let _ = self.core.control(Action::Pause);
        }
        let fault = self.fault.borrow().clone();
        if let Some(reason) = fault {
            self.latch(reason);
        } else {
            self.evaluate();
            self.publish();
        }
        Ok(json!({"receipt":receipt,"state":self.state.borrow().as_ref(),"replayed":false}))
    }
    pub async fn run(mut self) {
        let tick_period = Duration::from_millis(100);
        let mut timer = tokio::time::interval(tick_period);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let initial_fault = self.fault.borrow().clone();
        if let Some(reason) = initial_fault {
            self.latch(reason);
        }
        let mut fault_open = true;
        loop {
            if *self.shutdown.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = self.shutdown.changed() => { if changed.is_err() || *self.shutdown.borrow() { break; } }
                changed = self.fault.changed(), if fault_open => {
                    if changed.is_ok() { let reason = self.fault.borrow().clone(); if let Some(reason) = reason { self.latch(reason); } }
                    else { fault_open = false; self.latch("feed fault channel closed".into()); }
                }
                operation = self.commands.recv() => {
                    match operation {
                        Some(Operation::Command(command, reply)) => { let result = self.command(command).await; let _ = reply.send(result); }
                        Some(Operation::Mirror(mut command)) => {
                            let version=self.journal.as_ref().and_then(|journal|journal.history().iter().find(|(prior,_)|prior.id()==command.id()).map(|(prior,_)|prior.expected_version())).unwrap_or(self.version);
                            match &mut command {Command::Control {expected_version,..}|Command::Limits {expected_version,..}=>*expected_version=version}
                            if let Err(error)=self.command(command).await { self.latch(format!("mirrored control failed: {}",error.kind)); }
                        }
                        Some(Operation::Market(market, reply)) => {
                            // The source owner joins the old stream first. Drain its queued generation.
                            while self.feeds.try_recv().is_ok() {}
                            let result = self.select_market(market).await;
                            let _ = reply.send(result);
                        }
                        Some(Operation::ResolutionTargets(reply)) => {
                            let _ = reply.send(self.simulation.as_ref().map_or_else(Vec::new, |s|s.resolution_targets()));
                        }
                        Some(Operation::PublicResolution(evidence,reply)) => {
                            let result = self.admit_public_resolution(evidence).await;
                            let _ = reply.send(result);
                        }
                        Some(Operation::SourceStatus(phase,message,retry)) => {
                            self.source_status=json!({"phase":phase,"message":message.map(|m|m.chars().take(256).collect::<String>()),"retry_in_ms":retry,"at_ms":wall_ms()});self.publish();
                        }
                        None => break,
                    }
                }
                _ = timer.tick() => {
                    let began = tokio::time::Instant::now();
                    self.expire_native_snapshot_recovery();
                    self.evaluate(); self.drive_live(); self.simulation_step(false).await;
                    #[cfg(test)]
                    tokio::time::sleep(self.timer_work_delay).await;
                    self.publish_if_due();
                    if began.elapsed() >= tick_period {
                        // An overdue durability step must not make the biased
                        // timer permanently ready ahead of feed intake. Keep
                        // control/fault priority and the normal cadence; after
                        // an overrun allow one period of intake before retrying.
                        self.execution_periodic_overruns = self.execution_periodic_overruns.saturating_add(1);
                        timer.reset();
                    }
                }
                event = self.feeds.recv() => {
                    match event {
                        Some(event) => {
                            let mut batch = Vec::with_capacity(self.feed_batch_limit);
                            batch.push(event);
                            while batch.len() < self.feed_batch_limit && self.commands.is_empty()
                                && self.fault.borrow().is_none() && !*self.shutdown.borrow() {
                                match self.feeds.try_recv() { Ok(event) => batch.push(event), Err(_) => break }
                            }
                            if self.awaiting_source_refresh { continue; }
                            batch.retain(|event| {
                                let identity = match event { FeedEvent::Snapshot { condition_id,.. } | FeedEvent::Delta { condition_id,.. } | FeedEvent::Tick { condition_id,.. } | FeedEvent::Trade {condition_id,..} => Some(condition_id), _ => None };
                                if identity.is_some_and(|id| self.condition.as_ref() != Some(id)) { return false; }
                                let observed = match event { FeedEvent::Snapshot { observed_mono_ms,.. } | FeedEvent::Delta { observed_mono_ms,.. } | FeedEvent::Tick { observed_mono_ms,.. } | FeedEvent::Reference { observed_mono_ms,.. } | FeedEvent::Trade {observed_mono_ms,..} => Some(*observed_mono_ms),_=>None };
                                !self.observation_barrier.is_some_and(|barrier| observed.is_some_and(|rx|rx<=barrier))
                            });
                            self.execution_feed_batches = self.execution_feed_batches.saturating_add(1);
                            // Genuine invalid input cuts the suffix. A recoverable
                            // split delta blocks quote admission while preserving
                            // independently validated trade witnesses and native books.
                            let valid_prefix = if self.source == "public" {
                                valid_feed_prefix_with_recovery(&self.core, &batch, self.native_snapshot_recovery.map(|(_,seen)|seen))
                            } else { valid_feed_prefix(&self.core, &batch) };
                            self.observed_trades(&batch[..valid_prefix]).await;
                            // A fault/stop received during sync must inhibit effects before
                            // processing the next input, even though this batch is durable.
                            let fault = self.fault.borrow().clone();
                            if let Some(reason) = fault { self.latch(reason); }
                            if *self.shutdown.borrow() { self.closing = true; let _ = self.core.control(Action::Pause); }
                            for event in batch {
                                if !self.apply_feed_event(event) { break; }
                            }
                        }
                        None => { self.latch("feed owner stopped".into()); break; }
                    }
                }
            }
        }
        self.closing = true;
        let _ = self.core.control(Action::Pause);
        self.evaluate();
        self.drive_live();
        if let Some(live) = &self.live {
            let _ = live.shutdown().await;
        }
        self.simulation_step(true).await;
        self.publish();
        if let Some(worker) = self.publication_task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(1), worker).await;
        }
        self.commands.close();
        while let Ok(operation) = self.commands.try_recv() {
            match operation {
                Operation::Command(_, reply) => {
                    let _ = reply.send(Err(Failure::new("unavailable", "service shutting down")));
                }
                Operation::Market(_, reply) => {
                    let _ = reply.send(Err("service shutting down".into()));
                }
                Operation::SourceStatus(..) => {}
                Operation::Mirror(_) => {}
                Operation::ResolutionTargets(reply) => {
                    let _ = reply.send(Vec::new());
                }
                Operation::PublicResolution(_, reply) => {
                    let _ = reply.send(Err("service shutting down".into()));
                }
            }
        }
    }
}

impl Publication {
    fn render(self) -> Arc<Value> {
        let snapshot = self.snapshot;
        let intents: Vec<_> = if self.fatal.is_none()
            && !self.committing
            && !self.closing
            && !self.awaiting_source_refresh
            && !self.awaiting_native_snapshots
        {
            self.desired_quotes
                .iter()
                .map(|i| {
                    json!({"slot":i.slot,"outcome": if i.outcome==mm2_core::Outcome::Up {"up"}else{"down"}, "price":decimal(i.price_micros),
                "shares":decimal(i.quantity_micros), "submitted":false})
                })
                .collect()
        } else {
            Vec::new()
        };
        let now = self.now;
        let mono = self.mono;
        let feed_ready = !self.awaiting_source_refresh
            && !self.awaiting_native_snapshots
            && snapshot
                .market
                .as_ref()
                .is_some_and(|m| now >= m.window_start_ms && now < m.window_end_ms)
            && snapshot.clob_connected
            && book_fresh(
                &snapshot.up_book,
                now,
                mono,
                snapshot.config.max_feed_age_ms,
            )
            && book_fresh(
                &snapshot.down_book,
                now,
                mono,
                snapshot.config.max_feed_age_ms,
            )
            && snapshot.last_error.is_none();
        let api_ready = self.fatal.is_none() && !self.committing && !self.closing;
        let simulation = self
            .simulation
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .or_else(|| self.paper.as_ref().map(|paper| paper["simulation"].clone()));
        let simulation_error = self
            .simulation_error
            .clone()
            .or(self.paper_error)
            .or_else(|| {
                self.paper.as_ref().and_then(|paper| {
                    paper["fault"]
                        .as_str()
                        .or(paper["simulation_error"].as_str())
                        .map(str::to_owned)
                })
            });
        let simulation_condition = self.simulation_condition.clone().or_else(|| {
            self.paper.as_ref().and_then(|paper| {
                paper["simulation_market_condition"]
                    .as_str()
                    .map(str::to_owned)
            })
        });
        let live_eligible =
            api_ready && feed_ready && self.live.as_ref().is_some_and(|live| live.can_resume);
        let mut ordered: Vec<_> = self.durations_ns.iter().copied().collect();
        ordered.sort_unstable();
        let percentile = |n: usize| {
            ordered
                .get((ordered.len().saturating_sub(1) * n).div_ceil(100))
                .map(u64::to_string)
        };
        let market = snapshot.market.as_ref().map(|m| json!({"slug":m.slug,"condition_id":m.condition_id,
            "up_token":m.up_token,"down_token":m.down_token,"window_start_ms":m.window_start_ms,"window_end_ms":m.window_end_ms}));
        let metrics = json!({"feed_events":self.feed_events,"decision_count":self.decision_count,"sample_count":ordered.len(),
            "telemetry_periodic_max_hz":4,"telemetry_coalesced":self.coalesced_publications,"last_snapshot_copy_ns":self.publication_duration_ns,
            "telemetry_policy":"periodic formatting on one-slot background worker; owner command, source and fault transitions publish immediately",
            "execution_noop_ticks":self.execution_noop_ticks,
            "execution_periodic_overruns":self.execution_periodic_overruns,
            "execution_feed_batches":self.execution_feed_batches,
            "execution_trade_commits":self.execution_trade_commits,
            "execution_feed_batch_limit":self.feed_batch_limit,
            "decision_ns_p50":percentile(50),"decision_ns_p99":if ordered.len()>=1000 {percentile(99)} else {None},
            "measurement":"core and strategy evaluation; excludes transport, parsing, queue wait, execution durability and snapshot publication"});
        let diagnostics = json!({
            "awaiting_native_snapshots":self.awaiting_native_snapshots,
            "paper_resolution":self.paper_resolution.or_else(||self.paper.as_ref().map(|paper|paper["paper_resolution"].clone())),
            "fee_check":self.fee_check,
            "feed_diagnostics":self.feed_diagnostics,
            "feed_diagnostics_policy":"bounded atomic counters sampled independently; local boot scope, not a durable event stream",
            "fault_class":self.fatal.as_deref().map(runtime_failure_class),
            "input_failure_class":snapshot.last_error.as_deref().map(crate::feeds::input_failure_class)
        });
        let mut publication = json!({
            "schema_version":1, "boot_id":self.boot_id,"sequence":self.sequence,"published_at_ms":now,
            "mode":mode_name(snapshot.mode),"command_version":self.version,"feed_source":self.source,
            "reset_epoch":self.reset_epoch,"source_status":self.source_status,
            "awaiting_source_refresh":self.awaiting_source_refresh,
            "api_ready":api_ready,"feed_ready":feed_ready,"shadow_ready":api_ready && feed_ready && !intents.is_empty(),
            "live_eligible":live_eligible,"exact_twap_available":false,"strategy":"mo_money_book_candidate",
            "execution_mode":self.execution_mode,
            "simulation":simulation,"simulation_error":simulation_error,
            "simulation_market_condition":simulation_condition,
            "strategy_gate":self.strategy_gate,
            "live":self.live,"parallel_simulation":self.paper.as_ref().map(|paper|json!({"mode":paper["mode"],"fault":paper["fault"],"published_at_ms":paper["published_at_ms"],"command_version":paper["command_version"],"journal":paper["journal"]})),
            "journal":self.journal_metrics,
            "paper_portfolio":self.paper_portfolio.or_else(||self.paper.as_ref().and_then(|paper|serde_json::from_value(paper["paper_portfolio"].clone()).ok())),
            "market":market,"limits":self.limits,"shadow_intents":intents,
            "hypothetical_reservation_usd": if api_ready { decimal(self.last_decision.hypothetical_reservation_micro) } else { "0".into() },
            "worst_case_imbalance_shares": if api_ready { decimal(snapshot.worst_case_imbalance_micro) } else { "0".into() },
            "pnl":null,
            "books":{"up":book_json(&snapshot.up_book,now,mono),"down":book_json(&snapshot.down_book,now,mono)},
            "reference":snapshot.reference.as_ref().map(|r| json!({"source":"binance_btcusdt", "price":decimal(r.price_micro),
                "exchange_ms":r.exchange_ms,"observed_mono_ms":r.observed_mono_ms,"oracle":false})),
            "binance_connected":snapshot.binance_connected,"fault":self.fatal,"last_input_error":snapshot.last_error,
            "stop_reasons":self.last_decision.stop_reasons.iter().map(reason_name).collect::<Vec<_>>(),
            "committing":self.committing,"shutting_down":self.closing,"journal_bytes":self.journal_bytes,
            "metrics":metrics
        });
        publication["paper_strategy_policy"] = if self.execution_mode == "simulation"
            && self.source == "public"
        {
            json!({"id":"r17-directional-admission-subsets-v1","experimental":true,"entry_guard_bps":1,"entry_guard_horizon_ms":1000,"entry_filter_before_risk":true,"quote_selection":"full_set_else_safe_strategy_priority_subset","repair":"passive_complement_strict_gross_risk_reduction","minimum_order_shares":5,"global_risk_limits_unchanged":true})
        } else {
            Value::Null
        };
        if let (Value::Object(publication), Value::Object(diagnostics)) =
            (&mut publication, diagnostics)
        {
            publication.extend(diagnostics);
        }
        Arc::new(publication)
    }
}

fn runtime_failure_class(reason: &str) -> &'static str {
    if reason.contains("unknown") && reason.contains("acknowledgement") {
        "external_acknowledgement_unknown"
    } else if reason.contains("quota") || reason.contains("capacity") || reason.contains("bound") {
        "capacity_fault"
    } else if reason.contains("journal")
        || reason.contains("durable")
        || reason.contains("sync")
        || reason.contains("writer")
    {
        "durability_fault"
    } else if reason.contains("invariant") {
        "simulation_invariant_fault"
    } else if reason.contains("feed")
        || reason.contains("source")
        || reason.contains("WebSocket")
        || reason.contains("REST")
    {
        "source_fault"
    } else {
        "runtime_fault"
    }
}

fn outcome_name(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Up => "up",
        Outcome::Down => "down",
    }
}
fn common_tick(up: u64, down: u64) -> u64 {
    if up == 0 || down == 0 {
        return 0;
    }
    let (mut a, mut b) = (up, down);
    while b != 0 {
        let remainder = a % b;
        a = b;
        b = remainder;
    }
    (up / a)
        .checked_mul(down)
        .filter(|tick| *tick < 1_000_000)
        .unwrap_or(0)
}
fn book_fresh(book: &BookSnapshot, wall: u64, mono: u64, max_age: u64) -> bool {
    matches!((book.exchange_ms,book.observed_mono_ms), (Some(ex),Some(rx)) if ex <= wall && rx <= mono && wall-ex <= max_age && mono-rx <= max_age)
}
fn book_json(book: &BookSnapshot, wall: u64, mono: u64) -> Value {
    json!({"initialized":book.initialized,"tick":decimal(book.tick_micro),"best_bid":book.best_bid_micro.map(decimal),
        "best_ask":book.best_ask_micro.map(decimal),"exchange_ms":book.exchange_ms,"observed_mono_ms":book.observed_mono_ms,
        "exchange_age_ms":book.exchange_ms.and_then(|ex|wall.checked_sub(ex)),"receive_age_ms":book.observed_mono_ms.and_then(|rx|mono.checked_sub(rx))})
}
fn reason_name(reason: &StopReason) -> String {
    match reason {
        StopReason::MissingBook(o) => format!("missing_{}_book", outcome_name(*o)),
        StopReason::StaleBook(o) => format!("stale_{}_book", outcome_name(*o)),
        StopReason::NoLegalQuote(o) => format!("no_legal_{}_quote", outcome_name(*o)),
        StopReason::Paused => "paused".into(),
        StopReason::Killed => "killed".into(),
        StopReason::MissingMarket => "missing_market".into(),
        StopReason::OutsideWindow => "outside_window".into(),
        StopReason::ClobDisconnected => "clob_disconnected".into(),
        StopReason::InvalidClock => "invalid_clock".into(),
        StopReason::InvalidInput => "invalid_input".into(),
        StopReason::BudgetExceeded => "budget_exceeded".into(),
        StopReason::ImbalanceExceeded => "imbalance_exceeded".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_book_refresh_preserves_owner_and_portfolio_and_is_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        let (_tx, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_stop, shutdown) = watch::channel(false);
        let (handle, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        let now = wall_ms();
        let market = Market {
            slug: "offline-refresh".into(),
            condition_id: format!("0x{}", "a".repeat(64)),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: now - 1000,
            window_end_ms: now + 299_000,
            tick_micro: 10_000,
        };
        actor.select_market(market.clone()).await.unwrap();
        actor.core.control(Action::Resume).unwrap();
        let finance = actor.simulation.as_ref().unwrap().portfolio_snapshot();
        let seed = |outcome| FeedEvent::Snapshot {
            condition_id: market.condition_id.clone(),
            outcome,
            exchange_ms: now,
            observed_mono_ms: 1,
            bids: vec![crate::domain::Level {
                price_micro: 490_000,
                quantity_micro: 10_000_000,
            }],
            asks: vec![crate::domain::Level {
                price_micro: 510_000,
                quantity_micro: 10_000_000,
            }],
        };
        let adverse = || FeedEvent::Snapshot {
            condition_id: market.condition_id.clone(),
            outcome: Outcome::Up,
            exchange_ms: now + 1,
            observed_mono_ms: 2,
            bids: vec![crate::domain::Level {
                price_micro: 520_000,
                quantity_micro: 1_000_000,
            }],
            asks: vec![crate::domain::Level {
                price_micro: 510_000,
                quantity_micro: 1_000_000,
            }],
        };
        for attempt in 0..3 {
            if attempt > 0 {
                actor.select_market(market.clone()).await.unwrap();
            }
            actor.core.apply(seed(Outcome::Up)).unwrap();
            actor.core.apply(seed(Outcome::Down)).unwrap();
            let print = FeedEvent::Trade {
                condition_id: market.condition_id.clone(),
                outcome: Outcome::Up,
                exchange_ms: now + 1,
                observed_mono_ms: 2,
                price_micro: 490_000,
                quantity_micro: 5_000_000,
                fingerprint: "f".repeat(64),
            };
            assert_eq!(valid_feed_prefix(&actor.core, &[adverse(), print]), 0);
            assert!(!actor.apply_feed_event(adverse()));
            let state = handle.state.borrow().clone();
            assert_eq!(state["command_version"], 0);
            assert_eq!(state["reset_epoch"], 0);
            assert_eq!(state["awaiting_source_refresh"], true);
            assert_eq!(
                actor.simulation.as_ref().unwrap().portfolio_snapshot(),
                finance
            );
            if attempt < 2 {
                assert_eq!(state["mode"], "shadow");
                assert!(state["fault"].is_null());
            } else {
                assert_eq!(state["mode"], "paused");
                assert!(
                    state["fault"]
                        .as_str()
                        .unwrap()
                        .contains("repeated public input validation")
                );
            }
        }
    }

    #[tokio::test]
    async fn split_delta_preserves_queued_native_recovery_and_source_clock_barrier() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        let (_tx, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_stop, shutdown) = watch::channel(false);
        let (handle, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        let now = wall_ms();
        let market = Market {
            slug: "split-public-update".into(),
            condition_id: format!("0x{}", "a".repeat(64)),
            up_token: "1".into(),
            down_token: "2".into(),
            window_start_ms: now - 1000,
            window_end_ms: now + 299_000,
            tick_micro: 10_000,
        };
        actor.select_market(market.clone()).await.unwrap();
        actor.core.control(Action::Resume).unwrap();
        let finance = actor.simulation.as_ref().unwrap().portfolio_snapshot();
        let seed = |outcome, at| FeedEvent::Snapshot {
            condition_id: market.condition_id.clone(),
            outcome,
            exchange_ms: now + at,
            observed_mono_ms: at,
            bids: vec![crate::domain::Level {
                price_micro: 490_000,
                quantity_micro: 10_000_000,
            }],
            asks: vec![crate::domain::Level {
                price_micro: 510_000,
                quantity_micro: 10_000_000,
            }],
        };
        let crossed = |at| FeedEvent::Delta {
            condition_id: market.condition_id.clone(),
            exchange_ms: now + at,
            observed_mono_ms: at,
            changes: vec![crate::domain::Change {
                outcome: Outcome::Up,
                side: crate::domain::BookSide::Bid,
                level: crate::domain::Level {
                    price_micro: 510_000,
                    quantity_micro: 5_000_000,
                },
            }],
        };
        actor.core.apply(seed(Outcome::Up, 1)).unwrap();
        actor.core.apply(seed(Outcome::Down, 1)).unwrap();
        let print = FeedEvent::Trade {
            condition_id: market.condition_id.clone(),
            outcome: Outcome::Up,
            exchange_ms: now,
            observed_mono_ms: 0,
            price_micro: 490_000,
            quantity_micro: 5_000_000,
            fingerprint: "f".repeat(64),
        };
        let queued = [
            crossed(2),
            print.clone(),
            seed(Outcome::Up, 3),
            seed(Outcome::Down, 3),
        ];
        assert_eq!(valid_feed_prefix(&actor.core, &queued), 0);
        assert_eq!(
            valid_feed_prefix_with_recovery(&actor.core, &queued, None),
            queued.len()
        );
        let barrier = actor.observation_barrier;
        assert!(actor.apply_feed_event(queued[0].clone()));
        assert!(actor.native_snapshot_recovery.is_some());
        assert_eq!(actor.observation_barrier, barrier);
        let recovering = actor.publication().render();
        assert_eq!(recovering["awaiting_native_snapshots"], true);
        assert_eq!(recovering["feed_ready"], false);
        assert!(recovering["shadow_intents"].as_array().unwrap().is_empty());
        assert!(actor.apply_feed_event(crossed(2))); // still waiting: a delta cannot initialize
        actor.observed_trades(&queued).await;
        assert!(
            actor
                .simulation
                .as_ref()
                .unwrap()
                .has_public_trade(&"f".repeat(64))
        );
        assert!(actor.apply_feed_event(print));
        assert!(actor.apply_feed_event(queued[2].clone()));
        assert!(actor.native_snapshot_recovery.is_some());
        assert!(actor.apply_feed_event(queued[3].clone()));
        assert!(actor.native_snapshot_recovery.is_none());
        assert!(!actor.awaiting_source_refresh && actor.source_refresh_failures.is_empty());
        assert_eq!(actor.observation_barrier, barrier);
        assert_eq!(
            actor.simulation.as_ref().unwrap().portfolio_snapshot(),
            finance
        );
        assert_eq!(actor.version, 0);
        assert_eq!(actor.reset_epoch, 0);
        // Genuine invalid input still cuts its trade suffix and requests a refresh.
        let mut invalid = seed(Outcome::Up, 4);
        if let FeedEvent::Snapshot { bids, .. } = &mut invalid {
            bids[0].price_micro = 520_000;
        }
        assert_eq!(
            valid_feed_prefix_with_recovery(&actor.core, &[invalid], None),
            0
        );
        assert!(actor.apply_feed_event(crossed(4)));
        actor.native_snapshot_recovery.as_mut().unwrap().0 =
            Instant::now() - Duration::from_millis(config.limits.max_feed_age_ms + 1);
        actor.expire_native_snapshot_recovery();
        assert!(actor.awaiting_source_refresh && actor.native_snapshot_recovery.is_none());
        assert_eq!(actor.source_refresh_failures.len(), 1);
        assert_eq!(handle.state.borrow()["command_version"], 0);
        assert_eq!(
            actor.simulation.as_ref().unwrap().portfolio_snapshot(),
            finance
        );
    }

    fn batch_config(directory: &std::path::Path) -> Config {
        Config {
            mode: "simulation".into(),
            feed_source: crate::config::FeedSource::Public,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.into(),
            live_state_dir: None,
            journal_max_bytes: 1_048_576,
            journal: None,
            strategy: Default::default(),
            live: None,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "100".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
        }
    }

    fn retained_windows(config: &Config, count: u64) {
        retained_windows_version(config, count, false)
    }
    fn retained_windows_version(config: &Config, count: u64, exact: bool) {
        let mut journal = Journal::open_streaming(
            &config.state_dir,
            crate::journal::JournalOptions::default(),
            config.limits.canonical().unwrap(),
        )
        .unwrap();
        if exact {
            journal
                .commit_execution(
                    simulation::values(&[SimulationOperation::ExactAccounting {}]).unwrap(),
                )
                .unwrap();
        }
        for i in 0..count {
            let now = i * 20_000;
            let inputs = vec![
                SimulationOperation::Market {
                    condition_id: format!("0x{:064x}", i + 1),
                    start_ms: Some(i * 300_000),
                    end_ms: Some((i + 1) * 300_000),
                },
                SimulationOperation::Book {
                    now_ms: now,
                    outcome: "up".into(),
                    bid: 400_000,
                    ask: 500_000,
                    ahead_micros: 0,
                },
                SimulationOperation::Quotes {
                    now_ms: now,
                    quotes: vec![simulation::Quote {
                        slot: "up".into(),
                        outcome: "up".into(),
                        price_micros: 400_000,
                        quantity_micros: 5_000_000,
                    }],
                },
                SimulationOperation::Advance { now_ms: now + 50 },
                SimulationOperation::Trade {
                    now_ms: now + 60,
                    outcome: "up".into(),
                    worst_price_micros: 400_000,
                    quantity_micros: 5_000_000,
                },
                SimulationOperation::Advance { now_ms: now + 100 },
                SimulationOperation::CancelAll { now_ms: now + 100 },
                SimulationOperation::Advance { now_ms: now + 1000 },
            ];
            journal
                .commit_execution(simulation::values(&inputs).unwrap())
                .unwrap();
        }
    }

    #[tokio::test]
    async fn unresolved_cap_pauses_without_killing_actor_and_retains_readable_state_and_owner_receipt()
     {
        let directory = tempfile::tempdir().unwrap();
        let mut config = batch_config(directory.path());
        config.limits.initial_collateral_usd = "1000".into();
        retained_windows(&config, 64);
        let (_tx, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (stop, shutdown) = watch::channel(false);
        let (handle, actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        let task = tokio::spawn(actor.run());
        let market = Market {
            slug: "offline-cap-regression".into(),
            condition_id: format!("0x{:064x}", 65),
            up_token: "11".into(),
            down_token: "12".into(),
            window_start_ms: 64 * 300_000,
            window_end_ms: 65 * 300_000,
            tick_micro: 1000,
        };
        handle.select_market(market).await.unwrap();
        let state = handle.state.borrow().clone();
        assert_eq!(state["mode"], "paused");
        assert!(
            state["fault"]
                .as_str()
                .unwrap()
                .contains("unresolved market capacity")
        );
        assert_eq!(state["paper_portfolio"]["held_market_count"], 64);
        assert_eq!(
            state["paper_portfolio"]["initial_cash_pico"],
            "1000000000000000"
        );
        assert_eq!(
            state["simulation_market_condition"],
            format!("0x{:064x}", 64)
        );
        assert!(!task.is_finished());
        let reply = handle
            .command(Command::Control {
                id: "capacity-pause".into(),
                expected_version: 0,
                action: "pause".into(),
            })
            .await
            .unwrap();
        assert_eq!(reply["receipt"]["applied_version"], 1);
        assert!(
            handle
                .command(Command::Control {
                    id: "capacity-resume".into(),
                    expected_version: 1,
                    action: "resume".into()
                })
                .await
                .is_err()
        );
        assert_eq!(handle.resolution_targets().await.unwrap().len(), 63);
        stop.send_replace(true);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn finalized_paper_credit_is_durable_and_sync_failure_does_not_publish_candidate_cash() {
        for fail_sync in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let config = batch_config(directory.path());
            retained_windows(&config, 2);
            let (_tx, rx) = mpsc::channel(16);
            let (_fault, fault) = watch::channel(None);
            let (_stop, shutdown) = watch::channel(false);
            let (_, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
            let before = actor.simulation.as_ref().unwrap().portfolio_snapshot();
            let target = crate::paper_resolution::Target {
                condition_id: format!("0x{:064x}", 1),
                start_ms: 0,
                end_ms: 300_000,
            };
            let proof = crate::paper_resolution::tests::evidence(target, true, 300_000);
            if fail_sync {
                actor.journal.as_mut().unwrap().inject_sync_failure();
            }
            let result = actor.admit_public_resolution(proof).await;
            let after = actor.simulation.as_ref().unwrap().portfolio_snapshot();
            if fail_sync {
                assert!(result.is_err());
                assert_eq!(before.cash_pico, after.cash_pico);
                assert!(actor.fatal.is_some());
            } else {
                result.unwrap();
                assert_eq!(after.held_market_count, Some(1));
                assert_eq!(after.realized_pnl_pico, "3000000000000");
                drop(actor);
                let (_tx, rx) = mpsc::channel(16);
                let (_fault, fault) = watch::channel(None);
                let (_stop, shutdown) = watch::channel(false);
                let (_, recovered) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
                assert_eq!(
                    recovered
                        .simulation
                        .as_ref()
                        .unwrap()
                        .portfolio_snapshot()
                        .cash_pico,
                    after.cash_pico
                );
                assert_eq!(
                    recovered.simulation.as_ref().unwrap().resolution_status()["completed"],
                    1
                );
            }
        }
    }

    #[tokio::test]
    async fn public_paper_entry_filter_blocks_adverse_side_and_missing_reference() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        let mut actor = batch_actor(&config).await;
        simulation::apply(
            actor.simulation.as_mut().unwrap(),
            &SimulationOperation::ExactAccounting {},
        )
        .unwrap();
        actor.start = Instant::now() - Duration::from_secs(2);
        let now = wall_ms();
        let mono = mono_ms(actor.start);
        let condition = actor.condition.clone().unwrap();
        let market = actor.core.snapshot().market.unwrap();
        let (_fees, rx) = watch::channel(Some(mm2_live::fees::FeePolicy {
            condition_id: market.condition_id,
            up_token: market.up_token,
            down_token: market.down_token,
            rate: "0.07".into(),
            exponent: 1,
            taker_only: true,
            observed_at_ms: now,
            response_sha256: "a".repeat(64),
        }));
        actor.fee_policy = rx;
        for (outcome, bid, ask) in [
            (Outcome::Up, 490_000, 510_000),
            (Outcome::Down, 490_000, 510_000),
        ] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: now,
                    observed_mono_ms: mono,
                    bids: vec![crate::domain::Level {
                        price_micro: bid,
                        quantity_micro: 5_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: ask,
                        quantity_micro: 5_000_000,
                    }],
                })
                .unwrap();
        }
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        assert!(actor.desired_quotes.is_empty());
        actor
            .reference_guard
            .observe(now - 1_000, mono - 1_000, 100_000_000);
        actor
            .core
            .apply(FeedEvent::Reference {
                exchange_ms: now,
                observed_mono_ms: mono,
                price_micro: 100_020_000,
            })
            .unwrap();
        actor.evaluate();
        assert!(
            !actor.desired_quotes.is_empty(),
            "gate={:?} stops={:?} reference={:?}",
            actor.strategy_gate,
            actor.last_decision.stop_reasons,
            actor
                .reference_guard
                .blocked(mono_ms(actor.start), wall_ms())
        );
        assert!(
            actor
                .desired_quotes
                .iter()
                .all(|q| q.outcome == mm2_core::Outcome::Up)
        );
    }

    #[tokio::test]
    async fn public_filter_precedes_budget_and_selection_preserves_all_retained_market_risk() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        retained_windows_version(&config, 4, true); // $8 of unresolved prior risk.
        let mut actor = batch_actor(&config).await;
        actor.start = Instant::now() - Duration::from_secs(2);
        let now = wall_ms();
        let mono = mono_ms(actor.start);
        let market = actor.core.snapshot().market.unwrap();
        let (_fees, rx) = watch::channel(Some(mm2_live::fees::FeePolicy {
            condition_id: market.condition_id.clone(),
            up_token: market.up_token,
            down_token: market.down_token,
            rate: "0.07".into(),
            exponent: 1,
            taker_only: true,
            observed_at_ms: now,
            response_sha256: "a".repeat(64),
        }));
        actor.fee_policy = rx;
        for (outcome, bid, ask) in [
            (Outcome::Up, 250_000, 260_000),
            (Outcome::Down, 740_000, 750_000),
        ] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: market.condition_id.clone(),
                    outcome,
                    exchange_ms: now,
                    observed_mono_ms: mono,
                    bids: vec![crate::domain::Level {
                        price_micro: bid,
                        quantity_micro: 5_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: ask,
                        quantity_micro: 5_000_000,
                    }],
                })
                .unwrap();
        }
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        assert_eq!(actor.strategy_gate, Some(mm2_strategy::Gate::MissingSignal));
        assert!(actor.desired_quotes.is_empty());
        for (latest, expected_gate) in [
            (100_020_000, mm2_strategy::Gate::Quote),
            (100_000_000, mm2_strategy::Gate::Quote),
            (99_980_000, mm2_strategy::Gate::LossBudget),
        ] {
            actor.reference_guard = Default::default();
            actor
                .reference_guard
                .observe(now - 1_000, mono - 1_000, 100_000_000);
            actor
                .core
                .apply(FeedEvent::Reference {
                    exchange_ms: now,
                    observed_mono_ms: mono,
                    price_micro: latest,
                })
                .unwrap();
            actor.evaluate();
            assert_eq!(actor.strategy_gate, Some(expected_gate));
            if expected_gate == mm2_strategy::Gate::Quote {
                assert_eq!(actor.desired_quotes.len(), 1);
                assert_eq!(actor.desired_quotes[0].outcome, mm2_core::Outcome::Up);
                assert!(
                    actor
                        .simulation
                        .as_ref()
                        .unwrap()
                        .quote_risk_permitted(&actor.desired_quotes)
                        .unwrap()
                );
            } else {
                assert!(actor.desired_quotes.is_empty());
            }
        }
        assert_eq!(
            actor
                .simulation
                .as_ref()
                .unwrap()
                .portfolio_snapshot()
                .held_market_count,
            Some(4)
        );
        assert_eq!(actor.limits, config.limits.canonical().unwrap());
    }

    async fn batch_actor(config: &Config) -> Actor {
        let (_feeds, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_stop, shutdown) = watch::channel(false);
        let (_, mut actor) = prepare(config, Instant::now(), rx, fault, shutdown).unwrap();
        let condition = format!("0x{}", "a".repeat(64));
        let now = wall_ms();
        actor
            .core
            .set_market(Market {
                slug: "offline-trade-batch".into(),
                condition_id: condition.clone(),
                up_token: "1".into(),
                down_token: "2".into(),
                window_start_ms: now - 1000,
                window_end_ms: now + 60_000,
                tick_micro: 1000,
            })
            .unwrap();
        actor.bind_simulation_market(&condition).await.unwrap();
        actor.condition = Some(condition);
        actor
    }
    #[tokio::test]
    async fn paper_actor_uses_exact_fill_basis_before_proposing_a_complement() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = batch_config(directory.path());
        config.feed_source = crate::config::FeedSource::Demo;
        let mut actor = batch_actor(&config).await;
        let now = actor.simulation.as_ref().unwrap().now_ms();
        for operation in [
            SimulationOperation::Book {
                now_ms: now,
                outcome: "down".into(),
                bid: 640_000,
                ask: 650_000,
                ahead_micros: 0,
            },
            SimulationOperation::Quotes {
                now_ms: now,
                quotes: vec![simulation::Quote {
                    slot: "down:lot".into(),
                    outcome: "down".into(),
                    price_micros: 640_000,
                    quantity_micros: 5_000_000,
                }],
            },
            SimulationOperation::Advance { now_ms: now + 100 },
            SimulationOperation::Trade {
                now_ms: now + 101,
                outcome: "down".into(),
                worst_price_micros: 640_000,
                quantity_micros: 5_000_000,
            },
            SimulationOperation::Advance { now_ms: now + 1000 },
        ] {
            simulation::apply(actor.simulation.as_mut().unwrap(), &operation).unwrap();
        }
        let condition = actor.condition.clone().unwrap();
        for (outcome, bid, ask) in [
            (Outcome::Up, 600_000, 610_000),
            (Outcome::Down, 390_000, 400_000),
        ] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: wall_ms(),
                    observed_mono_ms: mono_ms(actor.start),
                    bids: vec![crate::domain::Level {
                        price_micro: bid,
                        quantity_micro: 5_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: ask,
                        quantity_micro: 5_000_000,
                    }],
                })
                .unwrap();
        }
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        assert_eq!(actor.strategy_gate, Some(mm2_strategy::Gate::Quote));
        assert_eq!(actor.desired_quotes.len(), 1);
        assert_eq!(actor.desired_quotes[0].slot, "up:hedge");
        assert_eq!(actor.desired_quotes[0].price_micros, 360_000);
        assert_eq!(actor.desired_quotes[0].quantity_micros, 5_000_000);
        assert_eq!(actor.limits, config.limits.canonical().unwrap());
    }
    #[tokio::test]
    async fn public_fee_gate_requires_current_market_and_fresh_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let mut actor = batch_actor(&batch_config(directory.path())).await;
        let (fees, rx) = watch::channel(None);
        actor.fee_policy = rx;
        let now = wall_ms();
        assert!(!actor.fee_ready(now));
        let market = actor.core.snapshot().market.unwrap();
        let policy = mm2_live::fees::parse_policy(
            &json!({"c":market.condition_id,
            "t":[{"t":"1","o":"Up"},{"t":"2","o":"Down"}],"fd":{"r":0.07,"e":1,"to":true}}),
            &market.condition_id,
            "1",
            "2",
            now,
        )
        .unwrap();
        fees.send_replace(Some(policy.clone()));
        assert!(actor.fee_ready(now));
        assert!(!actor.fee_ready(now + mm2_live::fees::FEE_TTL_MS));
        let mut foreign = policy;
        foreign.condition_id = format!("0x{}", "b".repeat(64));
        fees.send_replace(Some(foreign));
        assert!(!actor.fee_ready(now));
        actor.evaluate();
        assert!(actor.desired_quotes.is_empty());
        fees.send_replace(None);
        assert_eq!(
            actor.fee_check(now)["status"],
            "BLOCKED_MISSING_STALE_OR_UNSUPPORTED"
        );
    }

    #[tokio::test]
    async fn quote_is_recomputed_after_pending_partial_fill_and_cancel_keeps_possible_fills() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = batch_config(directory.path());
        config.feed_source = crate::config::FeedSource::Demo;
        let mut actor = batch_actor(&config).await;
        let now = actor.simulation.as_ref().unwrap().now_ms();
        let setup = [
            SimulationOperation::Book {
                now_ms: now,
                outcome: "up".into(),
                bid: 600_000,
                ask: 610_000,
                ahead_micros: 0,
            },
            SimulationOperation::Quotes {
                now_ms: now,
                quotes: vec![simulation::Quote {
                    slot: "up:lot".into(),
                    outcome: "up".into(),
                    price_micros: 600_000,
                    quantity_micros: 5_000_000,
                }],
            },
            SimulationOperation::Advance { now_ms: now + 100 },
            SimulationOperation::Trade {
                now_ms: now + 101,
                outcome: "up".into(),
                worst_price_micros: 600_000,
                quantity_micros: 50_000,
            },
        ];
        for operation in &setup {
            simulation::apply(actor.simulation.as_mut().unwrap(), operation).unwrap();
        }
        actor
            .journal
            .as_mut()
            .unwrap()
            .commit_execution(simulation::values(&setup).unwrap())
            .unwrap();
        assert_eq!(
            actor
                .simulation
                .as_ref()
                .unwrap()
                .snapshot()
                .account
                .up_inventory_micros,
            0
        );
        let condition = actor.condition.clone().unwrap();
        for (outcome, bid, ask) in [
            (Outcome::Up, 600_000, 610_000),
            (Outcome::Down, 390_000, 400_000),
        ] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: wall_ms(),
                    observed_mono_ms: mono_ms(actor.start),
                    bids: vec![crate::domain::Level {
                        price_micro: bid,
                        quantity_micro: 5_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: ask,
                        quantity_micro: 5_000_000,
                    }],
                })
                .unwrap();
        }
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        assert_eq!(actor.strategy_gate, Some(mm2_strategy::Gate::Quote));
        assert!(
            actor
                .desired_quotes
                .iter()
                .any(|quote| quote.slot == "up:lot")
        );
        actor.simulation_origin_ms = (now + 1000).saturating_sub(mono_ms(actor.start));
        actor.simulation_last_trade_ms = now + 1000;
        actor.simulation_step(false).await;
        assert!(actor.fatal.is_none());
        assert_eq!(actor.strategy_gate, Some(mm2_strategy::Gate::InventoryDust));
        assert!(actor.desired_quotes.is_empty());
        let cancelling = actor.simulation.as_ref().unwrap().snapshot();
        assert_eq!(cancelling.account.up_inventory_micros, 50_000);
        assert!(cancelling.pending_events > 0);
        assert!(cancelling.account.reserved_micros_ceil > 0);
        actor.simulation_step(true).await;
        let terminal = actor.simulation.as_ref().unwrap().snapshot();
        assert_eq!(terminal.pending_events, 0);
        assert_eq!(terminal.account.reserved_micros_ceil, 0);
        assert!(terminal.orders.is_empty());
        assert_eq!(terminal.account.up_inventory_micros, 50_000);
        let journal = actor.journal.as_ref().unwrap();
        let mut replayed = simulation::new(journal.bootstrap()).unwrap();
        journal
            .visit_execution(|entries| simulation::replay(&mut replayed, entries))
            .unwrap();
        assert!(
            simulation::same_state_without_clock(&replayed, actor.simulation.as_ref().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn simulation_actor_gates_new_risk_against_old_unresolved_positions() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        {
            let mut journal = Journal::open_streaming(
                &config.state_dir,
                crate::journal::JournalOptions::default(),
                config.limits.canonical().unwrap(),
            )
            .unwrap();
            journal
                .commit_execution(
                    simulation::values(&[SimulationOperation::ExactAccounting {}]).unwrap(),
                )
                .unwrap();
        }
        retained_windows(&config, 4);
        let mut actor = batch_actor(&config).await;
        actor.start = Instant::now() - Duration::from_secs(2);
        let condition = actor.condition.clone().unwrap();
        for (outcome, bid, ask) in [
            (Outcome::Up, 490_000, 510_000),
            (Outcome::Down, 490_000, 510_000),
        ] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: wall_ms(),
                    observed_mono_ms: mono_ms(actor.start),
                    bids: vec![crate::domain::Level {
                        price_micro: bid,
                        quantity_micro: 5_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: ask,
                        quantity_micro: 5_000_000,
                    }],
                })
                .unwrap();
        }
        let now = wall_ms();
        let mono = mono_ms(actor.start);
        actor
            .reference_guard
            .observe(now - 1_000, mono - 1_000, 100_000_000);
        actor
            .core
            .apply(FeedEvent::Reference {
                exchange_ms: now,
                observed_mono_ms: mono,
                price_micro: 100_000_000,
            })
            .unwrap();
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        assert_eq!(actor.strategy_gate, Some(mm2_strategy::Gate::LossBudget));
        assert!(actor.desired_quotes.is_empty());
        assert!(actor.fatal.is_none());
        assert_eq!(
            actor
                .simulation
                .as_ref()
                .unwrap()
                .portfolio_snapshot()
                .held_market_count,
            Some(4)
        );
        assert!(
            actor
                .simulation
                .as_ref()
                .unwrap()
                .portfolio_halt()
                .is_none()
        );
    }

    fn trades(actor: &Actor, count: usize) -> Vec<FeedEvent> {
        (1..=count)
            .map(|i| FeedEvent::Trade {
                condition_id: actor.condition.clone().unwrap(),
                outcome: Outcome::Up,
                exchange_ms: wall_ms(),
                observed_mono_ms: mono_ms(actor.start),
                price_micro: 450_000,
                quantity_micro: 5_000_000,
                fingerprint: format!("{i:064x}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn trade_batch_replays_exactly_and_duplicate_inputs_do_not_add_commits() {
        let directory = tempfile::tempdir().unwrap();
        let config = batch_config(directory.path());
        let mut actor = batch_actor(&config).await;
        let now = actor.simulation.as_ref().unwrap().now_ms();
        let setup = vec![
            SimulationOperation::Book {
                now_ms: now,
                outcome: "up".into(),
                bid: 450_000,
                ask: 550_000,
                ahead_micros: 1_000_000,
            },
            SimulationOperation::Quotes {
                now_ms: now,
                quotes: vec![simulation::Quote {
                    slot: "up".into(),
                    outcome: "up".into(),
                    price_micros: 450_000,
                    quantity_micros: 5_000_000,
                }],
            },
            SimulationOperation::Advance { now_ms: now + 100 },
        ];
        for operation in &setup {
            simulation::apply(actor.simulation.as_mut().unwrap(), operation).unwrap();
        }
        actor
            .journal
            .as_mut()
            .unwrap()
            .commit_execution(simulation::values(&setup).unwrap())
            .unwrap();
        let operations_before = actor
            .journal
            .as_ref()
            .unwrap()
            .metrics()
            .execution_operations;
        let events = trades(&actor, FEED_BATCH_LIMIT);
        actor.observed_trades(&events).await;
        assert!(actor.fatal.is_none());
        assert_eq!(actor.execution_trade_commits, 1);
        let journal = actor.journal.as_ref().unwrap();
        assert_eq!(
            journal.metrics().execution_operations,
            FEED_BATCH_LIMIT + operations_before
        );
        // Apply fill notices before comparing exact cash, inventory and reservations.
        let drain = SimulationOperation::Advance {
            now_ms: actor.simulation.as_ref().unwrap().now_ms() + 1000,
        };
        simulation::apply(actor.simulation.as_mut().unwrap(), &drain).unwrap();
        actor
            .journal
            .as_mut()
            .unwrap()
            .commit_execution(simulation::values(&[drain]).unwrap())
            .unwrap();
        let journal = actor.journal.as_ref().unwrap();
        let sequence = journal.metrics().record_count;
        let mut replayed = simulation::new(journal.bootstrap()).unwrap();
        journal
            .visit_execution(|entries| simulation::replay(&mut replayed, entries))
            .unwrap();
        assert_eq!(
            serde_json::to_value(&replayed).unwrap(),
            serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap()
        );
        assert_eq!(replayed.portfolio_snapshot().spent_pico, "2250000000000");
        assert_eq!(replayed.snapshot().account.up_inventory_micros, 5_000_000);
        actor.observed_trades(&events).await;
        assert_eq!(actor.execution_trade_commits, 1);
        assert_eq!(
            actor.journal.as_ref().unwrap().metrics().record_count,
            sequence
        );
    }

    #[tokio::test]
    async fn failed_batch_never_publishes_candidate_finance_or_partial_operations() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = batch_config(directory.path());
        config.journal = Some(crate::journal::JournalOptions {
            max_execution_operations: 2,
            ..Default::default()
        });
        let mut actor = batch_actor(&config).await;
        let operations_before = actor
            .journal
            .as_ref()
            .unwrap()
            .metrics()
            .execution_operations;
        let before = serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap();
        actor.observed_trades(&trades(&actor, 3)).await;
        assert!(actor.fatal.as_deref().unwrap().contains("capacity"));
        assert_eq!(actor.execution_trade_commits, 0);
        assert_eq!(
            actor
                .journal
                .as_ref()
                .unwrap()
                .metrics()
                .execution_operations,
            operations_before
        );
        assert_eq!(
            before,
            serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap()
        );
        assert_eq!(actor.core.snapshot().mode, Mode::Paused);
    }

    #[tokio::test]
    async fn delayed_and_future_trade_clocks_cannot_become_fresh_during_batching() {
        let directory = tempfile::tempdir().unwrap();
        let mut actor = batch_actor(&batch_config(directory.path())).await;
        let operations_before = actor
            .journal
            .as_ref()
            .unwrap()
            .metrics()
            .execution_operations;
        let mut events = trades(&actor, 2);
        if let FeedEvent::Trade { exchange_ms, .. } = &mut events[0] {
            *exchange_ms -= 1001;
        }
        if let FeedEvent::Trade {
            observed_mono_ms, ..
        } = &mut events[1]
        {
            *observed_mono_ms += 1001;
        }
        actor.observed_trades(&events).await;
        assert_eq!(actor.execution_trade_commits, 0);
        assert_eq!(
            actor
                .journal
                .as_ref()
                .unwrap()
                .metrics()
                .execution_operations,
            operations_before
        );
    }

    #[tokio::test]
    async fn failed_batch_sync_withholds_finance_and_poisoned_writer_cannot_continue() {
        let directory = tempfile::tempdir().unwrap();
        let mut actor = batch_actor(&batch_config(directory.path())).await;
        let operations_before = actor
            .journal
            .as_ref()
            .unwrap()
            .metrics()
            .execution_operations;
        let before = serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap();
        actor.journal.as_mut().unwrap().inject_sync_failure();
        actor
            .observed_trades(&trades(&actor, FEED_BATCH_LIMIT))
            .await;
        assert!(actor.fatal.as_deref().unwrap().contains("fsync"));
        assert_eq!(actor.execution_trade_commits, 0);
        assert_eq!(
            actor
                .journal
                .as_ref()
                .unwrap()
                .metrics()
                .execution_operations,
            operations_before
        );
        assert_eq!(
            before,
            serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap()
        );
        assert!(
            actor
                .journal
                .as_ref()
                .unwrap()
                .metrics()
                .committed_prefix
                .is_none()
        );
        let result = actor.journal.as_mut().unwrap().commit_execution(
            simulation::values(&[SimulationOperation::Advance { now_ms: 10_001 }]).unwrap(),
        );
        assert!(result.unwrap_err().contains("writer failed"));
        assert_eq!(actor.core.snapshot().mode, Mode::Paused);
    }

    #[tokio::test]
    async fn batching_respects_small_supported_journal_segments_without_raising_quotas() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = batch_config(directory.path());
        config.journal = Some(crate::journal::JournalOptions {
            segment_max_bytes: 1024,
            ..Default::default()
        });
        let mut actor = batch_actor(&config).await;
        let operations_before = actor
            .journal
            .as_ref()
            .unwrap()
            .metrics()
            .execution_operations;
        assert_eq!(actor.feed_batch_limit, 1);
        for index in 0..FEED_BATCH_LIMIT {
            // Each durable write may exceed the source freshness window on a slow disk.
            // Admit a freshly observed, distinct trade for each supported one-event batch.
            let mut event = trades(&actor, 1).remove(0);
            if let FeedEvent::Trade { fingerprint, .. } = &mut event {
                *fingerprint = format!("{index:064x}");
            }
            actor.observed_trades(&[event]).await;
        }
        assert!(actor.fatal.is_none(), "{:?}", actor.fatal);
        assert_eq!(actor.execution_trade_commits, FEED_BATCH_LIMIT as u64);
        assert_eq!(
            actor.journal.as_ref().unwrap().metrics().segment_max_bytes,
            1024
        );
        assert_eq!(
            actor
                .journal
                .as_ref()
                .unwrap()
                .metrics()
                .execution_operations,
            FEED_BATCH_LIMIT + operations_before
        );
    }
    #[tokio::test(start_paused = true)]
    async fn overdue_periodic_work_does_not_starve_ready_feeds_or_override_fault_priority() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            mode: "simulation".into(),
            feed_source: crate::config::FeedSource::Public,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.path().into(),
            live_state_dir: None,
            journal_max_bytes: 1_048_576,
            journal: None,
            strategy: mm2_strategy::Rules::default(),
            live: None,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "100".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
        };
        let (tx, rx) = mpsc::channel(16);
        let (fault, observed_fault) = watch::channel(None);
        let (shutdown, control) = watch::channel(false);
        let (handle, mut actor) =
            prepare(&config, Instant::now(), rx, observed_fault, control).unwrap();
        actor.timer_work_delay = Duration::from_millis(150);
        for _ in 0..4 {
            tx.try_send(FeedEvent::Reference {
                exchange_ms: wall_ms(),
                observed_mono_ms: mono_ms(actor.start),
                price_micro: 80_000_000_000,
            })
            .unwrap();
        }
        let task = tokio::spawn(actor.run());
        tokio::task::yield_now().await;
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(150)).await;
            tokio::task::yield_now().await;
        }
        fault.send_replace(Some("journal writer stopped; reconcile on restart".into()));
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        shutdown.send_replace(true);
        task.await.unwrap();
        let final_state = handle.state.borrow();
        assert_eq!(final_state["metrics"]["feed_events"], 4);
        assert!(
            final_state["metrics"]["execution_periodic_overruns"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(final_state["fault_class"], "durability_fault");
        assert_eq!(final_state["mode"], "paused");
        assert_eq!(final_state["api_ready"], false);
        assert_eq!(final_state["command_version"], 0);
        assert_eq!(final_state["journal"]["execution_operations"], 0);
    }

    #[tokio::test]
    async fn classified_durability_fault_remains_latched_after_valid_source_input() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            mode: "shadow".into(),
            feed_source: crate::config::FeedSource::Public,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.path().into(),
            live_state_dir: None,
            journal_max_bytes: 1_048_576,
            journal: None,
            strategy: mm2_strategy::Rules::default(),
            live: None,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "100".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
        };
        let (_tx, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_shutdown, shutdown) = watch::channel(false);
        let (handle, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        actor.latch("journal writer stopped; reconcile on restart".into());
        actor
            .core
            .apply(FeedEvent::Reference {
                exchange_ms: wall_ms(),
                observed_mono_ms: mono_ms(actor.start),
                price_micro: 80_000_000_000,
            })
            .unwrap();
        actor.evaluate();
        actor.publish();
        assert_eq!(handle.state.borrow()["fault_class"], "durability_fault");
        assert_eq!(handle.state.borrow()["mode"], "paused");
        assert_eq!(handle.state.borrow()["api_ready"], false);
        assert_eq!(handle.state.borrow()["last_input_error"], Value::Null);
        assert!(handle.state.borrow()["fault"].is_string());
        let reply = actor
            .command(Command::Control {
                id: "must-stay-paused".into(),
                expected_version: 0,
                action: "resume".into(),
            })
            .await;
        assert!(reply.is_err());
        assert_eq!(actor.version, 0);
        assert!(actor.journal.as_ref().unwrap().history().is_empty());
    }

    #[tokio::test]
    async fn saturated_secondary_worker_drops_snapshot_and_cannot_overwrite_newer_control() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            mode: "shadow".into(),
            feed_source: crate::config::FeedSource::Public,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.path().into(),
            live_state_dir: None,
            journal_max_bytes: 1_048_576,
            journal: None,
            strategy: mm2_strategy::Rules::default(),
            live: None,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "100".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
        };
        let (_feeds, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_shutdown, shutdown) = watch::channel(false);
        let (handle, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        let permit = actor.publication_gate.clone().try_acquire_owned().unwrap();
        actor.last_publication = Instant::now() - Duration::from_millis(500);
        let sequence = actor.sequence;
        actor.publish_if_due();
        assert_eq!(actor.sequence, sequence);
        assert_eq!(actor.coalesced_publications, 1);
        drop(permit);
        actor.publish_if_due();
        actor.version = 7;
        actor.publish();
        let latest = actor.sequence;
        if let Some(worker) = actor.publication_task.take() {
            worker.await.unwrap();
        }
        assert_eq!(handle.state.borrow()["sequence"], latest);
        assert_eq!(handle.state.borrow()["command_version"], 7);
    }
    #[tokio::test]
    async fn empty_market_rollover_seeds_fresh_external_depth_before_new_orders() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            mode: "emulator".into(),
            feed_source: crate::config::FeedSource::Demo,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.path().into(),
            live_state_dir: None,
            journal_max_bytes: 1_048_576,
            journal: None,
            strategy: mm2_strategy::Rules::default(),
            live: None,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "100".into(),
                max_imbalance_shares: "10".into(),
                lot_shares: "5".into(),
                half_width: "0.01".into(),
                max_feed_age_ms: 1000,
            },
        };
        let (_feeds, rx) = mpsc::channel(16);
        let (_fault, fault) = watch::channel(None);
        let (_shutdown, shutdown) = watch::channel(false);
        let (_handle, mut actor) = prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        actor
            .bind_simulation_market(&format!("0x{}", "a".repeat(64)))
            .await
            .unwrap();
        let now = actor.simulation.as_ref().unwrap().now_ms();
        let inputs = [
            SimulationOperation::Book {
                now_ms: now,
                outcome: "up".into(),
                bid: 400_000,
                ask: 600_000,
                ahead_micros: 100_000_000,
            },
            SimulationOperation::Trade {
                now_ms: now,
                outcome: "up".into(),
                worst_price_micros: 1,
                quantity_micros: 100_000_000,
            },
        ];
        for input in &inputs {
            simulation::apply(actor.simulation.as_mut().unwrap(), input).unwrap();
        }
        actor
            .journal
            .as_mut()
            .unwrap()
            .commit_execution(simulation::values(&inputs).unwrap())
            .unwrap();
        actor.simulation_books = [(400_000, 600_000); 2];
        let condition = format!("0x{}", "b".repeat(64));
        actor.bind_simulation_market(&condition).await.unwrap();
        assert_eq!(actor.simulation_books, [(0, 0); 2]);
        let wall = wall_ms();
        let mono = mono_ms(actor.start);
        actor
            .core
            .set_market(Market {
                slug: "fresh".into(),
                condition_id: condition.clone(),
                up_token: "1".into(),
                down_token: "2".into(),
                window_start_ms: wall - 1000,
                window_end_ms: wall + 60_000,
                tick_micro: 1000,
            })
            .unwrap();
        for outcome in [Outcome::Up, Outcome::Down] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: wall,
                    observed_mono_ms: mono,
                    bids: vec![crate::domain::Level {
                        price_micro: 400_000,
                        quantity_micro: 100_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: 600_000,
                        quantity_micro: 100_000_000,
                    }],
                })
                .unwrap();
        }
        actor.core.control(Action::Resume).unwrap();
        actor.evaluate();
        actor.simulation_step(false).await;
        // Wide books produce no orders, so unused depth is not journal churn.
        let unused = serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap();
        assert!(unused["session"]["books"]["up"].is_null());
        for outcome in [Outcome::Up, Outcome::Down] {
            actor
                .core
                .apply(FeedEvent::Snapshot {
                    condition_id: condition.clone(),
                    outcome,
                    exchange_ms: wall_ms(),
                    observed_mono_ms: mono_ms(actor.start),
                    bids: vec![crate::domain::Level {
                        price_micro: 400_000,
                        quantity_micro: 100_000_000,
                    }],
                    asks: vec![crate::domain::Level {
                        price_micro: 410_000,
                        quantity_micro: 100_000_000,
                    }],
                })
                .unwrap();
        }
        actor.evaluate();
        actor.simulation_step(false).await;
        let restored = serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap();
        assert_eq!(
            restored["session"]["books"]["up"]["bid_levels"][0]["quantity_micros"],
            100_000_000
        );
    }
}
