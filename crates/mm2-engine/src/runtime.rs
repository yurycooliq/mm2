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
enum Operation {
    Command(Command, oneshot::Sender<Reply>),
    Market(Market, oneshot::Sender<Result<(), String>>),
    SourceStatus(&'static str, Option<String>, Option<u64>),
}

#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Operation>,
    pub state: watch::Receiver<Arc<Value>>,
}
impl Handle {
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
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Operation::Market(market, tx))
            .map_err(|_| "market control queue full or closed")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "market selection timed out")?
            .map_err(|_| "market actor stopped")?
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
    decision_count: u64,
    durations_ns: VecDeque<u64>,
    last_decision: Decision,
    condition: Option<String>,
    reset_epoch: u64,
    observation_barrier: Option<u64>,
    source_status: Value,
    awaiting_source_refresh: bool,
    simulation: Option<mm2_emulator::PaperSession>,
    simulation_origin_ms: u64,
    simulation_last_trade_ms: u64,
    simulation_error: Option<String>,
    last_publication: Instant,
    coalesced_publications: u64,
    publication_duration_ns: u64,
    simulation_books: [(u64, u64); 2],
    execution_noop_ticks: u64,
    publication_gate: Arc<tokio::sync::Semaphore>,
    publication_task: Option<tokio::task::JoinHandle<()>>,
    simulation_condition: Option<String>,
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
    awaiting_source_refresh: bool,
    fatal: Option<String>,
    committing: bool,
    closing: bool,
    feed_events: u64,
    decision_count: u64,
    durations_ns: VecDeque<u64>,
    now: u64,
    mono: u64,
    journal_bytes: u64,
    coalesced_publications: u64,
    publication_duration_ns: u64,
    execution_noop_ticks: u64,
    simulation_condition: Option<String>,
}

pub fn prepare(
    config: &Config,
    start: Instant,
    feeds: mpsc::Receiver<FeedEvent>,
    fault: watch::Receiver<Option<String>>,
    shutdown: watch::Receiver<bool>,
) -> Result<(Handle, Actor), String> {
    config.validate()?;
    let mut journal = Journal::open(
        &config.state_dir,
        config.journal_max_bytes,
        config.limits.canonical()?,
    )?;
    let mut limits = journal.bootstrap().clone();
    let mut core = Core::new(limits.core()?)?;
    // Only complete durable records are recovered; a response may have been lost after sync.
    // Feed state and proposals are always rebuilt.
    for (command, _) in journal.history() {
        apply_command(&mut core, &mut limits, command)?;
    }
    core.control(Action::Pause)?; // Pause preserves the durable Kill latch.
    let mut simulation = if config.mode == "emulator" {
        Some(simulation::new(journal.bootstrap())?)
    } else {
        None
    };
    if simulation.is_none() && !journal.execution().is_empty() {
        return Err(
            "state directory contains emulator history; use its emulator configuration".into(),
        );
    }
    let mut simulation_origin_ms = 0;
    let simulation_condition = journal
        .execution()
        .iter()
        .rev()
        .find(|entry| entry["kind"] == "market")
        .and_then(|entry| entry["condition_id"].as_str().map(str::to_owned));
    if let Some(session) = &mut simulation {
        simulation::replay(session, journal.execution())?;
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
    let (tx, commands) = mpsc::channel(config.command_capacity);
    let (state, state_rx) = watch::channel(Arc::new(json!({"api_ready":false})));
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
        decision_count: 0,
        durations_ns: VecDeque::with_capacity(1024),
        last_decision,
        condition: None,
        reset_epoch: 0,
        observation_barrier: None,
        source_status: json!({"phase":"starting","at_ms":wall_ms()}),
        awaiting_source_refresh: false,
        simulation,
        simulation_origin_ms,
        simulation_last_trade_ms: simulation_origin_ms,
        simulation_error: None,
        last_publication: Instant::now(),
        coalesced_publications: 0,
        publication_duration_ns: 0,
        simulation_books: [(0, 0); 2],
        execution_noop_ticks: 0,
        publication_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        publication_task: None,
        simulation_condition,
    };
    actor.publish();
    Ok((
        Handle {
            tx,
            state: state_rx,
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
    fn evaluate(&mut self) {
        let began = Instant::now();
        self.last_decision = self.core.evaluate(wall_ms(), mono_ms(self.start));
        self.decision_count = self.decision_count.saturating_add(1);
        if self.durations_ns.len() == 1024 {
            self.durations_ns.pop_front();
        }
        self.durations_ns
            .push_back(began.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
    }
    fn latch(&mut self, reason: String) {
        if self.fatal.is_none() {
            self.fatal = Some(reason.chars().take(256).collect());
        }
        let _ = self.core.control(Action::Pause);
        self.evaluate();
        self.publish();
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
            awaiting_source_refresh: self.awaiting_source_refresh,
            fatal: self.fatal.clone(),
            committing: self.committing,
            closing: self.closing,
            feed_events: self.feed_events,
            decision_count: self.decision_count,
            durations_ns: self.durations_ns.clone(),
            now: wall_ms(),
            mono: mono_ms(self.start),
            journal_bytes: self.journal_bytes,
            coalesced_publications: self.coalesced_publications,
            publication_duration_ns: self.publication_duration_ns,
            execution_noop_ticks: self.execution_noop_ticks,
            simulation_condition: self.simulation_condition.clone(),
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
        let mut operations = vec![SimulationOperation::Advance { now_ms: now }];
        if active && snapshot.pending_events == 0 && snapshot.orders.is_empty() {
            let minimum_cost = self
                .last_decision
                .intents
                .iter()
                .map(|intent| u128::from(intent.price_micro) * 5_000_000u128 / 1_000_000u128)
                .min();
            if minimum_cost
                .is_some_and(|cost| cost > u128::from(snapshot.account.available_micros_floor))
            {
                let _ = self.core.control(Action::Pause);
                self.evaluate();
                active = false;
                self.simulation_error =
                    Some("finite simulated collateral exhausted; session is paused".into());
            }
        }
        if !active || settling {
            operations.push(SimulationOperation::CancelAll { now_ms: now });
        }
        if active && !settling {
            let snapshot = self.core.snapshot();
            for (index, (outcome, book)) in
                [("up", &snapshot.up_book), ("down", &snapshot.down_book)]
                    .into_iter()
                    .enumerate()
            {
                if let (Some(bid), Some(ask)) = (book.best_bid_micro, book.best_ask_micro)
                    && self.simulation_books[index] != (bid, ask)
                {
                    operations.push(SimulationOperation::Book {
                        now_ms: now,
                        outcome: outcome.into(),
                        bid,
                        ask,
                        ahead_micros: 100_000_000,
                    });
                }
            }
        }
        for operation in &operations {
            if let Err(error) = simulation::apply(&mut candidate, operation) {
                self.latch(format!("simulation invariant: {error}"));
                return;
            }
        }
        if active && !settling {
            let quote = SimulationOperation::Quotes {
                now_ms: now,
                quotes: simulation::desired(&self.last_decision),
            };
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
            if now.saturating_sub(self.simulation_last_trade_ms) >= 500 {
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
        let account = session.snapshot().account;
        if self.simulation_condition.is_some()
            && (account.up_inventory_micros > 0
                || account.down_inventory_micros > 0
                || account.reserved_micros_ceil > 0)
        {
            let _ = self.core.control(Action::Pause);
            self.simulation_error=Some("emulator market changed with unsettled inventory; original contract ledger retained".into());
            return Ok(());
        }
        let operations = simulation::values(&[SimulationOperation::Market {
            condition_id: condition.into(),
        }])?;
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
                self.simulation_books = [(0, 0); 2];
                Ok(())
            }
            Err(_) => Err("market identity journal writer stopped".into()),
        }
    }
    async fn command(&mut self, mut command: Command) -> Reply {
        command.validate().map_err(|e| Failure::new("invalid", e))?;
        if let Command::Limits { limits, .. } = &mut command {
            *limits = limits.canonical().map_err(|e| Failure::new("invalid", e))?;
        }
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| Failure::new("unavailable", "journal unavailable; restart required"))?;
        if let Some(receipt) = journal
            .existing(&command)
            .map_err(|e| Failure::new("conflict", e))?
        {
            return Ok(
                json!({"receipt":receipt,"state":self.state.borrow().as_ref(),"replayed":true}),
            );
        }
        if self.fatal.is_some() || self.closing {
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
        if resets {
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
        let mut timer = tokio::time::interval(Duration::from_millis(100));
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
                        Some(Operation::Market(market, reply)) => {
                            // The source owner joins the old stream first. Drain its queued generation.
                            while self.feeds.try_recv().is_ok() {}
                            let mut result = self.core.set_market(market.clone());
                            if result.is_ok() && let Err(error)=self.bind_simulation_market(&market.condition_id).await {self.latch(error.clone());result=Err(error);}
                            if result.is_ok() { self.condition = Some(market.condition_id); self.observation_barrier = Some(mono_ms(self.start)); self.awaiting_source_refresh = false; }
                            self.evaluate(); self.publish(); let _ = reply.send(result);
                        }
                        Some(Operation::SourceStatus(phase,message,retry)) => {
                            self.source_status=json!({"phase":phase,"message":message.map(|m|m.chars().take(256).collect::<String>()),"retry_in_ms":retry,"at_ms":wall_ms()});self.publish();
                        }
                        None => break,
                    }
                }
                _ = timer.tick() => { self.evaluate(); self.simulation_step(false).await;self.publish_if_due(); }
                event = self.feeds.recv() => {
                    match event {
                        Some(event) => {
                            if self.awaiting_source_refresh { continue; }
                            let identity = match &event { FeedEvent::Snapshot { condition_id,.. } | FeedEvent::Delta { condition_id,.. } | FeedEvent::Tick { condition_id,.. } => Some(condition_id), _ => None };
                            if identity.is_some_and(|id| self.condition.as_ref() != Some(id)) { continue; }
                            let observed = match &event { FeedEvent::Snapshot { observed_mono_ms,.. } | FeedEvent::Delta { observed_mono_ms,.. } | FeedEvent::Tick { observed_mono_ms,.. } | FeedEvent::Reference { observed_mono_ms,.. } => Some(*observed_mono_ms),_=>None };
                            if self.observation_barrier.is_some_and(|barrier| observed.is_some_and(|rx|rx<=barrier)) { continue; }
                            self.feed_events = self.feed_events.saturating_add(1);
                            let _ = self.core.apply(event); self.evaluate();
                        }
                        None => { self.latch("feed owner stopped".into()); break; }
                    }
                }
            }
        }
        self.closing = true;
        let _ = self.core.control(Action::Pause);
        self.evaluate();
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
        {
            snapshot
                .intents
                .iter()
                .map(|i| {
                    json!({"outcome": outcome_name(i.outcome), "price":decimal(i.price_micro),
                "shares":decimal(i.quantity_micro), "submitted":false})
                })
                .collect()
        } else {
            Vec::new()
        };
        let now = self.now;
        let mono = self.mono;
        let feed_ready = !self.awaiting_source_refresh
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
            "decision_ns_p50":percentile(50),"decision_ns_p99":if ordered.len()>=1000 {percentile(99)} else {None},
            "measurement":"core.evaluate only; excludes transport, parsing, queue wait, simulation durability and snapshot publication"});
        Arc::new(json!({
            "schema_version":1, "boot_id":self.boot_id,"sequence":self.sequence,"published_at_ms":now,
            "mode":mode_name(snapshot.mode),"command_version":self.version,"feed_source":self.source,
            "reset_epoch":self.reset_epoch,"source_status":self.source_status,
            "awaiting_source_refresh":self.awaiting_source_refresh,
            "api_ready":api_ready,"feed_ready":feed_ready,"shadow_ready":api_ready && feed_ready && !intents.is_empty(),
            "live_eligible":false,"exact_twap_available":false,"strategy":"experimental_book_only",
            "execution_mode":if self.simulation.is_some() { "emulator" } else { "shadow" },
            "simulation":self.simulation,"simulation_error":self.simulation_error,
            "simulation_market_condition":self.simulation_condition,
            "market":market,"limits":self.limits,"shadow_intents":intents,
            "hypothetical_reservation_usd": if api_ready { decimal(snapshot.hypothetical_reservation_micro) } else { "0".into() },
            "worst_case_imbalance_shares": if api_ready { decimal(snapshot.worst_case_imbalance_micro) } else { "0".into() },
            "actual_orders":[],"wallet_balance_usd":null,"inventory":null,"pnl":null,
            "books":{"up":book_json(&snapshot.up_book,now,mono),"down":book_json(&snapshot.down_book,now,mono)},
            "reference":snapshot.reference.as_ref().map(|r| json!({"source":"binance_btcusdt", "price":decimal(r.price_micro),
                "exchange_ms":r.exchange_ms,"observed_mono_ms":r.observed_mono_ms,"oracle":false})),
            "binance_connected":snapshot.binance_connected,"fault":self.fatal,"last_input_error":snapshot.last_error,
            "stop_reasons":self.last_decision.stop_reasons.iter().map(reason_name).collect::<Vec<_>>(),
            "committing":self.committing,"shutting_down":self.closing,"journal_bytes":self.journal_bytes,
            "metrics":metrics
        }))
    }
}

fn outcome_name(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Up => "up",
        Outcome::Down => "down",
    }
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
    async fn saturated_secondary_worker_drops_snapshot_and_cannot_overwrite_newer_control() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            mode: "shadow".into(),
            feed_source: crate::config::FeedSource::Public,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: directory.path().into(),
            journal_max_bytes: 1_048_576,
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
            journal_max_bytes: 1_048_576,
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
        let restored = serde_json::to_value(actor.simulation.as_ref().unwrap()).unwrap();
        assert_eq!(
            restored["books"]["up"]["bid_levels"][0]["quantity_micros"],
            100_000_000
        );
    }
}
