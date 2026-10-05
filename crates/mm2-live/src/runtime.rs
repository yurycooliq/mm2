use crate::{
    journal::Journal,
    model::Ledger,
    sdk::{SdkVenue, now_ms},
    *,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

#[derive(Clone)]
struct Quotes {
    quotes: Vec<LiveQuote>,
    now_ms: u64,
    sequence: u64,
}

#[cfg(test)]
#[path = "chain_runtime_tests.rs"]
mod chain_runtime_tests;
enum Command {
    Control(
        String,
        ControlAction,
        oneshot::Sender<Result<LiveControlReceipt, String>>,
    ),
    Shutdown(oneshot::Sender<()>),
}
enum ResultEvent {
    Prepared(Box<(LiveQuote, Result<PreparedOrder, String>)>),
    Posted(String, Result<PostResult, String>),
    Cancelled(Vec<String>, Result<Vec<String>, String>),
    Reconciled(u64, Result<AccountObservation, String>),
    Heartbeat(Result<(), String>),
    ChainPrepared(Box<(ChainPlan, Result<PreparedChainTransaction, String>)>),
    ChainSubmitted(Result<Option<String>, String>),
    ChainObserved(Result<ChainObservation, String>),
}
#[derive(Clone)]
pub struct LiveHandle {
    commands: mpsc::Sender<Command>,
    quotes: watch::Sender<Quotes>,
    snapshots: watch::Receiver<Arc<LiveSnapshot>>,
    settings: Arc<LiveSettings>,
    sequence: Arc<AtomicU64>,
}
impl LiveHandle {
    pub fn snapshot(&self) -> Arc<LiveSnapshot> {
        self.snapshots.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<Arc<LiveSnapshot>> {
        self.snapshots.clone()
    }
    pub fn resume_ready(&self) -> bool {
        self.snapshot().can_resume
    }
    /// Complete desired set, coalesced in a single watch slot. Does not wait for signing/network/disk.
    pub fn try_submit_quotes(
        &self,
        quotes: Vec<LiveQuote>,
        timestamp_ms: u64,
    ) -> Result<(), String> {
        if quotes.len() > self.settings.risk.max_orders || timestamp_ms.abs_diff(now_ms()) > 5000 {
            return Err("quote batch size or receive clock outside bounds".into());
        }
        if !quotes.is_empty() && self.snapshot().phase != LivePhase::Running {
            return Err("live executor is paused or stopped".into());
        }
        let mut keys = BTreeSet::new();
        for quote in &quotes {
            quote.validate(&self.settings, timestamp_ms)?;
            if !keys.insert(quote.key()) {
                return Err("duplicate desired live slot".into());
            }
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.quotes.send_replace(Quotes {
            quotes,
            now_ms: timestamp_ms,
            sequence,
        });
        Ok(())
    }
    pub async fn command(
        &self,
        command_id: String,
        action: ControlAction,
    ) -> Result<LiveControlReceipt, String> {
        if !(1..=128).contains(&command_id.len())
            || !command_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':'))
        {
            return Err("invalid bounded live command ID".into());
        }
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Control(command_id, action, tx))
            .map_err(|_| "live control queue full or closed")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "live control outcome unknown; retry original command ID")?
            .map_err(|_| "live actor stopped; recover original command ID")?
    }
    pub async fn shutdown(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Shutdown(tx))
            .map_err(|_| "live shutdown queue unavailable")?;
        tokio::time::timeout(Duration::from_secs(15), rx)
            .await
            .map_err(|_| "live shutdown timed out; outcomes remain reserved")?
            .map_err(|_| "live shutdown actor stopped".to_owned())
    }
}
pub struct LiveRuntime;
impl LiveRuntime {
    pub async fn start(settings: LiveSettings, state_dir: PathBuf) -> Result<LiveHandle, String> {
        settings.validate()?;
        let venue = SdkVenue::from_env(settings.clone()).await?;
        Self::start_with_venue(settings, state_dir, venue).await
    }
    /// Injection seam for meaningful offline transport tests. No production mock flag.
    pub async fn start_with_venue(
        settings: LiveSettings,
        state_dir: PathBuf,
        venue: Arc<dyn Venue>,
    ) -> Result<LiveHandle, String> {
        settings.validate()?;
        let open_settings = settings.clone();
        let (mut journal, mut ledger) =
            tokio::task::spawn_blocking(move || Journal::open(&state_dir, &open_settings))
                .await
                .map_err(|_| "live ledger reader stopped")??;
        if ledger.phase != LivePhase::Killed {
            ledger.phase = LivePhase::Paused;
        }
        for order in ledger
            .orders
            .values_mut()
            .filter(|o| o.phase != OrderPhase::Closed)
        {
            order.phase = OrderPhase::UnknownCancel;
            order.retry_at_ms = 0;
        }
        journal.commit("startup_paused_recovery", &ledger)?;
        let settings = Arc::new(settings);
        let (commands_tx, commands) = mpsc::channel(32);
        let (quotes_tx, quotes) = watch::channel(Quotes {
            quotes: vec![],
            now_ms: 0,
            sequence: 0,
        });
        let (events_tx, events) = mpsc::channel(256);
        let (stop_tx, stop_rx) = watch::channel(false);
        let listening_venue = venue.clone();
        let listener =
            tokio::spawn(async move { listening_venue.listen(events_tx, stop_rx).await });
        let (results_tx, results) = mpsc::channel(128);
        let initial = snapshot(&ledger, &settings, &journal, PublicationState::default());
        let (publish, snapshots) = watch::channel(Arc::new(initial));
        let sequence = Arc::new(AtomicU64::new(0));
        let actor = Actor {
            settings: settings.clone(),
            venue,
            ledger,
            journal: Some(journal),
            commands,
            quotes,
            events,
            results,
            results_tx,
            stop_tx,
            listener,
            workers: JoinSet::new(),
            desired: BTreeMap::new(),
            quote_received: None,
            quote_clock_ms: 0,
            quote_sequence: 0,
            coalesced: 0,
            account_reconciled: false,
            connected: false,
            last_account: None,
            last_heartbeat: None,
            preparing: false,
            posting: BTreeSet::new(),
            cancelling: BTreeSet::new(),
            reconciling: false,
            heartbeating: false,
            next_prepare: Instant::now(),
            next_cancel: Instant::now(),
            next_reconcile: Instant::now(),
            next_heartbeat: Instant::now(),
            next_publish: Instant::now(),
            prepare_attempts: 0,
            prepare_failures: 0,
            last_prepare_error: None,
            publish,
            shutdown: None,
            commands_open: true,
            events_open: true,
            quotes_open: true,
            chain_busy: false,
            chain_query_next: true,
            next_chain: Instant::now(),
            last_chain_error: None,
            chain_effect_epoch: 0,
            chain_last_condition: None,
        };
        tokio::spawn(actor.run());
        Ok(LiveHandle {
            commands: commands_tx,
            quotes: quotes_tx,
            snapshots,
            settings,
            sequence,
        })
    }
}
struct Actor {
    settings: Arc<LiveSettings>,
    venue: Arc<dyn Venue>,
    ledger: Ledger,
    journal: Option<Journal>,
    commands: mpsc::Receiver<Command>,
    quotes: watch::Receiver<Quotes>,
    events: mpsc::Receiver<VenueNotice>,
    results: mpsc::Receiver<ResultEvent>,
    results_tx: mpsc::Sender<ResultEvent>,
    stop_tx: watch::Sender<bool>,
    listener: tokio::task::JoinHandle<()>,
    workers: JoinSet<()>,
    desired: BTreeMap<String, LiveQuote>,
    quote_received: Option<Instant>,
    quote_clock_ms: u64,
    quote_sequence: u64,
    coalesced: u64,
    account_reconciled: bool,
    connected: bool,
    last_account: Option<u64>,
    last_heartbeat: Option<u64>,
    preparing: bool,
    posting: BTreeSet<String>,
    cancelling: BTreeSet<String>,
    reconciling: bool,
    heartbeating: bool,
    next_prepare: Instant,
    next_cancel: Instant,
    next_reconcile: Instant,
    next_heartbeat: Instant,
    next_publish: Instant,
    prepare_attempts: u64,
    prepare_failures: u64,
    last_prepare_error: Option<String>,
    publish: watch::Sender<Arc<LiveSnapshot>>,
    shutdown: Option<(Instant, oneshot::Sender<()>)>,
    commands_open: bool,
    events_open: bool,
    quotes_open: bool,
    chain_busy: bool,
    chain_query_next: bool,
    next_chain: Instant,
    last_chain_error: Option<String>,
    chain_effect_epoch: u64,
    chain_last_condition: Option<String>,
}
fn matching(actual: &LiveQuote, desired: &LiveQuote) -> bool {
    let mut a = actual.clone();
    let mut b = desired.clone();
    // SDK-normalized actual fees must not trigger replacement on every conservative caller quote.
    let valid_fee = a.fee_bps <= b.fee_bps;
    a.fee_bps = 0;
    b.fee_bps = 0;
    valid_fee && a == b
}
#[derive(Default)]
struct PublicationState {
    connected: bool,
    account: Option<u64>,
    heartbeat: Option<u64>,
    reconciled: bool,
    coalesced: u64,
}
fn available_collateral(ledger: &Ledger) -> u64 {
    let spent: u128 = ledger.session_spent_pico.parse().unwrap_or(u128::MAX);
    let spent_micro = spent.div_ceil(1_000_000).min(u128::from(u64::MAX)) as u64;
    let base = ledger
        .initial_collateral_micros
        .unwrap_or(0)
        .saturating_add(ledger.returned_collateral_micros)
        .saturating_sub(spent_micro);
    ledger
        .collateral_micros
        .min(ledger.allowance_micros)
        .min(base)
        .saturating_sub(
            ledger
                .reserved_pico()
                .div_ceil(1_000_000)
                .min(u128::from(u64::MAX)) as u64,
        )
}
fn snapshot(
    ledger: &Ledger,
    settings: &LiveSettings,
    journal: &Journal,
    health: PublicationState,
) -> LiveSnapshot {
    let PublicationState {
        connected,
        account,
        heartbeat,
        reconciled,
        coalesced,
    } = health;
    let now = now_ms();
    let healthy = reconciled
        && connected
        && account.is_some_and(|t| now.saturating_sub(t) < 5000)
        && heartbeat.is_some_and(|t| {
            now.saturating_sub(t) < settings.heartbeat_interval_ms * 3 + settings.request_timeout_ms
        })
        && ledger.fault.is_none()
        && ledger.foreign_order_ids.is_empty()
        && !matches!(ledger.phase, LivePhase::Killed | LivePhase::Faulted)
        && !ledger.orders.values().any(|o| {
            matches!(
                o.phase,
                OrderPhase::UnknownPost
                    | OrderPhase::UnknownCancel
                    | OrderPhase::Posting
                    | OrderPhase::Cancelling
            )
        });
    let resume = healthy
        && (ledger.phase == LivePhase::Running
            || ledger
                .orders
                .values()
                .all(|o| o.phase == OrderPhase::Closed));
    let reserved = ledger.reserved_pico();
    let available = available_collateral(ledger);
    LiveSnapshot {
        simulated: false,
        phase: ledger.phase,
        can_resume: resume,
        account_reconciled: reconciled,
        user_stream_connected: connected,
        last_reconciliation_ms: account,
        last_heartbeat_ms: heartbeat,
        collateral_micros: ledger.collateral_micros,
        allowance_micros: ledger.allowance_micros,
        reserved_pico: reserved.to_string(),
        available_micros: available,
        session_spent_pico: ledger.session_spent_pico.clone(),
        daily_loss_micros: ledger.daily_loss_micros,
        daily_loss_metric: "conservative_purchase_minus_confirmed_owned_returns_loss_bound".into(),
        pnl_available: false,
        orders: ledger
            .orders
            .values()
            .filter(|order| order.phase != OrderPhase::Closed)
            .chain(
                ledger
                    .orders
                    .values()
                    .rev()
                    .filter(|order| order.phase == OrderPhase::Closed)
                    .take(16),
            )
            .cloned()
            .collect(),
        tracked_order_count: ledger.orders.len(),
        active_order_count: ledger
            .orders
            .values()
            .filter(|order| order.phase != OrderPhase::Closed)
            .count(),
        positions: ledger.positions.values().cloned().collect(),
        foreign_order_ids: ledger.foreign_order_ids.clone(),
        fault: ledger.fault.clone(),
        durable_sequence: journal.sequence,
        journal_bytes: journal.bytes,
        journal_max_bytes: settings.journal_max_bytes,
        quotes_coalesced: coalesced,
        prepare_attempts: 0,
        prepare_failures: 0,
        last_prepare_error: None,
        unsupported_operations: vec!["sell".into(), "v3_positions".into()]
            .into_iter()
            .chain(if settings.chain.is_none() {
                vec![
                    "onchain_merge_disabled".into(),
                    "onchain_redemption_disabled".into(),
                ]
            } else {
                vec![]
            })
            .collect(),
        returned_collateral_micros: ledger.returned_collateral_micros,
        conservative_realized_pnl_pico: (i128::from(ledger.returned_collateral_micros) * 1_000_000
            - ledger.realized_cost_pico.parse::<i128>().unwrap_or(0))
        .to_string(),
        chain_pending_hash: ledger.chain_pending.as_ref().map(|p| p.hash.clone()),
        chain_pending_kind: ledger.chain_pending.as_ref().map(|p| {
            if p.relayer.is_some() {
                "batch"
            } else {
                "transaction"
            }
            .into()
        }),
        chain_receipts: ledger.chain_receipts.clone(),
        chain_expiries: ledger.chain_expiries.clone(),
        last_chain_error: None,
        risk_limits: settings.risk.clone(),
        session_gas_cost_wei: if ledger.session_gas_cost_wei.is_empty() {
            "0".into()
        } else {
            ledger.session_gas_cost_wei.clone()
        },
    }
}
impl Actor {
    async fn launch_chain_submit(&mut self, mut prepared: PreparedChainTransaction) {
        if let Some(relay) = &mut prepared.relayer {
            if relay.dispatched {
                return;
            }
            relay.dispatched = true;
            let mut next = self.ledger.clone();
            next.chain_pending = Some(prepared.clone());
            if self
                .persist("relayer_dispatch_before_send_once", next)
                .await
                .is_err()
            {
                return;
            }
        }
        self.chain_busy = true;
        self.chain_query_next = true;
        let venue = self.venue.clone();
        let tx = self.results_tx.clone();
        self.workers.spawn(async move {
            let result =
                tokio::time::timeout(Duration::from_secs(10), venue.submit_chain(prepared))
                    .await
                    .unwrap_or_else(|_| {
                        Err(
                    "chain broadcast response unknown; original transaction remains reserved"
                        .into(),
                )
                    });
            let _ = tx.send(ResultEvent::ChainSubmitted(result)).await;
        });
    }
    fn publication_state(&self) -> PublicationState {
        PublicationState {
            connected: self.connected,
            account: self.last_account,
            heartbeat: self.last_heartbeat,
            reconciled: self.account_reconciled,
            coalesced: self.coalesced,
        }
    }
    fn publish(&self) {
        if let Some(journal) = &self.journal {
            let mut state = snapshot(
                &self.ledger,
                &self.settings,
                journal,
                self.publication_state(),
            );
            state.prepare_attempts = self.prepare_attempts;
            state.prepare_failures = self.prepare_failures;
            state.last_prepare_error = self.last_prepare_error.clone();
            state.last_chain_error = self.last_chain_error.clone();
            self.publish.send_replace(Arc::new(state));
        }
    }
    fn healthy(&self) -> bool {
        self.account_reconciled
            && self.connected
            && self
                .last_account
                .is_some_and(|t| now_ms().saturating_sub(t) < 5000)
            && self.last_heartbeat.is_some_and(|t| {
                now_ms().saturating_sub(t)
                    < self.settings.heartbeat_interval_ms * 3 + self.settings.request_timeout_ms
            })
            && self.ledger.fault.is_none()
            && self.ledger.foreign_order_ids.is_empty()
            && !self.ledger.orders.values().any(|o| {
                matches!(
                    o.phase,
                    OrderPhase::UnknownPost | OrderPhase::UnknownCancel | OrderPhase::Cancelling
                )
            })
    }
    async fn persist(&mut self, reason: &str, next: Ledger) -> Result<(), String> {
        if self.journal.as_ref().is_none_or(Journal::is_poisoned) {
            return Err("live persistence is poisoned; checked restart required".into());
        }
        if let Err(error) = crate::journal::validate_ledger(&next, &self.settings) {
            self.ledger.phase = LivePhase::Faulted;
            self.ledger.fault = Some(error.clone());
            self.desired.clear();
            return Err(error);
        }
        let Some(mut journal) = self.journal.take() else {
            return Err("live ledger writer unavailable".into());
        };
        let reason = reason.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            let result = journal.commit(&reason, &next);
            (journal, next, result)
        })
        .await;
        match result {
            Ok((journal, next, result)) => {
                self.journal = Some(journal);
                if let Err(error) = result {
                    self.ledger.phase = LivePhase::Faulted;
                    self.ledger.fault = Some(error.clone());
                    self.desired.clear();
                    return Err(error);
                }
                self.ledger = next;
                Ok(())
            }
            Err(_) => {
                self.ledger.phase = LivePhase::Faulted;
                self.ledger.fault = Some("live ledger writer stopped".into());
                self.desired.clear();
                Err("live ledger writer stopped".into())
            }
        }
    }
    async fn fault(&mut self, message: String) {
        if self.ledger.fault.as_ref() == Some(&message) {
            return;
        }
        let mut next = self.ledger.clone();
        next.phase = LivePhase::Faulted;
        next.fault = Some(message.chars().take(256).collect());
        self.desired.clear();
        let _ = self.persist("fault", next).await;
    }
    async fn control(
        &mut self,
        id: String,
        action: ControlAction,
    ) -> Result<LiveControlReceipt, String> {
        if let Some(receipt) = self.ledger.receipts.get(&id) {
            if receipt.action != action {
                return Err("live command ID payload conflict".into());
            }
            return Ok(receipt.clone());
        }
        if self.journal.as_ref().is_none_or(Journal::is_poisoned) {
            return Err(
                "live persistence is poisoned; Reset cannot recover it; checked restart required"
                    .into(),
            );
        }
        if self.ledger.receipts.len() >= 2048 {
            return Err("live command receipt capacity reached".into());
        }
        let mut next = self.ledger.clone();
        match action {
            ControlAction::Resume => {
                if !self.publish.borrow().can_resume {
                    return Err(
                        "live resume prerequisites or cancellation reconciliation pending".into(),
                    );
                }
                next.phase = LivePhase::Running;
            }
            ControlAction::Pause => {
                if next.phase != LivePhase::Killed {
                    next.phase = LivePhase::Paused;
                }
            }
            ControlAction::Kill => next.phase = LivePhase::Killed,
            ControlAction::Reset => {
                next.phase = LivePhase::Paused;
                next.fault = None;
                self.account_reconciled = false;
                self.next_reconcile = Instant::now();
            }
        }
        if action != ControlAction::Resume {
            self.desired.clear();
        }
        let receipt = LiveControlReceipt {
            command_id: id.clone(),
            action,
            phase: next.phase,
            durable_sequence: self
                .journal
                .as_ref()
                .ok_or("live ledger writer unavailable")?
                .sequence
                + 1,
        };
        next.receipts.insert(id, receipt.clone());
        self.persist("owner_control", next).await?;
        self.publish();
        Ok(receipt)
    }
    fn quotes_changed(&mut self) {
        let batch = self.quotes.borrow_and_update().clone();
        self.coalesced += batch.sequence.saturating_sub(self.quote_sequence + 1);
        self.quote_sequence = batch.sequence;
        self.quote_received = Some(Instant::now());
        self.quote_clock_ms = batch.now_ms;
        self.desired = if self.ledger.phase == LivePhase::Running {
            batch.quotes.into_iter().map(|q| (q.key(), q)).collect()
        } else {
            BTreeMap::new()
        };
    }
    fn risk(&self, quote: &LiveQuote) -> Result<(), String> {
        if self
            .ledger
            .chain_pending
            .as_ref()
            .is_some_and(|p| p.plan.condition_id == quote.condition_id)
        {
            return Err("market inventory reserved for pending closeout".into());
        }
        let limits = &self.settings.risk;
        let active: Vec<_> = self
            .ledger
            .orders
            .values()
            .filter(|o| o.phase != OrderPhase::Closed)
            .collect();
        if active.len() >= limits.max_orders {
            return Err("live active order limit reached".into());
        }
        let commitment = quote.commitment_pico();
        let reserve = self.ledger.reserved_pico();
        let spent = self.ledger.net_spent_pico()?;
        let positions_cost = self
            .ledger
            .positions
            .values()
            .try_fold(0u128, |sum, p| {
                p.cost_pico
                    .parse::<u128>()
                    .ok()
                    .and_then(|cost| sum.checked_add(cost))
                    .ok_or("portfolio cost overflow")
            })
            .map_err(|_| "corrupt portfolio cost")?;
        if commitment + reserve + spent > u128::from(limits.max_budget_micros) * 1_000_000
            || commitment + reserve + positions_cost
                > u128::from(limits.max_gross_micros) * 1_000_000
            || commitment + reserve + u128::from(self.ledger.daily_loss_micros) * 1_000_000
                > u128::from(limits.daily_loss_limit_micros) * 1_000_000
        {
            return Err("live capital/gross/worst-loss budget exceeded".into());
        }
        if commitment.div_ceil(1_000_000) > u128::from(available_collateral(&self.ledger)) {
            return Err("insufficient reconciled available collateral/allowance".into());
        }
        let mut markets: BTreeSet<&str> = self
            .ledger
            .positions
            .values()
            .filter(|p| p.quantity_micros > 0)
            .map(|p| p.condition_id.as_str())
            .collect();
        markets.extend(active.iter().map(|o| o.quote.condition_id.as_str()));
        markets.insert(&quote.condition_id);
        if markets.len() > limits.max_markets {
            return Err("live retained market limit reached".into());
        }
        let mut signed = 0i128;
        for p in self
            .ledger
            .positions
            .values()
            .filter(|p| p.condition_id == quote.condition_id)
        {
            signed += if p.outcome == mm2_core::Outcome::Up {
                i128::from(p.quantity_micros)
            } else {
                -i128::from(p.quantity_micros)
            };
        }
        for outcome in [mm2_core::Outcome::Up, mm2_core::Outcome::Down] {
            let committed: u64 = active
                .iter()
                .filter(|o| {
                    o.quote.condition_id == quote.condition_id && o.quote.outcome == outcome
                })
                .map(|o| {
                    o.quote
                        .quantity_micros
                        .saturating_sub(o.cumulative_filled_micros)
                })
                .sum();
            let extra = if quote.outcome == outcome {
                quote.quantity_micros
            } else {
                0
            };
            let possible = (if outcome == mm2_core::Outcome::Up {
                signed
            } else {
                -signed
            }) + i128::from(committed)
                + i128::from(extra);
            if possible > i128::from(limits.max_imbalance_micros) {
                return Err("independent possible-fill market imbalance limit exceeded".into());
            }
        }
        Ok(())
    }
    async fn result(&mut self, event: ResultEvent) {
        match event {
            ResultEvent::Prepared(preparation) => {
                let (requested, result) = *preparation;
                self.preparing = false;
                let prepared = match result {
                    Ok(value) => {
                        self.last_prepare_error = None;
                        value
                    }
                    Err(error) => {
                        self.prepare_failures = self.prepare_failures.saturating_add(1);
                        self.last_prepare_error = Some(error.chars().take(256).collect());
                        self.publish();
                        return;
                    }
                };
                if self.ledger.phase != LivePhase::Running
                    || !self.healthy()
                    || !self
                        .desired
                        .get(&requested.key())
                        .is_some_and(|q| matching(&requested, q))
                    || self.quote_received.is_none_or(|t| {
                        t.elapsed().as_millis() > u128::from(self.settings.quote_ttl_ms)
                    })
                {
                    return;
                }
                if !matching(&prepared.quote, &requested) {
                    self.fault("prepared identity differs from desired intent".into())
                        .await;
                    return;
                }
                if let Err(error) = self.risk(&prepared.quote) {
                    self.prepare_failures = self.prepare_failures.saturating_add(1);
                    self.last_prepare_error = Some(
                        format!("risk admission: {error}")
                            .chars()
                            .take(256)
                            .collect(),
                    );
                    self.publish();
                    return;
                }
                let mut next = self.ledger.clone();
                next.collect_terminal_diagnostics();
                if let Err(error) = next.remember_market(&prepared.quote) {
                    self.fault(error).await;
                    return;
                }
                // Only terminal diagnostics may be pruned; active uncertainty is never evicted.
                if next.orders.len() >= 256 {
                    let keys: Vec<_> = next
                        .orders
                        .values()
                        .filter(|o| o.phase == OrderPhase::Closed)
                        .take(128)
                        .map(|o| o.order_id.clone())
                        .collect();
                    for key in keys {
                        next.orders.remove(&key);
                        next.signed_intents.remove(&key);
                    }
                }
                if next.orders.len() >= 256 || next.orders.contains_key(&prepared.order_id) {
                    self.fault("signed identity/order capacity collision".into())
                        .await;
                    return;
                }
                let now = now_ms();
                let id = prepared.order_id.clone();
                let position_key = format!(
                    "{}:{}",
                    prepared.quote.condition_id, prepared.quote.token_id
                );
                // Freeze the baseline before the external effect, not lazily after account data can lead a fill query.
                if !next.position_floor.contains_key(&position_key) {
                    let baseline =
                        next.positions
                            .get(&position_key)
                            .cloned()
                            .unwrap_or(LivePosition {
                                condition_id: prepared.quote.condition_id.clone(),
                                token_id: prepared.quote.token_id.clone(),
                                outcome: prepared.quote.outcome,
                                quantity_micros: 0,
                                cost_pico: "0".into(),
                                redeemable: false,
                                mergeable: false,
                            });
                    next.position_floor.insert(position_key, baseline);
                }
                next.signed_intents
                    .insert(id.clone(), prepared.signed_wire.clone());
                next.orders.insert(
                    id.clone(),
                    LiveOrder {
                        order_id: id.clone(),
                        quote: prepared.quote.clone(),
                        phase: OrderPhase::Posting,
                        cumulative_filled_micros: 0,
                        created_at_ms: now,
                        last_operation_ms: now,
                        retry_at_ms: now + 500,
                    },
                );
                if self
                    .persist("signed_submission_intent", next)
                    .await
                    .is_err()
                {
                    return;
                }
                self.posting.insert(id.clone());
                let venue = self.venue.clone();
                let tx = self.results_tx.clone();
                let timeout = self.settings.request_timeout_ms;
                self.workers.spawn(async move {
                    let result =
                        tokio::time::timeout(Duration::from_millis(timeout), venue.post(prepared))
                            .await
                            .unwrap_or_else(|_| Err("post response timeout".into()));
                    let _ = tx.send(ResultEvent::Posted(id, result)).await;
                });
            }
            ResultEvent::Posted(id, result) => {
                self.posting.remove(&id);
                let mut next = self.ledger.clone();
                if let Some(order) = next.orders.get_mut(&id) {
                    match result {
                        Ok(PostResult::Accepted { order_id }) if order_id == id => {
                            if order.phase == OrderPhase::Posting {
                                order.phase = OrderPhase::Live;
                            }
                        }
                        Ok(PostResult::Rejected) if order.cumulative_filled_micros == 0 => {
                            order.phase = OrderPhase::Closed
                        }
                        _ => {
                            if order.phase != OrderPhase::Closed
                                && !matches!(
                                    order.phase,
                                    OrderPhase::Cancelling | OrderPhase::UnknownCancel
                                )
                            {
                                order.phase = OrderPhase::UnknownPost;
                            }
                        }
                    }
                    order.retry_at_ms = now_ms() + 500;
                    let _ = self.persist("post_result", next).await;
                    self.next_reconcile = Instant::now();
                }
            }
            ResultEvent::Cancelled(ids, response) => {
                let mut next = self.ledger.clone();
                for id in ids {
                    self.cancelling.remove(&id);
                    if let Some(o) = next.orders.get_mut(&id)
                        && o.phase != OrderPhase::Closed
                    {
                        o.phase = OrderPhase::UnknownCancel;
                        o.retry_at_ms = now_ms() + if response.is_ok() { 500 } else { 1000 };
                    }
                }
                let _ = self.persist("cancel_response_requires_query", next).await;
                self.next_reconcile = Instant::now();
            }
            ResultEvent::Reconciled(epoch, result) => {
                self.reconciling = false;
                if epoch != self.chain_effect_epoch {
                    self.next_reconcile = Instant::now();
                    return;
                }
                match result {
                    Ok(account) => {
                        let mut next = self.ledger.clone();
                        let day = now_ms() / 86_400_000;
                        if next.loss_day != day {
                            next.loss_day = day;
                            next.daily_loss_micros = 0;
                        }
                        let before_spent = next.session_spent_pico.parse::<u128>().unwrap_or(0);
                        for observation in account.orders {
                            if next.orders.contains_key(&observation.order_id)
                                && let Err(error) = next.observe(&observation)
                            {
                                self.fault(error).await;
                                return;
                            }
                        }
                        next.collateral_micros = account.collateral_micros;
                        next.allowance_micros = account.allowance_micros;
                        if next.initial_collateral_micros.is_none() {
                            next.initial_collateral_micros = Some(account.collateral_micros);
                        }
                        for position in account.positions {
                            let key = format!("{}:{}", position.condition_id, position.token_id);
                            let mut position = position;
                            next.position_floor
                                .entry(key.clone())
                                .or_insert_with(|| position.clone());
                            if let Some(existing) = next.positions.get(&key)
                                && existing.outcome != position.outcome
                            {
                                self.fault("portfolio outcome identity changed".into())
                                    .await;
                                return;
                            }
                            if next.positions.get(&key).is_some_and(|existing| {
                                existing.quantity_micros > position.quantity_micros
                            }) {
                                continue;
                            }
                            if let Some(existing) = next.positions.get(&key) {
                                let prior = existing.cost_pico.parse::<u128>().unwrap_or(u128::MAX);
                                let observed =
                                    position.cost_pico.parse::<u128>().unwrap_or(u128::MAX);
                                position.cost_pico = prior.max(observed).to_string();
                            }
                            next.positions.insert(key, position);
                        }
                        if next.positions.len() > 128 {
                            self.fault("bounded retained position ledger exhausted".into())
                                .await;
                            return;
                        }
                        next.foreign_order_ids = account
                            .foreign_order_ids
                            .into_iter()
                            .filter(|id| !next.orders.contains_key(id))
                            .collect();
                        let day = now_ms() / 86_400_000;
                        if next.loss_day != day {
                            next.loss_day = day;
                            next.daily_loss_micros = 0;
                        }
                        let spent = next.session_spent_pico.parse::<u128>().unwrap_or(u128::MAX);
                        next.daily_loss_micros = next.daily_loss_micros.saturating_add(
                            spent
                                .saturating_sub(before_spent)
                                .div_ceil(1_000_000)
                                .min(u128::from(u64::MAX)) as u64,
                        );
                        next.collect_terminal_diagnostics();
                        let old = serde_json::to_vec(&self.ledger).ok();
                        let new = serde_json::to_vec(&next).ok();
                        if old != new
                            && self
                                .persist("authoritative_account_reconciliation", next)
                                .await
                                .is_err()
                        {
                            return;
                        }
                        self.last_account = Some(now_ms());
                        self.account_reconciled = true;
                        if !self.ledger.foreign_order_ids.is_empty() {
                            self.desired.clear();
                            if self.ledger.phase == LivePhase::Running {
                                let mut next = self.ledger.clone();
                                next.phase = LivePhase::Paused;
                                let _ = self.persist("foreign_orders_pause", next).await;
                            }
                        }
                    }
                    Err(_) => {
                        self.account_reconciled = false;
                        self.desired.clear();
                    }
                }
            }
            ResultEvent::Heartbeat(result) => {
                self.heartbeating = false;
                if result.is_ok() {
                    self.last_heartbeat = Some(now_ms());
                } else {
                    self.last_heartbeat = None;
                    self.desired.clear();
                }
            }
            ResultEvent::ChainPrepared(preparation) => {
                self.chain_busy = false;
                self.next_chain = Instant::now() + Duration::from_secs(5);
                let (plan, result) = *preparation;
                let prepared = match result {
                    Ok(value) => value,
                    Err(error) => {
                        self.last_chain_error = Some(error.chars().take(256).collect());
                        self.publish();
                        return;
                    }
                };
                if self.ledger.phase != LivePhase::Running || !self.healthy() {
                    return;
                }
                if prepared.plan.id != plan.id
                    || prepared.plan.condition_id != plan.condition_id
                    || prepared.plan.operation != plan.operation
                    || plan.token_ids.iter().any(|requested| {
                        !requested.is_empty() && !prepared.plan.token_ids.contains(requested)
                    })
                {
                    self.fault("prepared closeout differs from requested intent".into())
                        .await;
                    return;
                }
                let Some(chain_settings) = &self.settings.chain else {
                    return;
                };
                if let Err(error) =
                    crate::chain::validate_prepared(&self.settings, chain_settings, &prepared)
                        .and_then(|()| self.ledger.admit_chain(&prepared))
                {
                    self.last_chain_error = Some(error.chars().take(256).collect());
                    self.publish();
                    return;
                }
                let gas = self
                    .ledger
                    .session_gas_cost_wei
                    .parse::<u128>()
                    .unwrap_or(0);
                let reserve = prepared
                    .maximum_gas_cost_wei
                    .parse::<u128>()
                    .unwrap_or(u128::MAX);
                let cap = chain_settings
                    .max_session_gas_cost_wei
                    .parse::<u128>()
                    .unwrap_or(0);
                if gas.checked_add(reserve).is_none_or(|sum| sum > cap) {
                    self.fault("session Polygon gas budget reached".into())
                        .await;
                    return;
                }
                let mut next = self.ledger.clone();
                next.chain_pending = Some(prepared.clone());
                if self
                    .persist("signed_chain_intent_before_broadcast", next)
                    .await
                    .is_err()
                {
                    return;
                }
                self.last_chain_error = None;
                self.launch_chain_submit(prepared).await;
            }
            ResultEvent::ChainSubmitted(result) => {
                self.chain_busy = false;
                self.chain_query_next = true;
                self.next_chain = Instant::now() + Duration::from_secs(1);
                match result {
                    Ok(Some(id)) => {
                        if id.is_empty()
                            || id.len() > 128
                            || !id
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                        {
                            self.fault(
                                "invalid relayer acknowledgement identity; batch remains reserved"
                                    .into(),
                            )
                            .await;
                            return;
                        }
                        let mut next = self.ledger.clone();
                        let Some(relay) =
                            next.chain_pending.as_mut().and_then(|p| p.relayer.as_mut())
                        else {
                            self.fault("unexpected relayer acknowledgement".into())
                                .await;
                            return;
                        };
                        if !relay.dispatched
                            || relay.request_id.as_ref().is_some_and(|prior| prior != &id)
                        {
                            self.fault(
                                "relayer acknowledgement differs from durable dispatch".into(),
                            )
                            .await;
                            return;
                        }
                        relay.request_id = Some(id);
                        let _ = self.persist("relayer_request_acknowledged", next).await;
                    }
                    Ok(None) => {}
                    Err(error) => self.last_chain_error = Some(error.chars().take(256).collect()),
                }
            }
            ResultEvent::ChainObserved(result) => {
                self.chain_busy = false;
                self.next_chain = Instant::now() + Duration::from_secs(5);
                match result {
                    Ok(ChainObservation::Finalized(receipt)) => {
                        let success = receipt.success;
                        let mut next = self.ledger.clone();
                        if let Err(error) = next.apply_chain_receipt(&receipt) {
                            self.fault(error).await;
                            return;
                        }
                        if !success {
                            next.phase = LivePhase::Faulted;
                            next.fault = Some(
                                "finalized closeout failed; inspect receipt before Reset".into(),
                            );
                            self.desired.clear();
                        }
                        if self
                            .persist("finalized_chain_receipt_capital_reconciliation", next)
                            .await
                            .is_ok()
                        {
                            self.chain_effect_epoch = self.chain_effect_epoch.saturating_add(1);
                            self.account_reconciled = false;
                            self.next_reconcile = Instant::now();
                            self.last_chain_error = None;
                        }
                        self.chain_query_next = true;
                    }
                    Ok(ChainObservation::Expired(expiry)) => {
                        let mut next = self.ledger.clone();
                        if let Err(error) = next.apply_chain_expiry(&expiry) {
                            self.fault(error).await;
                            return;
                        }
                        next.phase = LivePhase::Faulted;
                        next.fault = Some("signed DepositWallet batch expired without execution; inspect proof before Reset".into());
                        self.desired.clear();
                        if self
                            .persist("expired_relayer_batch_without_economic_credit", next)
                            .await
                            .is_ok()
                        {
                            self.chain_effect_epoch = self.chain_effect_epoch.saturating_add(1);
                            self.account_reconciled = false;
                            self.next_reconcile = Instant::now();
                            self.last_chain_error = None;
                        }
                        self.chain_query_next = true;
                    }
                    Ok(ChainObservation::Unknown) => {
                        self.chain_query_next = false;
                    }
                    Err(error) => {
                        self.chain_query_next = true;
                        self.last_chain_error = Some(error.chars().take(256).collect());
                    }
                }
            }
        }
        self.publish();
    }
    async fn pulse(&mut self) {
        let now = now_ms();
        let instant = Instant::now();
        if self.settings.chain.is_some() && !self.chain_busy && instant >= self.next_chain {
            if let Some(prepared) = self.ledger.chain_pending.clone() {
                if !self.chain_query_next
                    && self.ledger.phase == LivePhase::Running
                    && self.healthy()
                    && prepared.relayer.as_ref().is_none_or(|r| !r.dispatched)
                {
                    self.launch_chain_submit(prepared).await;
                } else {
                    self.chain_busy = true;
                    let venue = self.venue.clone();
                    let tx = self.results_tx.clone();
                    self.workers.spawn(async move {
                        let result = tokio::time::timeout(
                            Duration::from_secs(30),
                            venue.lookup_chain(prepared),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            Err("chain lookup timed out; transaction remains reserved".into())
                        });
                        let _ = tx.send(ResultEvent::ChainObserved(result)).await;
                    });
                }
            } else if self.ledger.phase == LivePhase::Running
                && self.healthy()
                && let Some(plan) = self
                    .ledger
                    .closeout_candidate(now, self.chain_last_condition.as_deref())
            {
                self.chain_busy = true;
                self.chain_last_condition = Some(plan.condition_id.clone());
                let venue = self.venue.clone();
                let tx = self.results_tx.clone();
                self.workers.spawn(async move {
                    let result = tokio::time::timeout(
                        Duration::from_secs(30),
                        venue.prepare_chain(plan.clone()),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err("chain preparation timed out; no transaction broadcast".into())
                    });
                    let _ = tx
                        .send(ResultEvent::ChainPrepared(Box::new((plan, result))))
                        .await;
                });
            }
        }
        if self
            .quote_received
            .is_none_or(|t| t.elapsed().as_millis() > u128::from(self.settings.quote_ttl_ms))
            || self.quote_clock_ms.abs_diff(now) > self.settings.quote_ttl_ms + 2000
            || self.ledger.phase != LivePhase::Running
            || !self.healthy()
        {
            self.desired.clear();
        }
        self.desired
            .retain(|_, quote| quote.window_end_ms > now.saturating_add(1000));
        // Dedicated cancel capacity is not consumed by post preparation, posts or account requests.
        if instant >= self.next_cancel && self.cancelling.is_empty() {
            let ids: Vec<_> = self
                .ledger
                .orders
                .values()
                .filter(|o| {
                    o.phase != OrderPhase::Closed
                        && o.retry_at_ms <= now
                        && (!self
                            .desired
                            .get(&o.quote.key())
                            .is_some_and(|q| matching(&o.quote, q))
                            || matches!(
                                o.phase,
                                OrderPhase::UnknownPost
                                    | OrderPhase::UnknownCancel
                                    | OrderPhase::Cancelling
                            ))
                })
                .take(64)
                .map(|o| o.order_id.clone())
                .collect();
            if !ids.is_empty() {
                let mut next = self.ledger.clone();
                for id in &ids {
                    if let Some(o) = next.orders.get_mut(id) {
                        o.phase = OrderPhase::Cancelling;
                        o.last_operation_ms = now;
                        o.retry_at_ms = now + 500;
                    }
                }
                if self.persist("cancel_intent", next).await.is_ok() {
                    self.cancelling.extend(ids.iter().cloned());
                    self.next_cancel = instant + Duration::from_millis(250);
                    let venue = self.venue.clone();
                    let tx = self.results_tx.clone();
                    let timeout = self.settings.request_timeout_ms;
                    self.workers.spawn(async move {
                        let result = tokio::time::timeout(
                            Duration::from_millis(timeout),
                            venue.cancel(ids.clone()),
                        )
                        .await
                        .unwrap_or_else(|_| Err("cancel response timeout".into()));
                        let _ = tx.send(ResultEvent::Cancelled(ids, result)).await;
                    });
                }
            }
        }
        if !self.reconciling && instant >= self.next_reconcile {
            self.reconciling = true;
            self.next_reconcile = instant + Duration::from_secs(1);
            let orders = self.ledger.orders.values().cloned().collect();
            let venue = self.venue.clone();
            let tx = self.results_tx.clone();
            let timeout = self.settings.request_timeout_ms * 4;
            let epoch = self.chain_effect_epoch;
            self.workers.spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_millis(timeout), venue.reconcile(orders))
                        .await
                        .unwrap_or_else(|_| Err("account reconciliation timeout".into()));
                let _ = tx.send(ResultEvent::Reconciled(epoch, result)).await;
            });
        }
        // A persistence failure must not keep venue orders alive indefinitely with heartbeats.
        if !self.heartbeating && self.ledger.fault.is_none() && instant >= self.next_heartbeat {
            self.heartbeating = true;
            self.next_heartbeat =
                instant + Duration::from_millis(self.settings.heartbeat_interval_ms);
            let venue = self.venue.clone();
            let tx = self.results_tx.clone();
            let timeout = self.settings.request_timeout_ms;
            self.workers.spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_millis(timeout), venue.heartbeat())
                        .await
                        .unwrap_or_else(|_| Err("heartbeat timeout".into()));
                let _ = tx.send(ResultEvent::Heartbeat(result)).await;
            });
        }
        if !self.preparing
            && self.posting.is_empty()
            && self.cancelling.is_empty()
            && self.healthy()
            && self.ledger.phase == LivePhase::Running
            && instant >= self.next_prepare
        {
            let quote = self
                .desired
                .values()
                .find(|q| {
                    !self
                        .ledger
                        .orders
                        .values()
                        .any(|o| o.phase != OrderPhase::Closed && o.quote.key() == q.key())
                        && {
                            let mut optimistic = (**q).clone();
                            optimistic.fee_bps = 0;
                            self.risk(&optimistic).is_ok()
                        }
                })
                .cloned();
            if let Some(quote) = quote {
                self.preparing = true;
                self.prepare_attempts = self.prepare_attempts.saturating_add(1);
                self.next_prepare = instant + Duration::from_millis(250);
                let venue = self.venue.clone();
                let tx = self.results_tx.clone();
                let timeout = self.settings.request_timeout_ms;
                self.workers.spawn(async move {
                    let result = tokio::time::timeout(
                        Duration::from_millis(timeout),
                        venue.prepare(quote.clone()),
                    )
                    .await
                    .unwrap_or_else(|_| Err("order preparation timeout".into()));
                    let _ = tx
                        .send(ResultEvent::Prepared(Box::new((quote, result))))
                        .await;
                });
            }
        }
        if instant >= self.next_publish {
            self.publish();
            self.next_publish = instant + Duration::from_millis(250);
        }
    }
    async fn run(mut self) {
        let mut timer = tokio::time::interval(Duration::from_millis(50));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = self.commands.recv(), if self.commands_open => match command {
                    Some(Command::Control(id, action, tx)) => { let result = self.control(id, action).await; let _ = tx.send(result); }
                    Some(Command::Shutdown(tx)) => { self.desired.clear(); let mut next = self.ledger.clone(); if next.phase != LivePhase::Killed { next.phase = LivePhase::Paused; }
                        let _ = self.persist("shutdown_pause", next).await; self.shutdown = Some((Instant::now() + Duration::from_secs(10), tx)); }
                    None => { self.commands_open = false; self.desired.clear(); let mut next = self.ledger.clone(); if next.phase != LivePhase::Killed { next.phase = LivePhase::Paused; }
                        let _ = self.persist("owner_handle_dropped_pause", next).await;
                        if self.shutdown.is_none() { let (tx, _) = oneshot::channel(); self.shutdown = Some((Instant::now() + Duration::from_secs(10), tx)); } }
                },
                event = self.events.recv(), if self.events_open => match event {
                    Some(VenueNotice::Connected(value)) => {
                        self.connected = value; self.account_reconciled = false; self.next_reconcile = Instant::now();
                        if !value { self.desired.clear(); if self.ledger.phase == LivePhase::Running {
                            let mut next = self.ledger.clone(); next.phase = LivePhase::Paused; let _ = self.persist("private_disconnect_pause", next).await;
                        } }
                        self.publish();
                    }
                    Some(VenueNotice::OrderChanged(_)) | Some(VenueNotice::AccountChanged) => { self.next_reconcile = Instant::now(); }
                    Some(VenueNotice::Fault(error)) => self.fault(error).await,
                    None => { self.events_open = false; self.connected = false; self.fault("private stream task stopped".into()).await; }
                },
                result = self.results.recv() => { if let Some(result) = result { self.result(result).await; } },
                changed = self.quotes.changed(), if self.quotes_open => { if changed.is_ok() { self.quotes_changed(); } else { self.quotes_open = false; self.desired.clear(); } },
                _ = timer.tick() => self.pulse().await,
                joined = self.workers.join_next(), if !self.workers.is_empty() => { if joined.is_some_and(|r| r.is_err()) { self.fault("bounded live network worker stopped".into()).await; } }
            }
            if self.shutdown.as_ref().is_some_and(|(deadline, _)| {
                Instant::now() >= *deadline
                    || self
                        .ledger
                        .orders
                        .values()
                        .all(|o| o.phase == OrderPhase::Closed)
            }) {
                break;
            }
        }
        self.stop_tx.send_replace(true);
        self.listener.abort();
        let _ = (&mut self.listener).await;
        self.workers.abort_all();
        while self.workers.join_next().await.is_some() {}
        self.publish();
        if let Some((_, tx)) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
