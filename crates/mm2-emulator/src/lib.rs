//! Deterministic local venue approximation. Synthetic fills are never real fills.
pub use mm2_core::{
    AccountSnapshot, DesiredQuote, ExecutionStats, Order, Outcome, RiskLimits, SCALE,
};
use mm2_core::{Action, Core, Observation, Operation, OrderPhase};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
pub type Result<T> = std::result::Result<T, String>;
const MAX_EVENTS: usize = 8_192;
const MAX_AUDIT: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyConfig {
    pub post_ms: u64,
    pub cancel_ms: u64,
    pub ack_ms: u64,
    pub fill_ms: u64,
    pub timeout_ms: u64,
}
impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            post_ms: 25,
            cancel_ms: 20,
            ack_ms: 15,
            fill_ms: 30,
            timeout_ms: 500,
        }
    }
}
impl LatencyConfig {
    pub fn validate(&self) -> Result<()> {
        if [self.post_ms, self.cancel_ms, self.ack_ms, self.fill_ms]
            .iter()
            .any(|x| *x > 60_000)
            || self.timeout_ms == 0
            || self.timeout_ms > 300_000
        {
            return Err("invalid bounded emulator latency".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price_micros: u64,
    pub quantity_micros: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    pub bid: Option<u64>,
    pub ask: Option<u64>,
    pub bid_levels: Vec<Level>,
}
impl Book {
    pub fn validate(&self) -> Result<()> {
        if self.bid_levels.len() > 256
            || self.bid.is_some_and(|p| p == 0 || p >= SCALE)
            || self.ask.is_some_and(|p| p == 0 || p >= SCALE)
            || self.bid.zip(self.ask).is_some_and(|(b, a)| b >= a)
        {
            return Err("invalid bounded emulator book".into());
        }
        let mut prior = SCALE;
        for level in &self.bid_levels {
            if level.price_micros == 0
                || level.price_micros >= prior
                || level.quantity_micros > 1_000_000_000 * SCALE
                || self.bid.is_none_or(|bid| level.price_micros > bid)
            {
                return Err("bid levels must be unique descending and within best bid".into());
            }
            prior = level.price_micros;
        }
        Ok(())
    }
    fn external_at(&self, price: u64) -> u64 {
        self.bid_levels
            .iter()
            .find(|l| l.price_micros == price)
            .map_or(0, |l| l.quantity_micros)
    }
    fn consume_external(&mut self, price: u64, quantity: u64) {
        if let Some(level) = self.bid_levels.iter_mut().find(|l| l.price_micros == price) {
            level.quantity_micros = level.quantity_micros.saturating_sub(quantity);
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fault {
    DropNextPostAck,
    DropNextCancelAck,
    DuplicateNextNotice,
    RejectNextPost429 { retry_after_ms: u64 },
    RejectNextCancel429 { retry_after_ms: u64 },
    Disconnect { until_ms: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueOrder {
    pub id: u64,
    pub quote: DesiredQuote,
    pub cumulative_filled_micros: u64,
    pub remaining_micros: u64,
    pub external_queue_ahead_micros: u64,
    pub arrival_sequence: u64,
    pub terminal: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub at_ms: u64,
    pub kind: String,
    pub order_id: Option<u64>,
    pub quantity_micros: Option<u64>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueStats {
    pub accepted_posts: u64,
    pub rejected_posts: u64,
    pub cancelled_orders: u64,
    pub simulated_filled_micros: u64,
    pub external_consumed_micros: u64,
    pub dropped_notifications: u64,
    pub duplicate_notifications: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub simulated: bool,
    pub now_ms: u64,
    pub account: AccountSnapshot,
    pub orders: Vec<Order>,
    pub stats: ExecutionStats,
    pub venue_stats: VenueStats,
    pub venue_orders: Vec<VenueOrder>,
    pub pending_events: usize,
    pub recent_audit: Vec<AuditEvent>,
    pub audit_sequence: u64,
    pub audit_evicted: u64,
    pub fault: Option<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeResult {
    pub input_quantity_micros: u64,
    pub own_filled_micros: u64,
    pub external_consumed_micros: u64,
    pub unconsumed_micros: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Event {
    at_ms: u64,
    sequence: u64,
    payload: EventPayload,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EventPayload {
    Wake,
    Post {
        order: Order,
    },
    Cancel {
        order_id: u64,
    },
    Query {
        order_id: u64,
    },
    Observe {
        observation: Observation,
    },
    Reject {
        order_id: u64,
    },
    RateLimited {
        order_id: u64,
        operation: Operation,
        retry_after_ms: u64,
    },
    Timeout {
        order_id: u64,
        operation: Operation,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperSession {
    version: u32,
    core: Core,
    latency: LatencyConfig,
    now_ms: u64,
    next_sequence: u64,
    events: Vec<Event>,
    books: BTreeMap<Outcome, Book>,
    venue: BTreeMap<u64, VenueOrder>,
    faults: VecDeque<Fault>,
    disconnected_until_ms: u64,
    audit: VecDeque<AuditEvent>,
    audit_sequence: u64,
    audit_evicted: u64,
    venue_stats: VenueStats,
    fault: Option<String>,
}
impl PaperSession {
    pub fn defer_automatic_posts(&mut self) {
        self.core.defer_automatic_posts();
    }
    pub fn automatic_posts_deferred(&self) -> bool {
        self.core.automatic_posts_deferred()
    }
    pub fn new(limits: RiskLimits, latency: LatencyConfig) -> Result<Self> {
        latency.validate()?;
        Ok(Self {
            version: 1,
            core: Core::new(limits)?,
            latency,
            now_ms: 0,
            next_sequence: 0,
            events: vec![],
            books: BTreeMap::new(),
            venue: BTreeMap::new(),
            faults: VecDeque::new(),
            disconnected_until_ms: 0,
            audit: VecDeque::new(),
            audit_sequence: 0,
            audit_evicted: 0,
            venue_stats: VenueStats::default(),
            fault: None,
        })
    }
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }
    pub fn limits(&self) -> &RiskLimits {
        self.core.limits()
    }
    pub fn set_risk_limits(&mut self, now_ms: u64, limits: RiskLimits) -> Result<()> {
        self.advance(now_ms)?;
        let actions = self.core.set_risk_limits(limits, now_ms)?;
        self.dispatch(actions)
    }
    pub fn quote(&mut self, now_ms: u64, quotes: Vec<DesiredQuote>) -> Result<()> {
        self.advance(now_ms)?;
        let actions = self.core.quote(now_ms, quotes)?;
        self.dispatch(actions)
    }
    pub fn cancel_all(&mut self, now_ms: u64) -> Result<()> {
        self.advance(now_ms)?;
        let actions = self.core.cancel_all(now_ms)?;
        self.dispatch(actions)
    }
    pub fn inject(&mut self, fault: Fault) -> Result<()> {
        if self.faults.len() >= 32 {
            return Err("fault injection capacity exceeded".into());
        }
        match fault {
            Fault::Disconnect { until_ms } => {
                if until_ms < self.now_ms || until_ms.saturating_sub(self.now_ms) > 300_000 {
                    return Err("invalid disconnect horizon".into());
                }
                self.disconnected_until_ms = until_ms;
            }
            Fault::RejectNextPost429 { retry_after_ms }
            | Fault::RejectNextCancel429 { retry_after_ms }
                if retry_after_ms == 0 || retry_after_ms > 60_000 =>
            {
                return Err("invalid retry delay".into());
            }
            _ => self.faults.push_back(fault),
        }
        self.record("fault_injected", None, None);
        Ok(())
    }
    pub fn set_book(&mut self, now_ms: u64, outcome: Outcome, book: Book) -> Result<()> {
        self.set_book_inner(now_ms, outcome, book, false)
    }
    /// Public depth reductions do not prove which cancellations were ahead of us.
    pub fn set_book_conservative(
        &mut self,
        now_ms: u64,
        outcome: Outcome,
        book: Book,
    ) -> Result<()> {
        self.set_book_inner(now_ms, outcome, book, true)
    }
    fn set_book_inner(
        &mut self,
        now_ms: u64,
        outcome: Outcome,
        book: Book,
        conservative: bool,
    ) -> Result<()> {
        book.validate()?;
        self.advance(now_ms)?;
        for order in self
            .venue
            .values_mut()
            .filter(|o| !conservative && !o.terminal && o.quote.outcome == outcome)
        {
            order.external_queue_ahead_micros = order
                .external_queue_ahead_micros
                .min(book.external_at(order.quote.price_micros));
        }
        self.books.insert(outcome, book);
        self.record("book", None, None);
        Ok(())
    }
    pub fn advance(&mut self, now_ms: u64) -> Result<()> {
        if now_ms < self.now_ms {
            return Err("event-time regression".into());
        }
        // Event order is stable by logical timestamp then unique scheduling sequence.
        let mut handled = 0;
        loop {
            let next = self
                .events
                .iter()
                .enumerate()
                .filter(|(_, e)| e.at_ms <= now_ms)
                .min_by_key(|(_, e)| (e.at_ms, e.sequence))
                .map(|(i, _)| i);
            let Some(index) = next else { break };
            handled += 1;
            if handled > MAX_EVENTS * 4 {
                return self.fail("event processing budget exhausted");
            }
            let event = self.events.swap_remove(index);
            self.now_ms = event.at_ms;
            if let Err(error) = self.handle(event.payload) {
                return self.fail(&error);
            }
        }
        self.now_ms = now_ms;
        let actions = self.core.wake(now_ms)?;
        self.dispatch(actions)?;
        // Zero-latency newly scheduled work completes on this same advance call.
        if self.events.iter().any(|e| e.at_ms <= now_ms) {
            self.advance(now_ms)?;
        }
        Ok(())
    }
    pub fn trade(
        &mut self,
        now_ms: u64,
        outcome: Outcome,
        worst_price_micros: u64,
        quantity_micros: u64,
    ) -> Result<TradeResult> {
        self.trade_inner(now_ms, outcome, worst_price_micros, quantity_micros, false)
    }
    /// A public print witnesses volume at one price, not a whole taker sweep.
    pub fn trade_at_price(
        &mut self,
        now_ms: u64,
        outcome: Outcome,
        price_micros: u64,
        quantity_micros: u64,
    ) -> Result<TradeResult> {
        self.trade_inner(now_ms, outcome, price_micros, quantity_micros, true)
    }
    fn trade_inner(
        &mut self,
        now_ms: u64,
        outcome: Outcome,
        worst_price_micros: u64,
        quantity_micros: u64,
        exact_price: bool,
    ) -> Result<TradeResult> {
        if worst_price_micros == 0
            || worst_price_micros >= SCALE
            || quantity_micros == 0
            || quantity_micros > 1_000_000_000 * SCALE
        {
            return Err("invalid simulated taker trade".into());
        }
        self.advance(now_ms)?;
        let mut prices: std::collections::BTreeSet<u64> = self
            .books
            .get(&outcome)
            .map(|b| {
                b.bid_levels
                    .iter()
                    .filter(|l| {
                        if exact_price {
                            l.price_micros == worst_price_micros
                        } else {
                            l.price_micros >= worst_price_micros
                        }
                    })
                    .map(|l| l.price_micros)
                    .collect()
            })
            .unwrap_or_default();
        prices.extend(
            self.venue
                .values()
                .filter(|o| {
                    !o.terminal
                        && o.quote.outcome == outcome
                        && if exact_price {
                            o.quote.price_micros == worst_price_micros
                        } else {
                            o.quote.price_micros >= worst_price_micros
                        }
                })
                .map(|o| o.quote.price_micros),
        );
        let mut result = TradeResult {
            input_quantity_micros: quantity_micros,
            unconsumed_micros: quantity_micros,
            ..TradeResult::default()
        };
        for price in prices.into_iter().rev() {
            let mut ids: Vec<_> = self
                .venue
                .values()
                .filter(|o| {
                    !o.terminal && o.quote.outcome == outcome && o.quote.price_micros == price
                })
                .map(|o| (o.arrival_sequence, o.id))
                .collect();
            ids.sort_unstable();
            for (_, id) in ids {
                if result.unconsumed_micros == 0 {
                    break;
                }
                let ahead = self
                    .venue
                    .get(&id)
                    .expect("known order")
                    .external_queue_ahead_micros;
                let external = ahead.min(result.unconsumed_micros);
                self.consume_external(outcome, price, external);
                result.external_consumed_micros += external;
                result.unconsumed_micros -= external;
                let order = self.venue.get_mut(&id).expect("known order");
                let got = order.remaining_micros.min(result.unconsumed_micros);
                if got == 0 {
                    continue;
                }
                order.remaining_micros -= got;
                order.cumulative_filled_micros += got;
                order.terminal = order.remaining_micros == 0;
                let observation = Observation {
                    order_id: id,
                    cumulative_filled_micros: order.cumulative_filled_micros,
                    terminal: order.terminal,
                };
                result.own_filled_micros += got;
                result.unconsumed_micros -= got;
                self.venue_stats.simulated_filled_micros += got;
                self.record("venue_fill", Some(id), Some(got));
                self.notice(self.latency.fill_ms, EventPayload::Observe { observation })?;
            }
            let external = self
                .books
                .get(&outcome)
                .map_or(0, |b| b.external_at(price))
                .min(result.unconsumed_micros);
            self.consume_external(outcome, price, external);
            result.external_consumed_micros += external;
            result.unconsumed_micros -= external;
            if result.unconsumed_micros == 0 {
                break;
            }
        }
        if result.own_filled_micros + result.external_consumed_micros + result.unconsumed_micros
            != quantity_micros
        {
            return self.fail("trade volume conservation violated");
        }
        self.venue_stats.external_consumed_micros += result.external_consumed_micros;
        Ok(result)
    }
    fn consume_external(&mut self, outcome: Outcome, price: u64, quantity: u64) {
        if let Some(book) = self.books.get_mut(&outcome) {
            book.consume_external(price, quantity);
        }
        for order in self
            .venue
            .values_mut()
            .filter(|o| !o.terminal && o.quote.outcome == outcome && o.quote.price_micros == price)
        {
            order.external_queue_ahead_micros =
                order.external_queue_ahead_micros.saturating_sub(quantity);
        }
    }
    fn take_fault(&mut self, matches: impl Fn(&Fault) -> bool) -> Option<Fault> {
        let i = self.faults.iter().position(matches)?;
        self.faults.remove(i)
    }
    fn dispatch(&mut self, actions: Vec<Action>) -> Result<()> {
        for action in actions {
            match action {
                Action::Post { order } => {
                    self.record(
                        "post_requested",
                        Some(order.id),
                        Some(order.quote.quantity_micros),
                    );
                    self.schedule(
                        self.latency.timeout_ms,
                        EventPayload::Timeout {
                            order_id: order.id,
                            operation: Operation::Post,
                        },
                    )?;
                    self.schedule(self.latency.post_ms, EventPayload::Post { order })?;
                }
                Action::Cancel { order_id } => {
                    self.record("cancel_requested", Some(order_id), None);
                    self.schedule(
                        self.latency.timeout_ms,
                        EventPayload::Timeout {
                            order_id,
                            operation: Operation::Cancel,
                        },
                    )?;
                    self.schedule(self.latency.cancel_ms, EventPayload::Cancel { order_id })?;
                }
                Action::Query { order_id } => {
                    self.record("query_requested", Some(order_id), None);
                    self.schedule(self.latency.ack_ms, EventPayload::Query { order_id })?;
                }
            }
        }
        Ok(())
    }
    fn schedule(&mut self, delay: u64, payload: EventPayload) -> Result<()> {
        if self.events.len() >= MAX_EVENTS {
            return self.fail("pending venue events exceeded capacity");
        }
        let at_ms = self
            .now_ms
            .checked_add(delay)
            .ok_or("event-time overflow")?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or("event identity exhausted")?;
        self.events.push(Event {
            at_ms,
            sequence: self.next_sequence,
            payload,
        });
        Ok(())
    }
    fn notice(&mut self, delay: u64, payload: EventPayload) -> Result<()> {
        let duplicate = self
            .take_fault(|f| matches!(f, Fault::DuplicateNextNotice))
            .is_some();
        self.schedule(delay, payload.clone())?;
        if duplicate {
            self.venue_stats.duplicate_notifications += 1;
            self.schedule(delay.saturating_add(1), payload)?;
        }
        Ok(())
    }
    fn handle(&mut self, event: EventPayload) -> Result<()> {
        match event {
            EventPayload::Wake => {
                let actions = self.core.wake(self.now_ms)?;
                self.dispatch(actions)?;
            }
            EventPayload::Post { order } => {
                if self.now_ms < self.disconnected_until_ms {
                    self.record("post_transport_unknown", Some(order.id), None);
                    return Ok(());
                }
                if let Some(Fault::RejectNextPost429 { retry_after_ms }) =
                    self.take_fault(|f| matches!(f, Fault::RejectNextPost429 { .. }))
                {
                    self.notice(
                        self.latency.ack_ms,
                        EventPayload::RateLimited {
                            order_id: order.id,
                            operation: Operation::Post,
                            retry_after_ms,
                        },
                    )?;
                    return Ok(());
                }
                let allowed = self.books.get(&order.quote.outcome).is_some_and(|b| {
                    b.bid.is_some() && b.ask.is_some_and(|a| order.quote.price_micros < a)
                });
                if !allowed {
                    self.venue_stats.rejected_posts += 1;
                    self.record("post_only_rejected", Some(order.id), None);
                    self.notice(
                        self.latency.ack_ms,
                        EventPayload::Reject { order_id: order.id },
                    )?;
                    return Ok(());
                }
                self.prune_venue();
                if self.venue.len() >= mm2_core::MAX_HISTORY {
                    return self.fail("venue order history capacity exceeded");
                }
                let queue = self
                    .books
                    .get(&order.quote.outcome)
                    .map_or(0, |b| b.external_at(order.quote.price_micros));
                self.venue.insert(
                    order.id,
                    VenueOrder {
                        id: order.id,
                        quote: order.quote.clone(),
                        cumulative_filled_micros: 0,
                        remaining_micros: order.quote.quantity_micros,
                        external_queue_ahead_micros: queue,
                        arrival_sequence: self.next_sequence,
                        terminal: false,
                    },
                );
                self.venue_stats.accepted_posts += 1;
                self.record("post_accepted", Some(order.id), None);
                if self
                    .take_fault(|f| matches!(f, Fault::DropNextPostAck))
                    .is_some()
                {
                    self.venue_stats.dropped_notifications += 1;
                } else {
                    self.notice(
                        self.latency.ack_ms,
                        EventPayload::Observe {
                            observation: Observation {
                                order_id: order.id,
                                cumulative_filled_micros: 0,
                                terminal: false,
                            },
                        },
                    )?;
                }
            }
            EventPayload::Cancel { order_id } => {
                if self.now_ms < self.disconnected_until_ms {
                    self.record("cancel_transport_unknown", Some(order_id), None);
                    return Ok(());
                }
                if let Some(Fault::RejectNextCancel429 { retry_after_ms }) =
                    self.take_fault(|f| matches!(f, Fault::RejectNextCancel429 { .. }))
                {
                    self.notice(
                        self.latency.ack_ms,
                        EventPayload::RateLimited {
                            order_id,
                            operation: Operation::Cancel,
                            retry_after_ms,
                        },
                    )?;
                    return Ok(());
                }
                let cumulative = if let Some(order) = self.venue.get_mut(&order_id) {
                    if !order.terminal {
                        self.venue_stats.cancelled_orders += 1;
                    }
                    order.terminal = true;
                    order.remaining_micros = 0;
                    order.cumulative_filled_micros
                } else {
                    0
                };
                self.record("cancel_arrived", Some(order_id), None);
                if self
                    .take_fault(|f| matches!(f, Fault::DropNextCancelAck))
                    .is_some()
                {
                    self.venue_stats.dropped_notifications += 1;
                } else {
                    self.notice(
                        self.latency.ack_ms,
                        EventPayload::Observe {
                            observation: Observation {
                                order_id,
                                cumulative_filled_micros: cumulative,
                                terminal: true,
                            },
                        },
                    )?;
                }
            }
            EventPayload::Query { order_id } => {
                // An absent order cannot release collateral while the original delayed
                // post is still in transport. This local scheduler knows that boundary.
                let pending_post = self
                    .events
                    .iter()
                    .filter_map(|e| match &e.payload {
                        EventPayload::Post { order } if order.id == order_id => Some(e.at_ms),
                        _ => None,
                    })
                    .max();
                if self.now_ms < self.disconnected_until_ms || pending_post.is_some() {
                    let retry_at = pending_post.map_or(self.now_ms.saturating_add(1_000), |at| {
                        at.saturating_add(self.latency.ack_ms).saturating_add(1)
                    });
                    self.core.query_unavailable(order_id, retry_at);
                    self.schedule(retry_at.saturating_sub(self.now_ms), EventPayload::Wake)?;
                    self.record("query_unavailable", Some(order_id), None);
                    return Ok(());
                }
                let observation = self.venue.get(&order_id).map_or(
                    Observation {
                        order_id,
                        cumulative_filled_micros: 0,
                        terminal: true,
                    },
                    |o| Observation {
                        order_id,
                        cumulative_filled_micros: o.cumulative_filled_micros,
                        terminal: o.terminal,
                    },
                );
                self.record("query_reconciled", Some(order_id), None);
                let actions = self.core.observe(self.now_ms, observation)?;
                self.dispatch(actions)?;
            }
            EventPayload::Observe { observation } => {
                self.record(
                    "observation",
                    Some(observation.order_id),
                    Some(observation.cumulative_filled_micros),
                );
                let actions = self.core.observe(self.now_ms, observation)?;
                self.dispatch(actions)?;
            }
            EventPayload::Reject { order_id } => {
                let actions = self.core.rejected(self.now_ms, order_id)?;
                self.dispatch(actions)?;
            }
            EventPayload::RateLimited {
                order_id,
                operation,
                retry_after_ms,
            } => {
                self.record("rate_limited", Some(order_id), None);
                let actions =
                    self.core
                        .rate_limited(self.now_ms, order_id, operation, retry_after_ms)?;
                self.dispatch(actions)?;
                self.schedule(retry_after_ms, EventPayload::Wake)?;
            }
            EventPayload::Timeout {
                order_id,
                operation,
            } => {
                let actions = self.core.timeout(self.now_ms, order_id, operation)?;
                self.dispatch(actions)?;
            }
        }
        Ok(())
    }
    fn prune_venue(&mut self) {
        if self.venue.len() < mm2_core::MAX_HISTORY {
            return;
        }
        let ids: Vec<_> = self
            .venue
            .iter()
            .filter(|(id, o)| {
                o.terminal
                    && self
                        .core
                        .order(**id)
                        .is_none_or(|o| o.phase == OrderPhase::Closed)
            })
            .take(256)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.venue.remove(&id);
        }
    }
    fn record(&mut self, kind: &str, order_id: Option<u64>, quantity_micros: Option<u64>) {
        self.audit_sequence = self.audit_sequence.saturating_add(1);
        if self.audit.len() == MAX_AUDIT {
            self.audit.pop_front();
            self.audit_evicted += 1;
        }
        self.audit.push_back(AuditEvent {
            sequence: self.audit_sequence,
            at_ms: self.now_ms,
            kind: kind.into(),
            order_id,
            quantity_micros,
        });
    }
    fn fail<T>(&mut self, reason: &str) -> Result<T> {
        self.fault = Some(reason.to_string());
        self.core.halt(reason);
        Err(reason.into())
    }
    pub fn snapshot(&self) -> SessionSnapshot {
        let core = self.core.snapshot();
        SessionSnapshot {
            simulated: true,
            now_ms: self.now_ms,
            account: core.account,
            orders: core.orders,
            stats: core.stats,
            venue_stats: self.venue_stats.clone(),
            venue_orders: self
                .venue
                .values()
                .filter(|o| !o.terminal)
                .cloned()
                .collect(),
            pending_events: self.events.len(),
            recent_audit: self.audit.iter().cloned().collect(),
            audit_sequence: self.audit_sequence,
            audit_evicted: self.audit_evicted,
            fault: self.fault.clone().or(core.halted),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.core.validate()?;
        self.latency.validate()?;
        if self.version != 1
            || self.events.len() > MAX_EVENTS
            || self.venue.len() > mm2_core::MAX_HISTORY
            || self.faults.len() > 32
            || self.audit.len() > MAX_AUDIT
            || self.books.len() > 2
            || self.fault.as_ref().is_some_and(|s| s.len() > 512)
        {
            return Err("invalid persisted session bounds".into());
        }
        for book in self.books.values() {
            book.validate()?;
        }
        let mut seqs = std::collections::BTreeSet::new();
        for event in &self.events {
            if event.at_ms < self.now_ms
                || event.sequence > self.next_sequence
                || !seqs.insert(event.sequence)
            {
                return Err("invalid persisted event order".into());
            }
        }
        for (id, o) in &self.venue {
            if *id != o.id
                || o.cumulative_filled_micros > o.quote.quantity_micros
                || o.remaining_micros
                    != if o.terminal {
                        0
                    } else {
                        o.quote.quantity_micros - o.cumulative_filled_micros
                    }
                || o.external_queue_ahead_micros > 1_000_000_000 * SCALE
            {
                return Err("invalid persisted venue order".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tape {
    pub name: String,
    #[serde(default)]
    pub limits: RiskLimits,
    #[serde(default)]
    pub latency: LatencyConfig,
    pub steps: Vec<TapeStep>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TapeStep {
    pub at_ms: u64,
    #[serde(flatten)]
    pub operation: TapeOperation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TapeOperation {
    Book {
        outcome: Outcome,
        book: Book,
    },
    Quote {
        quotes: Vec<DesiredQuote>,
    },
    CancelAll,
    Trade {
        outcome: Outcome,
        worst_price_micros: u64,
        quantity_micros: u64,
    },
    Advance,
    Fault {
        fault: Fault,
    },
    Restart,
}
pub fn run_tape(tape: &Tape) -> Result<PaperSession> {
    if tape.steps.len() > 10_000 || tape.name.len() > 128 {
        return Err("event tape bounds exceeded".into());
    }
    let mut session = PaperSession::new(tape.limits.clone(), tape.latency.clone())?;
    for step in &tape.steps {
        match &step.operation {
            TapeOperation::Book { outcome, book } => {
                session.set_book(step.at_ms, *outcome, book.clone())?
            }
            TapeOperation::Quote { quotes } => session.quote(step.at_ms, quotes.clone())?,
            TapeOperation::CancelAll => session.cancel_all(step.at_ms)?,
            TapeOperation::Trade {
                outcome,
                worst_price_micros,
                quantity_micros,
            } => {
                session.trade(step.at_ms, *outcome, *worst_price_micros, *quantity_micros)?;
            }
            TapeOperation::Advance => session.advance(step.at_ms)?,
            TapeOperation::Fault { fault } => {
                session.advance(step.at_ms)?;
                session.inject(fault.clone())?;
            }
            TapeOperation::Restart => {
                session.advance(step.at_ms)?;
                let bytes = serde_json::to_vec(&session).map_err(|e| e.to_string())?;
                session = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                session.validate()?;
            }
        }
    }
    Ok(session)
}
