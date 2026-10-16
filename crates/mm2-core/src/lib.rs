//! Deterministic buy-only execution lifecycle. No I/O, clocks, floats or UI.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCALE: u64 = 1_000_000;
pub const MAX_HISTORY: usize = 2_048;
pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Up,
    Down,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredQuote {
    pub slot: String,
    pub outcome: Outcome,
    pub price_micros: u64,
    pub quantity_micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskLimits {
    pub initial_cash_micros: u64,
    pub max_imbalance_micros: u64,
    pub min_order_micros: u64,
    pub tick_micros: u64,
    pub max_orders: usize,
}
impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            initial_cash_micros: 100 * SCALE,
            max_imbalance_micros: 20 * SCALE,
            min_order_micros: 5 * SCALE,
            tick_micros: 10_000,
            max_orders: 16,
        }
    }
}
impl RiskLimits {
    pub fn validate(&self) -> Result<()> {
        if self.initial_cash_micros == 0
            || self.initial_cash_micros > 1_000_000_000 * SCALE
            || self.max_imbalance_micros == 0
            || self.max_imbalance_micros > 1_000_000_000 * SCALE
            || self.min_order_micros < 5 * SCALE
            || self.min_order_micros > self.max_imbalance_micros
            || ![100_000, 10_000, 5_000, 2_500, 1_000, 100].contains(&self.tick_micros)
            || !(1..=64).contains(&self.max_orders)
        {
            return Err("invalid finite execution limits".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderPhase {
    Posting,
    Live,
    Cancelling,
    UnknownPost,
    UnknownCancel,
    Closed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Post,
    Cancel,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quote: DesiredQuote,
    pub requested_quantity_micros: u64,
    pub phase: OrderPhase,
    pub cumulative_filled_micros: u64,
    /// All shares possibly still executable or already executed but unreported.
    pub reserved_quantity_micros: u64,
    pub retry_at_ms: u64,
}
fn matches_desired(order: &Order, desired: Option<&DesiredQuote>) -> bool {
    desired.is_some_and(|q| {
        q.slot == order.quote.slot
            && q.outcome == order.quote.outcome
            && q.price_micros == order.quote.price_micros
            && q.quantity_micros == order.requested_quantity_micros
    })
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    Post { order: Order },
    Cancel { order_id: u64 },
    Query { order_id: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub order_id: u64,
    pub cumulative_filled_micros: u64,
    pub terminal: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub submitted_posts: u64,
    pub confirmed_posts: u64,
    pub confirmed_cancels: u64,
    pub rejected_posts: u64,
    pub risk_rejections: u64,
    pub rate_limits: u64,
    pub unknown_outcomes: u64,
    pub queries: u64,
    pub fill_updates: u64,
    pub duplicate_observations: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub initial_cash_micros: u64,
    pub spent_micros_ceil: u64,
    pub reserved_micros_ceil: u64,
    pub available_micros_floor: u64,
    /// Exact 1e-12 USD amounts as strings, avoiding float/JS precision loss.
    pub spent_pico: String,
    /// Exact cumulative BUY fill costs by outcome. Legacy checkpoints retain unknown.
    #[serde(default)]
    pub outcome_spent_pico: Option<[String; 2]>,
    pub reserved_pico: String,
    pub available_pico: String,
    pub up_inventory_micros: u64,
    pub down_inventory_micros: u64,
    pub worst_up_imbalance_micros: i64,
    pub worst_down_imbalance_micros: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreSnapshot {
    pub account: AccountSnapshot,
    pub orders: Vec<Order>,
    pub stats: ExecutionStats,
    pub halted: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Core {
    limits: RiskLimits,
    next_id: u64,
    orders: BTreeMap<u64, Order>,
    desired: BTreeMap<String, DesiredQuote>,
    slots: BTreeMap<String, u64>,
    cooldowns: BTreeMap<String, u64>,
    spent_pico: u128,
    #[serde(default)]
    outcome_spent_pico: Option<[u128; 2]>,
    inventory: [u64; 2],
    stats: ExecutionStats,
    halted: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    defer_automatic_posts: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
impl Core {
    pub fn new(limits: RiskLimits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            next_id: 0,
            orders: BTreeMap::new(),
            desired: BTreeMap::new(),
            slots: BTreeMap::new(),
            cooldowns: BTreeMap::new(),
            spent_pico: 0,
            outcome_spent_pico: Some([0, 0]),
            inventory: [0, 0],
            stats: ExecutionStats::default(),
            halted: None,
            defer_automatic_posts: false,
        })
    }
    pub fn limits(&self) -> &RiskLimits {
        &self.limits
    }
    /// Paper V2 must reauthorize risk after fills/cancels before another post.
    pub fn defer_automatic_posts(&mut self) {
        self.defer_automatic_posts = true;
    }
    pub fn automatic_posts_deferred(&self) -> bool {
        self.defer_automatic_posts
    }
    pub fn order(&self, id: u64) -> Option<&Order> {
        self.orders.get(&id)
    }
    pub fn active_orders(&self) -> impl Iterator<Item = &Order> {
        self.orders
            .values()
            .filter(|o| o.phase != OrderPhase::Closed)
    }
    pub fn halt(&mut self, reason: impl Into<String>) {
        self.halted = Some(reason.into());
        self.desired.clear();
    }
    pub fn set_risk_limits(&mut self, limits: RiskLimits, now_ms: u64) -> Result<Vec<Action>> {
        limits.validate()?;
        if limits.initial_cash_micros != self.limits.initial_cash_micros {
            return Err("initial collateral cannot change in an existing session".into());
        }
        if self.active_orders().next().is_some()
            && (limits.tick_micros != self.limits.tick_micros
                || limits.min_order_micros != self.limits.min_order_micros)
        {
            return Err("cancel and settle orders before changing tick/minimum".into());
        }
        self.limits = limits;
        self.desired.clear();
        self.reconcile(now_ms)
    }
    pub fn quote(&mut self, now_ms: u64, quotes: Vec<DesiredQuote>) -> Result<Vec<Action>> {
        if quotes.len() > self.limits.max_orders {
            return Err("desired quote capacity exceeded".into());
        }
        let mut desired = BTreeMap::new();
        for quote in quotes {
            if quote.slot.is_empty()
                || quote.slot.len() > 64
                || !quote
                    .slot
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_:.".contains(&b))
                || quote.price_micros == 0
                || quote.price_micros >= SCALE
                || quote.price_micros % self.limits.tick_micros != 0
                || quote.quantity_micros < self.limits.min_order_micros
                || quote.quantity_micros > 1_000_000_000 * SCALE
            {
                return Err("invalid passive quote".into());
            }
            if desired.insert(quote.slot.clone(), quote).is_some() {
                return Err("duplicate desired slot".into());
            }
        }
        if self.halted.is_some() && !desired.is_empty() {
            return Err("execution is halted".into());
        }
        self.desired = desired;
        self.reconcile_inner(now_ms, true)
    }
    pub fn cancel_all(&mut self, now_ms: u64) -> Result<Vec<Action>> {
        self.desired.clear();
        self.reconcile(now_ms)
    }
    pub fn wake(&mut self, now_ms: u64) -> Result<Vec<Action>> {
        self.reconcile(now_ms)
    }
    pub fn timeout(&mut self, now_ms: u64, id: u64, operation: Operation) -> Result<Vec<Action>> {
        let Some(order) = self.orders.get_mut(&id) else {
            return Ok(vec![]);
        };
        let expected = match operation {
            Operation::Post => OrderPhase::Posting,
            Operation::Cancel => OrderPhase::Cancelling,
        };
        if order.phase != expected {
            return Ok(vec![]);
        }
        order.phase = match operation {
            Operation::Post => OrderPhase::UnknownPost,
            Operation::Cancel => OrderPhase::UnknownCancel,
        };
        order.retry_at_ms = now_ms;
        self.stats.unknown_outcomes += 1;
        self.reconcile(now_ms)
    }
    pub fn query_unavailable(&mut self, id: u64, retry_at_ms: u64) {
        if let Some(o) = self.orders.get_mut(&id) {
            o.retry_at_ms = retry_at_ms;
        }
    }
    pub fn rate_limited(
        &mut self,
        now_ms: u64,
        id: u64,
        operation: Operation,
        retry_after_ms: u64,
    ) -> Result<Vec<Action>> {
        let Some(o) = self.orders.get_mut(&id) else {
            return Ok(vec![]);
        };
        if o.phase == OrderPhase::Closed {
            return Ok(vec![]);
        }
        self.stats.rate_limits += 1;
        let retry = now_ms
            .checked_add(retry_after_ms)
            .ok_or("retry time overflow")?;
        match operation {
            Operation::Post => {
                o.phase = OrderPhase::Closed;
                o.reserved_quantity_micros = 0;
                self.slots.remove(&o.quote.slot);
                self.cooldowns.insert(o.quote.slot.clone(), retry);
            }
            Operation::Cancel => {
                o.phase = OrderPhase::Live;
                o.retry_at_ms = retry;
            }
        }
        self.reconcile(now_ms)
    }
    pub fn rejected(&mut self, now_ms: u64, id: u64) -> Result<Vec<Action>> {
        let Some(o) = self.orders.get_mut(&id) else {
            return Ok(vec![]);
        };
        if o.phase == OrderPhase::Closed {
            self.stats.duplicate_observations += 1;
            return Ok(vec![]);
        }
        if o.cumulative_filled_micros != 0 {
            return Err("rejection contradicts fills".into());
        }
        o.phase = OrderPhase::Closed;
        o.reserved_quantity_micros = 0;
        self.slots.remove(&o.quote.slot);
        // A stale crossed quote is not reposted in a busy loop. A new strategy quote is required.
        if matches_desired(o, self.desired.get(&o.quote.slot)) {
            self.desired.remove(&o.quote.slot);
        }
        self.stats.rejected_posts += 1;
        self.reconcile(now_ms)
    }
    pub fn observe(&mut self, now_ms: u64, observation: Observation) -> Result<Vec<Action>> {
        let Some(o) = self.orders.get_mut(&observation.order_id) else {
            return Ok(vec![]);
        };
        if observation.cumulative_filled_micros > o.quote.quantity_micros {
            return Err("venue overfilled immutable order quantity".into());
        }
        if o.phase == OrderPhase::Closed
            && observation.cumulative_filled_micros > o.cumulative_filled_micros
        {
            return Err("fill contradicts terminal reconciliation".into());
        }
        if !observation.terminal && observation.cumulative_filled_micros == o.quote.quantity_micros
        {
            return Err("fully filled order must be terminal".into());
        }
        if observation.cumulative_filled_micros < o.cumulative_filled_micros
            || o.phase == OrderPhase::Closed
        {
            self.stats.duplicate_observations += 1;
            return Ok(vec![]);
        }
        let delta = observation.cumulative_filled_micros - o.cumulative_filled_micros;
        if delta > 0 {
            let cost = u128::from(delta) * u128::from(o.quote.price_micros);
            self.spent_pico += cost;
            let index = if o.quote.outcome == Outcome::Up { 0 } else { 1 };
            if let Some(costs) = &mut self.outcome_spent_pico {
                costs[index] += cost;
            }
            self.inventory[index] = self.inventory[index]
                .checked_add(delta)
                .ok_or("inventory overflow")?;
            self.stats.fill_updates += 1;
        }
        o.cumulative_filled_micros = observation.cumulative_filled_micros;
        o.reserved_quantity_micros = o.quote.quantity_micros - o.cumulative_filled_micros;
        if observation.terminal {
            if matches!(o.phase, OrderPhase::Cancelling | OrderPhase::UnknownCancel) {
                self.stats.confirmed_cancels += 1;
            }
            o.phase = OrderPhase::Closed;
            o.reserved_quantity_micros = 0;
            self.slots.remove(&o.quote.slot);
            // Completion consumes this intent. A changed/coalesced successor remains desired.
            if o.cumulative_filled_micros == o.quote.quantity_micros
                && matches_desired(o, self.desired.get(&o.quote.slot))
            {
                self.desired.remove(&o.quote.slot);
            }
        } else if matches!(o.phase, OrderPhase::Posting | OrderPhase::UnknownPost) {
            o.phase = OrderPhase::Live;
            o.retry_at_ms = now_ms;
            self.stats.confirmed_posts += 1;
        } else if o.phase == OrderPhase::UnknownCancel {
            o.phase = OrderPhase::Live;
            o.retry_at_ms = now_ms;
        }
        if delta == 0 {
            self.stats.duplicate_observations += 1;
        }
        self.reconcile(now_ms)
    }
    fn reserved_pico(&self) -> u128 {
        self.active_orders()
            .map(|o| u128::from(o.reserved_quantity_micros) * u128::from(o.quote.price_micros))
            .sum()
    }
    fn commitments(&self, outcome: Outcome) -> u64 {
        self.active_orders()
            .filter(|o| o.quote.outcome == outcome)
            .map(|o| o.reserved_quantity_micros)
            .sum()
    }
    fn reconcile(&mut self, now_ms: u64) -> Result<Vec<Action>> {
        self.reconcile_inner(now_ms, !self.defer_automatic_posts)
    }
    fn reconcile_inner(&mut self, now_ms: u64, allow_new_posts: bool) -> Result<Vec<Action>> {
        let mut actions = vec![];
        let keys: Vec<String> = self
            .slots
            .keys()
            .chain(self.desired.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        for key in keys {
            if let Some(id) = self.slots.get(&key).copied() {
                let o = self.orders.get_mut(&id).ok_or("slot lost its order")?;
                if matches!(o.phase, OrderPhase::UnknownPost | OrderPhase::UnknownCancel) {
                    if now_ms >= o.retry_at_ms {
                        o.retry_at_ms = now_ms.saturating_add(1_000);
                        actions.push(Action::Query { order_id: id });
                        self.stats.queries += 1;
                    }
                } else if o.phase == OrderPhase::Live
                    && now_ms >= o.retry_at_ms
                    && !matches_desired(o, self.desired.get(&key))
                {
                    o.phase = OrderPhase::Cancelling;
                    actions.push(Action::Cancel { order_id: id });
                }
                continue;
            }
            if !allow_new_posts
                || self.halted.is_some()
                || self.cooldowns.get(&key).is_some_and(|at| now_ms < *at)
            {
                continue;
            }
            let Some(mut q) = self.desired.get(&key).cloned() else {
                continue;
            };
            if self.active_orders().count() >= self.limits.max_orders {
                continue;
            }
            let signed = i128::from(self.inventory[0]) - i128::from(self.inventory[1]);
            let directional = if q.outcome == Outcome::Up {
                signed
            } else {
                -signed
            };
            let room = i128::from(self.limits.max_imbalance_micros)
                - directional
                - i128::from(self.commitments(q.outcome));
            let available = (u128::from(self.limits.initial_cash_micros) * u128::from(SCALE))
                .checked_sub(self.spent_pico + self.reserved_pico())
                .ok_or("collateral invariant violated")?;
            let cash_room = available / u128::from(q.price_micros);
            let requested_quantity_micros = q.quantity_micros;
            q.quantity_micros = q
                .quantity_micros
                .min(u64::try_from(cash_room.min(u128::from(u64::MAX))).unwrap_or(u64::MAX))
                .min(u64::try_from(room.max(0)).unwrap_or(u64::MAX));
            if q.quantity_micros < self.limits.min_order_micros {
                self.stats.risk_rejections += 1;
                continue;
            }
            self.next_id = self
                .next_id
                .checked_add(1)
                .ok_or("order identity exhausted")?;
            let order = Order {
                id: self.next_id,
                quote: q,
                requested_quantity_micros,
                phase: OrderPhase::Posting,
                cumulative_filled_micros: 0,
                reserved_quantity_micros: 0,
                retry_at_ms: 0,
            };
            let mut order = order;
            order.reserved_quantity_micros = order.quote.quantity_micros;
            self.slots.insert(key.clone(), order.id);
            self.orders.insert(order.id, order.clone());
            self.stats.submitted_posts += 1;
            actions.push(Action::Post { order });
        }
        self.cooldowns
            .retain(|key, _| self.desired.contains_key(key) || self.slots.contains_key(key));
        while self.orders.len() > MAX_HISTORY {
            let Some(id) = self
                .orders
                .iter()
                .find(|(_, o)| o.phase == OrderPhase::Closed)
                .map(|(id, _)| *id)
            else {
                return Err("execution history capacity exceeded".into());
            };
            self.orders.remove(&id);
        }
        Ok(actions)
    }
    /// The identical account projection used by the full snapshot, without
    /// cloning active orders, execution statistics or halt diagnostics.
    pub fn account_snapshot(&self) -> AccountSnapshot {
        let reserved = self.reserved_pico();
        let budget = u128::from(self.limits.initial_cash_micros) * u128::from(SCALE);
        let available = budget.saturating_sub(self.spent_pico + reserved);
        let signed = i128::from(self.inventory[0]) - i128::from(self.inventory[1]);
        AccountSnapshot {
            initial_cash_micros: self.limits.initial_cash_micros,
            spent_micros_ceil: (self.spent_pico.div_ceil(u128::from(SCALE))) as u64,
            reserved_micros_ceil: (reserved.div_ceil(u128::from(SCALE))) as u64,
            available_micros_floor: (available / u128::from(SCALE)) as u64,
            spent_pico: self.spent_pico.to_string(),
            outcome_spent_pico: self
                .outcome_spent_pico
                .map(|costs| costs.map(|c| c.to_string())),
            reserved_pico: reserved.to_string(),
            available_pico: available.to_string(),
            up_inventory_micros: self.inventory[0],
            down_inventory_micros: self.inventory[1],
            worst_up_imbalance_micros: (signed + i128::from(self.commitments(Outcome::Up))) as i64,
            worst_down_imbalance_micros: (-signed + i128::from(self.commitments(Outcome::Down)))
                as i64,
        }
    }
    pub fn snapshot(&self) -> CoreSnapshot {
        CoreSnapshot {
            account: self.account_snapshot(),
            orders: self.active_orders().cloned().collect(),
            stats: self.stats.clone(),
            halted: self.halted.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.orders.len() > MAX_HISTORY
            || self.desired.len() > 64
            || self.slots.len() > 64
            || self.cooldowns.len() > 64
            || self.spent_pico + self.reserved_pico()
                > u128::from(self.limits.initial_cash_micros) * u128::from(SCALE)
            || self.halted.as_ref().is_some_and(|x| x.len() > 512)
            || self.outcome_spent_pico.is_some_and(|costs| {
                costs[0].checked_add(costs[1]) != Some(self.spent_pico)
                    || costs.iter().zip(self.inventory).any(|(cost, quantity)| {
                        (*cost == 0) != (quantity == 0)
                            || *cost > u128::from(quantity) * u128::from(SCALE)
                    })
            })
        {
            return Err("invalid persisted core bounds or collateral".into());
        }
        for (id, o) in &self.orders {
            if *id != o.id
                || o.id > self.next_id
                || o.cumulative_filled_micros > o.quote.quantity_micros
                || o.reserved_quantity_micros
                    != if o.phase == OrderPhase::Closed {
                        0
                    } else {
                        o.quote.quantity_micros - o.cumulative_filled_micros
                    }
                || o.quote.price_micros == 0
                || o.quote.price_micros >= SCALE
                || o.quote.quantity_micros > 1_000_000_000 * SCALE
            {
                return Err("invalid persisted order".into());
            }
            if o.phase != OrderPhase::Closed && self.slots.get(&o.quote.slot) != Some(id) {
                return Err("unfenced active order".into());
            }
        }
        for (slot, id) in &self.slots {
            if self
                .orders
                .get(id)
                .is_none_or(|o| o.quote.slot != *slot || o.phase == OrderPhase::Closed)
            {
                return Err("invalid persisted slot".into());
            }
        }
        Ok(())
    }
}
