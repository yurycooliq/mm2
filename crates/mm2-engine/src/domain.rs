use std::collections::BTreeMap;

pub const MICRO: u64 = 1_000_000;
const MIN_LOT_MICRO: u64 = 5 * MICRO;
const MAX_LEVELS: usize = 10_000;

/// Exact decimal construction shared by adapters. Extra zero scale is harmless; rounding is forbidden.
pub fn parse_micro(input: &str) -> Result<u64, String> {
    if input.is_empty() || input.len() > 128 {
        return Err("invalid decimal length".into());
    }
    let mut parts = input.split('.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction.is_some_and(|f| f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("unsigned fixed decimal required".into());
    }
    let fraction = fraction.unwrap_or("");
    if fraction.bytes().skip(6).any(|b| b != b'0') {
        return Err("nonzero precision exceeds six decimals".into());
    }
    let whole = whole.parse::<u64>().map_err(|_| "decimal overflow")?;
    let mut tail = 0u64;
    for i in 0..6 {
        tail = tail * 10
            + fraction
                .as_bytes()
                .get(i)
                .map_or(0, |b| u64::from(b - b'0'));
    }
    whole
        .checked_mul(MICRO)
        .and_then(|n| n.checked_add(tail))
        .ok_or_else(|| "decimal overflow".into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Up,
    Down,
}
impl Outcome {
    fn index(self) -> usize {
        if self == Self::Up { 0 } else { 1 }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedSource {
    Clob,
    Binance,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    pub price_micro: u64,
    pub quantity_micro: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub outcome: Outcome,
    pub side: BookSide,
    pub level: Level,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreConfig {
    pub initial_collateral_micro: u64,
    pub max_imbalance_micro: u64,
    pub lot_micro: u64,
    pub half_width_micro: u64,
    pub max_feed_age_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Market {
    pub slug: String,
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub tick_micro: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEvent {
    Trade {
        condition_id: String,
        outcome: Outcome,
        exchange_ms: u64,
        observed_mono_ms: u64,
        price_micro: u64,
        quantity_micro: u64,
        fingerprint: String,
    },
    Snapshot {
        condition_id: String,
        outcome: Outcome,
        exchange_ms: u64,
        observed_mono_ms: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Delta {
        condition_id: String,
        exchange_ms: u64,
        observed_mono_ms: u64,
        changes: Vec<Change>,
    },
    Tick {
        condition_id: String,
        outcome: Outcome,
        exchange_ms: u64,
        observed_mono_ms: u64,
        old_tick_micro: Option<u64>,
        new_tick_micro: u64,
    },
    Reference {
        exchange_ms: u64,
        observed_mono_ms: u64,
        price_micro: u64,
    },
    Disconnect {
        source: FeedSource,
        reason: String,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Pause,
    Resume,
    Kill,
    Reset,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Paused,
    Shadow,
    Killed,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopReason {
    Paused,
    Killed,
    MissingMarket,
    OutsideWindow,
    ClobDisconnected,
    MissingBook(Outcome),
    StaleBook(Outcome),
    InvalidClock,
    InvalidInput,
    BudgetExceeded,
    ImbalanceExceeded,
    NoLegalQuote(Outcome),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Intent {
    pub outcome: Outcome,
    pub price_micro: u64,
    pub quantity_micro: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub mode: Mode,
    pub intents: Vec<Intent>,
    pub stop_reasons: Vec<StopReason>,
    pub hypothetical_reservation_micro: u64,
    pub worst_case_imbalance_micro: u64,
    pub experimental_book_only: bool,
    pub exact_twap_available: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookSnapshot {
    pub initialized: bool,
    pub tick_micro: u64,
    pub best_bid_micro: Option<u64>,
    pub best_ask_micro: Option<u64>,
    pub exchange_ms: Option<u64>,
    pub observed_mono_ms: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceSnapshot {
    pub price_micro: u64,
    pub exchange_ms: u64,
    pub observed_mono_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreSnapshot {
    pub mode: Mode,
    pub config: CoreConfig,
    pub market: Option<Market>,
    pub intents: Vec<Intent>,
    pub hypothetical_reservation_micro: u64,
    pub worst_case_imbalance_micro: u64,
    pub up_book: BookSnapshot,
    pub down_book: BookSnapshot,
    pub reference: Option<ReferenceSnapshot>,
    pub clob_connected: bool,
    pub binance_connected: bool,
    pub last_error: Option<String>,
}
#[derive(Clone, Debug)]
struct Book {
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    tick_micro: u64,
    exchange_ms: Option<u64>,
    observed_mono_ms: Option<u64>,
    tick_exchange_ms: Option<u64>,
}
impl Book {
    fn empty(tick_micro: u64) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tick_micro,
            exchange_ms: None,
            observed_mono_ms: None,
            tick_exchange_ms: None,
        }
    }
    fn snapshot(&self) -> BookSnapshot {
        BookSnapshot {
            initialized: self.exchange_ms.is_some(),
            tick_micro: self.tick_micro,
            best_bid_micro: self.bids.last_key_value().map(|(p, _)| *p),
            best_ask_micro: self.asks.first_key_value().map(|(p, _)| *p),
            exchange_ms: self.exchange_ms,
            observed_mono_ms: self.observed_mono_ms,
        }
    }
    fn valid(&self) -> bool {
        self.bids.len() <= MAX_LEVELS
            && self.asks.len() <= MAX_LEVELS
            && match (self.bids.last_key_value(), self.asks.first_key_value()) {
                (Some((b, _)), Some((a, _))) => b < a,
                _ => true,
            }
    }
}

/// Shadow proposals are unsubmitted. No lifecycle, fills, wallet, fees, capital recycling or queue priority is inferred.
#[derive(Clone)]
pub struct Core {
    config: CoreConfig,
    market: Option<Market>,
    mode: Mode,
    books: [Book; 2],
    reference: Option<ReferenceSnapshot>,
    clob_connected: bool,
    binance_connected: bool,
    last_clob_mono_ms: Option<u64>,
    last_evaluate_mono_ms: Option<u64>,
    intents: Vec<Intent>,
    reserved_micro: u64,
    imbalance_micro: u64,
    last_error: Option<String>,
    input_invalid: bool,
}
/// Venue tick set shared by market discovery and public-feed adapters.
pub fn valid_tick_micro(tick: u64) -> bool {
    matches!(tick, 100_000 | 10_000 | 5_000 | 2_500 | 1_000 | 100)
}
fn valid_config(c: &CoreConfig) -> Result<(), String> {
    if c.initial_collateral_micro == 0
        || c.max_imbalance_micro == 0
        || c.lot_micro < MIN_LOT_MICRO
        || c.half_width_micro == 0
        || c.half_width_micro >= MICRO
        || c.max_feed_age_ms == 0
    {
        return Err(
            "positive finite budget/cap/age, at least five shares and legal quote width required"
                .into(),
        );
    }
    Ok(())
}
fn valid_token(token: &str) -> bool {
    const MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    !token.is_empty()
        && token.bytes().all(|b| b.is_ascii_digit())
        && (token == "0" || !token.starts_with('0'))
        && (token.len() < MAX.len() || token.len() == MAX.len() && token <= MAX)
}
fn valid_market(m: &Market) -> bool {
    let id = m.condition_id.strip_prefix("0x").unwrap_or("");
    !m.slug.is_empty()
        && m.slug.len() <= 128
        && id.len() == 64
        && id.bytes().all(|b| b.is_ascii_hexdigit())
        && valid_token(&m.up_token)
        && valid_token(&m.down_token)
        && m.up_token != m.down_token
        && m.window_start_ms < m.window_end_ms
        && valid_tick_micro(m.tick_micro)
}
fn levels(input: Vec<Level>) -> Result<BTreeMap<u64, u64>, String> {
    if input.len() > MAX_LEVELS {
        return Err("book depth exceeds bound".into());
    }
    let mut result = BTreeMap::new();
    for l in input {
        if l.price_micro == 0 || l.price_micro >= MICRO || result.contains_key(&l.price_micro) {
            return Err("invalid or duplicate book level".into());
        }
        result.insert(l.price_micro, l.quantity_micro);
    }
    result.retain(|_, q| *q > 0);
    Ok(result)
}
impl Core {
    pub fn new(config: CoreConfig) -> Result<Self, String> {
        valid_config(&config)?;
        Ok(Self {
            config,
            market: None,
            mode: Mode::Paused,
            books: [Book::empty(10_000), Book::empty(10_000)],
            reference: None,
            clob_connected: false,
            binance_connected: false,
            last_clob_mono_ms: None,
            last_evaluate_mono_ms: None,
            intents: Vec::new(),
            reserved_micro: 0,
            imbalance_micro: 0,
            last_error: None,
            input_invalid: false,
        })
    }
    fn clear_proposals(&mut self) {
        self.intents.clear();
        self.reserved_micro = 0;
        self.imbalance_micro = 0;
    }
    fn clear_clob(&mut self) {
        self.clear_proposals();
        self.books = [
            Book::empty(self.books[0].tick_micro),
            Book::empty(self.books[1].tick_micro),
        ];
        self.clob_connected = false;
    }
    fn invalid(&mut self, reason: &str) -> String {
        self.clear_clob();
        self.input_invalid = true;
        self.last_error = Some(reason.into());
        reason.into()
    }
    pub fn set_market(&mut self, market: Market) -> Result<(), String> {
        if !valid_market(&market) {
            return Err(self.invalid("invalid market identity/window/tick"));
        }
        self.clear_clob();
        self.books = [
            Book::empty(market.tick_micro),
            Book::empty(market.tick_micro),
        ];
        self.market = Some(market);
        self.last_error = None;
        self.input_invalid = false;
        Ok(())
    }
    pub fn set_config(&mut self, config: CoreConfig) -> Result<(), String> {
        self.clear_proposals();
        if let Err(error) = valid_config(&config) {
            if self.mode != Mode::Killed {
                self.mode = Mode::Paused;
            }
            self.input_invalid = true;
            self.last_error = Some(error.clone());
            return Err(error);
        }
        self.config = config;
        self.last_error = None;
        self.input_invalid = false;
        Ok(())
    }
    pub fn control(&mut self, action: Action) -> Result<(), String> {
        self.clear_proposals();
        match action {
            Action::Pause => {
                if self.mode != Mode::Killed {
                    self.mode = Mode::Paused;
                }
            }
            Action::Resume => {
                if self.mode == Mode::Killed {
                    return Err("kill latch requires explicit reset".into());
                }
                self.mode = Mode::Shadow;
            }
            Action::Kill => {
                self.mode = Mode::Killed;
            }
            Action::Reset => {
                self.mode = Mode::Paused;
                self.clear_clob();
                self.reference = None;
                self.binance_connected = false;
                self.last_error = None;
                self.input_invalid = false;
            }
        }
        Ok(())
    }
    fn accept_clob_clock(
        &self,
        condition: &str,
        exchange_ms: u64,
        mono_ms: u64,
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        if self
            .market
            .as_ref()
            .is_none_or(|m| m.condition_id != condition)
        {
            return Err("event market identity mismatch".into());
        }
        if self.last_clob_mono_ms.is_some_and(|m| mono_ms < m) {
            return Err("CLOB receive clock regressed".into());
        }
        if outcomes.iter().any(|o| {
            self.books[o.index()]
                .exchange_ms
                .is_some_and(|t| exchange_ms < t)
                || self.books[o.index()]
                    .tick_exchange_ms
                    .is_some_and(|t| exchange_ms < t)
        }) {
            return Err("CLOB exchange clock regressed".into());
        }
        Ok(())
    }
    pub fn apply(&mut self, event: FeedEvent) -> Result<(), String> {
        match event {
            FeedEvent::Trade { .. } => return Ok(()),
            FeedEvent::Disconnect { source, reason } => {
                self.clear_proposals();
                self.last_error = Some(reason);
                if source == FeedSource::Clob {
                    self.clear_clob();
                } else {
                    self.reference = None;
                    self.binance_connected = false;
                }
                return Ok(());
            }
            FeedEvent::Reference {
                exchange_ms,
                observed_mono_ms,
                price_micro,
            } => {
                if price_micro == 0
                    || self.reference.as_ref().is_some_and(|r| {
                        exchange_ms < r.exchange_ms || observed_mono_ms < r.observed_mono_ms
                    })
                {
                    self.clear_proposals();
                    self.reference = None;
                    self.binance_connected = false;
                    self.input_invalid = true;
                    self.last_error = Some("invalid or regressed reference input".into());
                    return Err("invalid or regressed reference input".into());
                }
                self.reference = Some(ReferenceSnapshot {
                    price_micro,
                    exchange_ms,
                    observed_mono_ms,
                });
                self.binance_connected = true;
            }
            FeedEvent::Snapshot {
                condition_id,
                outcome,
                exchange_ms,
                observed_mono_ms,
                bids,
                asks,
            } => {
                if let Err(error) =
                    self.accept_clob_clock(&condition_id, exchange_ms, observed_mono_ms, &[outcome])
                {
                    return Err(self.invalid(&error));
                }
                let bids = match levels(bids) {
                    Ok(b) => b,
                    Err(e) => return Err(self.invalid(&e)),
                };
                let asks = match levels(asks) {
                    Ok(a) => a,
                    Err(e) => return Err(self.invalid(&e)),
                };
                let mut book = self.books[outcome.index()].clone();
                book.bids = bids;
                book.asks = asks;
                book.exchange_ms = Some(exchange_ms);
                book.observed_mono_ms = Some(observed_mono_ms);
                if !book.valid() {
                    return Err(self.invalid("crossed or oversized book"));
                }
                self.books[outcome.index()] = book;
                self.last_clob_mono_ms = Some(observed_mono_ms);
                self.clob_connected = true;
            }
            FeedEvent::Delta {
                condition_id,
                exchange_ms,
                observed_mono_ms,
                changes,
            } => {
                if changes.is_empty() || changes.len() > MAX_LEVELS {
                    return Err(self.invalid("empty or oversized delta batch"));
                }
                let outcomes: Vec<_> = changes.iter().map(|c| c.outcome).collect();
                if let Err(error) =
                    self.accept_clob_clock(&condition_id, exchange_ms, observed_mono_ms, &outcomes)
                {
                    return Err(self.invalid(&error));
                }
                let mut books = self.books.clone();
                for c in changes {
                    let book = &mut books[c.outcome.index()];
                    if book.exchange_ms.is_none()
                        || c.level.price_micro == 0
                        || c.level.price_micro >= MICRO
                    {
                        return Err(self.invalid("delta before snapshot or invalid price"));
                    }
                    let side = if c.side == BookSide::Bid {
                        &mut book.bids
                    } else {
                        &mut book.asks
                    };
                    if c.level.quantity_micro == 0 {
                        side.remove(&c.level.price_micro);
                    } else {
                        side.insert(c.level.price_micro, c.level.quantity_micro);
                    }
                    book.exchange_ms = Some(exchange_ms);
                    book.observed_mono_ms = Some(observed_mono_ms);
                }
                if books.iter().any(|b| !b.valid()) {
                    return Err(self.invalid("crossed or oversized batch result"));
                }
                self.books = books;
                self.last_clob_mono_ms = Some(observed_mono_ms);
                self.clob_connected = true;
            }
            FeedEvent::Tick {
                condition_id,
                outcome,
                exchange_ms,
                observed_mono_ms,
                old_tick_micro,
                new_tick_micro,
            } => {
                if let Err(error) =
                    self.accept_clob_clock(&condition_id, exchange_ms, observed_mono_ms, &[outcome])
                {
                    return Err(self.invalid(&error));
                }
                let book = &self.books[outcome.index()];
                if !valid_tick_micro(new_tick_micro)
                    || old_tick_micro.is_some_and(|t| t != book.tick_micro)
                    || book.tick_exchange_ms.is_some_and(|t| exchange_ms < t)
                {
                    return Err(self.invalid("invalid or incompatible tick transition"));
                }
                let book = &mut self.books[outcome.index()];
                book.tick_micro = new_tick_micro;
                book.tick_exchange_ms = Some(exchange_ms);
                self.last_clob_mono_ms = Some(observed_mono_ms);
            }
        }
        self.clear_proposals();
        self.last_error = None;
        self.input_invalid = false;
        Ok(())
    }
    pub fn evaluate(&mut self, now_wall_ms: u64, now_mono_ms: u64) -> Decision {
        self.clear_proposals();
        let mut reasons = Vec::new();
        if self.last_evaluate_mono_ms.is_some_and(|t| now_mono_ms < t) {
            self.invalid("evaluation monotonic clock regressed");
            reasons.push(StopReason::InvalidClock);
        }
        self.last_evaluate_mono_ms = Some(now_mono_ms);
        if self.last_clob_mono_ms.is_some_and(|t| t > now_mono_ms)
            || self
                .books
                .iter()
                .any(|b| b.tick_exchange_ms.is_some_and(|t| t > now_wall_ms))
            || self
                .reference
                .as_ref()
                .is_some_and(|r| r.observed_mono_ms > now_mono_ms || r.exchange_ms > now_wall_ms)
        {
            reasons.push(StopReason::InvalidClock);
        }
        match self.mode {
            Mode::Paused => reasons.push(StopReason::Paused),
            Mode::Killed => reasons.push(StopReason::Killed),
            Mode::Shadow => {}
        }
        if self.input_invalid {
            reasons.push(StopReason::InvalidInput);
        }
        if let Some(market) = &self.market {
            if now_wall_ms < market.window_start_ms || now_wall_ms >= market.window_end_ms {
                reasons.push(StopReason::OutsideWindow);
            }
        } else {
            reasons.push(StopReason::MissingMarket);
        }
        if !self.clob_connected {
            reasons.push(StopReason::ClobDisconnected);
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let book = &self.books[outcome.index()];
            match (book.exchange_ms, book.observed_mono_ms) {
                (Some(ex), Some(rx)) if ex > now_wall_ms || rx > now_mono_ms => {
                    reasons.push(StopReason::InvalidClock)
                }
                (Some(ex), Some(rx))
                    if now_wall_ms - ex > self.config.max_feed_age_ms
                        || now_mono_ms - rx > self.config.max_feed_age_ms =>
                {
                    reasons.push(StopReason::StaleBook(outcome))
                }
                (None, _) | (_, None) => reasons.push(StopReason::MissingBook(outcome)),
                _ => {}
            }
        }
        if self.config.lot_micro > self.config.max_imbalance_micro {
            reasons.push(StopReason::ImbalanceExceeded);
        }
        if reasons.is_empty() {
            let mut candidates = Vec::new();
            let mut reserve = 0u128;
            for outcome in [Outcome::Up, Outcome::Down] {
                let book = &self.books[outcome.index()];
                let price = book
                    .bids
                    .last_key_value()
                    .and_then(|(p, _)| p.checked_sub(self.config.half_width_micro))
                    .map(|p| p / book.tick_micro * book.tick_micro);
                if let Some(price_micro) = price.filter(|p| *p > 0 && *p < MICRO) {
                    reserve += (u128::from(price_micro) * u128::from(self.config.lot_micro))
                        .div_ceil(u128::from(MICRO));
                    candidates.push(Intent {
                        outcome,
                        price_micro,
                        quantity_micro: self.config.lot_micro,
                    });
                } else {
                    reasons.push(StopReason::NoLegalQuote(outcome));
                }
            }
            if reserve > u128::from(self.config.initial_collateral_micro) {
                reasons.push(StopReason::BudgetExceeded);
            }
            if reasons.is_empty() {
                self.intents = candidates;
                self.reserved_micro = reserve as u64;
                // Either side can be the only one filled: a paired proposal is not zero worst-case exposure.
                self.imbalance_micro = self.config.lot_micro;
            }
        }
        Decision {
            mode: self.mode,
            intents: self.intents.clone(),
            stop_reasons: reasons,
            hypothetical_reservation_micro: self.reserved_micro,
            worst_case_imbalance_micro: self.imbalance_micro,
            experimental_book_only: true,
            exact_twap_available: false,
        }
    }
    pub fn snapshot(&self) -> CoreSnapshot {
        CoreSnapshot {
            mode: self.mode,
            config: self.config.clone(),
            market: self.market.clone(),
            intents: self.intents.clone(),
            hypothetical_reservation_micro: self.reserved_micro,
            worst_case_imbalance_micro: self.imbalance_micro,
            up_book: self.books[0].snapshot(),
            down_book: self.books[1].snapshot(),
            reference: self.reference.clone(),
            clob_connected: self.clob_connected,
            binance_connected: self.binance_connected,
            last_error: self.last_error.clone(),
        }
    }
}
