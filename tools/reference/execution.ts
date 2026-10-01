type Outcome = 'Up' | 'Down';

export type BookSide = 'B' | 'S';
export interface OrderIntent {
  outcome: Outcome;
  price: number;
  lot: number;
  hedge: number;
  key: string;
}
export interface ExecutionPolicy {
  cancelMs: number;
  postMs: number;
  fillFeedMs: number;
  initialCollateralUsd: number;
  maxImbalanceShares: number;
  minOrderShares: number;
  tickSize: number;
}
export interface ExecutionFill {
  ms: number;
  outcome: Outcome;
  price: number;
  shares: number;
  role: 'maker';
  fee: number;
  orderId: number;
  kind: 'lot' | 'hedge';
}
export interface ExecutionStats {
  acceptedPosts: number;
  cancelledOrders: number;
  rejectedPosts: number;
  rejectedRisk: number;
}
type Desired = { outcome: Outcome; price: number; units: bigint; key: string; kind: 'lot' | 'hedge' };
type Order = Desired & {
  id: number;
  market: number;
  remaining: bigint;
  reserved: bigint;
  queueAhead: number;
  behind: number;
  state: 'posting' | 'live' | 'cancelling' | 'closed';
};
type Slot = { desired: Desired | null; order: Order | null };
type Window = {
  slots: Map<string, Slot>;
  inventory: { up: bigint; down: bigint };
  closed: boolean;
  fills: ExecutionFill[];
  stats: ExecutionStats;
};
type Schedule = (atMs: number, priority: number, action: () => void) => void;
type Book = { bid: number | null; ask: number | null; size: (side: BookSide, price: number) => number };
const SCALE = 1_000_000n;
const numberOf = (n: bigint): number => Number(n) / Number(SCALE);
const units = (n: number): bigint => {
  const scaled = n * Number(SCALE);
  if (!Number.isFinite(n) || n < 0 || !Number.isSafeInteger(Math.round(scaled)) || Math.abs(scaled - Math.round(scaled)) > 0.0001)
    throw new Error('quantity or price is outside six-decimal research precision');
  return BigInt(Math.round(scaled));
};
const same = (a: Desired | null, b: Desired | null): boolean =>
  a !== null && b !== null && a.key === b.key && a.outcome === b.outcome && a.price === b.price && a.units === b.units;
const sideOf = (o: Desired): BookSide => o.outcome === 'Up' ? 'B' : 'S';
const levelOf = (o: Desired): number => numberOf(o.outcome === 'Up' ? units(o.price) : SCALE - units(o.price));

/** Passive orders only. Reservations survive cancellation requests and unreported fills. */
export class ExecutionRun {
  private readonly windows = new Map<number, Window>();
  private readonly commitments = new Set<Order>();
  private readonly budget: bigint;
  private readonly cap: bigint;
  private spent = 0n;
  private nextId = 0;

  constructor(
    private readonly policy: ExecutionPolicy,
    private readonly schedule: Schedule,
    private readonly book: (market: number) => Book,
    private readonly receivedFill: (market: number, ms: number) => void,
    private readonly cancels: 'proportional' | 'back',
  ) {
    for (const delay of [policy.cancelMs, policy.postMs, policy.fillFeedMs])
      if (!Number.isFinite(delay) || delay < 0) throw new Error('execution delays must be finite and non-negative');
    this.budget = units(policy.initialCollateralUsd) * SCALE;
    this.cap = units(policy.maxImbalanceShares);
    if (this.budget <= 0n || this.cap <= 0n || !Number.isFinite(policy.minOrderShares) || policy.minOrderShares < 5 || !(policy.tickSize > 0 && policy.tickSize <= 0.01))
      throw new Error('finite positive collateral, exposure cap, legal minimum and tick are required');
    units(policy.minOrderShares);
  }

  open(market: number): void {
    this.windows.set(market, {
      slots: new Map(), inventory: { up: 0n, down: 0n }, closed: false, fills: [],
      stats: { acceptedPosts: 0, cancelledOrders: 0, rejectedPosts: 0, rejectedRisk: 0 },
    });
  }

  inventory(market: number): { up: number; down: number } {
    const inv = this.windows.get(market)!.inventory;
    return { up: numberOf(inv.up), down: numberOf(inv.down) };
  }

  result(market: number): { fills: ExecutionFill[]; execution: ExecutionStats } {
    const w = this.windows.get(market)!;
    return { fills: w.fills, execution: { ...w.stats } };
  }

  collateral(): { spentUsd: number; reservedUsd: number; availableUsd: number } {
    const reserved = this.reservedCost();
    return { spentUsd: Number(this.spent) / 1e12, reservedUsd: Number(reserved) / 1e12, availableUsd: Number(this.budget - this.spent - reserved) / 1e12 };
  }

  setDesired(market: number, ms: number, intents: readonly OrderIntent[] | null): void {
    const w = this.windows.get(market)!;
    if (intents === null || w.closed) return;
    const wanted = new Map<string, Desired>();
    for (const q of intents) {
      if (!q.key || !['Up', 'Down'].includes(q.outcome) || !(q.price > 0 && q.price < 1) || q.lot < 0 || q.hedge < 0)
        throw new Error('invalid passive order intent');
      if (Math.abs(q.price / this.policy.tickSize - Math.round(q.price / this.policy.tickSize)) > 1e-6)
        throw new Error('order price violates configured tick');
      units(q.price);
      for (const kind of ['hedge', 'lot'] as const) {
        const size = q[kind];
        if (size === 0) continue;
        const slotKey = `${q.outcome}|${kind}|${q.key.endsWith('-stub') ? 'stub' : 'regular'}`;
        if (wanted.has(slotKey)) throw new Error('multiple desired orders occupy one slot');
        wanted.set(slotKey, { outcome: q.outcome, price: q.price, units: units(size), key: q.key, kind });
      }
    }
    for (const key of new Set([...w.slots.keys(), ...wanted.keys()])) {
      const slot = w.slots.get(key) ?? { desired: null, order: null };
      slot.desired = wanted.get(key) ?? null;
      w.slots.set(key, slot);
      this.reconcile(market, slot, ms);
    }
  }

  close(market: number, ms: number): void {
    const w = this.windows.get(market)!;
    w.closed = true;
    for (const slot of w.slots.values()) {
      slot.desired = null;
      this.reconcile(market, slot, ms);
    }
  }

  change(market: number, side: BookSide, price: number, before: number, after: number, isCancel: boolean): void {
    for (const slot of this.windows.get(market)!.slots.values()) {
      const o = slot.order;
      if (!o || !['live', 'cancelling'].includes(o.state) || sideOf(o) !== side || levelOf(o) !== price) continue;
      if (after > before) o.behind += after - before;
      else if (isCancel && this.cancels === 'proportional' && before > 0) o.queueAhead *= after / before;
      else if (isCancel) {
        const back = Math.min(o.behind, before - after);
        o.behind -= back;
        o.queueAhead = Math.max(0, o.queueAhead - (before - after - back));
      }
    }
  }

  consume(market: number, ms: number, side: BookSide, worst: number, size: number, levels: readonly [number, number][]): void {
    const w = this.windows.get(market)!;
    const orders = [...w.slots.values()].map((s) => s.order).filter((o): o is Order =>
      o !== null && ['live', 'cancelling'].includes(o.state) && sideOf(o) === side && o.remaining > 0n,
    ).sort((a, b) => (side === 'B' ? levelOf(b) - levelOf(a) : levelOf(a) - levelOf(b)) || a.id - b.id);
    const original = new Map(orders.map((o) => [o.id, numberOf(o.remaining)]));
    const externalAtLevel = new Map<number, number>();
    const ownFilledAtLevel = new Map<number, number>();
    let filled = 0;
    for (const o of orders) {
      const level = levelOf(o);
      if (!(side === 'B' ? level >= worst - 1e-9 : level <= worst + 1e-9)) continue;
      const better = levels.filter(([p]) => side === 'B' ? p > level : p < level).reduce((n, [, s]) => n + s, 0);
      const ownAhead = orders.filter((p) => p.id !== o.id && (side === 'B' ? levelOf(p) > level : levelOf(p) < level) ||
        p.id < o.id && levelOf(p) === level).reduce((n, p) => n + original.get(p.id)!, 0);
      const reaching = Math.max(0, size - better - ownAhead);
      const got = units(Math.floor(Math.min(numberOf(o.remaining), Math.max(0, reaching - o.queueAhead)) * 1e6 + 1e-7) / 1e6);
      const ownBetter = orders.filter((p) => side === 'B' ? levelOf(p) > level : levelOf(p) < level).reduce((n, p) => n + original.get(p.id)!, 0);
      externalAtLevel.set(level, Math.max(0, size - better - ownBetter));
      if (got === 0n) continue;
      o.remaining -= got;
      filled += numberOf(got);
      ownFilledAtLevel.set(level, (ownFilledAtLevel.get(level) ?? 0) + numberOf(got));
      w.fills.push({ ms, outcome: o.outcome, price: o.price, shares: numberOf(got), role: 'maker', fee: 0, orderId: o.id, kind: o.kind });
      this.schedule(ms + this.policy.fillFeedMs, 3, () => {
        o.reserved -= got;
        // Shares and prices each have 6 decimals. Their product is pUSD in 1e-12 units, avoiding rounded-fragment overdraw.
        this.spent += got * units(o.price);
        if (o.outcome === 'Up') w.inventory.up += got;
        else w.inventory.down += got;
        if (o.reserved === 0n) this.commitments.delete(o);
        this.receivedFill(market, ms + this.policy.fillFeedMs);
      });
    }
    // All orders at a level share the historical queue. External fills before the first own order
    // also advance every later own order, even when its own-ahead quantity blocked this sweep.
    for (const o of orders) {
      const level = levelOf(o);
      const reached = externalAtLevel.get(level);
      if (reached === undefined) continue;
      const historical = levels.find(([p]) => p === level)?.[1] ?? 0;
      const externalFilled = Math.min(historical, Math.max(0, reached - (ownFilledAtLevel.get(level) ?? 0)));
      o.queueAhead = Math.max(0, o.queueAhead - externalFilled);
    }
    if (filled > size + 1e-6) throw new Error('simulated fills exceed taker sweep volume');
  }

  private reservedCost(): bigint {
    let cost = 0n;
    for (const o of this.commitments) cost += o.reserved * units(o.price);
    return cost;
  }

  private reconcile(market: number, slot: Slot, ms: number): void {
    const w = this.windows.get(market)!;
    const o = slot.order;
    if (o) {
      if (o.state === 'posting' || o.state === 'cancelling' || same(o, slot.desired)) return;
      o.state = 'cancelling';
      this.schedule(ms + this.policy.cancelMs, 2, () => {
        o.state = 'closed';
        o.reserved -= o.remaining;
        o.remaining = 0n;
        if (o.reserved === 0n) this.commitments.delete(o);
        slot.order = null;
        w.stats.cancelledOrders++;
        this.reconcile(market, slot, ms + this.policy.cancelMs);
      });
      return;
    }
    const desired = slot.desired;
    if (!desired || w.closed) return;
    const inv = w.inventory.up - w.inventory.down;
    let committed = 0n;
    for (const candidate of this.commitments)
      if (candidate.market === market && candidate.outcome === desired.outcome)
        committed += candidate.reserved;
    const exposureLeft = this.cap - (desired.outcome === 'Up' ? inv : -inv) - committed;
    const cashLeft = this.budget - this.spent - this.reservedCost();
    const quantity = [desired.units, exposureLeft, cashLeft / units(desired.price)].reduce((a, b) => a < b ? a : b);
    if (quantity < units(this.policy.minOrderShares)) {
      w.stats.rejectedRisk++;
      return;
    }
    const next: Order = { ...desired, id: ++this.nextId, market, remaining: quantity, reserved: quantity, queueAhead: 0, behind: 0, state: 'posting' };
    slot.order = next;
    this.commitments.add(next);
    this.schedule(ms + this.policy.postMs, 2, () => {
      const book = this.book(market);
      const level = levelOf(next);
      const crosses = next.outcome === 'Up' ? book.ask !== null && level >= book.ask : book.bid !== null && level <= book.bid;
      if (w.closed || crosses || (book.bid === null && book.ask === null)) {
        next.state = 'closed';
        next.reserved = 0n;
        next.remaining = 0n;
        this.commitments.delete(next);
        slot.order = null;
        w.stats.rejectedPosts++;
        return;
      }
      next.state = 'live';
      next.queueAhead = book.size(sideOf(next), level);
      w.stats.acceptedPosts++;
      this.reconcile(market, slot, ms + this.policy.postMs);
    });
  }
}
