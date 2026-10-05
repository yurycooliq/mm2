/** A bounded oracle for the explicit book-only candidate contract, not full replay parity. */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

type Pair = [string, string, number, number];
type Book = { bid_micros: number; ask_micros: number; received_ms: number };
type Case = { name: string; rules: Record<string, number>; input: Record<string, unknown>; expected: Pair[]; reason: string };
const cases: Case[] = JSON.parse(readFileSync(new URL('./parity.json', import.meta.url), 'utf8'));
assert.ok(cases.length <= 32);

function candidate(c: Case): { quotes: Pair[]; reason: string } {
  const r = { lot_full_micros: 10_000_000, half_width_micros: 45_000, shrink_ppm: 977_000,
    stub_price_micros: 0, min_price_micros: 20_000, max_feed_age_ms: 1_000,
    max_book_spread_micros: 50_000, max_imbalance_micros: 20_000_000, ...c.rules };
  const i = { now_ms: 54_000_000, market_start_ms: 53_900_000, market_end_ms: 54_300_000,
    up: { bid_micros: 600_000, ask_micros: 610_000, received_ms: 54_000_000 },
    down: { bid_micros: 390_000, ask_micros: 400_000, received_ms: 54_000_000 },
    inventory_up_micros: 0, inventory_down_micros: 0, reserved_up_micros: 0,
    reserved_down_micros: 0, available_cash_micros: 1_000_000_000,
    tick_micros: 10_000, min_order_micros: 5_000_000, ...c.input };
  const books = [i.up, i.down] as Book[];
  if (books.some(b => b.received_ms > i.now_ms || i.now_ms - b.received_ms > r.max_feed_age_ms)) {
    return { quotes: [], reason: 'stale_book' };
  }
  const scale = 1_000_000n;
  const mid = BigInt(Math.floor((books[0]!.bid_micros + books[0]!.ask_micros) / 2));
  const center = scale / 2n + BigInt(r.shrink_ppm) * (mid - scale / 2n) / scale;
  const tick = BigInt(i.tick_micros);
  const targets = [center - BigInt(r.half_width_micros), scale - center - BigInt(r.half_width_micros)];
  const prices = targets.map((x, side) => Number(((x > 0n ? x : 0n) + tick / 2n) / tick * tick < BigInt(books[side]!.ask_micros - 1) / tick * tick
    ? ((x > 0n ? x : 0n) + tick / 2n) / tick * tick : BigInt(books[side]!.ask_micros - 1) / tick * tick));
  const imbalance = BigInt(i.inventory_up_micros) - BigInt(i.inventory_down_micros);
  const capacity = [BigInt(r.max_imbalance_micros) - imbalance - BigInt(i.reserved_up_micros),
    BigInt(r.max_imbalance_micros) + imbalance - BigInt(i.reserved_down_micros)].map(x => x > 0n ? x : 0n);
  let cash = BigInt(i.available_cash_micros) * scale;
  const quotes: Pair[] = [];
  function add(side: number, kind: string, price: number, size: bigint): void {
    if (price <= 0 || price % i.tick_micros !== 0) return;
    const affordable = cash / BigInt(price);
    const quantity = [size, capacity[side]!, affordable].reduce((a, b) => a < b ? a : b);
    if (quantity < BigInt(i.min_order_micros)) return;
    const outcome = side === 0 ? 'up' : 'down';
    quotes.push([`${outcome}:${kind}`, outcome, price, Number(quantity)]);
    capacity[side] = capacity[side]! - quantity;
    cash -= BigInt(price) * quantity;
  }
  const lagging = imbalance > 0n ? 1 : 0;
  if (imbalance !== 0n && prices[lagging]! >= r.min_price_micros) add(lagging, 'hedge', prices[lagging]!, imbalance < 0n ? -imbalance : imbalance);
  const date = new Date(i.now_ms);
  const full = date.getUTCDay() >= 1 && date.getUTCDay() <= 5 && date.getUTCHours() >= 12 && date.getUTCHours() < 23;
  const lot = full ? r.lot_full_micros : Math.round(r.lot_full_micros / 2_000_000) * 1_000_000;
  prices.forEach((price, side) => { if (price >= r.min_price_micros) add(side, 'lot', price, BigInt(lot)); });
  if (r.stub_price_micros) books.forEach((book, side) => { if (r.stub_price_micros < book.ask_micros) add(side, 'stub', r.stub_price_micros, BigInt(lot)); });
  return { quotes, reason: quotes.length ? 'quote' : 'insufficient_capital' };
}

for (const c of cases) test(c.name, () => assert.deepEqual(candidate(c), { quotes: c.expected, reason: c.reason }));
