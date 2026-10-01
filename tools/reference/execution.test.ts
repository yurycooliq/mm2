import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ExecutionRun } from './execution.ts';

const directory = fileURLToPath(new URL('../../fixtures/execution/', import.meta.url));
for (const file of readdirSync(directory).filter(name => name.endsWith('.json'))) {
  test(`corrected TypeScript oracle: ${file}`, () => {
    const tape = JSON.parse(readFileSync(directory + file, 'utf8'));
    assert.equal(tape.latency.ack_ms, 0, 'oracle subset requires ideal acknowledgements');
    const actions: { at: number; priority: number; sequence: number; action: () => void }[] = [];
    let sequence = 0;
    let book = { bid: null as number | null, ask: null as number | null, levels: [] as [number, number][] };
    const scale = 1_000_000;
    const limits = tape.limits ?? { initial_cash_micros: 100 * scale, max_imbalance_micros: 20 * scale, min_order_micros: 5 * scale, tick_micros: 10_000 };
    const run = new ExecutionRun({ postMs: tape.latency.post_ms, cancelMs: tape.latency.cancel_ms,
      fillFeedMs: tape.latency.fill_ms, initialCollateralUsd: limits.initial_cash_micros / scale,
      maxImbalanceShares: limits.max_imbalance_micros / scale, minOrderShares: limits.min_order_micros / scale,
      tickSize: limits.tick_micros / scale },
      (at, priority, action) => actions.push({ at, priority, sequence: sequence++, action }),
      () => ({ bid: book.bid, ask: book.ask, size: (_side, price) => book.levels.find(level => level[0] === price)?.[1] ?? 0 }),
      () => {}, 'back');
    run.open(1);
    function advance(until: number) {
      while (actions.some(action => action.at <= until)) {
        actions.sort((a, b) => a.at - b.at || a.priority - b.priority || a.sequence - b.sequence);
        actions.shift()!.action();
      }
    }
    for (const step of tape.steps) {
      advance(step.at_ms);
      switch (step.op) {
        case 'book':
          assert.equal(step.outcome, 'up', 'shared oracle tapes use Up-depth identity');
          book = { bid: step.book.bid === null ? null : step.book.bid / scale,
            ask: step.book.ask === null ? null : step.book.ask / scale,
            levels: step.book.bid_levels.map((level: { price_micros: number; quantity_micros: number }) => [level.price_micros / scale, level.quantity_micros / scale]) };
          break;
        case 'quote':
          run.setDesired(1, step.at_ms, step.quotes.map((quote: { slot: string; outcome: string; price_micros: number; quantity_micros: number }) => ({
            outcome: quote.outcome === 'up' ? 'Up' : 'Down', price: quote.price_micros / scale,
            lot: quote.slot.endsWith(':hedge') ? 0 : quote.quantity_micros / scale,
            hedge: quote.slot.endsWith(':hedge') ? quote.quantity_micros / scale : 0, key: quote.slot,
          })));
          break;
        case 'cancel_all': run.setDesired(1, step.at_ms, []); break;
        case 'trade':
          assert.equal(step.outcome, 'up');
          run.consume(1, step.at_ms, 'B', step.worst_price_micros / scale, step.quantity_micros / scale, book.levels);
          break;
        // Restart has no effect on the reference timeline. Rust separately tests actual serialization/recovery.
        case 'restart': case 'advance': break;
        default: throw new Error('unsupported oracle operation');
      }
    }
    const inventory = run.inventory(1), collateral = run.collateral(), execution = run.result(1).execution;
    const observed = { up_inventory_micros: Math.round(inventory.up * scale), down_inventory_micros: Math.round(inventory.down * scale),
      spent_pico: String(Math.round(collateral.spentUsd * 1e12)), reserved_pico: String(Math.round(collateral.reservedUsd * 1e12)),
      available_pico: String(Math.round(collateral.availableUsd * 1e12)), accepted_posts: execution.acceptedPosts,
      cancelled_orders: execution.cancelledOrders, rejected_posts: execution.rejectedPosts };
    assert.deepEqual(observed, tape.expected);
  });
}
