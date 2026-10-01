import { describe, test } from 'node:test';
import { expect } from './expect.ts';
import { inspectPendulumflow } from './pendulumflow.ts';



const CONDITION = `0x${'a'.repeat(64)}`, HASH = 'b'.repeat(64), HOUR = '2026-09-29T20';
const START = 1790712600, END = START + 300, MS = START * 1000;
const PRODUCTS = ['book', 'price_change', 'best_bid_ask', 'last_trade_price', 'new_market', 'market_resolved', 'tick_size_change'];
function event(type: string, seq: number, at: number, token: string | null, payload: Record<string, unknown>, witness = 'a'): Record<string, unknown> {
  return { schemaVersion: 1, source: 'pendulumflow-v3', clockPrecision: 'ms', eventType: type,
    eventId: `${HOUR}:${type}:${seq}`, conditionId: CONDITION, tokenId: token,
    eventTimeMs: String(at), receivedTimeUs: String(BigInt(at) * 1000n + 123n), sequence: String(seq), partitionHour: HOUR,
    sourceWitness: witness, witnessSet: '|a|b|', arrivalSkewUs: '9007199254740993',
    sourceRef: { manifestSha256: HASH, fileSha256Expected: HASH, rowGroup: 0 },
    payload: { market: CONDITION, ...(token === null ? {} : { asset_id: token }), ...payload } };
}
const depth = (bid: string, ask: string, size = '20') => ({ bids: [{ price: bid, size }], asks: [{ price: ask, size }] });
function manifest(count: number): Record<string, unknown> {
  return { schemaVersion: 1, source: 'pendulumflow-v3', datasetId: 'fixture', captureMode: 'merged-union', status: 'acquired-unvalidated', liveEligible: false,
    markets: [{ conditionId: CONDITION, slug: `btc-updown-5m-${START}`, windowStart: START, windowEnd: END,
      tokens: [{ tokenId: '1', outcome: 'Up' }, { tokenId: '2', outcome: 'Down' }], metadataSource: { sha256: HASH } }],
    hours: [{ hour: HOUR, manifestSha256: HASH, fileSha256Expected: HASH, manifestFooterVerified: true, wholeFileChecksumVerified: false,
      products: Object.fromEntries(PRODUCTS.map((p) => [p, { rowCount: 100, selectedRows: 100, expectedProductSha256: HASH, wholeProductChecksumVerified: false, selectedRowGroups: [0] }])) }],
    coverage: { allSevenTypesHandled: true, completeSelectedPartitions: true, binance: false, rtdsTwap: false },
    eventCount: count, eventFile: 'events.jsonl', eventsSha256: HASH };
}
function fixture(): Record<string, unknown>[] {
  return [
    event('new_market', 1, MS - 2000, null, { id: 'native-id', slug: `btc-updown-5m-${START}`, assets_ids: ['1', '2'], outcomes: ['Up', 'Down'], question: 'BTC?' }),
    event('tick_size_change', 2, MS - 1000, '1', { old_tick_size: '0.001', new_tick_size: '0.01' }),
    event('book', 3, MS, '1', depth('0.49', '0.51')),
    event('book', 4, MS, '2', depth('0.49', '0.51'), 'b'),
    event('best_bid_ask', 5, MS + 10, '1', { best_bid: '0.49', best_ask: '0.51', spread: '0.02' }),
    event('price_change', 6, MS + 20, '1', { side: 'BUY', price: '0.49', size: '30', best_bid: '0.49', best_ask: '0.51' }),
    event('price_change', 7, MS + 20, '2', { side: 'SELL', price: '0.51', size: '30', best_bid: '0.49', best_ask: '0.51' }),
    event('last_trade_price', 8, MS + 100, '1', { side: 'SELL', price: '0.49', size: '40', fee_rate_bps: '700', transaction_hash: `0x${HASH}` }),
    event('book', 9, END * 1000, '1', depth('0.49', '0.51')),
    event('book', 10, END * 1000, '2', depth('0.49', '0.51')),
    event('market_resolved', 11, END * 1000 + 100, null, { id: 'native-id', assets_ids: ['1', '2'], winning_asset_id: '1', winning_outcome: 'Up' }),
  ];
}
const inspect = (events: Record<string, unknown>[]) => inspectPendulumflow(manifest(events.length), events, { maxBookGapMs: 300_000 });

describe('Pendulumflow canonical acquisition boundary', () => {
  test('all seven products retain exact clocks, witness/native identity and source trace while sorting exchange chronology', () => {
    const rows = fixture(), dataset = inspect([...rows].reverse());
    expect(dataset.events.map((e) => String(e.product))).toEqual(rows.map((r) => String(r['eventType'])));
    for (const [p, count] of Object.entries(dataset.quality.products)) expect(count).toBe(rows.filter((r) => r['eventType'] === p).length);
    const original = dataset.events[3]!;
    expect(original.receivedTimeUs).toBe(String(BigInt(MS) * 1000n + 123n));
    expect(original.arrivalSkewUs).toBe('9007199254740993');
    expect(original.sourceWitness).toBe('b');
    expect(original.sourceRef.manifestSha256).toBe(HASH);
    expect(dataset.quality.markets[0]!.resolvedOutcome).toBe('Up');
    expect(dataset.quality.markets[0]!.replayable).toBe(true);
    expect(dataset.quality.completeBookCoverage).toBe(false);
    expect(dataset.quality.markets[0]!.scoringEligible).toBe(false);
  });
  test('exact duplicate native identity is suppressed; equal-clock equal-price distinct native events survive', () => {
    const rows = fixture(), trade = rows[7]!;
    const another = { ...trade, eventId: `${HOUR}:last_trade_price:12`, sequence: '12' };
    const dataset = inspect([...rows, { ...trade }, another]);
    expect(dataset.quality.exactDuplicates).toBe(1);
    expect(dataset.quality.products.last_trade_price).toBe(2);
    expect(dataset.events.filter((e) => e.operation.kind === 'trade')).toHaveLength(2);
    expect(() => inspect([...rows, { ...trade, receivedTimeUs: '1' }])).toThrow('conflicting native event identity');
  });
  test('wrong condition, unmapped token, changed trace and contradictory resolution fail closed', () => {
    const rows = fixture(), book = rows[2]!;
    expect(() => inspect([{ ...book, tokenId: '3' }])).toThrow('unmapped');
    expect(() => inspect([{ ...book, conditionId: `0x${'c'.repeat(64)}` }])).toThrow('unselected');
    expect(() => inspect([{ ...book, sourceRef: { manifestSha256: 'c'.repeat(64), fileSha256Expected: HASH, rowGroup: 0 } }])).toThrow('pins');
    const resolved = rows[10]!, p = resolved['payload'] as Record<string, unknown>;
    expect(() => inspect([{ ...resolved, payload: { ...p, winning_asset_id: '2' } }])).toThrow('resolution identity');
  });
  test('native precision survives padded decimal values, unsupported precision cannot silently round', () => {
    const rows = fixture(), p = rows[2]!['payload'] as Record<string, unknown>;
    rows[2] = { ...rows[2], payload: { ...p, bids: [{ price: '0.490000000000000000', size: '20.000000000' }] } };
    expect(inspect(rows).quality.markets[0]!.replayable).toBe(true);
    rows[2] = { ...rows[2], payload: { ...p, bids: [{ price: '0.4900001', size: '20' }] } };
    expect(() => inspect(rows)).toThrow('six-decimal');
  });
  test('a delta before full snapshot, BBO disagreement and complementary depth disagreement are exclusions', () => {
    const rows = fixture();
    const deltaFirst = rows.filter((r) => r['sequence'] !== '3');
    expect(inspect(deltaFirst).quality.markets[0]!.reasons).toContain('DELTA_BEFORE_SNAPSHOT');
    const bboBad = [...rows], p = rows[4]!['payload'] as Record<string, unknown>;
    bboBad[4] = { ...rows[4], payload: { ...p, best_bid: '0.48', spread: '0.03' } };
    expect(inspect(bboBad).quality.markets[0]!.reasons).toContain('BBO_BOOK_MISMATCH');
    const complementBad = [...rows], bp = rows[3]!['payload'] as Record<string, unknown>;
    complementBad[3] = { ...rows[3], payload: { ...bp, ...depth('0.49', '0.51', '21') } };
    expect(inspect(complementBad).quality.markets[0]!.reasons).toContain('COMPLEMENT_BOOK_MISMATCH');
  });
  test('book gaps and missing terminal token evidence cannot be certified by unrelated events', () => {
    const rows = fixture(), missing = rows.filter((r) => r['sequence'] !== '10');
    const q = inspectPendulumflow(manifest(missing.length), missing).quality.markets[0]!;
    expect(q.reasons).toContain('MISSING_DOWN_TERMINAL_BOOK_EVIDENCE');
    expect(q.reasons).toContain('UP_BOOK_GAP');
    expect(q.replayable).toBe(false);
  });
  test('in-window tick transitions are retained and excluded; post-window transitions do not rewrite initial tick', () => {
    const rows = fixture();
    const tick = event('tick_size_change', 12, MS + 50, '1', { old_tick_size: '0.01', new_tick_size: '0.001' });
    expect(inspect([...rows, tick]).quality.markets[0]!.reasons).toContain('DYNAMIC_TICK_REPLAY_UNSUPPORTED');
    const after = inspect([...rows, { ...tick, eventTimeMs: String(END * 1000 + 200) }]);
    expect(after.quality.markets[0]!.replayable).toBe(true);
    expect(after.quality.markets[0]!.initialTickMicro.Up).toBe('10000');
  });

  test('a completed clock projection is labelled and must cover the selected window', () => {
    const rows = fixture(), projected = manifest(rows.length);
    projected['coverage'] = { allSevenTypesHandled: true, completeSelectedPartitions: false, completeProjectedSelection: true, binance: false, rtdsTwap: false };
    projected['projection'] = { enabled: true, clock: 'exchange-ms', bounds: 'inclusive', unknownClockRetained: 0, lifecycleOutsideRetained: 1, lifecycleUnknownClockRetained: 0, excludedByTime: 0 };
    const market = (projected['markets'] as Record<string, unknown>[])[0]!;
    market['archiveStartMs'] = MS - 2000; market['archiveEndMs'] = END * 1000;
    expect(inspectPendulumflow(projected, rows, { maxBookGapMs: 300_000 }).quality.provenance.selectionExtent).toBe('projected-selection');
    market['archiveStartMs'] = MS + 1;
    expect(() => inspectPendulumflow(projected, rows)).toThrow('does not cover');
    market['archiveStartMs'] = MS; market['archiveEndMs'] = END * 1000;
    expect(() => inspectPendulumflow(projected, rows)).toThrow('outside the declared');
  });
  test('flattened equal-clock delta batches are ambiguous until a later snapshot, not proven BBO corruption', () => {
    const rows = fixture(), up = rows[2]!['payload'] as Record<string, unknown>, down = rows[3]!['payload'] as Record<string, unknown>;
    rows[2] = { ...rows[2], payload: { ...up, bids: [{ price: '0.49', size: '20' }, { price: '0.48', size: '20' }, { price: '0.47', size: '20' }] } };
    rows[3] = { ...rows[3], payload: { ...down, asks: [{ price: '0.51', size: '20' }, { price: '0.52', size: '20' }, { price: '0.53', size: '20' }] } };
    const changes = [
      event('price_change', 12, MS + 20, '1', { side: 'BUY', price: '0.49', size: '0', best_bid: '0.47', best_ask: '0.51' }),
      event('price_change', 13, MS + 20, '1', { side: 'BUY', price: '0.48', size: '0', best_bid: '0.47', best_ask: '0.51' }),
      event('price_change', 14, MS + 20, '2', { side: 'SELL', price: '0.51', size: '0', best_bid: '0.49', best_ask: '0.53' }),
      event('price_change', 15, MS + 20, '2', { side: 'SELL', price: '0.52', size: '0', best_bid: '0.49', best_ask: '0.53' }),
    ];
    const atomicRows = [...rows.filter((r) => !['6', '7'].includes(String(r['sequence']))), ...changes];
    const q = inspect(atomicRows).quality.markets[0]!;
    expect(q.reasons).toContain('AMBIGUOUS_EQUAL_CLOCK_BBO_ORDERING');
    expect(q.reasons).not.toContain('BBO_BOOK_MISMATCH');
    expect(q.replayable).toBe(false);
    expect(q.ambiguityCount).toBe(2);
    expect(q.ambiguities[0]!.resolvedMs).toBe(END * 1000);
    expect(q.ambiguities[0]!.resolvedBySnapshotId).toBe(HOUR + ':book:9');
    const recovered = atomicRows.map((r) => ['3', '4'].includes(String(r['sequence'])) ? { ...r, eventTimeMs: String(MS - 2000) } :
      ['12', '13', '14', '15'].includes(String(r['sequence'])) ? { ...r, eventTimeMs: String(MS - 1000) } : r);
    recovered.push(event('book', 16, MS, '1', depth('0.49', '0.51')), event('book', 17, MS, '2', depth('0.49', '0.51')));
    const recoveredQ = inspect(recovered).quality.markets[0]!;
    expect(recoveredQ.ambiguityCount).toBe(2);
    expect(recoveredQ.replayable).toBe(true);
  });

  test('native 0/1 sentinels match only reconstructed empty sides and remain unchanged in provenance', () => {
    for (const emptySide of ['bid', 'ask'] as const) {
      const bidEmpty = emptySide === 'bid', up = bidEmpty
        ? { bids: [], asks: [{ price: '0.01', size: '20' }] }
        : { bids: [{ price: '0.99', size: '20' }], asks: [] };
      const down = bidEmpty
        ? { bids: [{ price: '0.99', size: '20' }], asks: [] }
        : { bids: [], asks: [{ price: '0.01', size: '20' }] };
      const nativeBbo = { best_bid: bidEmpty ? '0' : '0.99', best_ask: bidEmpty ? '0.01' : '1' };
      const rows = fixture().filter((r) => !['7', '8'].includes(String(r['sequence']))).map((r) => {
        const payload = r['payload'] as Record<string, unknown>;
        if (r['eventType'] === 'book') return { ...r, payload: { ...payload, ...(r['tokenId'] === '1' ? up : down) } };
        if (r['eventType'] === 'best_bid_ask') return { ...r, payload: { ...payload, ...nativeBbo, spread: '0.01' } };
        if (r['eventType'] === 'price_change') return { ...r, payload: { ...payload, ...nativeBbo, side: bidEmpty ? 'SELL' : 'BUY', price: bidEmpty ? '0.01' : '0.99', size: '20' } };
        return r;
      });
      const dataset = inspect(rows), q = dataset.quality.markets[0]!;
      expect(q.replayable).toBe(true);
      expect(q.emptySideSentinelMatches).toBe(2);
      const native = dataset.events.find((e) => e.product === 'price_change')!.original['payload'] as Record<string, unknown>;
      expect(native['best_bid']).toBe(nativeBbo.best_bid);
      expect(native['best_ask']).toBe(nativeBbo.best_ask);
    }
  });
  test('a native zero bid is a mismatch when reconstructed bid depth is nonempty', () => {
    const rows = fixture(), p = rows[4]!['payload'] as Record<string, unknown>;
    rows[4] = { ...rows[4], payload: { ...p, best_bid: '0', spread: '0.51' } };
    const q = inspect(rows).quality.markets[0]!;
    expect(q.violationCounts['BBO_BOOK_MISMATCH']).toBe(1);
    expect(q.replayable).toBe(false);
  });
  test('repeated identical token-tick transitions at one clock retain both identities as nonexcluding tail ambiguity', () => {
    const first = event('tick_size_change', 12, END * 1000 + 1000, '1', { old_tick_size: '0.01', new_tick_size: '0.001' });
    const repeated = event('tick_size_change', 13, END * 1000 + 1000, '1', { old_tick_size: '0.01', new_tick_size: '0.001' });
    const dataset = inspect([...fixture(), first, repeated]), q = dataset.quality.markets[0]!;
    expect(dataset.quality.products.tick_size_change).toBe(3);
    expect(q.diagnosticCounts['AMBIGUOUS_REPEATED_TICK_TRANSITION']).toBe(1);
    expect(q.violationCounts['TICK_CHAIN_CONTRADICTION']).toBeUndefined();
    expect(q.diagnostics[0]!.relatedEventId).toBe(HOUR + ':tick_size_change:12');
    expect(q.diagnostics[0]!.scope).toBe('tail');
    expect(q.replayable).toBe(true);
    const changedClock = inspect([...fixture(), first, { ...repeated, eventTimeMs: String(END * 1000 + 1001) }]).quality.markets[0]!;
    expect(changedClock.violationCounts['TICK_CHAIN_CONTRADICTION']).toBe(1);
    expect(changedClock.violations.find((v) => v.code === 'TICK_CHAIN_CONTRADICTION')!.affectsTradingWindow).toBe(false);
    expect(changedClock.replayable).toBe(true);
  });
  test('pre-snapshot warmup is resolved at opening, while a first snapshot one millisecond late excludes the window', () => {
    const prefix = [
      event('price_change', 12, MS - 500, '1', { side: 'BUY', price: '0.49', size: '30', best_bid: '0.49', best_ask: '0.51' }),
      event('best_bid_ask', 13, MS - 400, '1', { best_bid: '0.49', best_ask: '0.51', spread: '0.02' }),
    ];
    const q = inspect([...fixture(), ...prefix]).quality.markets[0]!;
    expect(q.diagnosticCounts['WARMUP_DELTA_BEFORE_SNAPSHOT']).toBe(1);
    expect(q.diagnosticCounts['WARMUP_BBO_BEFORE_SNAPSHOT']).toBe(1);
    expect(q.violationCounts['DELTA_BEFORE_SNAPSHOT']).toBeUndefined();
    expect(q.diagnostics[0]!.resolvedMs).toBe(MS);
    expect(q.diagnostics[0]!.resolvedBySnapshotId).toBe(HOUR + ':book:3');
    expect(q.replayable).toBe(true);
    const late = fixture().map((r) => r['sequence'] === '3' ? { ...r, eventTimeMs: String(MS + 1) } : r);
    const lateQ = inspect([...late, ...prefix]).quality.markets[0]!;
    expect(lateQ.reasons).toContain('MISSING_UP_OPENING_SNAPSHOT');
    expect(lateQ.replayable).toBe(false);
  });

  test('tail-only book parity failures do not relabel an earlier window, while resolution identity is global', () => {
    const invalid = event('best_bid_ask', 12, END * 1000 + 50, '1', { best_bid: '0.48', best_ask: '0.51', spread: '0.03' });
    const tail = inspect([...fixture(), invalid]).quality.markets[0]!;
    expect(tail.violationCounts['BBO_BOOK_MISMATCH']).toBe(1);
    expect(tail.tradingViolationCounts['BBO_BOOK_MISMATCH']).toBeUndefined();
    expect(tail.violations[0]!.affectsTradingWindow).toBe(false);
    expect(tail.replayable).toBe(true);
    const inside = inspect([...fixture(), { ...invalid, eventTimeMs: String(MS + 101) }]).quality.markets[0]!;
    expect(inside.tradingViolationCounts['BBO_BOOK_MISMATCH']).toBe(1);
    expect(inside.replayable).toBe(false);
    const conflicting = event('market_resolved', 13, END * 1000 + 200, null, { id: 'native-id', assets_ids: ['1', '2'], winning_asset_id: '2', winning_outcome: 'Down' });
    const resolution = inspect([...fixture(), conflicting]).quality.markets[0]!;
    expect(resolution.tradingViolationCounts['RESOLUTION_CONTRADICTION']).toBe(1);
    expect(resolution.replayable).toBe(false);
  });
});
