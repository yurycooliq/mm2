import { createHash } from 'node:crypto';
import { createReadStream, statSync } from 'node:fs';
import { join } from 'node:path';
import { catchError, parseCryptoUpDownSlug, readLines } from './runtime.ts';
import { readFile } from 'node:fs/promises';
type Outcome = 'Up' | 'Down';

const TYPES = ['book', 'price_change', 'best_bid_ask', 'last_trade_price', 'new_market', 'market_resolved', 'tick_size_change'] as const;
type Product = typeof TYPES[number];
type Level = readonly [priceMicro: bigint, sharesMicro: bigint];
type Bbo = { bid: bigint | null; ask: bigint | null };
type Operation =
  | { kind: 'snapshot'; bids: Level[]; asks: Level[] }
  | { kind: 'change'; side: 'B' | 'S'; price: bigint; size: bigint; bbo: Bbo }
  | { kind: 'bbo'; bbo: Bbo; spread: bigint | null }
  | { kind: 'trade'; price: bigint; size: bigint; side: 'BUY' | 'SELL'; feeRateBps: string | null; transactionHash: string | null }
  | { kind: 'listed'; nativeId: string | null; slug: string; tokens: string[]; outcomes: string[] }
  | { kind: 'resolved'; nativeId: string | null; tokens: string[]; winnerToken: string; winnerOutcome: string }
  | { kind: 'tick'; oldTick: bigint; newTick: bigint };
export interface ArchiveMarket {
  conditionId: string;
  slug: string;
  windowStart: number;
  windowEnd: number;
  tokens: { tokenId: string; outcome: Outcome }[];
  metadataSource: unknown;
  selectionMetadata: Readonly<Record<string, unknown>>;
}
/** The selected copy's microsecond receive clock is provenance, never a strategy latency measurement. */
export interface ArchiveEvent {
  id: string;
  product: Product;
  conditionId: string;
  tokenId: string | null;
  outcome: Outcome | null;
  exchangeMs: number | null;
  receivedTimeUs: string;
  sequence: string;
  partitionHour: string;
  sourceWitness: string | null;
  witnessSet: string | null;
  arrivalSkewUs: string | null;
  sourceRef: { manifestSha256: string; fileSha256Expected: string; rowGroup: number };
  operation: Operation;
  /** Keeps native identifiers and trace fields without converting provider DTOs into engine input. */
  original: Readonly<Record<string, unknown>>;
}
export interface ArchiveMarketQuality {
  conditionId: string;
  slug: string;
  replayable: boolean;
  scoringEligible: false;
  reasons: string[];
  violations: { code: string; eventId: string; affectsTradingWindow: boolean }[];
  violationCounts: Record<string, number>;
  tradingViolationCounts: Record<string, number>;
  diagnosticCounts: Record<string, number>;
  diagnostics: { code: string; eventId: string; relatedEventId: string | null; exchangeMs: number; outcome: Outcome;
    scope: 'warmup' | 'window' | 'tail'; resolvedBySnapshotId: string | null; resolvedMs: number | null }[];
  emptySideSentinelMatches: number;
  ambiguityCount: number;
  ambiguities: { code: string; eventId: string; exchangeMs: number; outcome: Outcome; resolvedBySnapshotId: string | null; resolvedMs: number | null }[];
  snapshots: Record<Outcome, number>;
  firstSnapshotMs: Record<Outcome, number | null>;
  lastBookEvidenceMs: Record<Outcome, number | null>;
  maxBookGapMs: Record<Outcome, number>;
  bboChecks: number;
  complementChecks: number;
  initialTickMicro: Record<Outcome, string | null>;
  tickChanges: number;
  resolvedOutcome: Outcome | null;
}
export interface ArchiveQuality {
  source: 'pendulumflow-v3';
  captureMode: 'merged-union';
  ordering: 'exchange-ms/partition/collector-sequence/event-id; equal exchange clocks do not establish FIFO';
  inputEvents: number;
  distinctEvents: number;
  exactDuplicates: number;
  products: Record<Product, number>;
  crossWitnessEqualClockGroups: number;
  maxBookGapMs: number;
  provenance: {
    datasetId: string;
    selectedEventsSha256: string;
    wholeArchiveChecksumVerified: false;
    selectionExtent: 'selected-partitions' | 'projected-selection';
    projection: Projection | null;
    markets: ArchiveMarket[];
    partitions: { hour: string; manifestSha256: string; fileSha256Expected: string }[];
  };
  completeBookCoverage: false;
  externalFeeds: { binance: false; rtdsTwap: false };
  liveEligible: false;
  markets: ArchiveMarketQuality[];
}
export interface ArchiveDataset {
  markets: ArchiveMarket[];
  events: ArchiveEvent[];
  quality: ArchiveQuality;
}
type Projection = { clock: 'exchange-ms'; bounds: 'inclusive'; unknownClockRetained: number; lifecycleOutsideRetained: number; lifecycleUnknownClockRetained: number; excludedByTime: number };
type Hour = { manifestSha256: string; fileSha256Expected: string; products: Map<Product, number[]> };
type Manifest = { datasetId: string; projection: Projection | null; selectionExtent: 'selected-partitions' | 'projected-selection'; markets: ArchiveMarket[]; hours: Map<string, Hour>; eventCount: number; eventsSha256: string; eventFile: string };
const MICRO = 1_000_000n;
const uint = (value: unknown, label: string, max = 2n ** 256n - 1n): string => {
  if (typeof value !== 'string' || value.length > 80 || !/^(0|[1-9]\d*)$/.test(value) || BigInt(value) > max) throw new Error(`invalid ${label}`);
  return value;
};
const text = (value: unknown, label: string): string => {
  if (typeof value !== 'string' || !value) throw new Error(`invalid ${label}`);
  return value;
};
const object = (value: unknown, label: string): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`invalid ${label}`);
  return value as Record<string, unknown>;
};
const integer = (value: unknown, label: string): number => {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error(`invalid ${label}`);
  return value;
};
const hash = (value: unknown, label: string, prefix = false): string => {
  const valueText = text(value, label);
  if (!(prefix ? /^0x[0-9a-f]{64}$/i : /^[0-9a-f]{64}$/i).test(valueText)) throw new Error(`invalid ${label}`);
  return valueText.toLowerCase();
};
const nullableText = (value: unknown, label: string): string | null => value === null ? null : text(value, label);
const fixed = (value: unknown, label: string, price = false): bigint => {
  if (typeof value !== 'string' || value.length > 128 || !/^\d+(\.\d+)?$/.test(value)) throw new Error(`invalid ${label}`);
  const [whole, fraction = ''] = value.split('.');
  if (fraction.slice(6).replace(/0/g, '').length > 0) throw new Error(`${label} exceeds exact six-decimal replay precision`);
  const n = BigInt(whole!) * MICRO + BigInt(fraction.slice(0, 6).padEnd(6, '0'));
  if (n > BigInt(Number.MAX_SAFE_INTEGER) || price && n > MICRO) throw new Error(`invalid ${label}`);
  return n;
};
const optionalPrice = (value: unknown, label: string): bigint | null => value === null ? null : fixed(value, label, true);
const bbo = (p: Record<string, unknown>): Bbo => ({ bid: optionalPrice(p['best_bid'], 'best_bid'), ask: optionalPrice(p['best_ask'], 'best_ask') });
const strings = (value: unknown, label: string): string[] => {
  if (!Array.isArray(value)) throw new Error(`invalid ${label}`);
  return value.map((v: unknown) => text(v, label));
};
const tokens = (value: unknown): string[] => strings(value, 'assets_ids').map((v) => uint(v, 'asset token'));
const levels = (value: unknown): Level[] => {
  if (!Array.isArray(value)) throw new Error('invalid snapshot levels');
  const seen = new Set<bigint>();
  return value.map((v: unknown) => {
    const row = object(v, 'snapshot level');
    const p = fixed(row['price'], 'level price', true), q = fixed(row['size'], 'level shares');
    if (seen.has(p)) throw new Error('duplicate snapshot price');
    seen.add(p);
    return [p, q] as const;
  });
};
function parseManifest(value: unknown): Manifest {
  const v = object(value, 'dataset manifest');
  if (v['schemaVersion'] !== 1 || v['source'] !== 'pendulumflow-v3' || v['captureMode'] !== 'merged-union' || v['status'] !== 'acquired-unvalidated')
    throw new Error('unsupported or incomplete canonical dataset');
  if (v['liveEligible'] !== false) throw new Error('archive cannot certify live eligibility');
  const coverage = object(v['coverage'], 'acquisition coverage');
  const projected = coverage['completeSelectedPartitions'] === false && coverage['completeProjectedSelection'] === true;
  if (coverage['allSevenTypesHandled'] !== true || !(coverage['completeSelectedPartitions'] === true || projected) || coverage['binance'] !== false || coverage['rtdsTwap'] !== false)
    throw new Error('unsupported acquisition/source-availability contract');
  let projection: Projection | null = null;
  if (projected) {
    const p = object(v['projection'], 'projected selection contract');
    if (p['enabled'] !== true || p['clock'] !== 'exchange-ms' || p['bounds'] !== 'inclusive') throw new Error('unsupported projected selection clock/bounds');
    projection = { clock: 'exchange-ms', bounds: 'inclusive', unknownClockRetained: integer(p['unknownClockRetained'], 'unknown clock count'),
      lifecycleOutsideRetained: integer(p['lifecycleOutsideRetained'], 'lifecycle outside count'),
      lifecycleUnknownClockRetained: integer(p['lifecycleUnknownClockRetained'], 'lifecycle unknown clock count'),
      excludedByTime: integer(p['excludedByTime'], 'projection exclusion count') };
  }
  if (!Array.isArray(v['markets']) || v['markets'].length === 0 || !Array.isArray(v['hours']) || v['hours'].length === 0)
    throw new Error('market identities and source partitions are required');
  const conditions = new Set<string>(), tokenIds = new Set<string>();
  const markets = v['markets'].map((item: unknown): ArchiveMarket => {
    const m = object(item, 'market identity');
    const conditionId = hash(m['conditionId'], 'condition id', true), slug = text(m['slug'], 'market slug');
    const start = integer(m['windowStart'], 'window start'), end = integer(m['windowEnd'], 'window end');
    const series = parseCryptoUpDownSlug(slug);
    if (!series || series.startTs !== start || start + series.durationSec !== end || conditions.has(conditionId)) throw new Error('conflicting market identity/window');
    if (projected) {
      const archiveStart = integer(m['archiveStartMs'], 'projection opening bound'), archiveEnd = integer(m['archiveEndMs'], 'projection closing bound');
      if (archiveStart > start * 1000 || archiveEnd < end * 1000) throw new Error('projected selection does not cover the requested window');
    }
    conditions.add(conditionId);
    if (!Array.isArray(m['tokens']) || m['tokens'].length !== 2) throw new Error('two outcome tokens are required');
    const outcomes = new Set<string>();
    const mappedTokens = m['tokens'].map((itemToken: unknown): { tokenId: string; outcome: Outcome } => {
      const t = object(itemToken, 'outcome token'), tokenId = uint(t['tokenId'], 'token id');
      if (t['outcome'] !== 'Up' && t['outcome'] !== 'Down' || outcomes.has(t['outcome']) || tokenIds.has(tokenId)) throw new Error('conflicting outcome token identity');
      outcomes.add(t['outcome']); tokenIds.add(tokenId);
      return { tokenId, outcome: t['outcome'] };
    });
    if (m['metadataSource'] === undefined) throw new Error('market metadata provenance is required');
    return { conditionId, slug, windowStart: start, windowEnd: end, tokens: mappedTokens, metadataSource: m['metadataSource'], selectionMetadata: m };
  });
  const hours = new Map<string, Hour>();
  for (const item of v['hours']) {
    const h = object(item, 'source hour'), hour = text(h['hour'], 'partition hour');
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(hour) || hours.has(hour) || h['manifestFooterVerified'] !== true || h['wholeFileChecksumVerified'] !== false)
      throw new Error('invalid source partition verification');
    const rawProducts = object(h['products'], 'source products'), products = new Map<Product, number[]>();
    for (const product of TYPES) {
      const p = object(rawProducts[product], `${product} acquisition`);
      hash(p['expectedProductSha256'], 'expected product hash');
      integer(p['rowCount'], 'product row count'); integer(p['selectedRows'], 'selected row count');
      if (!Array.isArray(p['selectedRowGroups'])) throw new Error('selected row groups are required');
      products.set(product, p['selectedRowGroups'].map((n: unknown) => integer(n, 'row group')));
    }
    hours.set(hour, { manifestSha256: hash(h['manifestSha256'], 'manifest hash'), fileSha256Expected: hash(h['fileSha256Expected'], 'file hash'), products });
  }
  const eventFile = text(v['eventFile'], 'event file');
  if (eventFile !== 'events.jsonl') throw new Error('unsupported event file path');
  return { datasetId: text(v['datasetId'], 'dataset id'), projection, selectionExtent: projected ? 'projected-selection' : 'selected-partitions', markets, hours, eventFile, eventCount: integer(v['eventCount'], 'event count'), eventsSha256: hash(v['eventsSha256'], 'events hash') };
}
function parseEvent(value: unknown, manifest: Manifest): ArchiveEvent {
  const v = object(value, 'canonical event');
  if (v['schemaVersion'] !== 1 || v['source'] !== 'pendulumflow-v3' || v['clockPrecision'] !== 'ms' || !TYPES.includes(v['eventType'] as Product))
    throw new Error('unsupported canonical event schema');
  const product = v['eventType'] as Product, partitionHour = text(v['partitionHour'], 'partition hour');
  const hour = manifest.hours.get(partitionHour);
  if (!hour) throw new Error('event references an undeclared partition');
  const sequence = uint(v['sequence'], 'collector sequence', 2n ** 64n - 1n), id = text(v['eventId'], 'event id');
  if (id !== `${partitionHour}:${product}:${sequence}`) throw new Error('event id does not match its native provenance');
  const conditionId = hash(v['conditionId'], 'condition id', true);
  const market = manifest.markets.find((m) => m.conditionId === conditionId);
  if (!market) throw new Error('event references an unselected market');
  const tokenId = v['tokenId'] === null ? null : uint(v['tokenId'], 'token id');
  const token = market.tokens.find((t) => t.tokenId === tokenId);
  if (tokenId !== null && !token) throw new Error('unmapped outcome token');
  const exchangeMs = v['eventTimeMs'] === null ? null : Number(uint(v['eventTimeMs'], 'exchange milliseconds', BigInt(Number.MAX_SAFE_INTEGER)));
  const receivedTimeUs = uint(v['receivedTimeUs'], 'receive microseconds', 2n ** 63n - 1n);
  const sourceWitness = nullableText(v['sourceWitness'], 'source witness'), witnessSet = nullableText(v['witnessSet'], 'witness set');
  if (sourceWitness !== null && witnessSet !== null && !witnessSet.split('|').includes(sourceWitness)) throw new Error('selected witness is absent from witness set');
  const arrivalSkewUs = v['arrivalSkewUs'] === null ? null : uint(v['arrivalSkewUs'], 'arrival skew microseconds', 2n ** 63n - 1n);
  const ref = object(v['sourceRef'], 'source trace'), rowGroup = integer(ref['rowGroup'], 'trace row group');
  const sourceRef = { rowGroup, manifestSha256: hash(ref['manifestSha256'], 'trace manifest hash'), fileSha256Expected: hash(ref['fileSha256Expected'], 'trace file hash') };
  if (sourceRef.manifestSha256 !== hour.manifestSha256 || sourceRef.fileSha256Expected !== hour.fileSha256Expected || !hour.products.get(product)!.includes(rowGroup))
    throw new Error('event trace contradicts acquisition pins');
  const p = object(v['payload'], 'event payload');
  if (hash(p['market'], 'payload condition id', true) !== conditionId) throw new Error('payload condition identity mismatch');
  const tokenProduct = !['new_market', 'market_resolved'].includes(product);
  if (tokenProduct && (tokenId === null || uint(p['asset_id'], 'payload token') !== tokenId) || !tokenProduct && tokenId !== null)
    throw new Error('payload token identity mismatch');
  if (manifest.selectionExtent === 'projected-selection' && tokenProduct && exchangeMs !== null &&
    (exchangeMs < (market.selectionMetadata['archiveStartMs'] as number) || exchangeMs > (market.selectionMetadata['archiveEndMs'] as number)))
    throw new Error('canonical event is outside the declared clock projection');
  let operation: Operation;
  if (product === 'book') operation = { kind: 'snapshot', bids: levels(p['bids']), asks: levels(p['asks']) };
  else if (product === 'price_change') {
    if (p['side'] !== 'BUY' && p['side'] !== 'SELL') throw new Error('invalid book change side');
    operation = { kind: 'change', side: p['side'] === 'BUY' ? 'B' : 'S', price: fixed(p['price'], 'change price', true), size: fixed(p['size'], 'change shares'), bbo: bbo(p) };
  } else if (product === 'best_bid_ask') operation = { kind: 'bbo', bbo: bbo(p), spread: p['spread'] === null ? null : fixed(p['spread'], 'spread', true) };
  else if (product === 'last_trade_price') {
    if (p['side'] !== 'BUY' && p['side'] !== 'SELL') throw new Error('invalid trade side');
    const feeRateBps = p['fee_rate_bps'] === null ? null : uint(p['fee_rate_bps'], 'fee rate bps', 10_000n);
    operation = { kind: 'trade', side: p['side'], price: fixed(p['price'], 'trade price', true), size: fixed(p['size'], 'trade shares'), feeRateBps,
      transactionHash: p['transaction_hash'] === null ? null : hash(p['transaction_hash'], 'transaction hash', true) };
  } else if (product === 'tick_size_change') {
    const oldTick = fixed(p['old_tick_size'], 'old tick', true), newTick = fixed(p['new_tick_size'], 'new tick', true);
    if (oldTick <= 0n || newTick <= 0n || oldTick > 10_000n || newTick > 10_000n) throw new Error('invalid tick size');
    operation = { kind: 'tick', oldTick, newTick };
  } else {
    const nativeId = nullableText(p['id'], 'native market id'), assetTokens = tokens(p['assets_ids']);
    if (assetTokens.length !== 2 || new Set(assetTokens).size !== 2 || assetTokens.some((idToken) => !market.tokens.some((t) => t.tokenId === idToken)))
      throw new Error('native market token identity mismatch');
    if (product === 'new_market') {
      const slug = text(p['slug'], 'native slug'), outcomes = strings(p['outcomes'], 'native outcomes');
      if (slug !== market.slug || outcomes.length !== 2 || outcomes.some((o, i) => !market.tokens.some((t) => t.tokenId === assetTokens[i] && t.outcome.toLowerCase() === o.toLowerCase())))
        throw new Error('native market metadata contradicts selection');
      operation = { kind: 'listed', nativeId, slug, tokens: assetTokens, outcomes };
    } else {
      const winnerToken = uint(p['winning_asset_id'], 'winner token'), winnerOutcome = text(p['winning_outcome'], 'winner outcome');
      if (!market.tokens.some((t) => t.tokenId === winnerToken && t.outcome.toLowerCase() === winnerOutcome.toLowerCase())) throw new Error('resolution identity contradiction');
      operation = { kind: 'resolved', nativeId, tokens: assetTokens, winnerToken, winnerOutcome };
    }
  }
  return { id, product, partitionHour, sequence, conditionId, tokenId, outcome: token?.outcome ?? null, exchangeMs, receivedTimeUs,
    sourceWitness, witnessSet, arrivalSkewUs, sourceRef, operation, original: v };
}
const fingerprint = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(fingerprint).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${fingerprint(v)}`).join(',')}}`;
  return JSON.stringify(value);
};
const before = (a: ArchiveEvent, b: ArchiveEvent): number => (a.exchangeMs ?? Infinity) - (b.exchangeMs ?? Infinity) ||
  a.partitionHour.localeCompare(b.partitionHour) || (BigInt(a.sequence) < BigInt(b.sequence) ? -1 : BigInt(a.sequence) > BigInt(b.sequence) ? 1 : a.id.localeCompare(b.id));
type Book = { B: Map<bigint, bigint>; S: Map<bigint, bigint>; initialized: boolean; stamp: number | null };
const best = (book: Book, side: 'B' | 'S'): bigint | null => {
  let result: bigint | null = null;
  for (const [p, q] of book[side]) if (q > 0n && (result === null || (side === 'B' ? p > result : p < result))) result = p;
  return result;
};
// The native feed uses 0/1 at an empty bid/ask boundary; actual nonempty levels remain authoritative.
const matchesBbo = (book: Book, expected: Bbo): boolean => {
  const bid = best(book, 'B'), ask = best(book, 'S');
  return (bid === expected.bid || bid === null && expected.bid === 0n) &&
    (ask === expected.ask || ask === null && expected.ask === MICRO);
};
const equalLevels = (left: Map<bigint, bigint>, right: Map<bigint, bigint>): boolean =>
  left.size === right.size && [...left].every(([p, q]) => right.get(MICRO - p) === q);
function quality(manifest: Manifest, events: ArchiveEvent[], inputEvents: number, duplicates: number, maxGap: number): ArchiveQuality {
  const markets = manifest.markets;
  const products = Object.fromEntries(TYPES.map((p) => [p, 0])) as Record<Product, number>;
  const witnesses = new Map<number, Set<string>>();
  const reports = markets.map((market): ArchiveMarketQuality => {
    const report: ArchiveMarketQuality = { conditionId: market.conditionId, slug: market.slug, replayable: true, scoringEligible: false,
      reasons: ['CAPTURE_COMPLETENESS_UNPROVEN', 'MERGED_WITNESS_RECEIVE_CLOCK', 'BINANCE_UNAVAILABLE', 'RTDS_TWAP_UNAVAILABLE', 'SETTLEMENT_PROVENANCE_UNRECONCILED'],
      violations: [], violationCounts: {}, tradingViolationCounts: {}, diagnosticCounts: {}, diagnostics: [], emptySideSentinelMatches: 0, ambiguityCount: 0, ambiguities: [], snapshots: { Up: 0, Down: 0 }, firstSnapshotMs: { Up: null, Down: null }, lastBookEvidenceMs: { Up: null, Down: null },
      maxBookGapMs: { Up: 0, Down: 0 }, bboChecks: 0, complementChecks: 0, initialTickMicro: { Up: null, Down: null }, tickChanges: 0, resolvedOutcome: null };
    const books: Record<Outcome, Book> = { Up: { B: new Map(), S: new Map(), initialized: false, stamp: null }, Down: { B: new Map(), S: new Map(), initialized: false, stamp: null } };
    const ticks: Record<Outcome, bigint | null> = { Up: null, Down: null };
    const lastTick: Record<Outcome, { oldTick: bigint; newTick: bigint; ms: number; eventId: string } | null> = { Up: null, Down: null };
    let nativeId: string | null = null;
    const violation = (code: string, e: ArchiveEvent, affectsTradingWindow = e.exchangeMs === null || e.exchangeMs < market.windowEnd * 1000): void => {
      report.violationCounts[code] = (report.violationCounts[code] ?? 0) + 1;
      if (report.violations.length < 1000) report.violations.push({ code, eventId: e.id, affectsTradingWindow });
      if (affectsTradingWindow) {
        report.tradingViolationCounts[code] = (report.tradingViolationCounts[code] ?? 0) + 1;
        report.replayable = false;
      }
    };
    const diagnostic = (code: string, e: ArchiveEvent, relatedEventId: string | null = null): void => {
      const ms = e.exchangeMs!;
      report.diagnosticCounts[code] = (report.diagnosticCounts[code] ?? 0) + 1;
      if (report.diagnostics.length < 1000) report.diagnostics.push({ code, eventId: e.id, relatedEventId, exchangeMs: ms, outcome: e.outcome!,
        scope: ms < market.windowStart * 1000 ? 'warmup' : ms >= market.windowEnd * 1000 ? 'tail' : 'window',
        resolvedBySnapshotId: null, resolvedMs: null });
    };
    const beforeSnapshot = (code: string, e: ArchiveEvent): void => {
      if (e.exchangeMs! < market.windowStart * 1000) diagnostic('WARMUP_' + code, e);
      else violation(code, e);
    };
    type Observation = { expected: Bbo; event: ArchiveEvent; matchedImmediately: boolean };
    const observations: Record<Outcome, Observation[]> = { Up: [], Down: [] };
    const changes: Record<Outcome, number> = { Up: 0, Down: 0 };
    const unresolved: Record<Outcome, ArchiveMarketQuality['ambiguities'][number] | null> = { Up: null, Down: null };
    let groupMs: number | null = null, groupEvent: ArchiveEvent | null = null;
    const checkBbo = (book: Book, expected: Bbo, e: ArchiveEvent): void => {
      if (!book.initialized) { beforeSnapshot('BBO_BEFORE_SNAPSHOT', e); return; }
      report.bboChecks++;
      if (best(book, 'B') === null && expected.bid === 0n) report.emptySideSentinelMatches++;
      if (best(book, 'S') === null && expected.ask === MICRO) report.emptySideSentinelMatches++;
      observations[e.outcome!].push({ expected, event: e, matchedImmediately: matchesBbo(book, expected) });
    };
    const flushGroup = (): void => {
      if (groupMs === null || groupEvent === null) return;
      for (const outcome of ['Up', 'Down'] as const) {
        const book = books[outcome], checks = observations[outcome];
        const mismatch = checks.find((o) => !o.matchedImmediately);
        if (mismatch) {
          const finalMatches = checks.some((o) => matchesBbo(book, o.expected));
          // Millisecond ties without a packet identity cannot prove these rows are one atomic update.
          if (finalMatches || changes[outcome] > 1) {
            report.ambiguityCount++;
            const ambiguous = { code: 'AMBIGUOUS_EQUAL_CLOCK_BBO_ORDERING', eventId: mismatch.event.id, exchangeMs: groupMs, outcome,
              resolvedBySnapshotId: null, resolvedMs: null };
            unresolved[outcome] ??= ambiguous;
            if (report.ambiguities.length < 1000) report.ambiguities.push(ambiguous);
          } else violation('BBO_BOOK_MISMATCH', mismatch.event);
        }
        if (book.initialized && book.stamp === groupMs) {
          const bid = best(book, 'B'), ask = best(book, 'S');
          if (bid !== null && ask !== null && bid >= ask) violation('CROSSED_OR_LOCKED_BOOK', groupEvent);
        }
        observations[outcome] = []; changes[outcome] = 0;
      }
      if (books.Up.initialized && books.Down.initialized && books.Up.stamp === groupMs && books.Down.stamp === groupMs) {
        report.complementChecks++;
        if (!equalLevels(books.Up.B, books.Down.S) || !equalLevels(books.Up.S, books.Down.B)) violation('COMPLEMENT_BOOK_MISMATCH', groupEvent);
      }
    };
    const marketEvents = events.filter((event) => event.conditionId === market.conditionId);
    for (const e of marketEvents) {
      products[e.product]++;
      if (e.exchangeMs === null) { violation('MISSING_EXCHANGE_CLOCK', e); continue; }
      const ms = e.exchangeMs, op = e.operation;
      if (groupMs !== ms) { flushGroup(); groupMs = ms; }
      groupEvent = e;
      if (e.sourceWitness !== null) { const group = witnesses.get(ms) ?? new Set(); group.add(e.sourceWitness); witnesses.set(ms, group); }
      if (op.kind === 'listed' || op.kind === 'resolved') {
        if (nativeId !== null && op.nativeId !== null && nativeId !== op.nativeId) violation('NATIVE_MARKET_ID_CONFLICT', e, true);
        if (op.nativeId !== null) nativeId = op.nativeId;
        if (op.kind === 'resolved') {
          const winner = market.tokens.find((t) => t.tokenId === op.winnerToken)!.outcome;
          if (ms < market.windowEnd * 1000 || report.resolvedOutcome !== null && report.resolvedOutcome !== winner) violation('RESOLUTION_CONTRADICTION', e, true);
          report.resolvedOutcome = winner;
        }
        continue;
      }
      const outcome = e.outcome!, book = books[outcome];
      if (op.kind === 'tick') {
        const previous = lastTick[outcome];
        if (ticks[outcome] !== null && ticks[outcome] !== op.oldTick) {
          if (previous !== null && previous.ms === ms && previous.oldTick === op.oldTick && previous.newTick === op.newTick)
            diagnostic('AMBIGUOUS_REPEATED_TICK_TRANSITION', e, previous.eventId);
          else violation('TICK_CHAIN_CONTRADICTION', e, ms < market.windowEnd * 1000);
        }
        if (ms <= market.windowStart * 1000) report.initialTickMicro[outcome] = op.newTick.toString();
        ticks[outcome] = op.newTick; lastTick[outcome] = { oldTick: op.oldTick, newTick: op.newTick, ms, eventId: e.id }; report.tickChanges++;
        if (ms >= market.windowStart * 1000 && ms < market.windowEnd * 1000) violation('DYNAMIC_TICK_REPLAY_UNSUPPORTED', e);
        continue;
      }
      if (op.kind === 'trade') {
        if (!book.initialized) beforeSnapshot('TRADE_BEFORE_SNAPSHOT', e);
        continue;
      }
      if (op.kind === 'snapshot') {
        for (const entry of report.diagnostics) if (entry.outcome === outcome && entry.code.startsWith('WARMUP_') && entry.resolvedMs === null) {
          entry.resolvedBySnapshotId = e.id; entry.resolvedMs = ms;
        }
        const pending = unresolved[outcome];
        if (pending && ms > pending.exchangeMs) {
          for (const ambiguity of report.ambiguities) if (ambiguity.outcome === outcome && ambiguity.resolvedMs === null) {
            ambiguity.resolvedBySnapshotId = e.id; ambiguity.resolvedMs = ms;
          }
          if (pending.exchangeMs < market.windowEnd * 1000 && ms > market.windowStart * 1000) report.replayable = false;
          unresolved[outcome] = null;
        }
        changes[outcome]++;
        book.B = new Map(op.bids.filter(([, q]) => q > 0n)); book.S = new Map(op.asks.filter(([, q]) => q > 0n)); book.initialized = true;
        report.snapshots[outcome]++; report.firstSnapshotMs[outcome] ??= ms;
      } else if (op.kind === 'change') {
        if (!book.initialized) { beforeSnapshot('DELTA_BEFORE_SNAPSHOT', e); continue; }
        changes[outcome]++;
        if (op.size === 0n) book[op.side].delete(op.price); else book[op.side].set(op.price, op.size);
        checkBbo(book, op.bbo, e);
      } else {
        checkBbo(book, op.bbo, e);
        if (op.spread !== null && op.bbo.bid !== null && op.bbo.ask !== null && op.spread !== op.bbo.ask - op.bbo.bid) violation('BBO_SPREAD_CONTRADICTION', e);
      }
      book.stamp = ms;
      if (ms >= market.windowStart * 1000) {
        const end = market.windowEnd * 1000;
        report.maxBookGapMs[outcome] = Math.max(report.maxBookGapMs[outcome], Math.min(ms, end) - Math.min(Math.max(report.lastBookEvidenceMs[outcome] ?? market.windowStart * 1000, market.windowStart * 1000), end));
        report.lastBookEvidenceMs[outcome] = ms;
      } else if (ms < market.windowStart * 1000) report.lastBookEvidenceMs[outcome] = ms;
    }
    flushGroup();
    for (const outcome of ['Up', 'Down'] as const) {
      if (report.firstSnapshotMs[outcome] === null || report.firstSnapshotMs[outcome]! > market.windowStart * 1000) report.reasons.push(`MISSING_${outcome.toUpperCase()}_OPENING_SNAPSHOT`);
      if (report.lastBookEvidenceMs[outcome] === null || report.lastBookEvidenceMs[outcome]! < market.windowEnd * 1000) report.reasons.push(`MISSING_${outcome.toUpperCase()}_TERMINAL_BOOK_EVIDENCE`);
      report.maxBookGapMs[outcome] = Math.max(report.maxBookGapMs[outcome], market.windowEnd * 1000 - Math.max(report.lastBookEvidenceMs[outcome] ?? market.windowStart * 1000, market.windowStart * 1000));
      if (report.maxBookGapMs[outcome] > maxGap) report.reasons.push(`${outcome.toUpperCase()}_BOOK_GAP`);
      if (report.initialTickMicro[outcome] === null) report.reasons.push(`INITIAL_${outcome.toUpperCase()}_TICK_UNPROVEN`);
      if (unresolved[outcome] && unresolved[outcome]!.exchangeMs < market.windowEnd * 1000) report.replayable = false;
    }
    if (report.complementChecks === 0) report.reasons.push('COMPLEMENT_PARITY_UNCHECKED');
    if (report.resolvedOutcome === null) report.reasons.push('ARCHIVE_RESOLUTION_MISSING');
    if (report.initialTickMicro.Up !== null && report.initialTickMicro.Down !== null && report.initialTickMicro.Up !== report.initialTickMicro.Down) {
      report.reasons.push('OUTCOME_INITIAL_TICK_MISMATCH'); report.replayable = false;
    }
    if (report.ambiguityCount > 0) report.reasons.push('AMBIGUOUS_EQUAL_CLOCK_BBO_ORDERING');
    report.reasons.push(...Object.keys(report.tradingViolationCounts));
    if (report.reasons.some((reason) => /OPENING_SNAPSHOT|TERMINAL_BOOK_EVIDENCE|BOOK_GAP|PARITY_UNCHECKED/.test(reason))) report.replayable = false;
    return report;
  });
  return { source: 'pendulumflow-v3', captureMode: 'merged-union', ordering: 'exchange-ms/partition/collector-sequence/event-id; equal exchange clocks do not establish FIFO',
    inputEvents, distinctEvents: events.length, exactDuplicates: duplicates, products, crossWitnessEqualClockGroups: [...witnesses.values()].filter((group) => group.size > 1).length,
    maxBookGapMs: maxGap, provenance: { datasetId: manifest.datasetId, selectedEventsSha256: manifest.eventsSha256, wholeArchiveChecksumVerified: false, selectionExtent: manifest.selectionExtent, projection: manifest.projection, markets,
      partitions: [...manifest.hours].map(([hour, pins]) => ({ hour, manifestSha256: pins.manifestSha256, fileSha256Expected: pins.fileSha256Expected })) },
    completeBookCoverage: false, externalFeeds: { binance: false, rtdsTwap: false }, liveEligible: false, markets: reports };
}

/** Bounded, strict acquisition boundary. Distinct native identities survive equal timestamps/prices. */
export function inspectPendulumflow(manifestValue: unknown, input: readonly unknown[], options: { maxEvents?: number; maxBookGapMs?: number } = {}): ArchiveDataset {
  const manifest = parseManifest(manifestValue), maxEvents = options.maxEvents ?? 1_000_000, gap = options.maxBookGapMs ?? 1_000;
  if (!Number.isSafeInteger(maxEvents) || maxEvents <= 0 || !Number.isFinite(gap) || gap <= 0) throw new Error('invalid archive inspection bounds');
  if (input.length > maxEvents || input.length !== manifest.eventCount) throw new Error('canonical event count/bound mismatch');
  const identities = new Map<string, string>(), events: ArchiveEvent[] = [];
  let duplicates = 0;
  for (const raw of input) {
    const event = parseEvent(raw, manifest), identity = `${event.partitionHour}:${event.sequence}`, digest = fingerprint(raw);
    const previous = identities.get(identity);
    if (previous !== undefined) {
      if (previous !== digest) throw new Error('conflicting native event identity');
      duplicates++; continue;
    }
    identities.set(identity, digest); events.push(event);
  }
  events.sort(before);
  return { markets: manifest.markets, events, quality: quality(manifest, events, input.length, duplicates, gap) };
}

/** File hash covers the exported selection only. It does not upgrade partial Parquet ranges to whole-file verification. */
export async function readPendulumflow(directory: string, options: { maxEvents?: number; maxBookGapMs?: number; maxBytes?: number } = {}): Promise<ArchiveDataset> {
  const [manifestError, manifestValue] = await catchError(readFile(join(directory, 'dataset-manifest.json'), 'utf8').then(text => JSON.parse(text) as unknown));
  if (manifestError) throw new Error('cannot read canonical dataset manifest', { cause: manifestError });
  const manifest = parseManifest(manifestValue), path = join(directory, manifest.eventFile), maxBytes = options.maxBytes ?? 256_000_000;
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0 || statSync(path).size > maxBytes || manifest.eventCount > (options.maxEvents ?? 1_000_000))
    throw new Error('canonical dataset exceeds inspection budget');
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(path)) digest.update(chunk);
  if (digest.digest('hex') !== manifest.eventsSha256) throw new Error('exported canonical event checksum mismatch');
  const input: unknown[] = [];
  for await (const raw of readLines([path])) {
    const [error, value] = catchError(() => JSON.parse(raw) as unknown);
    if (error) throw new Error('invalid canonical JSONL', { cause: error });
    input.push(value);
    if (input.length > (options.maxEvents ?? 1_000_000)) throw new Error('canonical event limit exceeded');
  }
  return inspectPendulumflow(manifestValue, input, options);
}
