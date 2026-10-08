import test from 'node:test';
import assert from 'node:assert/strict';
import { comparisonAmount, comparisonAge, comparisonDate, comparisonExport, comparisonReason, comparisonMode, comparisonKind, validComparison } from '../.test-build/comparisonModel.js';

test('comparison amounts preserve exact large decimals, tiny losses, and explicit zero', () => {
  assert.equal(comparisonAmount('12345678901234567890.123456789012'), '$12 345 678 901 234 567 890,123456789012');
  assert.equal(comparisonAmount('-0.000000000001'), '$−0,000000000001');
  assert.equal(comparisonAmount('0'), '$0');
  assert.equal(comparisonAmount('-0'), '$0');
  assert.equal(comparisonAmount('0.120000'), '$0,12');
  assert.equal(comparisonAmount('12.5', 'shares'), '12,5 долей');
});

test('missing evidence is unknown, never a zero or rounded count', () => {
  for (const value of [null, undefined, '', 0, 100.25, 'NaN', '1e3', '+2', '1'.repeat(41), '${VALUE}']) assert.equal(comparisonAmount(value), 'Неизвестно');
  for (const value of ['-1', '1.5', '1.0']) assert.equal(comparisonAmount(value, 'count'), 'Неизвестно');
  assert.equal(comparisonAmount('0', 'count'), '0');
  assert.equal(comparisonAge(null), 'Возраст неизвестен');
  assert.equal(comparisonAge(-1), 'Возраст неизвестен');
  assert.equal(comparisonDate(null), 'Время неизвестно');
});

test('dates explicitly use UTC and quality explains incomplete public evidence', () => {
  assert.equal(comparisonDate(Date.UTC(2026, 9, 8, 13, 14, 15)), '08.10.2026, 13:14:15 UTC');
  assert.match(comparisonReason('PUBLIC_WALLET_NOT_BOT_ATTRIBUTED'), /ручные операции/);
  assert.match(comparisonReason('REFERENCE_OUTSIDE_REQUESTED_PERIOD'), /не перекрывает/);
  assert.equal(comparisonReason('FUTURE_NEW_REASON'), 'FUTURE_NEW_REASON');
});

test('active simulation shadow flag and cumulative fills have clear meanings', () => {
  assert.equal(comparisonMode('simulation/shadow'), 'Симуляция · Работает');
  assert.equal(comparisonMode('shadow/shadow'), 'Наблюдение · Наблюдение');
  assert.equal(comparisonMode('simulation/paused'), 'Симуляция · Пауза');
  assert.equal(comparisonKind('observation'), 'Обновление накопленного исполнения');
  assert.equal(comparisonKind('venue_fill'), 'Условное исполнение');
});

function snapshot() {
  const metric = { value: null, unit: 'USD', scope: 'unknown', reason: 'INCOMPLETE_ACCOUNTING' };
  const lane = { status: 'UNAVAILABLE', metrics: Object.fromEntries(['cash_usd', 'nav_usd', 'net_pnl_usd', 'equity_change_usd', 'turnover_usd', 'observed_events', 'cost_bound_usd'].map(key => [key, { ...metric }])), freshness: {}, coverage: {}, period: {}, actions: [], positions: [], equity: [], source_refs: [], warnings: [] };
  return { schema_version: 1, generated_at_ms: 100, requested_period: '1h', period: {}, own: { ...lane, lane: 'simulation' }, reference: { ...lane, lane: 'public-benchmark' }, comparison: { status: 'INCONCLUSIVE', reasons: [] } };
}

test('period switches cannot adopt a previous-period result; missing or mixed lanes fail closed', () => {
  const value = snapshot();
  assert.equal(validComparison(value, '1h'), true);
  assert.equal(validComparison(value, '24h'), false);
  const invalid = snapshot(); delete invalid.reference.metrics.cost_bound_usd;
  assert.equal(validComparison(invalid, '1h'), false);
  const mixed = snapshot(); mixed.reference.lane = 'simulation';
  assert.equal(validComparison(mixed, '1h'), false);
  assert.equal(validComparison(null, '1h'), false);
});

test('export preserves full evidence, exact decimals, nulls, selected period, and future source fields', () => {
  const value = snapshot();
  value.reference.metrics.turnover_usd.value = '12345678901234567890.123456789012';
  value.reference.source_refs = [{kind:'verified-public-observer',id:'source-id',sha256:'a'.repeat(64),at_ms:99}];
  value.reference.coverage = {actions_complete:false,financial_complete:false,omitted_actions:300};
  value.future_evidence = {unknown:null,attribution:'public-wallet'};
  const exported = comparisonExport(value);
  assert.equal(exported.filename, 'mm2-comparison-1h-100.json');
  assert.deepEqual(JSON.parse(exported.text),value);
  assert.equal(JSON.parse(exported.text).reference.metrics.net_pnl_usd.value,null);
});
