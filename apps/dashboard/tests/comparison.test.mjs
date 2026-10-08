import test from 'node:test';
import assert from 'node:assert/strict';
import { comparisonAmount, comparisonAge, comparisonDate, comparisonExport, comparisonPeriodText, comparisonReason, comparisonMode, comparisonKind, comparisonSourceFidelity, comparisonTrendPath, validComparison, validModelResult, validReportedPerformance } from '../.test-build/comparisonModel.js';

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
  assert.match(comparisonReason('V2_DEFAULT_FEED_EXCLUDES_OPT_IN_TIPS'), /чаевые.*не учтены/);
  assert.match(comparisonReason('V2_OPEN_POSITIONS_NOT_COMPLETE_WALLET_LIFECYCLE'), /Закрытые позиции.*не входят/);
  assert.equal(comparisonReason('FUTURE_NEW_REASON'), 'FUTURE_NEW_REASON');
});

test('active simulation shadow flag and cumulative fills have clear meanings', () => {
  assert.equal(comparisonMode('simulation/shadow'), 'Симуляция · Работает');
  assert.equal(comparisonMode('shadow/shadow'), 'Наблюдение · Наблюдение');
  assert.equal(comparisonMode('simulation/paused'), 'Симуляция · Пауза');
  assert.equal(comparisonKind('observation'), 'Обновление накопленного исполнения');
  assert.equal(comparisonKind('venue_fill'), 'Условное исполнение');
});

test('persisted action history explains sampling, retention and source continuity without claiming a complete ledger', () => {
  const cases = [
    ['SAMPLED_ACTION_HISTORY_NOT_COMPLETE_LEDGER', /сохраненная выборка.*не полный журнал.*нельзя подтвердить/],
    ['BOOT_CONTINUITY_UNPROVEN', /разных запусков.*одного счета и журнала.*не подтверждена/],
    ['ACTION_HISTORY_LOCATOR_UNBOUND', /не подтверждена.*только.*текущего снимка/],
    ['ACTION_HISTORY_GENERATION_CHANGED', /отката или несовпадения.*отделены.*не подтверждена/],
    ['ACTION_HISTORY_SAMPLE_GAPS', /есть пропуски.*могла не попасть/],
    ['ACTION_HISTORY_RETENTION_LIMIT', /4 096 наблюдениями и 7 днями.*исключена из этой выборки/],
    ['SECONDARY_ACTION_HISTORY_UNAVAILABLE', /выборка действий недоступна.*снимок проверяется отдельно.*не заменяются нулями/],
    ['ACTION_HISTORY_INVALID_SOURCE', /конфликтующие записи.*не прошли проверку.*не подтверждена/],
  ];
  for (const [reason, description] of cases) assert.match(comparisonReason(reason), description, reason);
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

const valuationContext = JSON.stringify({profile:'simulation',reset_epoch:'0',boot_id:'01234567-89ab-cdef-0123-456789abcdef'});
const otherValuationContext = JSON.stringify({profile:'simulation',reset_epoch:'0',boot_id:'11234567-89ab-cdef-0123-456789abcdef'});

test('sparse equity samples do not draw a gain across a gap longer than 120 seconds', () => {
  const points = [
    {at_ms:0,value_usd:'100',context:valuationContext},
    {at_ms:60000,value_usd:'101',context:valuationContext},
    {at_ms:180001,value_usd:'110',context:valuationContext},
    {at_ms:240001,value_usd:'111',context:valuationContext},
  ];
  assert.deepEqual(comparisonTrendPath(points).match(/[ML]/g), ['M','L','M','L']);
  assert.deepEqual(comparisonTrendPath([
    {at_ms:0,value_usd:'100',context:valuationContext},
    {at_ms:120000,value_usd:'101',context:valuationContext},
  ]).match(/[ML]/g), ['M','L']);
});

test('equity nulls and context changes break lines without substituting a zero', () => {
  const points = [
    {at_ms:0,value_usd:'100',context:valuationContext},
    {at_ms:60000,value_usd:null,context:valuationContext},
    {at_ms:120000,value_usd:'105',context:valuationContext},
    {at_ms:180000,value_usd:'106',context:otherValuationContext},
    {at_ms:240000,value_usd:'107',context:otherValuationContext},
  ];
  assert.deepEqual(comparisonTrendPath(points).match(/[ML]/g), ['M','M','M','L']);
  assert.equal(comparisonTrendPath(points.slice(0,2)),null);
});

test('legacy, unknown, and malformed boot contexts keep observed points but never connect them', () => {
  const contexts = [
    JSON.stringify({profile:'simulation',reset_epoch:'0'}),
    JSON.stringify({profile:'simulation',reset_epoch:'0',boot_id:null}),
    JSON.stringify({profile:'simulation',reset_epoch:'0',boot_id:'unknown'}),
    JSON.stringify({boot_id:'01234567-89AB-CDEF-0123-456789ABCDEF'}),
    JSON.stringify({boot_id:'01234567-89ab-cdef-0123-456789abcdef-extra'}),
    JSON.stringify({boot_id:123}),
    'invalid context', 'null', '[]',
  ];
  for (const context of contexts) {
    const path = comparisonTrendPath([
      {at_ms:0,value_usd:'100',context},
      {at_ms:60000,value_usd:'110',context},
    ]);
    assert.deepEqual(path.match(/[ML]/g), ['M','M'], context);
    assert.equal(path.trim().split(' ').length, 2, 'both observed points remain');
  }
  assert.deepEqual(comparisonTrendPath([
    {at_ms:0,value_usd:'100',context:valuationContext},
    {at_ms:60000,value_usd:'110',context:valuationContext},
  ]).match(/[ML]/g), ['M','L']);
  assert.deepEqual(comparisonTrendPath([
    {at_ms:0,value_usd:'100',context:valuationContext},
    {at_ms:60000,value_usd:'110',context:otherValuationContext},
  ]).match(/[ML]/g), ['M','M']);
});

function reportedPerformance() {
  const latest = {economic_pnl_usd:'488700.192192',realized_pnl_usd:'444039.806094',unrealized_pnl_usd:'-509.673902',wallet_income_usd:'45170.06'};
  const first = Date.UTC(2026,9,7,18), last = Date.UTC(2026,9,8,17);
  return {status:'AVAILABLE',evidence_class:'venue-reported-wallet-estimate',wallet:null,endpoint:'https://data-api.polymarket.com/v2/pnl',interval:'1d',fidelity:'1h',source_fidelity:'1d',source_at_ms:last+60000,response_sha256:'a'.repeat(64),latest_at_ms:last,latest,
    period:{start_ms:first,end_ms:last},change_economic_pnl_usd:'-3740.914710',series:[{at_ms:first,...latest,economic_pnl_usd:'492441.106902'},{at_ms:last,...latest}],warnings:['PNL_DAILY_SOURCE_ON_HOURLY_GRID']};
}

test('wallet estimates preserve exact signed USDC amounts without implying dollar conversion', () => {
  assert.equal(comparisonAmount('488700.192192','USDC'), '488 700,192192 USDC');
  assert.equal(comparisonAmount('-3740.914710','USDC'), '−3 740,91471 USDC');
  assert.equal(comparisonAmount('-509.673902','USDC'), '−509,673902 USDC');
  assert.equal(comparisonAmount('0','USDC'), '0 USDC');
  assert.equal(comparisonAmount(null,'USDC'), 'Неизвестно');
  assert.equal(comparisonAmount(488700.192192,'USDC'), 'Неизвестно');
});

test('reported estimate period shows actual 23-hour UTC endpoints and daily source fidelity', () => {
  const reported = reportedPerformance();
  assert.equal(validReportedPerformance(reported),true);
  assert.equal(reported.period.end_ms-reported.period.start_ms,23*3600000);
  assert.equal(comparisonPeriodText(reported.period),'07.10.2026, 18:00:00 UTC — 08.10.2026, 17:00:00 UTC');
  assert.equal(comparisonPeriodText(null),'Период наблюдений неизвестен');
  assert.match(comparisonSourceFidelity('1d'),/Дневные.*не повышает.*точность/);
  assert.match(comparisonSourceFidelity(null),/неизвестна/);
  assert.match(comparisonReason('PNL_PERIOD_BASELINE_INSUFFICIENT'),/Накопленная оценка не является результатом этого периода/);
});

test('missing or invalid optional wallet estimates do not invalidate the underlying comparison lane', () => {
  assert.equal(validReportedPerformance(null),false);
  const invalid = reportedPerformance(); invalid.latest.economic_pnl_usd = 488700.192192;
  assert.equal(validReportedPerformance(invalid),false);
  const unsupported = reportedPerformance(); unsupported.source_fidelity = '1h';
  assert.equal(validReportedPerformance(unsupported),false);
  const missingField = reportedPerformance(); delete missingField.latest.wallet_income_usd;
  assert.equal(validReportedPerformance(missingField),false);
  const malformedSource = reportedPerformance(); malformedSource.endpoint = {url:'unsafe-shape'};
  assert.equal(validReportedPerformance(malformedSource),false);
  const value = snapshot(); value.reference.reported_performance = invalid;
  assert.equal(validComparison(value,'1h'),true);
  const noBaseline = reportedPerformance(); noBaseline.change_economic_pnl_usd = null; noBaseline.series = [noBaseline.series[1]]; noBaseline.period.start_ms = noBaseline.period.end_ms;
  assert.equal(validReportedPerformance(noBaseline),true);
});

test('cumulative model results retain exact loss and reject missing or malformed ledger evidence independently', () => {
  const result={status:'AVAILABLE',kind:'cumulative-synthetic-ledger',at_ms:Date.UTC(2026,9,8,17),initial_cash_usd:'100',cash_usd:'95.12',spent_usd:'10',fees_usd:'0.01',returned_usd:'5.12',realized_pnl_usd:'-1.125000000001',reason:'SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT'};
  assert.equal(validModelResult(result),true);
  assert.equal(comparisonAmount(result.realized_pnl_usd),'$−1,125000000001');
  assert.match(comparisonReason(result.reason),/Накопленный итог.*Условные деньги.*выбранного периода не подтвержден/);
  assert.equal(validModelResult(null),false);
  const invalid={...result,realized_pnl_usd:-1.125};
  assert.equal(validModelResult(invalid),false);
  const missing={...result}; delete missing.returned_usd;
  assert.equal(validModelResult(missing),false);
  const value=snapshot();value.own.model_result=invalid;
  assert.equal(validComparison(value,'1h'),true);
});
