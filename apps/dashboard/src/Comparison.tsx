import { useEffect, useState } from 'react';
import { api } from './api';
import { comparisonAge, comparisonAmount, comparisonDate, comparisonExport, comparisonKind, comparisonMode, comparisonPeriods, comparisonPeriodText, comparisonReason, comparisonSourceFidelity, comparisonTrendPath, validComparison, validModelResult, validReportedPerformance } from './comparisonModel';
import type { ComparisonLane, ComparisonMetric, ComparisonPeriod, ComparisonSnapshot } from './comparisonModel';
import type { Envelope } from './types';

const statusLabels = { AVAILABLE: 'Данные доступны', STALE: 'Данные устарели', UNAVAILABLE: 'Нет данных', INVALID: 'Данные не прошли проверку' };
const reportedMetricGrid = { gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 240px), 1fr))' };

function downloadComparison(snapshot: ComparisonSnapshot) {
  const file = comparisonExport(snapshot);
  const url = URL.createObjectURL(new Blob([file.text], { type: 'application/json;charset=utf-8' }));
  const link = document.createElement('a');
  try {
    link.href = url; link.download = file.filename; document.body.append(link); link.click();
  } finally { link.remove(); URL.revokeObjectURL(url); }
}

function useComparison(period: ComparisonPeriod) {
  const [snapshot, setSnapshot] = useState<ComparisonSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let cancelled = false, timer = 0;
    setSnapshot(null); setError(null); setLoading(true);
    const poll = async () => {
      try {
        const value = await api.comparison(period);
        if (!validComparison(value, period)) throw new Error('invalid_snapshot');
        if (!cancelled) { setSnapshot(value); setError(null); }
      } catch {
        if (!cancelled) setError('Сравнение сейчас недоступно. Последние показанные данные могут быть устаревшими. Состояние движка проверяется отдельно.');
      } finally {
        if (!cancelled) { setLoading(false); timer = window.setTimeout(poll, 5000); }
      }
    };
    void poll();
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [period]);
  return { snapshot: snapshot?.requested_period === period ? snapshot : null, error, loading };
}

function Metric({ label, metric, note }: { label: string; metric: ComparisonMetric | undefined; note?: string }) {
  const amount = comparisonAmount(metric?.value, metric?.unit);
  const known = amount !== 'Неизвестно';
  return <div className={`comparison-metric${known ? '' : ' metric-unknown'}`}>
    <dt>{label}</dt><dd>{amount}</dd>
    <p>{known ? note ?? 'Значение по источнику' : comparisonReason(metric?.reason ?? null)}</p>
  </div>;
}

function ReportedAmount({ label, value, note, reason }: { label: string; value: string | null | undefined; note: string; reason: string }) {
  const amount = comparisonAmount(value, 'USDC');
  const known = amount !== 'Неизвестно';
  return <div className={`comparison-metric${known ? '' : ' metric-unknown'}`}><dt>{label}</dt><dd>{amount}</dd><p>{known ? note : comparisonReason(reason)}</p></div>;
}

function ModelResult({ lane, transportStale }: { lane: ComparisonLane; transportStale: boolean }) {
  const result = validModelResult(lane.model_result) ? lane.model_result : null;
  const status = result?.status ?? (lane.model_result ? 'INVALID' : 'UNAVAILABLE');
  const known = status === 'AVAILABLE';
  const reason = known ? 'SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT' : status === 'INVALID' ? 'MODEL_LEDGER_INVALID' : 'MODEL_LEDGER_UNAVAILABLE';
  const metric = (value: string | null | undefined): ComparisonMetric => ({value:known ? value ?? null : null,unit:'USD',scope:'cumulative-synthetic-ledger',reason});
  return <section className="comparison-actions model-result" aria-label="Накопленные результаты модели">
    <div className="comparison-subheading"><h3>Накопленные результаты модели</h3><span>{transportStale ? 'Свежесть не подтверждена' : statusLabels[status]}</span></div>
    <p className="small">Итоги журнала модели на дату снимка, за все накопленные закрытые позиции. Это условные деньги модели, а не заработанные средства и не результат выбранного периода.</p>
    <dl className="comparison-metrics">
      <Metric label="Накопленный результат закрытых позиций · модель" metric={metric(result?.realized_pnl_usd)} note="Накопленный реализованный результат модели; текущие открытые позиции сюда не входят" />
      <Metric label="Начальный капитал модели" metric={metric(result?.initial_cash_usd)} note="Начальная сумма условных денег по журналу модели" />
    </dl>
    <p className="small">Дата снимка модели: {comparisonDate(known ? result?.at_ms ?? null : null)}.</p>
    <details className="comparison-quality"><summary>Расходы и возвраты модели</summary>
      <dl className="comparison-metrics">
        <Metric label="Накопленные расходы модели" metric={metric(result?.spent_usd)} note="Включают комиссии модели; это расходы, не убыток" />
        <Metric label="Комиссии внутри расходов" metric={metric(result?.fees_usd)} note="Уже входят в расходы; повторно не вычитаются" />
        <Metric label="Накопленные возвраты модели" metric={metric(result?.returned_usd)} note="Возвраты из закрытых позиций по журналу; не весь остаток счета" />
      </dl>
      <p className="small">{comparisonReason(reason)}</p>
    </details>
  </section>;
}

function ReportedPerformance({ lane, transportStale }: { lane: ComparisonLane; transportStale: boolean }) {
  const reported = validReportedPerformance(lane.reported_performance) ? lane.reported_performance : null;
  const status = reported?.status ?? (lane.reported_performance ? 'INVALID' : 'UNAVAILABLE');
  const known = status === 'AVAILABLE' || status === 'STALE';
  const latest = known ? reported?.latest : null;
  const unavailableReason = status === 'INVALID' ? 'PNL_SOURCE_INVALID' : 'PNL_SOURCE_UNAVAILABLE';
  return <section className="comparison-actions reported-performance" aria-label="Оценки кошелька по данным Polymarket">
    <div className="comparison-subheading"><h3>Оценки кошелька по данным Polymarket</h3><span>{transportStale ? 'Свежесть не подтверждена' : statusLabels[status]}</span></div>
    <p className="small">Оценка публичного кошелька. Не подтверждает прибыль отдельного бота. Фактическое окно и точность источника указаны; полного учета денег/комиссий нет. Суммы выражены в USDC; перевод в доллары не подтвержден.</p>
    <dl className="comparison-metrics" style={reportedMetricGrid}>
      <ReportedAmount label="Накопленная оценка API" value={latest?.economic_pnl_usd} note="Последняя точка провайдера; начало накопления неизвестно. Это не результат выбранного периода." reason={known ? 'INCOMPLETE_ACCOUNTING' : unavailableReason} />
      <ReportedAmount label="Изменение оценки API за доступный период" value={known ? reported?.change_economic_pnl_usd : null} note="Разница между первой и последней пригодной точкой API в фактическом окне; не подтвержденная чистая прибыль." reason={known ? 'PNL_PERIOD_BASELINE_INSUFFICIENT' : unavailableReason} />
    </dl>
    <div className="lane-time"><p><strong>Последняя точка провайдера</strong><br />{comparisonDate(known ? reported?.latest_at_ms ?? null : null)}</p>
      <p><strong>Фактическое окно изменения оценки</strong><br />{comparisonPeriodText(known ? reported?.period : null)}</p></div>
    <p className="small">Точность источника: {comparisonSourceFidelity(reported?.source_fidelity)} {reported?.interval === '1d' && reported?.fidelity === '1h' ? 'Запрошены окно API 1 день и сетка 1 час.' : ''}</p>
    <details className="comparison-quality"><summary>Состав оценки и источник</summary>
      <dl className="comparison-metrics" style={reportedMetricGrid}>
        <ReportedAmount label="Реализованная часть по API" value={latest?.realized_pnl_usd} note="Оценка провайдера для кошелька, без подтвержденного полного журнала бота" reason={known ? 'INCOMPLETE_ACCOUNTING' : unavailableReason} />
        <ReportedAmount label="Нереализованная часть по API" value={latest?.unrealized_pnl_usd} note="Оценка открытых позиций провайдером; цена выхода не гарантирована" reason={known ? 'INCOMPLETE_ACCOUNTING' : unavailableReason} />
        <ReportedAmount label="Доходы кошелька по API" value={latest?.wallet_income_usd} note="Отдельная составляющая оценки провайдера; принадлежность дохода боту неизвестна" reason={known ? 'INCOMPLETE_ACCOUNTING' : unavailableReason} />
      </dl>
      <p className="small">Источник получен: {comparisonDate(reported?.source_at_ms ?? null)}. Точек в доступном окне: {reported ? comparisonAmount(String(reported.series.length), 'count') : 'Неизвестно'}.</p>
      {reported?.warnings.length ? <ul>{reported.warnings.map((warning, index) => <li key={index}>{comparisonReason(warning)}</li>)}</ul> : <p className="small">{comparisonReason(known ? 'PNL_ESTIMATE_NOT_AUTHENTICATED_BOT_RESULT' : unavailableReason)}</p>}
      {reported?.endpoint && <p className="small mono">Источник: {reported.endpoint}</p>}
      {reported?.response_sha256 && <p className="small mono">SHA-256 ответа: {reported.response_sha256}</p>}
    </details>
  </section>;
}

function Trend({ lane }: { lane: ComparisonLane }) {
  const path = comparisonTrendPath(lane.equity);
  if (path === null) return <p className="trend-empty">История оценки капитала появится после нескольких пригодных снимков. Пропуски не заменяются нулем.</p>;
  const start = lane.equity[0].at_ms;
  return <div className="comparison-trend"><h3>Оценка капитала по снимкам</h3>
    <svg viewBox="0 0 700 108" role="img" aria-label="История оценочного капитала симуляции. Пробелы означают отсутствие пригодных данных."><path d={path} /></svg>
    <p className="small">{comparisonDate(start)} — {comparisonDate(lane.equity[lane.equity.length - 1].at_ms)}. Оценка по ценам источника; комиссии и продажа по этой цене не гарантированы.</p>
  </div>;
}

function ActionList({ lane }: { lane: ComparisonLane }) {
  const shown = lane.actions.slice(0, 20);
  const summary = lane.activity_summary && typeof lane.activity_summary.buy_count === 'string' ? lane.activity_summary : null;
  return <div className="comparison-actions"><div className="comparison-subheading"><h3>{lane.lane === 'simulation' ? 'Наши действия' : 'Наблюдаемые действия mo-money'}</h3><span>{shown.length} показано</span></div>
    {summary && <div className="activity-counts"><p>За фактический период источника</p><dl>{[
      ['Покупки', summary.buy_count], ['Продажи', summary.sell_count], ['Сделки', summary.trade_count], ['Объединения', summary.merge_count], ['Погашения', summary.redeem_count],
    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{comparisonAmount(value, 'count')}</dd></div>)}</dl></div>}
    {shown.length ? <ol>{shown.map((action, index) => <li key={`${action.id}-${index}`}>
      <div className="action-heading"><strong>{comparisonKind(action.kind)}{action.side ? ` · ${comparisonKind(action.side)}` : ''}</strong><time>{comparisonDate(action.at_ms)}</time></div>
      <p className="action-market">{action.title ?? action.market ?? 'Рынок не указан'}{action.outcome ? ` · ${action.outcome}` : ''}</p>
      <div className="action-numbers"><span>{action.kind === 'observation' ? 'Накопленное исполнение' : 'Объем'}: {comparisonAmount(action.quantity_shares, 'shares')}</span><span>Сумма: {comparisonAmount(action.amount_usd)}</span>{action.price_usd && <span>Цена источника: {comparisonAmount(action.price_usd)}</span>}{action.order_id && <span className="action-order">Заявка: {action.order_id}</span>}</div>
    </li>)}</ol> : <p className="comparison-empty">Нет пригодных действий в выбранном периоде. Это не подтверждает отсутствие операций.</p>}
    <p className="small">{lane.lane === 'simulation' ? 'Условные исполнения симуляции.' : 'Сумма события — движение денег, не прибыль.'} {lane.actions.length > shown.length ? `Еще ${lane.actions.length - shown.length} событий в доступной выборке. ` : ''}
      {lane.coverage.omitted_actions !== null && lane.coverage.omitted_actions > 0 ? `Не показано источником: ${lane.coverage.omitted_actions}. ` : ''}
      {lane.coverage.actions_complete ? lane.lane === 'simulation'
        ? 'Источник подтверждает полноту действий для указанного периода.'
        : 'Запрошенная публичная лента прочитана полностью. Действия вне ленты и недоступные данные кошелька сюда не входят.'
        : lane.lane === 'simulation' ? 'Полнота действий не подтверждена.' : 'Полнота запрошенной публичной ленты не подтверждена.'}</p>
  </div>;
}

function Lane({ lane, transportStale }: { lane: ComparisonLane; transportStale: boolean }) {
  const own = lane.lane === 'simulation';
  return <article id={own ? 'comparison-mm2' : 'comparison-mo-money'} className={`comparison-lane ${own ? 'own-lane' : 'reference-lane'}`}>
    <div className="lane-heading"><div><p className="eyebrow">{own ? 'Наш бот · условные исполнения' : 'Публичный кошелек · наблюдения'}</p><h2>{own ? 'MM2 · симуляция' : 'mo-money'}</h2></div>
      <span className={`data-status ${!transportStale && lane.status === 'AVAILABLE' && lane.freshness.fresh ? 'data-fresh' : 'data-warning'}`}>{transportStale ? 'Свежесть не подтверждена' : statusLabels[lane.status]}</span></div>
    <p className="lane-mode">{own ? comparisonMode(lane.mode) : 'Наблюдения публичных операций'}</p>
    <div className="lane-time"><p><strong>Последний снимок</strong><br />{comparisonDate(lane.freshness.observed_at_ms)} · {comparisonAge(lane.freshness.age_ms)}</p>
      <p><strong>{own ? 'Период снимков для оценки' : 'Фактический период наблюдений'}</strong><br />{comparisonPeriodText(lane.period)}</p></div>
    <dl className="comparison-metrics">
      <Metric label="Деньги на счете" metric={lane.metrics.cash_usd} note="Остаток по последнему снимку" />
      <Metric label="Оценка капитала с позициями" metric={lane.metrics.nav_usd} note="Оценка источника, не подтвержденная продажа" />
      {!own && <Metric label="Стоимость позиций с известной оценкой" metric={lane.metrics.position_value_usd} note="Сумма только позиций с известной оценкой. Остальная стоимость и деньги кошелька неизвестны." />}
      {own && <><Metric label="Доступно в текущем рынке" metric={lane.metrics.available_usd} note="С учетом бюджета рынка, лимитов портфеля и резервов" /><Metric label="Зарезервировано" metric={lane.metrics.reserved_usd} note="Под активные заявки по снимку" /></>}
      <Metric label={own ? 'Чистый результат симуляции' : 'Чистый результат кошелька'} metric={lane.metrics.net_pnl_usd} />
      <Metric label="Изменение оценки между снимками" metric={lane.metrics.equity_change_usd} note="Первый и последний пригодный снимок; не чистая прибыль" />
      <Metric label="Оборот в наблюдениях" metric={lane.metrics.turnover_usd} note="Объем событий с известной суммой" />
      <Metric label="Наблюдаемые события" metric={lane.metrics.observed_events} note="Число событий доступной выборки" />
    </dl>
    {own ? <ModelResult lane={lane} transportStale={transportStale} /> : <ReportedPerformance lane={lane} transportStale={transportStale} />}
    {own && <Trend lane={lane} />}
    <details className="comparison-positions"><summary>Позиции по последнему снимку · {lane.positions.length} в выборке</summary>
      {lane.positions.length ? <ul>{lane.positions.slice(0, 10).map((position, index) => <li key={index}><span>{position.market ?? 'Рынок неизвестен'}{position.outcome ? ` · ${position.outcome}` : ''}</span><strong>{comparisonAmount(position.quantity_shares, 'shares')}</strong><span>Оценка: {comparisonAmount(position.value_usd)}</span></li>)}</ul>
        : <p className="small">Нет пригодных позиций в выборке. Общая экспозиция неизвестна, если полнота источника не подтверждена.</p>}
      {lane.positions.length > 10 && <p className="small">Показаны первые 10 из {lane.positions.length} доступных позиций.</p>}
    </details>
    <ActionList lane={lane} />
    <details className="comparison-quality"><summary>Качество и происхождение данных</summary>
      <p className="small">{comparisonReason(lane.coverage.reason)} Учет результата {lane.coverage.financial_complete ? 'полный в заявленном источником охвате' : 'неполный'}.</p>
      <p className="small">Подтвержденный предел затрат: {comparisonAmount(lane.metrics.cost_bound_usd.value)}.{lane.metrics.cost_bound_usd.value === null ? ` ${comparisonReason(lane.metrics.cost_bound_usd.reason)}` : ''}</p>
      {lane.warnings.length > 0 && <ul>{lane.warnings.map((warning, index) => <li key={index}>{comparisonReason(warning)}</li>)}</ul>}
      {lane.account_id && <p className="small mono">Кошелек: {lane.account_id}</p>}
      {lane.source_refs.length > 0 ? <ul className="source-list">{lane.source_refs.map((source, index) => <li key={index}><strong>{source.kind}</strong> · {comparisonDate(source.at_ms)}<br /><span className="mono">{source.id}</span>{source.sha256 && <><br /><span className="mono">SHA-256: {source.sha256}</span></>}</li>)}</ul> : <p className="small">Проверенный источник пока не указан.</p>}
    </details>
  </article>;
}

export function Comparison({ envelope }: { envelope: Envelope | null }) {
  const [period, setPeriod] = useState<ComparisonPeriod>('1h');
  const { snapshot, error, loading } = useComparison(period);
  const state = envelope?.state;
  return <section className="comparison" aria-labelledby="comparison-title">
    <div className="comparison-heading"><div><p className="eyebrow">Наблюдать · проверять · улучшать</p><h1 id="comparison-title">Симуляции и сравнение</h1><p>Наши действия и результаты рядом с публичными наблюдениями mo-money.</p></div>
      <div className="period-picker" role="group" aria-label="Период сравнения">{comparisonPeriods.map(option => <button key={option.value} aria-pressed={period === option.value} onClick={() => setPeriod(option.value)}>{option.label}</button>)}</div></div>
    <div className="comparison-engine"><span className={`data-status ${envelope?.connected ? 'data-fresh' : 'data-warning'}`}>Движок {envelope?.connected ? 'на связи' : 'недоступен'}</span>
      <span>Режим: {comparisonMode(state ? `${state.execution_mode}/${state.mode}` : null)}</span><span>Потоки данных: {envelope?.connected && state?.feed_ready ? 'готовы' : 'не готовы'}</span>
      <span>Торговля: {state?.live && envelope?.connected ? comparisonMode(state.live.phase) : 'нет подтвержденного активного счета'}</span></div>
    {state?.fault && <div className="notice error" role="alert">{envelope?.connected ? 'Ошибка движка' : 'Последняя известная ошибка движка'}: {state.fault}</div>}
    {state?.simulation_error && <div className="notice error" role="alert">{envelope?.connected ? 'Симуляция остановлена' : 'Последняя известная ошибка симуляции'}: {state.simulation_error}</div>}
    {state?.live?.fault && <div className="notice error" role="alert">{envelope?.connected ? 'Торговля остановлена' : 'Последняя известная ошибка торговли'}: {state.live.fault}</div>}
    {envelope?.connected && ['warning', 'critical', 'full'].includes(envelope.execution?.journal?.stage ?? '') && <div className="notice warning" role="alert">{envelope.execution?.journal?.stage === 'full' ? 'Журнал достиг лимита.' : 'Журнал приближается к лимиту.'} Откройте управление и диагностику: продолжение работы требует проверки сохраненного состояния.</div>}
    <p className="comparison-disclaimer">Симуляция использует условные исполнения. Публичные операции кошелька не раскрывают внутреннюю стратегию mo-money и не подтверждают прибыль его бота.</p>
    {snapshot && <nav className="comparison-jumps" aria-label="Перейти к данным"><button onClick={() => document.getElementById('comparison-mm2')?.scrollIntoView({ block: 'start' })}>MM2 · симуляция</button><button onClick={() => document.getElementById('comparison-mo-money')?.scrollIntoView({ block: 'start' })}>mo-money</button></nav>}
    {error && <div className="notice warning" role="status">{error}</div>}
    {snapshot ? <><div className="comparison-export"><p className="comparison-period">Выбранный интервал: {comparisonPeriodText(snapshot.period)}. Экран сформирован {comparisonDate(snapshot.generated_at_ms)}.</p><button onClick={() => downloadComparison(snapshot)}>Скачать полный срез JSON</button></div>
      <p className="comparison-export-note">Файл содержит весь ответ сравнения: суммы, действия, даты, источники, неизвестные значения и ограничения выборки.{error ? ' Это последний полученный срез; его свежесть не подтверждена.' : ''}</p>
      <div className="comparison-grid"><Lane lane={snapshot.own} transportStale={!!error} /><Lane lane={snapshot.reference} transportStale={!!error} /></div>
      <section className="comparison-conclusion"><h2>Сопоставимый итог пока не подтвержден</h2><p>Для оценки стратегии нужны одинаковые рынки и периоды, начальный капитал, полные исполнения и затраты. Денежный оборот и отдельные удачные сделки не заменяют эти данные.</p>
        {snapshot.comparison.reasons.length > 0 && <ul>{snapshot.comparison.reasons.map((reason, index) => <li key={index}>{comparisonReason(reason)}</li>)}</ul>}</section></>
      : <div className="comparison-no-data" role="status"><h2>{loading ? 'Собираем доступные наблюдения…' : 'Экран сравнения не получил данные'}</h2><p>{loading ? 'Снимки движка и проверенный пакет mo-money загружаются отдельно.' : 'Нулевой результат не подставляется. Ниже доступны управление и диагностика движка.'}</p></div>}
  </section>;
}
