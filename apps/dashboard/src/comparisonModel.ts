export type ComparisonPeriod = '1h' | '6h' | '24h' | 'all';
export interface ComparisonMetric { value: string | null; unit: 'USD' | 'count'; scope: string; reason: string | null }
export interface ComparisonModelResult {
  status: 'AVAILABLE' | 'UNAVAILABLE' | 'INVALID'; kind: 'cumulative-synthetic-ledger'; at_ms: number | null;
  initial_cash_usd: string | null; cash_usd: string | null; spent_usd: string | null; fees_usd: string | null;
  returned_usd: string | null; realized_pnl_usd: string | null;
  reason: 'SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT' | 'MODEL_LEDGER_UNAVAILABLE' | 'MODEL_LEDGER_INVALID';
}
export interface ComparisonReportedPerformance {
  // Provider field names are retained; these reported amounts are USDC, with no verified USD conversion.
  status: 'AVAILABLE' | 'STALE' | 'UNAVAILABLE' | 'INVALID'; evidence_class: 'venue-reported-wallet-estimate';
  wallet: string | null; endpoint: string | null; interval: '1d' | null; fidelity: '1h' | null;
  source_fidelity: '1d' | null; source_at_ms: number | null; response_sha256: string | null; latest_at_ms: number | null;
  latest: { economic_pnl_usd: string | null; realized_pnl_usd: string | null; unrealized_pnl_usd: string | null; wallet_income_usd: string | null };
  period: { start_ms: number | null; end_ms: number | null }; change_economic_pnl_usd: string | null;
  series: { at_ms: number; economic_pnl_usd: string | null; realized_pnl_usd: string | null; unrealized_pnl_usd: string | null; wallet_income_usd: string | null }[];
  warnings: string[];
}
export interface ComparisonAction {
  id: string; at_ms: number | null; kind: string; market: string | null; side: string | null;
  outcome: string | null; quantity_shares: string | null; amount_usd: string | null; source: string;
  title?: string | null; price_usd?: string | null; order_id?: string | null;
}
export interface ComparisonLane {
  name: string; lane: 'simulation' | 'public-benchmark'; status: 'AVAILABLE' | 'STALE' | 'UNAVAILABLE' | 'INVALID';
  mode: string | null; account_id: string | null; period: { start_ms: number | null; end_ms: number | null };
  freshness: { observed_at_ms: number | null; age_ms: number | null; fresh: boolean };
  source_refs: { kind: string; id: string; sha256: string | null; at_ms: number | null }[];
  coverage: { actions_complete: boolean; financial_complete: boolean; reason: string; observed_actions: number; omitted_actions: number | null };
  metrics: {
    cash_usd: ComparisonMetric; nav_usd: ComparisonMetric; net_pnl_usd: ComparisonMetric;
    equity_change_usd: ComparisonMetric; turnover_usd: ComparisonMetric; observed_events: ComparisonMetric;
    cost_bound_usd: ComparisonMetric; available_usd?: ComparisonMetric; reserved_usd?: ComparisonMetric;
    position_value_usd?: ComparisonMetric;
  };
  activity_summary?: { buy_count: string; sell_count: string; trade_count: string; merge_count: string; redeem_count: string };
  reported_performance?: ComparisonReportedPerformance;
  model_result?: ComparisonModelResult;
  actions: ComparisonAction[];
  positions: { market: string | null; outcome: string | null; quantity_shares: string | null; value_usd: string | null }[];
  equity: { at_ms: number; value_usd: string | null; context: string }[];
  warnings: string[];
}
export interface ComparisonSnapshot {
  schema_version: 1; generated_at_ms: number; requested_period: ComparisonPeriod;
  period: { start_ms: number | null; end_ms: number }; own: ComparisonLane; reference: ComparisonLane;
  comparison: { status: 'INCONCLUSIVE'; reasons: string[] };
}

export const comparisonPeriods: { value: ComparisonPeriod; label: string }[] = [
  { value: '1h', label: '1 час' }, { value: '6h', label: '6 часов' }, { value: '24h', label: '24 часа' }, { value: 'all', label: 'Все доступные' },
];

// Display exact decimal text without a floating-point round trip or invented zeros.
export function comparisonAmount(value: unknown, unit: 'USD' | 'USDC' | 'count' | 'shares' = 'USD'): string {
  if (typeof value !== 'string' || !/^-?\d{1,40}(?:\.\d{1,12})?$/.test(value)) return 'Неизвестно';
  if (unit === 'count' && !/^\d{1,40}$/.test(value)) return 'Неизвестно';
  const negative = value.startsWith('-'), [whole, fraction = ''] = (negative ? value.slice(1) : value).split('.');
  const grouped = whole.replace(/^0+(?=\d)/, '').replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
  const decimals = fraction.replace(/0+$/, '');
  const nonzero = /[1-9]/.test(whole + fraction);
  const text = `${negative && nonzero ? '−' : ''}${grouped}${decimals ? `,${decimals}` : ''}`;
  return unit === 'USD' ? `$${text}` : unit === 'USDC' ? `${text} USDC` : unit === 'shares' ? `${text} долей` : text;
}

export function comparisonDate(ms: number | null): string {
  return ms !== null && Number.isSafeInteger(ms) && ms >= 0
    ? new Date(ms).toLocaleString('ru-RU', { timeZone: 'UTC', day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }) + ' UTC'
    : 'Время неизвестно';
}

export function comparisonPeriodText(period: { start_ms: number | null; end_ms: number | null } | null | undefined): string {
  if (!period || (period.start_ms === null && period.end_ms === null)) return 'Период наблюдений неизвестен';
  return `${period.start_ms === null ? 'Начало неизвестно' : comparisonDate(period.start_ms)} — ${period.end_ms === null ? 'Конец неизвестен' : comparisonDate(period.end_ms)}`;
}

export function comparisonSourceFidelity(value: string | null | undefined): string {
  return value === '1d' ? 'Дневные исходные наблюдения; отображение на часовой сетке API не повышает их точность.' : 'Точность исходных наблюдений неизвестна.';
}

export function validReportedPerformance(value: unknown): value is ComparisonReportedPerformance {
  if (!value || typeof value !== 'object') return false;
  const reported = value as ComparisonReportedPerformance;
  const amount = (value: unknown) => value === null || (typeof value === 'string' && comparisonAmount(value, 'USDC') !== 'Неизвестно');
  const timestamp = (value: unknown) => value === null || (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && Number.isFinite(new Date(value).getTime()));
  const text = (value: unknown) => value === null || (typeof value === 'string' && value.length <= 256);
  const fields = ['economic_pnl_usd', 'realized_pnl_usd', 'unrealized_pnl_usd', 'wallet_income_usd'] as const;
  return ['AVAILABLE', 'STALE', 'UNAVAILABLE', 'INVALID'].includes(reported.status)
    && reported.evidence_class === 'venue-reported-wallet-estimate'
    && text(reported.wallet) && text(reported.endpoint)
    && (reported.response_sha256 === null || (typeof reported.response_sha256 === 'string' && /^[0-9a-f]{64}$/.test(reported.response_sha256)))
    && [null, '1d'].includes(reported.interval) && [null, '1h'].includes(reported.fidelity) && [null, '1d'].includes(reported.source_fidelity)
    && timestamp(reported.source_at_ms) && timestamp(reported.latest_at_ms)
    && !!reported.latest && fields.every(field => amount(reported.latest[field]))
    && !!reported.period && timestamp(reported.period.start_ms) && timestamp(reported.period.end_ms)
    && amount(reported.change_economic_pnl_usd)
    && Array.isArray(reported.series) && reported.series.every(point => point && point.at_ms !== null && timestamp(point.at_ms) && fields.every(field => amount(point[field])))
    && Array.isArray(reported.warnings) && reported.warnings.every(warning => typeof warning === 'string');
}

export function validModelResult(value: unknown): value is ComparisonModelResult {
  if (!value || typeof value !== 'object') return false;
  const result = value as ComparisonModelResult;
  const fields = ['initial_cash_usd', 'cash_usd', 'spent_usd', 'fees_usd', 'returned_usd', 'realized_pnl_usd'] as const;
  return ['AVAILABLE', 'UNAVAILABLE', 'INVALID'].includes(result.status) && result.kind === 'cumulative-synthetic-ledger'
    && (result.at_ms === null || (Number.isSafeInteger(result.at_ms) && result.at_ms >= 0 && Number.isFinite(new Date(result.at_ms).getTime())))
    && fields.every(field => result[field] === null || (typeof result[field] === 'string' && comparisonAmount(result[field]) !== 'Неизвестно'))
    && ['SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT', 'MODEL_LEDGER_UNAVAILABLE', 'MODEL_LEDGER_INVALID'].includes(result.reason);
}

export function comparisonAge(ms: number | null): string {
  if (ms === null || !Number.isSafeInteger(ms) || ms < 0) return 'Возраст неизвестен';
  if (ms < 60_000) return `${Math.floor(ms / 1000)} сек. назад`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)} мин. назад`;
  if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)} ч. назад`;
  return `${Math.floor(ms / 86_400_000)} дн. назад`;
}

export function comparisonMode(mode: string | null): string {
  const labels: Record<string, string> = { emulator: 'Демо', simulation: 'Симуляция', shadow: 'Наблюдение', trading: 'Торговля', both: 'Торговля и симуляция', paused: 'Пауза', running: 'Работает', killed: 'Остановлен', faulted: 'Ошибка' };
  if (!mode) return 'Режим неизвестен';
  const parts = mode.split(/[·/]/).map(part => part.trim());
  return parts.map((part, index) => part === 'shadow' && index > 0 && parts[0] !== 'shadow' ? 'Работает' : labels[part] ?? part).join(' · ');
}

export function comparisonKind(kind: string): string {
  const labels: Record<string, string> = {
    TRADE: 'Сделка', trade: 'Сделка', BUY: 'Покупка', SELL: 'Продажа', buy: 'Покупка', sell: 'Продажа',
    REDEEM: 'Погашение', redeem: 'Погашение', MERGE: 'Объединение', merge: 'Объединение', SPLIT: 'Разделение', split: 'Разделение',
    post: 'Создание заявки', posted: 'Заявка создана', quote_posted: 'Заявка создана', fill: 'Исполнение', filled: 'Исполнение',
    cancel: 'Отмена', cancelled: 'Заявка отменена', cancel_requested: 'Запрос отмены', order_posted: 'Заявка создана', order_filled: 'Исполнение',
    book: 'Снимок стакана', venue_fill: 'Условное исполнение', post_requested: 'Запрос заявки', post_accepted: 'Заявка принята',
    post_transport_unknown: 'Результат отправки неизвестен', post_only_rejected: 'Заявка отклонена: пересечение стакана',
    cancel_transport_unknown: 'Результат отмены неизвестен', cancel_arrived: 'Отмена дошла до симулятора',
    query_requested: 'Запрос состояния заявки', query_unavailable: 'Состояние заявки недоступно', query_reconciled: 'Состояние заявки сверено',
    observation: 'Обновление накопленного исполнения', rate_limited: 'Ограничение частоты запросов', fault_injected: 'Тестовая ошибка',
  };
  return labels[kind] ?? kind.replaceAll('_', ' ');
}

export function validComparison(value: unknown, period: ComparisonPeriod): value is ComparisonSnapshot {
  if (!value || typeof value !== 'object') return false;
  const snapshot = value as ComparisonSnapshot;
  const metricKeys = ['cash_usd', 'nav_usd', 'net_pnl_usd', 'equity_change_usd', 'turnover_usd', 'observed_events', 'cost_bound_usd'] as const;
  return snapshot.schema_version === 1 && snapshot.requested_period === period && Number.isSafeInteger(snapshot.generated_at_ms)
    && !!snapshot.period && !!snapshot.comparison && Array.isArray(snapshot.comparison.reasons)
    && snapshot.own?.lane === 'simulation' && snapshot.reference?.lane === 'public-benchmark'
    && [snapshot.own, snapshot.reference].every(lane => lane && lane.metrics && lane.freshness && lane.coverage && lane.period
      && ['AVAILABLE', 'STALE', 'UNAVAILABLE', 'INVALID'].includes(lane.status)
      && metricKeys.every(key => lane.metrics[key] && (lane.metrics[key].value === null || typeof lane.metrics[key].value === 'string'))
      && Array.isArray(lane.actions) && Array.isArray(lane.positions) && Array.isArray(lane.equity)
      && Array.isArray(lane.source_refs) && Array.isArray(lane.warnings));
}

export function comparisonExport(snapshot: ComparisonSnapshot): { filename: string; text: string } {
  // Keep the complete sanitized API payload, including future evidence fields and explicit nulls.
  return { filename: `mm2-comparison-${snapshot.requested_period}-${snapshot.generated_at_ms}.json`, text: JSON.stringify(snapshot, null, 2) + '\n' };
}

function hasKnownValuationBoot(context: string): boolean {
  try {
    const parsed: unknown = JSON.parse(context);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return false;
    const boot = (parsed as Record<string, unknown>).boot_id;
    return typeof boot === 'string' && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(boot);
  } catch {
    return false;
  }
}

export function comparisonTrendPath(points: ComparisonLane['equity']): string | null {
  // Floating-point coordinates draw the chart; financial amounts remain exact text.
  const values = points.map(point => point.value_usd !== null && /^\d{1,12}(?:\.\d{1,12})?$/.test(point.value_usd) ? Number(point.value_usd) : null);
  const known = values.filter((value): value is number => value !== null && Number.isFinite(value));
  if (known.length < 2) return null;
  const min = Math.min(...known), span = Math.max(...known) - min || 1;
  const start = points[0].at_ms, duration = points[points.length - 1].at_ms - start || 1;
  let path = '', previousContext: string | null = null, previousAt = 0, connected = false;
  points.forEach((point, index) => {
    const value = values[index];
    if (value === null) { connected = false; return; }
    const x = 12 + (point.at_ms - start) / duration * 676, y = 91 - (value - min) / span * 74;
    const elapsed = point.at_ms - previousAt;
    const continuous = connected && previousContext === point.context && hasKnownValuationBoot(point.context) && elapsed >= 0 && elapsed <= 120_000;
    path += `${continuous ? 'L' : 'M'}${x.toFixed(2)},${y.toFixed(2)} `;
    previousContext = point.context; previousAt = point.at_ms; connected = true;
  });
  return path;
}

const reasons: Record<string, string> = {
  SIMULATION_SYNTHETIC_EXECUTION: 'Заявки и исполнения моделируются. Очередь, комиссия и исполнение на бирже могут отличаться.',
  RECENT_AUDIT_NOT_COMPLETE_LEDGER: 'Показан ограниченный хвост журнала. По нему нельзя посчитать все действия за период.',
  SIMULATION_CLOCK_NOT_WALL_TIME: 'Часы демо условные; их нельзя сопоставлять с временем UTC сделок кошелька.',
  CURRENT_SNAPSHOT_NOT_PERIOD_RESULT: 'Баланс и позиции относятся к последнему снимку, а не к итогам выбранного периода.',
  SAMPLED_NAV_NOT_REALIZED_PROFIT: 'Оценка капитала по публичным ценам не подтверждает прибыль после выхода из позиций.',
  FULL_BOT_LEDGER_REQUIRED: 'Для чистого результата нужен полный журнал бота, начальный и конечный капитал, комиссии и подтвержденные возвраты.',
  NO_FRESH_ENGINE_SNAPSHOT: 'Свежего снимка движка нет. Текущее состояние и результаты неизвестны.',
  PUBLIC_ACTIVITY_NOT_AUTHENTICATED_FILLS: 'Публичная активность показывает наблюдаемые события, а не полный журнал подтвержденных исполнений.',
  PUBLIC_CASH_UNKNOWN: 'Остаток денег в публичном кошельке не подтвержден этими наблюдениями.',
  INCOMPLETE_ACCOUNTING: 'Учет капитала, позиций или затрат неполный. Чистый результат неизвестен.',
  PUBLIC_REFERENCE_VALUATION: 'Стоимость публичных позиций — оценка источника, без гарантии продажи по этой цене.',
  ACTIONS_TRUNCATED: 'Часть действий не помещается в ограниченную выборку.',
  POSITIONS_TRUNCATED: 'Часть позиций не помещается в ограниченную выборку.',
  REFERENCE_OUTSIDE_REQUESTED_PERIOD: 'Пакет mo-money не перекрывает выбранный период. Его старые события не являются результатами этого периода.',
  REFERENCE_SOURCE_STALE: 'Наблюдения mo-money устарели. Дата фактических данных указана отдельно.',
  REFERENCE_NOT_PUBLISHED: 'Проверенный пакет наблюдений mo-money еще не опубликован для этой консоли.',
  REFERENCE_ARTIFACT_INVALID: 'Пакет mo-money не прошел проверку целостности или формата.',
  SOURCE_CUTOFF_LAGS_REQUEST: 'Конец доступных наблюдений раньше выбранного интервала. Данные mo-money показаны за фактический период источника.',
  SOURCE_PERIOD_SHORTER_THAN_REQUESTED: 'Источник покрывает только часть выбранной длительности.',
  PROFILE_ATTRIBUTION_REVERIFY_REQUIRED: 'Связь имени mo-money с этим публичным кошельком нужно проверить повторно.',
  FULL_SIMULATION_PERIOD_LEDGER_REQUIRED: 'Ограниченного хвоста действий недостаточно для оборота и затрат за весь период симуляции.',
  COST_BOUND_COVERAGE_UNPROVEN: 'Предел затрат для всего фактического периода не подтвержден.',
  EQUITY_CONTINUITY_UNPROVEN: 'Нет непрерывной серии пригодных оценок капитала одного запуска. Изменение неизвестно.',
  SIMULATION_VS_PUBLIC_OBSERVATION: 'Условные исполнения MM2 и наблюдения публичного кошелька имеют разное качество доказательств.',
  MATCHED_COMPLETE_ACCOUNTING_REQUIRED: 'Для сопоставимого результата нужны одинаковый период и рынки, полный учет капитала, исполнений и затрат.',
  SIMULATION_NOT_SELECTED: 'В текущем режиме симуляция не выбрана.',
  SECONDARY_EQUITY_UNAVAILABLE: 'История вторичных оценок капитала недоступна. Текущие снимки проверяются отдельно.',
  V2_DEFAULT_FEED_EXCLUDES_OPT_IN_TIPS: 'Запрошенная публичная лента не включает TIP (чаевые). Возможные чаевые и их расходы здесь не учтены.',
  V2_OPEN_POSITIONS_NOT_COMPLETE_WALLET_LIFECYCLE: 'Снимок показывает открытые позиции. Закрытые позиции и завершенные расчеты не входят в этот снимок; полный учет кошелька не подтвержден.',
  PNL_ESTIMATE_NOT_AUTHENTICATED_BOT_RESULT: 'API оценивает результат публичного кошелька. Прибыль отдельного бота не подтверждена.',
  PNL_DAILY_SOURCE_ON_HOURLY_GRID: 'Дневные исходные наблюдения отображаются на часовой сетке. Это не доказывает точность оценки за каждый час.',
  PNL_WINDOW_DIFFERS_FROM_ACTIVITY: 'Фактическое окно оценок API может отличаться от периода публичных действий.',
  PNL_PERIOD_BASELINE_INSUFFICIENT: 'В выбранном окне недостаточно пригодных точек для изменения оценки. Накопленная оценка не является результатом этого периода.',
  PNL_CASHFLOWS_UNKNOWN: 'Полные движения денег, комиссии и затраты кошелька неизвестны.',
  PNL_SOURCE_UNAVAILABLE: 'Источник оценок результата кошелька недоступен. Остальные наблюдения проверяются отдельно.',
  PNL_SOURCE_INVALID: 'Данные оценок результата кошелька не прошли проверку.',
  PNL_SOURCE_STALE: 'Оценки результата кошелька устарели. Дата последней точки указана отдельно.',
  SYNTHETIC_CUMULATIVE_NOT_PERIOD_RESULT: 'Накопленный итог по журналу модели на дату снимка. Условные деньги; результат выбранного периода не подтвержден.',
  MODEL_LEDGER_UNAVAILABLE: 'Пригодные накопленные итоги журнала модели недоступны. Значения не заменяются нулем.',
  MODEL_LEDGER_INVALID: 'Итоги журнала модели не прошли проверку согласованности. Результат неизвестен.',
  PUBLIC_WALLET_NOT_BOT_ATTRIBUTED: 'Наблюдаемый кошелек может включать ручные операции и другие стратегии. Результат самого бота не подтвержден.',
  PUBLIC_WALLET_ATTRIBUTION_UNVERIFIED: 'Публичный кошелек не подтверждает, что все операции принадлежат одному боту.',
  AUTHENTICATED_BOT_LEDGER_UNAVAILABLE: 'Нет полного журнала подтвержденных операций нашего бота.',
  SYNTHETIC_EXECUTION_NOT_REAL_PROFIT: 'Исполнения симуляции условные. Это не заработанные деньги.',
  COSTS_INCOMPLETE: 'Не все комиссии и затраты подтверждены.',
  FINANCIAL_EVIDENCE_INCOMPLETE: 'Данных о капитале, позициях и затратах недостаточно для чистой прибыли.',
  NO_REFERENCE_ARTIFACT: 'Локальный пакет наблюдений mo-money еще не подключен.',
  SOURCE_UNAVAILABLE: 'Источник данных недоступен.',
  SOURCE_STALE: 'Наблюдения устарели; свежие результаты неизвестны.',
  PERIOD_MISMATCH: 'Фактические периоды наблюдений не совпадают.',
  INSUFFICIENT_DATA: 'Недостаточно данных для сопоставимого результата.',
};

export function comparisonReason(reason: string | null): string {
  if (!reason) return 'Значение не подтверждено источником.';
  return reasons[reason] ?? reason;
}
