import { useEffect, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { micro, modeLabel, money, picoMoney, signedPicoMoney, simulated, timeLabel } from './model';
import type { Command, CommandReply, Control, EngineState, Envelope, Limits as LimitsValue, Log, Sample } from './types';

export function Login({ busy, error, login }: { busy: boolean; error: string | null; login: (token: string) => Promise<void> }) {
  const [token, setToken] = useState('');
  const submit = async (event: FormEvent) => { event.preventDefault(); const input = token; setToken(''); await login(input); };
  return <main className="login"><p className="wordmark">mm2</p><h1>Owner access</h1><p>Operator console</p>
    <form onSubmit={submit}><label htmlFor="owner-token">Console owner token</label>
      <input id="owner-token" type="password" autoComplete="off" value={token} maxLength={256} onChange={event => setToken(event.target.value)} required autoFocus />
      <button className="primary" disabled={busy || !token}>{busy ? 'Signing in…' : 'Sign in'}</button></form>
    {error && <p role="alert" className="error">{error}</p>}<p className="muted">Use the separate console token from your private runtime configuration.</p>
    <footer>Engine decisions do not depend on this console.</footer></main>;
}

function Indicator({ good, children }: { good: boolean; children: ReactNode }) { return <span className={good ? 'indicator good' : 'indicator warning'}><i aria-hidden="true" />{children}</span>; }

export function Overview({ envelope, disabled, command }: { envelope: Envelope | null; disabled: boolean; command: (action: Control) => Promise<void> }) {
  const state = envelope?.state ?? null;
  const [confirm, setConfirm] = useState<Control | null>(null);
  const act = (action: Control) => action === 'kill' || action === 'reset' || (action === 'resume' && !!state?.live) ? setConfirm(action) : void command(action);
  return <section className="overview">
    <div className="overview-heading"><div><h1>Engine overview</h1><p>{modeLabel(state)}</p><p className="mono muted">Single sampler · 1 Hz</p></div>
      <div className="operations"><div className="controls">{(['resume', 'pause', 'kill', 'reset'] as const).map(action =>
        <button key={action} className={action === 'resume' ? 'primary' : action === 'kill' ? 'danger' : ''} disabled={disabled || (action==='resume' && !!state?.live && !state.live_eligible)} onClick={() => act(action)}>{action[0].toUpperCase() + action.slice(1)}</button>)}</div>
      <div className="indicators"><Indicator good={!!envelope?.connected}>ENGINE {envelope?.connected ? 'CONNECTED' : 'DISCONNECTED'}</Indicator>
        <Indicator good={!!envelope?.connected && !!state?.feed_ready}>FEEDS {state?.feed_ready && envelope?.connected ? 'FRESH' : 'NOT READY'}</Indicator><Indicator good={!!state?.live_eligible && !!envelope?.connected}>{state?.live ? state.live_eligible && envelope?.connected ? state.live.phase === 'running' ? 'TRADING ACTIVE' : 'TRADING READY' : 'TRADING NOT READY' : 'SIMULATION / READ ONLY'}</Indicator></div></div></div>
    {envelope?.error && <div className="notice" role="status">{envelope.error}</div>}
    {state?.fault && <div className="notice error" role="alert">Engine fault: {state.fault}</div>}
    {state?.simulation_error && <div className="notice error" role="alert">Emulator stopped: {state.simulation_error}</div>}
    {state?.live?.fault && <div className="notice error" role="alert">Trading stopped: {state.live.fault}</div>}
    {state?.parallel_simulation && <p className="small muted">Parallel simulation: {state.parallel_simulation.mode}{state.parallel_simulation.fault ? ` · ${state.parallel_simulation.fault}` : ''}. Its account and journal are separate.</p>}
    {state?.journal && <p className="small mono muted">Journal {state.journal.total_bytes.toLocaleString()} / {state.journal.total_max_bytes.toLocaleString()} bytes · {state.journal.archive_segments} archived segments</p>}
    {state?.last_input_error && <div className="notice">Input unavailable: {state.last_input_error}</div>}
    <div className="market"><h2>{state?.market ? 'BTC / Up or Down · 5m' : 'Waiting for market'}</h2>
      <p>{state?.market ? state.execution_mode === 'emulator' ? 'Synthetic market · deterministic demo' : state.market.slug : 'No selected market has been sampled'}</p>
      {state?.books && <p className="small mono muted">Book age · Up receive {state.books.up.receive_age_ms ?? 'unknown'} ms / exchange {state.books.up.exchange_age_ms ?? 'unknown'} ms · Down receive {state.books.down.receive_age_ms ?? 'unknown'} ms / exchange {state.books.down.exchange_age_ms ?? 'unknown'} ms</p>}
      {state?.source_status?.message && <p className="small muted">{state.source_status.message}</p>}</div>
    {confirm && <div className="modal-backdrop"><section className="confirm" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
      <h2 id="confirm-title">Confirm {confirm}</h2><p>{confirm === 'resume' ? 'Resume trading with real funds under the configured account and risk limits. Simulation remains a separate account.' : confirm === 'kill' ? 'Latch Kill and cancel executable quotes. A durable acknowledgement is required.' : 'Reset the engine control state and require fresh source snapshots. A durable acknowledgement is required.'}</p>
      <div className="controls"><button onClick={() => setConfirm(null)} autoFocus>Cancel</button><button className={confirm === 'kill' ? 'danger' : 'primary'} onClick={() => { setConfirm(null); void command(confirm); }}>Confirm {confirm}</button></div>
    </section></div>}
  </section>;
}

export function AccountBand({ state }: { state: EngineState | null }) {
  const session = simulated(state), account = session?.account;
  const cash = account ? account.initial_cash_micros - account.spent_micros_ceil : null;
  const values = [
    ['SIMULATED CASH', state?.paper_portfolio ? picoMoney(state.paper_portfolio.cash_pico) : money(cash)], ['RESERVED', money(account?.reserved_micros_ceil)],
    ['AVAILABLE FOR THIS MARKET', money(account?.available_micros_floor)], ['UP / DOWN INVENTORY', account ? `${micro(account.up_inventory_micros, 2)} / ${micro(account.down_inventory_micros, 2)}` : '— / —'],
  ];
  const live=state?.live;
  return <>{live && <section className="account-band" aria-label="Real account">
    <div><dt>REAL COLLATERAL</dt><dd>{live.account_reconciled ? money(live.collateral_micros) : '—'}</dd></div>
    <div><dt>REAL RESERVED</dt><dd>{picoMoney(live.reserved_pico)}</dd></div>
    <div><dt>REAL AVAILABLE</dt><dd>{live.account_reconciled ? money(live.available_micros) : '—'}</dd></div>
    <div><dt>REAL POSITIONS</dt><dd>{live.positions.length}</dd></div>
    <p className="unknown-funds">Account {live.account_reconciled ? 'reconciled' : 'not reconciled'} · user stream {live.user_stream_connected ? 'connected' : 'disconnected'} · last reconciliation {live.last_reconciliation_ms ? timeLabel(live.last_reconciliation_ms) : 'unknown'}. {!state?.live_eligible && 'Displayed balances are the last sampled account state.'}</p>
    {live.risk_limits && <p className="unknown-funds small">Trading caps · capital {money(live.risk_limits.max_budget_micros)} · gross {money(live.risk_limits.max_gross_micros)} · daily loss bound {money(live.risk_limits.daily_loss_limit_micros)} · {live.risk_limits.max_orders} orders / {live.risk_limits.max_markets} markets.</p>}
    {live.returned_collateral_micros !== undefined && <p className="unknown-funds small">Confirmed owned collateral returned {money(live.returned_collateral_micros)} · conservative realized result {signedPicoMoney(live.conservative_realized_pnl_pico)}. POL gas is accounted separately; this is not final net PnL.</p>}
    {live.chain_pending_hash && <p className="unknown-funds small mono">Closeout {live.chain_pending_kind === 'batch' ? 'batch' : 'transaction'} pending · {live.chain_pending_hash.slice(0, 18)}… · inventory remains reserved.</p>}
    {live.last_chain_error && <p className="unknown-funds warning">Closeout: {live.last_chain_error}</p>}
  </section>}{session && <section className="account-band" aria-label="Simulated account">
    {values.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}
    {session?.fault && <p className="unknown-funds error">Emulator fault: {session.fault}</p>}
    {state?.paper_portfolio && <p className="unknown-funds">{state.paper_portfolio.position_count ?? state.paper_portfolio.positions.length} retained market ledgers{state.paper_portfolio.positions_truncated ? ` · latest ${state.paper_portfolio.positions.length} shown` : ''}. Simulated executions and settlement assumptions.</p>}
  </section>}{!session && !live && <p className="unknown-funds">Real account funds and inventory are unknown in public shadow.</p>}</>;
}

export function Orders({ state }: { state: EngineState | null }) {
  const orders = simulated(state)?.orders ?? [];
  const realOrders=state?.live?.orders.filter(order=>order.phase!=='closed') ?? [];
  return <>{state?.live && <section className="orders"><h2>Real orders</h2><div className="table-scroll"><table><thead><tr>{['Order', 'Side', 'Outcome', 'Price', 'Remaining', 'Status'].map(label => <th key={label}>{label}</th>)}</tr></thead>
    <tbody>{realOrders.length ? realOrders.map(order=><tr key={order.order_id}><td className="mono" title={order.order_id}>{order.order_id.slice(0,14)}…</td><td>BUY</td><td>{order.quote.outcome}</td><td className="mono">{micro(order.quote.price_micros)}</td><td className="mono">{micro(order.quote.quantity_micros-order.cumulative_filled_micros)}</td><td>{order.phase.replaceAll('_',' ')}</td></tr>) : <tr><td colSpan={6} className="empty">No tracked active orders · account {state.live.account_reconciled ? 'reconciled' : 'unavailable'}</td></tr>}</tbody></table></div></section>}
  {(simulated(state) || !state?.live) && <section className="orders"><h2>{state?.live ? 'Simulation orders' : 'Orders'}</h2><div className="table-scroll"><table><thead><tr>{['Order', 'Side', 'Outcome', 'Price', 'Remaining', 'Status'].map(label => <th key={label}>{label}</th>)}</tr></thead>
    <tbody>{orders.length ? orders.map(order => <tr key={order.id}><td className="mono">{order.id}</td><td>BUY</td><td>{order.quote.outcome}</td><td className="mono">{micro(order.quote.price_micros)}</td>
      <td className="mono">{micro(order.quote.quantity_micros - order.cumulative_filled_micros)}</td><td>{order.phase.replaceAll('_', ' ')}</td></tr>) : <tr><td colSpan={6} className="empty">{simulated(state) ? 'No simulated orders' : 'No actual orders submitted; public shadow proposals are unsubmitted'}</td></tr>}</tbody></table></div></section>}</>;
}

const fields = [
  ['initial_collateral_usd', 'Initial collateral USD'], ['max_imbalance_shares', 'Max imbalance shares'],
  ['lot_shares', 'Lot shares'], ['half_width', 'Half width'], ['max_feed_age_ms', 'Max feed age ms'],
] as const;
export function Limits({ state, disabled, apply }: { state: EngineState | null; disabled: boolean; apply: (limits: LimitsValue) => Promise<void> }) {
  const [values, setValues] = useState<Record<string, string>>({});
  const [dirty, setDirty] = useState(false);
  const [validation, setValidation] = useState<string | null>(null);
  useEffect(() => { if (!dirty && state?.limits) setValues(Object.fromEntries(fields.map(([key]) => [key, String(state.limits[key])]))); }, [state?.limits, dirty]);
  const submit = async (event: FormEvent) => {
    event.preventDefault(); setValidation(null);
    if (fields.some(([key]) => key !== 'max_feed_age_ms' && (!/^\d+(\.\d+)?$/.test(values[key] ?? '') || (values[key].split('.')[1] ?? '').slice(6).split('').some(char => char !== '0')))) {
      setValidation('Use exact fixed decimals with at most six nonzero fractional digits.'); return;
    }
    const age = Number(values.max_feed_age_ms);
    if (!Number.isSafeInteger(age) || age < 1 || age > 60000) { setValidation('Feed age must be an integer between 1 and 60000 ms.'); return; }
    await apply({ initial_collateral_usd: values.initial_collateral_usd, max_imbalance_shares: values.max_imbalance_shares,
      lot_shares: values.lot_shares, half_width: values.half_width, max_feed_age_ms: age });
  };
  return <section className="limits"><h2>Quote settings</h2><p className="small">Initial capital is fixed at startup. Trading capital, gross exposure, order count and loss caps come from the deployment profile.</p><form onSubmit={submit}>{fields.map(([key, label]) => <label key={key} htmlFor={key}><span>{label}</span>
    <input id={key} value={values[key] ?? ''} inputMode={key === 'max_feed_age_ms' ? 'numeric' : 'decimal'} maxLength={48} disabled={disabled || key === 'initial_collateral_usd'} required onChange={event => { setDirty(true); setValues(previous => ({ ...previous, [key]: event.target.value })); }} /></label>)}
    <button className="primary" disabled={disabled || !state}>Apply limits</button>{dirty && <button className="text-button" type="button" onClick={() => setDirty(false)}>Restore sampled limits</button>}
    {validation && <p className="error small" role="alert">{validation}</p>}</form></section>;
}

export function Receipt({ receipt, pending, busy, error, retry }: { receipt: CommandReply | null; pending: Command | null; busy: boolean; error: string | null; retry: () => Promise<void> }) {
  return <section className="receipt"><h3>Command receipt</h3><div className="receipt-content" role="status">
    {busy ? <p>Waiting for durable acknowledgement…</p> : pending ? <><p className="warning">Outcome unknown. Retry uses the same ID and payload.</p><p className="mono small">{pending.id}</p><p>Original {'action' in pending ? pending.action : 'limits'} · expected version {pending.expected_version}</p>{'limits' in pending && <p className="mono small">{JSON.stringify(pending.limits)}</p>}<button onClick={() => void retry()} className="primary">Retry command</button></>
      : receipt ? <><p>Durable acknowledgement{receipt.replayed ? ' · replayed' : ''}</p><p className="mono small">{receipt.receipt.command_id}</p><p>Version {receipt.receipt.applied_version} · {receipt.receipt.mode_at_ack}</p></> : <p>No command submitted</p>}
    {error && <p role="alert" className="error">{error}</p>}
  </div></section>;
}

export function Activity({ logs }: { logs: Log[] }) { return <section className="activity"><h2>Recent activity</h2><div className="table-scroll"><table><thead><tr><th>Time</th><th>Level</th><th>Message</th></tr></thead>
  <tbody>{logs.length ? logs.slice(0, 12).map(log => <tr key={log.id}><td className="mono">{timeLabel(log.observed_at_ms)}</td><td className={`mono ${log.level === 'ERROR' ? 'error' : log.level === 'WARN' ? 'warning' : ''}`}>{log.level}</td><td>{log.message}</td></tr>) : <tr><td colSpan={3} className="empty">No secondary activity sampled</td></tr>}</tbody></table></div></section>; }

export function History({ samples, envelope }: { samples: Sample[]; envelope: Envelope | null }) {
  const points = samples.map(sample => ({ sample, value: simulated(sample.state)?.account.available_micros_floor }))
    .filter((point): point is { sample: Sample; value: number } => typeof point.value === 'number' && Number.isSafeInteger(point.value));
  const min = Math.min(...points.map(point => point.value)), max = Math.max(...points.map(point => point.value));
  const span = max - min || 1;
  const plot = points.map((point, index) => `${20 + index * 960 / Math.max(1, points.length - 1)},${120 - (point.value - min) * 90 / span}`).join(' ');
  const telemetry = envelope?.telemetry;
  return <section className="history"><div className="section-heading"><h2>Sample history</h2><p className="small muted"><span className="legend" />Simulated available cash <span className="ledger-note">Secondary telemetry · not the trading ledger</span></p></div>
    <div className="history-plot">{points.length ? <><svg viewBox="0 0 1000 150" role="img" aria-label="Sampled simulated available cash"><polyline points={plot} />{points.length === 1 && <circle cx="20" cy="120" r="3" />}</svg>
      <div className="history-axis"><span>{timeLabel(points[0].sample.observed_at_ms)}</span><span className="mono">Latest {money(points[points.length - 1].value)}</span><span>{timeLabel(points[points.length - 1].sample.observed_at_ms)}</span></div></>
      : <p className="empty">{samples.length ? 'No simulated account history in sampled state' : 'History appears after the first sampled state'}</p>}</div>
    {telemetry && <p className="telemetry small muted">Samples {telemetry.sample_count} · publication lag {telemetry.publication_lag_ms === null ? 'unknown' : `${telemetry.publication_lag_ms} ms`} · history dropped {telemetry.history_dropped} · logs dropped {telemetry.logs_dropped} · overruns {telemetry.overruns}</p>}
    {telemetry?.storage_error && <p className="warning small">{telemetry.storage_error}</p>}
    </section>;
}
