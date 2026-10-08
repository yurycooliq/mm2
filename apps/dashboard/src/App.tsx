import { AccountBand, Activity, History, Limits, Login, Orders, Overview, Receipt } from './components';
import { safeVersion } from './model';
import { useConsole } from './useConsole';
import { Comparison } from './Comparison';

export default function App() {
  const console = useConsole();
  if (console.authenticated === null) return <main className="login"><p>Loading…</p></main>;
  if (!console.authenticated) {
    if (console.access === 'local') return <Login login={console.login} busy={console.busy} error={console.error} />;
    const title = console.access === 'session_expired' ? 'Session ended' : console.access === 'unavailable' ? 'Temporarily unavailable' : 'Access unavailable';
    return <main className="login"><h1>{title}</h1>
      <p>Close this Mini App and open it again from the bot to restore access.</p>
      {console.access === 'session_expired' && <p>The bot continues independently. Any unconfirmed command is preserved; reopening does not send it again.</p>}</main>;
  }
  const state = console.envelope?.state ?? null;
  const disabled = console.busy || !!console.storageError || !!console.pending || !console.envelope?.connected || !safeVersion(state) || !state?.api_ready;
  return <div className="app-shell"><header><div className="brand"><span className="wordmark">mm2</span><span>Operator console</span></div><div className="owner"><span>Owner session</span><button className="text-button" onClick={() => void console.logout()}>Sign out</button></div></header>
    <main><Comparison envelope={console.envelope} /><details className="operator-details"><summary>Управление и диагностика движка</summary><Overview envelope={console.envelope} disabled={disabled} command={console.command} /><AccountBand state={state} />
      <div className="workspace"><div className="left-column"><Orders state={state} /><Activity logs={console.logs} /></div><aside><Limits state={state} disabled={disabled} apply={console.limits} />
        <Receipt receipt={console.receipt} pending={console.pending} busy={console.busy} error={console.error} retry={console.retry} /></aside></div>
      <History samples={console.history} envelope={console.envelope} /></details></main><footer>Engine decisions do not depend on this console.</footer></div>;
}
