import { AccountBand, Activity, History, Limits, Login, Orders, Overview, Receipt } from './components';
import { safeVersion } from './model';
import { useConsole } from './useConsole';

export default function App() {
  const console = useConsole();
  if (console.authenticated === null) return <main className="login"><p className="wordmark">mm2</p><h1>Operator console</h1><p>Checking owner session…</p></main>;
  if (!console.authenticated) return <Login login={console.login} busy={console.busy} error={console.error} />;
  const state = console.envelope?.state ?? null;
  const disabled = console.busy || !!console.storageError || !!console.pending || !console.envelope?.connected || !safeVersion(state) || !state?.api_ready;
  return <div className="app-shell"><header><div className="brand"><span className="wordmark">mm2</span><span>Operator console</span></div><div className="owner"><span>Owner session</span><button className="text-button" onClick={() => void console.logout()}>Sign out</button></div></header>
    <main><Overview envelope={console.envelope} disabled={disabled} command={console.command} /><AccountBand state={state} />
      <div className="workspace"><div className="left-column"><Orders state={state} /><Activity logs={console.logs} /></div><aside><Limits state={state} disabled={disabled} apply={console.limits} />
        <Receipt receipt={console.receipt} pending={console.pending} busy={console.busy} error={console.error} retry={console.retry} /></aside></div>
      <History samples={console.history} envelope={console.envelope} /></main><footer>Engine decisions do not depend on this console.</footer></div>;
}
