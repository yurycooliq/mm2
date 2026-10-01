import { useCallback, useEffect, useRef, useState } from 'react';
import { api, ApiError, telegramInitData } from './api';
import { safeVersion } from './model';
import { clearPending, definitiveRejection, matchingReceipt, persistPending, restorePending } from './pending';
import type { Command, CommandReply, Control, Envelope, Limits, Log, Sample } from './types';

export function useConsole() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [envelope, setEnvelope] = useState<Envelope | null>(null);
  const [history, setHistory] = useState<Sample[]>([]);
  const [logs, setLogs] = useState<Log[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [receipt, setReceipt] = useState<CommandReply | null>(null);
  const [storageError, setStorageError] = useState<string | null>(null);
  const [pending, setPending] = useState<Command | null>(null);
  const pendingRef = useRef<Command | null>(null);
  const sending = useRef(false);
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    try {
      let saved = restorePending(localStorage);
      if (!saved) { saved = restorePending(sessionStorage); if (saved) { persistPending(localStorage, saved); clearPending(sessionStorage); } }
      pendingRef.current = saved; setPending(saved);
    }
    catch (failure) { setStorageError(failure instanceof Error ? failure.message : 'Command storage unavailable; controls are blocked.'); }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try { await api.state(); if (!cancelled) setAuthenticated(true); }
      catch (first) {
        try {
          const init = await telegramInitData();
          if (init) { await api.telegramAuth(init); if (!cancelled) { setAuthenticated(true); return; } }
        } catch { if (!cancelled) setError('Telegram owner authentication failed. Reopen the Mini App or use the owner token.'); }
        if (!cancelled) { setAuthenticated(false); if (!(first instanceof ApiError && first.status === 401)) setError('Console unavailable.'); }
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!authenticated) return;
    let cancelled = false, timer = 0, tick = 0;
    const poll = async () => {
      try {
        const value = await api.state();
        if (cancelled) return;
        setEnvelope(value);
        if (tick % 5 === 0) {
          const [samples, activity] = await Promise.all([api.history(), api.logs()]);
          if (!cancelled) { setHistory(samples.samples); setLogs(activity.logs); }
        }
      } catch (failure) {
        if (cancelled) return;
        if (failure instanceof ApiError && failure.status === 401) { setAuthenticated(false); setEnvelope(null); }
        else setEnvelope(previous => previous ? { ...previous, connected: false, error: 'Console unavailable. Displayed state is stale.' } : null);
      } finally { tick += 1; if (!cancelled) timer = window.setTimeout(poll, 1000); }
    };
    void poll();
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [authenticated]);

  const login = async (token: string) => {
    setBusy(true); setError(null);
    try { await api.localAuth(token); setAuthenticated(true); }
    catch (failure) { setError(failure instanceof Error ? failure.message : 'Owner authentication failed'); }
    finally { if (mounted.current) setBusy(false); }
  };
  const logout = async () => {
    try { await api.logout(); setAuthenticated(false); setEnvelope(null); setHistory([]); setLogs([]); }
    catch { setError('Sign out was not acknowledged. The existing session may remain valid until expiry.'); }
  };
  const send = useCallback(async (payload: Command) => {
    if (sending.current) return;
    sending.current = true;
    setBusy(true); setError(null);
    try {
      // This must succeed before a command leaves the browser. Reload retries the same identity.
      persistPending(localStorage, payload); pendingRef.current = payload; setPending(payload);
      const reply = await api.command(payload);
      if (!matchingReceipt(reply, payload)) throw new ApiError(0, 'outcome_unknown', 'A matching durable receipt was not received.');
      clearPending(localStorage); pendingRef.current = null; setReceipt(reply); setPending(null);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : 'Command failed';
      setError(message);
      if (failure instanceof ApiError && definitiveRejection(failure.status, failure.kind)) {
        try { clearPending(localStorage); pendingRef.current = null; setPending(null); }
        catch { setStorageError('Command storage unavailable; controls are blocked.'); }
      } else if (!(failure instanceof ApiError)) setStorageError('Command storage unavailable; controls are blocked.');
      if (failure instanceof ApiError && failure.status === 401) setAuthenticated(false);
    } finally { sending.current = false; if (mounted.current) setBusy(false); }
  }, []);
  const command = async (action: Control) => {
    if (storageError || pendingRef.current || sending.current || !safeVersion(envelope?.state ?? null) || !envelope?.connected) return;
    await send({ id: crypto.randomUUID(), expected_version: envelope!.state!.command_version, action });
  };
  const limits = async (values: Limits) => {
    if (storageError || pendingRef.current || sending.current || !safeVersion(envelope?.state ?? null) || !envelope?.connected) return;
    await send({ id: crypto.randomUUID(), expected_version: envelope!.state!.command_version, limits: values });
  };
  return { authenticated, envelope, history, logs, error: storageError ?? error, busy, receipt, pending, storageError, login, logout, command, limits,
    retry: () => authenticated && envelope?.connected && !storageError && pending ? send(pending) : Promise.resolve(), dismissError: () => setError(null) };
}
