import type { Command, CommandReply, Envelope, Log, Sample } from './types';

export class ApiError extends Error {
  constructor(public readonly status: number, public readonly kind: string, message: string) { super(message); }
}
async function request<T>(path: string, method = 'GET', payload?: unknown): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(path, { method, credentials: 'same-origin', cache: 'no-store', signal: controller.signal,
      headers: payload === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: payload === undefined ? undefined : JSON.stringify(payload) });
    const value = await response.json();
    if (!response.ok) throw new ApiError(response.status, String(value.error ?? 'request_failed'), String(value.message ?? value.error ?? 'Request failed'));
    return value as T;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError(0, method === 'GET' ? 'console_offline' : 'outcome_unknown',
      method === 'GET' ? 'Console unavailable. Displayed state may be stale.' : 'No response received. Command outcome is unknown; retry the same ID and payload.');
  } finally { window.clearTimeout(timer); }
}
export const api = {
  state: () => request<Envelope>('/api/state'),
  history: () => request<{ samples: Sample[] }>('/api/history?limit=120'),
  logs: () => request<{ logs: Log[] }>('/api/logs?limit=100'),
  localAuth: (token: string) => request('/api/auth/local', 'POST', { token }),
  telegramAuth: (init_data: string) => request('/api/auth/telegram', 'POST', { init_data }),
  logout: () => request('/api/logout', 'POST', {}),
  command: (payload: Command) => request<CommandReply>('limits' in payload ? '/api/limits' : '/api/control', 'limits' in payload ? 'PUT' : 'POST', payload),
};

declare global { interface Window { Telegram?: { WebApp?: { initData: string; ready?: () => void; expand?: () => void } } } }
export async function telegramInitData(): Promise<string | null> {
  if (!new URLSearchParams(location.hash.slice(1)).has('tgWebAppData')) return null;
  if (!window.Telegram?.WebApp) {
    await new Promise<void>((resolve, reject) => {
      const script = document.createElement('script');
      script.src = 'https://telegram.org/js/telegram-web-app.js?63';
      script.onload = () => resolve(); script.onerror = () => reject(new Error('Telegram integration unavailable'));
      document.head.append(script);
    });
  }
  const app = window.Telegram?.WebApp;
  app?.ready?.(); app?.expand?.();
  return app?.initData || null;
}
