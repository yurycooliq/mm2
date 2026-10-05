import { ApiError } from './api.js';

export type OwnerAccess = 'authenticated' | 'local' | 'not_found' | 'unavailable';
type AuthenticationApi = { state: () => Promise<unknown>; telegramAuth: (initData: string) => Promise<unknown> };

export async function ownerAccess(client: AuthenticationApi, readTelegram: () => Promise<string | null>): Promise<OwnerAccess> {
  let initData: string | null;
  try { initData = await readTelegram(); }
  catch { return 'unavailable'; }
  if (initData !== null) {
    // The server validates the signature, timestamp and exact owner. Client-side
    // Telegram user fields and any pre-existing cookie cannot bypass that check.
    try {
      const reply = await client.telegramAuth(initData);
      return reply && typeof reply === 'object' && 'authenticated' in reply && reply.authenticated === true ? 'authenticated' : 'unavailable';
    }
    catch (failure) { return failure instanceof ApiError && failure.status === 0 ? 'unavailable' : 'not_found'; }
  }
  try { await client.state(); return 'authenticated'; }
  catch (failure) {
    if (failure instanceof ApiError && failure.status === 401) return 'local';
    if (failure instanceof ApiError && failure.status === 404) return 'not_found';
    return 'unavailable';
  }
}
