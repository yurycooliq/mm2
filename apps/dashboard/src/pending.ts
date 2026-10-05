import type { Command, CommandReply } from './types';

export const pendingKey = 'mm2.pending-command.v1';
export interface CommandStorage { getItem(key: string): string | null; setItem(key: string, value: string): void; removeItem(key: string): void }

function validCommand(value: unknown): value is Command {
  if (!value || typeof value !== 'object') return false;
  const data = value as Record<string, unknown>;
  if (typeof data.id !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(data.id) || !Number.isSafeInteger(data.expected_version) || Number(data.expected_version) < 0) return false;
  if ('action' in data) return Object.keys(data).length === 3 && ['pause', 'resume', 'kill', 'reset'].includes(String(data.action));
  if (Object.keys(data).length !== 3 || !data.limits || typeof data.limits !== 'object') return false;
  const limits = data.limits as Record<string, unknown>;
  const decimals = ['initial_collateral_usd', 'max_imbalance_shares', 'lot_shares', 'half_width'];
  return Object.keys(limits).length === 5 && decimals.every(key => typeof limits[key] === 'string' && /^[0-9]{1,20}(?:\.[0-9]{1,24})?$/.test(limits[key] as string) && !/[1-9]/.test((limits[key] as string).split('.')[1]?.slice(6) ?? ''))
    && Number.isSafeInteger(limits.max_feed_age_ms) && Number(limits.max_feed_age_ms) >= 1 && Number(limits.max_feed_age_ms) <= 60000;
}

export function restorePending(storage: CommandStorage): Command | null {
  const raw = storage.getItem(pendingKey);
  if (raw === null) return null;
  if (raw.length > 8192) throw new Error('Saved command identity is invalid; controls are blocked.');
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw new Error('Saved command identity is invalid; controls are blocked.'); }
  if (!validCommand(value)) throw new Error('Saved command identity is invalid; controls are blocked.');
  return value;
}

export function persistPending(storage: CommandStorage, command: Command): void {
  if (!validCommand(command)) throw new Error('Command identity is invalid; request was not sent.');
  storage.setItem(pendingKey, JSON.stringify(command));
}
export function clearPending(storage: CommandStorage): void { storage.removeItem(pendingKey); }
export function matchingReceipt(reply: CommandReply, command: Command): boolean {
  return !!reply?.receipt && reply.receipt.command_id === command.id && Number.isSafeInteger(reply.receipt.applied_version)
    && reply.receipt.applied_version >= 0 && ['paused', 'shadow', 'killed'].includes(reply.receipt.mode_at_ack);
}
export function definitiveRejection(status: number, kind: string): boolean {
  // Authentication, Origin and rate rejection do not settle an earlier dispatch.
  return status >= 400 && status < 500 && ![401, 403, 404, 408, 429].includes(status) && kind !== 'outcome_unknown';
}
