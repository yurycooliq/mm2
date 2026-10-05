import type { EngineState } from './types';

export function micro(value: unknown, digits = 6): string {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) return '—';
  const exact = BigInt(value);
  return `${exact / 1_000_000n}.${(exact % 1_000_000n).toString().padStart(6, '0').slice(0, digits)}`;
}
export function money(value: unknown): string { const result = micro(value, 2); return result === '—' ? result : `$${Number(result.split('.')[0]).toLocaleString('en-US')}.${result.split('.')[1]}`; }
export function simulated(state: EngineState | null) { return state?.simulation?.simulated === true ? state.simulation : null; }
export function modeLabel(state: EngineState | null): string {
  if (!state) return 'Waiting for engine';
  const mode = state.mode === 'shadow' && state.execution_mode !== 'shadow' ? 'running' : state.mode;
  const labels = { emulator: 'Demo emulator', simulation: 'Simulation', shadow: 'Public shadow', trading: 'Trading', both: 'Trading + simulation' };
  return `${labels[state.execution_mode]} · ${mode}`;
}
export function picoMoney(value: unknown): string {
  if (typeof value !== 'string' || !/^\d{1,40}$/.test(value)) return '—';
  const cents = BigInt(value) / 10_000_000_000n;
  return `$${(cents / 100n).toLocaleString('en-US')}.${(cents % 100n).toString().padStart(2, '0')}`;
}
export function signedPicoMoney(value: unknown): string {
  if (typeof value !== 'string' || !/^-?\d{1,40}$/.test(value)) return '—';
  const exact = BigInt(value);
  const cents = (exact < 0n ? -exact : exact) / 10_000_000_000n;
  return `${exact < 0n ? '-' : ''}$${(cents / 100n).toLocaleString('en-US')}.${(cents % 100n).toString().padStart(2, '0')}`;
}
export function safeVersion(state: EngineState | null): boolean { return !!state && Number.isSafeInteger(state.command_version) && state.command_version >= 0; }
export function timeLabel(ms: number): string { return new Date(ms).toLocaleTimeString([], { hour12: false }); }
