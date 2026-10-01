import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';

export function catchError<T>(task: () => T): [unknown, T | undefined];
export function catchError<T>(task: Promise<T>): Promise<[unknown, T | undefined]>;
export function catchError<T>(task: (() => T) | Promise<T>): [unknown, T | undefined] | Promise<[unknown, T | undefined]> {
  if (typeof task !== 'function') return task.then(value => [null, value] as [unknown, T], error => [error, undefined]);
  try { return [null, task()]; } catch (error) { return [error, undefined]; }
}

// This standalone validator deliberately supports only compact five/fifteen minute identities.
export function parseCryptoUpDownSlug(slug: string) {
  const match = /^(btc|eth|sol|xrp)-updown-(5m|15m)-(\d+)$/.exec(slug);
  if (!match) return null;
  const startTs = Number(match[3]);
  if (!Number.isSafeInteger(startTs)) return null;
  return { startTs, durationSec: match[2] === '5m' ? 300 : 900 };
}

export async function* readLines(paths: readonly string[]): AsyncGenerator<string> {
  for (const path of paths) {
    const input = path.endsWith('.gz') ? createReadStream(path).pipe(createGunzip()) : createReadStream(path);
    const reader = createInterface({ input, crlfDelay: Infinity });
    try { for await (const line of reader) if (line) yield line; }
    finally { reader.close(); input.destroy(); }
  }
}
