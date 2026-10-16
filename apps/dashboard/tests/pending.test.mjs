import test from 'node:test';
import assert from 'node:assert/strict';
import { clearPending, definitiveRejection, matchingReceipt, pendingKey, persistPending, restorePending } from '../.test-build/pending.js';

class Storage {
  values = new Map();
  getItem(key) { return this.values.get(key) ?? null; }
  setItem(key, value) { this.values.set(key, value); }
  removeItem(key) { this.values.delete(key); }
}
const command = {id: 'original-stable-id', expected_version: 7, action: 'pause'};
test('persist before transport; reload and unknown retries retain original identity and CAS', () => {
  const storage = new Storage();
  persistPending(storage, command);
  const transport = () => { assert.deepEqual(restorePending(storage), command); throw new Error('connection closed after dispatch'); };
  assert.throws(transport);
  const reloaded = restorePending(storage);
  assert.deepEqual(reloaded, command);
  persistPending(storage, reloaded);
  assert.deepEqual(restorePending(storage), command);
});
test('definitive 409 clears pending; transport, 408, 5xx and explicit unknown retain it', () => {
  const storage = new Storage(); persistPending(storage, command);
  for (const [status, kind] of [[0, 'outcome_unknown'], [401, 'unauthorized'], [403, 'origin_rejected'], [404, 'not_found'], [429, 'command_rate_limited'], [408, 'timeout'], [500, 'failed'], [503, 'engine_offline'], [409, 'outcome_unknown']]) assert.equal(definitiveRejection(status, kind), false);
  assert.equal(definitiveRejection(409, 'version_conflict'), true);
  if (definitiveRejection(409, 'version_conflict')) clearPending(storage);
  assert.equal(restorePending(storage), null);
});
test('masked authentication 404 cannot erase an earlier unknown command or exact limits identity', () => {
  const limits = {id: 'original-limits-id', expected_version: 5, limits: {initial_collateral_usd: '1000.000000', max_imbalance_shares: '25', lot_shares: '5', half_width: '0.045', max_feed_age_ms: 1000}};
  for (const payload of [command, limits]) {
    const storage = new Storage(); persistPending(storage, payload);
    // The initial request may have executed before its response was lost. A
    // later retry is rejected by authentication before it reaches the engine.
    if (definitiveRejection(404, 'not_found')) clearPending(storage);
    assert.deepEqual(restorePending(storage), payload);
  }
});
test('200 receipt requires original identity, safe applied version and known mode', () => {
  const reply = {receipt: {command_id: command.id, applied_version: 8, mode_at_ack: 'paused'}, replayed: true};
  assert.equal(matchingReceipt(reply, command), true);
  for (const receipt of [undefined, {}, {...reply.receipt, command_id: 'another-command'}, {...reply.receipt, applied_version: -1}, {...reply.receipt, applied_version: true}, {...reply.receipt, applied_version: 2 ** 53}, {...reply.receipt, mode_at_ack: 'unknown'}]) assert.equal(matchingReceipt({receipt}, command), false);
});
test('exact limits survive reload; malformed storage blocks without erasing unknown identity', () => {
  const storage = new Storage();
  const limits = {id: 'limits-id', expected_version: 5, limits: {initial_collateral_usd: '1000.000000', max_imbalance_shares: '25', lot_shares: '5', half_width: '0.045', max_feed_age_ms: 1000}};
  persistPending(storage, limits); assert.deepEqual(restorePending(storage), limits);
  storage.setItem(pendingKey, '{bad'); assert.throws(() => restorePending(storage)); assert.equal(storage.getItem(pendingKey), '{bad');
  assert.throws(() => persistPending({setItem() {throw new Error('quota');}}, command));
});
