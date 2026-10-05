import test from 'node:test';
import assert from 'node:assert/strict';
import { picoMoney, signedPicoMoney } from '../.test-build/model.js';

test('realized losses retain their sign with exact large integer arithmetic', () => {
  assert.equal(signedPicoMoney('-2500000000000'), '-$2.50');
  assert.equal(signedPicoMoney('-1234567890123456789012345'), '-$1,234,567,890,123.45');
  assert.equal(signedPicoMoney('-1'), '-$0.00');
  assert.equal(picoMoney('-2500000000000'), '—');
});

test('zero and confirmed positive realized results remain distinct from unavailable evidence', () => {
  assert.equal(signedPicoMoney('0'), '$0.00');
  assert.equal(signedPicoMoney('-0'), '$0.00');
  assert.equal(signedPicoMoney('12345000000000'), '$12.34');
  assert.equal(signedPicoMoney(undefined), '—');
  assert.equal(signedPicoMoney(null), '—');
});

test('signed amounts reject lossy numbers, decimals, interpolation and oversized values', () => {
  for (const value of [2500000000000, '', '1.5', ' 1', '+1', '--1', 'NaN', '${VALUE}', '1'.repeat(41)]) {
    assert.equal(signedPicoMoney(value), '—');
  }
});
