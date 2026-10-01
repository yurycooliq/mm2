import assert from 'node:assert/strict';

// Small compatibility layer for migrated offline reference tests; no runtime application dependency.
export function expect(actual: unknown, negate = false) {
  const check = (success: boolean) => assert.equal(success, !negate);
  return {
    get not() { return expect(actual, !negate); },
    toBe(value: unknown) { if (negate) assert.notStrictEqual(actual, value); else assert.strictEqual(actual, value); },
    toEqual(value: unknown) { if (negate) assert.notDeepStrictEqual(actual, value); else assert.deepStrictEqual(actual, value); },
    toContain(value: unknown) { check((actual as { includes(v: unknown): boolean }).includes(value)); },
    toHaveLength(value: number) { check((actual as { length: number }).length === value); },
    toBeUndefined() { check(actual === undefined); },
    toThrow(message?: string) {
      let caught: unknown;
      try { (actual as () => unknown)(); } catch (error) { caught = error; }
      check(caught !== undefined && (!message || String(caught).includes(message)));
    },
  };
}
