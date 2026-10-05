import test from 'node:test';
import assert from 'node:assert/strict';
import { ApiError, telegramInitData } from '../.test-build/api.js';
import { ownerAccess } from '../.test-build/auth.js';

function client({ stateStatus = 200, telegramStatus = 200, telegramReply = { authenticated: true } } = {}) {
  const calls = [];
  return { calls,
    async state() { calls.push('state'); if (stateStatus !== 200) throw new ApiError(stateStatus, 'not_found', 'Rejected'); return {}; },
    async telegramAuth(data) { calls.push(['telegram', data]); if (telegramStatus !== 200) throw new ApiError(telegramStatus, 'not_found', 'Rejected'); return telegramReply; },
  };
}

test('Mini App owner authenticates automatically before any existing browser session lookup', async () => {
  const api = client();
  assert.equal(await ownerAccess(api, async () => 'raw-signed-init-data'), 'authenticated');
  assert.deepEqual(api.calls, [['telegram', 'raw-signed-init-data']]);
});

test('wrong owner, expired signature and replay never fall back to a cookie or token login', async () => {
  for (const telegramStatus of [400, 401, 403, 404, 429]) {
    const api = client({ telegramStatus });
    assert.equal(await ownerAccess(api, async () => 'untrusted-init-data'), 'not_found');
    assert.deepEqual(api.calls, [['telegram', 'untrusted-init-data']]);
  }
});

test('invalid success response cannot acknowledge automatic authentication', async () => {
  for (const telegramReply of [{}, { authenticated: false }, { authenticated: 'true' }, null]) {
    const api = client({ telegramReply });
    assert.equal(await ownerAccess(api, async () => 'signed-data'), 'unavailable');
    assert.equal(api.calls.length, 1);
  }
});

test('SDK and network failures expose no manual token fallback', async () => {
  const api = client();
  assert.equal(await ownerAccess(api, async () => { throw new Error('SDK unavailable'); }), 'unavailable');
  assert.deepEqual(api.calls, []);
  assert.equal(await ownerAccess(client({ telegramStatus: 0 }), async () => 'signed-data'), 'unavailable');
});

test('production direct browser access is neutral while offline legacy development retains its token login', async () => {
  assert.equal(await ownerAccess(client({ stateStatus: 404 }), async () => null), 'not_found');
  assert.equal(await ownerAccess(client({ stateStatus: 401 }), async () => null), 'local');
  assert.equal(await ownerAccess(client({ stateStatus: 0 }), async () => null), 'unavailable');
  assert.equal(await ownerAccess(client(), async () => null), 'authenticated');
});

test('existing Telegram SDK data is used even when the client has removed the URL fragment', async () => {
  let ready = 0, expanded = 0;
  globalThis.location = { hash: '' };
  globalThis.window = { Telegram: { WebApp: { initData: 'raw-signed-data', ready: () => ready++, expand: () => expanded++ } } };
  assert.equal(await telegramInitData(), 'raw-signed-data');
  assert.equal(ready, 1); assert.equal(expanded, 1);
  delete globalThis.window; delete globalThis.location;
});

test('empty data in a declared Telegram launch is rejected rather than treated as browser token access', async () => {
  globalThis.location = { hash: '#tgWebAppData=' };
  globalThis.window = { Telegram: { WebApp: { initData: '' } } };
  await assert.rejects(telegramInitData(), /launch data unavailable/);
  delete globalThis.window; delete globalThis.location;
});

test('a plain browser does not load the Telegram SDK', async () => {
  globalThis.location = { hash: '' }; globalThis.window = {};
  assert.equal(await telegramInitData(), null);
  delete globalThis.window; delete globalThis.location;
});
