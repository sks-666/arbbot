'use strict';
// node --test test/   (no dependencies). Exercises the Vercel functions with fake req/res objects and a fake fetch.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

process.env.AUTH_FAIL_DELAY_MS = '0';
const page = require('../api/page');
const snapshot = require('../api/snapshot');

const PASSWORD = 'correct-horse-battery-staple';
const basic = (user, pass) => 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');

function makeRes() {
  const res = { statusCode: 200, headers: {}, body: '' };
  res.setHeader = (k, v) => { res.headers[k.toLowerCase()] = v; };
  res.end = (b = '') => { res.body = String(b); };
  return res;
}
const makeReq = (method = 'GET', authorization) => ({ method, headers: authorization ? { authorization } : {} });

const ENV_KEYS = ['DASHBOARD_PASSWORD', 'DASHBOARD_USER', 'KV_REST_API_URL', 'UPSTASH_REDIS_REST_URL', 'KV_REST_API_READ_ONLY_TOKEN',
  'UPSTASH_REDIS_REST_READ_ONLY_TOKEN', 'KV_REST_API_TOKEN', 'UPSTASH_REDIS_REST_TOKEN', 'SNAPSHOT_KEY'];
let realFetch;
test.beforeEach(() => { realFetch = global.fetch; for (const k of ENV_KEYS) delete process.env[k]; });
test.afterEach(() => { global.fetch = realFetch; });

function configure(extra = {}) {
  process.env.DASHBOARD_PASSWORD = PASSWORD;
  process.env.KV_REST_API_URL = 'https://store.example.upstash.io/';
  process.env.KV_REST_API_READ_ONLY_TOKEN = 'READ-ONLY-TOKEN';
  Object.assign(process.env, extra);
}
const fakeFetch = (handler) => { const calls = []; global.fetch = async (url, opts) => { calls.push({ url, opts }); return handler(url, opts); }; return calls; };
const jsonResponse = (body, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => body });

// ---- auth ---------------------------------------------------------------------------------------
test('fails closed when no password is configured', async () => {
  for (const fn of [page, snapshot]) {
    const res = makeRes();
    await fn(makeReq('GET', basic('viewer', PASSWORD)), res);
    assert.equal(res.statusCode, 503);
    assert.match(res.body, /not configured/);
  }
});

test('refuses a weak (short) password even when it matches', async () => {
  process.env.DASHBOARD_PASSWORD = 'short';
  const res = makeRes();
  await page(makeReq('GET', basic('viewer', 'short')), res);
  assert.equal(res.statusCode, 503);
});

test('401 with a Basic challenge for missing, wrong-user and wrong-password credentials', async () => {
  configure();
  for (const auth of [undefined, 'Bearer abc', basic('viewer', 'wrong-password-xx'), basic('intruder', PASSWORD), 'Basic !!notbase64!!']) {
    const res = makeRes();
    await page(makeReq('GET', auth), res);
    assert.equal(res.statusCode, 401, String(auth));
    assert.match(res.headers['www-authenticate'], /^Basic realm=/);
    assert.ok(!res.body.includes('<html'), 'no page content before authentication');
  }
});

test('correct credentials pass, including a password containing a colon', async () => {
  configure({ DASHBOARD_PASSWORD: 'pass:with:colons-123' });
  const res = makeRes();
  await page(makeReq('GET', basic('viewer', 'pass:with:colons-123')), res);
  assert.equal(res.statusCode, 200);
});

test('a custom user name is honoured', async () => {
  configure({ DASHBOARD_USER: 'sam' });
  let res = makeRes();
  await page(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 401);
  res = makeRes();
  await page(makeReq('GET', basic('sam', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
});

test('every response is uncacheable, unindexable and unframeable', async () => {
  configure();
  for (const auth of [undefined, basic('viewer', PASSWORD)]) {
    const res = makeRes();
    await page(makeReq('GET', auth), res);
    assert.equal(res.headers['cache-control'], 'no-store');
    assert.match(res.headers['x-robots-tag'], /noindex/);
    assert.equal(res.headers['x-frame-options'], 'DENY');
    assert.equal(res.headers['x-content-type-options'], 'nosniff');
  }
});

// ---- page ---------------------------------------------------------------------------------------
test('the page is read-only: it shows the badge and contains no control code', async () => {
  configure();
  const res = makeRes();
  await page(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
  assert.match(res.headers['content-type'], /text\/html/);
  assert.match(res.body, /READ-ONLY/);
  assert.match(res.body, /\/api\/snapshot/);
  for (const forbidden of ['btn-pause', 'btn-flat', 'WebSocket(', '__TOKEN__', 'X-Token', '/api/halt', '/api/flatten']) {
    assert.ok(!res.body.includes(forbidden), `page must not contain ${forbidden}`);
  }
  assert.match(res.headers['content-security-policy'], /default-src 'none'/);
  assert.match(res.headers['content-security-policy'], /connect-src 'self'/);
});

test('HEAD returns no body and other methods are rejected', async () => {
  configure();
  let res = makeRes();
  await page(makeReq('HEAD', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.body, '');
  res = makeRes();
  await page(makeReq('POST', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 405);
});

test('the committed page has no control endpoints or secrets', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'views', 'dashboard.html'), 'utf8');
  assert.ok(!/(upstash|BEARER|api_key|secret)/i.test(html));
  assert.ok(html.includes('/api/snapshot'));
});

// ---- snapshot -----------------------------------------------------------------------------------
test('snapshot: not configured -> 503 without calling out', async () => {
  process.env.DASHBOARD_PASSWORD = PASSWORD;
  const calls = fakeFetch(() => { throw new Error('should not be called'); });
  const res = makeRes();
  await snapshot(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 503);
  assert.equal(calls.length, 0);
});

test('snapshot: unauthenticated requests never reach the store', async () => {
  configure();
  const calls = fakeFetch(() => jsonResponse({ result: '{}' }));
  const res = makeRes();
  await snapshot(makeReq('GET'), res);
  assert.equal(res.statusCode, 401);
  assert.equal(calls.length, 0);
});

test('snapshot: returns the stored snapshot with the server time, using the READ-ONLY token and an encoded key', async () => {
  configure({ KV_REST_API_TOKEN: 'FULL-ACCESS-TOKEN', SNAPSHOT_KEY: 'arbbot:snapshot' });
  const stored = { schema: 1, published_at: 1000, markets: [{ sym: 'BTCUSDT' }] };
  const calls = fakeFetch(() => jsonResponse({ result: JSON.stringify(stored) }));
  const res = makeRes();
  await snapshot(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.deepEqual(body.snapshot, stored);
  assert.ok(body.server_time > 1e9);
  assert.equal(calls[0].url, 'https://store.example.upstash.io/get/arbbot%3Asnapshot'); // trailing slash trimmed, key encoded
  assert.equal(calls[0].opts.headers.Authorization, 'Bearer READ-ONLY-TOKEN'); // never the full-access token
  assert.equal(res.headers['content-type'], 'application/json; charset=utf-8');
});

test('snapshot: an expired key means the bot is offline', async () => {
  configure();
  fakeFetch(() => jsonResponse({ result: null }));
  const res = makeRes();
  await snapshot(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.offline, true);
  assert.equal(body.snapshot, undefined);
});

test('snapshot: store problems become generic 502s that never leak upstream text or tokens', async () => {
  configure();
  const secretish = 'WRONGPASS READ-ONLY-TOKEN store.example.upstash.io';
  for (const handler of [
    () => jsonResponse({ error: secretish }, 401),
    () => jsonResponse({ error: secretish }, 200),
    () => { throw new Error(secretish); },
    () => jsonResponse({ result: '{not json' }),
  ]) {
    fakeFetch(handler);
    const res = makeRes();
    await snapshot(makeReq('GET', basic('viewer', PASSWORD)), res);
    assert.equal(res.statusCode, 502);
    assert.ok(!res.body.includes('READ-ONLY-TOKEN') && !res.body.includes('upstash') && !res.body.includes('WRONGPASS'), res.body);
  }
});

test('snapshot: only GET is allowed', async () => {
  configure();
  const res = makeRes();
  await snapshot(makeReq('POST', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 405);
});

test('snapshot: accepts the Upstash variable names as well as the Vercel KV ones', async () => {
  process.env.DASHBOARD_PASSWORD = PASSWORD;
  process.env.UPSTASH_REDIS_REST_URL = 'https://other.example.io';
  process.env.UPSTASH_REDIS_REST_READ_ONLY_TOKEN = 'RO2';
  const calls = fakeFetch(() => jsonResponse({ result: '{"a":1}' }));
  const res = makeRes();
  await snapshot(makeReq('GET', basic('viewer', PASSWORD)), res);
  assert.equal(res.statusCode, 200);
  assert.equal(calls[0].opts.headers.Authorization, 'Bearer RO2');
});
