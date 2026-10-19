'use strict';
// GET /api/snapshot: the latest bot snapshot from Upstash Redis (READ-ONLY token), behind Basic auth.
// Never forwards upstream error text (it could name hosts or hint at credentials).
const { guard, send } = require('../lib/auth');

const env = (...names) => names.map((n) => process.env[n]).find(Boolean) || '';
const json = (res, status, obj) => send(res, status, JSON.stringify(obj), 'application/json; charset=utf-8');

module.exports = async function snapshot(req, res) {
  if (!(await guard(req, res))) return;
  if (req.method !== 'GET') return send(res, 405, 'Method not allowed');

  const base = env('KV_REST_API_URL', 'UPSTASH_REDIS_REST_URL').replace(/\/$/, '');
  const token = env('KV_REST_API_READ_ONLY_TOKEN', 'UPSTASH_REDIS_REST_READ_ONLY_TOKEN', 'KV_REST_API_TOKEN', 'UPSTASH_REDIS_REST_TOKEN');
  if (!base || !token) return json(res, 503, { error: 'The data store is not configured.' });

  const key = process.env.SNAPSHOT_KEY || 'arbbot:snapshot';
  const serverTime = Date.now() / 1000;
  try {
    const r = await fetch(`${base}/get/${encodeURIComponent(key)}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(5000),
    });
    const data = await r.json();
    if (!r.ok || data.error) return json(res, 502, { error: 'The data store returned an error.' });
    if (data.result == null) return json(res, 200, { offline: true, server_time: serverTime }); // key expired: bot is not publishing
    let snap;
    try { snap = JSON.parse(data.result); } catch { return json(res, 502, { error: 'The stored snapshot is unreadable.' }); }
    return json(res, 200, { snapshot: snap, server_time: serverTime });
  } catch {
    return json(res, 502, { error: 'Could not reach the data store.' });
  }
};
