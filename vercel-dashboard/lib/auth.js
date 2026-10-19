'use strict';
// Shared guard for every route: HTTP Basic auth against DASHBOARD_PASSWORD, failing CLOSED.
// With no (or a short) password configured nothing is served, so a mis-deployed copy can never be public.
const crypto = require('node:crypto');

const MIN_PASSWORD = 12;
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest();
const same = (a, b) => crypto.timingSafeEqual(sha(a), sha(b)); // equal-length digests: constant-time compare
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function setSecurityHeaders(res) {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
}

function send(res, status, body, type = 'text/plain; charset=utf-8') {
  res.statusCode = status;
  res.setHeader('Content-Type', type);
  res.end(body);
}

async function guard(req, res) {
  setSecurityHeaders(res);
  const password = process.env.DASHBOARD_PASSWORD || '';
  if (password.length < MIN_PASSWORD) {
    send(res, 503, `Dashboard is not configured: set DASHBOARD_PASSWORD (at least ${MIN_PASSWORD} characters).`);
    return false;
  }
  const user = process.env.DASHBOARD_USER || 'viewer';
  const header = req.headers.authorization || '';
  let ok = false;
  if (header.startsWith('Basic ')) {
    const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
    const i = decoded.indexOf(':'); // the password itself may contain ':'
    if (i >= 0) {
      const userOk = same(user, decoded.slice(0, i));
      const passOk = same(password, decoded.slice(i + 1));
      ok = userOk && passOk;
    }
  }
  if (ok) return true;
  await sleep(Number(process.env.AUTH_FAIL_DELAY_MS ?? 400)); // makes online guessing slow
  res.setHeader('WWW-Authenticate', 'Basic realm="arbbot", charset="UTF-8"');
  send(res, 401, 'Authentication required');
  return false;
}

module.exports = { guard, send, setSecurityHeaders, MIN_PASSWORD };
