'use strict';
// GET / (rewritten to this function): the read-only dashboard page, behind Basic auth.
const fs = require('node:fs');
const path = require('node:path');
const { guard, send } = require('../lib/auth');

const CSP = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; " +
  "img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
let cached = null;

function loadHtml() {
  if (cached) return cached;
  const candidates = [path.join(__dirname, '..', 'views', 'dashboard.html'), path.join(process.cwd(), 'views', 'dashboard.html')];
  for (const file of candidates) {
    try { cached = fs.readFileSync(file, 'utf8'); return cached; } catch { /* try the next location */ }
  }
  throw new Error('views/dashboard.html is missing from the deployment');
}

module.exports = async function page(req, res) {
  if (!(await guard(req, res))) return;
  if (req.method !== 'GET' && req.method !== 'HEAD') return send(res, 405, 'Method not allowed');
  let html;
  try { html = loadHtml(); } catch { return send(res, 500, 'Dashboard page is missing.'); }
  res.setHeader('Content-Security-Policy', CSP);
  return send(res, 200, req.method === 'HEAD' ? '' : html, 'text/html; charset=utf-8');
};
