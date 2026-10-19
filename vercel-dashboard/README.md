# arbbot hosted dashboard (read-only)

A password-protected, read-only view of the bot, hosted on Vercel.

```
bot (your computer) --push every 10 s--> Upstash Redis <--read-only token-- Vercel app <-- you (Basic auth)
```

* The bot only ever **sends** data out. Nothing on your computer is reachable from the internet.
* The page has **no controls** (no pause, no flatten) and the hosted code never holds a key that can write.
* What is published: market table, open positions, recent trades, P&L, event log, feed health. Not published: API keys,
  Telegram token, error strings, control state.
* If the bot stops, the stored snapshot expires after 2 minutes and the page says "The bot is not publishing".
* Until `DASHBOARD_PASSWORD` is set the app serves only `503 not configured` (it fails closed).

## Setup (about 10 minutes; you enter the secrets, nobody else needs to see them)

**1. Create the data store (free).** On <https://console.upstash.com> create a **Redis** database (any region near you).
Open it and copy, from the REST API section: the **REST URL**, the **read-write token**, and the **read-only token**
(there is a *Read-Only Token* toggle).

**2. Give the Vercel app its settings.** In Vercel: project `arbbot-dashboard` → Settings → Environment Variables
(Production), add:

| Name | Value |
|---|---|
| `DASHBOARD_PASSWORD` | a long password you choose (at least 12 characters; 16+ random is better) |
| `KV_REST_API_URL` | the Upstash REST URL |
| `KV_REST_API_READ_ONLY_TOKEN` | the Upstash **read-only** token (never the read-write one) |
| `DASHBOARD_USER` | optional; defaults to `viewer` |

Mark the password and token as **Sensitive**. Then redeploy (Deployments → ••• → Redeploy) so they take effect.

**3. Tell the bot where to publish.** In `~/Documents/binance-arb-bot/.env` set:

```
DASHBOARD_PUSH_URL=<the Upstash REST URL>
DASHBOARD_PUSH_TOKEN=<the Upstash READ-WRITE token>
```

Restart the bot. The log should say `hosted dashboard: publishing a read-only snapshot every 10s`.

**4. Open it:** <https://arbbot-dashboard.vercel.app> and log in with user `viewer` and your password.

## Budget

At the default 10 s interval the bot makes ~260,000 Upstash commands a month, inside the free tier. Each open browser tab
adds one read every 5 s while it is visible. Raise `[publisher] interval_s` in `config.toml` to use less.

## Maintenance

* The page is generated from the local dashboard: after changing `web/index.html` run
  `.venv/bin/python scripts/build_hosted_dashboard.py`, then redeploy `vercel-dashboard/`.
* Tests: `cd vercel-dashboard && node --test` (16 tests: auth fails closed, no secret or upstream text leaks, read-only
  token preferred, offline/stale handling).
* Rotate access by changing `DASHBOARD_PASSWORD`; rotate the data store by regenerating the Upstash tokens and updating
  both places.
