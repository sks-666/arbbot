# binance-arb-bot

Scans 50+ Binance **USD-M perpetuals** against their **spot** counterparts every second, finds
perp/spot basis dislocations, and trades them as hedged cash-and-carry positions (long spot + short
perp). Includes a live multi-market dashboard, risk controls, a paper-trading mode (default) and an
opt-in live executor.

## Quick start

```bash
cd ~/Documents/binance-arb-bot
.venv/bin/python run.py --feed sim          # offline demo with synthetic dislocations
.venv/bin/python run.py                     # REAL Binance WebSocket data (REST fallback), PAPER trading, no keys
```

Open <http://127.0.0.1:8765>. Run the tests with `.venv/bin/python -m unittest tests.test_core`.

| Command | What it does |
|---|---|
| `run.py --feed sim` | Offline simulator. Dislocations are synthetic. State kept separately. |
| `run.py` | Live public Binance data over WebSockets (REST fallback), **paper** fills (no orders ever sent). |
| `run.py --feed rest` | REST polling only (1 Hz). |
| `run.py --no-trade` | Monitor only. |
| `run.py --live --testnet` | Real order flow against Binance **testnet**. Needs testnet API keys. |
| `run.py --live` | Real money. Also needs `ARBBOT_LIVE_ACK` (see below). |

Useful flags: `--max-markets N`, `--interval S`, `--min-samples N`, `--port P`.

## The strategy, and what it can honestly deliver

### Core entry formula (all in basis points)

```
B_mid  = (perp_mid − spot_mid) / spot_mid            B_entry = (perp_bid − spot_ask) / spot_ask   (executable)
z      = (B_mid − μ) / σ                              μ, σ = median and 1.4826·MAD of the last 5 min of B_mid
target = μ + 0.5σ + ExitCost                          ExitCost = p75 of recent exit spreads (never below today's)
Net    = (B_entry − target) + ExpectedFunding − Fees − Slippage − RiskBuffer
```

Enter only if `z ≥ 3` **and** `Net ≥ 1 bp` **and** the perp is at/above spot. What makes it conservative:

* **Robust baseline.** Median/MAD, so the dislocation itself can't inflate its own yardstick.
* **Frozen at entry.** For an open trade μ and σ are frozen; convergence means `(B_mid − μ_entry)/σ_entry ≤ 0.5`.
  A rolling baseline that the spike has dragged upward could declare "converged" while the basis never came back.
* **Discrete funding.** Funding counts only for funding events actually crossed during the expected hold, using each
  coin's real interval (in live data 32 of 55 coins settle every 8 h, 22 every 4 h, 1 every hour) and exchange caps.
* **Risk buffer.** A floor (`risk_buffer_bps`: **8 bps** in the shipped `config.toml`, because the first real paper
  trades missed their predictions by 30-180 bps; the code default is 2), rising to the 95th percentile of
  `predicted − realised` P&L over the last 100 trades (capped at 30 bps). If the model keeps over-promising,
  entries dry up until reality matches.
* **Thin-touch guard.** A market whose best bid or ask holds less than `min_touch_depth_usd` (**$300**) is skipped.
  Thin books produce fake signals: AVAUSDT showed a +69 bps "edge" on ~$50-110 of touch liquidity while the spot
  price was simply dumping, and lost 111 bps in 3 seconds.
* **Decide on fresh quotes.** The order-book request takes time and the market keeps moving. After it returns, the
  signal is **re-evaluated on the freshest quotes** and the trade is skipped if it has gone (`SKIP … signal gone`).
  The book request has a 1.5 s timeout, the entry basis recorded is the one **actually obtained** from the fill
  prices, and the position's open time is stamped at the real fill so `min_hold_s` counts real seconds.
* **Size-dependent edge.** Right before entering, both order books are walked (VWAP for the real size) and the
  bot takes the largest of `[100%, 75%, 50%, 35%, 20%]` of the target size whose Net still clears the bar.
* **P&L stop.** Exit when closing now would lose more than the round-trip cost plus 25 bps (real leg prices, funding,
  fees); plus a mid-basis stop, so a momentarily wide spread alone can't trigger it.

With default VIP-0 fees the round trip costs **34 bps** (live mode replaces this with your account's real fee tier),
so only dislocations well above that qualify. The dashboard and `/market BTC` show each market's net edge so you
can see exactly why nothing is firing.

Be realistic about the edge: on liquid USDT perps the basis normally sits within a few bps of spot, and
the rare large gaps are contested by co-located market makers with maker-fee rebates. A bot polling REST
once per second will usually be late to them. Paper mode exists so you can measure the *actual* net P&L
before risking money. Lowering fees (BNB discount, VIP tier, maker orders) is what widens the opportunity
set; edit `[fees]` in `config.toml` to match your account.

Only the **positive-basis** side is traded (buy spot, short perp), because the reverse needs borrowed spot.

## Backtesting: recorder and replay

On a real feed the bot records what it sees to `data/recordings/quotes-YYYY-MM-DD.jsonl.gz` (one gzip file per UTC
day, safe to read while the bot runs, ~100-130 MB/day; the oldest files are deleted beyond `[recorder] max_mb`):
top-of-book quotes for every fresh market each second, mark/index/funding, one **order-book snapshot per second**
(round-robin over all markets), the books fetched at each signal, and the bot's own entries and exits.
The simulator is never recorded. A recording failure disables recording only; it can never affect trading.

```bash
.venv/bin/python backtest.py info                    # what has been recorded
.venv/bin/python backtest.py slippage --sizes 200,1000,5000,25000
.venv/bin/python backtest.py replay                  # the real strategy over the recorded days
.venv/bin/python backtest.py replay --set strategy.z_entry=4 --set fees.futures_taker_bps=4
.venv/bin/python backtest.py replay --sweep strategy.z_entry=2.5,3,4,5     # one row per value
.venv/bin/python backtest.py replay --no-depth       # top-of-book only, to see what depth-aware sizing is worth
```

* **`slippage`** measures book impact per fill (bps vs the touch) by size from the recorded snapshots, the share of
  snapshots too thin to fill at all, the hardest markets, and compares the p75 with your `[fees] slippage_bps`.
* **`replay`** drives the same Engine, Scanner, Risk and PaperExecutor as the live bot on a simulated clock, so a
  parameter change shows exactly what it would have done on those days: trades, win rate, P&L per day, max drawdown,
  exit reasons, and the model error (predicted − realised bps) that feeds the adaptive risk buffer.
* **Honest limits.** Replay sizes and fills entries from the most recent recorded snapshot of that market (≤120 s old,
  `--depth-age`), otherwise top-of-book. Exits fill at top-of-book plus the configured slippage. Latency slippage
  (price moving between decision and fill) can't be measured from recordings, only from live fills, so keep
  `slippage_bps` as a cushion for it. A few days of paper data is anecdote, not proof: sweep widely, then check that
  the parameters you like still work on days they weren't tuned on.

## Risk controls

| Control | Behaviour |
|---|---|
| Paper by default | Real orders need `--live` **and** `execution.mode="live"` in config; mainnet also needs `ARBBOT_LIVE_ACK`. |
| Size limits | Per-position size, max open positions, total exposure cap, ≤50% of top-of-book depth, the largest size whose edge survives the real order book, and a $300 minimum touch depth. |
| Daily loss limit | UTC-day realised loss limit halts entries **and flattens**. Rebuilt from the trade log, so a restart cannot reset it. |
| Kill switch | `touch KILL` in the project folder halts and flattens within a second. Delete the file to re-arm. |
| Fault breaker | 3 consecutive execution errors, or an unhedged leg, latches a halt that needs a restart. |
| Stale data | No signals on quotes older than 3 s; positions are force-closed after 30 s of staleness. |
| Data integrity | Markets with mark ≠ index by >50 bps are skipped (halted spot, thin books); entries need the perp at/above spot. |
| Exits | Convergence against the baseline **frozen at entry** (`(B_mid − μ₀)/σ₀ ≤ 0.5`, after ≥5 s), P&L stop (close-now loss > round-trip cost + 25 bps), mid-basis stop (+25 bps), 15-minute timeout. |
| Dashboard | Binds to localhost; Host/Origin checks; control endpoints need a per-run token. |

## Architecture

```
feeds.py      WsFeed (streams) + RestFeed fallback via HybridFeed | SimFeed (offline)  ->  Board
scanner.py    robust baseline, discrete funding, exit cost, risk buffer, fully-costed net edge  ->  Signal
sizing.py     order-book walk (VWAP), size-dependent edge, REST depth fetch
risk.py       sizing, caps, thin-touch guard, cooldowns, daily loss, kill switch, fault breaker
engine.py     each tick: scan -> exits (frozen baseline, P&L stop) -> entries (fresh-quote re-check) -> snapshot
executor.py   PaperExecutor (book-VWAP fills + fees + slippage)
live.py       LiveExecutor (both legs concurrently, partial-fill rebalancing, real fee tiers, reconcile)
recorder.py   gzip-JSONL market recorder + depth sampler (data/recordings)
backtest.py   (arbbot/backtest.py) info | slippage | replay with --set / --sweep
commands.py   Telegram questions and /pause /resume (your chat only)
portfolio.py  positions + SQLite persistence (data/<mode>-<env>-<feed>.db)
server.py     aiohttp dashboard + WebSocket snapshots + guarded controls
alerts.py     Telegram notifier (queued, rate-limited, token-safe)
```

`1000SHIB`-style perps are mapped onto the unscaled spot symbol with a price multiplier.

### Data feed

* **WebSocket (default).** Futures: `!markPrice@arr@1s` (mark, index, funding) plus a `@bookTicker` per
  market; spot: a `@bookTicker` per market. Two connections, auto-reconnect with backoff.
* **REST fallback.** If either stream goes quiet for `ws_stale_s` (5 s) the bot polls REST at 1 Hz until the
  streams recover, then hands back. The dashboard shows `WebSocket`, or `REST (fallback)`.
* **Quiet books.** A book stream only emits on change, so while a connection is alive quotes that changed
  within `ws_max_silent_s` are re-confirmed each scan. A symbol silent longer than that goes stale.
* **Speed.** Data is always the latest tick; scan cadence is `feed.interval_s` (try `0.25`). REST weight at 1 Hz
  is ~900/2400 per minute, so keep REST-only scans at 1 s or slower.
* Stream hosts are overridable with `ws_futures_url` / `ws_spot_url` in `config.toml` in case Binance changes
  its endpoints; the testnet defaults are best-known values and worth confirming.

## Telegram alerts

The bot messages you about **signals** (always `LONG spot / SHORT perp`; that is the only side it trades),
**entries and exits** with P&L, **halts / resumes** (manual pause, kill file, daily loss limit), **faults**
(unhedged leg, repeated execution errors, startup mismatch), **feed health** (REST fallback, outage, recovery)
and **start / stop / crash**. You can also **ask it questions** (next section).

1. In Telegram, message **@BotFather** → `/newbot` → copy the token into `.env` as `TELEGRAM_BOT_TOKEN=`.
2. Open your new bot and press **Start**, then run `.venv/bin/python run.py --find-chat-id` and put the id in
   `.env` as `TELEGRAM_CHAT_ID=`.
3. Verify: `.venv/bin/python run.py --test-alert`. Alerts then switch on automatically at startup.

Tuning is in `[alerts]` in `config.toml` (per-category switches, one signal alert per market per 5 minutes,
10 s debounce on feed problems). Messages are queued and rate-limited, failures never affect trading, and the
token is never logged.

### Ask the bot from Telegram

Type in plain words or use the `/` menu (registered automatically):

| You send | You get |
|---|---|
| "any trades long or short in the market?" or `/signals` | Live signals (LONG spot / SHORT perp) or, if none, the closest candidates and why they don't qualify, plus markets where the perp is cheaper than spot (information only) |
| "how is BTC?" or `/market BTC` | Perp/spot prices, basis vs its normal range, z-score, net edge after costs, funding, depth, and which side the trade would be |
| `/status` | Feed health, entries active/halted, open positions, P&L, uptime |
| `/positions` · `/pnl` · `/top 10` | Open positions · profit/loss and last trades · best markets by edge |
| `/pause` · `/resume` | Stop / allow new entries |

Safety: only your configured chat is answered (everyone else is ignored); commands sent while the bot was
offline are skipped, never replayed; plain words like "stop trading" never change anything (only the explicit
`/pause` and `/resume` do); `/resume` cannot override a fault, the kill file or the daily loss limit; and
nothing here can place an order or close a position. Only **one** running bot may poll a given Telegram bot
token (a second one is detected and backs off). Turn it off with `commands = false` under `[alerts]`.

## Hosted read-only dashboard (Vercel)

The bot itself can't run on Vercel (it needs an always-on process, local storage, and Binance blocks many cloud
regions), so Vercel only hosts a **read-only view**: the bot pushes a sanitised snapshot every 10 s to Upstash Redis
and a password-protected Vercel app reads it. No endpoint on your computer is exposed, the page has no controls, and
the app serves nothing until a password is configured. Setup and architecture: `vercel-dashboard/README.md`.
Deployed at <https://arbbot-dashboard.vercel.app> (needs the setup steps before it shows data).

## Running 24/7 (macOS launchd)

`deploy/com.arbbot.plist` starts the bot at login and restarts it if it crashes (non-zero exit); a clean stop
is not restarted. Open positions and the daily-loss total survive restarts (SQLite).

```bash
mkdir -p ~/Library/LaunchAgents && cp deploy/com.arbbot.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.arbbot.plist   # start + enable
launchctl bootout gui/$(id -u)/com.arbbot                                   # stop + disable
tail -f logs/bot.log
```

Caveats: macOS may block a launch agent from reading `~/Documents` until the Python binary is granted
access under *System Settings → Privacy & Security → Files and Folders* (or move the project outside
`~/Documents`); the Mac must be awake and online (use `caffeinate` or Energy settings); and if your DNS
blocks Binance the bot stays safe but blind (feed down, no entries) until it recovers.

## Running without your computer (Render)

`python run.py --hosted` is the container-host mode: **paper only** (`--hosted --live` refuses to start), and the
recorder stays off unless `ARBBOT_RECORD=1`. Instead of the local dashboard (which has pause/flatten controls and is
never exposed) it serves, on `0.0.0.0:$PORT`, the same **read-only** page as the Vercel copy straight from the running
bot at `/` and `/api/snapshot`, behind HTTP Basic auth (user `viewer` or `DASHBOARD_USER`, password
`DASHBOARD_PASSWORD`, at least 12 characters). It fails closed: without a password only `GET /healthz` answers. Failed
logins are delayed and capped at 30 a minute. Entries can still be paused from Telegram (`/pause`, `/resume`). The
`Dockerfile` and `render.yaml` package it.

1. Put the project in a **private** GitHub repository. `.env`, `data/` and `logs/` are in `.gitignore`; check
   `git status` shows no `.env` before pushing. Never make this repo public.
2. In Render: New -> Blueprint -> pick the repo. `render.yaml` sets the **Frankfurt** region (Binance refuses US
   addresses; the region can't be changed later) and asks you for five secrets: `TELEGRAM_BOT_TOKEN`,
   `TELEGRAM_CHAT_ID`, `DASHBOARD_PUSH_URL`, `DASHBOARD_PUSH_TOKEN` (the Upstash **read-write** token; only needed for
   the Vercel copy) and `DASHBOARD_PASSWORD` (the login for the dashboard on Render itself).
3. **Stop the bot on your computer first.** Two instances would both publish to the same page and both poll Telegram.
4. Watch the Render logs for `hosted mode: health check on 0.0.0.0:...` and `hosted dashboard: publishing ...`.

Free vs paid: a free web service sleeps after 15 minutes without inbound requests (point an uptime pinger such as
UptimeRobot at `/healthz` every 5 minutes) and its disk is wiped on every restart, so the paper book and daily-loss
total reset. For history that survives, switch to a paid instance and uncomment the `disk` block in `render.yaml`
(mount `/app/data`).

## Verification status

| Area | Status |
|---|---|
| Scanner, risk, paper executor, persistence, engine cycle, kill switch, new core formula, recorder, replay | Unit-tested (186 tests, including regressions for the real losing trades) |
| Funding intervals/caps, order-book depth parsing, VWAP slippage | **Checked against real Binance** (public endpoints) |
| Partial-fill rebalancing, exit top-ups, fee-tier lookup | Tested against a fake exchange with programmable partial fills; **not run on Binance** |
| WebSocket feed, REST fallback, chunked connections, Telegram, reconciliation | Tested against local mocks that mimic Binance/Telegram message formats |
| REST universe load, WebSocket streams, REST→WS handoff, dashboard, pause/resume | **Run against real Binance** (2h+ paper session, 0 stream errors until DNS dropped; clean degradation afterwards) |
| Strategy on real data | Found and fixed: broken-price markets (halted spot / mark≠index) and entries below spot (see tests) |
| `live.py` orders, preflight balance endpoints, reconciliation endpoints | **Not run against Binance.** Run `--live --testnet` first |
| Telegram delivery to your phone | **Needs your bot token**; use `--test-alert` |

## What real paper trading has taught us

The first real-data paper session (8 trades, −$4.16, one win) exposed bugs that no unit test had caught; each now
has a regression test. Reviewing a trade means reading the recorded quotes around it (`data/recordings`), not just
the summary line.

| Symptom | Cause | Fix |
|---|---|---|
| ERA/SAND "signals" at −138/−75 bps basis | Halted/broken spot made mark ≠ index; rolling mean was itself broken | mark/index gate, entries need perp ≥ spot |
| ZEC: signal +32 bps, filled at −11 bps, lost 33.6 bps | Order-book fetch timed out (4 s); the bot then filled on later quotes but recorded the old signal | re-check on fresh quotes, 1.5 s timeout, real entry basis |
| "held 5 s" after 1 s | open time stamped at tick start, not at fill | stamped at fill |
| AVA: +69 bps predicted, −111 realised | ~$50-110 touch liquidity; spot dumping, perp lagging | $300 minimum touch depth, larger risk buffer |

The saved trade history persists across restarts on purpose (so a restart can't reset the daily loss limit). To start
a fresh paper book, stop the bot and move `data/paper-mainnet-real.db` aside (don't delete it); the next start creates
an empty one.

## Going live (read before using real money)

**`live.py` has not been run against the exchange.** The unit tests cover quantity alignment and the engine
logic, but not Binance's responses. Do these in order:

0. On startup the bot **reconciles** saved positions with Binance (short perp present, spot inventory present,
   no unknown perp positions in tracked markets) and trips a fault if anything disagrees. Use a dedicated
   account or sub-account so unrelated positions are not mistaken for orphans.
1. Create **testnet** keys, put them in `.env` (see `.env.example`), run `run.py --live --testnet`.
2. Fund both wallets: the **spot** wallet needs USDT for the long leg and the **USDⓈ-M futures** wallet
   needs USDT for margin (isolated, `leverage = 2`). The futures account must be in **One-way mode**.
3. Watch several full entry/exit cycles; compare fills and fees with the dashboard.
4. For mainnet use a dedicated API key with **withdrawals disabled** and IP restriction, set
   `ARBBOT_LIVE_ACK=I_UNDERSTAND_THIS_TRADES_REAL_MONEY`, and start with `trade_notional_usd` at the
   exchange minimum.

**Live execution details.** Partial fills are accepted, not treated as errors: after an entry the legs are compared
in delta terms (`spot_qty` vs `perp_qty × multiplier`) and any excess beyond 1% is unwound; exits top up partial
fills (up to 3 orders per leg) and fault if a non-dust remainder is left. Contract multipliers (`1000SHIB` etc.)
are handled on both price and quantity. The account's real taker fees are read at startup. None of this has run
against the exchange yet (see the verification table), so testnet first.

**Not possible yet: backtested slippage.** That needs historical order-book data the bot doesn't have. Instead the
bot measures its own model error live (predicted vs realised P&L) and widens the risk buffer accordingly, and
paper trades fill at the real book VWAP. A recorder plus replay tool would be the next step for true backtests.

Known limits: the WebSocket feed is tested against a local mock of Binance's combined-stream format, not
Binance itself; futures fees and funding are estimated (not queried); spot fees in BNB are estimated; orders
are still REST (no WebSocket order API).

*Not financial advice. Trading carries risk of loss.*
