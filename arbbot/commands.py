"""Telegram command interface: ask the bot what the market looks like, and pause/resume entries.

CommandHandler turns a message into a reply from live engine state (pure, no network: easy to test).
TelegramListener long-polls getUpdates, answers ONLY the configured chat, and ignores everyone else.
Controls are limited to /pause and /resume. Nothing here can place an order or close a position.
"""
import asyncio
import logging
import re

import aiohttp

from .alerts import Notifier
from .config import AlertsCfg
from .engine import Engine
from .models import Signal

log = logging.getLogger("commands")

MENU = [
    ("signals", "Any long/short trades in the market right now?"),
    ("status", "Bot health, feed and P&L summary"),
    ("positions", "Open positions"),
    ("pnl", "Profit and loss, recent trades"),
    ("top", "Top markets by edge: /top 10"),
    ("market", "One market: /market BTC"),
    ("pause", "Pause new entries"),
    ("resume", "Resume entries"),
    ("help", "What I can answer"),
]

HELP = (
    "🤖 arbbot: ask me anything about the market, in plain words or with commands.\n\n"
    "Questions\n"
    "• \"any trades long or short?\" or /signals\n"
    "• \"how is BTC?\" or /market BTC\n"
    "• /status · /positions · /pnl · /top 10\n\n"
    "Controls\n"
    "• /pause stops new entries · /resume allows them again\n\n"
    "I only trade LONG spot / SHORT perp, when the perp is rich versus spot. "
    "Everything else I show is information only."
)

_STOP = {"a", "i", "is", "it", "me", "my", "of", "on", "or", "to", "do", "in", "at", "an", "and", "the", "any", "all"}


def _money(v: float) -> str:
    return f"{'+' if v >= 0 else '-'}${abs(v):.2f}"


def _bps(v: float) -> str:
    return f"{v:+.1f} bps"


def _px(p: float) -> str:
    return f"{p:,.2f}" if p >= 1000 else f"{p:.4f}" if p >= 1 else f"{p:.6g}"


def _dur(s: float) -> str:
    s = max(0, round(s))
    return f"{s}s" if s < 90 else f"{round(s / 60)}m" if s < 5400 else f"{s / 3600:.1f}h"


def _usd(v: float) -> str:
    return f"${v / 1000:.1f}k" if v >= 1000 else f"${v:.0f}"


class CommandHandler:
    def __init__(self, engine: Engine, label: str = ""):
        self.e = engine
        self.label = label

    # ---- routing -------------------------------------------------------------------------------
    def answer(self, text: str) -> str:
        t = (text or "").strip()
        if not t:
            return ""
        if t.startswith("/"):
            cmd, _, arg = t.partition(" ")
            return self._slash(cmd.lower().split("@")[0], arg.strip())
        return self._natural(t.lower())

    def _slash(self, cmd: str, arg: str) -> str:
        if cmd in ("/start", "/help"):
            return HELP
        if cmd in ("/signals", "/signal", "/trades", "/opportunities", "/scan", "/long", "/short"):
            return self._signals()
        if cmd == "/status":
            return self._status()
        if cmd in ("/positions", "/pos"):
            return self._positions()
        if cmd in ("/pnl", "/profit"):
            return self._pnl()
        if cmd in ("/top", "/markets"):
            return self._top(int(arg) if arg.isdigit() else 10)
        if cmd in ("/market", "/m", "/coin"):
            return self._market(arg) if arg else "Which market? Example: /market BTC"
        if cmd == "/pause":
            return self._pause()
        if cmd == "/resume":
            return self._resume()
        return f"Unknown command {cmd}.\n\n{HELP}"

    def _natural(self, low: str) -> str:
        words = set(re.findall(r"[a-z0-9&]+", low))
        has = lambda *keys: any(k in low for k in keys)  # noqa: E731
        if has("help", "what can you", "commands"):
            return HELP
        if has("position", "holding", "open trade", "what do you hold"):
            return self._positions()
        if has("pnl", "p&l", "profit", "loss", "performance", "how much"):
            return self._pnl()
        if has("status", "health", "alive", "uptime", "running", "feed"):
            return self._status()
        sym = self._find_symbol(words)
        if sym:
            return self._market(sym)
        if has("pause", "stop trading", "halt"):
            return "To change anything, use a command: /pause or /resume."
        if has("signal", "trade", "long", "short", "opportunit", "arb", "mispric", "dislocat", "setup", "scan",
               "market", "anything", "any ", "entry", "enter", "buy", "sell"):
            return self._signals()
        return "I didn't catch that. Try \"any trades long or short?\", \"how is BTC?\", /status or /help."

    def _find_symbol(self, words: set[str]) -> str | None:
        index: dict[str, str] = {}
        for sym, m in self.e.board.markets.items():
            index[sym.lower()] = sym
            index[sym.lower().removesuffix("usdt")] = sym
            index[m.base_asset.lower()] = sym
        for w in words:
            if w not in _STOP and len(w) >= 2 and w in index:
                return index[w]
        return None

    # ---- shared helpers ------------------------------------------------------------------------
    def _tag(self) -> str:
        return f"[{self.label}]" if self.label else ""

    def _fresh(self) -> list[Signal]:
        return [s for s in self.e.sigs.values() if s.status in ("ok", "watch", "signal")]

    def _why_not(self, s: Signal) -> str:
        st = self.e.cfg.strategy
        reasons = []
        if s.z < st.z_entry:
            reasons.append(f"z {s.z:.1f} < {st.z_entry:g}")
        if s.net_edge_bps < st.min_net_edge_bps:
            reasons.append(f"net {s.net_edge_bps:+.1f} < {st.min_net_edge_bps:g} bps")
        if s.basis_in_bps < st.min_entry_basis_bps:
            reasons.append("perp below spot")
        return ", ".join(reasons) or "all conditions met"

    def _trading_note(self) -> str:
        risk = self.e.risk
        if not self.e.trading:
            return "Monitor-only mode: I am not trading."
        if risk.halted:
            return f"Entries are halted ({risk.halt_reason}), so I will NOT trade these."
        return "Entries are active: I enter these automatically."

    # ---- answers -------------------------------------------------------------------------------
    def _signals(self) -> str:
        sigs = list(self.e.sigs.values())
        fresh = self._fresh()
        warm = sum(1 for s in sigs if s.status == "warmup")
        stale = sum(1 for s in sigs if s.status == "stale")
        live = sorted((s for s in fresh if s.status == "signal"), key=lambda s: -s.net_edge_bps)
        out: list[str] = []
        if live:
            out.append(f"🟢 {len(live)} trade signal(s) now: LONG spot / SHORT perp (perp is rich vs spot) {self._tag()}")
            for i, s in enumerate(live[:5], 1):
                out.append(f"{i}. {s.symbol}  basis {_bps(s.basis_mid_bps)} · z {s.z:.1f} · net edge {_bps(s.net_edge_bps)}"
                           f" · depth {_usd(s.fillable_usd)}")
            out.append(self._trading_note())
        else:
            out.append(f"No actionable trades right now {self._tag()}")
            near = sorted((s for s in fresh if s.basis_in_bps > 0), key=lambda s: -s.net_edge_bps)[:3]
            if near:
                out.append("\nClosest to a LONG spot / SHORT perp entry:")
                for s in near:
                    out.append(f"• {s.symbol}  basis {_bps(s.basis_mid_bps)} · z {s.z:.1f} · net {_bps(s.net_edge_bps)}"
                               f"  (needs: {self._why_not(s)})")
        rev = sorted((s for s in fresh if s.basis_mid_bps <= -10), key=lambda s: s.basis_mid_bps)[:3]
        if rev:
            out.append("\nPerp cheaper than spot (SHORT spot / LONG perp side): information only, I don't trade this:")
            for s in rev:
                out.append(f"• {s.symbol}  basis {_bps(s.basis_mid_bps)} · z {s.z:.1f}")
        out.append(f"\nScanned {len(sigs)} markets: {len(fresh)} live, {warm} warming up, {stale} unavailable.")
        return "\n".join(out)

    def _status(self) -> str:
        s = self.e.snapshot()
        f, st, h = s["feed"], s["stats"], s["halt"]
        feed = {"ws": "WebSocket", "rest": "REST" + (" (fallback)" if f["configured"] == "ws" else ""), "sim": "simulated"}[f["kind"]]
        entries = "monitor-only" if not h["trading"] else f"HALTED: {h['reason']}" if h["halted"] else "active"
        return (
            f"📊 arbbot {self._tag()}\n"
            f"Feed: {feed}{' · lag %.0f ms' % f['latency_ms'] if f['kind'] == 'ws' else ''} · "
            f"{'OK' if f['ok'] else 'DOWN'} · {st['fresh']}/{st['scanned']} markets fresh\n"
            f"Entries: {entries}\n"
            f"Signals now: {st['signals']} · open positions: {st['open']}/{st['max_open']} (${st['open_notional']:.0f})\n"
            f"P&L today {_money(st['day'])} · all-time {_money(st['realized'])} · unrealised {_money(st['unrealized'])}\n"
            f"Trades: {st['wins']}W / {st['losses']}L · uptime {_dur(st['uptime_s'])}"
        )

    def _positions(self) -> str:
        s = self.e.snapshot()
        if not s["positions"]:
            return f"No open positions {self._tag()}"
        lines = [f"📂 {len(s['positions'])} open position(s) {self._tag()}"]
        for p in s["positions"]:
            lines.append(f"• {p['sym']} ${p['notional']:.0f} · LONG spot / SHORT perp · held {_dur(p['held_s'])}\n"
                         f"  entry basis {_bps(p['entry_basis'])} → now {_bps(p['basis_now'])} · unrealised {_money(p['unreal'])}")
        return "\n".join(lines)

    def _pnl(self) -> str:
        s = self.e.snapshot()
        st = s["stats"]
        n = st["wins"] + st["losses"]
        lines = [f"💰 P&L {self._tag()}",
                 f"Today (UTC) {_money(st['day'])} (limit -${st['day_limit']:.0f}) · all-time {_money(st['realized'])}",
                 f"Unrealised {_money(st['unrealized'])} · {st['wins']}W / {st['losses']}L"
                 + (f" ({st['win_rate'] * 100:.0f}% win)" if n else "")]
        if s["trades"]:
            lines.append("\nLast trades:")
            for t in s["trades"][:5]:
                lines.append(f"{'✅' if t['pnl'] > 0 else '❌'} {t['sym']} {_money(t['pnl'])} ({_bps(t['bps'])}) · "
                             f"{_dur(t['held_s'])} · {t['reason']}")
        else:
            lines.append("No closed trades yet.")
        return "\n".join(lines)

    def _top(self, n: int) -> str:
        n = max(1, min(n, 25))
        fresh = sorted(self._fresh(), key=lambda s: -s.net_edge_bps)[:n]
        if not fresh:
            return "No live markets yet (still warming up or the feed is down)."
        lines = [f"🏁 Top {len(fresh)} by net edge {self._tag()}"]
        for s in fresh:
            lines.append(f"{s.symbol:<13} basis {s.basis_mid_bps:+6.1f} · z {s.z:+5.1f} · net {s.net_edge_bps:+6.1f} · {s.status}")
        return "\n".join(lines)

    def _market(self, query: str) -> str:
        q = query.strip().lower()
        sym = self._find_symbol({q}) or self._find_symbol(set(re.findall(r"[a-z0-9]+", q)))
        if sym is None:
            return f"I don't track \"{query}\". Try /top to see what I scan."
        s = self.e.sigs.get(sym)
        if s is None or s.status in ("stale", "warmup"):
            why = "still warming up (collecting history)" if s and s.status == "warmup" else (s.note if s and s.note else "no fresh quotes")
            return f"{sym} {self._tag()}\nNo reading right now: {why}."
        direction = ("perp is RICH vs spot → the trade would be LONG spot / SHORT perp"
                     if s.basis_mid_bps > 0 else "perp is CHEAPER than spot → reverse side (SHORT spot / LONG perp), which I don't trade")
        if s.status == "signal":
            verdict = "🟢 SIGNAL: " + ("entering" if self.e.trading and not self.e.risk.halted else "not trading (halted)")
        else:
            verdict = f"not a signal yet ({self._why_not(s)})"
        held = ""
        pos = self.e.pf.open.get(sym)
        if pos:
            held = f"\nI hold LONG spot / SHORT perp here: ${pos.notional:.0f}, entry basis {_bps(pos.entry_basis_bps)}"
        return (
            f"📈 {sym} {self._tag()}\n"
            f"Perp {_px(s.perp_mid)} · spot {_px(s.spot_mid)}\n"
            f"Basis {_bps(s.basis_mid_bps)} (entry {_bps(s.basis_in_bps)}), normal {_bps(s.mean_bps)} ± {s.std_bps:.1f} → z {s.z:+.2f}\n"
            f"Net edge after costs {_bps(s.net_edge_bps)} (round trip {s.cost_bps:.0f} bps)\n"
            f"Funding {s.funding_apr_pct:+.1f}% APR · mark−index {_bps(s.mark_index_bps)} · depth {_usd(s.fillable_usd)}\n"
            f"Right now: {direction}\n{verdict}{held}"
        )

    # ---- controls ------------------------------------------------------------------------------
    def _pause(self) -> str:
        if not self.e.trading:
            return "I'm in monitor-only mode, so I'm not trading anyway."
        if self.e.risk.manual_halt:
            return "Entries are already paused."
        self.e.request_halt()
        return "⛔ Entries paused. Open positions are still managed and will exit normally. Send /resume to continue."

    def _resume(self) -> str:
        r = self.e.risk
        if r.fault:
            return f"Can't resume: {r.halt_reason}. This needs a restart and a manual check."
        if not r.manual_halt:
            return f"Entries weren't paused by you. Current state: {r.halt_reason or 'active'}."
        self.e.request_resume()
        if r.halted:
            return f"Pause cleared, but entries stay halted: {r.halt_reason}."
        return "▶️ Entries resumed."


class TelegramListener:
    """Long-polls getUpdates and answers the configured chat only."""

    def __init__(self, cfg: AlertsCfg, notifier: Notifier, handler: CommandHandler,
                 session: aiohttp.ClientSession | None = None, poll_timeout_s: float = 25.0,
                 conflict_backoff_s: float = 30.0):
        self.cfg, self.n, self.handler = cfg, notifier, handler
        self.session = session or notifier.session
        self.poll_timeout_s = poll_timeout_s
        self.conflict_backoff_s = conflict_backoff_s
        self._warned_chats: set[str] = set()
        self.handled = 0
        self.ignored = 0

    def _url(self, method: str) -> str:
        return f"{self.cfg.api_base}/bot{self.n.token}/{method}"

    async def _call(self, method: str, *, params: dict | None = None, body: dict | None = None, timeout: float = 15.0):
        kw = {"timeout": aiohttp.ClientTimeout(total=timeout)}
        if body is not None:
            r = await self.session.post(self._url(method), json=body, **kw)
        else:
            r = await self.session.get(self._url(method), params=params, **kw)
        async with r:
            data = await r.json(content_type=None)
            return r.status, data

    async def _register_menu(self) -> None:
        try:
            await self._call("setMyCommands", body={"commands": [{"command": c, "description": d} for c, d in MENU]})
        except Exception as e:  # cosmetic: the menu is optional
            log.info("could not register the Telegram command menu: %s", self.n._redact(f"{type(e).__name__}: {e}"))

    async def _starting_offset(self) -> int:
        """Skip commands that piled up while the bot was offline (they could be hours old)."""
        backoff = 2.0
        while True:
            try:
                status, data = await self._call("getUpdates", params={"offset": -1, "timeout": 0})
                if status == 200 and data.get("ok"):
                    res = data.get("result", [])
                    return (res[-1]["update_id"] + 1) if res else 0
                if status == 409:
                    log.warning("another process is polling this Telegram bot; commands disabled until it stops")
                    await asyncio.sleep(self.conflict_backoff_s)
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("telegram command listener waiting: %s", self.n._redact(f"{type(e).__name__}: {e}"))
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)

    async def handle_update(self, upd: dict) -> None:
        msg = upd.get("message") or {}
        chat = msg.get("chat") or {}
        text = msg.get("text")
        if not text or "id" not in chat:
            return
        if str(chat["id"]) != self.n.chat_id:
            self.ignored += 1
            cid = str(chat["id"])
            if cid not in self._warned_chats:
                self._warned_chats.add(cid)
                log.warning("ignored a Telegram message from an unauthorised chat")
            return
        log.info("telegram command: %s", text[:60].replace("\n", " "))
        try:
            reply = self.handler.answer(text)
        except Exception as e:  # a bug in an answer must never take the bot down
            log.exception("command failed")
            reply = f"⚠️ I hit an error answering that ({type(e).__name__}). Try /status."
        self.handled += 1
        if reply:
            self.n.send(reply[:3900], priority=True)

    async def run(self) -> None:
        if not (self.n.enabled and self.cfg.commands):
            while True:  # keep the task alive: a finished task would be mistaken for a crash
                await asyncio.sleep(3600)
        await self._register_menu()
        offset = await self._starting_offset()
        backoff = 1.0
        while True:
            try:
                status, data = await self._call(
                    "getUpdates", params={"offset": offset, "timeout": int(self.poll_timeout_s), "allowed_updates": '["message"]'},
                    timeout=self.poll_timeout_s + 10,
                )
                if status == 409:
                    log.warning("another process is polling this Telegram bot; pausing commands for %.0fs", self.conflict_backoff_s)
                    await asyncio.sleep(self.conflict_backoff_s)
                    continue
                if status != 200 or not data.get("ok"):
                    raise RuntimeError(f"HTTP {status}")
                backoff = 1.0
                updates = data.get("result", [])
                for upd in updates:
                    offset = max(offset, upd["update_id"] + 1)
                    await self.handle_update(upd)
                if not updates and self.poll_timeout_s < 1:
                    await asyncio.sleep(self.poll_timeout_s)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("telegram command poll failed: %s", self.n._redact(f"{type(e).__name__}: {e}"))
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2)
