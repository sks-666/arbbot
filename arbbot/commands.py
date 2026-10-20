"""Telegram command interface: ask the bot what the market looks like, pause/resume entries, and open or close
custom PAPER trades.

CommandHandler turns a message into a reply from live engine state (no network of its own: easy to test).
TelegramListener long-polls getUpdates, answers ONLY the configured chat, and ignores everyone else.
Controls: /pause and /resume, plus /open (needs /confirm within a minute), /cancel and /close for paper trades.
The engine refuses all of the trade commands when it is running live: no real order is ever placed from a chat.
"""
import asyncio
import logging
import re
import time

import aiohttp

from .alerts import Notifier
from .config import AlertsCfg
from .engine import Engine
from .models import Signal
from .portfolio import unrealized_usd
from .scanner import round_trip_cost_bps

log = logging.getLogger("commands")
PENDING_TTL_S = 60.0  # a /open request must be confirmed within this long

MENU = [
    ("signals", "Any long/short trades in the market right now?"),
    ("status", "Bot health, feed and P&L summary"),
    ("positions", "Open positions; details: /positions BTC"),
    ("pnl", "Profit and loss, recent trades"),
    ("top", "Top markets by edge: /top 10"),
    ("market", "One market: /market BTC"),
    ("pause", "Pause new entries"),
    ("resume", "Resume entries"),
    ("open", "Custom PAPER trade: /open BTC 200, then /confirm"),
    ("confirm", "Confirm the pending /open"),
    ("cancel", "Cancel the pending /open"),
    ("close", "Close a paper position: /close BTC"),
    ("help", "What I can answer"),
]

HELP = (
    "🤖 arbbot: ask me anything about the market, in plain words or with commands.\n\n"
    "Questions\n"
    "• \"any trades long or short?\" or /signals\n"
    "• \"how is BTC?\" or /market BTC\n"
    "• /status · /positions · /pnl · /top 10\n"
    "• /positions BTC for the full detail of one open trade\n\n"
    "Controls\n"
    "• /pause stops new entries · /resume allows them again\n\n"
    "Custom PAPER trades (never real orders)\n"
    "• /open BTC 200 shows a preview, /confirm within 60 s opens it, /cancel drops it\n"
    "• /close BTC closes it; until then only the protective exits apply\n\n"
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
        self.pending: tuple[str, float, float] | None = None  # (symbol, usd, expires_at) from /open, awaiting /confirm
        self._now = time.monotonic                            # replaceable in tests

    # ---- routing -------------------------------------------------------------------------------
    async def reply(self, text: str) -> str:
        """Like answer(), plus the trade commands, which need the engine (async)."""
        t = (text or "").strip()
        cmd, _, arg = t.partition(" ")
        cmd = cmd.lower().split("@")[0]
        if cmd == "/open":
            return self._open_request(arg.strip())
        if cmd == "/confirm":
            return await self._confirm()
        if cmd == "/cancel":
            return self._cancel()
        if cmd == "/close":
            return await self._close_request(arg.strip())
        return self.answer(t)

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
        if cmd in ("/positions", "/pos", "/position"):
            return self._positions(arg)
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
        if low.startswith(("open ", "buy ", "enter ")):
            return ("To open a custom PAPER trade use a command: /open BTC 200, then /confirm. "
                    "I never place real orders from a chat message.")
        if has("position", "holding", "open trade", "what do you hold"):
            sym = self._find_symbol(words)
            return self._position_detail(sym) if sym and sym in self.e.pf.open else self._positions()
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

    def _positions(self, arg: str = "") -> str:
        if arg.strip():
            return self._position_detail(arg.strip().split()[0])
        s = self.e.snapshot()
        if not s["positions"]:
            return f"No open positions {self._tag()}"
        lines = [f"📂 {len(s['positions'])} open position(s) {self._tag()}"]
        for p in s["positions"]:
            tag = " · manual" if p.get("manual") else ""
            bps = p["unreal"] / p["notional"] * 1e4 if p["notional"] else 0.0
            lines.append(f"• {p['sym']}{tag} ${p['notional']:.0f} · LONG spot / SHORT perp · held {_dur(p['held_s'])}\n"
                         f"  entry basis {_bps(p['entry_basis'])} → now {_bps(p['basis_now'])} · "
                         f"unrealised {_money(p['unreal'])} ({_bps(bps)})")
        lines.append("Details of one: /positions BTC")
        return "\n".join(lines)

    def _position_detail(self, token: str) -> str:
        """Everything about one open position: what was bought, what it has cost, where it stands, how it will exit."""
        e = self.e
        sym = self._resolve(token)
        if sym is None:
            return f"I don't know the market \"{token}\". Try /positions."
        pos = e.pf.open.get(sym)
        if pos is None:
            return f"{sym} is not open. Use /positions to see what is."
        m, sig = e.board.markets.get(sym), e.sigs.get(sym)
        st, fees = e.cfg.strategy, e.cfg.fees
        held = max(0.0, e.clock() - pos.opened_at)
        base = sym.removesuffix("USDT")
        lines = [
            f"📂 {sym} · {'MANUAL' if pos.manual else 'AUTO'} · LONG spot / SHORT perp {self._tag()}",
            f"Size ${pos.notional:.0f}: spot {pos.spot_qty:.6g} @ {_px(pos.spot_entry)} · "
            f"perp {pos.perp_qty:.6g} @ {_px(pos.perp_entry)}",
            f"Opened {_dur(held)} ago · entry basis {_bps(pos.entry_basis_bps)} · fees paid ${pos.fees_paid:.2f} · "
            f"funding {_money(pos.funding_acc)}",
        ]
        if not pos.manual:
            lines.append(f"Model at entry: z {pos.entry_z:.1f}, expected net {_bps(pos.predicted_net_bps)}")
        if m is None or sig is None or m.spot is None or m.perp is None or sig.status == "stale":
            lines.append("No fresh quote right now, so the live figures are unavailable.")
            return "\n".join(lines)
        unreal = unrealized_usd(pos, m, fees)
        pnl_bps = unreal / pos.notional * 1e4 if pos.notional else 0.0
        lines.append(f"Now: unwind basis {_bps(sig.basis_out_bps)} (mid {_bps(sig.basis_mid_bps)}) · "
                     f"P&L if closed now {_money(unreal)} ({_bps(pnl_bps)})")
        exits = []
        if pos.manual:
            exits.append(f"held until you send /close {base}")
        else:
            if pos.sigma_entry > 0:
                z_now = (sig.basis_mid_bps - pos.mu_entry) / pos.sigma_entry
                exits.append(f"converged when z ≤ {st.exit_z:g} (now {z_now:.1f})")
            exits.append(f"timeout in {_dur(max(0.0, st.max_hold_s - held))}")
        exits.append(f"P&L stop at -{round_trip_cost_bps(fees) + st.stop_loss_bps:.0f} bps (now {_bps(pnl_bps)})")
        if pos.sigma_entry > 0 or pos.manual:
            exits.append(f"basis stop at +{st.stop_loss_bps:g} bps vs entry "
                         f"(now {_bps(sig.basis_mid_bps - pos.entry_mid_bps)})")
        lines.append("Exits: " + " · ".join(exits))
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

    # ---- custom paper trades -------------------------------------------------------------------
    def _resolve(self, token: str) -> str | None:
        return self._find_symbol(set(re.findall(r"[a-z0-9&]+", token.lower())))

    def _open_request(self, arg: str) -> str:
        """Step 1: validate and show a preview. Nothing is opened until /confirm."""
        e = self.e
        tokens = arg.replace("$", " ").split()
        default = e.cfg.risk.trade_notional_usd
        if not tokens:
            return (f"Which market? Example: /open BTC 200 (size in USD, default ${default:.0f}). "
                    f"I'll show a preview and you send /confirm.")
        sym = self._resolve(tokens[0])
        if sym is None:
            return f"I don't know the market \"{tokens[0]}\". Try /top to see what I scan."
        usd = default
        if len(tokens) > 1:
            try:
                usd = float(tokens[1])
            except ValueError:
                return f"\"{tokens[1]}\" is not a number. Example: /open {tokens[0]} 200"
        why = e.manual_block_reason(sym, usd)
        if why:
            self.pending = None
            return f"Can't open {sym}: {why}."
        self.pending = (sym, usd, self._now() + PENDING_TTL_S)
        sig = e.sigs[sym]
        cost = round_trip_cost_bps(e.cfg.fees)
        return (
            f"📝 Custom PAPER trade, not opened yet {self._tag()}\n"
            f"{sym} · LONG spot / SHORT perp · ${usd:.0f}\n"
            f"Executable basis now {_bps(sig.basis_in_bps)} (the model's own signal bar is z ≥ {e.cfg.strategy.z_entry:g}; "
            f"this market is at z {sig.z:.1f}).\n"
            f"Round-trip cost is about {cost:.0f} bps (${usd * cost / 1e4:.2f}), so unless the basis narrows by more "
            f"than that, it loses money.\n"
            f"Send /confirm within {PENDING_TTL_S:.0f}s to open it, or /cancel."
        )

    async def _confirm(self) -> str:
        pending, self.pending = self.pending, None
        if pending is None:
            return "Nothing to confirm. Start with /open BTC 200."
        sym, usd, expires = pending
        if self._now() > expires:
            return f"That request expired ({PENDING_TTL_S:.0f}s). Send /open {sym.removesuffix('USDT')} again."
        _ok, msg = await self.e.open_manual(sym, usd)
        return msg

    def _cancel(self) -> str:
        if self.pending is None:
            return "Nothing was pending."
        sym = self.pending[0]
        self.pending = None
        return f"Cancelled. {sym} was not opened."

    async def _close_request(self, arg: str) -> str:
        e = self.e
        if not arg:
            held = ", ".join(sym.removesuffix("USDT") for sym in e.pf.open) or "nothing is open"
            return f"Which one? Example: /close BTC. Open now: {held}."
        sym = self._resolve(arg.split()[0])
        if sym is None:
            return f"I don't know the market \"{arg.split()[0]}\"."
        _ok, msg = await e.close_manual(sym)
        return msg


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
            reply = await self.handler.reply(text)
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
