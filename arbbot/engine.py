"""One scan cycle: evaluate every market -> manage exits -> take new entries -> expose a snapshot."""
import asyncio
import logging
import math
import time
from collections import deque

from .alerts import Notifier
from .config import AlertsCfg, Config
from .executor import UnhedgedError
from .feeds import Board
from .models import Position, Signal
from .portfolio import Portfolio, unrealized_usd
from .risk import Risk
from .scanner import Scanner, percentile, round_trip_cost_bps
from .sizing import plan_size

log = logging.getLogger("engine")


class Engine:
    def __init__(self, cfg: Config, board: Board, scanner: Scanner, portfolio: Portfolio, risk: Risk, executor,
                 trading: bool = True, alerts: Notifier | None = None, depth=None, recorder=None):
        self.recorder = recorder                # optional Recorder: saves quotes/books/trades for backtests
        self.depth = depth                      # RestDepth-like: `await depth.books(market)`; None = top-of-book sizing
        self._depth_block: dict[str, float] = {}
        self._skip_logged: dict[str, float] = {}
        self.alerts = alerts or Notifier(AlertsCfg(enabled=False))
        self._last_halt = ""
        self._feed_alerted = "ok"
        self._feed_pending: tuple[str, float] | None = None
        self.cfg = cfg
        self.board = board
        self.scanner = scanner
        self.pf = portfolio
        self.risk = risk
        self.executor = executor
        self.trading = trading
        self.sigs: dict[str, Signal] = {}
        self.events: deque[dict] = deque(maxlen=200)
        self.tick_count = 0
        self.tick_ms = 0.0
        self.flatten_requested = False
        self.started = time.time()
        self.clock = time.time                  # wall clock for manual trades; replaceable so tests can use simulated time
        self._update_buffer()

    def _update_buffer(self) -> None:
        """RiskBuffer = max(floor, p95 of (predicted - realised) over recent trades), capped.

        The buffer learns from real outcomes: if the model keeps over-promising, the edge it demands
        grows (and entries dry up) until realised results line up with predictions again.
        """
        st = self.cfg.strategy
        buf = st.risk_buffer_bps
        if st.adaptive_buffer:
            errors = [t.predicted_net_bps - t.pnl_bps for t in list(self.pf.closed)[-100:] if t.predicted_net_bps]
            if len(errors) >= st.adaptive_min_trades:
                buf = max(buf, percentile(errors, st.adaptive_percentile))
        self.scanner.risk_buffer_bps = min(buf, max(st.risk_buffer_max_bps, st.risk_buffer_bps))

    # ---- logging -------------------------------------------------------------------------------
    def event(self, level: str, msg: str, now: float | None = None) -> None:
        getattr(log, "error" if level == "error" else "info")(msg)
        self.events.append({"ts": now or time.time(), "lvl": level, "msg": msg})

    # ---- control surface (dashboard) -----------------------------------------------------------
    def request_halt(self) -> None:
        self.risk.manual_halt = True
        self.event("warn", "entries paused from dashboard")

    def request_resume(self) -> None:
        self.risk.manual_halt = False
        self.event("info", "entries resumed from dashboard")

    def request_flatten(self) -> None:
        self.flatten_requested = True
        self.risk.manual_halt = True
        self.event("warn", "flatten-all requested from dashboard (entries paused)")

    # ---- main loop -----------------------------------------------------------------------------
    async def run(self) -> None:
        interval = self.cfg.feed.interval_s
        next_t = time.monotonic()
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("tick failed")
                self.event("error", f"tick failed: {type(e).__name__}: {e}")
            next_t = max(next_t + interval, time.monotonic())
            await asyncio.sleep(max(0.0, next_t - time.monotonic()))

    async def tick(self, now: float | None = None) -> None:
        t0 = time.monotonic()
        now = time.time() if now is None else now
        self.tick_count += 1
        self.risk.refresh(now)
        self.sigs = {sym: self.scanner.evaluate(m, now) for sym, m in self.board.markets.items()}
        self._watch_state(now)
        if self.recorder is not None:
            self.recorder.tick(now, self.board.markets)
        self._accrue_funding(now)
        await self._exits(now)
        if self.trading and not self.risk.halted:
            await self._entries(now)
        self.tick_ms = (time.monotonic() - t0) * 1000

    # ---- alerts --------------------------------------------------------------------------------
    def _watch_state(self, now: float) -> None:
        """Turn state changes (signals, halts/faults, feed health) into alerts. Cheap no-op when alerts are off."""
        if not self.alerts.enabled:
            return
        risk = self.risk
        will_trade = self.trading and not risk.halted
        for sig in self.sigs.values():
            if sig.status == "signal":
                self.alerts.signal(sig, now, will_trade, "monitor-only" if not self.trading else risk.halt_reason)

        reason = risk.halt_reason
        if reason != self._last_halt:
            self.alerts.halted(reason, fault=bool(risk.fault)) if reason else self.alerts.resumed()
            self._last_halt = reason

        b = self.board
        if b.kind == "sim":
            return
        if not b.ok or now - b.last_update > 10:
            state = "down"
        else:
            state = "fallback" if (b.kind == "ws" and b.source == "rest") else "ok"
        if state == self._feed_alerted:
            self._feed_pending = None
        elif self._feed_pending is None or self._feed_pending[0] != state:
            self._feed_pending = (state, now)
        elif now - self._feed_pending[1] >= self.cfg.alerts.feed_debounce_s:
            self.alerts.feed(state, b.last_error if state != "ok" else "")
            self._feed_alerted, self._feed_pending = state, None

    # ---- exits ---------------------------------------------------------------------------------
    def _accrue_funding(self, now: float) -> None:
        """Credit the short perp with funding each time a funding timestamp passes (estimate)."""
        now_ms = int(now * 1000)
        for pos in self.pf.open.values():
            m = self.board.markets.get(pos.symbol)
            if m is None or not pos.next_funding_ms or now_ms < pos.next_funding_ms or m.perp is None:
                continue
            pos.funding_acc += pos.perp_qty * m.perp.mid * m.funding_rate
            step_ms = int(m.funding_interval_h * 3600 * 1000)
            pos.next_funding_ms = m.next_funding_ms if m.next_funding_ms > pos.next_funding_ms else pos.next_funding_ms + step_ms
            self.pf.update(pos)

    def _exit_reason(self, pos: Position, sig: Signal, m, now: float) -> str | None:
        st = self.cfg.strategy
        if self.flatten_requested:
            return "manual-flatten"
        if self.risk.flatten_required:
            return "risk-flatten"
        if sig.status == "stale":
            return "stale-feed" if now - pos.last_fresh >= st.stale_force_exit_s else None
        # Primary stop: what closing right now would actually realise (real leg prices, funding, all fees).
        # Right after entry that is already about -RoundTripCost, so the stop is that cost plus stop_loss_bps.
        cost = round_trip_cost_bps(self.cfg.fees)
        loss_bps = -unrealized_usd(pos, m, self.cfg.fees) / pos.notional * 1e4 if pos.notional else 0.0
        if loss_bps >= cost + st.stop_loss_bps:
            return "stop-loss"
        frozen = pos.sigma_entry > 0  # False only for positions saved by an older version
        # Secondary stop on the mid basis (not the executable one, so a momentarily wide spread can't trigger it).
        if (frozen or pos.manual) and sig.basis_mid_bps - pos.entry_mid_bps >= st.stop_loss_bps:
            return "basis-stop"
        if pos.manual:
            return None  # hand-picked trades stay open until /close; only the protective exits above apply
        held = now - pos.opened_at
        if held < st.min_hold_s:
            return None
        # Convergence is judged against the regime frozen at entry, never against a rolling baseline that
        # the dislocation itself has dragged upward.
        if frozen:
            z_trade = (sig.basis_mid_bps - pos.mu_entry) / pos.sigma_entry
        else:
            z_trade = sig.z if sig.status != "warmup" else float("inf")
        if z_trade <= st.exit_z:
            return "converged"
        if held >= st.max_hold_s:
            return "timeout"
        return None

    async def _exits(self, now: float) -> None:
        for sym, pos in list(self.pf.open.items()):
            m, sig = self.board.markets.get(sym), self.sigs.get(sym)
            if m is None or sig is None or m.spot is None or m.perp is None:
                continue
            if sig.status != "stale":
                pos.last_fresh = now
            reason = self._exit_reason(pos, sig, m, now)
            if reason:
                await self._close(pos, sig, now, reason)
        if self.flatten_requested and not self.pf.open:
            self.flatten_requested = False

    async def _close(self, pos: Position, sig: Signal, now: float, reason: str) -> None:
        m = self.board.markets[pos.symbol]
        try:
            trade = await self.executor.close(pos, m, sig, now, reason)
        except UnhedgedError as e:
            self.risk.trip_fault(str(e))
            self.event("error", f"UNHEDGED on close: {e}", now)
            return
        except Exception as e:
            self.event("error", f"close {pos.symbol} failed: {e}", now)
            if self.risk.record_error():
                self.event("error", f"fault breaker tripped: {self.risk.fault}", now)
            return
        self.risk.record_success()
        self.pf.close(pos, trade)
        self.risk.note_exit(pos.symbol, now)
        self.alerts.exit(trade)
        if self.recorder is not None:
            self.recorder.event("exit", now, sym=pos.symbol, pnl_usd=trade.pnl_usd, pnl_bps=trade.pnl_bps,
                                reason=reason, predicted_net_bps=trade.predicted_net_bps,
                                held_s=now - pos.opened_at, live=trade.live)
        self._update_buffer()
        self.event(
            "win" if trade.pnl_usd > 0 else "loss",
            f"EXIT {pos.symbol} [{reason}] pnl {trade.pnl_usd:+.3f} USD ({trade.pnl_bps:+.1f} bps), held {now - pos.opened_at:.0f}s",
            now,
        )

    # ---- entries -------------------------------------------------------------------------------
    async def _entries(self, now: float) -> None:
        cands = sorted((s for s in self.sigs.values() if s.status == "signal"), key=lambda s: -s.net_edge_bps)
        for sig in cands:
            notional, why = self.risk.size_entry(sig, now)
            if notional <= 0:
                if why == "max open positions" or self.risk.halted:
                    break
                continue
            m = self.board.markets[sig.symbol]
            books, plan = None, None
            t_start, now_fill = time.monotonic(), now
            if self.depth is not None:
                if now < self._depth_block.get(sig.symbol, 0.0):
                    continue  # this market failed the size-dependent edge check a moment ago
                books = await self.depth.books(m)  # None on error: fall back to top-of-book sizing
                now_fill = now + (time.monotonic() - t_start)
                # The market keeps moving while we wait on the network. Decide on FRESH quotes, never on the ones
                # that triggered the signal (a 4 s depth timeout once "entered" a dislocation that had already closed).
                sig = self.scanner.evaluate(m, now_fill)
                notional, why = self.risk.size_entry(sig, now_fill) if sig.status == "signal" else (0.0, "signal gone")
                if notional <= 0:
                    self._depth_block[sig.symbol] = now_fill + 5.0
                    if now - self._skip_logged.get(sig.symbol, 0.0) > 60:
                        self._skip_logged[sig.symbol] = now
                        self.event("info", f"SKIP {sig.symbol}: {why} after {now_fill - now:.1f}s waiting for the "
                                           f"order book (the market moved)", now)
                    continue
                if books is not None and self.recorder is not None:
                    self.recorder.depth(sig.symbol, books, now_fill, "signal")
                if books is not None:
                    plan = plan_size(m, sig, notional, books, self.cfg.strategy, self.cfg.risk.min_trade_usd,
                                     self.cfg.risk.size_steps)
                    if plan is None:
                        self._depth_block[sig.symbol] = now + 5.0
                        if now - self._skip_logged.get(sig.symbol, 0.0) > 60:
                            self._skip_logged[sig.symbol] = now
                            self.event("info", f"SKIP {sig.symbol}: edge disappears at executable size "
                                               f"(top-of-book net {sig.net_edge_bps:+.1f} bps, thin book)", now)
                        continue
                    notional = plan.notional
            try:
                pos = await self.executor.open(m, sig, notional, now_fill, books=books)
            except UnhedgedError as e:
                self.risk.trip_fault(str(e))
                self.event("error", f"UNHEDGED on entry: {e}", now)
                return
            except Exception as e:
                self.event("error", f"entry {sig.symbol} failed: {e}", now)
                if self.risk.record_error():
                    self.event("error", f"fault breaker tripped: {self.risk.fault}", now)
                    return
                continue
            self.risk.record_success()
            pos.opened_at = now + (time.monotonic() - t_start)  # when it really filled, so min_hold counts real seconds
            pos.predicted_net_bps = plan.net_edge_bps if plan else sig.net_edge_bps
            self.pf.add(pos)
            if self.recorder is not None:
                self.recorder.event("entry", now, sym=sig.symbol, notional=pos.notional, z=sig.z,
                                    predicted_net_bps=pos.predicted_net_bps, entry_basis_bps=pos.entry_basis_bps,
                                    perp_px=pos.perp_entry, spot_px=pos.spot_entry, live=pos.live)
            self.alerts.entry(pos, sig)
            self.event(
                "entry",
                f"ENTER {sig.symbol} {pos.notional:.0f} USD | basis {pos.entry_basis_bps:+.1f} bps, z {sig.z:.1f}, "
                f"net edge {pos.predicted_net_bps:+.1f} bps (buffer {sig.risk_buffer_bps:.1f})",
                now,
            )

    # ---- custom (manual) paper trades, driven from Telegram ------------------------------------
    def manual_block_reason(self, symbol: str, usd: float, sig: Signal | None = None) -> str:
        """Why a hand-picked trade can't be opened right now ('' = it can). No side effects.

        Paper only, and it respects the same limits as automatic entries (halts, open-position cap, total
        exposure, order-book depth); it only skips the signal itself, which is the point of choosing by hand.
        """
        c = self.cfg.risk
        if self.executor.live:
            return "custom trades are paper-only: real orders are never placed from a chat message"
        if self.risk.halted:
            return f"entries are halted ({self.risk.halt_reason})"
        m = self.board.markets.get(symbol)
        sig = sig or self.sigs.get(symbol)
        if m is None or sig is None or m.spot is None or m.perp is None:
            return "it is not being scanned"
        if sig.status == "stale":
            return "there are no fresh quotes right now"
        if symbol in self.pf.open:
            return "it is already open"
        if len(self.pf.open) >= c.max_open_positions:
            return f"already at the limit of {c.max_open_positions} open positions"
        if not math.isfinite(usd) or usd < c.min_trade_usd:
            return f"the size must be at least ${c.min_trade_usd:.0f}"
        room = c.max_total_notional_usd - self.pf.open_notional()
        if usd > room:
            return f"that would exceed the total exposure cap (${max(room, 0):.0f} left)"
        if sig.fillable_usd < c.min_touch_depth_usd:
            return (f"the order book is too thin (${sig.fillable_usd:.0f} at the best price, "
                    f"need ${c.min_touch_depth_usd:.0f})")
        cap = sig.fillable_usd * c.depth_fraction
        if usd > cap:
            return f"too big for this book: at most ${cap:.0f} (half of the ${sig.fillable_usd:.0f} at the best price)"
        return ""

    async def open_manual(self, symbol: str, usd: float) -> tuple[bool, str]:
        """Open a hand-picked LONG spot / SHORT perp paper position. Returns (ok, message for the chat)."""
        now = self.clock()
        why = self.manual_block_reason(symbol, usd)
        if why:
            return False, f"Can't open {symbol}: {why}."
        m = self.board.markets[symbol]
        t_start, books = time.monotonic(), None
        if self.depth is not None:
            books = await self.depth.books(m)  # None on error: fall back to top-of-book fills
        now_fill = now + (time.monotonic() - t_start)
        sig = self.scanner.evaluate(m, now_fill)  # fresh quotes, never the ones from before the wait
        why = self.manual_block_reason(symbol, usd, sig)  # and re-check: the market and our own book may have moved
        if why:
            return False, f"Can't open {symbol}: {why} (after {now_fill - now:.1f}s waiting for the order book)."
        try:
            pos = await self.executor.open(m, sig, usd, now_fill, books=books)
        except UnhedgedError as e:
            self.risk.trip_fault(str(e))
            self.event("error", f"UNHEDGED on manual entry: {e}", now)
            return False, f"⚠️ Entry for {symbol} failed and left a leg unhedged; entries are halted. Check /status."
        except Exception as e:
            self.event("error", f"manual entry {symbol} failed: {e}", now)
            if self.risk.record_error():
                self.event("error", f"fault breaker tripped: {self.risk.fault}", now)
            return False, f"The entry for {symbol} failed ({type(e).__name__}). Nothing was opened."
        self.risk.record_success()
        pos.manual = True
        pos.opened_at = now + (time.monotonic() - t_start)
        pos.predicted_net_bps = 0.0  # no model forecast for a hand-picked trade: keeps the adaptive buffer clean
        self.pf.add(pos)
        if self.recorder is not None:
            self.recorder.event("entry", now, sym=symbol, notional=pos.notional, z=sig.z, predicted_net_bps=0.0,
                                entry_basis_bps=pos.entry_basis_bps, perp_px=pos.perp_entry, spot_px=pos.spot_entry,
                                live=False, manual=True)
        self.event("entry", f"MANUAL ENTER {symbol} {pos.notional:.0f} USD | basis {pos.entry_basis_bps:+.1f} bps", now)
        return True, (
            f"✅ Opened a paper trade: {symbol} ${pos.notional:.0f}\n"
            f"• LONG spot @ {pos.spot_entry:.6g} / SHORT perp @ {pos.perp_entry:.6g}\n"
            f"• entry basis {pos.entry_basis_bps:+.1f} bps · fees paid ${pos.fees_paid:.2f}\n"
            f"It stays open until you send /close {symbol.removesuffix('USDT')}. "
            f"Protective exits still apply (stop-loss, stale feed, daily loss limit, kill switch)."
        )

    async def close_manual(self, symbol: str) -> tuple[bool, str]:
        """Close an open paper position now. Returns (ok, message for the chat)."""
        if self.executor.live:
            return False, "Closing from chat is paper-only: real orders are never placed from a chat message."
        pos = self.pf.open.get(symbol)
        if pos is None:
            return False, f"{symbol} is not open."
        m, sig = self.board.markets.get(symbol), self.sigs.get(symbol)
        if m is None or sig is None or m.spot is None or m.perp is None or sig.status == "stale":
            return False, f"There are no fresh quotes for {symbol} right now. Try again in a moment."
        await self._close(pos, sig, self.clock(), "manual-close")
        if symbol in self.pf.open:
            return False, f"The close for {symbol} failed. Check /status."
        t = self.pf.closed[-1]
        return True, (f"✅ Closed {symbol}: pnl {t.pnl_usd:+.2f} USD ({t.pnl_bps:+.1f} bps), "
                      f"held {max(0.0, t.closed_at - t.opened_at):.0f}s, fees ${t.fees_usd:.2f}.")

    # ---- dashboard snapshot --------------------------------------------------------------------
    def snapshot(self) -> dict:
        now = time.time()
        pf, risk, board, cfg = self.pf, self.risk, self.board, self.cfg
        rows, unreal = [], 0.0
        positions = []
        for sym, sig in self.sigs.items():
            m = board.markets[sym]
            held = sym in pf.open
            rows.append({
                "sym": sym, "perp": sig.perp_mid, "basis": sig.basis_mid_bps, "entry": sig.basis_in_bps,
                "z": sig.z, "net": sig.net_edge_bps, "mi": sig.mark_index_bps, "fund": sig.funding_apr_pct,
                "depth": sig.fillable_usd, "age": sig.age_ms, "n": sig.samples,
                "status": "held" if held else sig.status, "note": sig.note, "mult": m.mult,
            })
        for sym, pos in pf.open.items():
            m, sig = board.markets.get(sym), self.sigs.get(sym)
            u = unrealized_usd(pos, m, cfg.fees) if m else 0.0
            unreal += u
            positions.append({
                "sym": sym, "held_s": now - pos.opened_at, "notional": pos.notional, "entry_basis": pos.entry_basis_bps,
                "basis_now": sig.basis_out_bps if sig else 0.0, "z": sig.z if sig else 0.0, "unreal": u,
                "funding": pos.funding_acc, "live": pos.live, "manual": pos.manual,
            })
        trades = [
            {"sym": t.symbol, "closed_at": t.closed_at, "pnl": t.pnl_usd, "bps": t.pnl_bps, "reason": t.reason,
             "held_s": t.closed_at - t.opened_at, "fees": t.fees_usd}
            for t in reversed(list(pf.closed)[-25:])
        ]
        return {
            "ts": now,
            "mode": "live" if self.executor.live else "paper",
            "env": cfg.execution.env,
            "feed": {
                "kind": board.source, "configured": board.kind, "ok": board.ok, "latency_ms": board.latency_ms, "polls": board.polls,
                "errors": board.errors, "last_error": board.last_error,
                "age_s": (now - board.last_update) if board.last_update else None,
            },
            "halt": {
                "halted": risk.halted, "reason": risk.halt_reason, "manual": risk.manual_halt,
                "trading": self.trading, "flattening": self.flatten_requested,
            },
            "stats": {
                "scanned": len(rows), "fresh": sum(1 for r in rows if r["status"] != "stale"),
                "signals": sum(1 for r in rows if r["status"] == "signal"), "open": len(pf.open),
                "max_open": cfg.risk.max_open_positions, "open_notional": pf.open_notional(),
                "realized": pf.realized_usd, "day": pf.day_pnl(now), "day_limit": cfg.risk.daily_loss_limit_usd,
                "unrealized": unreal, "wins": pf.wins, "losses": pf.losses, "win_rate": pf.win_rate,
                "tick_ms": self.tick_ms, "ticks": self.tick_count, "uptime_s": now - self.started,
            },
            "model": {
                "cost_bps": round_trip_cost_bps(cfg.fees), "z_entry": cfg.strategy.z_entry,
                "exit_z": cfg.strategy.exit_z, "min_net_edge_bps": cfg.strategy.min_net_edge_bps,
                "min_samples": cfg.strategy.min_samples, "trade_usd": cfg.risk.trade_notional_usd,
                "risk_buffer_bps": self.scanner.risk_buffer_bps, "baseline": cfg.strategy.baseline,
            },
            "markets": rows,
            "positions": positions,
            "trades": trades,
            "events": list(self.events)[-60:][::-1],
        }
