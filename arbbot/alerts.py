"""Telegram alerts: signals, entries/exits, halts, faults and feed health.

Outbound only (the bot never accepts commands over Telegram). Sending is queued and rate-limited, runs
in its own task, and can never raise into the trading loop. The bot token is never logged.
"""
import asyncio
import logging
import time
from collections import deque

import aiohttp

from .config import AlertsCfg
from .models import ClosedTrade, Position, Signal

log = logging.getLogger("alerts")


def _money(v: float) -> str:
    return f"{'+' if v >= 0 else '-'}${abs(v):.2f}"


def _dur(s: float) -> str:
    s = max(0, round(s))
    return f"{s}s" if s < 90 else f"{round(s / 60)}m"


class Notifier:
    def __init__(self, cfg: AlertsCfg, token: str = "", chat_id: str = "", label: str = "",
                 session: aiohttp.ClientSession | None = None):
        self.cfg = cfg
        self.token = token.strip()
        self.chat_id = str(chat_id).strip()
        self.label = label
        self.session = session
        self._q: deque[list] = deque()  # [text, priority, attempts]
        self._wake = asyncio.Event()
        self._busy = False
        self._last_signal: dict[str, float] = {}
        self.sent = 0
        self.failed = 0
        self.last_error = ""

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.enabled and self.token and self.chat_id and self.session is not None)

    # ---- plumbing ------------------------------------------------------------------------------
    def _redact(self, text: str) -> str:
        return text.replace(self.token, "***") if self.token else text

    def send(self, text: str, priority: bool = False) -> None:
        if not self.enabled:
            return
        if len(self._q) >= self.cfg.queue_size:  # drop the oldest ordinary message first
            for i, item in enumerate(self._q):
                if not item[1]:
                    del self._q[i]
                    break
            else:
                self._q.popleft()
        self._q.append([text, priority, 0])
        self._wake.set()

    async def _post(self, text: str) -> tuple[bool, str, float]:
        """Returns (ok, detail, retry_after_s). retry_after_s > 0 means worth retrying."""
        url = f"{self.cfg.api_base}/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        try:
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
                if r.status == 200 and isinstance(data, dict) and data.get("ok"):
                    return True, "ok", 0.0
                desc = data.get("description") if isinstance(data, dict) else None
                if r.status == 429:
                    params = data.get("parameters") if isinstance(data, dict) else None
                    retry = float((params or {}).get("retry_after", 5))
                    return False, f"rate limited (retry_after={retry:g}s)", max(retry, 0.01)
                if r.status >= 500:
                    return False, f"HTTP {r.status}", 2.0
                return False, self._redact(f"HTTP {r.status}: {desc}"), 0.0  # 400/401/403: retrying won't help
        except asyncio.CancelledError:
            raise
        except Exception as e:  # network trouble: retry once
            return False, self._redact(f"{type(e).__name__}: {e}"), 2.0

    async def send_now(self, text: str) -> tuple[bool, str]:
        """Direct, un-queued send for the --test-alert command. Returns (ok, detail)."""
        if not (self.token and self.chat_id and self.session is not None):
            return False, "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set"
        ok, detail, _ = await self._post(text)
        return ok, detail

    async def run(self) -> None:
        while True:
            await self._wake.wait()
            while self._q:
                item = self._q.popleft()
                self._busy = True
                try:
                    ok, detail, retry = await self._post(item[0])
                    if ok:
                        self.sent += 1
                    elif retry and item[2] < 2:
                        item[2] += 1
                        self._q.appendleft(item)
                        await asyncio.sleep(retry)
                        continue
                    else:
                        self.failed += 1
                        if detail != self.last_error:
                            log.warning("telegram alert failed: %s", detail)
                        self.last_error = detail
                finally:
                    self._busy = False
                await asyncio.sleep(self.cfg.min_interval_s)
            self._wake.clear()

    async def flush(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while (self._q or self._busy) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

    # ---- message builders ----------------------------------------------------------------------
    def _tag(self) -> str:
        return f"[{self.label}]" if self.label else ""

    def started(self, n_markets: int, open_positions: int = 0) -> None:
        extra = f"\nResuming with {open_positions} open position(s)." if open_positions else ""
        hint = "\nAsk me anything, e.g. \"any trades long or short?\", or send /help." if self.cfg.commands else ""
        self.send(f"🤖 arbbot started {self._tag()}\nScanning {n_markets} markets.{extra}{hint}")

    def stopped(self, open_symbols: list[str]) -> None:
        extra = f"\nOpen positions kept: {', '.join(open_symbols)}" if open_symbols else ""
        self.send(f"🛑 arbbot stopped {self._tag()}{extra}", priority=True)

    def signal(self, sig: Signal, now: float, will_trade: bool, reason: str = "") -> None:
        if not (self.enabled and self.cfg.notify_signals):
            return
        if now - self._last_signal.get(sig.symbol, -1e18) < self.cfg.signal_cooldown_s:
            return
        self._last_signal[sig.symbol] = now
        action = "→ entering now" if will_trade else f"→ NOT trading ({reason or 'entries halted'})"
        depth = f"${sig.fillable_usd / 1000:.1f}k" if sig.fillable_usd >= 1000 else f"${sig.fillable_usd:.0f}"
        self.send(
            f"🟢 SIGNAL · {sig.symbol} {self._tag()}\n"
            f"LONG spot / SHORT perp (perp is rich vs spot)\n"
            f"basis {sig.basis_mid_bps:+.1f} bps · z {sig.z:.1f} · net edge {sig.net_edge_bps:+.1f} bps\n"
            f"entry basis {sig.basis_in_bps:+.1f} · depth {depth} · funding {sig.funding_apr_pct:+.1f}% APR\n"
            f"{action}"
        )

    def entry(self, pos: Position, sig: Signal) -> None:
        if not (self.enabled and self.cfg.notify_trades):
            return
        self.send(
            f"📥 ENTERED · {pos.symbol} ${pos.notional:.0f} {self._tag()}\n"
            f"LONG spot @ {pos.spot_entry:.6g} · SHORT perp @ {pos.perp_entry:.6g}\n"
            f"basis {sig.basis_in_bps:+.1f} bps · z {sig.z:.1f}"
        )

    def exit(self, t: ClosedTrade) -> None:
        if not (self.enabled and self.cfg.notify_trades):
            return
        icon = "✅" if t.pnl_usd > 0 else "❌"
        self.send(
            f"{icon} EXIT · {t.symbol} {self._tag()}\n"
            f"P&L {_money(t.pnl_usd)} ({t.pnl_bps:+.1f} bps) · fees ${t.fees_usd:.2f}\n"
            f"held {_dur(t.closed_at - t.opened_at)} · reason: {t.reason}"
        )

    def halted(self, reason: str, fault: bool = False) -> None:
        if not (self.enabled and self.cfg.notify_halts):
            return
        if fault:
            self.send(f"🚨 FAULT {self._tag()}\n{reason}\nEntries stopped. Needs a restart and a manual check.", priority=True)
        else:
            self.send(f"⛔ ENTRIES HALTED {self._tag()}\n{reason}", priority=True)

    def resumed(self) -> None:
        if self.enabled and self.cfg.notify_halts:
            self.send(f"▶️ Entries resumed {self._tag()}", priority=True)

    def feed(self, state: str, detail: str = "") -> None:
        if not (self.enabled and self.cfg.notify_feed):
            return
        text = {
            "ok": "✅ Data feed recovered",
            "fallback": "⚠️ WebSocket down — using REST fallback (slower)",
            "down": "🔴 Data feed DOWN — no new entries until quotes return",
        }[state]
        self.send(f"{text} {self._tag()}" + (f"\n{detail}" if detail else ""), priority=state != "ok")


async def find_chat_ids(session: aiohttp.ClientSession, cfg: AlertsCfg, token: str) -> list[dict]:
    """List chats that have messaged the bot (getUpdates), so the user can pick TELEGRAM_CHAT_ID."""
    url = f"{cfg.api_base}/bot{token}/getUpdates"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
        data = await r.json(content_type=None)
    if not (isinstance(data, dict) and data.get("ok")):
        desc = data.get("description") if isinstance(data, dict) else "unexpected response"
        raise RuntimeError(str(desc).replace(token, "***"))
    chats: dict[int, dict] = {}
    for upd in data.get("result", []):
        msg = upd.get("message") or upd.get("channel_post") or {}
        chat = msg.get("chat")
        if chat:
            chats[chat["id"]] = {
                "id": chat["id"], "type": chat.get("type"),
                "name": chat.get("title") or chat.get("username") or chat.get("first_name") or "",
            }
    return list(chats.values())
