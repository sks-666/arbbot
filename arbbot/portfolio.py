"""Position book and SQLite persistence.

State survives restarts: open positions are reloaded and the daily-loss limit is rebuilt from the
trade log, so restarting the bot can never reset a risk limit.
"""
import json
import sqlite3
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .config import FeesCfg
from .models import ClosedTrade, Market, Position

BPS = 1e4


def utc_midnight(ts: float) -> float:
    d = datetime.fromtimestamp(ts, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return d.timestamp()


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)  # autocommit
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY, json TEXT NOT NULL)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS trades ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, closed_at REAL NOT NULL, "
            "symbol TEXT NOT NULL, pnl_usd REAL NOT NULL, json TEXT NOT NULL)"
        )

    def save_position(self, pos: Position) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO positions(symbol, json) VALUES (?, ?)", (pos.symbol, json.dumps(asdict(pos)))
        )

    def delete_position(self, symbol: str) -> None:
        self.db.execute("DELETE FROM positions WHERE symbol = ?", (symbol,))

    def load_positions(self) -> list[Position]:
        rows = self.db.execute("SELECT json FROM positions").fetchall()
        return [Position(**json.loads(r[0])) for r in rows]

    def append_trade(self, t: ClosedTrade) -> None:
        self.db.execute(
            "INSERT INTO trades(closed_at, symbol, pnl_usd, json) VALUES (?, ?, ?, ?)",
            (t.closed_at, t.symbol, t.pnl_usd, json.dumps(asdict(t))),
        )

    def recent_trades(self, n: int) -> list[ClosedTrade]:
        rows = self.db.execute("SELECT json FROM trades ORDER BY id DESC LIMIT ?", (n,)).fetchall()
        return [ClosedTrade(**json.loads(r[0])) for r in reversed(rows)]

    def totals(self) -> tuple[float, int, int]:
        row = self.db.execute(
            "SELECT COALESCE(SUM(pnl_usd), 0), COALESCE(SUM(pnl_usd > 0), 0), COALESCE(SUM(pnl_usd <= 0), 0) FROM trades"
        ).fetchone()
        return float(row[0]), int(row[1]), int(row[2])

    def pnl_since(self, ts: float) -> float:
        row = self.db.execute("SELECT COALESCE(SUM(pnl_usd), 0) FROM trades WHERE closed_at >= ?", (ts,)).fetchone()
        return float(row[0])


class Portfolio:
    def __init__(self, store: Store, now: float):
        self.store = store
        self.open: dict[str, Position] = {p.symbol: p for p in store.load_positions()}
        self.closed: deque[ClosedTrade] = deque(store.recent_trades(200), maxlen=200)
        self.realized_usd, self.wins, self.losses = store.totals()
        self._day_start = utc_midnight(now)
        self._day_pnl = store.pnl_since(self._day_start)

    def day_pnl(self, now: float) -> float:
        if now >= self._day_start + 86400:
            self._day_start = utc_midnight(now)
            self._day_pnl = 0.0
        return self._day_pnl

    def open_notional(self) -> float:
        return sum(p.notional for p in self.open.values())

    def add(self, pos: Position) -> None:
        self.open[pos.symbol] = pos
        self.store.save_position(pos)

    def update(self, pos: Position) -> None:
        self.store.save_position(pos)

    def close(self, pos: Position, trade: ClosedTrade) -> None:
        self.open.pop(pos.symbol, None)
        self.store.delete_position(pos.symbol)
        self.store.append_trade(trade)
        self.closed.append(trade)
        self.day_pnl(trade.closed_at)  # roll the day first
        self._day_pnl += trade.pnl_usd
        self.realized_usd += trade.pnl_usd
        if trade.pnl_usd > 0:
            self.wins += 1
        else:
            self.losses += 1

    @property
    def win_rate(self) -> float:
        n = self.wins + self.losses
        return self.wins / n if n else 0.0


def unrealized_usd(pos: Position, m: Market, fees: FeesCfg) -> float:
    """Mark-to-exit P&L: what closing at today's executable prices would realise, after all costs."""
    if m.spot is None or m.perp is None:
        return 0.0
    s = fees.slippage_bps / BPS
    spot_px = m.spot.bid * (1 - s)
    perp_px = m.perp.ask * (1 + s)
    exit_fee = (
        pos.spot_qty * spot_px * fees.spot_taker_bps / BPS + pos.perp_qty * perp_px * fees.futures_taker_bps / BPS
    )
    return (
        pos.spot_qty * (spot_px - pos.spot_entry)
        + pos.perp_qty * (pos.perp_entry - perp_px)
        + pos.funding_acc
        - pos.fees_paid
        - exit_fee
    )
