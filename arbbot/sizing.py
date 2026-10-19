"""Order-book aware sizing: edge as a function of position size q.

Top-of-book quotes say nothing about what $200 will really cost. Here both legs are walked through
the actual book (VWAP fill for q), so

    EntryEdge(q) = B_entry(q) - (mu + k*sigma + ExitCost(q)) + ExpectedFunding - Fees - Slippage - RiskBuffer

and the bot trades the LARGEST size (from `size_steps`) whose edge still clears the required minimum,
instead of "half the visible depth". Paper fills use the same VWAP prices.
"""
import asyncio
import logging
import time
from dataclasses import dataclass

from .config import StrategyCfg
from .models import Market, Signal

log = logging.getLogger("sizing")
BPS = 1e4
Levels = list[tuple[float, float]]  # (price, qty), best price first


@dataclass
class Books:
    spot_bids: Levels
    spot_asks: Levels
    perp_bids: Levels
    perp_asks: Levels


@dataclass
class Plan:
    notional: float
    perp_qty: float
    perp_px: float          # VWAP the short perp is expected to fill at
    spot_px: float          # VWAP the spot buy is expected to fill at (per spot unit)
    entry_basis_bps: float
    exit_cost_bps: float
    net_edge_bps: float


def walk(levels: Levels, qty: float) -> float | None:
    """Average price to trade `qty` against `levels`; None if the book is too thin to fill it."""
    if qty <= 0:
        return None
    remaining, cost = qty, 0.0
    for price, size in levels:
        take = min(remaining, size)
        cost += take * price
        remaining -= take
        if remaining <= qty * 1e-9:
            break
    if remaining > qty * 1e-9:
        return None
    return cost / (qty - max(remaining, 0.0))


def _basis(perp_px: float, spot_px: float, mult: float) -> float:
    spot_scaled = spot_px * mult
    return (perp_px - spot_scaled) / spot_scaled * BPS


def entry_fill(books: Books, m: Market, perp_qty: float) -> tuple[float, float] | None:
    """(perp_avg, spot_avg): sell the perp into the bids, buy the equivalent spot from the asks."""
    perp, spot = walk(books.perp_bids, perp_qty), walk(books.spot_asks, perp_qty * m.mult)
    return (perp, spot) if perp is not None and spot is not None else None


def exit_fill(books: Books, m: Market, perp_qty: float) -> tuple[float, float] | None:
    """(perp_avg, spot_avg): buy the perp back from the asks, sell the spot into the bids."""
    perp, spot = walk(books.perp_asks, perp_qty), walk(books.spot_bids, perp_qty * m.mult)
    return (perp, spot) if perp is not None and spot is not None else None


def plan_size(m: Market, sig: Signal, notional: float, books: Books, strat: StrategyCfg,
              min_trade_usd: float, steps: list[float]) -> Plan | None:
    """Largest size (fraction of `notional`) whose size-dependent net edge still clears the minimum."""
    if not books.perp_bids or books.perp_bids[0][0] <= 0:
        return None
    top = books.perp_bids[0][0]
    for frac in sorted(steps, reverse=True):
        n = notional * frac
        if n < min_trade_usd:
            break
        q = n / top
        ent, ext = entry_fill(books, m, q), exit_fill(books, m, q)
        if ent is None or ext is None:
            continue
        b_in = _basis(ent[0], ent[1], m.mult)
        b_out = _basis(ext[0], ext[1], m.mult)
        exit_cost = max(sig.exit_cost_bps, b_out - sig.basis_mid_bps)
        net = b_in - (sig.target_mid_bps + exit_cost) + sig.carry_bps - sig.cost_bps - sig.risk_buffer_bps
        if net >= strat.min_net_edge_bps and b_in >= strat.min_entry_basis_bps:
            return Plan(notional=n, perp_qty=q, perp_px=ent[0], spot_px=ent[1], entry_basis_bps=b_in,
                        exit_cost_bps=exit_cost, net_edge_bps=net)
    return None


def _levels(raw) -> Levels:
    return [(float(p), float(q)) for p, q in raw]


class RestDepth:
    """Fetches both order books on demand (only when a signal fires, so request weight stays tiny)."""

    def __init__(self, client, levels: int = 20, timeout_s: float = 1.5):
        self.client = client
        self.levels = levels
        self.timeout_s = timeout_s  # short on purpose: a slow book is a stale book, and the market keeps moving
        self._last_warned = 0.0
        self.failures = 0

    async def books(self, m: Market) -> Books | None:
        try:
            fut, spot = await asyncio.gather(
                self.client.get("fapi", "/fapi/v1/depth", {"symbol": m.symbol, "limit": self.levels}, timeout=self.timeout_s),
                self.client.get("spot", "/api/v3/depth", {"symbol": m.spot_symbol, "limit": self.levels}, timeout=self.timeout_s),
            )
            return Books(spot_bids=_levels(spot["bids"]), spot_asks=_levels(spot["asks"]),
                         perp_bids=_levels(fut["bids"]), perp_asks=_levels(fut["asks"]))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # the engine re-checks the signal on fresh quotes before trading without a book
            self.failures += 1
            now = time.monotonic()
            if now - self._last_warned > 60:  # at most one warning a minute, but never silent for good
                self._last_warned = now
                log.warning("depth fetch failed for %s (%s: %s); %d failure(s) so far", m.symbol,
                            type(e).__name__, e or "timeout", self.failures)
            return None
