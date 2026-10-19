"""Paper executor: fills against the live top of book with fees and slippage, no orders sent.

The live executor (live.py) exposes the same open()/close() interface, so the engine is unaware
of which one it is driving.
"""
from .config import FeesCfg
from .models import ClosedTrade, Market, Position, Signal
from .sizing import Books, entry_fill

BPS = 1e4


class UnhedgedError(Exception):
    """One leg is open without its hedge and could not be unwound. Trading must stop until a human looks."""


class PaperExecutor:
    live = False

    def __init__(self, fees: FeesCfg):
        self.fees = fees

    async def open(self, m: Market, sig: Signal, notional: float, now: float, books: Books | None = None) -> Position:
        f = self.fees
        s = f.slippage_bps / BPS
        spot_px, perp_px = m.spot.ask, m.perp.bid          # top of book: buy spot at the ask, sell perp at the bid
        fill = entry_fill(books, m, notional / m.perp.bid) if books is not None else None
        if fill is not None:                                 # real depth: VWAP for this size
            perp_px, spot_px = fill
        # The basis actually obtained, from the prices actually used (never the one that triggered the signal).
        entry_basis = (perp_px - spot_px * m.mult) / (spot_px * m.mult) * BPS
        spot_px *= 1 + s
        perp_px *= 1 - s
        perp_qty = notional / perp_px
        spot_qty = perp_qty * m.mult
        fees = spot_qty * spot_px * f.spot_taker_bps / BPS + perp_qty * perp_px * f.futures_taker_bps / BPS
        return Position(
            symbol=m.symbol, spot_symbol=m.spot_symbol, mult=m.mult, opened_at=now, notional=notional,
            spot_qty=spot_qty, perp_qty=perp_qty, spot_entry=spot_px, perp_entry=perp_px,
            entry_basis_bps=entry_basis, entry_z=sig.z, fees_paid=fees,
            next_funding_ms=m.next_funding_ms, last_fresh=now, live=False,
            mu_entry=sig.mean_bps, sigma_entry=sig.std_bps, entry_mid_bps=sig.basis_mid_bps,
            predicted_net_bps=sig.net_edge_bps, funding_interval_h=m.funding_interval_h,
        )

    async def close(self, pos: Position, m: Market, sig: Signal, now: float, reason: str) -> ClosedTrade:
        f = self.fees
        s = f.slippage_bps / BPS
        spot_px = m.spot.bid * (1 - s)   # sell spot at the bid
        perp_px = m.perp.ask * (1 + s)   # buy back perp at the ask
        exit_fees = pos.spot_qty * spot_px * f.spot_taker_bps / BPS + pos.perp_qty * perp_px * f.futures_taker_bps / BPS
        fees_total = pos.fees_paid + exit_fees
        pnl = (
            pos.spot_qty * (spot_px - pos.spot_entry)
            + pos.perp_qty * (pos.perp_entry - perp_px)
            + pos.funding_acc
            - fees_total
        )
        return ClosedTrade(
            symbol=pos.symbol, opened_at=pos.opened_at, closed_at=now, notional=pos.notional, pnl_usd=pnl,
            pnl_bps=pnl / pos.notional * BPS, entry_basis_bps=pos.entry_basis_bps,
            exit_basis_bps=sig.basis_out_bps, fees_usd=fees_total, funding_usd=pos.funding_acc,
            reason=reason, live=False, predicted_net_bps=pos.predicted_net_bps,
        )
