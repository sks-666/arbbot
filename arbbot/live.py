"""Live executor: hedged entries (long spot + short perp) and exits via Binance REST.

SAFETY MODEL
  * Both legs are sent concurrently. If only one fills, the filled leg is unwound immediately
    (retried); if that unwind also fails an UnhedgedError is raised and the engine trips the
    fault breaker (entries stop; needs a human + restart).
  * Partial fills are accepted, not treated as failures. After an entry the two legs are compared in
    delta terms (spot_qty vs perp_qty * multiplier); any excess beyond 1% is unwound so the position
    is always matched. Exits top up partial fills (up to 3 orders per leg) and raise UnhedgedError if a
    non-dust remainder is left on either leg.
  * Spot buys pay their fee in the base asset, so the position records the NET quantity actually
    received and sells only that on exit.
  * Real fee tiers are read from the account (fetch_fees). Futures fees on individual fills are still
    estimated from that rate; funding is tracked as an estimate.

STATUS: written against Binance's documented REST API but NOT exercised against the exchange.
Run it on testnet (--live --testnet) and then with minimum size before trusting it.
"""
import asyncio
import logging
from decimal import ROUND_DOWN, Decimal

from .binance_client import BinanceClient, BinanceError
from .config import Config
from .executor import UnhedgedError
from .models import ClosedTrade, Market, Position, Signal

log = logging.getLogger("live")
BPS = 1e4
HEDGE_TOLERANCE = 0.01  # legs may differ by 1% (or one lot step) before the excess is unwound
_TERMINAL = ("FILLED", "EXPIRED", "CANCELED", "REJECTED")


def _d(x) -> Decimal:
    return Decimal(str(x))


def fmt(d: Decimal) -> str:
    return format(d.normalize(), "f")


def floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step


def align_qty(raw_perp_qty: float, mult: float, fut_step: float, spot_step: float) -> tuple[Decimal, Decimal]:
    """Largest (perp_qty, spot_qty) with spot_qty == perp_qty * mult and both on their lot steps.

    Returns (0, 0) if no aligned size exists.
    """
    fs, ss, k = _d(fut_step), _d(spot_step), _d(mult)
    q = floor_to_step(_d(raw_perp_qty), fs)
    for _ in range(200):
        if q <= 0:
            break
        s = floor_to_step(q * k, ss)
        q2 = floor_to_step(s / k, fs)
        if q2 == q:  # q2 < q whenever q*k was not already on the spot grid, so equality means aligned
            return q, q * k
        q = q2
    return Decimal(0), Decimal(0)


def hedge_excess(perp_qty: Decimal, spot_net: Decimal, mult: float, fut_step: float, spot_step: float,
                 tol: float = HEDGE_TOLERANCE) -> tuple[str, Decimal] | None:
    """Which leg is over-sized, and by how much, if the two are not delta-matched.

    Returns ("spot", qty_in_spot_units) when too much spot is held, ("perp", qty_in_contracts) when the
    short is too large, or None when the legs match within tolerance (or the excess is below one lot step).
    """
    k, fs, ss = _d(mult), _d(fut_step), _d(spot_step)
    expected_spot = perp_qty * k
    diff = spot_net - expected_spot
    tolerance = max(expected_spot * _d(tol), ss)
    if diff > tolerance:
        q = floor_to_step(diff, ss)
        return ("spot", q) if q > 0 else None
    if diff < -tolerance:
        q = floor_to_step((-diff) / k, fs)
        return ("perp", q) if q > 0 else None
    return None


class LiveExecutor:
    live = True

    def __init__(self, client: BinanceClient, cfg: Config):
        self.c = client
        self.cfg = cfg
        self.fees = cfg.fees
        self._margin_ready: set[str] = set()

    async def preflight(self) -> dict:
        """Verify account mode and balances before any order can be sent. Raises on anything unsafe."""
        offset = await self.c.sync_time()
        dual = await self.c.get("fapi", "/fapi/v1/positionSide/dual", signed=True)
        if dual.get("dualSidePosition"):
            raise RuntimeError("Futures account is in Hedge Mode; switch to One-way Mode before running live")
        try:
            fut_bal = await self.c.get("fapi", "/fapi/v3/balance", signed=True)
        except BinanceError as e:
            if e.status != 404:
                raise
            fut_bal = await self.c.get("fapi", "/fapi/v2/balance", signed=True)
        fut_usdt = next((float(b.get("availableBalance", 0)) for b in fut_bal if b.get("asset") == "USDT"), 0.0)
        acct = await self.c.get("spot", "/api/v3/account", signed=True)
        spot_usdt = next((float(b["free"]) for b in acct["balances"] if b["asset"] == "USDT"), 0.0)
        info = {"time_offset_ms": offset, "futures_usdt": fut_usdt, "spot_usdt": spot_usdt}
        log.info("preflight ok: %s", info)
        return info

    async def fetch_fees(self) -> tuple[float, float]:
        """(futures_taker_bps, spot_taker_bps) from this account's actual commission rates (VIP tier, discounts).

        Futures rates are per symbol on paper but tier-wide in practice, so one liquid symbol is queried.
        """
        fut = await self.c.get("fapi", "/fapi/v1/commissionRate", {"symbol": "BTCUSDT"}, signed=True)
        acct = await self.c.get("spot", "/api/v3/account", signed=True)
        return float(fut["takerCommissionRate"]) * BPS, float(acct["commissionRates"]["taker"]) * BPS

    async def _fut_get(self, v3: str, v2: str):
        """Signed futures GET, falling back to the older endpoint version if the newer one is absent (404)."""
        try:
            return await self.c.get("fapi", v3, signed=True)
        except BinanceError as e:
            if e.status != 404:
                raise
            return await self.c.get("fapi", v2, signed=True)

    async def reconcile(self, positions: dict[str, Position], markets: dict[str, Market]) -> list[str]:
        """Compare the bot's saved positions with what Binance actually holds. Returns human-readable problems.

        Checks: every saved position still has its short perp and enough spot base asset, and no open
        perp position exists in a tracked market that the bot doesn't know about (use a dedicated
        account or sub-account for this bot so unrelated positions are never mistaken for orphans).
        """
        problems: list[str] = []
        risk = await self._fut_get("/fapi/v3/positionRisk", "/fapi/v2/positionRisk")
        shorts = {p["symbol"]: float(p["positionAmt"]) for p in risk if float(p.get("positionAmt", 0)) != 0}
        acct = await self.c.get("spot", "/api/v3/account", signed=True)
        held = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acct["balances"]}
        for sym, pos in positions.items():
            amt = shorts.pop(sym, 0.0)
            if amt >= 0 or abs(abs(amt) - pos.perp_qty) > max(1e-9, 0.01 * pos.perp_qty):
                problems.append(f"{sym}: bot expects short {pos.perp_qty:g} but Binance reports positionAmt {amt:g}")
            m = markets.get(sym)
            base = m.base_asset if m else ""
            if base and held.get(base, 0.0) < pos.spot_qty * 0.98:
                problems.append(f"{sym}: bot expects ~{pos.spot_qty:g} {base} spot but wallet holds {held.get(base, 0.0):g}")
        for sym, amt in shorts.items():
            if sym in markets:
                problems.append(f"{sym}: Binance has an open perp position ({amt:g}) the bot has no record of")
        return problems

    async def _ensure_margin(self, symbol: str) -> None:
        if symbol in self._margin_ready:
            return
        try:
            await self.c.post("fapi", "/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"})
        except BinanceError as e:
            if e.code != -4046:  # "No need to change margin type"
                raise
        await self.c.post("fapi", "/fapi/v1/leverage", {"symbol": symbol, "leverage": self.cfg.execution.leverage})
        self._margin_ready.add(symbol)

    # ---- single orders (partial fills are results, not errors) ---------------------------------
    async def _fut_market(self, symbol: str, side: str, qty: Decimal, reduce_only: bool = False) -> dict:
        params = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": fmt(qty), "newOrderRespType": "RESULT"}
        if reduce_only:
            params["reduceOnly"] = "true"
        o = await self.c.post("fapi", "/fapi/v1/order", params)
        for _ in range(5):
            if o.get("status") in _TERMINAL:
                break
            await asyncio.sleep(0.2)
            o = await self.c.get("fapi", "/fapi/v1/order", {"symbol": symbol, "orderId": o["orderId"]}, signed=True)
        if o.get("status") not in _TERMINAL:  # still working: stop it so the executed quantity is final
            try:
                await self.c.request("fapi", "DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": o["orderId"]}, signed=True)
            except BinanceError:
                pass
            o = await self.c.get("fapi", "/fapi/v1/order", {"symbol": symbol, "orderId": o["orderId"]}, signed=True)
        executed = float(o.get("executedQty") or 0)
        if executed <= 0:
            raise RuntimeError(f"futures {side} {symbol} not filled (status={o.get('status')})")
        return {"avg": float(o["avgPrice"]), "qty": executed, "quote": float(o["avgPrice"]) * executed, "fills": []}

    async def _spot_market(self, symbol: str, side: str, qty: Decimal) -> dict:
        o = await self.c.post(
            "spot", "/api/v3/order",
            {"symbol": symbol, "side": side, "type": "MARKET", "quantity": fmt(qty), "newOrderRespType": "FULL"},
        )
        executed = float(o.get("executedQty") or 0)
        if executed <= 0:
            raise RuntimeError(f"spot {side} {symbol} not filled (status={o.get('status')})")
        quote = float(o["cummulativeQuoteQty"])
        return {"avg": quote / executed, "qty": executed, "quote": quote, "fills": o.get("fills", [])}

    @staticmethod
    async def _retry(fn, attempts: int = 3, delay: float = 0.4):
        last = None
        for i in range(attempts):
            try:
                return await fn()
            except Exception as e:  # noqa: BLE001 - we re-raise the last one
                last = e
                log.warning("retry %d/%d after error: %s", i + 1, attempts, e)
                await asyncio.sleep(delay)
        raise last

    @staticmethod
    async def _drain(order_fn, qty: Decimal, step: Decimal, min_qty: float, attempts: int = 3) -> dict:
        """Market-order `qty`, topping up after partial fills. Aggregates the fills; `remaining` is what is left.

        The first order's failure propagates (nothing happened); later failures just stop the top-ups.
        """
        remaining, total_qty, total_quote, fills = qty, 0.0, 0.0, []
        for i in range(attempts):
            remaining = floor_to_step(remaining, step)
            if remaining <= 0 or float(remaining) < min_qty:
                break
            try:
                r = await order_fn(remaining)
            except Exception:
                if i == 0:
                    raise
                break
            total_qty += r["qty"]
            total_quote += r["quote"]
            fills += r["fills"]
            remaining -= _d(r["qty"])
        return {"qty": total_qty, "quote": total_quote, "avg": total_quote / total_qty if total_qty else 0.0,
                "fills": fills, "remaining": remaining}

    def _commission_split(self, fills: list, base_asset: str, quote_value: float) -> tuple[float, float]:
        """Return (commission_in_base_units, commission_in_quote_usd_estimate_for_non_base_fees)."""
        base_fee = sum(float(f["commission"]) for f in fills if f.get("commissionAsset") == base_asset)
        quote_fee = sum(float(f["commission"]) for f in fills if f.get("commissionAsset") == "USDT")
        other = [f for f in fills if f.get("commissionAsset") not in (base_asset, "USDT")]
        est_other = quote_value * self.fees.spot_taker_bps / BPS if other else 0.0
        return base_fee, quote_fee + est_other

    # ---- entry ---------------------------------------------------------------------------------
    async def open(self, m: Market, sig: Signal, notional: float, now: float, books=None) -> Position:
        ff, sf = m.fut_filter, m.spot_filter
        pq, sq = align_qty(notional / m.perp.bid, m.mult, ff.step, sf.step)
        min_notional = max(ff.min_notional, sf.min_notional) * 1.05
        if pq <= 0 or float(pq) < ff.min_qty or float(sq) < sf.min_qty or float(pq) * m.perp.bid < min_notional:
            raise RuntimeError(f"{m.symbol}: size {notional:.2f} USD does not satisfy exchange lot/notional filters")
        await self._ensure_margin(m.symbol)

        fr, sr = await asyncio.gather(
            self._fut_market(m.symbol, "SELL", pq),
            self._spot_market(m.spot_symbol, "BUY", sq),
            return_exceptions=True,
        )
        if isinstance(fr, Exception) and isinstance(sr, Exception):
            raise RuntimeError(f"both legs failed on entry: futures={fr}; spot={sr}")
        if isinstance(sr, Exception):  # futures short filled (maybe partly), spot did not -> buy the short back
            back = floor_to_step(_d(fr["qty"]), _d(ff.step))
            try:
                await self._retry(lambda: self._drain(
                    lambda q: self._fut_market(m.symbol, "BUY", q, reduce_only=True), back, _d(ff.step), ff.min_qty))
            except Exception as e:
                raise UnhedgedError(f"{m.symbol}: SHORT PERP OPEN, spot buy failed ({sr}) and unwind failed ({e})")
            raise RuntimeError(f"spot leg failed ({sr}); futures leg unwound")
        base_fee, quote_fee = self._commission_split(sr["fills"], m.base_asset, sr["quote"])
        spot_net = sr["qty"] - base_fee
        if isinstance(fr, Exception):  # spot filled (maybe partly), futures did not -> sell the spot back
            back = floor_to_step(_d(spot_net), _d(sf.step))
            try:
                await self._retry(lambda: self._drain(
                    lambda q: self._spot_market(m.spot_symbol, "SELL", q), back, _d(sf.step), sf.min_qty))
            except Exception as e:
                raise UnhedgedError(f"{m.symbol}: LONG SPOT OPEN, futures sell failed ({fr}) and unwind failed ({e})")
            raise RuntimeError(f"futures leg failed ({fr}); spot leg unwound")

        # Both legs filled, possibly partially and unequally: bring them back to delta-matched.
        perp_qty, perp_avg = fr["qty"], fr["avg"]
        spot_cost = sr["quote"]                      # total quote paid for the spot
        perp_proceeds = perp_avg * perp_qty          # total quote received for the short
        extra_fees = 0.0
        excess = hedge_excess(_d(perp_qty), _d(spot_net), m.mult, ff.step, sf.step)
        if excess is not None:
            side, q = excess
            try:
                if side == "spot":   # too much spot: sell the surplus
                    r = await self._retry(lambda: self._drain(
                        lambda x: self._spot_market(m.spot_symbol, "SELL", x), q, _d(sf.step), sf.min_qty))
                    spot_net -= r["qty"]
                    spot_cost -= r["quote"]
                    extra_fees += r["quote"] * self.fees.spot_taker_bps / BPS
                else:                # short too large: buy the surplus back
                    r = await self._retry(lambda: self._drain(
                        lambda x: self._fut_market(m.symbol, "BUY", x, reduce_only=True), q, _d(ff.step), ff.min_qty))
                    perp_qty -= r["qty"]
                    perp_proceeds -= r["quote"]
                    extra_fees += r["quote"] * self.fees.futures_taker_bps / BPS
            except Exception as e:
                raise UnhedgedError(f"{m.symbol}: legs filled unequally (spot {spot_net:g}, perp {perp_qty:g} x{m.mult:g}) "
                                    f"and rebalancing the {side} leg failed ({e})")
            log.warning("%s: partial fills left the legs unmatched; rebalanced the %s leg by %s", m.symbol, side, q)
        if perp_qty <= 0 or spot_net <= 0:
            raise UnhedgedError(f"{m.symbol}: nothing left after rebalancing (spot {spot_net:g}, perp {perp_qty:g})")

        fut_fee = fr["qty"] * perp_avg * self.fees.futures_taker_bps / BPS
        return Position(
            symbol=m.symbol, spot_symbol=m.spot_symbol, mult=m.mult, opened_at=now, notional=perp_qty * perp_avg,
            spot_qty=spot_net, perp_qty=perp_qty,
            spot_entry=spot_cost / spot_net,         # effective cost per unit actually held (fees/rebalancing embedded)
            perp_entry=perp_proceeds / perp_qty,
            entry_basis_bps=sig.basis_in_bps, entry_z=sig.z,
            fees_paid=quote_fee + fut_fee + extra_fees, next_funding_ms=m.next_funding_ms, last_fresh=now, live=True,
            mu_entry=sig.mean_bps, sigma_entry=sig.std_bps, entry_mid_bps=sig.basis_mid_bps,
            predicted_net_bps=sig.net_edge_bps, funding_interval_h=m.funding_interval_h,
        )

    # ---- exit ----------------------------------------------------------------------------------
    async def close(self, pos: Position, m: Market, sig: Signal, now: float, reason: str) -> ClosedTrade:
        ff, sf = m.fut_filter, m.spot_filter
        sell_qty = floor_to_step(_d(pos.spot_qty), _d(sf.step))
        buy_qty = _d(pos.perp_qty)

        def fut_leg():
            return self._drain(lambda q: self._fut_market(m.symbol, "BUY", q, reduce_only=True), buy_qty, _d(ff.step), ff.min_qty)

        def spot_leg():
            return self._drain(lambda q: self._spot_market(m.spot_symbol, "SELL", q), sell_qty, _d(sf.step), sf.min_qty)

        fr, sr = await asyncio.gather(fut_leg(), spot_leg(), return_exceptions=True)
        # Re-try only the leg that failed outright; if it still fails the book is unhedged.
        if isinstance(fr, Exception) and not isinstance(sr, Exception):
            try:
                fr = await self._retry(fut_leg)
            except Exception as e:
                raise UnhedgedError(f"{m.symbol}: spot sold but futures buy-back failed ({e}); SHORT PERP STILL OPEN")
        elif isinstance(sr, Exception) and not isinstance(fr, Exception):
            try:
                sr = await self._retry(spot_leg)
            except Exception as e:
                raise UnhedgedError(f"{m.symbol}: futures closed but spot sell failed ({e}); LONG SPOT STILL HELD")
        if isinstance(fr, Exception) or isinstance(sr, Exception):
            raise RuntimeError(f"close failed on both legs, position unchanged: futures={fr}; spot={sr}")

        # A remainder worth more than the exchange minimum is a real, unhedged exposure; smaller is dust.
        fut_left, spot_left = float(fr["remaining"]), float(sr["remaining"])
        if fut_left * m.perp.ask >= max(ff.min_notional, 1.0) and fut_left >= ff.min_qty:
            raise UnhedgedError(f"{m.symbol}: partial close, {fut_left:g} perp contracts STILL SHORT (spot leg closed)")
        if spot_left * m.spot.bid >= max(sf.min_notional, 1.0) and spot_left >= sf.min_qty:
            raise UnhedgedError(f"{m.symbol}: partial close, {spot_left:g} spot units STILL HELD (futures leg closed)")

        _, quote_fee = self._commission_split(sr["fills"], m.base_asset, sr["quote"])
        exit_fees = quote_fee + fr["quote"] * self.fees.futures_taker_bps / BPS
        fees_total = pos.fees_paid + exit_fees
        pnl = (
            sr["quote"] - sr["qty"] * pos.spot_entry
            + fr["qty"] * (pos.perp_entry - fr["avg"])
            + pos.funding_acc
            - fees_total
        )
        return ClosedTrade(
            symbol=pos.symbol, opened_at=pos.opened_at, closed_at=now, notional=pos.notional, pnl_usd=pnl,
            pnl_bps=pnl / pos.notional * BPS, entry_basis_bps=pos.entry_basis_bps, exit_basis_bps=sig.basis_out_bps,
            fees_usd=fees_total, funding_usd=pos.funding_acc, reason=reason, live=True,
            predicted_net_bps=pos.predicted_net_bps,
        )
