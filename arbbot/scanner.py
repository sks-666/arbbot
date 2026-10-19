"""Basis scanner: turns a Market snapshot into a Signal.

Strategy: perp-vs-spot basis convergence (cash-and-carry). When the perpetual trades rich to spot
by far more than its own recent norm, buy spot and short the perp, then unwind as the basis
reverts. Core entry formula (all in basis points):

    z        = (B_mid - mu) / sigma                      mu, sigma: median / 1.4826*MAD of recent B_mid
    target   = mu + k*sigma + ExitCost                   k = exit_z; ExitCost = conservative percentile
    Gross    = B_entry - target                          B_entry: executable (short perp @bid, buy spot @ask)
    NetEdge  = Gross + ExpectedFunding - Fees - Slippage - RiskBuffer

Entry needs z >= z_entry AND NetEdge >= MinimumRequiredEdge AND the perp at/above spot. Funding counts
only for funding events actually crossed during the expected hold; the risk buffer grows with observed
model error (see Engine). Nothing fires on gross spread alone.
"""
import math
from collections import deque
from statistics import median

from .config import FeesCfg, StrategyCfg
from .models import Market, Signal

BPS = 1e4
MAD_TO_SIGMA = 1.4826  # makes MAD a consistent estimator of sigma for normally distributed data


def round_trip_cost_bps(fees: FeesCfg) -> float:
    """Entry + exit on both legs (taker fees) plus assumed slippage on all four fills."""
    return 2 * (fees.futures_taker_bps + fees.spot_taker_bps) + 4 * fees.slippage_bps


def percentile(values, p: float) -> float:
    """Nearest-rank percentile, p in (0, 1]."""
    xs = sorted(values)
    return xs[min(len(xs) - 1, max(0, math.ceil(p * len(xs)) - 1))]


def expected_funding_bps(m: Market, now: float, hold_s: float) -> float:
    """Funding a SHORT perp expects to receive over the hold, counting only funding events actually crossed.

    Funding is a discrete cash flow at fixed timestamps (every `funding_interval_h` hours), not a
    continuous accrual: a 5-minute hold that crosses no funding time earns nothing from it.
    """
    if not m.next_funding_ms:
        return 0.0
    interval = max(m.funding_interval_h, 0.25) * 3600.0
    first = m.next_funding_ms / 1000.0
    if first < now:  # stale timestamp: roll forward to the next event
        first += math.ceil((now - first) / interval) * interval
    end = now + hold_s
    if first > end:
        return 0.0
    events = 1 + int((end - first) // interval)
    rate = m.funding_rate
    if m.funding_cap is not None:
        rate = min(rate, m.funding_cap)
    if m.funding_floor is not None:
        rate = max(rate, m.funding_floor)
    return events * rate * BPS


class Scanner:
    def __init__(self, strat: StrategyCfg, fees: FeesCfg, max_quote_age_s: float):
        self.strat = strat
        self.fees = fees
        self.max_age = max_quote_age_s
        self.risk_buffer_bps = strat.risk_buffer_bps  # the engine raises this as model error is observed
        self._hist: dict[str, deque[float]] = {}
        self._pen: dict[str, deque[float]] = {}  # recent exit penalties: B_out - B_mid
        self._last_sample_ts: dict[str, float] = {}

    def baseline(self, hist) -> tuple[float, float]:
        """Normal basis and its spread. Robust mode: a spike cannot inflate its own yardstick."""
        xs = list(hist)
        if self.strat.baseline == "robust":
            mu = median(xs)
            sigma = MAD_TO_SIGMA * median(abs(x - mu) for x in xs)
        else:
            mu = sum(xs) / len(xs)
            sigma = math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs))
        return mu, max(sigma, self.strat.std_floor_bps)

    def evaluate(self, m: Market, now: float) -> Signal:
        st = self.strat
        sig = Signal(symbol=m.symbol, ts=now)
        if m.index > 0 and m.mark > 0:
            sig.mark_index_bps = (m.mark - m.index) / m.index * BPS
        sig.funding_apr_pct = m.funding_rate * (24 / max(m.funding_interval_h, 0.25)) * 365 * 100

        sp, pp = m.spot, m.perp
        if sp is None or pp is None:
            sig.note = "no quotes"
            return sig
        quote_ts = min(sp.ts, pp.ts)
        sig.age_ms = max(0.0, (now - quote_ts) * 1000)
        if now - quote_ts > self.max_age:
            sig.note = "stale quotes"
            return sig
        if min(sp.bid, sp.ask, pp.bid, pp.ask) <= 0 or sp.bid > sp.ask or pp.bid > pp.ask:
            sig.note = "bad quote"
            return sig

        k = m.mult
        s_bid, s_ask = sp.bid * k, sp.ask * k
        s_mid = (s_bid + s_ask) / 2.0
        p_mid = pp.mid
        b_in = (pp.bid - s_ask) / s_ask * BPS    # short perp @bid, long spot @ask
        b_out = (pp.ask - s_bid) / s_bid * BPS   # buy perp @ask, sell spot @bid
        b_mid = (p_mid - s_mid) / s_mid * BPS
        if abs(b_mid) > st.max_abs_basis_bps:
            sig.note = "basis out of range"
            return sig

        sig.perp_mid, sig.spot_mid = p_mid, s_mid
        sig.basis_in_bps, sig.basis_out_bps, sig.basis_mid_bps = b_in, b_out, b_mid
        sig.fillable_usd = min(sp.ask_qty * sp.ask, pp.bid_qty * pp.bid)
        sig.cost_bps = round_trip_cost_bps(self.fees)
        sig.risk_buffer_bps = self.risk_buffer_bps
        if abs(sig.mark_index_bps) > st.max_mark_index_bps:
            # Mark far from index means one of the prices is broken (delisting, halted spot, thin book).
            sig.note = "mark/index dislocated"
            return sig

        hist = self._hist.setdefault(m.symbol, deque(maxlen=st.lookback))
        pen = self._pen.setdefault(m.symbol, deque(maxlen=st.lookback))
        n = len(hist)
        sig.samples = n
        if n < st.min_samples:
            sig.status = "warmup"
            self._push(m.symbol, hist, pen, b_mid, b_out - b_mid, quote_ts)
            return sig

        mu, sigma = self.baseline(hist)
        sig.mean_bps, sig.std_bps = mu, sigma
        sig.z = (b_mid - mu) / sigma

        # Expected exit spread: a conservative percentile of what exits have recently cost, never below today's.
        today_penalty = b_out - b_mid
        sig.exit_cost_bps = max(today_penalty, percentile(pen, st.exit_cost_percentile)) if pen else today_penalty
        sig.target_mid_bps = mu + st.exit_z * sigma
        sig.gross_bps = b_in - (sig.target_mid_bps + sig.exit_cost_bps)
        sig.carry_bps = expected_funding_bps(m, now, st.expected_hold_s)
        sig.net_edge_bps = sig.gross_bps + sig.carry_bps - sig.cost_bps - sig.risk_buffer_bps

        # Never short a perp that trades BELOW spot, however far it has "reverted" from a (possibly broken) mean.
        if sig.z >= st.z_entry and sig.net_edge_bps >= st.min_net_edge_bps and b_in >= st.min_entry_basis_bps:
            sig.status = "signal"
        elif sig.z >= 0.66 * st.z_entry or sig.net_edge_bps > 0:
            sig.status = "watch"
        else:
            sig.status = "ok"
        self._push(m.symbol, hist, pen, b_mid, today_penalty, quote_ts)
        return sig

    def _push(self, symbol: str, hist: deque, pen: deque, basis: float, penalty: float, quote_ts: float) -> None:
        """Record one sample per distinct quote snapshot (the engine may tick faster than the feed)."""
        if self._last_sample_ts.get(symbol) != quote_ts:
            hist.append(basis)
            pen.append(penalty)
            self._last_sample_ts[symbol] = quote_ts
