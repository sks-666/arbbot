"""Core data types shared by the feed, scanner, risk and execution layers."""
from dataclasses import dataclass, field


@dataclass
class Quote:
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    ts: float  # local receive time (epoch seconds)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass
class SymbolFilter:
    """Exchange lot rules for one instrument (used by live execution only)."""
    step: float = 0.0
    min_qty: float = 0.0
    min_notional: float = 5.0


@dataclass
class Market:
    """One tradeable pair: a USDT perpetual and its spot counterpart.

    `mult` rescales spot to perp price (1000SHIBUSDT perp ~ 1000 * SHIBUSDT spot).
    """
    symbol: str
    spot_symbol: str
    base_asset: str
    mult: float = 1.0
    spot: Quote | None = None
    perp: Quote | None = None
    mark: float = 0.0
    index: float = 0.0
    funding_rate: float = 0.0
    next_funding_ms: int = 0
    quote_volume_usd: float = 0.0
    funding_interval_h: float = 8.0       # most perps settle every 8h; some every 4h/1h (from /fapi/v1/fundingInfo)
    funding_cap: float | None = None      # adjustedFundingRateCap / Floor, as fractions
    funding_floor: float | None = None
    fut_filter: SymbolFilter = field(default_factory=SymbolFilter)
    spot_filter: SymbolFilter = field(default_factory=SymbolFilter)


@dataclass
class Signal:
    """Result of evaluating one market on one tick. All *_bps values are basis points."""
    symbol: str
    ts: float
    status: str = "stale"  # stale | warmup | ok | watch | signal
    samples: int = 0
    perp_mid: float = 0.0
    spot_mid: float = 0.0       # already multiplied to perp scale
    basis_in_bps: float = 0.0   # executable: short perp @bid vs long spot @ask
    basis_mid_bps: float = 0.0
    basis_out_bps: float = 0.0  # executable cost to unwind now
    mean_bps: float = 0.0
    std_bps: float = 0.0
    z: float = 0.0
    mark_index_bps: float = 0.0
    funding_apr_pct: float = 0.0
    carry_bps: float = 0.0          # expected funding over the hold: only funding events actually crossed
    target_mid_bps: float = 0.0     # convergence target: mu + k*sigma (before exit cost)
    exit_cost_bps: float = 0.0      # conservative expected exit spread (percentile of recent history)
    risk_buffer_bps: float = 0.0    # model-error allowance subtracted from the edge
    cost_bps: float = 0.0
    gross_bps: float = 0.0
    net_edge_bps: float = 0.0
    fillable_usd: float = 0.0
    age_ms: float = 0.0
    note: str = ""


@dataclass
class Position:
    symbol: str
    spot_symbol: str
    mult: float
    opened_at: float
    notional: float           # perp notional at entry (USD)
    spot_qty: float           # spot units held (net of any fee taken in base asset)
    perp_qty: float           # short perp contracts
    spot_entry: float         # avg spot price per spot unit
    perp_entry: float
    entry_basis_bps: float
    entry_z: float
    fees_paid: float = 0.0
    funding_acc: float = 0.0
    next_funding_ms: int = 0
    last_fresh: float = 0.0
    live: bool = False
    # Reference regime frozen at entry: convergence is judged against this, never against a baseline
    # that the dislocation itself has dragged upward.
    mu_entry: float = 0.0
    sigma_entry: float = 0.0
    entry_mid_bps: float = 0.0
    predicted_net_bps: float = 0.0
    funding_interval_h: float = 8.0


@dataclass
class ClosedTrade:
    symbol: str
    opened_at: float
    closed_at: float
    notional: float
    pnl_usd: float
    pnl_bps: float
    entry_basis_bps: float
    exit_basis_bps: float
    fees_usd: float
    funding_usd: float
    reason: str
    live: bool = False
    predicted_net_bps: float = 0.0   # what the model expected at entry (feeds the adaptive risk buffer)
