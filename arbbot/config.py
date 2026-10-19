"""Typed configuration loaded from config.toml. Unknown keys raise, so typos can't change risk limits."""
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path


@dataclass
class UniverseCfg:
    max_markets: int = 55
    min_quote_volume_usd: float = 20_000_000.0
    exclude_bases: list[str] = field(
        default_factory=lambda: ["USDC", "FDUSD", "TUSD", "USDP", "BUSD", "EUR", "AEUR"]
    )


@dataclass
class FeedCfg:
    mode: str = "ws"
    interval_s: float = 1.0
    max_quote_age_s: float = 3.0
    request_timeout_s: float = 4.0
    ws_stale_s: float = 5.0
    ws_max_silent_s: float = 30.0
    ws_futures_url: str = ""
    ws_spot_url: str = ""
    ws_streams_per_conn: int = 40


@dataclass
class FeesCfg:
    futures_taker_bps: float = 5.0
    spot_taker_bps: float = 10.0
    slippage_bps: float = 1.0


@dataclass
class StrategyCfg:
    lookback: int = 300
    min_samples: int = 60
    z_entry: float = 3.0
    exit_z: float = 0.5
    min_net_edge_bps: float = 1.0       # MinimumRequiredEdge: applies AFTER the risk buffer is subtracted
    baseline: str = "robust"            # "robust" = median / 1.4826*MAD, "meanstd" = mean / std
    exit_cost_percentile: float = 0.75  # expected exit spread = this percentile of recent history (or today's, if worse)
    risk_buffer_bps: float = 2.0        # floor for the model-error allowance
    risk_buffer_max_bps: float = 30.0   # cap on the adaptive buffer
    adaptive_buffer: bool = True        # buffer = max(floor, p95 of (predicted - realised) over recent trades)
    adaptive_min_trades: int = 10
    adaptive_percentile: float = 0.95
    expected_hold_s: float = 300.0
    min_hold_s: float = 5.0
    max_hold_s: float = 900.0
    stop_loss_bps: float = 25.0
    max_abs_basis_bps: float = 300.0
    max_mark_index_bps: float = 50.0
    min_entry_basis_bps: float = 0.0
    stale_force_exit_s: float = 30.0
    std_floor_bps: float = 0.5


@dataclass
class RiskCfg:
    trade_notional_usd: float = 200.0
    min_trade_usd: float = 25.0
    min_touch_depth_usd: float = 300.0  # skip books whose touch holds less than this on either side (thin = fake signals)
    depth_fraction: float = 0.5
    use_depth_sizing: bool = True       # walk the real order book and pick the largest size that still has edge
    size_steps: list[float] = field(default_factory=lambda: [1.0, 0.75, 0.5, 0.35, 0.2])
    max_open_positions: int = 5
    max_total_notional_usd: float = 1000.0
    symbol_cooldown_s: float = 120.0
    daily_loss_limit_usd: float = 25.0
    max_consecutive_errors: int = 3
    kill_file: str = "KILL"


@dataclass
class ExecutionCfg:
    mode: str = "paper"
    env: str = "mainnet"
    leverage: int = 2


@dataclass
class ServerCfg:
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass
class RecorderCfg:
    """Records live market data (gzip JSONL) so strategies and slippage can be backtested later."""
    enabled: bool = True              # real feeds only; the simulator is never recorded
    dir: str = "data/recordings"
    depth_every_s: float = 1.0        # one order-book sample per this many seconds, round-robin over all markets
    max_mb: int = 2000                # oldest recordings are deleted beyond this


@dataclass
class PublisherCfg:
    """Pushes a read-only snapshot to Upstash Redis for the hosted (Vercel) dashboard.
    Active only when DASHBOARD_PUSH_URL and DASHBOARD_PUSH_TOKEN are set in the environment."""
    enabled: bool = True
    interval_s: float = 10.0
    key: str = "arbbot:snapshot"
    ttl_s: int = 120                  # a dead bot disappears from the page after this long


@dataclass
class AlertsCfg:
    """Telegram alerts. Active only when TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set in the environment."""
    enabled: bool = True
    notify_signals: bool = True
    notify_trades: bool = True
    notify_halts: bool = True
    notify_feed: bool = True
    commands: bool = True
    poll_timeout_s: float = 25.0
    signal_cooldown_s: float = 300.0
    feed_debounce_s: float = 10.0
    min_interval_s: float = 1.1
    queue_size: int = 50
    api_base: str = "https://api.telegram.org"


@dataclass
class Config:
    universe: UniverseCfg = field(default_factory=UniverseCfg)
    feed: FeedCfg = field(default_factory=FeedCfg)
    fees: FeesCfg = field(default_factory=FeesCfg)
    strategy: StrategyCfg = field(default_factory=StrategyCfg)
    risk: RiskCfg = field(default_factory=RiskCfg)
    execution: ExecutionCfg = field(default_factory=ExecutionCfg)
    server: ServerCfg = field(default_factory=ServerCfg)
    alerts: AlertsCfg = field(default_factory=AlertsCfg)
    recorder: RecorderCfg = field(default_factory=RecorderCfg)
    publisher: PublisherCfg = field(default_factory=PublisherCfg)

    def validate(self) -> None:
        if self.feed.mode not in ("ws", "rest", "sim"):
            raise ValueError(f"feed.mode must be 'ws', 'rest' or 'sim', got {self.feed.mode!r}")
        if self.execution.mode not in ("paper", "live"):
            raise ValueError(f"execution.mode must be 'paper' or 'live', got {self.execution.mode!r}")
        if self.execution.env not in ("mainnet", "testnet"):
            raise ValueError(f"execution.env must be 'mainnet' or 'testnet', got {self.execution.env!r}")
        if self.strategy.baseline not in ("robust", "meanstd"):
            raise ValueError(f"strategy.baseline must be 'robust' or 'meanstd', got {self.strategy.baseline!r}")
        if not 0 < self.strategy.exit_cost_percentile <= 1:
            raise ValueError("strategy.exit_cost_percentile must be in (0, 1]")
        if self.feed.interval_s < 0.05:
            raise ValueError("feed.interval_s must be >= 0.05")
        if self.strategy.min_samples > self.strategy.lookback:
            raise ValueError("strategy.min_samples cannot exceed strategy.lookback")
        if self.risk.trade_notional_usd > self.risk.max_total_notional_usd:
            raise ValueError("risk.trade_notional_usd cannot exceed risk.max_total_notional_usd")


_SECTIONS = {
    "universe": UniverseCfg, "feed": FeedCfg, "fees": FeesCfg, "strategy": StrategyCfg,
    "risk": RiskCfg, "execution": ExecutionCfg, "server": ServerCfg, "alerts": AlertsCfg,
    "recorder": RecorderCfg, "publisher": PublisherCfg,
}


def _build(cls, section: str, data: dict):
    unknown = set(data) - {f.name for f in fields(cls)}
    if unknown:
        raise ValueError(f"[{section}] unknown config key(s): {', '.join(sorted(unknown))}")
    return cls(**data)


def load(path: str | Path | None = None) -> Config:
    raw: dict = {}
    if path is not None and Path(path).exists():
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)
    unknown = set(raw) - set(_SECTIONS)
    if unknown:
        raise ValueError(f"unknown config section(s): {', '.join(sorted(unknown))}")
    cfg = Config(**{name: _build(cls, name, raw.get(name, {})) for name, cls in _SECTIONS.items()})
    cfg.validate()
    return cfg
