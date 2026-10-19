"""Pre-trade risk gate and circuit breakers.

Halt sources (any one blocks new entries):
  manual  - dashboard / --no-trade; clearable from the dashboard
  kill    - KILL file exists; also flattens; clears when the file is removed
  loss    - UTC-day realised loss limit; also flattens; clears at the next UTC day
  fault   - repeated execution errors or an unhedged leg; needs a restart (fail closed)
"""
import os

from .config import RiskCfg
from .models import Signal
from .portfolio import Portfolio


class Risk:
    def __init__(self, cfg: RiskCfg, portfolio: Portfolio):
        self.cfg = cfg
        self.pf = portfolio
        self.manual_halt = False
        self.fault = ""
        self.kill_active = False
        self.loss_active = False
        self.consecutive_errors = 0
        self._cooldown: dict[str, float] = {}

    @property
    def halted(self) -> bool:
        return bool(self.manual_halt or self.fault or self.kill_active or self.loss_active)

    @property
    def halt_reason(self) -> str:
        if self.fault:
            return f"FAULT: {self.fault}"
        if self.kill_active:
            return f"kill file '{self.cfg.kill_file}' present"
        if self.loss_active:
            return f"daily loss limit ${self.cfg.daily_loss_limit_usd:.2f} reached"
        if self.manual_halt:
            return "paused manually"
        return ""

    @property
    def flatten_required(self) -> bool:
        return self.kill_active or self.loss_active

    def refresh(self, now: float) -> None:
        self.kill_active = os.path.exists(self.cfg.kill_file)
        self.loss_active = self.pf.day_pnl(now) <= -abs(self.cfg.daily_loss_limit_usd)

    def size_entry(self, sig: Signal, now: float) -> tuple[float, str]:
        """Return (notional_usd, reason). Notional 0 means blocked, and reason says why."""
        c = self.cfg
        if self.halted:
            return 0.0, self.halt_reason
        if sig.symbol in self.pf.open:
            return 0.0, "already open"
        if len(self.pf.open) >= c.max_open_positions:
            return 0.0, "max open positions"
        if now < self._cooldown.get(sig.symbol, 0.0):
            return 0.0, "symbol cooldown"
        if sig.fillable_usd < c.min_touch_depth_usd:
            return 0.0, f"thin touch (${sig.fillable_usd:.0f} < ${c.min_touch_depth_usd:.0f})"
        room = c.max_total_notional_usd - self.pf.open_notional()
        notional = min(c.trade_notional_usd, sig.fillable_usd * c.depth_fraction, room)
        if notional < c.min_trade_usd:
            return 0.0, "size below minimum (depth or exposure cap)"
        return notional, ""

    def note_exit(self, symbol: str, now: float) -> None:
        self._cooldown[symbol] = now + self.cfg.symbol_cooldown_s

    def record_success(self) -> None:
        self.consecutive_errors = 0

    def record_error(self) -> bool:
        """Count an execution error. Returns True if this tripped the fault breaker."""
        self.consecutive_errors += 1
        if self.consecutive_errors >= self.cfg.max_consecutive_errors and not self.fault:
            self.fault = f"{self.consecutive_errors} consecutive execution errors"
            return True
        return False

    def trip_fault(self, reason: str) -> None:
        self.fault = reason
