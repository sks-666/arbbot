"""Launcher: python backtest.py info|slippage|replay [FILES...]  (see arbbot/backtest.py)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from arbbot.backtest import main  # noqa: E402

sys.exit(main())
