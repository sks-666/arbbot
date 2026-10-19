"""Launcher that works from any directory: python /path/to/binance-arb-bot/run.py [args]"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from arbbot.main import main  # noqa: E402

main()
