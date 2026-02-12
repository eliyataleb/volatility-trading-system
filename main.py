"""Main runner for a constrained delta-hedged volatility strategy replay."""

from __future__ import annotations

import argparse
import csv
import math
import os
from datetime import datetime
from typing import Dict, List

from analytics.drawdown import DrawdownTracker
from analytics.exposure import compute_exposures
from analytics.pnl import PnLBreakdown
from analytics.plots import generate_plots
from execution.hedge import rebalance_delta_hedge
from execution.trades import execute_option_trade
from risk.kill_switch import KillSwitch
from risk.limits import RiskLimits
from strategy.position_sizing import target_option_contracts
from strategy.position_sizing import target_contracts_by_vega_budget
from strategy.signal import RegimeSignalEngine
from strategy.signal import SignalConfig


INITIAL_CAPITAL = 10_000.0
DATA_DIR = "data"
RESULTS_DIR = "results"
DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M"
DEFAULT_SIGNAL_CONFIG = SignalConfig()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run delta-hedged volatility replay with risk controls."
    )
    parser.add_argument(
        "--symbol",
        default="SPY",
        help="Ticker symbol for dataset selection (default: SPY).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Dataset year (default: 2025).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive start timestamp in YYYY-MM-DD or YYYY-MM-DD HH:MM.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end timestamp in YYYY-MM-DD or YYYY-MM-DD HH:MM.",
    )
    parser.add_argument(
        "--gamma-green-threshold",
        type=float,
        default=5.0,
        help="G1 threshold for green gamma zone (default: 5.0).",
    )
    parser.add_argument(
        "--gamma-red-threshold",
        type=float,
        default=10.0,
        help="G2 threshold for red gamma zone (default: 10.0).",
    )
    parser.add_argument(
        "--gamma-yellow-size-factor",
        type=float,
        default=0.50,
        help="Size multiplier in yellow zone (default: 0.50).",
    )
    parser.add_argument(
        "--gamma-red-size-factor",
        type=float,
        default=0.25,
        help="Size multiplier in red zone (default: 0.25).",
    )
    parser.add_argument(
        "--gamma-kill-drawdown-threshold",
        type=float,
        default=0.12,
        help=(
            "D1 drawdown threshold for flatten kill in red zone "
            "(default: 0.12 for 12%%)."
        ),
    )
    parser.add_argument(
        "--max-leverage",
        type=float,
        default=6.0,
        help="Global maximum leverage limit (default: 6.0).",
    )
    parser.add_argument(
        "--max-abs-gamma",
        type=float,
        default=75.0,
        help="Hard cap on absolute gamma exposure (default: 75.0).",
    )
    parser.add_argument(
        "--max-abs-vega",
        type=float,
        default=300.0,
        help="Hard cap on absolute vega exposure (default: 300.0).",
    )
    parser.add_argument(
        "--long-pause-drawdown-threshold",
        type=float,
        default=0.10,
        help="Long-vol pause trigger drawdown threshold (default: 0.10).",
    )
    parser.add_argument(
        "--long-pause-bars",
        type=int,
        default=30,
        help="Bars to stay paused after long-vol regime exit (default: 30).",
    )
    parser.add_argument(
        "--long-resume-confirm-bars",
        type=int,
        default=3,
        help="Consecutive valid bars required to resume long-vol after pause (default: 3).",
    )
    parser.add_argument(
        "--long-catastrophic-kill-threshold",
        type=float,
        default=0.40,
        help="Hard-kill drawdown threshold for long-vol (default: 0.40).",
    )
    parser.add_argument(
        "--long-min-hold-bars",
        type=int,
        default=20,
        help="Minimum bars to hold long-vol unless hard regime flip occurs (default: 20).",
    )
    parser.add_argument(
        "--long-derisk-factor",
        type=float,
        default=0.50,
        help="When long regime fails in high gamma risk, reduce to this fraction before flat (default: 0.50).",
    )
    parser.add_argument(
        "--long-hard-flip-edge-threshold",
        type=float,
        default=0.0,
        help="Long hard-flip condition uses edge >= threshold (default: 0.0).",
    )
    parser.add_argument(
        "--global-drawdown-throttle-threshold",
        type=float,
        default=0.10,
        help="Throttle size when drawdown reaches this level, independent of gamma (default: 0.10).",
    )
    parser.add_argument(
        "--global-drawdown-throttle-size-factor",
        type=float,
        default=0.50,
        help="Size multiplier applied after global drawdown throttle is active (default: 0.50).",
    )
    parser.add_argument(
        "--global-drawdown-kill-threshold",
        type=float,
        default=0.20,
        help="Flatten positions when drawdown reaches this level, independent of gamma (default: 0.20).",
    )
    parser.add_argument(
        "--strategy-mode",
        choices=["short", "long", "both", "adaptive"],
        default="both",
        help="Run short-vol, long-vol, both, or adaptive regime mode (default: both).",
    )
    parser.add_argument(
        "--adaptive-enter-persist-bars",
        type=int,
        default=3,
        help="Consecutive bars required to confirm adaptive regime entry (default: 3).",
    )
    parser.add_argument(
        "--adaptive-exit-persist-bars",
        type=int,
        default=2,
        help="Consecutive bars required to confirm adaptive regime exit (default: 2).",
    )
    parser.add_argument(
        "--adaptive-pause-bars",
        type=int,
        default=30,
        help="Bars to stay paused after adaptive exit or risk pause (default: 30).",
    )
    parser.add_argument(
        "--adaptive-short-edge-enter",
        type=float,
        default=0.02,
        help="Adaptive short entry edge threshold E (IV-RV_short > E).",
    )
    parser.add_argument(
        "--adaptive-short-edge-exit",
        type=float,
        default=0.01,
        help="Adaptive short exit threshold E_exit (IV-RV_short < E_exit).",
    )
    parser.add_argument(
        "--adaptive-short-trend-enter",
        type=float,
        default=0.004,
        help="Adaptive short entry trend threshold T_low (trend < T_low).",
    )
    parser.add_argument(
        "--adaptive-short-trend-exit",
        type=float,
        default=0.006,
        help="Adaptive short exit trend threshold T_high (trend > T_high).",
    )
    parser.add_argument(
        "--adaptive-vov-low",
        type=float,
        default=0.003,
        help="Adaptive low vol-of-vol threshold V_low for short entry (default: 0.003).",
    )
    parser.add_argument(
        "--adaptive-vov-high",
        type=float,
        default=0.006,
        help="Adaptive high vol-of-vol threshold V_high for short exit/long entry (default: 0.006).",
    )
    parser.add_argument(
        "--adaptive-vov-exit",
        type=float,
        default=0.004,
        help="Adaptive long exit vol-of-vol threshold V_exit (default: 0.004).",
    )
    parser.add_argument(
        "--adaptive-long-cheapness-enter",
        type=float,
        default=0.003,
        help="Adaptive long entry cheapness C (RV_short-IV > C). Can be negative.",
    )
    parser.add_argument(
        "--adaptive-long-cheapness-exit",
        type=float,
        default=0.0015,
        help="Adaptive long exit cheapness C_exit (RV_short-IV < C_exit). Can be negative.",
    )
    parser.add_argument(
        "--adaptive-long-trend-max",
        type=float,
        default=0.008,
        help="Adaptive long trend cap T_max (trend < T_max).",
    )
    parser.add_argument(
        "--adaptive-confidence-buffer",
        type=float,
        default=0.001,
        help=(
            "When both adaptive regimes qualify, require this minimum score gap "
            "before selecting a side (default: 0.001)."
        ),
    )
    parser.add_argument(
        "--rv-short-window",
        type=int,
        default=30,
        help="Short realized-vol rolling window (default: 30 bars).",
    )
    parser.add_argument(
        "--rv-medium-window",
        type=int,
        default=240,
        help="Medium realized-vol rolling window (default: 240 bars).",
    )
    parser.add_argument(
        "--trend-window",
        type=int,
        default=120,
        help="Trend filter rolling window (default: 120 bars).",
    )
    parser.add_argument(
        "--long-chop-window",
        type=int,
        default=30,
        help="Choppiness rolling window for long-vol filter (default: 30 bars).",
    )
    parser.add_argument(
        "--short-edge-threshold",
        type=float,
        default=0.02,
        help="Minimum IV-RV edge for short-vol entries (default: 0.02).",
    )
    parser.add_argument(
        "--short-edge-collapse-tolerance",
        type=float,
        default=0.005,
        help="Max allowed edge deterioration per bar for short-vol gate (default: 0.005).",
    )
    parser.add_argument(
        "--short-trend-threshold",
        type=float,
        default=0.004,
        help="Max trend strength allowed for short-vol (default: 0.004).",
    )
    parser.add_argument(
        "--short-jump-threshold",
        type=float,
        default=0.006,
        help="Max absolute 1-bar return allowed for short-vol (default: 0.006).",
    )
    parser.add_argument(
        "--short-rv-change-threshold",
        type=float,
        default=0.06,
        help="Max |RV short - RV medium| allowed for short-vol (default: 0.06).",
    )
    parser.add_argument(
        "--long-edge-threshold",
        type=float,
        default=0.015,
        help="Minimum RV-IV cheapness for long-vol entries (default: 0.015).",
    )
    parser.add_argument(
        "--long-max-iv-premium",
        type=float,
        default=0.03,
        help=(
            "Alternative long-vol pricing gate: allow entry when IV-RV is below this "
            "premium cap even if IV is not strictly below RV (default: 0.03)."
        ),
    )
    parser.add_argument(
        "--long-edge-collapse-tolerance",
        type=float,
        default=0.005,
        help="Max allowed IV-RV rebound per bar for long-vol gate (default: 0.005).",
    )
    parser.add_argument(
        "--long-rv-rise-threshold",
        type=float,
        default=0.003,
        help="Minimum RV-short minus RV-medium for long-vol instability filter (default: 0.003).",
    )
    parser.add_argument(
        "--long-chop-threshold",
        type=float,
        default=0.00025,
        help="Minimum rolling abs-return choppiness for long-vol instability filter (default: 0.00025).",
    )
    parser.add_argument(
        "--long-two-way-chop-score-threshold",
        type=float,
        default=2.0,
        help="Minimum chop-score for two-way long-vol filter (default: 2.0).",
    )
    parser.add_argument(
        "--long-two-way-trend-threshold",
        type=float,
        default=0.008,
        help="Alternative two-way pass: trend_strength <= threshold (default: 0.008).",
    )
    parser.add_argument(
        "--long-vega-budget-ratio",
        type=float,
        default=0.015,
        help="Long-vol sizing uses target_abs_vega = equity * ratio (default: 0.015).",
    )
    parser.add_argument(
        "--cooldown-bars",
        type=int,
        default=30,
        help="Bars to stay FLAT after a strategy exit (default: 30).",
    )
    parser.add_argument(
        "--skip-plots",
        action="store_true",
        help="Skip plot generation for faster runs.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Print progress every N bars (0 disables progress prints).",
    )
    return parser.parse_args()


def resolve_input_paths(
    symbol: str,
    year: int,
) -> tuple[str, str]:
    normalized = symbol.strip().lower()
    prices_path = os.path.join(DATA_DIR, f"{normalized}_{year}_prices.csv")
    options_path = os.path.join(DATA_DIR, f"{normalized}_{year}_options.csv")

    missing = [path for path in (prices_path, options_path) if not os.path.exists(path)]
    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(
            f"Missing dataset file(s): {joined}. "
            "Generate them first with: "
            f"python data/generate_real_data.py --symbol {symbol.upper()} --year {year}"
        )

    return prices_path, options_path


def parse_timestamp(timestamp_text: str) -> datetime:
    for fmt in (DATETIME_FMT, DATE_FMT):
        try:
            return datetime.strptime(timestamp_text, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid timestamp '{timestamp_text}'. "
        f"Use '{DATE_FMT}' or '{DATETIME_FMT}'."
    )


def parse_user_boundary(date_text: str | None, *, is_end: bool) -> datetime | None:
    if not date_text:
        return None
    try:
        return datetime.strptime(date_text, DATETIME_FMT)
    except ValueError:
        boundary = datetime.strptime(date_text, DATE_FMT)
        if is_end:
            return boundary.replace(hour=23, minute=59, second=59, microsecond=999999)
        return boundary


def load_prices(path: str) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                {
                    "date": row["date"],
                    "close": float(row["close"]),
                    "realized_vol": float(row["realized_vol"]),
                }
            )
    return rows


def load_options(path: str) -> Dict[str, Dict[str, float]]:
    rows: Dict[str, Dict[str, float]] = {}
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows[row["date"]] = {
                "option_mid": float(row["option_mid"]),
                "iv": float(row["iv"]),
                "delta": float(row["delta"]),
                "gamma": float(row["gamma"]),
                "vega": float(row["vega"]),
                "expiry_days": int(row["expiry_days"]),
            }
    return rows


def merge_market_data(
    prices: List[Dict[str, float]],
    options_by_date: Dict[str, Dict[str, float]],
) -> List[Dict[str, float]]:
    merged: List[Dict[str, float]] = []
    for row in prices:
        date = row["date"]
        if date not in options_by_date:
            continue
        merged_row: Dict[str, float] = {**row, **options_by_date[date]}
        merged.append(merged_row)

    merged.sort(key=lambda item: parse_timestamp(item["date"]))
    return merged


def filter_market_data_by_period(
    market_data: List[Dict[str, float]],
    start_date: str | None,
    end_date: str | None,
) -> List[Dict[str, float]]:
    start_dt = parse_user_boundary(start_date, is_end=False)
    end_dt = parse_user_boundary(end_date, is_end=True)

    filtered: List[Dict[str, float]] = []
    for row in market_data:
        dt = parse_timestamp(row["date"])
        if start_dt and dt < start_dt:
            continue
        if end_dt and dt > end_dt:
            continue
        filtered.append(row)
    return filtered


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def output_path(name: str, mode: str, multi_mode: bool, ext: str) -> str:
    suffix = f"_{mode}" if multi_mode else ""
    return os.path.join(RESULTS_DIR, f"{name}{suffix}.{ext}")


def signal_config_from_args(args: argparse.Namespace) -> SignalConfig:
    return SignalConfig(
        rv_short_window=args.rv_short_window,
        rv_medium_window=args.rv_medium_window,
        trend_window=args.trend_window,
        long_chop_window=args.long_chop_window,
        short_edge_threshold=args.short_edge_threshold,
        short_edge_collapse_tolerance=args.short_edge_collapse_tolerance,
        short_trend_threshold=args.short_trend_threshold,
        short_jump_threshold=args.short_jump_threshold,
        short_rv_change_threshold=args.short_rv_change_threshold,
        long_edge_threshold=args.long_edge_threshold,
        long_max_iv_premium=args.long_max_iv_premium,
        long_edge_collapse_tolerance=args.long_edge_collapse_tolerance,
        long_rv_rise_threshold=args.long_rv_rise_threshold,
        long_chop_threshold=args.long_chop_threshold,
        long_two_way_chop_score_threshold=args.long_two_way_chop_score_threshold,
        long_two_way_trend_threshold=args.long_two_way_trend_threshold,
        cooldown_bars=args.cooldown_bars,
        adaptive_enter_persist_bars=args.adaptive_enter_persist_bars,
        adaptive_exit_persist_bars=args.adaptive_exit_persist_bars,
        adaptive_pause_bars=args.adaptive_pause_bars,
        adaptive_short_edge_enter=args.adaptive_short_edge_enter,
        adaptive_short_edge_exit=args.adaptive_short_edge_exit,
        adaptive_short_trend_enter=args.adaptive_short_trend_enter,
        adaptive_short_trend_exit=args.adaptive_short_trend_exit,
        adaptive_vov_low=args.adaptive_vov_low,
        adaptive_vov_high=args.adaptive_vov_high,
        adaptive_vov_exit=args.adaptive_vov_exit,
        adaptive_long_cheapness_enter=args.adaptive_long_cheapness_enter,
        adaptive_long_cheapness_exit=args.adaptive_long_cheapness_exit,
        adaptive_long_trend_max=args.adaptive_long_trend_max,
        adaptive_confidence_buffer=args.adaptive_confidence_buffer,
    )


def infer_data_granularity(market_data: List[Dict[str, float]]) -> str:
    if not market_data:
        return "intraday"
    if any(" " in str(row["date"]) for row in market_data[: min(100, len(market_data))]):
        return "intraday"
    return "daily"


def signal_args_match_defaults(args: argparse.Namespace) -> bool:
    return (
        args.rv_short_window == DEFAULT_SIGNAL_CONFIG.rv_short_window
        and args.rv_medium_window == DEFAULT_SIGNAL_CONFIG.rv_medium_window
        and args.trend_window == DEFAULT_SIGNAL_CONFIG.trend_window
        and args.long_chop_window == DEFAULT_SIGNAL_CONFIG.long_chop_window
        and args.short_edge_threshold == DEFAULT_SIGNAL_CONFIG.short_edge_threshold
        and args.short_edge_collapse_tolerance
        == DEFAULT_SIGNAL_CONFIG.short_edge_collapse_tolerance
        and args.short_trend_threshold == DEFAULT_SIGNAL_CONFIG.short_trend_threshold
        and args.short_jump_threshold == DEFAULT_SIGNAL_CONFIG.short_jump_threshold
        and args.short_rv_change_threshold
        == DEFAULT_SIGNAL_CONFIG.short_rv_change_threshold
        and args.long_edge_threshold == DEFAULT_SIGNAL_CONFIG.long_edge_threshold
        and args.long_max_iv_premium == DEFAULT_SIGNAL_CONFIG.long_max_iv_premium
        and args.long_edge_collapse_tolerance
        == DEFAULT_SIGNAL_CONFIG.long_edge_collapse_tolerance
        and args.long_rv_rise_threshold == DEFAULT_SIGNAL_CONFIG.long_rv_rise_threshold
        and args.long_chop_threshold == DEFAULT_SIGNAL_CONFIG.long_chop_threshold
        and args.long_two_way_chop_score_threshold
        == DEFAULT_SIGNAL_CONFIG.long_two_way_chop_score_threshold
        and args.long_two_way_trend_threshold
        == DEFAULT_SIGNAL_CONFIG.long_two_way_trend_threshold
        and args.cooldown_bars == DEFAULT_SIGNAL_CONFIG.cooldown_bars
        and args.adaptive_enter_persist_bars
        == DEFAULT_SIGNAL_CONFIG.adaptive_enter_persist_bars
        and args.adaptive_exit_persist_bars
        == DEFAULT_SIGNAL_CONFIG.adaptive_exit_persist_bars
        and args.adaptive_pause_bars == DEFAULT_SIGNAL_CONFIG.adaptive_pause_bars
        and args.adaptive_short_edge_enter == DEFAULT_SIGNAL_CONFIG.adaptive_short_edge_enter
        and args.adaptive_short_edge_exit == DEFAULT_SIGNAL_CONFIG.adaptive_short_edge_exit
        and args.adaptive_short_trend_enter == DEFAULT_SIGNAL_CONFIG.adaptive_short_trend_enter
        and args.adaptive_short_trend_exit == DEFAULT_SIGNAL_CONFIG.adaptive_short_trend_exit
        and args.adaptive_vov_low == DEFAULT_SIGNAL_CONFIG.adaptive_vov_low
        and args.adaptive_vov_high == DEFAULT_SIGNAL_CONFIG.adaptive_vov_high
        and args.adaptive_vov_exit == DEFAULT_SIGNAL_CONFIG.adaptive_vov_exit
        and args.adaptive_long_cheapness_enter
        == DEFAULT_SIGNAL_CONFIG.adaptive_long_cheapness_enter
        and args.adaptive_long_cheapness_exit
        == DEFAULT_SIGNAL_CONFIG.adaptive_long_cheapness_exit
        and args.adaptive_long_trend_max == DEFAULT_SIGNAL_CONFIG.adaptive_long_trend_max
        and args.adaptive_confidence_buffer == DEFAULT_SIGNAL_CONFIG.adaptive_confidence_buffer
    )


def daily_signal_preset() -> SignalConfig:
    return SignalConfig(
        rv_short_window=5,
        rv_medium_window=20,
        trend_window=20,
        long_chop_window=10,
        short_edge_threshold=0.005,
        short_edge_collapse_tolerance=0.02,
        short_trend_threshold=0.03,
        short_jump_threshold=0.03,
        short_rv_change_threshold=0.20,
        long_edge_threshold=0.003,
        long_max_iv_premium=0.02,
        long_edge_collapse_tolerance=0.02,
        long_rv_rise_threshold=0.003,
        long_chop_threshold=0.004,
        long_two_way_chop_score_threshold=2.0,
        long_two_way_trend_threshold=0.03,
        cooldown_bars=3,
        adaptive_enter_persist_bars=2,
        adaptive_exit_persist_bars=2,
        adaptive_pause_bars=2,
        adaptive_short_edge_enter=0.02,
        adaptive_short_edge_exit=0.0,
        adaptive_short_trend_enter=0.015,
        adaptive_short_trend_exit=0.03,
        adaptive_vov_low=0.01,
        adaptive_vov_high=0.02,
        adaptive_vov_exit=0.015,
        adaptive_long_cheapness_enter=-0.04,
        adaptive_long_cheapness_exit=-0.04,
        adaptive_long_trend_max=0.05,
        adaptive_confidence_buffer=0.0,
    )


def daily_signal_with_user_overrides(args: argparse.Namespace) -> SignalConfig:
    """Use daily preset as baseline, then apply user overrides away from global defaults."""
    preset = daily_signal_preset()
    defaults = DEFAULT_SIGNAL_CONFIG
    return SignalConfig(
        rv_short_window=(
            args.rv_short_window
            if args.rv_short_window != defaults.rv_short_window
            else preset.rv_short_window
        ),
        rv_medium_window=(
            args.rv_medium_window
            if args.rv_medium_window != defaults.rv_medium_window
            else preset.rv_medium_window
        ),
        trend_window=(
            args.trend_window if args.trend_window != defaults.trend_window else preset.trend_window
        ),
        long_chop_window=(
            args.long_chop_window
            if args.long_chop_window != defaults.long_chop_window
            else preset.long_chop_window
        ),
        short_edge_threshold=(
            args.short_edge_threshold
            if args.short_edge_threshold != defaults.short_edge_threshold
            else preset.short_edge_threshold
        ),
        short_edge_collapse_tolerance=(
            args.short_edge_collapse_tolerance
            if args.short_edge_collapse_tolerance != defaults.short_edge_collapse_tolerance
            else preset.short_edge_collapse_tolerance
        ),
        short_trend_threshold=(
            args.short_trend_threshold
            if args.short_trend_threshold != defaults.short_trend_threshold
            else preset.short_trend_threshold
        ),
        short_jump_threshold=(
            args.short_jump_threshold
            if args.short_jump_threshold != defaults.short_jump_threshold
            else preset.short_jump_threshold
        ),
        short_rv_change_threshold=(
            args.short_rv_change_threshold
            if args.short_rv_change_threshold != defaults.short_rv_change_threshold
            else preset.short_rv_change_threshold
        ),
        long_edge_threshold=(
            args.long_edge_threshold
            if args.long_edge_threshold != defaults.long_edge_threshold
            else preset.long_edge_threshold
        ),
        long_max_iv_premium=(
            args.long_max_iv_premium
            if args.long_max_iv_premium != defaults.long_max_iv_premium
            else preset.long_max_iv_premium
        ),
        long_edge_collapse_tolerance=(
            args.long_edge_collapse_tolerance
            if args.long_edge_collapse_tolerance != defaults.long_edge_collapse_tolerance
            else preset.long_edge_collapse_tolerance
        ),
        long_rv_rise_threshold=(
            args.long_rv_rise_threshold
            if args.long_rv_rise_threshold != defaults.long_rv_rise_threshold
            else preset.long_rv_rise_threshold
        ),
        long_chop_threshold=(
            args.long_chop_threshold
            if args.long_chop_threshold != defaults.long_chop_threshold
            else preset.long_chop_threshold
        ),
        long_two_way_chop_score_threshold=(
            args.long_two_way_chop_score_threshold
            if args.long_two_way_chop_score_threshold != defaults.long_two_way_chop_score_threshold
            else preset.long_two_way_chop_score_threshold
        ),
        long_two_way_trend_threshold=(
            args.long_two_way_trend_threshold
            if args.long_two_way_trend_threshold != defaults.long_two_way_trend_threshold
            else preset.long_two_way_trend_threshold
        ),
        cooldown_bars=(
            args.cooldown_bars if args.cooldown_bars != defaults.cooldown_bars else preset.cooldown_bars
        ),
        adaptive_enter_persist_bars=(
            args.adaptive_enter_persist_bars
            if args.adaptive_enter_persist_bars != defaults.adaptive_enter_persist_bars
            else preset.adaptive_enter_persist_bars
        ),
        adaptive_exit_persist_bars=(
            args.adaptive_exit_persist_bars
            if args.adaptive_exit_persist_bars != defaults.adaptive_exit_persist_bars
            else preset.adaptive_exit_persist_bars
        ),
        adaptive_pause_bars=(
            args.adaptive_pause_bars
            if args.adaptive_pause_bars != defaults.adaptive_pause_bars
            else preset.adaptive_pause_bars
        ),
        adaptive_short_edge_enter=(
            args.adaptive_short_edge_enter
            if args.adaptive_short_edge_enter != defaults.adaptive_short_edge_enter
            else preset.adaptive_short_edge_enter
        ),
        adaptive_short_edge_exit=(
            args.adaptive_short_edge_exit
            if args.adaptive_short_edge_exit != defaults.adaptive_short_edge_exit
            else preset.adaptive_short_edge_exit
        ),
        adaptive_short_trend_enter=(
            args.adaptive_short_trend_enter
            if args.adaptive_short_trend_enter != defaults.adaptive_short_trend_enter
            else preset.adaptive_short_trend_enter
        ),
        adaptive_short_trend_exit=(
            args.adaptive_short_trend_exit
            if args.adaptive_short_trend_exit != defaults.adaptive_short_trend_exit
            else preset.adaptive_short_trend_exit
        ),
        adaptive_vov_low=(
            args.adaptive_vov_low
            if args.adaptive_vov_low != defaults.adaptive_vov_low
            else preset.adaptive_vov_low
        ),
        adaptive_vov_high=(
            args.adaptive_vov_high
            if args.adaptive_vov_high != defaults.adaptive_vov_high
            else preset.adaptive_vov_high
        ),
        adaptive_vov_exit=(
            args.adaptive_vov_exit
            if args.adaptive_vov_exit != defaults.adaptive_vov_exit
            else preset.adaptive_vov_exit
        ),
        adaptive_long_cheapness_enter=(
            args.adaptive_long_cheapness_enter
            if args.adaptive_long_cheapness_enter != defaults.adaptive_long_cheapness_enter
            else preset.adaptive_long_cheapness_enter
        ),
        adaptive_long_cheapness_exit=(
            args.adaptive_long_cheapness_exit
            if args.adaptive_long_cheapness_exit != defaults.adaptive_long_cheapness_exit
            else preset.adaptive_long_cheapness_exit
        ),
        adaptive_long_trend_max=(
            args.adaptive_long_trend_max
            if args.adaptive_long_trend_max != defaults.adaptive_long_trend_max
            else preset.adaptive_long_trend_max
        ),
        adaptive_confidence_buffer=(
            args.adaptive_confidence_buffer
            if args.adaptive_confidence_buffer != defaults.adaptive_confidence_buffer
            else preset.adaptive_confidence_buffer
        ),
    )


def resolve_signal_config(
    args: argparse.Namespace,
    market_data: List[Dict[str, float]],
) -> SignalConfig:
    configured = signal_config_from_args(args)
    granularity = infer_data_granularity(market_data)
    if granularity != "daily":
        print("Detected intraday timestamps; using intraday/default signal parameters.")
        return configured

    if signal_args_match_defaults(args):
        print(
            "Detected daily timestamps with default signal settings; "
            "applying built-in daily signal preset."
        )
        return daily_signal_preset()

    print(
        "Detected daily timestamps; applying daily preset baseline with user overrides."
    )
    return daily_signal_with_user_overrides(args)


def gamma_risk_metric(gamma_exposure: float, spot_price: float) -> float:
    # Scale gamma by spot to make thresholds more comparable across underlyings.
    return abs(gamma_exposure) * max(spot_price, 0.0) * 0.0001


def evaluate_projected_trade(
    *,
    risk_limits: RiskLimits,
    projected_option_contracts: int,
    spot_price: float,
    option_price: float,
    option_delta: float,
    option_gamma: float,
    option_vega: float,
    projected_equity: float,
) -> tuple[bool, str, Dict[str, float]]:
    projected_hedge_shares = int(round(-projected_option_contracts * option_delta * 100.0))
    projected_exposure = compute_exposures(
        option_contracts=projected_option_contracts,
        hedge_shares=projected_hedge_shares,
        spot_price=spot_price,
        option_price=option_price,
        option_delta=option_delta,
        option_gamma=option_gamma,
        option_vega=option_vega,
    )
    trade_allowed, block_reason = risk_limits.trade_allowed(
        projected_option_contracts=projected_option_contracts,
        option_price=option_price,
        projected_notional=projected_exposure["notional_exposure"],
        projected_gamma_abs=abs(projected_exposure["gamma_exposure"]),
        projected_vega_abs=abs(projected_exposure["vega_exposure"]),
        projected_equity=projected_equity,
    )
    return trade_allowed, block_reason, projected_exposure


def clamp_target_to_risk_limits(
    *,
    desired_contracts: int,
    risk_limits: RiskLimits,
    spot_price: float,
    option_price: float,
    option_delta: float,
    option_gamma: float,
    option_vega: float,
    projected_equity: float,
) -> int:
    if desired_contracts == 0:
        return 0

    sign = 1 if desired_contracts > 0 else -1
    low = 0
    high = abs(desired_contracts)
    best_abs = 0

    while low <= high:
        mid = (low + high) // 2
        candidate = sign * mid
        trade_allowed, _, _ = evaluate_projected_trade(
            risk_limits=risk_limits,
            projected_option_contracts=candidate,
            spot_price=spot_price,
            option_price=option_price,
            option_delta=option_delta,
            option_gamma=option_gamma,
            option_vega=option_vega,
            projected_equity=projected_equity,
        )
        if trade_allowed:
            best_abs = mid
            low = mid + 1
        else:
            high = mid - 1

    return sign * best_abs


def run_single_mode(
    *,
    mode: str,
    symbol: str,
    year: int,
    market_data: List[Dict[str, float]],
    args: argparse.Namespace,
    signal_config: SignalConfig,
    multi_mode: bool,
    prices_path: str,
    options_path: str,
    skip_plots: bool,
    progress_every: int,
) -> Dict[str, object]:
    risk_limits = RiskLimits(
        initial_capital=INITIAL_CAPITAL,
        max_leverage=args.max_leverage,
        max_abs_gamma=args.max_abs_gamma,
        max_abs_vega=args.max_abs_vega,
    )
    kill_switch = KillSwitch(
        gamma_green_threshold=args.gamma_green_threshold,
        gamma_red_threshold=args.gamma_red_threshold,
        gamma_yellow_size_factor=args.gamma_yellow_size_factor,
        gamma_red_size_factor=args.gamma_red_size_factor,
        kill_drawdown_threshold=args.gamma_kill_drawdown_threshold,
    )
    signal_engine = RegimeSignalEngine(mode=mode, config=signal_config)
    drawdown = DrawdownTracker(peak_equity=INITIAL_CAPITAL)
    pnl = PnLBreakdown()

    cash = INITIAL_CAPITAL
    option_contracts = 0
    hedge_shares = 0

    prev_spot = None
    prev_option_mid = None
    prev_equity = INITIAL_CAPITAL
    prev_stance = "FLAT"

    spot_history: List[float] = []
    rv_history: List[float] = []
    return_history: List[float] = []

    step_rows: List[Dict[str, object]] = []
    equity_rows: List[Dict[str, object]] = []
    events: List[str] = []
    pending_target_contracts = 0
    first_long_trade_pending: Dict[str, object] | None = None
    first_long_trade_logged = False
    long_resume_confirm_count = 0
    long_bars_in_position = 0
    adaptive_short_pause_remaining = 0
    adaptive_long_pause_remaining = 0

    total_rows = len(market_data)
    print(f"Starting mode '{mode}' with {total_rows} bars...")

    for idx, row in enumerate(market_data, start=1):
        if adaptive_short_pause_remaining > 0:
            adaptive_short_pause_remaining -= 1
        if adaptive_long_pause_remaining > 0:
            adaptive_long_pause_remaining -= 1

        date = row["date"]
        spot = float(row["close"])
        realized_vol = float(row["realized_vol"])
        option_mid = float(row["option_mid"])
        iv = float(row["iv"])
        delta = float(row["delta"])
        gamma = float(row["gamma"])
        vega = float(row["vega"])

        if first_long_trade_pending is not None:
            entry_spot = float(first_long_trade_pending["entry_spot"])
            next_bar_return = 0.0 if entry_spot <= 0 else math.log(spot / entry_spot)
            diagnostic = (
                f"{date} LONG_DIAGNOSTIC "
                f"(entry_time={first_long_trade_pending['entry_time']}, "
                f"iv={first_long_trade_pending['iv']:.6f}, "
                f"rv_short={first_long_trade_pending['rv_short']:.6f}, "
                f"rv_medium={first_long_trade_pending['rv_medium']:.6f}, "
                f"trend_strength={first_long_trade_pending['trend_strength']:.6f}, "
                f"contracts={first_long_trade_pending['contracts']}, "
                f"delta={first_long_trade_pending['delta_exposure']:.6f}, "
                f"gamma={first_long_trade_pending['gamma_exposure']:.6f}, "
                f"vega={first_long_trade_pending['vega_exposure']:.6f}, "
                f"next_bar_return={next_bar_return:.6f})"
            )
            print(f"[{mode}] {diagnostic}")
            events.append(diagnostic)
            first_long_trade_pending = None
            first_long_trade_logged = True

        if prev_spot is not None and prev_spot > 0:
            return_history.append(math.log(spot / prev_spot))
        spot_history.append(spot)
        rv_history.append(realized_vol)

        opening_option_contracts = option_contracts
        opening_hedge_shares = hedge_shares

        option_mtm = 0.0
        hedge_mtm = 0.0
        if prev_spot is not None and prev_option_mid is not None:
            option_mtm = opening_option_contracts * 100.0 * (option_mid - prev_option_mid)
            hedge_mtm = opening_hedge_shares * (spot - prev_spot)
        pnl.record_mtm(option_mtm, hedge_mtm)

        equity_before_trade = (
            cash
            + opening_option_contracts * 100.0 * option_mid
            + opening_hedge_shares * spot
        )
        daily_return = (
            0.0
            if prev_equity <= 0
            else (equity_before_trade - prev_equity) / prev_equity
        )
        drawdown_before_trade = drawdown.peek(equity_before_trade)

        exposures_before = compute_exposures(
            option_contracts=opening_option_contracts,
            hedge_shares=opening_hedge_shares,
            spot_price=spot,
            option_price=option_mid,
            option_delta=delta,
            option_gamma=gamma,
            option_vega=vega,
        )

        gamma_risk = gamma_risk_metric(exposures_before["gamma_exposure"], spot)

        kill_action = kill_switch.evaluate(
            gamma_risk=gamma_risk,
            total_drawdown=drawdown_before_trade,
        )
        risk_reasons: List[str] = []
        effective_size_factor = 1.0
        flatten_for_risk = False
        adaptive_pause_side = ""
        adaptive_pause_requires_invalid = False
        adaptive_pause_reason = ""

        if mode == "short":
            risk_reasons = list(kill_action.reasons)
            effective_size_factor = kill_action.size_factor
            flatten_for_risk = kill_action.flatten_positions

            if drawdown_before_trade >= args.global_drawdown_kill_threshold:
                flatten_for_risk = True
                if "GLOBAL_DRAWDOWN_KILL" not in risk_reasons:
                    risk_reasons.append("GLOBAL_DRAWDOWN_KILL")
            elif drawdown_before_trade >= args.global_drawdown_throttle_threshold:
                effective_size_factor *= args.global_drawdown_throttle_size_factor
                risk_reasons.append("GLOBAL_DRAWDOWN_THROTTLE")
        elif mode == "long":
            long_has_exposure = opening_option_contracts > 0
            risk_reasons = list(kill_action.reasons)
            effective_size_factor = kill_action.size_factor
            flatten_for_risk = kill_action.flatten_positions

            if drawdown_before_trade >= args.global_drawdown_kill_threshold and long_has_exposure:
                flatten_for_risk = True
                adaptive_pause_side = "long"
                adaptive_pause_reason = "GLOBAL_DRAWDOWN_KILL"
                if "GLOBAL_DRAWDOWN_KILL" not in risk_reasons:
                    risk_reasons.append("GLOBAL_DRAWDOWN_KILL")
            elif drawdown_before_trade >= args.global_drawdown_throttle_threshold and long_has_exposure:
                effective_size_factor *= args.global_drawdown_throttle_size_factor
                if "GLOBAL_DRAWDOWN_THROTTLE" not in risk_reasons:
                    risk_reasons.append("GLOBAL_DRAWDOWN_THROTTLE")

            if drawdown_before_trade >= args.long_catastrophic_kill_threshold and long_has_exposure:
                flatten_for_risk = True
                adaptive_pause_side = "long"
                adaptive_pause_reason = "LONG_CATASTROPHIC_KILL"
                if "LONG_CATASTROPHIC_KILL" not in risk_reasons:
                    risk_reasons.append("LONG_CATASTROPHIC_KILL")
            elif drawdown_before_trade >= args.long_pause_drawdown_threshold and long_has_exposure:
                adaptive_pause_side = "long"
                adaptive_pause_reason = "LONG_PAUSE_DRAWDOWN"
                adaptive_pause_requires_invalid = True
                if "LONG_PAUSE_DRAWDOWN" not in risk_reasons:
                    risk_reasons.append("LONG_PAUSE_DRAWDOWN")

            if kill_action.flatten_positions and long_has_exposure:
                flatten_for_risk = True
                adaptive_pause_side = "long"
                if not adaptive_pause_reason:
                    adaptive_pause_reason = "GAMMA_RED_DRAWDOWN_KILL"
        else:
            adaptive_has_exposure = opening_option_contracts != 0
            adaptive_side = "short" if opening_option_contracts < 0 else "long"
            risk_reasons = list(kill_action.reasons)
            effective_size_factor = kill_action.size_factor
            flatten_for_risk = kill_action.flatten_positions

            if drawdown_before_trade >= args.global_drawdown_kill_threshold and adaptive_has_exposure:
                flatten_for_risk = True
                adaptive_pause_side = adaptive_side
                adaptive_pause_reason = "GLOBAL_DRAWDOWN_KILL"
                if "GLOBAL_DRAWDOWN_KILL" not in risk_reasons:
                    risk_reasons.append("GLOBAL_DRAWDOWN_KILL")
            elif drawdown_before_trade >= args.global_drawdown_throttle_threshold and adaptive_has_exposure:
                effective_size_factor *= args.global_drawdown_throttle_size_factor
                if "GLOBAL_DRAWDOWN_THROTTLE" not in risk_reasons:
                    risk_reasons.append("GLOBAL_DRAWDOWN_THROTTLE")

            if (
                adaptive_side == "long"
                and drawdown_before_trade >= args.long_catastrophic_kill_threshold
                and adaptive_has_exposure
            ):
                flatten_for_risk = True
                adaptive_pause_side = "long"
                adaptive_pause_reason = "LONG_CATASTROPHIC_KILL"
                if "LONG_CATASTROPHIC_KILL" not in risk_reasons:
                    risk_reasons.append("LONG_CATASTROPHIC_KILL")
            elif (
                adaptive_side == "long"
                and drawdown_before_trade >= args.long_pause_drawdown_threshold
                and adaptive_has_exposure
            ):
                adaptive_pause_side = "long"
                adaptive_pause_reason = "LONG_PAUSE_DRAWDOWN"
                adaptive_pause_requires_invalid = True
                if "LONG_PAUSE_DRAWDOWN" not in risk_reasons:
                    risk_reasons.append("LONG_PAUSE_DRAWDOWN")

            if kill_action.flatten_positions and adaptive_has_exposure:
                flatten_for_risk = True
                if not adaptive_pause_side:
                    adaptive_pause_side = adaptive_side
                if not adaptive_pause_reason:
                    adaptive_pause_reason = "GAMMA_RED_DRAWDOWN_KILL"

        if risk_reasons:
            events.append(
                f"{date} RISK {'|'.join(risk_reasons)} "
                f"(mode={mode}, dd={drawdown_before_trade:.4f}, "
                f"gamma_risk={gamma_risk:.2f}, zone={kill_action.zone}, "
                f"size_factor={effective_size_factor:.4f})"
            )

        # Execute the target produced by the previous bar's signal.
        requested_target_contracts = pending_target_contracts
        requested_projected_hedge_shares = int(round(-requested_target_contracts * delta * 100.0))
        requested_projected_exposure = compute_exposures(
            option_contracts=requested_target_contracts,
            hedge_shares=requested_projected_hedge_shares,
            spot_price=spot,
            option_price=option_mid,
            option_delta=delta,
            option_gamma=gamma,
            option_vega=vega,
        )

        target_contracts = pending_target_contracts
        if flatten_for_risk:
            target_contracts = 0

        block_reason = ""
        forced_derisk = flatten_for_risk

        # Never block forced de-risk actions (flattening due to kill switch).
        if not forced_derisk:
            trade_allowed, block_reason, _ = evaluate_projected_trade(
                risk_limits=risk_limits,
                projected_option_contracts=target_contracts,
                spot_price=spot,
                option_price=option_mid,
                option_delta=delta,
                option_gamma=gamma,
                option_vega=vega,
                projected_equity=equity_before_trade,
            )
            if not trade_allowed:
                reducing_risk = abs(target_contracts) < abs(opening_option_contracts)
                if reducing_risk:
                    events.append(
                        f"{date} RISK_ALLOW_DERISK mode={mode} "
                        f"(target={target_contracts}, opening={opening_option_contracts}, "
                        f"reason={block_reason})"
                    )
                else:
                    clamped_target = clamp_target_to_risk_limits(
                        desired_contracts=target_contracts,
                        risk_limits=risk_limits,
                        spot_price=spot,
                        option_price=option_mid,
                        option_delta=delta,
                        option_gamma=gamma,
                        option_vega=vega,
                        projected_equity=equity_before_trade,
                    )
                    if abs(clamped_target) > abs(opening_option_contracts):
                        events.append(
                            f"{date} RISK_CLAMP mode={mode} "
                            f"(requested={target_contracts}, clamped={clamped_target}, "
                            f"reason={block_reason})"
                        )
                        target_contracts = clamped_target
                        block_reason = ""
                    else:
                        target_contracts = opening_option_contracts
                        events.append(f"{date} RISK_BLOCK mode={mode} {block_reason}")

        executed_target_contracts = target_contracts
        executed_target_hedge_shares = int(round(-executed_target_contracts * delta * 100.0))
        executed_target_exposure = compute_exposures(
            option_contracts=executed_target_contracts,
            hedge_shares=executed_target_hedge_shares,
            spot_price=spot,
            option_price=option_mid,
            option_delta=delta,
            option_gamma=gamma,
            option_vega=vega,
        )

        option_trade = execute_option_trade(
            current_contracts=opening_option_contracts,
            target_contracts=target_contracts,
            option_price=option_mid,
            cash=cash,
        )
        option_contracts = option_trade.new_contracts
        cash = option_trade.cash
        pnl.record_costs(option_trade.fees, option_trade.slippage)

        target_hedge_shares = int(round(-option_contracts * delta * 100.0))

        hedge_trade = rebalance_delta_hedge(
            current_shares=opening_hedge_shares,
            target_shares=target_hedge_shares,
            spot_price=spot,
            cash=cash,
        )
        hedge_shares = hedge_trade.new_shares
        cash = hedge_trade.cash
        pnl.record_costs(hedge_trade.fees, hedge_trade.slippage)

        exposures_after = compute_exposures(
            option_contracts=option_contracts,
            hedge_shares=hedge_shares,
            spot_price=spot,
            option_price=option_mid,
            option_delta=delta,
            option_gamma=gamma,
            option_vega=vega,
        )
        equity = cash + option_contracts * 100.0 * option_mid + hedge_shares * spot
        drawdown_after_trade = drawdown.update(equity)

        fees_today = option_trade.fees + hedge_trade.fees
        slippage_today = option_trade.slippage + hedge_trade.slippage
        total_day_pnl = option_mtm + hedge_mtm - fees_today - slippage_today

        leverage = (
            exposures_after["notional_exposure"] / equity if equity > 0 else float("inf")
        )
        cash_usage = max(0.0, (INITIAL_CAPITAL - cash) / INITIAL_CAPITAL)
        if mode == "long":
            long_bars_in_position = long_bars_in_position + 1 if option_contracts > 0 else 0

        # Compute this bar's signal and queue the trade for the next bar.
        decision = signal_engine.decide(
            implied_vol=iv,
            realized_vol_series=rv_history,
            spot=spot,
            price_series=spot_history,
            return_series=return_history,
        )

        if decision.stance != prev_stance:
            events.append(
                f"{date} STANCE {prev_stance}->{decision.stance} "
                f"(mode={mode}, reason={decision.reason})"
            )
        prev_stance = decision.stance

        if mode == "short":
            next_target_contracts = target_option_contracts(
                signal=decision.signal,
                option_price=option_mid,
                capital_base=max(equity, 0.0),
                max_capital_at_risk=risk_limits.max_capital_at_risk,
                size_factor=effective_size_factor,
            )
        elif mode == "long":
            next_target_contracts = target_contracts_by_vega_budget(
                signal=decision.signal,
                capital_base=max(equity, 0.0),
                option_vega=vega,
                max_abs_vega_ratio=args.long_vega_budget_ratio,
                size_factor=effective_size_factor,
            )
        else:
            if decision.signal < 0:
                next_target_contracts = target_option_contracts(
                    signal=-1,
                    option_price=option_mid,
                    capital_base=max(equity, 0.0),
                    max_capital_at_risk=risk_limits.max_capital_at_risk,
                    size_factor=effective_size_factor,
                )
            elif decision.signal > 0:
                next_target_contracts = target_contracts_by_vega_budget(
                    signal=1,
                    capital_base=max(equity, 0.0),
                    option_vega=vega,
                    max_abs_vega_ratio=args.long_vega_budget_ratio,
                    size_factor=effective_size_factor,
                )
            else:
                next_target_contracts = 0
        strategy_gate_reason = ""
        if mode == "short":
            if not decision.pricing_filter_passed or not decision.path_filter_passed:
                if next_target_contracts != 0 or option_contracts != 0:
                    events.append(
                        f"{date} STRATEGY_GATE mode={mode} SHORT_GATE_FORCE_FLAT "
                        f"(pricing_ok={decision.pricing_filter_passed}, "
                        f"path_ok={decision.path_filter_passed})"
                    )
                next_target_contracts = 0
                strategy_gate_reason = "SHORT_GATE_FORCE_FLAT"
            if flatten_for_risk:
                next_target_contracts = 0
                strategy_gate_reason = "SHORT_RISK_FLATTEN"
        elif mode == "long":
            if next_target_contracts < 0:
                next_target_contracts = 0

            pause_applied = False
            if adaptive_pause_side == "long":
                if adaptive_pause_requires_invalid:
                    pause_applied = decision.stance != "LONG_VOL"
                else:
                    pause_applied = True

            if pause_applied:
                pause_len = max(args.adaptive_pause_bars, args.cooldown_bars)
                adaptive_long_pause_remaining = max(adaptive_long_pause_remaining, pause_len)
                strategy_gate_reason = "ADAPTIVE_LONG_PAUSED_RISK"
                next_target_contracts = 0

            if decision.stance == "PAUSED":
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_PAUSED"
            elif flatten_for_risk:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_RISK_FLATTEN"

            if next_target_contracts > 0 and adaptive_long_pause_remaining > 0:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_LONG_PAUSED"
        else:
            pause_applied = False
            if adaptive_pause_side:
                if adaptive_pause_requires_invalid:
                    if adaptive_pause_side == "short":
                        pause_applied = decision.stance != "SHORT_VOL"
                    else:
                        pause_applied = decision.stance != "LONG_VOL"
                else:
                    pause_applied = True

            if pause_applied:
                pause_len = max(args.adaptive_pause_bars, args.cooldown_bars)
                if adaptive_pause_side == "short":
                    adaptive_short_pause_remaining = max(adaptive_short_pause_remaining, pause_len)
                    strategy_gate_reason = "ADAPTIVE_SHORT_PAUSED_RISK"
                elif adaptive_pause_side == "long":
                    adaptive_long_pause_remaining = max(adaptive_long_pause_remaining, pause_len)
                    strategy_gate_reason = "ADAPTIVE_LONG_PAUSED_RISK"
                next_target_contracts = 0

            if decision.stance == "PAUSED":
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_PAUSED"
            elif flatten_for_risk:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_RISK_FLATTEN"

            if next_target_contracts < 0 and adaptive_short_pause_remaining > 0:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_SHORT_PAUSED"
            elif next_target_contracts > 0 and adaptive_long_pause_remaining > 0:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_LONG_PAUSED"

            if option_contracts < 0 < next_target_contracts or option_contracts > 0 > next_target_contracts:
                next_target_contracts = 0
                strategy_gate_reason = "ADAPTIVE_SWITCH_THROUGH_FLAT"

        pending_target_contracts = next_target_contracts

        if (
            mode in {"long", "adaptive"}
            and not first_long_trade_logged
            and first_long_trade_pending is None
            and opening_option_contracts <= 0 < option_contracts
        ):
            first_long_trade_pending = {
                "entry_time": date,
                "entry_spot": spot,
                "iv": iv,
                "rv_short": decision.rv_short,
                "rv_medium": decision.rv_medium,
                "trend_strength": decision.trend_strength,
                "contracts": option_contracts,
                "delta_exposure": exposures_after["delta_exposure"],
                "gamma_exposure": exposures_after["gamma_exposure"],
                "vega_exposure": exposures_after["vega_exposure"],
            }

        step_rows.append(
            {
                "date": date,
                "symbol": symbol,
                "strategy_mode": mode,
                "stance": decision.stance,
                "signal": decision.signal,
                "signal_reason": decision.reason,
                "strategy_gate_reason": strategy_gate_reason,
                "pricing_filter_passed": decision.pricing_filter_passed,
                "path_filter_passed": decision.path_filter_passed,
                "instability_filter_passed": decision.instability_filter_passed,
                "two_way_filter_passed": decision.two_way_filter_passed,
                "rv_short": round(decision.rv_short, 6),
                "rv_medium": round(decision.rv_medium, 6),
                "edge_iv_minus_rv": round(decision.edge, 6),
                "edge_velocity": round(decision.edge_velocity, 6),
                "trend_strength": round(decision.trend_strength, 6),
                "jump_abs_return": round(decision.jump_abs_return, 6),
                "rv_change": round(decision.rv_change, 6),
                "choppiness": round(decision.choppiness, 6),
                "chop_score": (
                    round(decision.chop_score, 6)
                    if math.isfinite(decision.chop_score)
                    else "inf"
                ),
                "cooldown_remaining": decision.cooldown_remaining,
                "cooldown_active": decision.cooldown_active,
                "long_pause_remaining": (
                    max(
                        decision.cooldown_remaining,
                        adaptive_short_pause_remaining if mode == "adaptive" else 0,
                        adaptive_long_pause_remaining,
                    )
                    if mode in {"long", "adaptive"}
                    else 0
                ),
                "long_resume_confirm_count": long_resume_confirm_count,
                "long_bars_in_position": long_bars_in_position,
                "spot": round(spot, 4),
                "option_mid": round(option_mid, 4),
                "requested_option_contracts": requested_target_contracts,
                "executed_target_contracts": executed_target_contracts,
                "option_contracts": option_contracts,
                "queued_option_contracts": pending_target_contracts,
                "hedge_shares": hedge_shares,
                "delta": round(exposures_after["delta_exposure"], 6),
                "gamma": round(exposures_after["gamma_exposure"], 6),
                "vega": round(exposures_after["vega_exposure"], 6),
                "requested_notional_exposure": round(
                    requested_projected_exposure["notional_exposure"], 6
                ),
                "executed_target_notional_exposure": round(
                    executed_target_exposure["notional_exposure"], 6
                ),
                "requested_gamma_abs": round(
                    abs(requested_projected_exposure["gamma_exposure"]), 6
                ),
                "executed_target_gamma_abs": round(
                    abs(executed_target_exposure["gamma_exposure"]), 6
                ),
                "requested_vega_abs": round(
                    abs(requested_projected_exposure["vega_exposure"]), 6
                ),
                "executed_target_vega_abs": round(
                    abs(executed_target_exposure["vega_exposure"]), 6
                ),
                "notional_exposure": round(exposures_after["notional_exposure"], 6),
                "cash_usage": round(cash_usage, 6),
                "drawdown": round(drawdown_after_trade, 6),
                "leverage": round(leverage, 6) if leverage != float("inf") else "inf",
                "equity": round(equity, 6),
                "daily_return": round(daily_return, 6),
                "option_mtm_pnl": round(option_mtm, 6),
                "hedge_pnl": round(hedge_mtm, 6),
                "fees": round(-fees_today, 6),
                "slippage": round(-slippage_today, 6),
                "total_day_pnl": round(total_day_pnl, 6),
                "kill_switch_events": "|".join(kill_action.reasons),
                "risk_events": "|".join(risk_reasons),
                "gamma_risk": round(gamma_risk, 6),
                "gamma_zone": kill_action.zone,
                "gamma_band_size_factor": round(kill_action.size_factor, 6),
                "effective_size_factor": round(effective_size_factor, 6),
                "risk_block_reason": block_reason,
            }
        )
        equity_rows.append(
            {
                "date": date,
                "equity": round(equity, 6),
                "drawdown": round(drawdown_after_trade, 6),
            }
        )

        prev_spot = spot
        prev_option_mid = option_mid
        prev_equity = equity

        if progress_every > 0 and idx % progress_every == 0:
            print(
                f"[{mode}] progress {idx}/{total_rows} "
                f"(date={date}, equity=${equity:,.2f}, stance={decision.stance})"
            )

    step_fields = [
        "date",
        "symbol",
        "strategy_mode",
        "stance",
        "signal",
        "signal_reason",
        "strategy_gate_reason",
        "pricing_filter_passed",
        "path_filter_passed",
        "instability_filter_passed",
        "two_way_filter_passed",
        "rv_short",
        "rv_medium",
        "edge_iv_minus_rv",
        "edge_velocity",
        "trend_strength",
        "jump_abs_return",
        "rv_change",
        "choppiness",
        "chop_score",
        "cooldown_remaining",
        "cooldown_active",
        "long_pause_remaining",
        "long_resume_confirm_count",
        "long_bars_in_position",
        "spot",
        "option_mid",
        "requested_option_contracts",
        "executed_target_contracts",
        "option_contracts",
        "queued_option_contracts",
        "hedge_shares",
        "delta",
        "gamma",
        "vega",
        "requested_notional_exposure",
        "executed_target_notional_exposure",
        "requested_gamma_abs",
        "executed_target_gamma_abs",
        "requested_vega_abs",
        "executed_target_vega_abs",
        "notional_exposure",
        "cash_usage",
        "drawdown",
        "leverage",
        "equity",
        "daily_return",
        "option_mtm_pnl",
        "hedge_pnl",
        "fees",
        "slippage",
        "total_day_pnl",
        "kill_switch_events",
        "risk_events",
        "gamma_risk",
        "gamma_zone",
        "gamma_band_size_factor",
        "effective_size_factor",
        "risk_block_reason",
    ]

    timestep_path = output_path("timestep_log", mode, multi_mode, "csv")
    equity_path = output_path("equity_curve", mode, multi_mode, "csv")
    pnl_path = output_path("pnl_summary", mode, multi_mode, "csv")
    events_path = output_path("events", mode, multi_mode, "log")

    write_csv(timestep_path, step_fields, step_rows)
    write_csv(
        equity_path,
        ["date", "equity", "drawdown"],
        equity_rows,
    )

    summary = pnl.as_dict()
    summary_rows = [
        {
            "strategy_mode": mode,
            **{key: round(value, 6) for key, value in summary.items()},
            "ending_equity": round(prev_equity, 6),
            "max_drawdown": round(drawdown.max_drawdown, 6),
        }
    ]
    write_csv(
        pnl_path,
        [
            "strategy_mode",
            "option_mtm_pnl",
            "hedge_pnl",
            "fees",
            "slippage",
            "total_pnl",
            "ending_equity",
            "max_drawdown",
        ],
        summary_rows,
    )

    if first_long_trade_pending is not None:
        diagnostic = (
            f"{first_long_trade_pending['entry_time']} LONG_DIAGNOSTIC "
            f"(entry_time={first_long_trade_pending['entry_time']}, "
            f"iv={first_long_trade_pending['iv']:.6f}, "
            f"rv_short={first_long_trade_pending['rv_short']:.6f}, "
            f"rv_medium={first_long_trade_pending['rv_medium']:.6f}, "
            f"trend_strength={first_long_trade_pending['trend_strength']:.6f}, "
            f"contracts={first_long_trade_pending['contracts']}, "
            f"delta={first_long_trade_pending['delta_exposure']:.6f}, "
            f"gamma={first_long_trade_pending['gamma_exposure']:.6f}, "
            f"vega={first_long_trade_pending['vega_exposure']:.6f}, "
            "next_bar_return=NA)"
        )
        print(f"[{mode}] {diagnostic}")
        events.append(diagnostic)
        first_long_trade_pending = None

    if not events:
        events.append(
            f"{market_data[0]['date']} INFO mode={mode} NO_EVENTS "
            "No stance/risk transitions occurred. "
            "Tune signal thresholds/windows for this data regime."
        )

    with open(events_path, "w", encoding="utf-8") as handle:
        for line in events:
            handle.write(f"{line}\n")

    plots_generated = False
    if skip_plots:
        print(f"[{mode}] Plot generation skipped (--skip-plots).")
    else:
        try:
            generate_plots(
                log_path=timestep_path,
                output_suffix=mode if multi_mode else "",
                title_suffix=f" ({mode})" if multi_mode else "",
            )
            plots_generated = True
        except RuntimeError as exc:
            # Keep simulation usable even when plotting dependencies are missing.
            print(f"[{mode}] Plot generation skipped: {exc}")

    print(f"Simulation complete ({mode}).")
    print(f"Symbol: {symbol}")
    print(f"Year: {year}")
    print(
        "Gamma bands: "
        f"green<= {args.gamma_green_threshold:.2f}, "
        f"yellow<= {args.gamma_red_threshold:.2f}, "
        f"red> {args.gamma_red_threshold:.2f}"
    )
    print(
        "Gamma zone size factors: "
        f"yellow={args.gamma_yellow_size_factor:.2f}, "
        f"red={args.gamma_red_size_factor:.2f}"
    )
    print(f"Gamma red-zone kill drawdown (D1): {args.gamma_kill_drawdown_threshold:.2%}")
    print(
        "Global drawdown controls: "
        f"throttle@{args.global_drawdown_throttle_threshold:.2%} "
        f"(x{args.global_drawdown_throttle_size_factor:.2f}), "
        f"kill@{args.global_drawdown_kill_threshold:.2%}"
    )
    print(
        "Long-vol controls: "
        f"pause_dd@{args.long_pause_drawdown_threshold:.2%}, "
        f"pause_bars={args.long_pause_bars}, "
        f"resume_confirm={args.long_resume_confirm_bars}, "
        f"cat_kill@{args.long_catastrophic_kill_threshold:.2%}, "
        f"vega_budget_ratio={args.long_vega_budget_ratio:.4f}, "
        f"max_iv_premium={args.long_max_iv_premium:.4f}"
    )
    if mode == "adaptive":
        print(
            "Adaptive regime controls: "
            f"enter_persist={signal_config.adaptive_enter_persist_bars}, "
            f"exit_persist={signal_config.adaptive_exit_persist_bars}, "
            f"pause_bars={signal_config.adaptive_pause_bars}, "
            f"short(E={signal_config.adaptive_short_edge_enter:.4f},"
            f"E_exit={signal_config.adaptive_short_edge_exit:.4f}), "
            f"long(C={signal_config.adaptive_long_cheapness_enter:.4f},"
            f"C_exit={signal_config.adaptive_long_cheapness_exit:.4f}), "
            f"confidence_buffer={signal_config.adaptive_confidence_buffer:.4f}"
        )
    print(f"Max leverage limit: {args.max_leverage:.2f}x")
    print(
        "Hard greek caps: "
        f"|gamma|<= {args.max_abs_gamma:.2f}, "
        f"|vega|<= {args.max_abs_vega:.2f}"
    )
    print(f"Prices file: {prices_path}")
    print(f"Options file: {options_path}")
    if args.start_date or args.end_date:
        print(f"Period: {args.start_date or '-inf'} to {args.end_date or '+inf'}")
    print(f"Ending equity: ${prev_equity:,.2f}")
    print("PnL decomposition:")
    print(f"  Option MTM PnL: ${summary['option_mtm_pnl']:,.2f}")
    print(f"  Hedge PnL: ${summary['hedge_pnl']:,.2f}")
    print(f"  Fees: ${summary['fees']:,.2f}")
    print(f"  Slippage: ${summary['slippage']:,.2f}")
    print(f"  Total PnL: ${summary['total_pnl']:,.2f}")
    print(f"Max drawdown: {drawdown.max_drawdown:.2%}")
    print(f"Event log entries: {len(events)} ({events_path})")
    if plots_generated:
        if multi_mode:
            print(f"Plots saved to results/*_{mode}.png")
        else:
            print("Plots saved to results/*.png")

    return {
        "mode": mode,
        "summary": summary,
        "ending_equity": prev_equity,
        "max_drawdown": drawdown.max_drawdown,
        "events_count": len(events),
        "timestep_path": timestep_path,
        "equity_path": equity_path,
        "pnl_path": pnl_path,
        "events_path": events_path,
    }


def run() -> None:
    args = parse_args()
    if args.gamma_green_threshold < 0 or args.gamma_red_threshold < 0:
        raise ValueError("Gamma thresholds must be non-negative.")
    if args.gamma_green_threshold >= args.gamma_red_threshold:
        raise ValueError("Require gamma-green-threshold < gamma-red-threshold.")
    if not (0.0 < args.gamma_yellow_size_factor <= 1.0):
        raise ValueError("gamma-yellow-size-factor must be in (0, 1].")
    if not (0.0 < args.gamma_red_size_factor <= 1.0):
        raise ValueError("gamma-red-size-factor must be in (0, 1].")
    if args.gamma_red_size_factor > args.gamma_yellow_size_factor:
        raise ValueError("gamma-red-size-factor should be <= gamma-yellow-size-factor.")
    if args.gamma_kill_drawdown_threshold < 0:
        raise ValueError("gamma-kill-drawdown-threshold must be non-negative.")
    if args.max_leverage <= 0:
        raise ValueError("max-leverage must be > 0.")
    if args.max_abs_gamma <= 0:
        raise ValueError("max-abs-gamma must be > 0.")
    if args.max_abs_vega <= 0:
        raise ValueError("max-abs-vega must be > 0.")
    if args.long_pause_drawdown_threshold < 0:
        raise ValueError("long-pause-drawdown-threshold must be non-negative.")
    if args.long_pause_bars < 0:
        raise ValueError("long-pause-bars must be non-negative.")
    if args.long_resume_confirm_bars <= 0:
        raise ValueError("long-resume-confirm-bars must be positive.")
    if args.long_catastrophic_kill_threshold < 0:
        raise ValueError("long-catastrophic-kill-threshold must be non-negative.")
    if args.long_catastrophic_kill_threshold < args.long_pause_drawdown_threshold:
        raise ValueError(
            "long-catastrophic-kill-threshold must be >= long-pause-drawdown-threshold."
        )
    if args.long_min_hold_bars < 0:
        raise ValueError("long-min-hold-bars must be non-negative.")
    if not (0.0 < args.long_derisk_factor < 1.0):
        raise ValueError("long-derisk-factor must be in (0, 1).")
    if args.long_two_way_chop_score_threshold < 0:
        raise ValueError("long-two-way-chop-score-threshold must be non-negative.")
    if args.long_two_way_trend_threshold < 0:
        raise ValueError("long-two-way-trend-threshold must be non-negative.")
    if args.long_vega_budget_ratio <= 0:
        raise ValueError("long-vega-budget-ratio must be > 0.")
    if args.global_drawdown_throttle_threshold < 0:
        raise ValueError("global-drawdown-throttle-threshold must be non-negative.")
    if args.global_drawdown_kill_threshold < 0:
        raise ValueError("global-drawdown-kill-threshold must be non-negative.")
    if args.global_drawdown_kill_threshold < args.global_drawdown_throttle_threshold:
        raise ValueError(
            "global-drawdown-kill-threshold must be >= global-drawdown-throttle-threshold."
        )
    if not (0.0 < args.global_drawdown_throttle_size_factor <= 1.0):
        raise ValueError("global-drawdown-throttle-size-factor must be in (0, 1].")
    if args.rv_short_window <= 0 or args.rv_medium_window <= 0:
        raise ValueError("rv-short-window and rv-medium-window must be positive.")
    if args.rv_short_window > args.rv_medium_window:
        raise ValueError("rv-short-window should be <= rv-medium-window.")
    if args.trend_window <= 0 or args.long_chop_window <= 0:
        raise ValueError("trend-window and long-chop-window must be positive.")
    if args.short_edge_threshold < 0 or args.long_edge_threshold < 0:
        raise ValueError("Edge thresholds must be non-negative.")
    if args.long_max_iv_premium < 0:
        raise ValueError("long-max-iv-premium must be non-negative.")
    if args.adaptive_enter_persist_bars <= 0:
        raise ValueError("adaptive-enter-persist-bars must be positive.")
    if args.adaptive_exit_persist_bars <= 0:
        raise ValueError("adaptive-exit-persist-bars must be positive.")
    if args.adaptive_pause_bars < 0:
        raise ValueError("adaptive-pause-bars must be non-negative.")
    if args.adaptive_short_edge_enter < 0 or args.adaptive_short_edge_exit < 0:
        raise ValueError("adaptive short edge thresholds must be non-negative.")
    if args.adaptive_short_edge_exit > args.adaptive_short_edge_enter:
        raise ValueError("adaptive-short-edge-exit must be <= adaptive-short-edge-enter.")
    if args.adaptive_short_trend_enter < 0 or args.adaptive_short_trend_exit < 0:
        raise ValueError("adaptive short trend thresholds must be non-negative.")
    if args.adaptive_short_trend_exit < args.adaptive_short_trend_enter:
        raise ValueError("adaptive-short-trend-exit must be >= adaptive-short-trend-enter.")
    if (
        args.adaptive_vov_low < 0
        or args.adaptive_vov_high < 0
        or args.adaptive_vov_exit < 0
    ):
        raise ValueError("adaptive vol-of-vol thresholds must be non-negative.")
    if args.adaptive_vov_high <= args.adaptive_vov_low:
        raise ValueError("adaptive-vov-high must be > adaptive-vov-low.")
    if not (args.adaptive_vov_low <= args.adaptive_vov_exit <= args.adaptive_vov_high):
        raise ValueError("adaptive-vov-exit must be between adaptive-vov-low and adaptive-vov-high.")
    if args.adaptive_long_cheapness_exit > args.adaptive_long_cheapness_enter:
        raise ValueError(
            "adaptive-long-cheapness-exit must be <= adaptive-long-cheapness-enter."
        )
    if args.adaptive_long_trend_max < 0:
        raise ValueError("adaptive-long-trend-max must be non-negative.")
    if args.adaptive_confidence_buffer < 0:
        raise ValueError("adaptive-confidence-buffer must be non-negative.")
    if args.cooldown_bars < 0:
        raise ValueError("cooldown-bars must be non-negative.")
    if args.progress_every < 0:
        raise ValueError("progress-every must be non-negative.")

    symbol = args.symbol.strip().upper()
    year = args.year
    prices_path, options_path = resolve_input_paths(
        symbol=symbol,
        year=year,
    )

    prices = load_prices(prices_path)
    options = load_options(options_path)
    market_data = merge_market_data(prices, options)
    market_data = filter_market_data_by_period(
        market_data=market_data,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if not market_data:
        raise RuntimeError(
            "No market data available after loading/filtering. "
            "Check symbol/year files and start/end dates."
        )

    signal_config = resolve_signal_config(args, market_data)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    modes = ["short", "long"] if args.strategy_mode == "both" else [args.strategy_mode]
    multi_mode = len(modes) > 1
    mode_results: List[Dict[str, object]] = []

    for mode in modes:
        result = run_single_mode(
            mode=mode,
            symbol=symbol,
            year=year,
            market_data=market_data,
            args=args,
            signal_config=signal_config,
            multi_mode=multi_mode,
            prices_path=prices_path,
            options_path=options_path,
            skip_plots=args.skip_plots,
            progress_every=args.progress_every,
        )
        mode_results.append(result)

    if multi_mode:
        comparison_rows: List[Dict[str, object]] = []
        for result in mode_results:
            summary = result["summary"]
            comparison_rows.append(
                {
                    "strategy_mode": result["mode"],
                    "option_mtm_pnl": round(float(summary["option_mtm_pnl"]), 6),
                    "hedge_pnl": round(float(summary["hedge_pnl"]), 6),
                    "fees": round(float(summary["fees"]), 6),
                    "slippage": round(float(summary["slippage"]), 6),
                    "total_pnl": round(float(summary["total_pnl"]), 6),
                    "ending_equity": round(float(result["ending_equity"]), 6),
                    "max_drawdown": round(float(result["max_drawdown"]), 6),
                    "events_count": int(result["events_count"]),
                }
            )

        comparison_path = os.path.join(RESULTS_DIR, "pnl_summary_all_modes.csv")
        write_csv(
            comparison_path,
            [
                "strategy_mode",
                "option_mtm_pnl",
                "hedge_pnl",
                "fees",
                "slippage",
                "total_pnl",
                "ending_equity",
                "max_drawdown",
                "events_count",
            ],
            comparison_rows,
        )
        print(f"Cross-mode summary: {comparison_path}")


if __name__ == "__main__":
    run()
