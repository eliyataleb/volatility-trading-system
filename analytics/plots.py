"""Generate project deliverable plots from simulation outputs."""

import csv
import os
from datetime import datetime
from typing import Dict, List


RESULTS_DIR = "results"
DEFAULT_LOG_PATH = os.path.join(RESULTS_DIR, "timestep_log.csv")
DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M"


def load_log_rows(path: str) -> List[Dict[str, float]]:
    def _parse_float(value: str | None, default: float = 0.0) -> float:
        if value in (None, ""):
            return default
        try:
            return float(value)
        except ValueError:
            return default

    rows: List[Dict[str, float]] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                {
                    "date": row["date"],
                    "equity": _parse_float(row.get("equity")),
                    "option_mtm_pnl": _parse_float(row.get("option_mtm_pnl")),
                    "hedge_pnl": _parse_float(row.get("hedge_pnl")),
                    "fees": _parse_float(row.get("fees")),
                    "slippage": _parse_float(row.get("slippage")),
                    "drawdown": _parse_float(row.get("drawdown")),
                    "delta": _parse_float(row.get("delta")),
                    "gamma": _parse_float(row.get("gamma")),
                    "vega": _parse_float(row.get("vega")),
                    "cash_usage": _parse_float(row.get("cash_usage")),
                    "edge_iv_minus_rv": _parse_float(row.get("edge_iv_minus_rv")),
                    "rv_change": _parse_float(row.get("rv_change")),
                    "trend_strength": _parse_float(row.get("trend_strength")),
                }
            )
    return rows


def cumulative(values: List[float]) -> List[float]:
    total = 0.0
    out: List[float] = []
    for value in values:
        total += value
        out.append(total)
    return out


def parse_timestamp(timestamp_text: str) -> datetime:
    for fmt in (DATETIME_FMT, DATE_FMT):
        try:
            return datetime.strptime(timestamp_text, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid timestamp '{timestamp_text}'. "
        f"Expected '{DATE_FMT}' or '{DATETIME_FMT}'."
    )


def format_date_axis(ax, mdates_module) -> None:
    locator = mdates_module.AutoDateLocator(minticks=5, maxticks=9)
    formatter = mdates_module.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)


def _with_suffix(name: str, suffix: str) -> str:
    if not suffix:
        return name
    return f"{name}_{suffix}"


def generate_plots(
    *,
    log_path: str = DEFAULT_LOG_PATH,
    output_suffix: str = "",
    title_suffix: str = "",
) -> None:
    # Keep matplotlib cache local/writable to avoid repeated slow font-cache rebuilds.
    mpl_config_dir = os.path.join(RESULTS_DIR, ".mplconfig")
    os.makedirs(mpl_config_dir, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", mpl_config_dir)

    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "matplotlib is required for plots. Install it with: pip install matplotlib"
        ) from exc

    rows = load_log_rows(log_path)
    if not rows:
        raise RuntimeError(f"No rows found in {log_path}. Run main.py first.")

    dates = [parse_timestamp(row["date"]) for row in rows]
    equity = [row["equity"] for row in rows]
    day_total = [
        row["option_mtm_pnl"] + row["hedge_pnl"] + row["fees"] + row["slippage"]
        for row in rows
    ]
    cumulative_pnl = cumulative(day_total)
    drawdown = [row["drawdown"] * 100.0 for row in rows]
    delta = [row["delta"] for row in rows]
    gamma = [row["gamma"] for row in rows]
    vega = [row["vega"] for row in rows]
    cash_usage = [row["cash_usage"] * 100.0 for row in rows]
    edge_iv_minus_rv = [row["edge_iv_minus_rv"] for row in rows]
    vol_of_vol_proxy = [row["rv_change"] for row in rows]
    trend_strength = [row["trend_strength"] for row in rows]

    os.makedirs(RESULTS_DIR, exist_ok=True)

    plt.figure(figsize=(11, 4))
    plt.plot(dates, equity, color="#0b6efd", linewidth=2)
    plt.title(f"Equity Curve{title_suffix}")
    plt.ylabel("USD")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_equity_curve', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, cumulative_pnl, color="#1f77b4", linewidth=2)
    plt.title(f"Cumulative PnL{title_suffix}")
    plt.ylabel("USD")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_cumulative_pnl', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, drawdown, color="#d62728", linewidth=2)
    plt.title(f"Drawdown{title_suffix}")
    plt.ylabel("Percent")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_drawdown', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 5))
    plt.plot(dates, delta, label="Delta", linewidth=2)
    plt.plot(dates, gamma, label="Gamma", linewidth=2)
    plt.plot(dates, vega, label="Vega", linewidth=2)
    plt.title(f"Greeks Exposure Over Time{title_suffix}")
    plt.legend()
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_greeks', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, cash_usage, color="#2ca02c", linewidth=2)
    plt.title(f"Capital Usage{title_suffix}")
    plt.ylabel("Percent of initial capital")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_capital_usage', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, edge_iv_minus_rv, color="#6f42c1", linewidth=2)
    plt.axhline(0.0, color="#666666", linewidth=1.0, linestyle="--")
    plt.title(f"IV-RV Edge{title_suffix}")
    plt.ylabel("Vol points")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_iv_minus_rv', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, vol_of_vol_proxy, color="#fd7e14", linewidth=2)
    plt.title(f"Vol of Vol Proxy (|RV short - RV medium|){title_suffix}")
    plt.ylabel("Vol points")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_vol_of_vol', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(dates, trend_strength, color="#20c997", linewidth=2)
    plt.title(f"Trend Strength{title_suffix}")
    plt.ylabel("Absolute trend")
    format_date_axis(plt.gca(), mdates)
    plt.tight_layout()
    plt.savefig(
        os.path.join(RESULTS_DIR, f"{_with_suffix('plot_trend_strength', output_suffix)}.png"),
        dpi=130,
    )
    plt.close()

    if output_suffix:
        print(f"Saved plot files to results/*_{output_suffix}.png")
    else:
        print("Saved plot files to results/.")


def main() -> None:
    if os.path.exists(DEFAULT_LOG_PATH):
        generate_plots()
        return

    short_path = os.path.join(RESULTS_DIR, "timestep_log_short.csv")
    long_path = os.path.join(RESULTS_DIR, "timestep_log_long.csv")
    generated_any = False

    if os.path.exists(short_path):
        generate_plots(
            log_path=short_path,
            output_suffix="short",
            title_suffix=" (short)",
        )
        generated_any = True
    if os.path.exists(long_path):
        generate_plots(
            log_path=long_path,
            output_suffix="long",
            title_suffix=" (long)",
        )
        generated_any = True

    if not generated_any:
        raise RuntimeError(
            "No timestep log found. Run main.py first to generate results/timestep_log*.csv."
        )


if __name__ == "__main__":
    main()
