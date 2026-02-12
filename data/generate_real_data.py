"""Generate backtest-ready CSVs from real Yahoo Finance market data.

Output files:
- data/<symbol>_<year>_prices.csv
- data/<symbol>_<year>_options.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from statistics import pstdev
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo


DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M"
TRADING_DAYS_PER_YEAR = 252
INTERVAL_CHOICES = ["1m", "2m", "5m", "15m", "30m", "1h", "1d"]
YAHOO_CHART_ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart"
SQRT_2 = math.sqrt(2.0)
SQRT_2PI = math.sqrt(2.0 * math.pi)
BARS_PER_TRADING_DAY = {
    "1d": 1,
    "1h": 7,
    "30m": 13,
    "15m": 26,
    "5m": 78,
    "2m": 195,
    "1m": 390,
}
MAX_DAYS_PER_REQUEST = {
    "1m": 7,
    "2m": 60,
    "5m": 60,
    "15m": 60,
    "30m": 60,
    "1h": 730,
}


def parse_args() -> argparse.Namespace:
    default_year = date.today().year
    parser = argparse.ArgumentParser(
        description="Generate replay CSVs from real market data."
    )
    parser.add_argument(
        "--symbol",
        default="SPY",
        help="Ticker symbol (default: SPY).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=default_year,
        help=(
            "Year used for output file names and for daily date range defaults "
            f"(default: {default_year})."
        ),
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive start date in YYYY-MM-DD (optional override).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date in YYYY-MM-DD (optional override).",
    )
    parser.add_argument(
        "--interval",
        choices=INTERVAL_CHOICES,
        default="1m",
        help="Yahoo bar interval (default: 1m).",
    )
    parser.add_argument(
        "--rv-window",
        type=int,
        default=None,
        help=(
            "Rolling return window used for realized-vol estimate "
            "(default: auto by interval)."
        ),
    )
    parser.add_argument(
        "--option-tenor-days",
        type=int,
        default=14,
        help="ATM option tenor in trading days for the option proxy (default: 14).",
    )
    parser.add_argument(
        "--iv-multiplier",
        type=float,
        default=1.10,
        help="Fallback IV model: IV = rv * iv-multiplier + iv-spread (default: 1.10).",
    )
    parser.add_argument(
        "--iv-spread",
        type=float,
        default=0.01,
        help="Fallback IV additive spread (default: 0.01).",
    )
    parser.add_argument(
        "--iv-proxy-symbol",
        default=None,
        help="Optional symbol used as IV proxy (example: ^VIX for SPY).",
    )
    parser.add_argument(
        "--iv-proxy-scale",
        type=float,
        default=1.0,
        help="Scale factor applied to proxy-IV series after percent->decimal conversion (default: 1.0).",
    )
    parser.add_argument(
        "--risk-free-rate",
        type=float,
        default=0.0,
        help="Continuously compounded annualized rate for Black-Scholes proxy (default: 0.0).",
    )
    parser.add_argument(
        "--skip-plots",
        action="store_true",
        help="Skip PNG plot generation for prices/options CSV outputs.",
    )
    return parser.parse_args()


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def parse_date(text: str) -> date:
    return datetime.strptime(text, DATE_FMT).date()


def resolve_date_bounds(
    *,
    year: int,
    interval: str,
    start_date_text: str | None,
    end_date_text: str | None,
) -> tuple[date, date]:
    if interval == "1d":
        default_start = date(year, 1, 1)
        default_end = date(year, 12, 31)
    else:
        # Yahoo intraday availability is limited; default to a recent window.
        lookback_days = 30 if interval == "1m" else 60
        default_end = date.today()
        default_start = default_end - timedelta(days=lookback_days - 1)

    start_date = parse_date(start_date_text) if start_date_text else default_start
    end_date = parse_date(end_date_text) if end_date_text else default_end
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date.")
    return start_date, end_date


def default_rv_window(interval: str) -> int:
    if interval == "1d":
        return 21
    bars_per_day = BARS_PER_TRADING_DAY[interval]
    return max(30, bars_per_day * 5)


def interval_bars_per_year(interval: str) -> float:
    if interval not in BARS_PER_TRADING_DAY:
        raise ValueError(f"Unsupported interval: {interval}")
    return float(TRADING_DAYS_PER_YEAR * BARS_PER_TRADING_DAY[interval])


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / SQRT_2))


def normal_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI


def build_yahoo_chart_url(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str,
) -> str:
    period1 = int(
        datetime.combine(start_date, time(0, 0), tzinfo=timezone.utc).timestamp()
    )
    period2 = int(
        datetime.combine(
            end_date + timedelta(days=1), time(0, 0), tzinfo=timezone.utc
        ).timestamp()
    )
    query = urllib.parse.urlencode(
        {
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits",
        }
    )
    return f"{YAHOO_CHART_ENDPOINT}/{urllib.parse.quote(symbol)}?{query}"


def fetch_closes_from_yahoo_once(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str,
) -> List[Tuple[datetime, float]]:
    url = build_yahoo_chart_url(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
    )
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"Yahoo request failed for {symbol} with HTTP {exc.code}: {exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Yahoo request failed for {symbol}. Check internet/DNS access ({exc.reason})."
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Yahoo response for {symbol} was not valid JSON.") from exc

    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        description = error.get("description", str(error))
        raise RuntimeError(f"Yahoo API error for {symbol}: {description}")

    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"No data returned for {symbol}.")

    result = results[0]
    meta = result.get("meta") or {}
    tz_name = meta.get("exchangeTimezoneName")
    timezone_local = None
    if tz_name:
        try:
            timezone_local = ZoneInfo(tz_name)
        except Exception:
            timezone_local = None

    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []

    rows_by_timestamp: Dict[datetime, float] = {}
    gmtoffset = int(meta.get("gmtoffset", 0))

    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        close_value = float(close)
        if not math.isfinite(close_value) or close_value <= 0:
            continue

        dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        if timezone_local is not None:
            dt_local = dt_utc.astimezone(timezone_local).replace(tzinfo=None)
        else:
            dt_local = (dt_utc + timedelta(seconds=gmtoffset)).replace(tzinfo=None)

        if dt_local.date() < start_date or dt_local.date() > end_date:
            continue

        rows_by_timestamp[dt_local] = close_value

    return sorted(rows_by_timestamp.items(), key=lambda item: item[0])


def fetch_closes_from_yahoo(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    interval: str,
) -> List[Tuple[datetime, float]]:
    max_days = MAX_DAYS_PER_REQUEST.get(interval)
    if max_days is None:
        rows = fetch_closes_from_yahoo_once(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
        )
        if not rows:
            raise RuntimeError(
                f"No valid close prices returned for {symbol} in {start_date} to {end_date}."
            )
        return rows

    merged_rows: Dict[datetime, float] = {}
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=max_days - 1), end_date)
        try:
            chunk_rows = fetch_closes_from_yahoo_once(
                symbol=symbol,
                start_date=chunk_start,
                end_date=chunk_end,
                interval=interval,
            )
        except RuntimeError as exc:
            raise RuntimeError(
                f"{exc} (while requesting {chunk_start} to {chunk_end}, interval={interval})"
            ) from exc
        for dt_value, close_value in chunk_rows:
            merged_rows[dt_value] = close_value
        chunk_start = chunk_end + timedelta(days=1)

    rows = sorted(merged_rows.items(), key=lambda item: item[0])
    if not rows:
        raise RuntimeError(
            f"No valid close prices returned for {symbol} in {start_date} to {end_date}."
        )
    return rows


def row_key(dt_value: datetime, interval: str) -> str:
    if interval == "1d":
        return dt_value.strftime(DATE_FMT)
    return dt_value.strftime(DATETIME_FMT)


def black_scholes_atm_call_metrics(
    *,
    spot: float,
    iv: float,
    expiry_days: int,
    risk_free_rate: float,
) -> tuple[float, float, float, float]:
    sigma = max(iv, 1e-8)
    t_years = max(expiry_days, 1) / float(TRADING_DAYS_PER_YEAR)
    sqrt_t = math.sqrt(t_years)
    vol_sqrt_t = sigma * sqrt_t

    d1 = (risk_free_rate + 0.5 * sigma * sigma) * t_years / max(vol_sqrt_t, 1e-12)
    d2 = d1 - vol_sqrt_t

    nd1 = normal_cdf(d1)
    nd2 = normal_cdf(d2)
    pdf_d1 = normal_pdf(d1)

    strike = spot
    option_mid = spot * nd1 - strike * math.exp(-risk_free_rate * t_years) * nd2
    delta = nd1
    gamma = pdf_d1 / max(spot * vol_sqrt_t, 1e-12)
    vega = (spot * pdf_d1 * sqrt_t) / 100.0

    return (
        max(option_mid, 0.01),
        clamp(delta, 0.01, 0.99),
        clamp(gamma, 0.0001, 0.20),
        clamp(vega, 0.0, 5.0),
    )


def maybe_generate_plots(
    *,
    timestamps: List[datetime],
    closes: List[float],
    option_mids: List[float],
    prices_csv_path: str,
    options_csv_path: str,
) -> list[str]:
    # Keep matplotlib cache writable inside the project.
    mpl_config_dir = os.path.join("data", ".mplconfig")
    os.makedirs(mpl_config_dir, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", mpl_config_dir)

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
    except ImportError:
        print("Skipped plots: matplotlib is not installed.")
        return []

    locator = mdates.AutoDateLocator(minticks=5, maxticks=9)
    formatter = mdates.ConciseDateFormatter(locator)
    prices_plot_path = os.path.splitext(prices_csv_path)[0] + ".png"
    options_plot_path = os.path.splitext(options_csv_path)[0] + ".png"

    plt.figure(figsize=(11, 4))
    plt.plot(timestamps, closes, color="#0b6efd", linewidth=1.5)
    plt.title("Underlying Price (Yahoo)")
    plt.ylabel("Price")
    ax = plt.gca()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    plt.tight_layout()
    plt.savefig(prices_plot_path, dpi=130)
    plt.close()

    plt.figure(figsize=(11, 4))
    plt.plot(timestamps, option_mids, color="#198754", linewidth=1.5)
    plt.title("Option Mid Price (ATM Proxy)")
    plt.ylabel("Price")
    ax = plt.gca()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    plt.tight_layout()
    plt.savefig(options_plot_path, dpi=130)
    plt.close()

    return [prices_plot_path, options_plot_path]


def generate(
    *,
    symbol: str,
    year: int,
    start_date: date,
    end_date: date,
    interval: str,
    rv_window: int,
    option_tenor_days: int,
    iv_multiplier: float,
    iv_spread: float,
    iv_proxy_symbol: str | None,
    iv_proxy_scale: float,
    risk_free_rate: float,
    skip_plots: bool,
) -> tuple[int, str, str, list[str]]:
    symbol_norm = symbol.strip().lower()
    prices_out = f"data/{symbol_norm}_{year}_prices.csv"
    options_out = f"data/{symbol_norm}_{year}_options.csv"

    close_rows = fetch_closes_from_yahoo(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
    )
    proxy_iv_by_key: Dict[str, float] = {}
    if iv_proxy_symbol:
        try:
            proxy_rows = fetch_closes_from_yahoo(
                symbol=iv_proxy_symbol,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )
        except RuntimeError as exc:
            print(
                f"Warning: IV proxy fetch failed for {iv_proxy_symbol} ({exc}). "
                "Falling back to IV = RV * iv-multiplier + iv-spread."
            )
            proxy_rows = []
        for dt_value, close_value in proxy_rows:
            proxy_iv_by_key[row_key(dt_value, interval)] = close_value / 100.0

    annualization = math.sqrt(interval_bars_per_year(interval))
    returns: list[float] = []
    prev_close: float | None = None
    prev_trade_date: date | None = None
    expiry_days = option_tenor_days

    rows_prices: list[dict[str, object]] = []
    rows_options: list[dict[str, object]] = []
    timestamps: list[datetime] = []
    closes_for_plot: list[float] = []
    option_mids_for_plot: list[float] = []

    for dt_value, close_value in close_rows:
        current_date = dt_value.date()
        if prev_trade_date is not None and current_date != prev_trade_date:
            expiry_days -= 1
            if expiry_days <= 0:
                expiry_days = option_tenor_days
        prev_trade_date = current_date

        if prev_close is not None and prev_close > 0:
            returns.append(math.log(close_value / prev_close))
        prev_close = close_value

        window_returns = returns[-rv_window:]
        if len(window_returns) >= 2:
            rv = clamp(pstdev(window_returns) * annualization, 0.05, 2.00)
        elif len(returns) == 1:
            rv = clamp(abs(returns[0]) * annualization, 0.05, 2.00)
        else:
            rv = 0.20

        key = row_key(dt_value, interval)
        if key in proxy_iv_by_key:
            iv_raw = proxy_iv_by_key[key] * iv_proxy_scale
        else:
            iv_raw = rv * iv_multiplier
        iv = clamp(iv_raw + iv_spread, 0.05, 2.00)

        option_mid, delta, gamma, vega = black_scholes_atm_call_metrics(
            spot=close_value,
            iv=iv,
            expiry_days=expiry_days,
            risk_free_rate=risk_free_rate,
        )

        rows_prices.append(
            {
                "date": key,
                "close": round(close_value, 4),
                "realized_vol": round(rv, 4),
            }
        )
        rows_options.append(
            {
                "date": key,
                "option_mid": round(option_mid, 4),
                "iv": round(iv, 4),
                "delta": round(delta, 4),
                "gamma": round(gamma, 4),
                "vega": round(vega, 4),
                "expiry_days": expiry_days,
            }
        )

        timestamps.append(dt_value)
        closes_for_plot.append(close_value)
        option_mids_for_plot.append(option_mid)

    with open(prices_out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "close", "realized_vol"])
        writer.writeheader()
        writer.writerows(rows_prices)

    with open(options_out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "option_mid",
                "iv",
                "delta",
                "gamma",
                "vega",
                "expiry_days",
            ],
        )
        writer.writeheader()
        writer.writerows(rows_options)

    plot_paths: list[str] = []
    if not skip_plots:
        plot_paths = maybe_generate_plots(
            timestamps=timestamps,
            closes=closes_for_plot,
            option_mids=option_mids_for_plot,
            prices_csv_path=prices_out,
            options_csv_path=options_out,
        )

    return len(rows_prices), prices_out, options_out, plot_paths


def main() -> None:
    args = parse_args()
    if args.option_tenor_days <= 0:
        raise ValueError("option-tenor-days must be positive.")
    if args.iv_multiplier <= 0:
        raise ValueError("iv-multiplier must be positive.")
    if args.iv_proxy_scale <= 0:
        raise ValueError("iv-proxy-scale must be positive.")
    if args.risk_free_rate < 0:
        raise ValueError("risk-free-rate must be non-negative.")

    rv_window = args.rv_window if args.rv_window is not None else default_rv_window(args.interval)
    if rv_window <= 1:
        raise ValueError("rv-window must be greater than 1.")

    start_date, end_date = resolve_date_bounds(
        year=args.year,
        interval=args.interval,
        start_date_text=args.start_date,
        end_date_text=args.end_date,
    )

    try:
        count, prices_path, options_path, plot_paths = generate(
            symbol=args.symbol.strip().upper(),
            year=args.year,
            start_date=start_date,
            end_date=end_date,
            interval=args.interval,
            rv_window=rv_window,
            option_tenor_days=args.option_tenor_days,
            iv_multiplier=args.iv_multiplier,
            iv_spread=args.iv_spread,
            iv_proxy_symbol=args.iv_proxy_symbol,
            iv_proxy_scale=args.iv_proxy_scale,
            risk_free_rate=args.risk_free_rate,
            skip_plots=args.skip_plots,
        )
    except RuntimeError as exc:
        raise SystemExit(f"Error: {exc}") from exc

    print(f"Generated {count} rows.")
    print(f"Interval: {args.interval}")
    print(f"Date range used: {start_date} to {end_date}")
    print(f"RV window: {rv_window} bars")
    print(prices_path)
    print(options_path)
    for path in plot_paths:
        print(path)


if __name__ == "__main__":
    main()
