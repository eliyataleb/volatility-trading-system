# QUANT PNL: Delta-Hedged Volatility Strategy (Historical Replay)

This project implements a constrained options trading simulator focused on risk engineering, not headline returns.
Data resolution follows the input dataset (real-data generation defaults to minute bars).

## Scope & Limitations

This system is intended for research and educational purposes.
It does NOT claim live profitability and deliberately trades under
simplified option proxies and execution assumptions.

The focus is on:
- regime selection
- hedging bleed
- convexity exposure
- risk containment under stress

## Strategy

- Universe: `SPY` options + `SPY` shares for delta hedge
- Unified stance output: `SHORT_VOL`, `LONG_VOL`, `FLAT`
- Two strategy modes (same engine/risk/execution stack):
  - `short`: regime-filtered short vol (IV rich + benign path filters)
  - `long`: selective long vol (IV cheap + instability confirmation)
  - `both`: runs both modes on the same data for direct comparison
  - `adaptive`: single-book regime state machine that chooses one stance at a time
    - States: `FLAT`, `SHORT_VOL`, `LONG_VOL`, `PAUSED`
    - Uses hysteresis (`enter_persist` / `exit_persist`) to avoid flip-flopping
    - Any side switch is forced through `FLAT` + cooldown (`PAUSED`)
- Hedge: Rebalance shares each step to keep net delta near zero

## Hard Constraints

- Initial capital: `$10,000`
- Position sizing uses current equity/NAV (not static initial capital)
- Max capital at risk (option premium): `20%` of current equity
- Max leverage: configurable via `--max-leverage` (default `6.0x`)
- Hard greek caps:
  - `abs(gamma_exposure) <= --max-abs-gamma` (default `75`)
  - `abs(vega_exposure) <= --max-abs-vega` (default `300`)
- Gamma-band policy:
  - Green zone: `gamma <= G1` -> size `1.0`
  - Yellow zone: `G1 < gamma <= G2` -> size `yellow_factor` (default `0.50`)
  - Red zone: `gamma > G2` -> size `red_factor` (default `0.25`)
- Flatten kill condition:
  - Flatten only if `gamma > G2` AND `drawdown > D1`
- Global drawdown controls (independent of gamma):
  - Size throttle at `--global-drawdown-throttle-threshold` (default `10%`)
  - Flatten at `--global-drawdown-kill-threshold` (default `20%`)
- Strategy gate behavior:
  - Short-vol is hard-gated in orchestration: if required filters fail, target is forced `FLAT`
  - Cooldown bars can keep strategy flat after exits

## Required PnL Decomposition

The simulator writes and reports:

- Option MTM PnL
- Delta hedge PnL
- Fees
- Slippage
- Total PnL

## Project Layout

```text
data/
  generate_real_data.py
  <symbol>_<year>_prices.csv
  <symbol>_<year>_options.csv

strategy/
  signal.py
  position_sizing.py

risk/
  limits.py
  kill_switch.py

execution/
  hedge.py
  trades.py

analytics/
  pnl.py
  exposure.py
  drawdown.py
  plots.py

main.py
```

## Run

1) Generate input data:

Real market replay from Yahoo Finance. For a full year (for example all of 2025), use daily bars:

```bash
python data/generate_real_data.py --symbol SPY --year 2025 --interval 1d --start-date 2025-01-01 --end-date 2025-12-31 --iv-proxy-symbol '^VIX'
```

For intraday intervals, if `--start-date`/`--end-date` are omitted, the generator defaults to a recent window:
- `1m`: last 30 calendar days ending today
- `2m`/`5m`/`15m`/`30m`/`1h`: last 60 calendar days ending today

Yahoo does not provide full-year historical `1m` bars; use `--interval 1d` for year-long backtests.

`generate_real_data.py` writes the same CSV schema expected by `main.py`:

- `data/<symbol_lower>_<year>_prices.csv` with real closes + computed realized vol
- `data/<symbol_lower>_<year>_options.csv` with deterministic ATM option proxy fields

The generator also writes companion data plots in `data/`:

- `data/<symbol_lower>_<year>_prices.png`
- `data/<symbol_lower>_<year>_options.png`

If you only want CSVs:

```bash
python data/generate_real_data.py --symbol SPY --year 2025 --interval 1d --start-date 2025-01-01 --end-date 2025-12-31 --skip-plots
```

2) Run both strategies on that dataset:

```bash
python main.py --symbol SPY --year 2025 --strategy-mode both --start-date 2025-01-01 --end-date 2025-12-31
```

`--start-date` and `--end-date` accept either `YYYY-MM-DD` or `YYYY-MM-DD HH:MM`.

Run only short-vol mode:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode short
```

Run only long-vol mode:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode long
```

Run with custom gamma bands and mode filters:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode both --gamma-green-threshold 4.0 --gamma-red-threshold 8.0 --gamma-yellow-size-factor 0.6 --gamma-red-size-factor 0.3 --gamma-kill-drawdown-threshold 0.10 --short-edge-threshold 0.02 --long-edge-threshold 0.015 --cooldown-bars 30
```

Run adaptive regime mode:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode adaptive
```

Adaptive mode uses regime thresholds with hysteresis:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode adaptive \
  --adaptive-enter-persist-bars 3 \
  --adaptive-exit-persist-bars 2 \
  --adaptive-confidence-buffer 0.001 \
  --adaptive-short-edge-enter 0.02 \
  --adaptive-short-edge-exit 0.01 \
  --adaptive-vov-low 0.003 \
  --adaptive-vov-high 0.006 \
  --adaptive-long-cheapness-enter 0.003 \
  --adaptive-long-cheapness-exit 0.0015
```

`timestep_log*.csv` includes requested vs executed target exposure columns (`requested_*`, `executed_target_*`) to diagnose sizing/risk clipping.

Run with explicit global drawdown + leverage controls:

```bash
python main.py --symbol SPY --year 2026 --strategy-mode both --max-leverage 4.0 --global-drawdown-throttle-threshold 0.08 --global-drawdown-throttle-size-factor 0.5 --global-drawdown-kill-threshold 0.15
```

Faster/no-plot run (useful for quick checks):

```bash
python main.py --symbol SPY --year 2026 --strategy-mode both --skip-plots
```

Progress prints are enabled by default every 10,000 bars. Change or disable with:

```bash
python main.py --symbol SPY --year 2026 --progress-every 5000
python main.py --symbol SPY --year 2026 --progress-every 0
```

You can run a sub-period too:

```bash
python main.py --symbol SPY --year 2026 --start-date 2026-01-15 --end-date 2026-02-11
```

You can run an intraday window too:

```bash
python main.py --symbol SPY --year 2026 --start-date "2026-02-10 09:30" --end-date "2026-02-10 15:59"
```

File naming rule:

- `main.py` expects exactly:
  - `data/<symbol_lower>_<year>_prices.csv`
  - `data/<symbol_lower>_<year>_options.csv`
- Example: `--symbol SPY --year 2026` => `data/spy_2026_prices.csv` and `data/spy_2026_options.csv`
- Data generator also outputs:
  - `data/<symbol_lower>_<year>_prices.png`
  - `data/<symbol_lower>_<year>_options.png`

Outputs are written to:

- Single mode (`short`, `long`, or `adaptive`):
  - `results/timestep_log.csv`
  - `results/pnl_summary.csv`
  - `results/equity_curve.csv`
  - `results/events.log`
  - `results/*.png` plot files
- Both modes (`--strategy-mode both`):
  - `results/timestep_log_short.csv` and `results/timestep_log_long.csv`
  - `results/pnl_summary_short.csv` and `results/pnl_summary_long.csv`
  - `results/equity_curve_short.csv` and `results/equity_curve_long.csv`
  - `results/events_short.log` and `results/events_long.log`
  - `results/pnl_summary_all_modes.csv`
  - `results/*_short.png` and `results/*_long.png`

To generate deliverable figures (requires `matplotlib`):

```bash
python analytics/plots.py
```

This writes:

- `results/plot_equity_curve.png`
- `results/plot_cumulative_pnl.png`
- `results/plot_drawdown.png`
- `results/plot_greeks.png`
- `results/plot_capital_usage.png`
- `results/plot_iv_minus_rv.png`
- `results/plot_vol_of_vol.png`
- `results/plot_trend_strength.png`
- If only `timestep_log_short.csv`/`timestep_log_long.csv` exist, it auto-writes suffixed plot files for each mode.

## Sample Run (Current Data)

- Dataset example: `QQQ 2025` real minute bars from Yahoo Finance
- Approx rows: depends on date range and Yahoo intraday availability limits

## Notes

- `generate_real_data.py` uses real underlying close data from Yahoo Finance, then computes option fields (`option_mid`, greeks, IV) via a deterministic ATM Black-Scholes proxy so the existing engine can run unchanged.
- If an `--iv-proxy-symbol` fetch fails, the generator automatically falls back to `IV = RV * iv-multiplier + iv-spread`.
- `main.py` auto-detects daily timestamps and applies a built-in daily signal preset when signal parameters are left at defaults.
- Historical full-depth option-chain replay is not sourced by Yahoo in this setup; treat the real-data option leg as a practical proxy, not broker-grade options history.
- For intraday intervals, Yahoo lookback limits apply; as of February 11, 2026, `1m` is typically only available for recent history, so pass explicit `--start-date` and `--end-date` in a recent range.
- You can generate other years with `--year` later without changing code.
- In `both` mode, each strategy runs independently with identical costs/risk guards for apples-to-apples comparison.
- Execution ordering uses next-bar fills: signal at `t`, trade at `t+1`.
