"""Unified regime-filtered signal logic for long/short volatility modes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence


StrategyMode = Literal["short", "long", "adaptive"]
Stance = Literal["SHORT_VOL", "LONG_VOL", "FLAT", "PAUSED"]


@dataclass
class SignalConfig:
    rv_short_window: int = 30
    rv_medium_window: int = 240
    trend_window: int = 120
    long_chop_window: int = 30

    short_edge_threshold: float = 0.02
    short_edge_collapse_tolerance: float = 0.005
    short_trend_threshold: float = 0.004
    short_jump_threshold: float = 0.006
    short_rv_change_threshold: float = 0.06

    long_edge_threshold: float = 0.015
    long_max_iv_premium: float = 0.03
    long_edge_collapse_tolerance: float = 0.005
    long_rv_rise_threshold: float = 0.003
    long_chop_threshold: float = 0.00025
    long_two_way_chop_score_threshold: float = 2.0
    long_two_way_trend_threshold: float = 0.008

    cooldown_bars: int = 30
    adaptive_enter_persist_bars: int = 3
    adaptive_exit_persist_bars: int = 2
    adaptive_pause_bars: int = 30
    adaptive_short_edge_enter: float = 0.02
    adaptive_short_edge_exit: float = 0.01
    adaptive_short_trend_enter: float = 0.004
    adaptive_short_trend_exit: float = 0.006
    adaptive_vov_low: float = 0.003
    adaptive_vov_high: float = 0.006
    adaptive_vov_exit: float = 0.004
    adaptive_long_cheapness_enter: float = 0.003
    adaptive_long_cheapness_exit: float = 0.0015
    adaptive_long_trend_max: float = 0.008
    adaptive_confidence_buffer: float = 0.001

    @property
    def min_warmup_bars(self) -> int:
        return max(self.rv_short_window, self.rv_medium_window, self.trend_window)


@dataclass
class SignalDecision:
    stance: Stance
    signal: int
    reason: str

    pricing_filter_passed: bool
    path_filter_passed: bool
    instability_filter_passed: bool
    two_way_filter_passed: bool

    edge: float
    edge_velocity: float
    rv_short: float
    rv_medium: float
    trend_strength: float
    jump_abs_return: float
    rv_change: float
    choppiness: float
    chop_score: float
    cooldown_remaining: int
    cooldown_active: bool


def _rolling_mean(values: Sequence[float], window: int) -> float:
    if not values:
        return 0.0
    w = max(1, min(window, len(values)))
    subset = values[-w:]
    return sum(subset) / float(len(subset))


def _safe_price_trend_strength(spot: float, prices: Sequence[float], window: int) -> float:
    baseline = _rolling_mean(prices, window)
    if baseline <= 0:
        return 0.0
    return abs((spot / baseline) - 1.0)


def _signal_for_stance(stance: Stance) -> int:
    if stance == "LONG_VOL":
        return 1
    if stance == "SHORT_VOL":
        return -1
    return 0


class RegimeSignalEngine:
    """Stateful signal engine producing SHORT_VOL / LONG_VOL / FLAT stances."""

    def __init__(self, mode: StrategyMode, config: SignalConfig) -> None:
        self.mode = mode
        self.config = config
        self.prev_edge: float | None = None
        self.prev_stance: Stance = "FLAT"
        self.cooldown_remaining = 0
        self.adaptive_state: Stance = "FLAT"
        self.adaptive_pause_remaining = 0
        self.adaptive_short_enter_count = 0
        self.adaptive_long_enter_count = 0
        self.adaptive_short_exit_count = 0
        self.adaptive_long_exit_count = 0
        self.adaptive_pause_reason = ""

    def force_pause(self, bars: int, reason: str) -> None:
        pause_bars = max(0, bars)
        if pause_bars <= 0:
            return
        if self.mode in {"adaptive", "long"}:
            self.adaptive_state = "PAUSED"
            self.adaptive_pause_remaining = max(self.adaptive_pause_remaining, pause_bars)
            self.adaptive_short_enter_count = 0
            self.adaptive_long_enter_count = 0
            self.adaptive_short_exit_count = 0
            self.adaptive_long_exit_count = 0
            self.adaptive_pause_reason = reason
            self.prev_stance = "PAUSED"
        else:
            self.cooldown_remaining = max(self.cooldown_remaining, pause_bars)

    def _adaptive_short_enter_ok(self, metrics: dict[str, float]) -> bool:
        return (
            metrics["edge"] > self.config.adaptive_short_edge_enter
            and metrics["trend_strength"] < self.config.adaptive_short_trend_enter
            and metrics["rv_change"] < self.config.adaptive_vov_low
        )

    def _adaptive_short_exit_ok(self, metrics: dict[str, float]) -> bool:
        return (
            metrics["edge"] < self.config.adaptive_short_edge_exit
            or metrics["trend_strength"] > self.config.adaptive_short_trend_exit
            or metrics["rv_change"] > self.config.adaptive_vov_high
        )

    def _adaptive_long_enter_ok(self, metrics: dict[str, float]) -> bool:
        cheapness = -metrics["edge"]
        return (
            cheapness > self.config.adaptive_long_cheapness_enter
            and metrics["rv_change"] > self.config.adaptive_vov_high
            and metrics["trend_strength"] < self.config.adaptive_long_trend_max
        )

    def _adaptive_long_exit_ok(self, metrics: dict[str, float]) -> bool:
        cheapness = -metrics["edge"]
        return (
            cheapness < self.config.adaptive_long_cheapness_exit
            or metrics["rv_change"] < self.config.adaptive_vov_exit
        )

    def _base_metrics(
        self,
        *,
        implied_vol: float,
        realized_vol_series: Sequence[float],
        spot: float,
        price_series: Sequence[float],
        return_series: Sequence[float],
    ) -> dict[str, float]:
        rv_short = _rolling_mean(realized_vol_series, self.config.rv_short_window)
        rv_medium = _rolling_mean(realized_vol_series, self.config.rv_medium_window)
        edge = implied_vol - rv_short
        if self.prev_edge is None:
            edge_velocity = 0.0
        else:
            edge_velocity = edge - self.prev_edge
        self.prev_edge = edge

        trend_strength = _safe_price_trend_strength(spot, price_series, self.config.trend_window)
        jump_abs_return = abs(return_series[-1]) if return_series else 0.0
        rv_change = abs(rv_short - rv_medium)
        choppiness = _rolling_mean([abs(r) for r in return_series], self.config.long_chop_window)
        avg_return = _rolling_mean(return_series, self.config.long_chop_window)
        denom = abs(avg_return)
        chop_score = choppiness / denom if denom > 1e-12 else float("inf")

        return {
            "rv_short": rv_short,
            "rv_medium": rv_medium,
            "edge": edge,
            "edge_velocity": edge_velocity,
            "trend_strength": trend_strength,
            "jump_abs_return": jump_abs_return,
            "rv_change": rv_change,
            "choppiness": choppiness,
            "chop_score": chop_score,
        }

    def decide(
        self,
        *,
        implied_vol: float,
        realized_vol_series: Sequence[float],
        spot: float,
        price_series: Sequence[float],
        return_series: Sequence[float],
    ) -> SignalDecision:
        metrics = self._base_metrics(
            implied_vol=implied_vol,
            realized_vol_series=realized_vol_series,
            spot=spot,
            price_series=price_series,
            return_series=return_series,
        )

        pricing_ok = False
        path_ok = True
        instability_ok = True
        two_way_ok = True
        reason = "FLAT: warmup"
        stance: Stance = "FLAT"
        cooldown_active = False

        if len(price_series) < self.config.min_warmup_bars:
            stance = "FLAT"
            reason = "FLAT: warmup"
        elif self.mode == "adaptive":
            enter_persist = max(1, self.config.adaptive_enter_persist_bars)
            exit_persist = max(1, self.config.adaptive_exit_persist_bars)

            short_enter_ok = self._adaptive_short_enter_ok(metrics)
            long_enter_ok = self._adaptive_long_enter_ok(metrics)
            short_exit_ok = self._adaptive_short_exit_ok(metrics)
            long_exit_ok = self._adaptive_long_exit_ok(metrics)

            pricing_ok = short_enter_ok or long_enter_ok
            path_ok = short_enter_ok
            instability_ok = long_enter_ok
            two_way_ok = metrics["trend_strength"] < self.config.adaptive_long_trend_max

            if self.adaptive_state == "PAUSED":
                if self.adaptive_pause_remaining > 0:
                    self.adaptive_pause_remaining -= 1
                cooldown_active = True
                self.cooldown_remaining = self.adaptive_pause_remaining
                stance = "PAUSED"
                pause_reason = (
                    self.adaptive_pause_reason if self.adaptive_pause_reason else "cooldown"
                )
                reason = f"PAUSED: {pause_reason}"
                if self.adaptive_pause_remaining <= 0:
                    self.adaptive_state = "FLAT"
                    self.adaptive_pause_reason = ""
                    stance = "FLAT"
                    reason = "FLAT: pause complete"
            elif self.adaptive_state == "SHORT_VOL":
                if short_exit_ok:
                    self.adaptive_short_exit_count += 1
                else:
                    self.adaptive_short_exit_count = 0
                if self.adaptive_short_exit_count >= exit_persist:
                    self.adaptive_state = "PAUSED"
                    self.adaptive_pause_remaining = max(0, self.config.adaptive_pause_bars)
                    self.adaptive_pause_reason = "short exit"
                    self.adaptive_short_exit_count = 0
                    self.adaptive_short_enter_count = 0
                    self.adaptive_long_enter_count = 0
                    cooldown_active = self.adaptive_pause_remaining > 0
                    self.cooldown_remaining = self.adaptive_pause_remaining
                    stance = "PAUSED" if cooldown_active else "FLAT"
                    reason = "PAUSED: short exit"
                else:
                    stance = "SHORT_VOL"
                    reason = "SHORT: regime active"
            elif self.adaptive_state == "LONG_VOL":
                if long_exit_ok:
                    self.adaptive_long_exit_count += 1
                else:
                    self.adaptive_long_exit_count = 0
                if self.adaptive_long_exit_count >= exit_persist:
                    self.adaptive_state = "PAUSED"
                    self.adaptive_pause_remaining = max(0, self.config.adaptive_pause_bars)
                    self.adaptive_pause_reason = "long exit"
                    self.adaptive_long_exit_count = 0
                    self.adaptive_short_enter_count = 0
                    self.adaptive_long_enter_count = 0
                    cooldown_active = self.adaptive_pause_remaining > 0
                    self.cooldown_remaining = self.adaptive_pause_remaining
                    stance = "PAUSED" if cooldown_active else "FLAT"
                    reason = "PAUSED: long exit"
                else:
                    stance = "LONG_VOL"
                    reason = "LONG: regime active"

            if self.adaptive_state == "FLAT":
                if short_enter_ok:
                    self.adaptive_short_enter_count += 1
                else:
                    self.adaptive_short_enter_count = 0
                if long_enter_ok:
                    self.adaptive_long_enter_count += 1
                else:
                    self.adaptive_long_enter_count = 0

                selected_stance: Stance = "FLAT"
                if (
                    self.adaptive_short_enter_count >= enter_persist
                    and self.adaptive_long_enter_count >= enter_persist
                ):
                    short_strength = (
                        (metrics["edge"] - self.config.adaptive_short_edge_enter)
                        + (self.config.adaptive_short_trend_enter - metrics["trend_strength"])
                        + (self.config.adaptive_vov_low - metrics["rv_change"])
                    )
                    long_strength = (
                        ((-metrics["edge"]) - self.config.adaptive_long_cheapness_enter)
                        + (metrics["rv_change"] - self.config.adaptive_vov_high)
                        + (self.config.adaptive_long_trend_max - metrics["trend_strength"])
                    )
                    if abs(short_strength - long_strength) >= self.config.adaptive_confidence_buffer:
                        selected_stance = (
                            "SHORT_VOL" if short_strength >= long_strength else "LONG_VOL"
                        )
                elif self.adaptive_short_enter_count >= enter_persist:
                    selected_stance = "SHORT_VOL"
                elif self.adaptive_long_enter_count >= enter_persist:
                    selected_stance = "LONG_VOL"

                if selected_stance == "SHORT_VOL":
                    self.adaptive_state = "SHORT_VOL"
                    self.adaptive_short_exit_count = 0
                    self.adaptive_long_exit_count = 0
                    stance = "SHORT_VOL"
                    reason = "SHORT: adaptive enter persisted"
                elif selected_stance == "LONG_VOL":
                    self.adaptive_state = "LONG_VOL"
                    self.adaptive_short_exit_count = 0
                    self.adaptive_long_exit_count = 0
                    stance = "LONG_VOL"
                    reason = "LONG: adaptive enter persisted"
                else:
                    stance = "FLAT"
                    reason = "FLAT: no adaptive regime"
            self.cooldown_remaining = self.adaptive_pause_remaining
        elif self.mode == "long":
            enter_persist = max(1, self.config.adaptive_enter_persist_bars)
            exit_persist = max(1, self.config.adaptive_exit_persist_bars)

            long_enter_ok = self._adaptive_long_enter_ok(metrics)
            long_exit_ok = self._adaptive_long_exit_ok(metrics)

            pricing_ok = long_enter_ok
            instability_ok = long_enter_ok
            two_way_ok = metrics["trend_strength"] < self.config.adaptive_long_trend_max
            path_ok = True

            if self.adaptive_state == "PAUSED":
                if self.adaptive_pause_remaining > 0:
                    self.adaptive_pause_remaining -= 1
                cooldown_active = True
                self.cooldown_remaining = self.adaptive_pause_remaining
                stance = "PAUSED"
                pause_reason = (
                    self.adaptive_pause_reason if self.adaptive_pause_reason else "cooldown"
                )
                reason = f"PAUSED: {pause_reason}"
                if self.adaptive_pause_remaining <= 0:
                    self.adaptive_state = "FLAT"
                    self.adaptive_pause_reason = ""
                    stance = "FLAT"
                    reason = "FLAT: pause complete"
            elif self.adaptive_state == "LONG_VOL":
                if long_exit_ok:
                    self.adaptive_long_exit_count += 1
                else:
                    self.adaptive_long_exit_count = 0
                if self.adaptive_long_exit_count >= exit_persist:
                    self.adaptive_state = "PAUSED"
                    self.adaptive_pause_remaining = max(0, self.config.adaptive_pause_bars)
                    self.adaptive_pause_reason = "long exit"
                    self.adaptive_long_exit_count = 0
                    self.adaptive_long_enter_count = 0
                    cooldown_active = self.adaptive_pause_remaining > 0
                    self.cooldown_remaining = self.adaptive_pause_remaining
                    stance = "PAUSED" if cooldown_active else "FLAT"
                    reason = "PAUSED: long exit"
                else:
                    stance = "LONG_VOL"
                    reason = "LONG: regime active"
            else:
                # Long-only mode shares adaptive-long semantics, so any non-long state is FLAT.
                self.adaptive_state = "FLAT"
                if long_enter_ok:
                    self.adaptive_long_enter_count += 1
                else:
                    self.adaptive_long_enter_count = 0

                if self.adaptive_long_enter_count >= enter_persist:
                    self.adaptive_state = "LONG_VOL"
                    self.adaptive_long_exit_count = 0
                    stance = "LONG_VOL"
                    reason = "LONG: adaptive enter persisted"
                else:
                    stance = "FLAT"
                    reason = "FLAT: no long adaptive regime"
            self.cooldown_remaining = self.adaptive_pause_remaining
        elif self.cooldown_remaining > 0:
            stance = "FLAT"
            reason = "FLAT: cooldown"
            cooldown_active = True
            self.cooldown_remaining -= 1
        elif self.mode == "short":
            pricing_ok = (
                metrics["edge"] >= self.config.short_edge_threshold
                and metrics["edge_velocity"] >= -self.config.short_edge_collapse_tolerance
            )
            path_ok = (
                metrics["trend_strength"] <= self.config.short_trend_threshold
                and metrics["jump_abs_return"] <= self.config.short_jump_threshold
                and metrics["rv_change"] <= self.config.short_rv_change_threshold
            )
            if pricing_ok and path_ok:
                stance = "SHORT_VOL"
                reason = "SHORT: edge+path OK"
            else:
                stance = "FLAT"
                failed = []
                if not pricing_ok:
                    failed.append("pricing")
                if not path_ok:
                    failed.append("path")
                reason = f"FLAT: short gate fail ({'+'.join(failed)})"
        else:
            cheapness = -metrics["edge"]
            relative_premium_ok = metrics["edge"] <= self.config.long_max_iv_premium
            pricing_ok = (
                (cheapness >= self.config.long_edge_threshold or relative_premium_ok)
                and metrics["edge_velocity"] <= self.config.long_edge_collapse_tolerance
            )
            two_way_ok = (
                metrics["trend_strength"] <= self.config.long_two_way_trend_threshold
                or metrics["chop_score"] >= self.config.long_two_way_chop_score_threshold
            )
            instability_ok = (
                (metrics["rv_short"] - metrics["rv_medium"])
                >= self.config.long_rv_rise_threshold
                and metrics["choppiness"] >= self.config.long_chop_threshold
                and two_way_ok
            )
            if pricing_ok and instability_ok:
                stance = "LONG_VOL"
                reason = "LONG: pricing+instability OK"
            else:
                stance = "FLAT"
                failed = []
                if not pricing_ok:
                    failed.append("pricing")
                if not instability_ok:
                    failed.append("instability")
                if not two_way_ok:
                    failed.append("two_way")
                reason = f"FLAT: long gate fail ({'+'.join(failed)})"

        if (
            self.mode == "short"
            and stance == "FLAT"
            and self.prev_stance != "FLAT"
            and self.config.cooldown_bars > 0
        ):
            self.cooldown_remaining = self.config.cooldown_bars

        self.prev_stance = stance

        return SignalDecision(
            stance=stance,
            signal=_signal_for_stance(stance),
            reason=reason,
            pricing_filter_passed=pricing_ok,
            path_filter_passed=path_ok,
            instability_filter_passed=instability_ok,
            two_way_filter_passed=two_way_ok,
            edge=metrics["edge"],
            edge_velocity=metrics["edge_velocity"],
            rv_short=metrics["rv_short"],
            rv_medium=metrics["rv_medium"],
            trend_strength=metrics["trend_strength"],
            jump_abs_return=metrics["jump_abs_return"],
            rv_change=metrics["rv_change"],
            choppiness=metrics["choppiness"],
            chop_score=metrics["chop_score"],
            cooldown_remaining=self.cooldown_remaining,
            cooldown_active=cooldown_active,
        )
