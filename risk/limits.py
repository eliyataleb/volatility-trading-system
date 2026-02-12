"""Base risk limits for position/risk budget checks."""

from dataclasses import dataclass
from typing import Tuple


CONTRACT_MULTIPLIER = 100


@dataclass
class RiskLimits:
    initial_capital: float
    max_capital_at_risk: float = 0.20
    max_leverage: float = 6.0
    max_abs_gamma: float = 75.0
    max_abs_vega: float = 300.0

    def trade_allowed(
        self,
        projected_option_contracts: int,
        option_price: float,
        projected_notional: float,
        projected_gamma_abs: float,
        projected_vega_abs: float,
        projected_equity: float,
    ) -> Tuple[bool, str]:
        if projected_equity <= 0:
            return False, "Equity exhausted"

        capital_at_risk_ratio = (
            abs(projected_option_contracts * option_price * CONTRACT_MULTIPLIER)
            / projected_equity
        )
        if capital_at_risk_ratio > self.max_capital_at_risk:
            return False, "Blocked: capital-at-risk limit breached"

        leverage = projected_notional / projected_equity
        if leverage > self.max_leverage:
            return False, "Blocked: leverage limit breached"
        if projected_gamma_abs > self.max_abs_gamma:
            return False, "Blocked: gamma limit breached"
        if projected_vega_abs > self.max_abs_vega:
            return False, "Blocked: vega limit breached"

        return True, ""
