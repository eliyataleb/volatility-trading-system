"""Exposure calculations."""

from typing import Dict


CONTRACT_MULTIPLIER = 100


def compute_exposures(
    option_contracts: int,
    hedge_shares: int,
    spot_price: float,
    option_price: float,
    option_delta: float,
    option_gamma: float,
    option_vega: float,
) -> Dict[str, float]:
    option_delta_shares = option_contracts * option_delta * CONTRACT_MULTIPLIER
    total_delta = option_delta_shares + hedge_shares
    gamma_exposure = option_contracts * option_gamma * CONTRACT_MULTIPLIER
    vega_exposure = option_contracts * option_vega * CONTRACT_MULTIPLIER
    option_notional = abs(option_contracts * option_price * CONTRACT_MULTIPLIER)
    hedge_notional = abs(hedge_shares * spot_price)
    notional_exposure = option_notional + hedge_notional

    return {
        "delta_exposure": total_delta,
        "gamma_exposure": gamma_exposure,
        "vega_exposure": vega_exposure,
        "option_notional": option_notional,
        "hedge_notional": hedge_notional,
        "notional_exposure": notional_exposure,
    }
