"""Position sizing helpers."""


CONTRACT_MULTIPLIER = 100


def target_option_contracts(
    signal: int,
    option_price: float,
    capital_base: float,
    max_capital_at_risk: float,
    size_factor: float = 1.0,
) -> int:
    """
    Convert a directional signal into a contract count constrained by risk budget.
    `capital_base` is typically current equity/NAV.
    """
    if signal == 0 or option_price <= 0 or capital_base <= 0:
        return 0

    bounded_size = max(0.0, min(size_factor, 1.0))
    risk_budget = capital_base * max_capital_at_risk * bounded_size
    per_contract_notional = option_price * CONTRACT_MULTIPLIER
    contracts = int(risk_budget // per_contract_notional)
    if contracts <= 0:
        return 0

    return contracts if signal > 0 else -contracts


def target_contracts_by_vega_budget(
    signal: int,
    capital_base: float,
    option_vega: float,
    max_abs_vega_ratio: float,
    size_factor: float = 1.0,
) -> int:
    """
    Size contracts by a vega exposure budget:
    target_abs_vega = capital_base * max_abs_vega_ratio * size_factor.
    """
    if signal == 0 or capital_base <= 0 or option_vega <= 0:
        return 0

    bounded_size = max(0.0, min(size_factor, 1.0))
    target_abs_vega = capital_base * max(0.0, max_abs_vega_ratio) * bounded_size
    per_contract_vega = option_vega * CONTRACT_MULTIPLIER
    contracts = int(target_abs_vega // per_contract_vega)
    if contracts <= 0:
        return 0
    return contracts if signal > 0 else -contracts
