"""Option trade execution model with costs."""

from dataclasses import dataclass


CONTRACT_MULTIPLIER = 100


@dataclass
class OptionTradeResult:
    new_contracts: int
    cash: float
    traded_contracts: int
    notional_traded: float
    fees: float
    slippage: float


def execute_option_trade(
    current_contracts: int,
    target_contracts: int,
    option_price: float,
    cash: float,
    fee_per_contract: float = 0.65,
    slippage_bps: float = 5.0,
) -> OptionTradeResult:
    trade_qty = target_contracts - current_contracts
    notional = abs(trade_qty) * option_price * CONTRACT_MULTIPLIER
    fees = abs(trade_qty) * fee_per_contract
    slippage = notional * (slippage_bps / 10_000.0)

    # Buy => trade_qty > 0 decreases cash; sell does the opposite.
    cash_change = -(trade_qty * option_price * CONTRACT_MULTIPLIER) - fees - slippage
    new_cash = cash + cash_change

    return OptionTradeResult(
        new_contracts=target_contracts,
        cash=new_cash,
        traded_contracts=trade_qty,
        notional_traded=notional,
        fees=fees,
        slippage=slippage,
    )
