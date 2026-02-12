"""Delta hedge execution model with costs."""

from dataclasses import dataclass


@dataclass
class HedgeTradeResult:
    new_shares: int
    cash: float
    traded_shares: int
    notional_traded: float
    fees: float
    slippage: float


def rebalance_delta_hedge(
    current_shares: int,
    target_shares: int,
    spot_price: float,
    cash: float,
    fee_per_share: float = 0.005,
    slippage_bps: float = 1.0,
) -> HedgeTradeResult:
    trade_shares = target_shares - current_shares
    notional = abs(trade_shares) * spot_price
    fees = abs(trade_shares) * fee_per_share
    slippage = notional * (slippage_bps / 10_000.0)

    # Buy shares => positive trade_shares reduces cash.
    cash_change = -(trade_shares * spot_price) - fees - slippage
    new_cash = cash + cash_change

    return HedgeTradeResult(
        new_shares=target_shares,
        cash=new_cash,
        traded_shares=trade_shares,
        notional_traded=notional,
        fees=fees,
        slippage=slippage,
    )
