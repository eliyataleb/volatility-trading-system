"""PnL decomposition utilities."""

from dataclasses import dataclass
from typing import Dict


@dataclass
class PnLBreakdown:
    option_mtm_pnl: float = 0.0
    hedge_pnl: float = 0.0
    fees: float = 0.0
    slippage: float = 0.0

    def record_mtm(self, option_mtm: float, hedge_mtm: float) -> None:
        self.option_mtm_pnl += option_mtm
        self.hedge_pnl += hedge_mtm

    def record_costs(self, fees: float, slippage: float) -> None:
        self.fees += fees
        self.slippage += slippage

    @property
    def total_pnl(self) -> float:
        return self.option_mtm_pnl + self.hedge_pnl - self.fees - self.slippage

    def as_dict(self) -> Dict[str, float]:
        return {
            "option_mtm_pnl": self.option_mtm_pnl,
            "hedge_pnl": self.hedge_pnl,
            "fees": -self.fees,
            "slippage": -self.slippage,
            "total_pnl": self.total_pnl,
        }
