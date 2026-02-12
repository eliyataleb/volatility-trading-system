"""Drawdown tracking."""

from dataclasses import dataclass


@dataclass
class DrawdownTracker:
    peak_equity: float
    max_drawdown: float = 0.0

    def peek(self, equity: float) -> float:
        """Compute drawdown without mutating tracker state."""
        peak = max(self.peak_equity, equity)
        if peak <= 0:
            return 0.0
        return max(0.0, (peak - equity) / peak)

    def update(self, equity: float) -> float:
        if equity > self.peak_equity:
            self.peak_equity = equity

        current_drawdown = self.peek(equity)

        if current_drawdown > self.max_drawdown:
            self.max_drawdown = current_drawdown

        return current_drawdown
