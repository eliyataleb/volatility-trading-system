"""Gamma-band throttling and drawdown-conditioned kill switch."""

from dataclasses import dataclass, field
from typing import List


@dataclass
class KillSwitchAction:
    flatten_positions: bool = False
    size_factor: float = 1.0
    zone: str = "green"
    reasons: List[str] = field(default_factory=list)


@dataclass
class KillSwitch:
    gamma_green_threshold: float = 5.0  # G1
    gamma_red_threshold: float = 10.0  # G2
    gamma_yellow_size_factor: float = 0.50
    gamma_red_size_factor: float = 0.25
    kill_drawdown_threshold: float = 0.12  # D1

    def evaluate(
        self,
        gamma_risk: float,
        total_drawdown: float,
    ) -> KillSwitchAction:
        """
        Exact policy:
        - Green: gamma <= G1, size=1.0
        - Yellow: G1 < gamma <= G2, size=yellow_factor
        - Red: gamma > G2, size=red_factor
        - Flatten only if gamma > G2 AND drawdown > D1
        """
        action = KillSwitchAction()

        if gamma_risk <= self.gamma_green_threshold:
            action.zone = "green"
            action.size_factor = 1.0
            return action

        if gamma_risk <= self.gamma_red_threshold:
            action.zone = "yellow"
            action.size_factor = self.gamma_yellow_size_factor
            action.reasons.append("GAMMA_YELLOW_THROTTLE")
            return action

        action.zone = "red"
        action.size_factor = self.gamma_red_size_factor
        action.reasons.append("GAMMA_RED_THROTTLE")

        if total_drawdown > self.kill_drawdown_threshold:
            action.flatten_positions = True
            action.reasons.append("GAMMA_RED_DRAWDOWN_KILL")

        return action
