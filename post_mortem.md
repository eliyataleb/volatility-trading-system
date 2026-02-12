# Post-Mortem: Delta-Hedged Volatility Trading System

## 1. Objective

This project studied systematic volatility trading using delta-neutral options portfolios with explicit Greek-based risk controls.

The goal was **not** to predict market direction or maximize headline returns, but to understand:
- how volatility strategies generate or lose money,
- how convexity behaves across regimes,
- and why risk controls alone do not create positive expectancy.

---

## 2. Initial Hypothesis

The initial hypothesis was that a continuously traded, delta-hedged options strategy could extract volatility risk premium while controlling tail risk through:
- gamma and vega caps,
- drawdown-based throttling,
- and a hard kill switch.

The system traded liquid ETF options (QQQ, SPY, later TSLA), dynamically hedging delta with the underlying.

---

## 3. Early Results and Failure Pattern

Early backtests showed:
- sharp gains in isolated windows,
- followed by persistent equity decay and eventual trading halts.

While the strategy occasionally appeared profitable, long-horizon behavior consistently indicated **negative expectancy**, not execution errors.

This suggested a **structural issue**, not a tuning problem.

---

## 4. Diagnostic Expansion

To isolate the failure mode, the system was augmented with detailed diagnostics:
- equity and drawdown curves,
- time-series of delta, gamma, and vega exposure,
- PnL decomposition into option PnL, hedge PnL, and execution costs.

Losses were found to be **persistent and regime-dependent**, not random.

---

## 5. Resolution Upgrade: Key Insight

A critical insight emerged when testing shifted from daily bars to **minute-level resolution**.

At higher frequency:
- the strategy’s true convexity became visible,
- Greek exposure plots revealed persistent **short gamma and short vega** positioning.

This explained the steady equity decay in trending or unstable regimes.

**Resolution masked convexity at daily frequency.**

---

## 6. Why Risk Controls Failed

Risk controls successfully limited drawdowns but did not improve expectancy.

After each halt:
- the system re-entered with the same structural exposures,
- losses resumed under the same hostile regimes.

This demonstrated a key principle:

> Risk controls limit damage, but they do not create edge.

---

## 7. Philosophy Shift: From Containment to Expectancy

The project pivoted from loss containment to **expectancy engineering**.

The key realization:
- volatility strategies must explicitly include **no-trade regimes**,
- continuous participation is incompatible with regime-dependent convexity.

---

## 8. Final System Architecture

The system was redesigned around **regime awareness**, not continuous exposure.

Two strategies were implemented on a shared execution and risk engine:
- regime-filtered short volatility,
- selective long volatility.

A unified architecture ensures consistent sizing, hedging, and risk behavior across strategies.

---

## 9. Strategy A: Regime-Filtered Short Volatility

This strategy collects volatility risk premium opportunistically.

Entry requires:
- implied volatility above realized volatility,
- weak price trends,
- stable volatility dynamics.

Position sizing is governed strictly by gamma and vega limits.

### Outcome

Although implied volatility often exceeded realized volatility, losses clustered during:
- volatility-of-volatility spikes,
- trending price paths.

These regimes are structurally hostile to short gamma exposure.

Short volatility was reclassified as an **opportunistic carry trade**, not a continuous strategy.

---

## 10. Strategy B: Selective Long Volatility

A complementary long volatility strategy was introduced to exploit unstable regimes.

Characteristics:
- accepts frequent small losses (theta decay),
- seeks convex payoffs during volatility expansion,
- operates only under confirmed instability.

Entries require volatility cheapness, instability confirmation, and cooldown completion.

---

## 11. Pause vs Kill: Asymmetric Risk Management

Risk management was refined to distinguish:
- **temporary pauses** (loss of convexity),
- **permanent kill switches** (structural failure or extreme drawdown).

This avoids destroying future expectancy while maintaining strict downside protection.

---

## 12. Adaptive Regime Selection

The final evolution unified both strategies into an **adaptive regime selector**.

On each bar, the system evaluates:

- volatility level and volatility-of-volatility,
- trend strength,
- convexity alignment.

The system dynamically selects:
- `SHORT_VOL`,
- `LONG_VOL`,
- or `FLAT`.

A mismatch between standalone long-vol logic and adaptive long-vol logic was identified and corrected by unifying both under the same entry, pause, and risk rules.

---

## 13. Multi-Asset Validation

The finalized system was tested across:
- SPY,
- QQQ,
- TSLA.

Across assets:
- losses were clipped early,
- equity remained flat during hostile regimes,
- gains occurred selectively when convexity aligned.

Flat equity was interpreted as **correct abstention**, not failure.

---

## 14. Key Lessons Learned

- Convexity dominates volatility strategy behavior.
- Resolution critically affects diagnostic accuracy.
- Risk controls limit losses but do not generate edge.
- Regime selection determines expectancy.
- Losses are diagnostic signals, not implementation failures.

---

## 15. Conclusion

This project reflects a full research lifecycle:
**hypothesis → failure → diagnosis → redesign**.

It demonstrates why volatility strategies fail when misapplied, and why regime-aware participation, including disciplined non-participation, is essential for sustainable performance.
