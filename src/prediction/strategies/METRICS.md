
---

## **Final Token Metrics Layer**

| Metric                  | Type           | Formula / Definition                                    | Interpretation                                       |
| ----------------------- | -------------- | ------------------------------------------------------- | ---------------------------------------------------- |
| **VWAP deviation**      | internal       | `(current_price - vwap) / vwap`                         | Positive → token overextended; Negative → under VWAP |
| **Orderbook imbalance** | microstructure | `(bid_volume - ask_volume) / (bid_volume + ask_volume)` | Positive → buy pressure; Negative → sell pressure    |

---

## **Final BTC Metrics Layer**

| Metric                   | Type         | Formula / Definition                  | Interpretation                                           |
| ------------------------ | ------------ | ------------------------------------- | -------------------------------------------------------- |
| **BTC return %**         | directional  | Signed return over a window           | Indicates short-term movement direction & magnitude      |
| **BTC efficiency ratio** | structure    | `ER = directional_move / total_move`  | Near 1 → clean movement; Near 0 → choppy / noisy         |
| **BTC volatility**       | noise        | Std deviation over a window           | Measures amplitude of price fluctuations                 |
| **BTC near high/low**    | positioning  | `0.0 = recent low, 1.0 = recent high` | Shows how close BTC is to extremes                       |
| **BTC acceleration**     | regime shift | `Δ(BTC return)` over time             | Positive → momentum building; Negative → momentum fading |

---

## **Interpretation Hierarchy**

1. **Direction** → `btc_return_pct`
2. **Magnitude** → `abs(btc_return_pct)`
3. **Structure / signal quality** → `btc_efficiency_ratio`
4. **Noise / amplitude** → `btc_volatility`
5. **Regime / phase shift** → `btc_acceleration`
6. **Positioning / extremity** → `btc_near_high_low`
7. **Token deviation vs equilibrium** → `vwap_dev`
8. **Micro pressure / absorption** → `orderbook_imbalance`

---

---

## **Metrics Measured on BTC Price Tick Arrival**

| Metric                   | Tick-driven? | Notes                                                                            |
| ------------------------ | ------------ | -------------------------------------------------------------------------------- |
| **BTC return %**         | ✅ Yes        | Update whenever BTC price changes; compute over the window (e.g., last 1–5 min). |
| **BTC efficiency ratio** | ✅ Yes        | Needs directional vs total move; update as BTC price ticks arrive.               |
| **BTC volatility**       | ✅ Yes        | Rolling std dev over window; can be updated incrementally per tick.              |
| **BTC near high/low**    | ✅ Yes        | Track recent high/low; compute normalized position per tick.                     |
| **BTC acceleration**     | ✅ Yes        | Δ(return) over time; must be updated per tick to capture momentum changes.       |

**Summary:** All BTC metrics are **tick-driven** because they depend on the latest BTC price to compute direction, magnitude, noise, and momentum.

---

## **Metrics Measured on Token Orderbook / Trade Tick Arrival**

| Metric                  | Tick-driven? | Notes                                                                                            |
| ----------------------- | ------------ | ------------------------------------------------------------------------------------------------ |
| **VWAP deviation**      | ✅ Yes        | Token price tick or trade arrival triggers recomputation relative to rolling VWAP.               |
| **Orderbook imbalance** | ✅ Yes        | Recompute whenever bids/asks change; optional smoothing over small window to avoid noise spikes. |

**Summary:** Both token-level metrics are **tick-driven** as well, but they are **microstructure sensitive** (orderbook changes are often more frequent than trades).

---
