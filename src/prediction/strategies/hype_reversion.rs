//! # Hype Reversion Strategy
//!
//! Mean-reversion play on Polymarket BTC UP/DOWN 5-minute markets.
//!
//! ## Core Thesis
//!
//! When BTC makes a clean, directional move the crowd overshoots: the token
//! aligned with BTC's direction gets bid up beyond fair value while the
//! opposite token is ignored.  As the window progresses both tokens revert
//! toward 0.5.  We buy the *suppressed* (counter-BTC) token.
//!
//! "Clean move" is measured by [`BtcTrend::efficiency_ratio`]: a high ER
//! means the crowd had a coherent narrative to over-react to; a low ER
//! (choppy BTC) gives the crowd nothing to latch onto.
//!
//! ## What drives the signal
//!
//! | Dimension               | Role in this strategy                              |
//! |-------------------------|---------------------------------------------------|
//! | `side`                  | Determines which token is suppressed (opposite)   |
//! | `distance_from_origin`  | Magnitude of the move — bigger → more crowd hype  |
//! | `efficiency_ratio`      | Cleaner trend → stronger crowd over-reaction      |
//! | `momentum_persistence`  | Longer-lasting trend → more ingrained crowd bias  |
//! | `z_score`               | Extreme z-score → BTC unusually far out → snap-back likely |
//! | `volatility_30s`        | High volatility → suppressed token could rebound fast |
//! | `acceleration`          | Negative acceleration on an up-trend → trend fading |

use rust_decimal::prelude::ToPrimitive;

use crate::prediction::{
    MarketAnalyzer,
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};

#[derive(Debug, Default)]
pub struct HypeReversionStrategy;

impl HypeReversionStrategy {
    pub fn new() -> Self {
        Self
    }

    /// Which token side is being suppressed by the crowd hype.
    fn suppressed_side(btc_side: PredictionSide) -> PredictionSide {
        match btc_side {
            PredictionSide::Up   => PredictionSide::Down,
            PredictionSide::Down => PredictionSide::Up,
        }
    }
}

impl PredictionStrategy for HypeReversionStrategy {
    fn name(&self) -> &'static str {
        "hype_reversion"
    }

    fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> PredictionSignal {
        let name = self.name();

        let Some(trend) = MarketAnalyzer::btc_trend(ctx) else {
            return PredictionSignal::no_trade(
                name,
                "waiting: btc origin not locked",
                ctx.timestamp_ms,
            );
        };

        let target_side = Self::suppressed_side(trend.side);

        let target_token = match target_side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        let entry = target_token.current_price;
        if entry.is_zero() {
            return PredictionSignal::no_trade(
                name,
                "waiting: token price is zero",
                ctx.timestamp_ms,
            );
        }

        let Some(pred) = ctx.external_prediction.as_ref() else {
            return PredictionSignal::no_trade(
                name,
                "waiting: external prediction not available",
                ctx.timestamp_ms,
            );
        };

        let reason = format!(
            "dist={:.4} er={:.3} \
             er1s={:.3} er5s={:.3} er10s={:.3} er30s={:.3} erFull={:.3} \
             \r\npersist={:.2} accel={:.5} vol30={:.4} \
             z30={:.2} z60={:.2} z5m={:.2} \
             \r\nrangePos={:.2} \
             rPos30s={:.2} \
             rPos60s={:.2} \
             rPos5m={:.2} \
             rPos10m={:.2} \
             rPos30m={:.2} \
             rPos60m={:.2} \
             \r\nexternal_btc \
             mlFused={:?} ({:.2}) \
             fused={:?} ({:.2}) \
             short={:?} ({:.2}) \
             broad={:?} ({:.2})",
            trend.distance_from_origin_pct,
            trend.efficiency_ratio,
            trend.er_1s,
            trend.er_5s,
            trend.er_10s,
            trend.er_30s,
            trend.er_full,
            trend.momentum_persistence,
            trend.acceleration,
            trend.volatility_30s,
            trend.z_score_30,
            trend.z_score_60,
            trend.z_score_5m,
            trend.range_position,
            trend.range_position_30s,
            trend.range_position_60s,
            trend.range_position_5m,
            trend.range_position_10m,
            trend.range_position_30m,
            trend.range_position_60m,
            pred.fused_direction,
            pred.fused_confidence,
            pred.heuristic.fused_direction,
            pred.heuristic.fused_confidence,
            pred.short.direction,
            pred.short.confidence,
            pred.broad.direction,
            pred.broad.confidence,
        );

        let positions = [
            trend.range_position_30s,
            trend.range_position_60s,
            trend.range_position_5m,
            trend.range_position_10m,
            trend.range_position_30m,
            trend.range_position_60m,
        ];

        let weights = [1.0, 2.0, 4.0, 6.0, 10.0, 15.0];

        let range_position_confidence = compute_trend_confidence(&positions, &weights);

        PredictionSignal {
            strategy_name: name,
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: range_position_confidence.toward_high,
            target_entry: entry,
            target_exit: entry,
            stop_loss: entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        }
    }
}

#[derive(Debug)]
pub struct TrendConfidence {
    pub mean_pos: f64,
    pub agreement: f64,
    pub toward_high: f64,
    pub toward_low: f64,
    pub uncertain: f64,
}

pub fn compute_trend_confidence(
    positions: &[f64],
    weights: &[f64],
) -> TrendConfidence {
    assert_eq!(positions.len(), weights.len());
    assert!(!positions.is_empty());

    let weight_sum: f64 = weights.iter().sum();

    let mean_pos: f64 = positions
        .iter()
        .zip(weights.iter())
        .map(|(p, w)| p * w)
        .sum::<f64>()
        / weight_sum;

    let variance: f64 = positions
        .iter()
        .zip(weights.iter())
        .map(|(p, w)| w * (p - mean_pos).powi(2))
        .sum::<f64>()
        / weight_sum;

    let stddev = variance.sqrt();
    let agreement = (1.0 - stddev).clamp(0.0, 1.0);

    TrendConfidence {
        mean_pos,
        agreement,
        toward_high: mean_pos * agreement,
        toward_low: (1.0 - mean_pos) * agreement,
        uncertain: 1.0 - agreement,
    }
}