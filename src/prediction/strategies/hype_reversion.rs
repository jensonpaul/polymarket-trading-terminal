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
//! |-------------------------|----------------------------------------------------|
//! | `side`                  | Determines which token is suppressed (opposite)    |
//! | `distance_from_origin`  | Magnitude of the move — bigger → more crowd hype   |
//! | `efficiency_ratio`      | Cleaner trend → stronger crowd over-reaction       |
//! | `momentum_persistence`  | Longer-lasting trend → more ingrained crowd bias   |
//! | `z_score`               | Extreme z-score → BTC unusually far out → snap-back likely |
//! | `volatility_30s`        | High volatility → suppressed token could rebound fast |
//! | `acceleration`          | Negative acceleration on an up-trend → trend fading → snap-back soon |

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
    ) -> Option<PredictionSignal> {
        // BTC origin must be locked.
        let trend = MarketAnalyzer::btc_trend(ctx)?;

        let target_side = Self::suppressed_side(trend.side);

        let target_token = match target_side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        let entry = target_token.current_price;
        if entry.is_zero() {
            return None;
        }

        if let Some(pred) = &ctx.external_prediction {
            if pred.fused_confidence > 0.75 {
                // boost confidence
            }
        }

        let pred = ctx.external_prediction.as_ref()?;
        let reason = format!(
            /*
            "hype_reversion \
             btcSide={:?} dist={:.4} er={:.3} \
             persist={:.2} z30={:.2} z60={:.2} z5m={:.2} accel={:.5} \
             vol30={:.4} rangePos={:.2} \
             suppressed={:?} entry={:.4}",
            trend.side,
            trend.distance_from_origin_pct,
            trend.efficiency_ratio,
            trend.momentum_persistence,
            trend.z_score_30,
            trend.z_score_60,
            trend.z_score_5m,
            trend.acceleration,
            trend.volatility_30s,
            trend.range_position,
            target_side,
            entry.to_f64().unwrap_or(0.0),
            */
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

        Some(PredictionSignal {
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: 0.0,
            target_entry: entry,
            target_exit: entry,
            stop_loss: entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        })
    }
}
