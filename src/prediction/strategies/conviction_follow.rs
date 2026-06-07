//! # Trend Follow Strategy
//!
//! Momentum play on Polymarket BTC UP/DOWN 5-minute markets.
//!
//! ## Core Thesis
//!
//! When BTC is making a clean, persistent directional move the aligned token
//! continues to be bid up as traders pile in.  We trade *with* BTC's trend
//! while the efficiency ratio and momentum persistence confirm the move is
//! real rather than noise.
//!
//! ## What drives the signal
//!
//! | Dimension               | Role in this strategy                              |
//! |-------------------------|----------------------------------------------------|
//! | `side`                  | We trade the token that matches BTC's direction    |
//! | `efficiency_ratio`      | High ER → clean trend → follow with conviction     |
//! | `momentum_persistence`  | High persistence → trend has been sustained        |
//! | `avg_distance`          | Large time-averaged move → crowd already committed |
//! | `acceleration`          | Positive acceleration → trend accelerating → entry timing |
//! | `volatility_30s`        | Very high vol may indicate chop disguised as trend |
//! | `z_score`               | Extreme z in trend direction → strong positioning  |

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
pub struct ConvictionFollowStrategy;

impl ConvictionFollowStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl PredictionStrategy for ConvictionFollowStrategy {
    fn name(&self) -> &'static str {
        "conviction_follow"
    }

    fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> Option<PredictionSignal> {
        // BTC origin must be locked.
        let trend = MarketAnalyzer::btc_trend(ctx)?;

        let elapsed =
            300u64.saturating_sub(ctx.seconds_remaining) as f64;

        // Token on the same side as BTC's trend.
        let target_side = trend.side;

        let token_trend =
            MarketAnalyzer::token_trend(ctx, target_side, elapsed)?;

        let target_token = match target_side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        let entry = target_token.current_price;
        if entry.is_zero() {
            return None;
        }

        let reason = format!(
            "conviction_follow \
             btcSide={:?} dist={:.4} er={:.3} \
             er1s={:.3} er5s={:.3} er10s={:.3} er30s={:.3} erFull={:.3} \
             persist={:.2} avgDist={:.4} z30={:.2} z60={:.2} z5m={:.2} accel={:.5} \
             vol30={:.4} rangePos={:.2} \
             tokenDist={:.4} tokenImb={:.3} entry={:.4}",
            trend.side,
            trend.distance_from_origin_pct,
            trend.efficiency_ratio,
            trend.er_1s,
            trend.er_5s,
            trend.er_10s,
            trend.er_30s,
            trend.er_full,
            trend.momentum_persistence,
            trend.avg_distance_from_origin,
            trend.z_score_30,
            trend.z_score_60,
            trend.z_score_5m,
            trend.acceleration,
            trend.volatility_30s,
            trend.range_position,
            token_trend.distance_from_origin_pct,
            token_trend.imbalance,
            entry.to_f64().unwrap_or(0.0),
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
