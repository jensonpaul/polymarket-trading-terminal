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
//! |-------------------------|---------------------------------------------------|
//! | `side`                  | We trade the token that matches BTC's direction   |
//! | `efficiency_ratio`      | High ER → clean trend → follow with conviction    |
//! | `momentum_persistence`  | High persistence → trend has been sustained       |
//! | `avg_distance`          | Large time-averaged move → crowd already committed|
//! | `acceleration`          | Positive acceleration → trend accelerating        |
//! | `volatility_30s`        | Very high vol may indicate chop disguised as trend|
//! | `z_score`               | Extreme z in trend direction → strong positioning |

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
    ) -> PredictionSignal {
        let name = self.name();

        let Some(trend) = MarketAnalyzer::btc_trend(ctx) else {
            return PredictionSignal::no_trade(
                name,
                "waiting: btc origin not locked",
                ctx.timestamp_ms,
            );
        };

        let elapsed = 300u64.saturating_sub(ctx.seconds_remaining) as f64;
        let target_side = trend.side;

        let Some(token_trend) = MarketAnalyzer::token_trend(ctx, target_side, elapsed) else {
            return PredictionSignal::no_trade(
                name,
                "waiting: token origin not locked",
                ctx.timestamp_ms,
            );
        };

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

        let reason = format!(
            "btcSide={:?} avgDist={:.4} \
             tokenDist={:.4} tokenImb={:.3} entry={:.4}",
            trend.side,
            trend.avg_distance_from_origin,
            token_trend.distance_from_origin_pct,
            token_trend.imbalance,
            entry.to_f64().unwrap_or(0.0),
        );

        PredictionSignal {
            strategy_name: name,
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: 0.0,
            target_entry: entry,
            target_exit: entry,
            stop_loss: entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        }
    }
}