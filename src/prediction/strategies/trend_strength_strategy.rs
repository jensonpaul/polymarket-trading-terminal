//! # Trend Strength Strategy
//!
//! Consumes `TrendStrengthDetector`'s derived leader and trades it directly.
//! Runs parallel to `ExternalBtcStrategy`/`ConvictionFollowStrategy` — does
//! not read, alter, or depend on `ConvictionSnapshot` in any way.

use crate::prediction::{
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};

use btc_prediction_engine::types::TrendDirection;

#[derive(Debug, Default)]
pub struct TrendStrengthStrategy;

impl TrendStrengthStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl PredictionStrategy for TrendStrengthStrategy {
    fn name(&self) -> &'static str {
        "trend_strength"
    }

    fn evaluate(&self, ctx: &PredictionContext) -> PredictionSignal {
        let name = self.name();

        let Some(ts) = ctx.trend_strength.as_ref() else {
            return PredictionSignal::no_trade(
                name,
                "waiting: trend strength snapshot not available",
                ctx.timestamp_ms,
            );
        };

        let Some(leader) = ts.leader.as_ref() else {
            return PredictionSignal::no_trade(
                name,
                "waiting: no leader established yet",
                ctx.timestamp_ms,
            );
        };

        let target_side = match leader.direction {
            TrendDirection::Bullish  => PredictionSide::Up,
            TrendDirection::Bearish  => PredictionSide::Down,
            // Leader is only ever constructed from a ready() Bullish or
            // Bearish DirectionState — Sideways never accumulates a stream
            // in TrendStrengthDetector::push, so this arm is unreachable in
            // practice.  Handled explicitly rather than via unreachable!()
            // since TrendDirection is an external type whose invariants
            // this code does not control.
            TrendDirection::Sideways => {
                return PredictionSignal::no_trade(
                    name,
                    "no-trade: leader direction is Sideways",
                    ctx.timestamp_ms,
                );
            }
        };

        let target_token = match target_side {
            PredictionSide::Up   => &ctx.polymarket.up,
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
            "[trend_strength] direction={:?} score={:.3} ewma={:.3} \
             ticks={} held_ms={}",
            leader.direction,
            leader.score,
            leader.ewma_confidence,
            leader.consecutive_ticks,
            leader.held_ms,
        );

        PredictionSignal {
            strategy_name: name,
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: leader.score,
            target_entry: entry,
            target_exit: entry,
            stop_loss: entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        }
    }
}