use rust_decimal::prelude::ToPrimitive;

use crate::prediction::{
    MarketAnalyzer,
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};

use btc_prediction_engine::prelude::TrendDirection;

#[derive(Debug, Default)]
pub struct ExternalBtcStrategy;

impl ExternalBtcStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl PredictionStrategy for ExternalBtcStrategy {
    fn name(&self) -> &'static str {
        "external_btc"
    }

    fn evaluate(&self, ctx: &PredictionContext) -> Option<PredictionSignal> {
        // Read the snapshot from context — already populated by service.rs.
        // No Arc, no RwLock, no blocking_read().
        let pred = ctx.external_prediction.as_ref()?;

        // Staleness guard: reject snapshots older than 5 seconds.
        let age_ms = ctx.timestamp_ms.saturating_sub(
            (pred.snapshot_at / 1_000) as u64,  // micros → ms
        );
        if age_ms > 5_000 {
            return None;
        }

        let target_side = match pred.fused_direction {
            TrendDirection::Bullish => PredictionSide::Up,
            TrendDirection::Bearish => PredictionSide::Down,
            TrendDirection::Sideways => return None,
        };

        let target_token = match target_side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        let entry = target_token.current_price;
        if entry.is_zero() {
            return None;
        }

        let reason = format!(
            "external_btc fused={:?} ({:.2}) short={:?} ({:.2}) broad={:?} ({:.2})",
            pred.fused_direction,
            pred.fused_confidence,
            pred.short.direction,
            pred.short.confidence,
            pred.broad.direction,
            pred.broad.confidence,
        );

        Some(PredictionSignal {
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: pred.fused_confidence,
            target_entry: entry,
            target_exit: entry,
            stop_loss: entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        })
    }
}