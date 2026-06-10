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
pub struct ExternalBtcStrategy {
    latest: Arc<RwLock<Option<PredictionSnapshot>>>,
}

impl ExternalBtcStrategy {
    pub fn new(
        latest: Arc<RwLock<Option<PredictionSnapshot>>>,
    ) -> Self {
        Self { latest }
    }
}

impl PredictionStrategy for ExternalBtcStrategy {
    fn name(&self) -> &'static str {
        "external_btc"
    }

    fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> Option<PredictionSignal> {
        let pred =
            self.latest
                .blocking_read()
                .clone()?;

        let target_side = match pred.fused_direction {
            Direction::Bullish => PredictionSide::Down,
            Direction::Bearish => PredictionSide::Up,
            _ => return None,
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
            "external_btc \
            fused={:?} ({:.2}) \
            short={:?} ({:.2}) \
            broad={:?} ({:.2})",
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
