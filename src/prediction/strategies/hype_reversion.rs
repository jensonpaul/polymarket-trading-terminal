use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::prediction::{
    DecayType,
    MarketAnalyzer,
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};

#[derive(Debug, Default)]
pub struct HypeReversionStrategy {
    pub min_confidence: f64,
}

impl HypeReversionStrategy {
    pub fn new() -> Self {
        Self {
            min_confidence: 55.0,
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
        let analysis = MarketAnalyzer::hype_analysis(ctx)?;

        // Avoid late-window entries.
        if ctx.seconds_remaining <= 30 {
            return Some(PredictionSignal::no_trade(
                "settlement window",
                ctx.timestamp_ms,
            ));
        }

        let btc_volatility = ctx
            .btc
            .volatility_30s
            .max(ctx.btc.volatility_60s);

        let btc_near_extreme =
            ctx.btc.range_position <= 0.20
                || ctx.btc.range_position >= 0.80;

        let confidence =
            analysis.confidence
                + (btc_volatility * 100.0).min(20.0);

        /*
        if confidence < self.min_confidence {
            return None;
        }
        */

        // Strategy thesis:
        // Buy opposite of hyped side.
        let target_side = analysis.trending_side;

        let target_token = match target_side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        let entry = target_token.current_price;

        if entry <= Decimal::ZERO {
            return None;
        }

        let profit_multiplier = match analysis.decay_type {
            DecayType::Sudden => dec!(1.80),
            DecayType::Gradual => dec!(1.40),
            DecayType::Flat => dec!(1.20),
        };

        let target_exit = entry * profit_multiplier;

        let stop_loss = entry * dec!(0.50);

        let final_confidence = if btc_near_extreme {
            (confidence + 10.0).min(100.0)
        } else {
            confidence
        };

        Some(PredictionSignal {
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: final_confidence,

            target_entry: entry,
            target_exit,
            stop_loss,

            generated_at_ms: ctx.timestamp_ms,

            reason: format!(
                "Hyped={:?}, Trending={:?}, Decay={:?}, BTCRange={:.2}",
                analysis.hyped_side,
                analysis.trending_side,
                analysis.decay_type,
                ctx.btc.range_position,
            ),
        })
    }
}
