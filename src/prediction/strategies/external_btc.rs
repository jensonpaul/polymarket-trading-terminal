use crate::prediction::{
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};

use btc_prediction_engine::types::TrendDirection;

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

    fn evaluate(&self, ctx: &PredictionContext) -> PredictionSignal {
        let name = self.name();

        // ── 1. Resolve direction from conviction tracker ──────────────────
        //
        // Primary source: stable conviction that has been confirmed over
        // TICKS_REQUIRED consecutive model outputs.
        // Fallback: heuristic fused direction (available immediately, but
        // only accepted above 0.70 confidence).
        let resolution = if let Some(conv) = ctx.conviction.as_ref() {
            if let Some(active) = conv.active.as_ref() {
                Ok((active.direction, active.mean_confidence, "conviction"))
            } else {
                self.heuristic_fallback(ctx)
                    .ok_or_else(|| {
                        format!(
                            "waiting: conviction building ({} ticks, queued={})",
                            conv.building_ticks,
                            conv.queued,
                        )
                    })
            }
        } else {
            self.heuristic_fallback(ctx)
                .ok_or_else(|| "waiting: conviction tracker not populated".to_string())
        };

        let (direction, confidence, source) = match resolution {
            Ok(tuple) => tuple,
            Err(reason) => return PredictionSignal::no_trade(name, reason, ctx.timestamp_ms),
        };

        // ── 2. Sideways means no trade ────────────────────────────────────
        let target_side = match direction {
            TrendDirection::Bullish  => PredictionSide::Up,
            TrendDirection::Bearish  => PredictionSide::Down,
            TrendDirection::Sideways => {
                return PredictionSignal::no_trade(
                    name,
                    format!("[{source}] direction=Sideways conf={confidence:.3} — no directional edge"),
                    ctx.timestamp_ms,
                );
            }
        };

        // ── 3. Resolve entry price ────────────────────────────────────────
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

        // ── 4. Build reason string ────────────────────────────────────────
        let reason = if let Some(pred) = ctx.external_prediction.as_ref() {
            format!(
                "[{source}] direction={direction:?} conf={confidence:.3} | \
                 model_fused={:?} ({:.3}) short={:?} ({:.3}) heuristic={:?} ({:.3}) | \
                 conviction_queue={} building={:?}({}ticks)",
                pred.fused_direction,
                pred.fused_confidence,
                pred.short.direction,
                pred.short.confidence,
                pred.heuristic.fused_direction,
                pred.heuristic.fused_confidence,
                ctx.conviction.as_ref().map_or(0, |c| c.queued),
                ctx.conviction.as_ref().and_then(|c| c.building),
                ctx.conviction.as_ref().map_or(0, |c| c.building_ticks),
            )
        } else {
            format!("[{source}] direction={direction:?} conf={confidence:.3}")
        };

        PredictionSignal {
            strategy_name: name,
            signal_type: SignalType::Buy,
            side: target_side,
            confidence,
            target_entry: entry,
            target_exit:  entry,
            stop_loss:    entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        }
    }
}

impl ExternalBtcStrategy {
    /// Heuristic fallback: available immediately without accumulator warm-up.
    /// Only accepted above 0.70 confidence to suppress noise.
    fn heuristic_fallback(
        &self,
        ctx: &PredictionContext,
    ) -> Option<(TrendDirection, f64, &'static str)> {
        let pred = ctx.external_prediction.as_ref()?;

        // Staleness guard: reject snapshots older than 5 seconds.
        let age_ms = ctx.timestamp_ms.saturating_sub(
            (pred.snapshot_at as u64) / 1_000,
        );
        if age_ms > 5_000 {
            return None;
        }

        let dir  = pred.heuristic.fused_direction;
        let conf = pred.heuristic.fused_confidence;

        if dir == TrendDirection::Sideways || conf < 0.70 {
            return None;
        }

        Some((dir, conf, "heuristic"))
    }
}