//! Mean-reversion strategy — consumes [`ReversionOutput`] from
//! [`PredictionContext::reversion`] and translates the engine's best-horizon
//! trade bias into a [`PredictionSignal`].
//!
//! This strategy is intentionally conservative: it only emits `Buy` when
//! the engine's `opportunity_score` clears a minimum bar AND the
//! continuation barrier is not active. Everything else is `NoTrade` with a
//! descriptive reason, per the [`PredictionStrategy`] contract.

use crate::prediction::{
    PredictionContext,
    PredictionSide,
    PredictionSignal,
    PredictionStrategy,
    SignalType,
};
use crate::prediction::reversion::TradeBias;

/// Minimum opportunity score required to emit a directional signal.
/// Below this, the engine itself would already report `TradeBias::Neutral`,
/// but we keep an explicit floor here in case that threshold is tuned
/// independently in the future.
const MIN_OPPORTUNITY_SCORE: f64 = 0.40;

/// Minimum data-quality confidence required to trust the engine's output.
const MIN_DATA_QUALITY: f64 = 0.5;

#[derive(Debug, Default)]
pub struct MeanReversionStrategy;

impl MeanReversionStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl PredictionStrategy for MeanReversionStrategy {
    fn name(&self) -> &'static str {
        "mean_reversion"
    }

    fn evaluate(&self, ctx: &PredictionContext) -> PredictionSignal {
        let name = self.name();

        let Some(rev) = ctx.reversion.as_ref() else {
            return PredictionSignal::no_trade(
                name,
                "waiting: reversion engine not yet warmed up",
                ctx.timestamp_ms,
            );
        };

        // ── 1. Data quality gate ────────────────────────────────────────
        if rev.data_quality.confidence < MIN_DATA_QUALITY {
            return PredictionSignal::no_trade(
                name,
                format!(
                    "waiting: data quality too low (confidence={:.2}, feed_age_ms={})",
                    rev.data_quality.confidence, rev.data_quality.feed_age_ms,
                ),
                ctx.timestamp_ms,
            );
        }

        // ── 2. Barrier / trade-bias gate ─────────────────────────────────
        let target_side = match rev.trade_bias {
            TradeBias::Suppressed => {
                return PredictionSignal::no_trade(
                    name,
                    format!(
                        "no trade: continuation barrier active (regime={:?}, opportunity={:.3})",
                        rev.regime, rev.opportunity_score,
                    ),
                    ctx.timestamp_ms,
                );
            }
            TradeBias::Neutral => {
                return PredictionSignal::no_trade(
                    name,
                    format!(
                        "no trade: opportunity_score={:.3} below threshold (regime={:?})",
                        rev.opportunity_score, rev.regime,
                    ),
                    ctx.timestamp_ms,
                );
            }
            // Reversion engine says price is stretched UP and expected to
            // revert DOWN → that favours the DOWN polymarket token, and
            // vice versa.
            TradeBias::ShortMeanReversion => PredictionSide::Down,
            TradeBias::LongMeanReversion  => PredictionSide::Up,
        };

        if rev.opportunity_score < MIN_OPPORTUNITY_SCORE {
            return PredictionSignal::no_trade(
                name,
                format!(
                    "waiting: opportunity_score={:.3} < {MIN_OPPORTUNITY_SCORE:.2} floor",
                    rev.opportunity_score,
                ),
                ctx.timestamp_ms,
            );
        }

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

        // ── 4. Build reason string from best horizon ──────────────────────
        let best_horizon_info = rev.best_horizon
            .and_then(|h| rev.reversion.get(&h).map(|hr| (h, hr)))
            .map(|(h, hr)| {
                format!(
                    "best_horizon={h} p={:.3} target_50%={:.2} half_life={:.1}s(conf={:.2})",
                    hr.probability,
                    hr.levels.target_50pct,
                    rev.half_life_seconds,
                    rev.half_life_confidence,
                )
            })
            .unwrap_or_else(|| "best_horizon=none".to_string());

        let reason = format!(
            "[reversion] bias={:?} stretch={:.3}(σ={:.2}) regime={:?}({:.2}) \
             opportunity={:.3} brier={:.3} | {best_horizon_info}",
            rev.trade_bias,
            rev.stretch_score,
            rev.deviation_sigma,
            rev.regime,
            rev.regime_confidence,
            rev.opportunity_score,
            rev.calibration_brier,
        );

        PredictionSignal {
            strategy_name: name,
            signal_type: SignalType::Buy,
            side: target_side,
            confidence: rev.opportunity_score,
            target_entry: entry,
            target_exit:  entry,
            stop_loss:    entry,
            generated_at_ms: ctx.timestamp_ms,
            reason,
        }
    }
}
