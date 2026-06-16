use btc_prediction_engine::prelude::*;

use crate::prediction::conviction::ConvictionSnapshot;
use crate::prediction::trend_strength::TrendStrengthSnapshot;
use crate::prediction::features::{
    BtcFeatures,
    PolymarketFeatures,
};

#[derive(Debug, Clone, Default)]
pub struct PredictionContext {
    pub timestamp_ms: u64,

    pub btc: BtcFeatures,

    pub polymarket: PolymarketFeatures,

    pub seconds_remaining: u64,

    /// Raw model output — available for diagnostics and heuristic fallback.
    pub external_prediction: Option<PredictionSnapshot>,

    /// Stable conviction signal derived from the raw model output by
    /// ConvictionTracker.  This is what strategies should act on.
    pub conviction: Option<ConvictionSnapshot>,

    /// Continuously-updated, window-independent trend strength snapshot
    /// derived from the same raw model output by TrendStrengthDetector.
    /// Runs parallel to `conviction` — neither reads nor alters the other.
    pub trend_strength: Option<TrendStrengthSnapshot>,
}