use std::time::Duration;
use crate::prediction::reversion::errors::ReversionError;
use crate::prediction::reversion::horizon::Horizon;

/// Configuration for the reversion engine.
/// Validated at construction; immutable at runtime.
#[derive(Debug, Clone)]
pub struct ReversionConfig {
    /// Minimum ticks before inference is enabled.
    pub warmup_ticks: u64,

    /// Feed silence beyond this duration triggers `DataUnavailable`.
    pub stale_threshold: Duration,

    /// Enabled horizons in ascending order of duration.
    pub horizons: Vec<Horizon>,

    pub ewma_fast_lambda:   f64,
    pub ewma_medium_lambda: f64,
    pub vwap_weight:        f64,
    pub vol_window_ticks:   usize,
    pub vol_ewma_lambda:    f64,

    // ── Continuation barrier ──────────────────────────────────────────────
    pub barrier_enabled:            bool,
    pub barrier_velocity_k:         f64,
    pub barrier_volume_n:           f64,
    pub barrier_momentum_threshold: f64,
}

impl Default for ReversionConfig {
    fn default() -> Self {
        Self {
            warmup_ticks:               300,
            stale_threshold:            Duration::from_secs(5),
            horizons:                   Horizon::all().to_vec(),
            ewma_fast_lambda:           0.04,
            ewma_medium_lambda:         0.01,
            vwap_weight:                0.30,
            vol_window_ticks:           300,
            vol_ewma_lambda:            0.02,
            barrier_enabled:            true,
            barrier_velocity_k:         1.5,
            barrier_volume_n:           1.8,
            barrier_momentum_threshold: 0.7,
        }
    }
}

impl ReversionConfig {
    pub fn validate(&self) -> Result<(), ReversionError> {
        if self.warmup_ticks == 0 {
            return Err(ReversionError::Config("warmup_ticks must be > 0".into()));
        }
        if self.horizons.is_empty() {
            return Err(ReversionError::Config("at least one horizon required".into()));
        }
        if !(0.0..=1.0).contains(&self.ewma_fast_lambda) {
            return Err(ReversionError::Config("ewma_fast_lambda must be in [0,1]".into()));
        }
        if self.ewma_fast_lambda <= self.ewma_medium_lambda {
            return Err(ReversionError::Config(
                "ewma_fast_lambda must be greater than ewma_medium_lambda".into(),
            ));
        }
        Ok(())
    }
}
