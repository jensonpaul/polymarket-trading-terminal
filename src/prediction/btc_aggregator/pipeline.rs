//! The complete 4-layer BTC price cleaning pipeline.
//!
//! ```text
//! ExchangeTick (raw)
//!      │
//!   [Layer 1] SpikeFilter         per-exchange EMA gate
//!      │
//!   [Layer 2] OutlierGate         cross-exchange MAD gate
//!      │
//!   [Layer 3] Aggregator          time-bucket OHLCV + VWMP
//!      │  (emits Candle on bucket boundary)
//!   [Layer 4] KalmanSmoother      residual noise removal
//!      │
//!  CleanPrice (published downstream)
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! let mut pipeline = Pipeline::new(PipelineConfig::default());
//!
//! // On each incoming WebSocket message:
//! if let Some(clean) = pipeline.ingest(tick) {
//!     // Use clean.smoothed for charts / features.
//! }
//!
//! // At window reset:
//! pipeline.reset_smoother();
//! ```

use tracing::trace;

use crate::prediction::btc_aggregator::{
    aggregator::{Aggregator, AggregatorConfig, Candle},
    kalman::{KalmanConfig, KalmanSmoother, SmoothedPrice},
    outlier_gate::{OutlierGate, OutlierGateConfig},
    spike_filter::{SpikeFilter, SpikeFilterConfig},
    tick::ExchangeTick,
};

// ── Pipeline configuration ────────────────────────────────────────────────────

/// Top-level configuration for the entire cleaning pipeline.
///
/// Embed one sub-config per layer; each layer uses its own defaults
/// unless overridden here.
#[derive(Debug, Clone, Default)]
pub struct PipelineConfig {
    pub spike:      SpikeFilterConfig,
    pub outlier:    OutlierGateConfig,
    pub aggregator: AggregatorConfig,
    pub kalman:     KalmanConfig,
}

// ── Output ────────────────────────────────────────────────────────────────────

/// A fully cleaned, smoothed BTC price snapshot.
///
/// Emitted at most once per time-bucket (typically every 250 ms).
#[derive(Debug, Clone)]
pub struct CleanPrice {
    /// Timestamp of the bucket open (milliseconds since Unix epoch).
    pub timestamp_ms: u64,

    /// The Kalman-smoothed VWMP.  Use this for features and charts.
    pub smoothed: f64,

    /// Raw VWMP from the aggregator before Kalman smoothing.
    /// Compare with `smoothed` to measure filter lag.
    pub raw_vwmp: f64,

    /// Kalman gain for this update step.
    pub kalman_gain: f64,

    /// OHLCV from the completed bucket.
    pub candle: Candle,
}

// ── Pipeline ─────────────────────────────────────────────────────────────────

pub struct Pipeline {
    spike:      SpikeFilter,
    outlier:    OutlierGate,
    aggregator: Aggregator,
    kalman:     KalmanSmoother,

    /// Counts for diagnostics.
    stats: PipelineStats,
}

#[derive(Debug, Default, Clone)]
pub struct PipelineStats {
    pub ticks_received:        u64,
    pub ticks_spike_rejected:  u64,
    pub ticks_outlier_rejected: u64,
    pub ticks_accepted:        u64,
    pub candles_emitted:       u64,
}

impl Pipeline {
    pub fn new(cfg: PipelineConfig) -> Self {
        Self {
            spike:      SpikeFilter::new(cfg.spike),
            outlier:    OutlierGate::new(cfg.outlier),
            aggregator: Aggregator::new(cfg.aggregator),
            kalman:     KalmanSmoother::new(cfg.kalman),
            stats:      PipelineStats::default(),
        }
    }

    /// Feed one raw exchange tick through all four pipeline layers.
    ///
    /// Returns `Some(CleanPrice)` when a time-bucket is completed and
    /// the bucket price has been smoothed by the Kalman filter.
    /// Returns `None` for every tick that does not close a bucket
    /// (the majority of ticks during normal operation).
    pub fn ingest(&mut self, tick: &ExchangeTick) -> Option<CleanPrice> {
        self.stats.ticks_received += 1;

        // ── Layer 1: Spike filter ─────────────────────────────────────────
        if !self.spike.accept(tick) {
            self.stats.ticks_spike_rejected += 1;
            return None;
        }

        // ── Layer 2: Cross-exchange outlier gate ──────────────────────────
        let price = tick.mid_vwap()?;

        if !self.outlier.accept(tick.exchange, price, tick.received_ms) {
            self.stats.ticks_outlier_rejected += 1;
            return None;
        }

        self.stats.ticks_accepted += 1;

        // ── Layer 3: Time-bucket aggregation ──────────────────────────────
        let candle = self.aggregator.ingest(tick)?;

        // ── Layer 4: Kalman smoothing ─────────────────────────────────────
        let vol_proxy = if candle.vwmp > 0.0 {
            (candle.high - candle.low) / candle.vwmp
        } else {
            0.0
        };

        let smoothed: SmoothedPrice = self.kalman.update(candle.vwmp, vol_proxy);
        self.stats.candles_emitted += 1;

        trace!(
            open_ms       = candle.open_ms,
            raw_vwmp      = candle.vwmp,
            smoothed      = smoothed.estimate,
            kalman_gain   = smoothed.gain,
            r_effective   = smoothed.r_effective,
            tick_count    = candle.tick_count,
            "clean candle emitted"
        );

        Some(CleanPrice {
            timestamp_ms: candle.open_ms,
            smoothed:     smoothed.estimate,
            raw_vwmp:     candle.vwmp,
            kalman_gain:  smoothed.gain,
            candle,
        })
    }

    /// Force-flush the current bucket.
    ///
    /// Call before a 5-minute window reset or on graceful shutdown to
    /// avoid losing the partially accumulated bucket.
    pub fn flush(&mut self) -> Option<CleanPrice> {
        let candle = self.aggregator.flush()?;

        let vol_proxy = if candle.vwmp > 0.0 {
            (candle.high - candle.low) / candle.vwmp
        } else {
            0.0
        };

        let smoothed = self.kalman.update(candle.vwmp, vol_proxy);
        self.stats.candles_emitted += 1;

        Some(CleanPrice {
            timestamp_ms: candle.open_ms,
            smoothed:     smoothed.estimate,
            raw_vwmp:     candle.vwmp,
            kalman_gain:  smoothed.gain,
            candle,
        })
    }

    /// Reset the Kalman filter.
    ///
    /// Call at the start of each new 5-minute prediction window so the
    /// smoother does not carry state from the previous window's price
    /// level into the new window.
    pub fn reset_smoother(&mut self) {
        self.kalman.reset();
    }

    /// Snapshot of pipeline throughput counters.
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// Rejection rate as a fraction [0, 1].
    pub fn rejection_rate(&self) -> f64 {
        if self.stats.ticks_received == 0 {
            return 0.0;
        }
        let rejected = self.stats.ticks_spike_rejected
            + self.stats.ticks_outlier_rejected;
        rejected as f64 / self.stats.ticks_received as f64
    }
}
