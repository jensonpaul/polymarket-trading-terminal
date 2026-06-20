//! Top-level reversion engine coordinator.
//!
//! # Noisy-tick-first design
//!
//! Per integration requirement, this engine ingests **raw, noisy** BTC ticks
//! — i.e. it is wired to consume [`ExchangeTick`] data *before* the existing
//! 4-layer cleaning pipeline (`btc_aggregator::Pipeline`) applies its
//! spike/outlier/Kalman filtering. Reversion detection inherently needs the
//! micro-structure noise (stretch, volatility, OFI) that the cleaning
//! pipeline is designed to remove for the ONNX trend model's consumption.
//!
//! Both pipelines run independently and concurrently off the same raw tick
//! stream — see `btc_feed.rs` `run_engine()` for the fan-out point. Neither
//! filters for the other; each serves a different downstream consumer.

use std::time::Instant;

use tracing::{info, warn};

use crate::prediction::btc_aggregator::ExchangeTick;
use crate::prediction::reversion::config::ReversionConfig;
use crate::prediction::reversion::errors::ReversionError;
use crate::prediction::reversion::features::{
    EquilibriumTracker, FeatureSnapshot, MomentumFeatures, OrderFlowFeatures,
    VolatilityEstimator, stretch_score,
};
use crate::prediction::reversion::horizon::Horizon;
use crate::prediction::reversion::output::{BuilderInputs, OutputBuilder, ReversionOutput};
use crate::prediction::reversion::pipeline::{
    ContinuationBarrier, ExplainabilityDecomposer, HalfLifeEstimator, IsotonicCalibrator,
    ProbabilityEngine,
};
use crate::prediction::reversion::quality::FreshnessMonitor;
use crate::prediction::reversion::regime::RegimeClassifier;

/// Engine lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineLifecycle {
    Warming,
    Running,
    Stale,
}

/// The reversion engine. Owns the full computation + consumer pipeline.
///
/// Driven by [`BtcFeed`] on every raw [`ExchangeTick`] received from the
/// orderly-backed exchange feeds. Entirely synchronous, no I/O.
pub struct ReversionEngine {
    config: ReversionConfig,

    // ── lifecycle ─────────────────────────────────────────────────────────
    lifecycle:      EngineLifecycle,
    ticks_received: u64,
    stale_since:    Option<Instant>,

    // ── computation pipeline ─────────────────────────────────────────────
    equilibrium: EquilibriumTracker,
    volatility:  VolatilityEstimator,
    regime:      RegimeClassifier,
    freshness:   FreshnessMonitor,

    // order-flow accumulators
    last_ofi:            f64,
    cumulative_delta:    f64,
    rolling_volume_mean: f64,
    rolling_volume_var:  f64,
    volume_n:            f64,

    // momentum accumulators
    prev_price:    Option<f64>,
    momentum_ewma: f64,

    // ── consumer layer ───────────────────────────────────────────────────
    probability:    ProbabilityEngine,
    half_life:      HalfLifeEstimator,
    explainability: ExplainabilityDecomposer,
    barrier:        ContinuationBarrier,
    calibrator:     IsotonicCalibrator,

    // ── cache ─────────────────────────────────────────────────────────────
    last_output:      Option<ReversionOutput>,
    enabled_horizons: Vec<Horizon>,
}

impl ReversionEngine {
    pub fn new(config: ReversionConfig) -> Result<Self, ReversionError> {
        config.validate()?;

        let horizons = config.horizons.clone();
        let barrier  = ContinuationBarrier::new(&config);

        Ok(Self {
            equilibrium: EquilibriumTracker::new(
                config.ewma_fast_lambda,
                config.ewma_medium_lambda,
                config.vwap_weight,
                0.0,
            ),
            volatility: VolatilityEstimator::new(config.vol_window_ticks, config.vol_ewma_lambda),
            regime:     RegimeClassifier::new(),
            freshness:  FreshnessMonitor::new(),

            last_ofi: 0.0,
            cumulative_delta: 0.0,
            rolling_volume_mean: 0.0,
            rolling_volume_var: 0.0,
            volume_n: 0.0,

            prev_price: None,
            momentum_ewma: 0.0,

            probability:    ProbabilityEngine::new(&horizons),
            half_life:      HalfLifeEstimator::new(300, 0.25), // ~250ms tick cadence
            explainability: ExplainabilityDecomposer::new(),
            barrier,
            calibrator: IsotonicCalibrator::new(),

            lifecycle:      EngineLifecycle::Warming,
            ticks_received: 0,
            stale_since:    None,

            last_output: None,
            enabled_horizons: horizons,
            config,
        })
    }

    pub fn lifecycle(&self) -> EngineLifecycle { self.lifecycle }

    /// Ingest one **raw, noisy** [`ExchangeTick`] — called before any
    /// cleaning-pipeline filtering is applied. See module docs.
    ///
    /// Returns `Some(ReversionOutput)` once warmed up, or `None` while
    /// warming / when the snapshot could not be computed (e.g. degenerate
    /// VWAP). Staleness is tracked separately via [`check_staleness`].
    pub fn on_raw_tick(&mut self, tick: &ExchangeTick, received_ms: u64) -> Option<ReversionOutput> {
        self.ticks_received += 1;
        self.freshness.record();

        // Recover from stale state on any new tick.
        if self.lifecycle == EngineLifecycle::Stale {
            self.lifecycle = EngineLifecycle::Running;
            self.stale_since = None;
        }
        if self.lifecycle == EngineLifecycle::Warming
            && self.ticks_received >= self.config.warmup_ticks
        {
            self.lifecycle = EngineLifecycle::Running;
            info!(ticks = self.ticks_received, "ReversionEngine warm-up complete");
        }

        let price  = tick.mid_vwap()?;
        let volume = tick.total_bid_size() + tick.total_ask_size();
        let volume_f: f64 = {
            use rust_decimal::prelude::ToPrimitive;
            volume.to_f64().unwrap_or(0.0)
        };

        let missing = if tick.bids.is_empty() || tick.asks.is_empty() {
            crate::prediction::reversion::quality::DataQuality::MISSING_ORDER_BOOK
        } else {
            0
        };
        self.freshness.set_missing_fields(missing);

        let snapshot = self.build_snapshot(price, volume_f, tick.imbalance(), received_ms);

        if self.lifecycle != EngineLifecycle::Running {
            return None;
        }

        // Adapt equilibrium to current regime momentum
        let momentum_p = snapshot.regime.momentum_breakout;
        self.equilibrium.set_momentum_factor(momentum_p);

        let horizon_probs  = self.probability.estimate(&snapshot);
        let half_life      = self.half_life.update(snapshot.price);
        let contributors   = self.explainability.decompose(&snapshot);
        let barrier_active = self.barrier.is_continuation(&snapshot);

        let output = OutputBuilder::build(BuilderInputs {
            snapshot: &snapshot,
            horizon_probs,
            half_life,
            contributors,
            barrier_active,
            calibration_brier: self.calibrator.brier_score(),
            enabled_horizons: &self.enabled_horizons,
        });

        self.last_output = Some(output.clone());
        Some(output)
    }

    /// `true` when the feed has been silent beyond `stale_threshold`.
    pub fn check_staleness(&mut self) -> bool {
        let stale_ms = self.config.stale_threshold.as_millis() as u64;
        if self.freshness.is_stale(stale_ms) && self.lifecycle == EngineLifecycle::Running {
            if self.lifecycle != EngineLifecycle::Stale {
                self.stale_since = Some(Instant::now());
                self.lifecycle = EngineLifecycle::Stale;
                warn!("ReversionEngine: feed went stale");
            }
            return true;
        }
        false
    }

    pub fn current(&self) -> Option<&ReversionOutput> {
        self.last_output.as_ref()
    }

    /// Register a labelled outcome (host observed whether reversion occurred).
    pub fn register_outcome(&mut self, horizon: Horizon, predicted: f64, did_revert: bool) {
        self.probability.register_outcome(horizon, did_revert);
        self.calibrator.update(predicted, did_revert);
    }

    /// Reset engine state for a new prediction window. Calibration and the
    /// long-lived probability priors are intentionally **not** reset —
    /// only window-scoped accumulators (equilibrium, volatility, momentum)
    /// restart, matching the rest of the app's window lifecycle.
    ///
    /// Not currently called: the engine is presently reconstructed fresh on
    /// every `run_engine()` restart in `btc_feed.rs`, which already resets
    /// all window-scoped state. Exposed as a public hook for future callers
    /// that want in-place window resets without a full feed reconnect.
    #[allow(dead_code)]
    pub fn reset_window(&mut self) {
        self.equilibrium = EquilibriumTracker::new(
            self.config.ewma_fast_lambda,
            self.config.ewma_medium_lambda,
            self.config.vwap_weight,
            self.last_output.as_ref().map(|o| o.current_price).unwrap_or(0.0),
        );
        info!("ReversionEngine: window reset (equilibrium re-seeded)");
    }

    // ── private ───────────────────────────────────────────────────────────

    fn build_snapshot(
        &mut self,
        price: f64,
        volume: f64,
        ofi_from_book: f64,
        received_ms: u64,
    ) -> FeatureSnapshot {
        let vol_state = self.volatility.update(price);
        let (equilibrium, eq_confidence) = self.equilibrium.update(price, volume);

        let deviation_sigma = if vol_state.safe_sigma() > 0.0 {
            (price - equilibrium) / vol_state.safe_sigma()
        } else {
            0.0
        };
        let (stretch, _) = stretch_score(deviation_sigma);

        let order_flow = self.update_order_flow(ofi_from_book, volume);
        let momentum   = self.update_momentum(price, deviation_sigma, vol_state.safe_sigma());

        let regime = self.regime.classify(
            vol_state.vol_percentile,
            order_flow.ofi,
            order_flow.volume_zscore,
            momentum.velocity_sigma_per_tick,
            deviation_sigma,
        );

        let (tod_sin, tod_cos) = time_of_day_cyclical(received_ms as i64 * 1000);

        FeatureSnapshot {
            timestamp_us: received_ms as i64 * 1000,
            price,
            equilibrium,
            deviation_sigma,
            stretch_score: stretch,
            equilibrium_confidence: eq_confidence,
            volatility: vol_state,
            order_flow,
            momentum,
            regime,
            tod_sin,
            tod_cos,
            data_quality: self.freshness.quality(),
        }
    }

    fn update_order_flow(&mut self, ofi_from_book: f64, volume: f64) -> OrderFlowFeatures {
        self.last_ofi = ofi_from_book;

        self.volume_n += 1.0;
        let old_mean = self.rolling_volume_mean;
        self.rolling_volume_mean += (volume - old_mean) / self.volume_n;
        self.rolling_volume_var  += (volume - old_mean) * (volume - self.rolling_volume_mean);

        let volume_zscore = if self.volume_n > 2.0 {
            let std = (self.rolling_volume_var / (self.volume_n - 1.0)).sqrt().max(1e-12);
            (volume - self.rolling_volume_mean) / std
        } else {
            0.0
        };

        OrderFlowFeatures {
            ofi: ofi_from_book,
            cumulative_delta: self.cumulative_delta,
            volume_zscore,
            volume_surge: volume_zscore > self.config.barrier_volume_n,
        }
    }

    fn update_momentum(&mut self, price: f64, deviation_sigma: f64, safe_sigma: f64) -> MomentumFeatures {
        let velocity = match self.prev_price {
            Some(prev) => (price - prev) / safe_sigma,
            None => 0.0,
        };
        self.prev_price = Some(price);

        const LAMBDA: f64 = 0.1;
        self.momentum_ewma = self.momentum_ewma * (1.0 - LAMBDA) + velocity * LAMBDA;
        let rms = self.momentum_ewma.powi(2).sqrt().max(1e-12);
        let momentum_zscore = self.momentum_ewma / rms;

        let momentum_displacement_cosine = if velocity.abs() > 1e-12 && deviation_sigma.abs() > 1e-12 {
            (velocity * deviation_sigma) / (velocity.abs() * deviation_sigma.abs())
        } else {
            0.0
        };

        MomentumFeatures {
            velocity_sigma_per_tick: velocity,
            momentum_displacement_cosine,
            momentum_zscore,
        }
    }
}

fn time_of_day_cyclical(timestamp_us: i64) -> (f64, f64) {
    const DAY_US: f64 = 86_400.0 * 1_000_000.0;
    let frac = (timestamp_us as f64 % DAY_US) / DAY_US;
    let angle = 2.0 * std::f64::consts::PI * frac;
    (angle.sin(), angle.cos())
}
