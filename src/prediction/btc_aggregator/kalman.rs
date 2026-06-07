//! Layer 4 — Kalman filter smoother.
//!
//! Applies a 1-D Kalman filter to the sequence of VWMP candle prices.
//! The filter maintains an estimate of the "true" latent BTC price by
//! separating process noise (how fast the true price can move) from
//! measurement noise (how noisy the aggregated VWMP is).
//!
//! ## Tuning
//!
//! | Parameter         | Effect                                             |
//! |-------------------|----------------------------------------------------|
//! | `process_noise` Q | Higher → filter tracks fast moves; less smoothing |
//! | `measure_noise` R | Higher → smoother output; more lag               |
//!
//! Good starting values for BTC at 250 ms candles:
//! - Q = 1e-4  (true price moves relatively slowly between buckets)
//! - R = 1e-2  (VWMP is still somewhat noisy after aggregation)
//!
//! ## Adaptive mode (optional)
//!
//! When `adaptive` is enabled, R is scaled by the candle's short-term
//! volatility proxy: higher volatility → higher R → smoother during
//! choppy periods, faster during clean trends.
//!
//! ## Output
//!
//! [`KalmanSmoother::update`] returns a [`SmoothedPrice`] that carries
//! both the Kalman estimate and the Kalman gain (useful for diagnostics:
//! a gain near 1.0 means the filter is trusting the measurement heavily).

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KalmanConfig {
    /// Process noise variance Q.
    ///
    /// Models how much the true price can change between measurements.
    /// Default: 1e-4.
    pub process_noise: f64,

    /// Measurement noise variance R.
    ///
    /// Models how noisy the aggregated VWMP is.
    /// Default: 1e-2.
    pub measure_noise: f64,

    /// When `true`, R is scaled dynamically by the incoming candle's
    /// volatility proxy (high/low range as a fraction of mid).
    ///
    /// Default: `true`.
    pub adaptive: bool,

    /// Scalar applied to the volatility proxy when computing adaptive R.
    ///
    /// `R_effective = measure_noise * (1 + adaptive_scale * vol_proxy)`
    ///
    /// Default: 10.0.
    pub adaptive_scale: f64,
}

impl Default for KalmanConfig {
    fn default() -> Self {
        Self {
            process_noise: 1e-4,
            measure_noise: 1e-2,
            adaptive: true,
            adaptive_scale: 10.0,
        }
    }
}

// ── Output ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SmoothedPrice {
    /// Kalman-filtered price estimate.
    pub estimate: f64,

    /// Kalman gain in [0, 1].
    ///
    /// Near 1 → filter trusts the new measurement heavily (fast adaption).
    /// Near 0 → filter ignores the measurement (relies on prior estimate).
    pub gain: f64,

    /// Effective measurement noise used in this update step.
    pub r_effective: f64,

    /// Raw VWMP that was fed in (pre-smoothing).
    pub raw_vwmp: f64,
}

// ── KalmanSmoother ────────────────────────────────────────────────────────────

/// Single-state Kalman filter (constant-velocity model, scalar form).
///
/// Initialize once and call [`KalmanSmoother::update`] for each new candle.
pub struct KalmanSmoother {
    cfg: KalmanConfig,

    /// Current state estimate x̂.
    estimate: f64,

    /// Estimate error covariance P.
    error_cov: f64,

    /// Whether the filter has been seeded with the first measurement.
    initialized: bool,
}

impl KalmanSmoother {
    pub fn new(cfg: KalmanConfig) -> Self {
        Self {
            cfg,
            estimate: 0.0,
            error_cov: 1.0,
            initialized: false,
        }
    }

    /// Feed one VWMP measurement to the filter.
    ///
    /// On the first call the filter is initialized to the measurement
    /// directly (no smoothing applied for the seed value).
    ///
    /// # Arguments
    ///
    /// * `vwmp` — the aggregated, outlier-cleaned mid price.
    /// * `vol_proxy` — a volatility proxy in [0, ∞) used for adaptive R.
    ///   A convenient choice is `(high - low) / mid` from the candle.
    pub fn update(&mut self, vwmp: f64, vol_proxy: f64) -> SmoothedPrice {
        if !self.initialized {
            self.estimate = vwmp;
            self.initialized = true;
            return SmoothedPrice {
                estimate: vwmp,
                gain: 1.0,
                r_effective: self.cfg.measure_noise,
                raw_vwmp: vwmp,
            };
        }

        // ── Predict ───────────────────────────────────────────────────────
        // State prediction: x̂⁻ = x̂  (random walk model — price is a
        // martingale between candles at this resolution).
        let predicted_cov = self.error_cov + self.cfg.process_noise;

        // ── Adaptive R ────────────────────────────────────────────────────
        let r_effective = if self.cfg.adaptive {
            self.cfg.measure_noise * (1.0 + self.cfg.adaptive_scale * vol_proxy)
        } else {
            self.cfg.measure_noise
        };

        // ── Update (Innovation) ───────────────────────────────────────────
        let gain = predicted_cov / (predicted_cov + r_effective);
        let innovation = vwmp - self.estimate;

        self.estimate = self.estimate + gain * innovation;
        self.error_cov = (1.0 - gain) * predicted_cov;

        SmoothedPrice {
            estimate: self.estimate,
            gain,
            r_effective,
            raw_vwmp: vwmp,
        }
    }

    /// Current estimate (does not advance the filter).
    pub fn current_estimate(&self) -> Option<f64> {
        if self.initialized { Some(self.estimate) } else { None }
    }

    /// Reset the filter state (call on window boundary).
    pub fn reset(&mut self) {
        self.estimate = 0.0;
        self.error_cov = 1.0;
        self.initialized = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_on_first_call() {
        let mut kf = KalmanSmoother::new(KalmanConfig::default());
        let out = kf.update(105_000.0, 0.0);
        assert_eq!(out.estimate, 105_000.0);
        assert_eq!(out.gain, 1.0);
    }

    #[test]
    fn smooths_noise() {
        let mut kf = KalmanSmoother::new(KalmanConfig::default());
        kf.update(100.0, 0.0);

        // Simulate a noisy flat signal around 100.
        let measurements = [101.0, 99.0, 100.5, 99.5, 100.2];
        let mut estimates: Vec<f64> = Vec::new();

        for &m in &measurements {
            let out = kf.update(m, 0.001);
            estimates.push(out.estimate);
        }

        // The Kalman estimate should be smoother (less variance) than raw.
        let raw_var: f64 = measurements.iter().map(|x| (x - 100.0).powi(2)).sum::<f64>()
            / measurements.len() as f64;
        let est_var: f64 = estimates.iter().map(|x| (x - 100.0).powi(2)).sum::<f64>()
            / estimates.len() as f64;

        assert!(est_var < raw_var, "Kalman should reduce variance");
    }
}
