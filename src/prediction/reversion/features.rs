use crate::prediction::reversion::{DataQuality, RegimeDistribution};

/// The canonical, immutable representation of market state at one tick.
///
/// Built by [`FeatureBuilder`] from raw [`ExchangeTick`] data.
/// All consumer layers receive `&FeatureSnapshot` — nothing mutates it.
#[derive(Debug, Clone)]
pub struct FeatureSnapshot {
    pub timestamp_us: i64,

    // ── price ─────────────────────────────────────────────────────────────
    pub price:                  f64,
    pub equilibrium:            f64,
    /// Signed σ-displacement (positive = above equilibrium).
    pub deviation_sigma:        f64,
    /// Normalised stretch ∈ [0,1] via Φ(|deviation_σ|).
    pub stretch_score:          f64,
    pub equilibrium_confidence: f64,

    // ── volatility ────────────────────────────────────────────────────────
    pub volatility: VolatilityState,

    // ── order flow ────────────────────────────────────────────────────────
    pub order_flow: OrderFlowFeatures,

    // ── momentum ──────────────────────────────────────────────────────────
    pub momentum: MomentumFeatures,

    // ── regime ───────────────────────────────────────────────────────────
    pub regime: RegimeDistribution,

    // ── time-of-day (cyclical) ────────────────────────────────────────────
    pub tod_sin: f64,
    pub tod_cos: f64,

    // ── data quality ──────────────────────────────────────────────────────
    pub data_quality: DataQuality,
}

impl FeatureSnapshot {
    #[inline]
    pub fn displacement_sign(&self) -> f64 { self.deviation_sigma.signum() }

    #[inline]
    pub fn abs_deviation_sigma(&self) -> f64 { self.deviation_sigma.abs() }

    #[inline]
    pub fn is_trustworthy(&self) -> bool {
        self.data_quality.confidence > 0.5 && self.data_quality.feed_age_ms < 2_000
    }
}

// ── sub-types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct VolatilityState {
    pub rolling_sigma:  f64,
    pub ewma_sigma:     f64,
    pub blended_sigma:  f64,
    pub vol_percentile: f64,
}

impl VolatilityState {
    #[inline]
    pub fn safe_sigma(&self) -> f64 { self.blended_sigma.max(1e-8) }
}

impl Default for VolatilityState {
    fn default() -> Self {
        Self { rolling_sigma: 0.0, ewma_sigma: 0.0, blended_sigma: 0.0, vol_percentile: 0.0 }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OrderFlowFeatures {
    /// Order flow imbalance ∈ [-1, 1].
    pub ofi:              f64,
    pub cumulative_delta: f64,
    pub volume_zscore:    f64,
    pub volume_surge:     bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MomentumFeatures {
    /// Price velocity normalised by σ.
    pub velocity_sigma_per_tick:         f64,
    /// Cosine similarity of momentum vs displacement direction ∈ [-1,1].
    pub momentum_displacement_cosine:    f64,
    pub momentum_zscore:                 f64,
}

// ── Volatility estimator ──────────────────────────────────────────────────────

use std::collections::VecDeque;

pub struct VolatilityEstimator {
    window_ticks:   usize,
    ewma_lambda:    f64,
    window:         VecDeque<f64>,
    w_mean:         f64,
    w_m2:           f64,
    ewma_var:       f64,
    sigma_history:  VecDeque<f64>,
    n_total:        u64,
}

impl VolatilityEstimator {
    pub fn new(window_ticks: usize, ewma_lambda: f64) -> Self {
        Self {
            window_ticks,
            ewma_lambda,
            window:        VecDeque::with_capacity(window_ticks + 1),
            w_mean:        0.0,
            w_m2:          0.0,
            ewma_var:      0.0,
            sigma_history: VecDeque::with_capacity(2000),
            n_total:       0,
        }
    }

    pub fn update(&mut self, price: f64) -> VolatilityState {
        self.n_total += 1;
        let rolling_sigma = self.welford_update(price);

        if self.n_total == 1 {
            self.ewma_var = rolling_sigma.powi(2).max(1e-16);
        } else {
            let lambda = self.ewma_lambda;
            self.ewma_var = (1.0 - lambda) * self.ewma_var + lambda * price.powi(2);
        }
        let ewma_sigma = self.ewma_var.sqrt();
        let blended_sigma = harmonic_mean(rolling_sigma, ewma_sigma);

        self.sigma_history.push_back(blended_sigma);
        if self.sigma_history.len() > 2000 { self.sigma_history.pop_front(); }
        let vol_percentile = self.percentile(blended_sigma);

        VolatilityState { rolling_sigma, ewma_sigma, blended_sigma, vol_percentile }
    }

    fn welford_update(&mut self, price: f64) -> f64 {
        let n = self.window.len();
        if n >= self.window_ticks {
            if let Some(old) = self.window.pop_front() {
                let old_mean = self.w_mean;
                self.w_mean -= (old - self.w_mean) / n as f64;
                self.w_m2   -= (old - old_mean) * (old - self.w_mean);
                self.w_m2    = self.w_m2.max(0.0);
            }
        }
        self.window.push_back(price);
        let new_n = self.window.len() as f64;
        let old_mean = self.w_mean;
        self.w_mean += (price - old_mean) / new_n;
        self.w_m2   += (price - old_mean) * (price - self.w_mean);
        if new_n < 2.0 { return 0.0; }
        (self.w_m2 / (new_n - 1.0)).sqrt()
    }

    fn percentile(&self, sigma: f64) -> f64 {
        if self.sigma_history.is_empty() { return 0.5; }
        let below = self.sigma_history.iter().filter(|&&s| s <= sigma).count();
        below as f64 / self.sigma_history.len() as f64
    }
}

fn harmonic_mean(a: f64, b: f64) -> f64 {
    let a = a.max(1e-12);
    let b = b.max(1e-12);
    2.0 / (1.0 / a + 1.0 / b)
}

// ── Stretch scorer ────────────────────────────────────────────────────────────

pub fn stretch_score(deviation_sigma: f64) -> (f64, f64) {
    let abs_sigma = deviation_sigma.abs();
    let score = standard_normal_cdf(abs_sigma);
    let confidence = if abs_sigma < 0.1 { abs_sigma / 0.1 * 0.7 + 0.3 } else { 1.0 };
    (score, confidence)
}

fn standard_normal_cdf(x: f64) -> f64 {
    if x < 0.0 { return 1.0 - standard_normal_cdf(-x); }
    const P: f64 = 0.2316419;
    const B: [f64; 5] = [0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429];
    let t = 1.0 / (1.0 + P * x);
    let poly = t * (B[0] + t * (B[1] + t * (B[2] + t * (B[3] + t * B[4]))));
    1.0 - ((-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()) * poly
}

// ── Equilibrium tracker ───────────────────────────────────────────────────────

pub struct EquilibriumTracker {
    ewma_fast:              f64,
    ewma_medium:            f64,
    fast_lambda:            f64,
    medium_lambda:          f64,
    vwap_weight:            f64,
    vwap_cum_pv:            f64,
    vwap_cum_v:             f64,
    momentum_factor:        f64,
    ticks:                  u64,
}

impl EquilibriumTracker {
    pub fn new(fast_lambda: f64, medium_lambda: f64, vwap_weight: f64, seed: f64) -> Self {
        Self {
            ewma_fast:       seed,
            ewma_medium:     seed,
            fast_lambda,
            medium_lambda,
            vwap_weight,
            vwap_cum_pv:     seed * 1e-12,
            vwap_cum_v:      1e-12,
            momentum_factor: 0.0,
            ticks:           0,
        }
    }

    pub fn update(&mut self, price: f64, volume: f64) -> (f64, f64) {
        self.ticks += 1;
        self.ewma_fast   = self.fast_lambda   * price + (1.0 - self.fast_lambda)   * self.ewma_fast;
        self.ewma_medium = self.medium_lambda * price + (1.0 - self.medium_lambda) * self.ewma_medium;

        let vol = volume.max(0.0);
        self.vwap_cum_pv += price * vol;
        self.vwap_cum_v  += vol;
        let vwap = self.vwap_cum_pv / self.vwap_cum_v;

        let w_vwap  = self.vwap_weight;
        let w_ewma  = 1.0 - w_vwap;
        let fast_share   = (0.75 - 0.5 * self.momentum_factor).max(0.0);
        let medium_share = 1.0 - fast_share;

        let equilibrium = w_vwap  * vwap
            + w_ewma * fast_share   * self.ewma_fast
            + w_ewma * medium_share * self.ewma_medium;

        let warmup  = (self.ticks as f64 / 200.0).min(1.0);
        let penalty = 0.15 * self.momentum_factor;
        let confidence = (warmup - penalty).clamp(0.0, 1.0);

        (equilibrium, confidence)
    }

    pub fn set_momentum_factor(&mut self, factor: f64) {
        self.momentum_factor = 0.9 * self.momentum_factor + 0.1 * factor.clamp(0.0, 1.0);
    }
}
