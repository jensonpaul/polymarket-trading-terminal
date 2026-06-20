//! Consumer-layer components: probability estimation, half-life, calibration,
//! continuation barrier, explainability. All consume `&FeatureSnapshot`.

use std::collections::{HashMap, VecDeque};

use crate::prediction::reversion::config::ReversionConfig;
use crate::prediction::reversion::features::FeatureSnapshot;
use crate::prediction::reversion::horizon::Horizon;
use crate::prediction::reversion::output::Contributor;
use crate::prediction::reversion::regime::MarketRegime;

// ── Continuation barrier ──────────────────────────────────────────────────────

/// Suppresses signal emission when a trend continuation is more likely than
/// a mean reversion. Advisory only — never touches probability state or
/// calibration, so metrics are never poisoned by filtered samples.
pub struct ContinuationBarrier {
    velocity_k:         f64,
    volume_n:           f64,
    momentum_threshold: f64,
    enabled:            bool,
}

impl ContinuationBarrier {
    pub fn new(cfg: &ReversionConfig) -> Self {
        Self {
            velocity_k:         cfg.barrier_velocity_k,
            volume_n:           cfg.barrier_volume_n,
            momentum_threshold: cfg.barrier_momentum_threshold,
            enabled:            cfg.barrier_enabled,
        }
    }

    pub fn is_continuation(&self, snap: &FeatureSnapshot) -> bool {
        if !self.enabled { return false; }

        let regime = snap.regime.mode();
        let velocity_exceeded = snap.momentum.velocity_sigma_per_tick.abs() > self.velocity_k;
        let volume_surge      = snap.order_flow.volume_zscore > self.volume_n;
        let momentum_aligned  = snap.momentum.momentum_displacement_cosine > self.momentum_threshold;
        let hostile_regime    = matches!(
            regime,
            MarketRegime::MomentumBreakout | MarketRegime::LiquidationEvent
        );

        velocity_exceeded && volume_surge && momentum_aligned && hostile_regime
    }
}

// ── Online isotonic calibrator ────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct CalibrationBin {
    sum_predicted: f64,
    sum_outcome:   f64,
    count:         f64,
}

impl CalibrationBin {
    fn rate(&self) -> f64 {
        if self.count < 1.0 { return 0.5; }
        self.sum_outcome / self.count
    }
}

pub struct IsotonicCalibrator {
    bins: Vec<CalibrationBin>,
}

impl IsotonicCalibrator {
    pub fn new() -> Self {
        Self { bins: vec![CalibrationBin::default(); 10] }
    }

    pub fn update(&mut self, predicted: f64, did_revert: bool) {
        let idx = (predicted * 10.0).floor().clamp(0.0, 9.0) as usize;
        let b = &mut self.bins[idx];
        b.sum_predicted += predicted;
        b.sum_outcome   += if did_revert { 1.0 } else { 0.0 };
        b.count         += 1.0;
    }

    pub fn calibrate(&self, raw: f64) -> f64 {
        let idx = (raw * 10.0).floor().clamp(0.0, 9.0) as usize;
        if self.bins[idx].count < 10.0 { return raw; }
        let rate  = self.bins[idx].rate();
        let trust = (self.bins[idx].count / 100.0).min(1.0);
        trust * rate + (1.0 - trust) * raw
    }

    pub fn brier_score(&self) -> f64 {
        let (mut total, mut n) = (0.0_f64, 0.0_f64);
        for bin in &self.bins {
            if bin.count > 0.0 {
                let avg_pred = bin.sum_predicted / bin.count;
                let avg_out  = bin.rate();
                total += (avg_pred - avg_out).powi(2) * bin.count;
                n += bin.count;
            }
        }
        if n < 1.0 { 0.0 } else { total / n }
    }
}

impl Default for IsotonicCalibrator { fn default() -> Self { Self::new() } }

// ── Per-horizon Bayesian probability estimator ────────────────────────────────

struct FeatureWeights {
    stretch:        f64,
    deviation_abs:  f64,
    vol_percentile: f64,
    ofi:            f64,
    volume_zscore:  f64,
    momentum_cos:   f64,
    eq_confidence:  f64,
    tod_sin:        f64,
    intercept:      f64,
}

impl FeatureWeights {
    fn default_for(horizon: Horizon) -> Self {
        match horizon {
            Horizon::S5 | Horizon::S10 => Self {
                stretch: 1.8, deviation_abs: 1.2, vol_percentile: -0.3, ofi: 0.9,
                volume_zscore: -0.4, momentum_cos: 0.6, eq_confidence: 0.3,
                tod_sin: 0.0, intercept: -0.5,
            },
            Horizon::S30 | Horizon::M1 => Self {
                stretch: 1.5, deviation_abs: 1.0, vol_percentile: 0.2, ofi: 0.6,
                volume_zscore: -0.5, momentum_cos: 0.4, eq_confidence: 0.5,
                tod_sin: 0.1, intercept: -0.4,
            },
            Horizon::M5 => Self {
                stretch: 1.0, deviation_abs: 0.8, vol_percentile: 0.5, ofi: 0.3,
                volume_zscore: -0.2, momentum_cos: 0.2, eq_confidence: 0.8,
                tod_sin: 0.15, intercept: -0.3,
            },
        }
    }
}

struct HorizonEstimator {
    weights:    FeatureWeights,
    beta_alpha: f64,
    beta_beta:  f64,
}

impl HorizonEstimator {
    fn new(horizon: Horizon) -> Self {
        Self { weights: FeatureWeights::default_for(horizon), beta_alpha: 1.0, beta_beta: 1.0 }
    }

    fn infer(&self, snap: &FeatureSnapshot) -> f64 {
        let w = &self.weights;
        let logit =
              w.stretch        * snap.stretch_score
            + w.deviation_abs  * (snap.deviation_sigma.abs() / 5.0).min(1.0)
            + w.vol_percentile * snap.volatility.vol_percentile
            + w.ofi            * snap.order_flow.ofi * (-snap.deviation_sigma.signum())
            + w.volume_zscore  * snap.order_flow.volume_zscore.clamp(-3.0, 3.0) / 3.0
            + w.momentum_cos   * (-snap.momentum.momentum_displacement_cosine)
            + w.eq_confidence  * snap.equilibrium_confidence
            + w.tod_sin        * snap.tod_sin
            + w.intercept;
        sigmoid(logit)
    }

    fn prior_mean(&self) -> f64 { self.beta_alpha / (self.beta_alpha + self.beta_beta) }

    fn update_outcome(&mut self, did_revert: bool) {
        const LR: f64 = 0.05;
        let outcome = if did_revert { 1.0 } else { 0.0 };
        self.beta_alpha += LR * outcome;
        self.beta_beta  += LR * (1.0 - outcome);
    }
}

pub struct ProbabilityEngine {
    estimators: HashMap<Horizon, HorizonEstimator>,
}

impl ProbabilityEngine {
    pub fn new(horizons: &[Horizon]) -> Self {
        Self {
            estimators: horizons.iter().map(|&h| (h, HorizonEstimator::new(h))).collect(),
        }
    }

    pub fn estimate(&self, snapshot: &FeatureSnapshot) -> HashMap<Horizon, f64> {
        let regime_weight = snapshot.regime.soft_reversion_multiplier();
        let quality_weight = snapshot.data_quality.confidence;

        self.estimators.iter().map(|(&h, est)| {
            let raw = est.infer(snapshot);
            let regime_adjusted = (raw * regime_weight).clamp(0.0, 1.0);
            let final_prob = regime_adjusted * quality_weight
                + (1.0 - quality_weight) * est.prior_mean();
            (h, final_prob)
        }).collect()
    }

    pub fn register_outcome(&mut self, horizon: Horizon, did_revert: bool) {
        if let Some(est) = self.estimators.get_mut(&horizon) {
            est.update_outcome(did_revert);
        }
    }
}

fn sigmoid(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }

// ── Half-life estimator (Ornstein-Uhlenbeck / Vasicek) ────────────────────────

pub struct HalfLifeEstimator {
    window_size:  usize,
    window:       VecDeque<f64>,
    n:            f64,
    mean_x:       f64,
    mean_y:       f64,
    cov_xy:       f64,
    var_x:        f64,
    tick_seconds: f64,
}

impl HalfLifeEstimator {
    pub fn new(window_size: usize, tick_seconds: f64) -> Self {
        Self {
            window_size,
            window: VecDeque::with_capacity(window_size + 1),
            n: 0.0, mean_x: 0.0, mean_y: 0.0, cov_xy: 0.0, var_x: 0.0,
            tick_seconds: tick_seconds.max(1e-6),
        }
    }

    /// Returns `(half_life_seconds, confidence, (ci_lo, ci_hi))` or `None`
    /// before warm-up / when the process is not mean-reverting.
    pub fn update(&mut self, price: f64) -> Option<(f64, f64, (f64, f64))> {
        if let Some(&prev) = self.window.back() {
            let (x, y) = (prev, price);
            self.n += 1.0;
            let dx = x - self.mean_x;
            self.mean_x += dx / self.n;
            self.mean_y += (y - self.mean_y) / self.n;
            self.cov_xy += dx * (y - self.mean_y);
            self.var_x  += dx * (x - self.mean_x);
        }
        self.window.push_back(price);
        if self.window.len() > self.window_size { self.window.pop_front(); }
        if self.n < 30.0 { return None; }
        self.compute()
    }

    fn compute(&self) -> Option<(f64, f64, (f64, f64))> {
        if self.var_x < 1e-12 { return None; }
        let beta = (self.cov_xy / self.var_x).clamp(-0.9999, 0.9999);
        if beta <= 0.0 { return None; }

        let theta = -beta.ln() / self.tick_seconds;
        if theta < 1e-12 { return None; }
        let half_life = 2_f64.ln() / theta;

        let size_conf = (self.n / 200.0).min(1.0);
        let beta_conf = 1.0 - (beta - 0.5).abs().min(0.5) / 0.5 * 0.5;
        let confidence = (size_conf * beta_conf).clamp(0.0, 1.0);

        let var_beta = (1.0 - beta.powi(2)) / self.n.max(2.0);
        let se_beta  = var_beta.sqrt();
        let z90      = 1.645_f64;
        let d_hl_d_beta = 2_f64.ln() / (theta * beta.abs().max(1e-12));
        let lo = (half_life - z90 * d_hl_d_beta * se_beta).max(0.1);
        let hi = half_life + z90 * d_hl_d_beta * se_beta;

        Some((half_life, confidence, (lo, hi)))
    }
}

// ── Explainability decomposer ─────────────────────────────────────────────────

pub struct ExplainabilityDecomposer {
    stretch_mean:       f64,
    deviation_mean:     f64,
    vol_mean:           f64,
    ofi_mean:           f64,
    vol_zscore_mean:    f64,
    momentum_cos_mean:  f64,
}

impl ExplainabilityDecomposer {
    pub fn new() -> Self {
        Self {
            stretch_mean: 0.5, deviation_mean: 0.0, vol_mean: 0.5,
            ofi_mean: 0.0, vol_zscore_mean: 0.0, momentum_cos_mean: 0.0,
        }
    }

    pub fn decompose(&mut self, snap: &FeatureSnapshot) -> Vec<Contributor> {
        const ALPHA: f64 = 0.01;
        self.stretch_mean      = (1.0 - ALPHA) * self.stretch_mean      + ALPHA * snap.stretch_score;
        self.deviation_mean    = (1.0 - ALPHA) * self.deviation_mean    + ALPHA * snap.deviation_sigma.abs();
        self.vol_mean          = (1.0 - ALPHA) * self.vol_mean          + ALPHA * snap.volatility.vol_percentile;
        self.ofi_mean          = (1.0 - ALPHA) * self.ofi_mean          + ALPHA * snap.order_flow.ofi;
        self.vol_zscore_mean   = (1.0 - ALPHA) * self.vol_zscore_mean   + ALPHA * snap.order_flow.volume_zscore;
        self.momentum_cos_mean = (1.0 - ALPHA) * self.momentum_cos_mean + ALPHA * snap.momentum.momentum_displacement_cosine;

        const W_STRETCH: f64 = 0.31;
        const W_DEVIATION: f64 = 0.22;
        const W_VOL: f64 = 0.10;
        const W_OFI: f64 = 0.18;
        const W_VOL_Z: f64 = -0.08;
        const W_MOM_COS: f64 = -0.11;

        vec![
            Contributor { name: "price_stretch".into(),           delta: W_STRETCH   * (snap.stretch_score - self.stretch_mean),                                       weight: W_STRETCH },
            Contributor { name: "deviation_magnitude".into(),     delta: W_DEVIATION * (snap.deviation_sigma.abs() - self.deviation_mean),                             weight: W_DEVIATION },
            Contributor { name: "order_flow_exhaustion".into(),   delta: W_OFI       * (snap.order_flow.ofi * (-snap.displacement_sign()) - self.ofi_mean),            weight: W_OFI },
            Contributor { name: "volatility_regime".into(),       delta: W_VOL       * (snap.volatility.vol_percentile - self.vol_mean),                               weight: W_VOL },
            Contributor { name: "volume_surge".into(),            delta: W_VOL_Z     * (snap.order_flow.volume_zscore - self.vol_zscore_mean),                         weight: W_VOL_Z },
            Contributor { name: "momentum_alignment".into(),      delta: W_MOM_COS   * (snap.momentum.momentum_displacement_cosine - self.momentum_cos_mean),          weight: W_MOM_COS },
        ]
    }
}

impl Default for ExplainabilityDecomposer { fn default() -> Self { Self::new() } }
