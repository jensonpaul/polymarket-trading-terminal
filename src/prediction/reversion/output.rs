use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::prediction::reversion::{DataQuality, Horizon, MarketRegime, RegimeDistribution};
use crate::prediction::reversion::features::FeatureSnapshot;

// ── Multi-level retracement targets ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReversionLevels {
    pub entry_price:   f64,
    pub equilibrium:   f64,
    pub displacement:  f64,

    pub target_25pct:  f64,
    pub target_50pct:  f64,
    pub target_75pct:  f64,
    pub target_100pct: f64,

    pub prob_reach_25:  f64,
    pub prob_reach_50:  f64,
    pub prob_reach_75:  f64,
    pub prob_reach_100: f64,
}

impl ReversionLevels {
    pub fn from_prices(entry: f64, equilibrium: f64) -> Self {
        let d = entry - equilibrium;
        Self {
            entry_price:   entry,
            equilibrium,
            displacement:  d,
            target_25pct:  entry - 0.25 * d,
            target_50pct:  entry - 0.50 * d,
            target_75pct:  entry - 0.75 * d,
            target_100pct: equilibrium,
            prob_reach_25:  0.0,
            prob_reach_50:  0.0,
            prob_reach_75:  0.0,
            prob_reach_100: 0.0,
        }
    }
}

// ── Per-horizon output ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HorizonReversion {
    pub levels:      ReversionLevels,
    pub probability: f64,
}

// ── Contributor (explainability) ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contributor {
    pub name:   String,
    pub delta:  f64,
    pub weight: f64,
}

// ── Trade bias ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeBias {
    ShortMeanReversion,
    LongMeanReversion,
    Neutral,
    /// Continuation barrier fired; signal suppressed.
    Suppressed,
}

// ── Top-level output ──────────────────────────────────────────────────────────

/// The complete reversion inference snapshot for one tick.
///
/// Published to [`PredictionContext`] and consumed by [`MeanReversionStrategy`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReversionOutput {
    pub timestamp_us: i64,

    // ── stretch ──────────────────────────────────────────────────────────
    pub equilibrium_price:  f64,
    pub current_price:      f64,
    pub deviation_sigma:    f64,
    pub stretch_score:      f64,
    pub stretch_confidence: f64,

    // ── multi-level targets + probabilities ───────────────────────────────
    pub reversion: HashMap<Horizon, HorizonReversion>,

    // ── half-life ─────────────────────────────────────────────────────────
    pub half_life_seconds:    f64,
    pub half_life_confidence: f64,
    pub half_life_ci:         (f64, f64),

    // ── regime ───────────────────────────────────────────────────────────
    pub regime:               MarketRegime,
    pub regime_confidence:    f64,

    // ── opportunity ───────────────────────────────────────────────────────
    pub opportunity_score: f64,
    pub best_horizon:      Option<Horizon>,
    pub trade_bias:        TradeBias,

    // ── barrier ───────────────────────────────────────────────────────────
    pub barrier_active: bool,

    // ── explainability ────────────────────────────────────────────────────
    pub contributors: Vec<Contributor>,

    // ── calibration ───────────────────────────────────────────────────────
    pub calibration_brier: f64,

    // ── data quality ──────────────────────────────────────────────────────
    pub data_quality: DataQuality,
}

// ── OutputBuilder ─────────────────────────────────────────────────────────────

pub struct OutputBuilder;

pub struct BuilderInputs<'a> {
    pub snapshot:         &'a FeatureSnapshot,
    pub horizon_probs:    HashMap<Horizon, f64>,
    pub half_life:        Option<(f64, f64, (f64, f64))>,   // (seconds, conf, ci)
    pub contributors:     Vec<Contributor>,
    pub barrier_active:   bool,
    pub calibration_brier: f64,
    pub enabled_horizons: &'a [Horizon],
}

impl OutputBuilder {
    pub fn build(r: BuilderInputs<'_>) -> ReversionOutput {
        let snap = r.snapshot;
        let regime = snap.regime.mode();
        let regime_confidence = snap.regime.mode_confidence();

        let mut reversion = HashMap::with_capacity(r.enabled_horizons.len());
        for &h in r.enabled_horizons {
            let prob   = r.horizon_probs.get(&h).copied().unwrap_or(0.0);
            let mut lvl = ReversionLevels::from_prices(snap.price, snap.equilibrium);
            lvl.prob_reach_50  = prob;
            lvl.prob_reach_25  = reach_coeff(0.25) * prob;
            lvl.prob_reach_75  = reach_coeff(0.75) * prob;
            lvl.prob_reach_100 = reach_coeff(1.00) * prob;
            reversion.insert(h, HorizonReversion { levels: lvl, probability: prob });
        }

        let best_prob = r.horizon_probs.values().cloned().fold(f64::NEG_INFINITY, f64::max);
        let best_prob = if best_prob < 0.0 { 0.0 } else { best_prob };
        let best_horizon = r.horizon_probs
            .iter()
            .filter(|&(_, &p)| (p - best_prob).abs() < 1e-9)
            .map(|(&h, _)| h)
            .next();

        let opportunity_score = (best_prob
            * snap.regime.soft_reversion_multiplier()
            * snap.equilibrium_confidence
            * snap.data_quality.confidence)
            .clamp(0.0, 1.0);

        let trade_bias = if r.barrier_active {
            TradeBias::Suppressed
        } else if opportunity_score < 0.35 {
            TradeBias::Neutral
        } else if snap.deviation_sigma > 0.0 {
            TradeBias::ShortMeanReversion
        } else {
            TradeBias::LongMeanReversion
        };

        let (half_life_s, hl_conf, hl_ci) = r.half_life
            .unwrap_or((f64::NAN, 0.0, (f64::NAN, f64::NAN)));

        ReversionOutput {
            timestamp_us:       snap.timestamp_us,
            equilibrium_price:  snap.equilibrium,
            current_price:      snap.price,
            deviation_sigma:    snap.deviation_sigma,
            stretch_score:      snap.stretch_score,
            stretch_confidence: snap.equilibrium_confidence,
            reversion,
            half_life_seconds:    half_life_s,
            half_life_confidence: hl_conf,
            half_life_ci:         hl_ci,
            regime,
            regime_confidence,
            opportunity_score,
            best_horizon,
            trade_bias,
            barrier_active:     r.barrier_active,
            contributors:       r.contributors,
            calibration_brier:  r.calibration_brier,
            data_quality:       snap.data_quality,
        }
    }
}

fn reach_coeff(fraction: f64) -> f64 {
    (2.0 * fraction).clamp(0.0, 2.0).powf(0.7)
}
