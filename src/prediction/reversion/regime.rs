use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketRegime {
    NormalLiquidity,
    MeanRevertingVolatility,
    MomentumBreakout,
    LiquidationEvent,
}

impl MarketRegime {
    /// Mean-reversion confidence multiplier for this regime.
    pub fn reversion_multiplier(self) -> f64 {
        match self {
            Self::NormalLiquidity          => 1.00,
            Self::MeanRevertingVolatility  => 1.15,
            Self::MomentumBreakout         => 0.55,
            Self::LiquidationEvent         => 0.25,
        }
    }
}

/// Soft probability distribution over all regimes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeDistribution {
    pub normal_liquidity:          f64,
    pub mean_reverting_volatility: f64,
    pub momentum_breakout:         f64,
    pub liquidation_event:         f64,
}

impl Default for RegimeDistribution {
    fn default() -> Self {
        Self {
            normal_liquidity:          0.25,
            mean_reverting_volatility: 0.25,
            momentum_breakout:         0.25,
            liquidation_event:         0.25,
        }
    }
}

impl RegimeDistribution {
    pub fn mode(&self) -> MarketRegime {
        let arr = [
            (MarketRegime::NormalLiquidity,          self.normal_liquidity),
            (MarketRegime::MeanRevertingVolatility,  self.mean_reverting_volatility),
            (MarketRegime::MomentumBreakout,         self.momentum_breakout),
            (MarketRegime::LiquidationEvent,         self.liquidation_event),
        ];
        arr.into_iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(r, _)| r)
            .unwrap_or(MarketRegime::NormalLiquidity)
    }

    pub fn mode_confidence(&self) -> f64 {
        let total = self.total();
        if total < 1e-12 { return 0.0; }
        let mode_p = [
            self.normal_liquidity,
            self.mean_reverting_volatility,
            self.momentum_breakout,
            self.liquidation_event,
        ]
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);
        mode_p / total
    }

    pub fn normalise(&mut self) {
        let t = self.total();
        if t > 1e-12 {
            self.normal_liquidity          /= t;
            self.mean_reverting_volatility /= t;
            self.momentum_breakout         /= t;
            self.liquidation_event         /= t;
        }
    }

    /// Soft reversion multiplier weighted across the full distribution.
    pub fn soft_reversion_multiplier(&self) -> f64 {
        self.normal_liquidity          * MarketRegime::NormalLiquidity.reversion_multiplier()
            + self.mean_reverting_volatility * MarketRegime::MeanRevertingVolatility.reversion_multiplier()
            + self.momentum_breakout         * MarketRegime::MomentumBreakout.reversion_multiplier()
            + self.liquidation_event         * MarketRegime::LiquidationEvent.reversion_multiplier()
    }

    fn total(&self) -> f64 {
        self.normal_liquidity
            + self.mean_reverting_volatility
            + self.momentum_breakout
            + self.liquidation_event
    }
}

// ── RegimeClassifier ─────────────────────────────────────────────────────────

pub struct RegimeClassifier {
    momentum_ewma:  f64,
    vol_surge_ewma: f64,
    ofi_ewma:       f64,
}

impl RegimeClassifier {
    pub fn new() -> Self {
        Self { momentum_ewma: 0.0, vol_surge_ewma: 0.0, ofi_ewma: 0.0 }
    }

    pub fn classify(
        &mut self,
        vol_percentile:  f64,
        ofi:             f64,
        volume_zscore:   f64,
        velocity_sigma:  f64,
        deviation_sigma: f64,
    ) -> RegimeDistribution {
        const ALPHA: f64 = 0.05;
        self.momentum_ewma  = (1.0 - ALPHA) * self.momentum_ewma  + ALPHA * velocity_sigma.abs();
        self.vol_surge_ewma = (1.0 - ALPHA) * self.vol_surge_ewma + ALPHA * volume_zscore.max(0.0);
        self.ofi_ewma       = (1.0 - ALPHA) * self.ofi_ewma       + ALPHA * ofi;

        let normal_score = {
            let vol_ok    = 1.0 - vol_percentile;
            let no_surge  = if volume_zscore > 1.8 { 0.1 } else { 1.0 };
            let ofi_mod   = 1.0 - self.ofi_ewma.abs().min(1.0);
            (vol_ok * no_surge * ofi_mod).clamp(0.0, 1.0)
        };
        let mean_rev_score = {
            let vol_elevated  = vol_percentile;
            let low_momentum  = 1.0 - self.momentum_ewma.min(3.0) / 3.0;
            let stretched     = (deviation_sigma.abs() / 3.0).min(1.0);
            (vol_elevated * low_momentum * (0.5 + 0.5 * stretched)).clamp(0.0, 1.0)
        };
        let momentum_score = {
            let fast_vel    = (self.momentum_ewma / 2.0).min(1.0);
            let aligned_ofi = (self.ofi_ewma * velocity_sigma.signum()).max(0.0);
            (fast_vel * (0.5 + 0.5 * aligned_ofi)).clamp(0.0, 1.0)
        };
        let liq_score = {
            let surge    = (self.vol_surge_ewma / 3.0).min(1.0);
            let velocity = (velocity_sigma.abs() / 3.0).min(1.0);
            let high_vol = (vol_percentile - 0.85).max(0.0) / 0.15;
            (surge * velocity * high_vol).clamp(0.0, 1.0)
        };

        let mut dist = RegimeDistribution {
            normal_liquidity:          normal_score,
            mean_reverting_volatility: mean_rev_score,
            momentum_breakout:         momentum_score,
            liquidation_event:         liq_score,
        };
        dist.normalise();
        dist
    }
}

impl Default for RegimeClassifier { fn default() -> Self { Self::new() } }
