//! Layer 1 — Per-exchange spike gate.
//!
//! Each exchange feed gets its own [`ExchangeFilter`] instance.  A tick
//! is rejected if its mid-VWAP deviates by more than `max_change_pct`
//! from the exchange's own exponential moving average (EMA).
//!
//! Using an EMA (rather than the last raw tick) makes the gate robust to
//! an earlier false positive that slipped through: the EMA adapts gradually
//! so a single bad tick cannot permanently corrupt the baseline.

use std::collections::HashMap;
use tracing::warn;

use crate::prediction::btc_aggregator::tick::{Exchange, ExchangeTick};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Tuning parameters for the per-exchange spike filter.
#[derive(Debug, Clone)]
pub struct SpikeFilterConfig {
    /// Maximum allowed single-tick price change as a fraction of the
    /// current EMA.  Default: 0.003 (0.3 %).
    ///
    /// A tick whose mid-VWAP differs from the EMA by more than this
    /// fraction is classified as a spike and dropped.
    pub max_change_frac: f64,

    /// EMA smoothing factor α ∈ (0, 1].
    ///
    /// `α = 1.0` → EMA equals the latest accepted price (no smoothing).
    /// `α = 0.05` → very slow-moving baseline.
    ///
    /// Default: 0.1  (≈ N=19 EMA period).
    pub ema_alpha: f64,

    /// Minimum number of accepted ticks before the spike gate activates.
    ///
    /// During warm-up every tick is accepted and used to seed the EMA.
    /// Default: 3.
    pub warmup_ticks: usize,
}

impl Default for SpikeFilterConfig {
    fn default() -> Self {
        Self {
            max_change_frac: 0.003,
            ema_alpha: 0.1,
            warmup_ticks: 3,
        }
    }
}

// ── Per-exchange state ────────────────────────────────────────────────────────

struct ExchangeState {
    ema: f64,
    accepted: usize,
    rejected: usize,
}

impl ExchangeState {
    fn new(seed: f64) -> Self {
        Self { ema: seed, accepted: 1, rejected: 0 }
    }

    /// Test `price` against the current EMA; update EMA if accepted.
    fn test_and_update(
        &mut self,
        price: f64,
        cfg: &SpikeFilterConfig,
    ) -> bool {
        if self.accepted < cfg.warmup_ticks {
            // Warm-up: accept unconditionally, blend into EMA.
            self.ema = cfg.ema_alpha * price + (1.0 - cfg.ema_alpha) * self.ema;
            self.accepted += 1;
            return true;
        }

        let change = (price - self.ema).abs() / self.ema;
        if change > cfg.max_change_frac {
            self.rejected += 1;
            return false;
        }

        self.ema = cfg.ema_alpha * price + (1.0 - cfg.ema_alpha) * self.ema;
        self.accepted += 1;
        true
    }
}

// ── SpikeFilter ───────────────────────────────────────────────────────────────

/// Maintains independent EMA baselines for every known exchange.
///
/// Construct once and call [`SpikeFilter::accept`] for each incoming tick.
pub struct SpikeFilter {
    cfg: SpikeFilterConfig,
    state: HashMap<Exchange, ExchangeState>,
}

impl SpikeFilter {
    pub fn new(cfg: SpikeFilterConfig) -> Self {
        Self {
            cfg,
            state: HashMap::with_capacity(8),
        }
    }

    /// Returns `true` if the tick should be forwarded to the next pipeline
    /// stage; `false` if it is a spike and should be dropped.
    ///
    /// The first tick from any exchange always passes (it seeds the EMA).
    pub fn accept(&mut self, tick: &ExchangeTick) -> bool {
        let price = match tick.mid_vwap() {
            Some(p) if p > 0.0 => p,
            _ => {
                /*
                warn!(
                    exchange = tick.exchange.label(),
                    "tick has no computable mid-VWAP — dropped"
                );
                */
                return false;
            }
        };

        match self.state.get_mut(&tick.exchange) {
            Some(state) => state.test_and_update(price, &self.cfg),
            None => {
                // First tick from this exchange — always accept.
                self.state.insert(tick.exchange, ExchangeState::new(price));
                true
            }
        }
    }

    /// Returns the current EMA for an exchange, if it has been seeded.
    pub fn ema(&self, exchange: Exchange) -> Option<f64> {
        self.state.get(&exchange).map(|s| s.ema)
    }

    /// Diagnostic: total accepted / rejected counts per exchange.
    pub fn stats(&self) -> Vec<(Exchange, usize, usize)> {
        self.state
            .iter()
            .map(|(ex, s)| (*ex, s.accepted, s.rejected))
            .collect()
    }
}
