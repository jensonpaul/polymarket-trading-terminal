//! Layer 2 — Cross-exchange outlier gate (MAD-based).
//!
//! After the per-exchange spike filter passes a tick, this gate checks
//! whether the tick's VWAP is consistent with *all other exchanges*.
//!
//! ## Algorithm
//!
//! 1. Maintain a ring of the most-recent accepted price from each exchange.
//! 2. Compute the cross-exchange **median** and **MAD** (Median Absolute
//!    Deviation) over those prices.
//! 3. Compute the **modified Z-score**:
//!    `z = 0.6745 × |price − median| / (MAD + ε)`
//! 4. If `z > threshold` the tick is an outlier and is dropped.
//!
//! MAD is more robust than standard deviation because it is not inflated
//! by the very outliers it is trying to detect.

use std::collections::HashMap;
use tracing::warn;

use crate::prediction::btc_aggregator::tick::Exchange;

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OutlierGateConfig {
    /// Modified Z-score threshold.  Values above this are outliers.
    ///
    /// Industry standard is 3.5; loosen to 4.0–5.0 during high volatility.
    pub z_threshold: f64,

    /// Minimum number of exchanges with a recent price before the gate
    /// activates.  Until then every tick passes.
    ///
    /// Default: 2 (need at least one other exchange to compare against).
    pub min_exchanges: usize,

    /// Maximum age in milliseconds for a price to be considered "recent"
    /// for cross-exchange comparison.
    ///
    /// Default: 5 000 ms (5 s).  Stale exchange prices are excluded from
    /// the median computation.
    pub max_age_ms: u64,
}

impl Default for OutlierGateConfig {
    fn default() -> Self {
        Self {
            z_threshold: 3.5,
            min_exchanges: 2,
            max_age_ms: 5_000,
        }
    }
}

// ── OutlierGate ───────────────────────────────────────────────────────────────

/// Cross-exchange consistency gate.
///
/// Keeps the latest accepted price and timestamp for each exchange and
/// uses the cross-exchange distribution to classify new prices.
pub struct OutlierGate {
    cfg: OutlierGateConfig,
    latest: HashMap<Exchange, (f64, u64)>, // (price, timestamp_ms)
}

impl OutlierGate {
    pub fn new(cfg: OutlierGateConfig) -> Self {
        Self {
            cfg,
            latest: HashMap::with_capacity(8),
        }
    }

    /// Returns `true` if `price` from `exchange` at `timestamp_ms` is
    /// consistent with the other known exchange prices.
    ///
    /// If accepted, the exchange's latest entry is updated.
    pub fn accept(
        &mut self,
        exchange: Exchange,
        price: f64,
        timestamp_ms: u64,
    ) -> bool {
        // Gather recent prices from *other* exchanges.
        let peers: Vec<f64> = self
            .latest
            .iter()
            .filter(|(ex, (_, ts))| {
                **ex != exchange
                    && timestamp_ms.saturating_sub(*ts) <= self.cfg.max_age_ms
            })
            .map(|(_, (p, _))| *p)
            .collect();

        if peers.len() + 1 < self.cfg.min_exchanges {
            // Not enough peers yet — accept unconditionally to seed the gate.
            self.latest.insert(exchange, (price, timestamp_ms));
            return true;
        }

        // Include candidate price in the pool for the median computation
        // so the gate is symmetric (the pool is all current prices).
        let mut pool: Vec<f64> = peers.clone();
        pool.push(price);

        let med = median(&mut pool);
        let mad = mad(&mut pool.clone(), med);

        // Modified Z-score (Iglewicz & Hoaglin).
        let z = 0.6745 * (price - med).abs() / (mad + f64::EPSILON);

        if z > self.cfg.z_threshold {
            /*
            warn!(
                exchange = exchange.label(),
                price,
                median = med,
                mad,
                z_score = z,
                threshold = self.cfg.z_threshold,
                "cross-exchange outlier rejected"
            );
            */
            return false;
        }

        self.latest.insert(exchange, (price, timestamp_ms));
        true
    }

    /// Cross-exchange median price from all recent exchange prices.
    ///
    /// Returns `None` if fewer than `min_exchanges` have recent data.
    pub fn cross_median(&self, now_ms: u64) -> Option<f64> {
        let mut prices: Vec<f64> = self
            .latest
            .values()
            .filter(|(_, ts)| now_ms.saturating_sub(*ts) <= self.cfg.max_age_ms)
            .map(|(p, _)| *p)
            .collect();

        if prices.len() < self.cfg.min_exchanges {
            return None;
        }

        Some(median(&mut prices))
    }
}

// ── Statistics helpers ────────────────────────────────────────────────────────

/// In-place median (sorts the slice).
fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 0 {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    } else {
        v[n / 2]
    }
}

/// Median Absolute Deviation given a pre-computed median.
fn mad(v: &mut Vec<f64>, med: f64) -> f64 {
    let mut deviations: Vec<f64> = v.iter().map(|x| (x - med).abs()).collect();
    median(&mut deviations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_prices_pass() {
        let mut gate = OutlierGate::new(OutlierGateConfig::default());
        // Seed with three close prices.
        assert!(gate.accept(Exchange::Binance,  105_000.0, 1_000));
        assert!(gate.accept(Exchange::Coinbase, 105_010.0, 1_001));
        assert!(gate.accept(Exchange::Kraken,   104_990.0, 1_002));
        // A fourth price in range should pass.
        assert!(gate.accept(Exchange::Bitstamp, 105_005.0, 1_003));
    }

    #[test]
    fn stale_outlier_rejected() {
        let mut gate = OutlierGate::new(OutlierGateConfig::default());
        assert!(gate.accept(Exchange::Binance,  105_000.0, 1_000));
        assert!(gate.accept(Exchange::Coinbase, 105_010.0, 1_001));
        assert!(gate.accept(Exchange::Kraken,   104_990.0, 1_002));
        // A price 500 USD off should be rejected.
        assert!(!gate.accept(Exchange::Bitstamp, 105_600.0, 1_003));
    }
}
