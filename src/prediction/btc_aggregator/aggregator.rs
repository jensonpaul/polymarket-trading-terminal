//! Layer 3 — Time-bucket aggregator.
//!
//! Collapses all clean ticks within a configurable window into:
//! - An OHLCV candle
//! - A trust-weighted Volume-Weighted Mid Price (VWMP)
//! - Per-exchange contribution breakdown
//!
//! The trust weight blends exchange reliability (static, from
//! [`Exchange::trust_weight`]) with volume (dynamic): heavier-volume
//! feeds contribute more to the VWMP, but lower-trust feeds are
//! attenuated regardless of their volume.
//!
//! ## Bucket lifecycle
//!
//! A bucket is **open** from the moment its first tick arrives until
//! `bucket_ms` milliseconds have elapsed.  Calling [`Aggregator::flush`]
//! at any time returns a completed [`Candle`] if the bucket has enough
//! ticks (>= `min_ticks`) or `None` otherwise.
//!
//! The aggregator auto-flushes whenever a new tick arrives in a *later*
//! bucket period, so callers do not need to manage timing externally.

use std::collections::HashMap;

use crate::prediction::btc_aggregator::tick::{Exchange, ExchangeTick};

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// Bucket width in milliseconds.  250 ms is a good default for live
    /// charts: fresh enough to feel real-time, stable enough to be clean.
    pub bucket_ms: u64,

    /// Minimum number of clean ticks required to emit a candle.
    /// Buckets with fewer ticks are discarded.
    ///
    /// Default: 1 (emit even single-tick buckets).
    pub min_ticks: usize,

    /// If `true`, the VWMP is additionally attenuated by the per-exchange
    /// trust weight.  If `false`, only volume is used for weighting.
    ///
    /// Default: `true`.
    pub use_trust_weight: bool,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            bucket_ms: 250,
            min_ticks: 1,
            use_trust_weight: true,
        }
    }
}

// ── Candle ────────────────────────────────────────────────────────────────────

/// One completed time bucket.
#[derive(Debug, Clone)]
pub struct Candle {
    /// Bucket open time (floor of first tick's timestamp to bucket boundary).
    pub open_ms: u64,

    /// Bucket close time (open_ms + bucket_ms).
    pub close_ms: u64,

    pub open:  f64,
    pub high:  f64,
    pub low:   f64,
    pub close: f64,

    /// Trust-weighted volume-weighted mid price.
    /// This is the primary "clean price" consumed by downstream layers.
    pub vwmp: f64,

    /// Total base-currency volume across all exchanges in this bucket.
    pub volume: f64,

    /// Number of raw ticks that contributed to this candle.
    pub tick_count: usize,

    /// Per-exchange VWMP contribution for diagnostics.
    pub exchange_contributions: HashMap<Exchange, ExchangeContribution>,
}

#[derive(Debug, Clone)]
pub struct ExchangeContribution {
    pub mid_vwap: f64,
    pub volume:   f64,
    pub weight:   f64,
    pub ticks:    usize,
}

// ── Internal bucket state ─────────────────────────────────────────────────────

#[derive(Default)]
struct BucketState {
    open_ms:    u64,
    first_price: Option<f64>,
    high:       f64,
    low:        f64,
    last_price: f64,
    tick_count: usize,

    // Per exchange: (weighted_price_sum, weight_sum, volume_sum, ticks)
    per_exchange: HashMap<Exchange, (f64, f64, f64, usize)>,
}

impl BucketState {
    fn new(open_ms: u64) -> Self {
        Self {
            open_ms,
            high: f64::NEG_INFINITY,
            low:  f64::INFINITY,
            ..Default::default()
        }
    }

    fn ingest(&mut self, tick: &ExchangeTick, use_trust: bool) {
        let mid = match tick.mid_vwap() {
            Some(v) => v,
            None => return,
        };

        use rust_decimal::prelude::ToPrimitive;
        let vol = (tick.total_bid_size() + tick.total_ask_size())
            .to_f64()
            .unwrap_or(0.0);

        let trust = if use_trust {
            tick.exchange.trust_weight()
        } else {
            1.0
        };

        // Combined weight = volume × trust.
        let w = vol * trust;

        let entry = self.per_exchange.entry(tick.exchange).or_default();
        entry.0 += mid * w; // weighted price sum
        entry.1 += w;       // weight sum
        entry.2 += vol;     // raw volume (for reporting)
        entry.3 += 1;       // tick count

        if self.first_price.is_none() {
            self.first_price = Some(mid);
        }
        if mid > self.high { self.high = mid; }
        if mid < self.low  { self.low  = mid; }
        self.last_price = mid;
        self.tick_count += 1;
    }

    fn into_candle(self, bucket_ms: u64) -> Option<Candle> {
        if self.tick_count == 0 || self.first_price.is_none() {
            return None;
        }

        let mut total_weighted = 0.0;
        let mut total_weight   = 0.0;
        let mut total_volume   = 0.0;

        let mut contributions = HashMap::new();

        for (ex, (wp_sum, w_sum, vol, ticks)) in &self.per_exchange {
            if *w_sum == 0.0 { continue; }

            let mid_vwap = wp_sum / w_sum;
            total_weighted += mid_vwap * w_sum;
            total_weight   += w_sum;
            total_volume   += vol;

            contributions.insert(*ex, ExchangeContribution {
                mid_vwap,
                volume: *vol,
                weight: *w_sum,
                ticks: *ticks,
            });
        }

        let vwmp = if total_weight > 0.0 {
            total_weighted / total_weight
        } else {
            self.last_price
        };

        Some(Candle {
            open_ms:  self.open_ms,
            close_ms: self.open_ms + bucket_ms,
            open:     self.first_price.unwrap_or(self.last_price),
            high:     self.high,
            low:      self.low,
            close:    self.last_price,
            vwmp,
            volume:   total_volume,
            tick_count: self.tick_count,
            exchange_contributions: contributions,
        })
    }
}

// ── Aggregator ────────────────────────────────────────────────────────────────

/// Stateful time-bucket aggregator.
///
/// Feed clean ticks one at a time via [`Aggregator::ingest`]; completed
/// candles are returned as `Some(Candle)` when a bucket boundary is crossed.
pub struct Aggregator {
    cfg:    AggregatorConfig,
    bucket: Option<BucketState>,
}

impl Aggregator {
    pub fn new(cfg: AggregatorConfig) -> Self {
        Self { cfg, bucket: None }
    }

    /// Ingest one clean tick.
    ///
    /// Returns `Some(Candle)` if ingesting this tick caused the *previous*
    /// bucket to be finalized.  The tick itself is placed into the new bucket.
    /// Returns `None` when the tick is simply added to the current bucket.
    pub fn ingest(&mut self, tick: &ExchangeTick) -> Option<Candle> {
        let bucket_idx = tick.received_ms / self.cfg.bucket_ms;
        let open_ms    = bucket_idx * self.cfg.bucket_ms;

        match &self.bucket {
            Some(b) if b.open_ms == open_ms => {
                // Same bucket — just accumulate.
                let bucket = self.bucket.as_mut().unwrap();
                bucket.ingest(tick, self.cfg.use_trust_weight);
                None
            }
            _ => {
                // New bucket boundary — finalize old bucket, start fresh.
                let completed = self.bucket.take().and_then(|b| {
                    if b.tick_count >= self.cfg.min_ticks {
                        b.into_candle(self.cfg.bucket_ms)
                    } else {
                        None
                    }
                });

                let mut new_bucket = BucketState::new(open_ms);
                new_bucket.ingest(tick, self.cfg.use_trust_weight);
                self.bucket = Some(new_bucket);

                completed
            }
        }
    }

    /// Force-finalize the current bucket regardless of timing.
    ///
    /// Useful for graceful shutdown or flushing before a window reset.
    pub fn flush(&mut self) -> Option<Candle> {
        self.bucket.take().and_then(|b| {
            if b.tick_count >= self.cfg.min_ticks {
                b.into_candle(self.cfg.bucket_ms)
            } else {
                None
            }
        })
    }

    /// The open time of the currently accumulating bucket, if any.
    pub fn current_bucket_open_ms(&self) -> Option<u64> {
        self.bucket.as_ref().map(|b| b.open_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use crate::prediction::btc_aggregator::tick::Level;

    fn make_tick(exchange: Exchange, ms: u64, price: f64) -> ExchangeTick {
        use rust_decimal::prelude::FromPrimitive;
        let p = Decimal::from_f64(price).unwrap();
        let a = Decimal::from_f64(1.0).unwrap();
        let level = Level { price: p, amount: a };
        ExchangeTick {
            exchange,
            received_ms: ms,
            bids: vec![Level { price: p - Decimal::from_f64(0.5).unwrap(), amount: a }],
            asks: vec![level],
        }
    }

    #[test]
    fn emits_candle_on_bucket_boundary() {
        let mut agg = Aggregator::new(AggregatorConfig {
            bucket_ms: 250,
            ..Default::default()
        });

        // Two ticks in bucket 0 (ms 0–249).
        assert!(agg.ingest(&make_tick(Exchange::Binance,  100, 105_000.0)).is_none());
        assert!(agg.ingest(&make_tick(Exchange::Coinbase, 200, 105_010.0)).is_none());

        // Tick at ms 300 crosses the boundary → candle for bucket 0 emitted.
        let candle = agg.ingest(&make_tick(Exchange::Kraken, 300, 105_005.0));
        assert!(candle.is_some());
        let c = candle.unwrap();
        assert_eq!(c.open_ms,  0);
        assert_eq!(c.close_ms, 250);
        assert_eq!(c.tick_count, 2);
    }
}
