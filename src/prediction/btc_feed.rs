//! BTC price feed — orderly library consumer + dual downstream pipelines.
//!
//! ## Architecture
//!
//! ```text
//!   orderly::OrderlyEngine (watch::Receiver<OutTick>)
//!         │
//!   out_tick_to_exchange_ticks()   ← converts OutTick → ExchangeTick per exchange
//!         │
//!         ├──────────────────────────────────┬─────────────────────────────────┐
//!         │                                   │                                 │
//!   Pipeline::ingest()                ReversionEngine::on_raw_tick()    (future: order-flow / depth consumers)
//!   ← 4-layer noise filter            ← consumes RAW, noisy ticks
//!   (fires once per 250 ms bucket)    (fires on every tick — no smoothing)
//!         │                                   │
//!   BtcSample (clean price)           ReversionOutput (stretch, multi-level
//!   → RollingWindow + WindowState       targets, half-life, regime, etc.)
//! ```
//!
//! ## Why the reversion engine sees raw ticks
//!
//! The 4-layer cleaning pipeline (spike filter → outlier gate → aggregator →
//! Kalman smoother) exists to produce a *smooth* trend signal for the ONNX
//! direction model. Mean-reversion detection needs the opposite: the
//! micro-structure noise, volatility bursts, and order-flow imbalance that
//! the cleaning pipeline is explicitly designed to remove. Per the noisy-tick
//! requirement, [`ReversionEngine`] is fed directly from
//! `out_tick_to_exchange_ticks()`, in parallel with (not downstream of) the
//! existing cleaning `Pipeline`. Both consume the same raw tick stream
//! independently; neither blocks or alters the other.
//!
//! `orderly::OutTick` carries bids/asks with an `exchange` field on each
//! `Level`, so levels are split per exchange here and one [`ExchangeTick`]
//! is constructed per exchange group — exactly as before, without gRPC.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use orderly::{Exchange as OrdExchange, Level as OrdLevel, OrderlyEngine, OutTick};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::time;
use tracing::{error, info, warn};

use crate::prediction::btc_aggregator::{Exchange, ExchangeTick, Level, Pipeline, PipelineConfig};
use crate::prediction::reversion::{ReversionConfig, ReversionEngine, ReversionOutput};
use crate::prediction::{BtcFeatures, BtcSample, RollingWindow, WindowState};

// ── BtcSnapshot ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct BtcSnapshot {
    pub timestamp_ms: u64,
    pub price: Decimal,

    /// Most recent clean price from the cleaning pipeline (for display/debugging).
    pub clean: Option<crate::prediction::btc_aggregator::CleanPrice>,
}

pub type SharedBtcSnapshot = Arc<ArcSwap<BtcSnapshot>>;

/// Most recent reversion-engine output, published independently of the
/// clean-price snapshot above. `None` until the reversion engine has
/// completed its warm-up period.
pub type SharedReversionOutput = Arc<ArcSwap<Option<ReversionOutput>>>;

// ── BtcFeed ───────────────────────────────────────────────────────────────────

pub struct BtcFeed {
    symbol:           String,
    window:           Arc<RwLock<RollingWindow<BtcSample>>>,
    snapshot:         SharedBtcSnapshot,
    window_state:     Arc<RwLock<WindowState>>,
    reversion_output: SharedReversionOutput,
}

impl BtcFeed {
    pub fn new(
        symbol: impl Into<String>,
        snapshot: SharedBtcSnapshot,
        window_state: Arc<RwLock<WindowState>>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            snapshot,
            window_state,
            window: Arc::new(RwLock::new(RollingWindow::new(
                Duration::from_secs(60 * 60),
            ))),
            reversion_output: Arc::new(ArcSwap::from_pointee(None)),
        }
    }

    pub fn snapshot(&self) -> SharedBtcSnapshot {
        self.snapshot.clone()
    }

    /// Shared handle to the latest reversion-engine output.
    ///
    /// Cloned cheaply (it's an `Arc<ArcSwap<..>>`) and read with `.load()`.
    /// Used by [`PredictionService`] to populate [`PredictionContext`].
    pub fn reversion_output(&self) -> SharedReversionOutput {
        self.reversion_output.clone()
    }

    pub async fn notify_window_start(&self, window_started_ms: u64) {
        self.window_state.write().await.reset(window_started_ms);
    }

    pub async fn btc_origin_locked(&self) -> bool {
        self.window_state.read().await.btc_origin_locked
    }

    pub async fn run(&self) {
        loop {
            match self.run_engine().await {
                Ok(_)  => {}
                Err(e) => error!("btc feed error: {e}"),
            }
            time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Start the orderly engine for our symbol and consume ticks until the
    /// watch channel closes or the engine errors.  Fresh pipelines (both
    /// the cleaning pipeline and the reversion engine) are created on every
    /// call so state resets cleanly on restart.
    async fn run_engine(&self) -> anyhow::Result<()> {
        let engine = OrderlyEngine::new(self.symbol.clone());
        let (handle, mut rx) = engine.start().await?;
        info!("btc orderly engine started for {}", self.symbol);

        let mut pipeline = Pipeline::new(PipelineConfig::default());
        let mut reversion = ReversionEngine::new(ReversionConfig::default())
            .expect("default ReversionConfig must validate");

        let mut staleness_check = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                // ── Primary path: new merged book tick ──────────────────────
                changed = rx.changed() => {
                    if changed.is_err() {
                        // All senders dropped — engine has shut down.
                        break;
                    }

                    let tick: OutTick = rx.borrow().clone();

                    if tick.bids.is_empty() || tick.asks.is_empty() {
                        continue;
                    }

                    let received_ms = chrono::Utc::now().timestamp_millis() as u64;
                    let exchange_ticks = out_tick_to_exchange_ticks(&tick, received_ms);

                    for ex_tick in &exchange_ticks {
                        // ── Reversion engine: RAW, noisy tick — no filtering ──
                        //
                        // Runs independently of (in parallel with) the cleaning
                        // pipeline below. Every tick is fed; the reversion
                        // engine does its own internal smoothing (EWMA blend
                        // equilibrium) rather than relying on the spike/outlier
                        // gates designed for the trend model.
                        if let Some(output) = reversion.on_raw_tick(ex_tick, received_ms) {
                            self.reversion_output.store(Arc::new(Some(output)));
                        }

                        // ── Cleaning pipeline: filtered, bucketed, smoothed ──
                        if let Some(clean) = pipeline.ingest(ex_tick) {
                            self.publish_clean(clean, received_ms).await;
                        }
                    }
                }

                // ── Secondary path: staleness watchdog ──────────────────────
                //
                // The reversion engine needs to detect feed silence even when
                // no new ticks are arriving (the primary select branch above
                // only fires on tick arrival). This interval lets it notice
                // a stalled feed and flip into `Stale` lifecycle, after which
                // `current()` callers see a clearly degraded confidence via
                // `data_quality` rather than a silently frozen output.
                _ = staleness_check.tick() => {
                    if reversion.check_staleness() {
                        warn!("reversion engine: feed stale (no ticks within threshold)");
                    }
                }
            }
        }

        // Flush the partial bucket before restarting.
        if let Some(clean) = pipeline.flush() {
            self.publish_clean(clean, chrono::Utc::now().timestamp_millis() as u64)
                .await;
        }

        handle.shutdown().await;
        warn!("btc orderly engine disconnected");
        Ok(())
    }

    /// Store a clean price into the rolling window and update downstream state.
    async fn publish_clean(
        &self,
        clean: crate::prediction::btc_aggregator::CleanPrice,
        received_ms: u64,
    ) {
        let decimal_price = match Decimal::from_f64(clean.smoothed) {
            Some(d) => d,
            None => return,
        };

        let sample = BtcSample {
            timestamp_ms: received_ms,
            price: decimal_price,
        };

        self.window.write().await.push(sample);
        self.window_state
            .write()
            .await
            .ingest_btc(decimal_price, received_ms);

        self.snapshot.store(Arc::new(BtcSnapshot {
            timestamp_ms: received_ms,
            price: decimal_price,
            clean: Some(clean),
        }));
    }

    pub async fn features(&self) -> BtcFeatures {
        let window = self.window.read().await;
        let ws     = self.window_state.read().await;

        let latest = match window.latest() {
            Some(v) => v,
            None => return BtcFeatures::default(),
        };

        let current_price = latest.price;
        let current_f = match current_price.to_f64() {
            Some(v) => v,
            None => return BtcFeatures::default(),
        };

        let mut high = current_price;
        let mut low  = current_price;
        let now_ms   = latest.timestamp_ms;

        let mut prices_1s:  Vec<f64> = Vec::new();
        let mut prices_5s:  Vec<f64> = Vec::new();
        let mut prices_10s: Vec<f64> = Vec::new();
        let mut prices_30s: Vec<f64> = Vec::new();
        let mut prices_60s: Vec<f64> = Vec::new();
        let mut all_prices: Vec<f64> = Vec::new();

        let mut prices_5m:  Vec<f64> = Vec::new();
        let mut prices_10m: Vec<f64> = Vec::new();
        let mut prices_30m: Vec<f64> = Vec::new();
        let mut prices_60m: Vec<f64> = Vec::new();

        for sample in window.iter() {
            if sample.price > high { high = sample.price; }
            if sample.price < low  { low  = sample.price; }

            let age_ms  = now_ms.saturating_sub(sample.timestamp_ms);
            let price_f = match sample.price.to_f64() {
                Some(v) => v,
                None => continue,
            };

            all_prices.push(price_f);
            if age_ms <=  1_000 { prices_1s.push(price_f); }
            if age_ms <=  5_000 { prices_5s.push(price_f); }
            if age_ms <= 10_000 { prices_10s.push(price_f); }
            if age_ms <= 30_000 { prices_30s.push(price_f); }
            if age_ms <= 60_000 { prices_60s.push(price_f); }

            if age_ms <=    300_000 { prices_5m.push(price_f); }
            if age_ms <=    600_000 { prices_10m.push(price_f); }
            if age_ms <=  1_800_000 { prices_30m.push(price_f); }
            if age_ms <=  3_600_000 { prices_60m.push(price_f); }
        }

        let high_f = high.to_f64().unwrap_or(current_f);
        let low_f  = low.to_f64().unwrap_or(current_f);
        let range_position = if high_f > low_f {
            (current_f - low_f) / (high_f - low_f)
        } else {
            0.5
        };

        let range_position_30s  = range_position_over(current_f, &prices_30s);
        let range_position_60s  = range_position_over(current_f, &prices_60s);
        let range_position_5m   = range_position_over(current_f, &prices_5m);
        let range_position_10m  = range_position_over(current_f, &prices_10m);
        let range_position_30m  = range_position_over(current_f, &prices_30m);
        let range_position_60m  = range_position_over(current_f, &prices_60m);

        let net_displacement = ws.btc_distance_from_origin_pct.abs();
        let efficiency_ratio = if ws.btc_path_length > 0.0 {
            (net_displacement / ws.btc_path_length).min(1.0)
        } else {
            0.0
        };

        let er_1s   = strided_er(window.iter(), now_ms,  1_000,  60);
        let er_5s   = strided_er(window.iter(), now_ms,  5_000,  60);
        let er_10s  = strided_er(window.iter(), now_ms, 10_000,  30);
        let er_30s  = strided_er(window.iter(), now_ms, 30_000,  10);
        let er_full = strided_er(window.iter(), now_ms, 60_000,   5);

        let z_score_30 = z_score_of(current_f, &prices_30s);
        let z_score_60 = z_score_of(current_f, &prices_60s);
        let z_score_5m = z_score_of(current_f, &all_prices);

        let momentum_persistence = if ws.btc_elapsed_seconds > 0.0 {
            ws.btc_same_side_seconds / ws.btc_elapsed_seconds
        } else {
            0.5
        };

        let avg_distance_from_origin = if ws.btc_elapsed_seconds > 0.0 {
            ws.btc_area / ws.btc_elapsed_seconds
        } else {
            0.0
        };

        BtcFeatures {
            current_price,
            origin_price: ws.btc_origin_price,
            distance_from_origin_pct: ws.btc_distance_from_origin_pct,
            efficiency_ratio,
            er_1s,
            er_5s,
            er_10s,
            er_30s,
            er_full,
            volatility_30s: volatility(&prices_30s),
            volatility_60s: volatility(&prices_60s),
            z_score_30,
            z_score_60,
            z_score_5m,
            acceleration: ws.btc_acceleration,
            momentum_persistence,
            avg_distance_from_origin,
            high_5m: high,
            low_5m:  low,
            range_position,
            range_position_30s,
            range_position_60s,
            range_position_5m,
            range_position_10m,
            range_position_30m,
            range_position_60m,
        }
    }
}

// ── OutTick → ExchangeTick conversion ─────────────────────────────────────────

/// Split an [`OutTick`] into one [`ExchangeTick`] per exchange.
///
/// Each `orderly::Level` carries an `exchange` field.  Levels are bucketed
/// by exchange and one [`ExchangeTick`] is emitted per group so both
/// downstream pipelines (cleaning + reversion) can apply independent
/// per-exchange logic — exactly as the previous gRPC path did.
fn out_tick_to_exchange_ticks(tick: &OutTick, received_ms: u64) -> Vec<ExchangeTick> {
    let mut bid_map: HashMap<Exchange, Vec<Level>> = HashMap::new();
    let mut ask_map: HashMap<Exchange, Vec<Level>> = HashMap::new();

    for level in &tick.bids {
        bid_map
            .entry(map_exchange(&level.exchange))
            .or_default()
            .push(map_level(level));
    }
    for level in &tick.asks {
        ask_map
            .entry(map_exchange(&level.exchange))
            .or_default()
            .push(map_level(level));
    }

    let all_exchanges: std::collections::HashSet<Exchange> = bid_map
        .keys()
        .chain(ask_map.keys())
        .copied()
        .collect();

    all_exchanges
        .into_iter()
        .filter_map(|ex| {
            let bids = bid_map.remove(&ex).unwrap_or_default();
            let asks = ask_map.remove(&ex).unwrap_or_default();
            if bids.is_empty() && asks.is_empty() {
                return None;
            }
            Some(ExchangeTick { exchange: ex, received_ms, bids, asks })
        })
        .collect()
}

/// Map `orderly::Exchange` → local pipeline `Exchange`.
#[inline]
fn map_exchange(ex: &OrdExchange) -> Exchange {
    match ex {
        OrdExchange::Binance  => Exchange::Binance,
        OrdExchange::Coinbase => Exchange::Coinbase,
        OrdExchange::Kraken   => Exchange::Kraken,
        OrdExchange::Bitstamp => Exchange::Bitstamp,
    }
}

/// Map `orderly::Level` → local pipeline `Level` (price + amount only).
#[inline]
fn map_level(l: &OrdLevel) -> Level {
    Level { price: l.price, amount: l.amount }
}

// ── Statistics helpers (unchanged) ────────────────────────────────────────────

fn volatility(prices: &[f64]) -> f64 {
    if prices.len() < 2 { return 0.0; }
    let mean = prices.iter().sum::<f64>() / prices.len() as f64;
    let var  = prices.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>()
        / prices.len() as f64;
    var.sqrt() / mean.max(1.0)
}

fn z_score_of(value: f64, samples: &[f64]) -> f64 {
    if samples.len() < 2 { return 0.0; }
    let n    = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n;
    let var  = samples.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>() / n;
    let std  = var.sqrt();
    if std < 1e-12 { return 0.0; }
    (value - mean) / std
}

fn strided_er<'a>(
    iter:        impl Iterator<Item = &'a BtcSample>,
    now_ms:      u64,
    stride_ms:   u64,
    max_buckets: u64,
) -> f64 {
    let horizon_ms = stride_ms * max_buckets;
    let mut buckets: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();

    for sample in iter {
        let age_ms = now_ms.saturating_sub(sample.timestamp_ms);
        if age_ms > horizon_ms { continue; }
        let bucket  = age_ms / stride_ms;
        let price_f = match sample.price.to_f64() {
            Some(v) => v,
            None => continue,
        };
        buckets.insert(bucket, price_f);
    }

    if buckets.len() < 2 { return 0.0; }
    let prices: Vec<f64> = buckets.into_values().rev().collect();
    efficiency_ratio_over(&prices)
}

fn efficiency_ratio_over(prices: &[f64]) -> f64 {
    if prices.len() < 2 { return 0.0; }
    let net  = (prices.last().unwrap() - prices.first().unwrap()).abs();
    let path: f64 = prices.windows(2).map(|w| (w[1] - w[0]).abs()).sum();
    if path > 0.0 { (net / path).min(1.0) } else { 0.0 }
}

fn range_position_over(current: f64, prices: &[f64]) -> f64 {
    if prices.is_empty() { return 0.5; }
    let mut high = f64::MIN;
    let mut low  = f64::MAX;
    for p in prices {
        high = high.max(*p);
        low  = low.min(*p);
    }
    if high > low { (current - low) / (high - low) } else { 0.5 }
}