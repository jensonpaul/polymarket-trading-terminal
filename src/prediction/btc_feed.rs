//! BTC price feed — gRPC consumer + 4-layer cleaning pipeline.
//!
//! Replaces the original `btc_feed.rs`.  The core change is that raw
//! exchange ticks now pass through the [`Pipeline`] before being stored
//! in the rolling window or used to update [`WindowState`].
//!
//! ## Architecture
//!
//! ```text
//!   gRPC stream (aggregated order book)
//!         │
//!   build_exchange_tick()   ← converts proto → ExchangeTick per exchange
//!         │
//!   Pipeline::ingest()      ← 4-layer noise filter
//!         │  (fires once per 250 ms bucket)
//!   BtcSample (clean price) → RollingWindow + WindowState
//! ```
//!
//! The gRPC `Summary` message carries bids/asks with an `exchange` field
//! on each `Level`, so levels are split per exchange here and one
//! [`ExchangeTick`] is constructed per exchange group.

use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Mutex;

use arc_swap::ArcSwap;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::time;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use polymarket_client_sdk_v2::gamma::types::response::Market;

use crate::state::{slug_for_ts, stamp_5m};
use crate::prediction::{BtcFeatures, BtcSample, RollingWindow, WindowState};
use crate::prediction::btc_aggregator::{
    Exchange, ExchangeTick, Level,
    CleanPrice, Pipeline, PipelineConfig,
};

pub mod proto {
    tonic::include_proto!("orderbook");
}

use proto::orderbook_aggregator_client::OrderbookAggregatorClient;

pub type MarketCache = Arc<Mutex<HashMap<String, Market>>>;

#[derive(Debug, Clone, Default)]
pub struct BtcSnapshot {
    pub timestamp_ms: u64,
    pub price: Decimal,

    /// Most recent clean price from the pipeline (for display/debugging).
    pub clean: Option<CleanPrice>,
}

pub type SharedBtcSnapshot = Arc<ArcSwap<BtcSnapshot>>;

// ── BtcFeed ───────────────────────────────────────────────────────────────────

pub struct BtcFeed {
    port:         u16,
    window:       Arc<RwLock<RollingWindow<BtcSample>>>,
    snapshot:     SharedBtcSnapshot,
    window_state: Arc<RwLock<WindowState>>,
    pub market_cache: MarketCache,
}

impl BtcFeed {
    pub fn new(
        port: u16,
        snapshot: SharedBtcSnapshot,
        window_state: Arc<RwLock<WindowState>>,
    ) -> Self {
        Self {
            port,
            snapshot,
            window_state,
            market_cache: Arc::new(Mutex::new(HashMap::new())),
            window: Arc::new(RwLock::new(RollingWindow::new(
                Duration::from_secs(300),
            ))),
        }
    }

    pub fn snapshot(&self) -> SharedBtcSnapshot {
        self.snapshot.clone()
    }

    pub async fn notify_window_start(&self, window_started_ms: u64) {
        self.window_state.write().await.reset(window_started_ms);
    }

    pub async fn btc_origin_locked(&self) -> bool {
        self.window_state.read().await.btc_origin_locked
    }

    pub async fn run(&self) {
        loop {
            match self.run_connection().await {
                Ok(_) => {}
                Err(e) => error!("btc feed error: {e}"),
            }
            time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn connect(&self) -> anyhow::Result<OrderbookAggregatorClient<Channel>> {
        let addr = format!("http://[::1]:{}", self.port);
        Ok(OrderbookAggregatorClient::connect(addr).await?)
    }

    async fn run_connection(&self) -> anyhow::Result<()> {
        let mut client = self.connect().await?;
        info!("btc grpc connected");

        let request = tonic::Request::new(proto::Empty {});
        let mut stream = client.book_summary(request).await?.into_inner();

        // One pipeline instance per connection — resets cleanly on reconnect.
        let mut pipeline = Pipeline::new(PipelineConfig::default());

        while let Some(summary) = stream.message().await? {
            if summary.bids.is_empty() || summary.asks.is_empty() {
                continue;
            }

            let received_ms = chrono::Utc::now().timestamp_millis() as u64;

            // Split the Summary's levels by exchange field and build one
            // ExchangeTick per exchange for the pipeline.
            let ticks = build_exchange_ticks(&summary, received_ms);

            for tick in &ticks {
                if let Some(clean) = pipeline.ingest(tick) {
                    self.publish_clean(clean, received_ms).await;
                }
            }
        }

        // Flush the partial bucket before disconnecting.
        if let Some(clean) = pipeline.flush() {
            self.publish_clean(clean, chrono::Utc::now().timestamp_millis() as u64).await;
        }

        warn!("btc stream disconnected");
        Ok(())
    }

    /// Store a clean price into the rolling window and update downstream state.
    async fn publish_clean(&self, clean: CleanPrice, received_ms: u64) {
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

        // ── Dense bins (for volatility, z-score, high/low) ────────────────
        let mut prices_1s:  Vec<f64> = Vec::new();
        let mut prices_5s:  Vec<f64> = Vec::new();
        let mut prices_10s: Vec<f64> = Vec::new();
        let mut prices_30s: Vec<f64> = Vec::new();
        let mut prices_60s: Vec<f64> = Vec::new();
        let mut all_prices: Vec<f64> = Vec::new();

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
        }

        let high_f = high.to_f64().unwrap_or(current_f);
        let low_f  = low.to_f64().unwrap_or(current_f);
        let range_position = if high_f > low_f {
            (current_f - low_f) / (high_f - low_f)
        } else {
            0.5
        };

        let net_displacement = ws.btc_distance_from_origin_pct.abs();
        let efficiency_ratio = if ws.btc_path_length > 0.0 {
            (net_displacement / ws.btc_path_length).min(1.0)
        } else {
            0.0
        };

        // ── Strided ER: one sample per stride_ms, newest-first ────────────
        //
        // For each ER variant we walk the rolling window (oldest → newest)
        // and snap one price per stride bucket.  This means:
        //   er_1s  → samples every  1 000 ms  → up to  5 points over  5 min
        //   er_5s  → samples every  5 000 ms  → up to  5 points over  5 min  (wait — see below)
        //
        // We look back a fixed horizon equal to stride_ms * max_points so
        // that each variant uses the same number of strides regardless of
        // how far back data goes.
        //
        // Sampling logic (newest-first bucketing):
        //   bucket_index = (now_ms - sample.timestamp_ms) / stride_ms
        //   Keep the first (= newest) sample that falls in each bucket.
        //
        // The result is then reversed so prices run oldest → newest before
        // being handed to efficiency_ratio_over (net = last − first).
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
        }
    }
}

// ── Proto → ExchangeTick conversion ──────────────────────────────────────────

/// Build one [`ExchangeTick`] per exchange from a [`proto::Summary`].
///
/// Each [`proto::Level`] carries an `exchange` string field populated by
/// your gRPC aggregator server.  Levels are bucketed by exchange and one
/// [`ExchangeTick`] is emitted per group so the pipeline can apply
/// independent per-exchange spike filtering.
///
/// Levels whose `exchange` string is empty or unrecognised are attributed
/// to [`Exchange::Binance`] (highest-liquidity fallback) — this is logged
/// at WARN level in `parse_exchange` so you can catch unexpected values.
fn build_exchange_ticks(
    summary: &proto::Summary,
    received_ms: u64,
) -> Vec<ExchangeTick> {
    // Bucket bids and asks by exchange.
    let mut bid_map: HashMap<Exchange, Vec<Level>> = HashMap::new();
    let mut ask_map: HashMap<Exchange, Vec<Level>> = HashMap::new();

    for bid in &summary.bids {
        let ex = parse_exchange(&bid.exchange);
        bid_map.entry(ex).or_default().push(proto_level_to_level(bid));
    }
    for ask in &summary.asks {
        let ex = parse_exchange(&ask.exchange);
        ask_map.entry(ex).or_default().push(proto_level_to_level(ask));
    }

    // Merge the two maps into one tick per exchange.
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

fn parse_exchange(s: &str) -> Exchange {
    match s.to_lowercase().as_str() {
        "binance"  => Exchange::Binance,
        "coinbase" => Exchange::Coinbase,
        "kraken"   => Exchange::Kraken,
        "bitstamp" => Exchange::Bitstamp,
        "" => Exchange::Binance, // field absent — silent fallback
        other => {
            warn!(exchange = other, "unrecognised exchange string — attributed to Binance");
            Exchange::Binance
        }
    }
}

fn proto_level_to_level(lvl: &proto::Level) -> Level {
    use rust_decimal::prelude::FromPrimitive;
    Level {
        price:  Decimal::from_f64(lvl.price).unwrap_or_default(),
        amount: Decimal::from_f64(lvl.amount).unwrap_or_default(),
    }
}

// ── Statistics helpers (unchanged from original) ──────────────────────────────

fn volatility(prices: &[f64]) -> f64 {
    if prices.len() < 2 { return 0.0; }
    let mean = prices.iter().sum::<f64>() / prices.len() as f64;
    let var   = prices.iter().map(|v| { let d = v - mean; d * d }).sum::<f64>()
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

/// Compute the Efficiency Ratio using **strided** price sampling.
///
/// Instead of taking every available 250 ms candle (which inflates path
/// length with micro-noise), we snap **one price per `stride_ms` bucket**
/// and feed only those sparse samples into the ER formula.
///
/// # Parameters
/// - `iter`      – the rolling window iterator (oldest → newest `BtcSample`s)
/// - `now_ms`    – timestamp of the most recent sample
/// - `stride_ms` – bucket width in milliseconds (e.g. 1 000 for er_1s)
/// - `max_buckets` – how many buckets to look back; total look-back =
///                  `stride_ms * max_buckets`
///
/// # Bucket assignment (newest-first)
/// For each sample we compute:
/// ```text
/// bucket = (now_ms - sample.timestamp_ms) / stride_ms
/// ```
/// We keep the **newest** sample per bucket (i.e. the one with the
/// smallest `age_ms` within that bucket).  Because `iter` goes oldest →
/// newest, we just overwrite on each visit — the last write per bucket is
/// the newest sample in it.
///
/// After collecting, the bucket map is sorted by bucket index (ascending =
/// oldest first) and handed to `efficiency_ratio_over`.
fn strided_er<'a>(
    iter:        impl Iterator<Item = &'a BtcSample>,
    now_ms:      u64,
    stride_ms:   u64,
    max_buckets: u64,
) -> f64 {
    let horizon_ms = stride_ms * max_buckets;
 
    // bucket_index → price (newest sample in that bucket wins)
    let mut buckets: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();
 
    for sample in iter {
        let age_ms = now_ms.saturating_sub(sample.timestamp_ms);
        if age_ms > horizon_ms {
            continue;
        }
        let bucket = age_ms / stride_ms;
        let price_f = match sample.price.to_f64() {
            Some(v) => v,
            None => continue,
        };
        // Overwrite: since iter goes oldest → newest the last write is the
        // newest sample in each bucket, which is what we want.
        buckets.insert(bucket, price_f);
    }
 
    if buckets.len() < 2 {
        return 0.0;
    }
 
    // BTreeMap is sorted ascending by key (= ascending age = oldest first
    // when we reverse).  We want prices in chronological order (oldest →
    // newest), which is descending bucket index.
    let prices: Vec<f64> = buckets.into_values().rev().collect();
    efficiency_ratio_over(&prices)
}

fn efficiency_ratio_over(prices: &[f64]) -> f64 {
    if prices.len() < 2 { return 0.0; }
    let net  = (prices.last().unwrap() - prices.first().unwrap()).abs();
    let path: f64 = prices.windows(2).map(|w| (w[1] - w[0]).abs()).sum();
    if path > 0.0 { (net / path).min(1.0) } else { 0.0 }
}