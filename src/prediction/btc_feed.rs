use std::sync::Arc;
use std::time::Duration;

use std::sync::Mutex;
use std::collections::HashMap;

use arc_swap::ArcSwap;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::time;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use polymarket_client_sdk_v2::gamma::types::response::Market;

use crate::state::{slug_for_ts, stamp_5m};

use crate::prediction::{
    BtcFeatures,
    BtcSample,
    RollingWindow,
    WindowState,
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
}

pub type SharedBtcSnapshot = Arc<ArcSwap<BtcSnapshot>>;

pub struct BtcFeed {
    port: u16,
    window: Arc<RwLock<RollingWindow<BtcSample>>>,
    snapshot: SharedBtcSnapshot,
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
            window: Arc::new(RwLock::new(
                RollingWindow::new(Duration::from_secs(300)),
            )),
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

    async fn connect(
        &self,
    ) -> anyhow::Result<OrderbookAggregatorClient<Channel>> {
        let addr = format!("http://[::1]:{}", self.port);
        Ok(OrderbookAggregatorClient::connect(addr).await?)
    }

    async fn run_connection(&self) -> anyhow::Result<()> {
        let mut client = self.connect().await?;

        info!("btc grpc connected");

        let request = tonic::Request::new(proto::Empty {});

        let mut stream = client
            .book_summary(request)
            .await?
            .into_inner();

        while let Some(summary) = stream.message().await? {
            if summary.bids.is_empty() || summary.asks.is_empty() {
                continue;
            }

            let mut bid_sum = 0.0;
            let mut bid_size = 0.0;

            for bid in &summary.bids {
                bid_sum += bid.price * bid.amount;
                bid_size += bid.amount;
            }

            let mut ask_sum = 0.0;
            let mut ask_size = 0.0;

            for ask in &summary.asks {
                ask_sum += ask.price * ask.amount;
                ask_size += ask.amount;
            }

            if bid_size <= 0.0 || ask_size <= 0.0 {
                continue;
            }

            let bid_vwap = bid_sum / bid_size;
            let ask_vwap = ask_sum / ask_size;
            let price = (bid_vwap + ask_vwap) / 2.0;

            let timestamp_ms =
                chrono::Utc::now().timestamp_millis() as u64;

            let decimal_price =
                Decimal::from_f64(price).unwrap_or_default();

            let sample = BtcSample {
                timestamp_ms,
                price: decimal_price,
            };

            self.window.write().await.push(sample.clone());

            self.window_state
                .write()
                .await
                .ingest_btc(decimal_price, timestamp_ms);

            self.snapshot.store(Arc::new(BtcSnapshot {
                timestamp_ms,
                price: decimal_price,
            }));
        }

        warn!("btc stream disconnected");

        Ok(())
    }

    pub async fn features(&self) -> BtcFeatures {
        let window = self.window.read().await;
        let ws = self.window_state.read().await;

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
        let mut low = current_price;
        let now_ms = latest.timestamp_ms;

        let mut prices_30s: Vec<f64> = Vec::new();
        let mut prices_60s: Vec<f64> = Vec::new();
        let mut all_prices: Vec<f64> = Vec::new();

        for sample in window.iter() {
            if sample.price > high {
                high = sample.price;
            }
            if sample.price < low {
                low = sample.price;
            }

            let age_ms = now_ms.saturating_sub(sample.timestamp_ms);
            let price_f = match sample.price.to_f64() {
                Some(v) => v,
                None => continue,
            };

            all_prices.push(price_f);

            if age_ms <= 30_000 {
                prices_30s.push(price_f);
            }
            if age_ms <= 60_000 {
                prices_60s.push(price_f);
            }
        }

        let high_f = high.to_f64().unwrap_or(current_f);
        let low_f = low.to_f64().unwrap_or(current_f);
        let range_position = if high_f > low_f {
            (current_f - low_f) / (high_f - low_f)
        } else {
            0.5
        };

        // Efficiency Ratio: |net displacement| / cumulative path length.
        let net_displacement = ws.btc_distance_from_origin_pct.abs();
        let efficiency_ratio = if ws.btc_path_length > 0.0 {
            (net_displacement / ws.btc_path_length).min(1.0)
        } else {
            0.0
        };

        // Z-score of current price relative to the 5-minute rolling window.
        let z_score = z_score_of(current_f, &all_prices);

        // Momentum persistence: fraction of elapsed time on current side.
        let momentum_persistence = if ws.btc_elapsed_seconds > 0.0 {
            ws.btc_same_side_seconds / ws.btc_elapsed_seconds
        } else {
            0.5
        };

        // Average signed distance from origin over elapsed time.
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
            volatility_30s: volatility(&prices_30s),
            volatility_60s: volatility(&prices_60s),
            z_score,
            acceleration: ws.btc_acceleration,
            momentum_persistence,
            avg_distance_from_origin,
            high_5m: high,
            low_5m: low,
            range_position,
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Rolling coefficient of variation (std / mean) as a volatility proxy.
fn volatility(prices: &[f64]) -> f64 {
    if prices.len() < 2 {
        return 0.0;
    }

    let mean = prices.iter().sum::<f64>() / prices.len() as f64;
    let variance = prices
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / prices.len() as f64;

    variance.sqrt() / mean.max(1.0)
}

/// Z-score of `value` relative to the distribution of `samples`.
fn z_score_of(value: f64, samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let n = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n;
    let variance = samples
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / n;

    let std = variance.sqrt();
    if std < 1e-12 {
        return 0.0;
    }

    (value - mean) / std
}
