use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::time;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::prediction::{
    BtcFeatures,
    BtcSample,
    RollingWindow,
};

pub mod proto {
    tonic::include_proto!("orderbook");
}

use proto::orderbook_aggregator_client::OrderbookAggregatorClient;

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
}

impl BtcFeed {
    pub fn new(
        port: u16,
        snapshot: SharedBtcSnapshot,
    ) -> Self {
        Self {
            port,
            snapshot,
            window: Arc::new(RwLock::new(
                RollingWindow::new(Duration::from_secs(300)),
            )),
        }
    }

    pub fn snapshot(&self) -> SharedBtcSnapshot {
        self.snapshot.clone()
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

        Ok(
            OrderbookAggregatorClient::connect(addr)
                .await?,
        )
    }

    async fn run_connection(
        &self,
    ) -> anyhow::Result<()> {
        let mut client = self.connect().await?;

        info!("btc grpc connected");

        let request =
            tonic::Request::new(proto::Empty {});

        let mut stream = client
            .book_summary(request)
            .await?
            .into_inner();

        while let Some(summary) =
            stream.message().await?
        {
            if summary.bids.is_empty()
                || summary.asks.is_empty()
            {
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
                chrono::Utc::now()
                    .timestamp_millis() as u64;

            let sample = BtcSample {
                timestamp_ms,
                price: Decimal::from_f64(price)
                    .unwrap_or_default(),
            };

            self.window
                .write()
                .await
                .push(sample.clone());

            self.snapshot.store(Arc::new(
                BtcSnapshot {
                    timestamp_ms,
                    price: sample.price,
                },
            ));
        }

        warn!("btc stream disconnected");

        Ok(())
    }

    pub async fn features(
        &self,
    ) -> BtcFeatures {
        let window = self.window.read().await;

        let latest = match window.latest() {
            Some(v) => v,
            None => return BtcFeatures::default(),
        };

        let current_price = latest.price;

        let mut high = current_price;
        let mut low = current_price;

        let mut sum = Decimal::ZERO;
        let mut count = 0u64;

        let now_ms = latest.timestamp_ms;

        let mut price_30s: Option<Decimal> = None;
        let mut price_60s: Option<Decimal> = None;

        let mut prices_30s = Vec::new();
        let mut prices_60s = Vec::new();

        for sample in window.iter() {
            if sample.price > high {
                high = sample.price;
            }

            if sample.price < low {
                low = sample.price;
            }

            sum += sample.price;
            count += 1;

            let age_ms =
                now_ms.saturating_sub(
                    sample.timestamp_ms,
                );

            if age_ms <= 30_000 {
                prices_30s.push(
                    sample.price
                        .to_f64()
                        .unwrap_or(0.0),
                );

                price_30s = Some(sample.price);
            }

            if age_ms <= 60_000 {
                prices_60s.push(
                    sample.price
                        .to_f64()
                        .unwrap_or(0.0),
                );

                price_60s = Some(sample.price);
            }
        }

        let current_f =
            current_price.to_f64().unwrap_or(0.0);

        let momentum_30s =
            price_30s
                .and_then(|p| {
                    let base = p.to_f64()?;

                    if base.abs() < f64::EPSILON {
                        Some(0.0)
                    } else {
                        Some(
                            (current_f - base)
                                / base,
                        )
                    }
                })
                .unwrap_or(0.0);

        let momentum_60s =
            price_60s
                .and_then(|p| {
                    let base = p.to_f64()?;

                    if base.abs() < f64::EPSILON {
                        Some(0.0)
                    } else {
                        Some(
                            (current_f - base)
                                / base,
                        )
                    }
                })
                .unwrap_or(0.0);

        fn volatility(
            prices: &[f64],
        ) -> f64 {
            if prices.len() < 2 {
                return 0.0;
            }

            let mean =
                prices.iter().sum::<f64>()
                    / prices.len() as f64;

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

        let volatility_30s =
            volatility(&prices_30s);

        let volatility_60s =
            volatility(&prices_60s);

        let high_f =
            high.to_f64().unwrap_or(0.0);

        let low_f =
            low.to_f64().unwrap_or(0.0);

        let range_position =
            if (high_f - low_f).abs()
                < f64::EPSILON
            {
                0.5
            } else {
                ((current_f - low_f)
                    / (high_f - low_f))
                    .clamp(0.0, 1.0)
            };

        let distance_from_high_pct =
            if high_f <= 0.0 {
                0.0
            } else {
                (high_f - current_f)
                    / high_f
            };

        let distance_from_low_pct =
            if low_f <= 0.0 {
                0.0
            } else {
                (current_f - low_f)
                    / low_f
            };

        let vwap =
            if count > 0 {
                sum / Decimal::from(count)
            } else {
                Decimal::ZERO
            };

        BtcFeatures {
            current_price,
            high_5m: high,
            low_5m: low,
            vwap_5m: vwap,
            momentum_30s,
            momentum_60s,
            volatility_30s,
            volatility_60s,
            range_position,
            distance_from_high_pct,
            distance_from_low_pct,
        }
    }
}