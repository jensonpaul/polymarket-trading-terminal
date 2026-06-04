use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::info;

use polymarket_client_sdk_v2::{
    clob::ws::Client,
    types::U256,
};

use crate::prediction::{
    OrderbookSample,
    RollingWindow,
    TokenFeatures,
};

#[derive(Debug, Clone)]
pub struct MarketAssets {
    pub up_asset_id: U256,
    pub down_asset_id: U256,
}

struct TokenWindow {
    expires_at_sec: u64,
    window: RollingWindow<OrderbookSample>,
}

#[derive(Debug, Clone)]
struct LastTradePrice {
    price: Decimal,
    timestamp_ms: u64,
}

#[derive(Debug, Clone)]
struct OrderbookState {
    best_bid: Decimal,
    best_ask: Decimal,
    bid_depth: Decimal,
    ask_depth: Decimal,
    timestamp_ms: u64,
}

pub struct PolymarketFeed {
    client: Client,

    windows: Arc<
        RwLock<
            HashMap<
                String,
                TokenWindow,
            >,
        >,
    >,

    latest_orderbooks: Arc<
        RwLock<
            HashMap<
                String,
                OrderbookState,
            >,
        >,
    >,

    latest_last_trades: Arc<
        RwLock<
            HashMap<
                String,
                LastTradePrice,
            >,
        >,
    >,
}

impl PolymarketFeed {
    pub fn new() -> Self {
        Self {
            client: Client::default(),

            windows: Arc::new(
                RwLock::new(HashMap::new()),
            ),

            latest_orderbooks: Arc::new(
                RwLock::new(HashMap::new()),
            ),

            latest_last_trades: Arc::new(
                RwLock::new(HashMap::new()),
            ),
        }
    }

    pub async fn run_market(
        &self,
        assets: MarketAssets,
        window_ts: u64,
    ) -> anyhow::Result<()> {
        let expires_at_sec =
            window_ts + 300;

        let asset_ids = vec![
            assets.up_asset_id,
            assets.down_asset_id,
        ];

        let orderbook_stream = self
            .client
            .subscribe_orderbook(
                asset_ids.clone(),
            )?;

        let trade_stream = self
            .client
            .subscribe_last_trade_price(
                asset_ids,
            )?;

        let mut orderbook_stream =
            Box::pin(orderbook_stream);

        let mut trade_stream =
            Box::pin(trade_stream);

        info!(
            %window_ts,
            %expires_at_sec,
            "market websocket started"
        );

        loop {
            let now_sec =
                chrono::Utc::now()
                    .timestamp() as u64;

            if now_sec
                >= expires_at_sec
            {
                info!(
                    %window_ts,
                    "market expired"
                );

                self.evict_market(
                    &assets,
                )
                .await;

                break;
            }

            tokio::select! {
                msg = orderbook_stream.next() => {
                    let Some(msg) = msg else {
                        break;
                    };

                    let book = msg?;

                    let best_bid = book
                        .bids
                        .first()
                        .map(|v| v.price)
                        .unwrap_or_default();

                    let best_ask = book
                        .asks
                        .first()
                        .map(|v| v.price)
                        .unwrap_or_default();

                    let bid_depth: Decimal =
                        book.bids
                            .iter()
                            .map(|v| v.size)
                            .sum();

                    let ask_depth: Decimal =
                        book.asks
                            .iter()
                            .map(|v| v.size)
                            .sum();

                    let timestamp_ms =
                        chrono::Utc::now()
                            .timestamp_millis()
                            as u64;

                    self.latest_orderbooks
                        .write()
                        .await
                        .insert(
                            book.asset_id
                                .to_string(),
                            OrderbookState {
                                best_bid,
                                best_ask,
                                bid_depth,
                                ask_depth,
                                timestamp_ms,
                            },
                        );

                    self.try_publish_sample(
                        book.asset_id
                            .to_string(),
                        expires_at_sec,
                    )
                    .await;
                }

                msg = trade_stream.next() => {
                    let Some(msg) = msg else {
                        break;
                    };

                    let trade = msg?;

                    let price: Decimal = trade.price;

                    let timestamp_ms =
                        trade.timestamp as u64;

                    self.latest_last_trades
                        .write()
                        .await
                        .insert(
                            trade.asset_id.to_string(),
                            LastTradePrice {
                                price,
                                timestamp_ms,
                            },
                        );

                    self.try_publish_sample(
                        trade.asset_id.to_string(),
                        expires_at_sec,
                    )
                    .await;
                }
            }
        }

        info!(
            %window_ts,
            "market websocket closed"
        );

        Ok(())
    }

    async fn try_publish_sample(
        &self,
        asset_id: String,
        expires_at_sec: u64,
    ) {
        let orderbook = {
            self.latest_orderbooks
                .read()
                .await
                .get(&asset_id)
                .cloned()
        };

        let trade = {
            self.latest_last_trades
                .read()
                .await
                .get(&asset_id)
                .cloned()
        };

        let (
            Some(orderbook),
            Some(trade),
        ) = (orderbook, trade)
        else {
            return;
        };

        if trade.timestamp_ms
            < orderbook.timestamp_ms
        {
            return;
        }

        let sample =
            OrderbookSample {
                timestamp_ms:
                    trade.timestamp_ms,

                asset_id,

                best_bid:
                    orderbook.best_bid,

                best_ask:
                    orderbook.best_ask,

                last_trade_price:
                    trade.price,

                bid_depth:
                    orderbook.bid_depth,

                ask_depth:
                    orderbook.ask_depth,
            };

        self.push(
            sample,
            expires_at_sec,
        )
        .await;
    }

    async fn push(
        &self,
        sample: OrderbookSample,
        expires_at_sec: u64,
    ) {
        let now_sec =
            chrono::Utc::now()
                .timestamp() as u64;

        let mut windows =
            self.windows.write().await;

        if let Some(existing) =
            windows.get(
                &sample.asset_id,
            )
        {
            if now_sec
                >= existing
                    .expires_at_sec
            {
                windows.remove(
                    &sample.asset_id,
                );

                return;
            }
        }

        let token_window =
            windows
                .entry(
                    sample.asset_id.clone(),
                )
                .or_insert_with(|| {
                    TokenWindow {
                        expires_at_sec,
                        window:
                            RollingWindow::new(
                                Duration::from_secs(
                                    300,
                                ),
                            ),
                    }
                });

        token_window
            .window
            .push(sample);
    }

    pub async fn token_features(
        &self,
        asset_id: &str,
    ) -> Option<TokenFeatures> {
        let now_sec =
            chrono::Utc::now()
                .timestamp() as u64;

        {
            let windows =
                self.windows.read().await;

            let token_window =
                windows.get(asset_id)?;

            if now_sec
                >= token_window.expires_at_sec
            {
                drop(windows);

                self.windows
                    .write()
                    .await
                    .remove(asset_id);

                return None;
            }
        }

        let windows =
            self.windows.read().await;

        let token_window =
            windows.get(asset_id)?;

        let latest =
            token_window.window.latest()?;

        let spread =
            latest.best_ask - latest.best_bid;

        let spread_pct =
            if latest
                .last_trade_price
                .is_zero()
            {
                0.0
            } else {
                (spread
                    / latest
                        .last_trade_price)
                    .to_string()
                    .parse()
                    .unwrap_or(0.0)
            };

        let bid_depth_f =
            latest.bid_depth
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0);

        let ask_depth_f =
            latest.ask_depth
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0);

        let imbalance =
            if (bid_depth_f + ask_depth_f)
                <= 0.0
            {
                0.0
            } else {
                (bid_depth_f - ask_depth_f)
                    / (bid_depth_f + ask_depth_f)
            };

        let now_ms =
            latest.timestamp_ms;

        let current_price =
            latest
                .last_trade_price
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0);

        let mut price_15s = None;
        let mut price_30s = None;
        let mut price_60s = None;

        for sample in token_window.window.iter() {
            let age =
                now_ms.saturating_sub(
                    sample.timestamp_ms,
                );

            let price =
                sample
                    .last_trade_price
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0);

            if age <= 15_000 {
                price_15s = Some(price);
            }

            if age <= 30_000 {
                price_30s = Some(price);
            }

            if age <= 60_000 {
                price_60s = Some(price);
            }
        }

        let velocity =
            match price_30s {
                Some(p)
                    if p.abs()
                        > f64::EPSILON =>
                {
                    (current_price - p) / p
                }
                _ => 0.0,
            };

        let velocity_60 =
            match price_60s {
                Some(p)
                    if p.abs()
                        > f64::EPSILON =>
                {
                    (current_price - p) / p
                }
                _ => velocity,
            };

        let acceleration =
            velocity - velocity_60;

        let decay_rate =
            match (price_15s, price_30s) {
                (
                    Some(p15),
                    Some(p30),
                )
                    if p30.abs()
                        > f64::EPSILON =>
                {
                    let older =
                        (p15 - p30) / p30;

                    velocity - older
                }

                _ => 0.0,
            };

        Some(TokenFeatures {
            current_price:
                latest
                    .last_trade_price,

            velocity,
            acceleration,
            decay_rate,

            bid_depth:
                latest.bid_depth,

            ask_depth:
                latest.ask_depth,

            imbalance,
            spread_pct,
        })
    }

    async fn evict_market(
        &self,
        assets: &MarketAssets,
    ) {
        let up =
            assets.up_asset_id.to_string();

        let down =
            assets.down_asset_id.to_string();

        self.windows
            .write()
            .await
            .remove(&up);

        self.windows
            .write()
            .await
            .remove(&down);

        self.latest_orderbooks
            .write()
            .await
            .remove(&up);

        self.latest_orderbooks
            .write()
            .await
            .remove(&down);

        self.latest_last_trades
            .write()
            .await
            .remove(&up);

        self.latest_last_trades
            .write()
            .await
            .remove(&down);
    }
}