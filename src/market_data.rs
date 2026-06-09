//! # Market Data Feed
//!
//! Spawns one Tokio task per 5-minute window.  The task subscribes to the
//! Polymarket WebSocket and emits [`crate::events::AppEvent`]s through the
//! [`crate::events::EventBus`] on every price tick or connection change.
//!
//! **No direct state writes happen here.**  All mutations go through
//! [`crate::reducer::apply`] in the UI drain loop.
//!
//! The caller signals shutdown via `MarketFeedHandle::shutdown`
//! (a `tokio::sync::Notify`).

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::StreamExt;
use tracing::{error, info, warn};

use polymarket_client_sdk_v2::clob::ws::Client as WsClient;
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::prelude::ToPrimitive;

use crate::events::{AppEvent, EventBus};
use crate::state::{MarketFeedHandle, MarketPrices, SharedAppState, SharedMarketPrices};
use crate::worker::get_or_fetch_token_ids;

const STALE_TIMEOUT: Duration = Duration::from_secs(5);
const STALE_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Create and register a live price feed for `window_ts`.
///
/// 1. Inserts a default (stale) [`SharedMarketPrices`] into
///    `state.market_prices` so the UI can display a loading state immediately.
/// 2. Spawns a Tokio task that connects to the WS feed and emits
///    [`AppEvent::PriceTick`] / [`AppEvent::FeedStatusChanged`] events.
/// 3. Stores a [`MarketFeedHandle`] in `state.market_feeds` so the caller
///    can shut it down.
///
/// Intentionally `async fn` — the caller should `.await` it but it returns
/// immediately after spawning (the feed task runs independently).
pub async fn start_market_feed(
    window_ts: u64,
    slug: String,
    state: SharedAppState,
    bus: EventBus,
) {
    // Initialise a stale price snapshot so the UI has something to display.
    let prices: SharedMarketPrices = Arc::new(ArcSwap::from_pointee(MarketPrices::default()));
    state.market_prices.insert(window_ts, prices.clone());

    let shutdown = Arc::new(tokio::sync::Notify::new());
    state.market_feeds.insert(
        window_ts,
        MarketFeedHandle {
            shutdown: shutdown.clone(),
        },
    );

    // Emit initial stale status so the UI can show a connecting indicator.
    let _ = bus
        .send(AppEvent::FeedStatusChanged {
            window_ts,
            connected: false,
            stale: true,
            error: None,
        })
        .await;

    tokio::spawn(async move {
        info!(%window_ts, %slug, "market feed task started");

        // ------------------------------------------------------------------
        // Fetch token IDs (shutdown-aware retry)
        // ------------------------------------------------------------------
        let gamma = GammaClient::default();

        let token_ids = loop {
            tokio::select! {
                biased;
                _ = shutdown.notified() => {
                    info!(%window_ts, "market feed cancelled before init");
                    return;
                }
                res = get_or_fetch_token_ids(&gamma, &slug) => {
                    match res {
                        Ok(ids) if ids.len() >= 2 => break ids,
                        Ok(_)   => error!(%slug, "token IDs count < 2"),
                        Err(e)  => error!(%slug, error=%e, "failed to fetch token IDs"),
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        let asset_ids: Vec<U256> = match token_ids.iter().map(|id| U256::from_str(id)).collect() {
            Ok(v)  => v,
            Err(e) => {
                error!(%slug, error=%e, "asset ID conversion failed");
                return;
            }
        };

        let up_asset_id   = Arc::<str>::from(token_ids[0].as_str());
        let down_asset_id = Arc::<str>::from(token_ids[1].as_str());

        // Store asset IDs into the local prices snapshot so the reducer's
        // PriceTick arm can route up/down correctly.  We do one direct write
        // here to initialise the asset-ID fields — the reducer does not carry
        // them in the event payload to avoid redundancy.
        prices.store(Arc::new(MarketPrices {
            up_asset_id:   up_asset_id.clone(),
            down_asset_id: down_asset_id.clone(),
            connected:     false,
            stale:         true,
            ..Default::default()
        }));

        let _ = bus
            .send(AppEvent::FeedStatusChanged {
                window_ts,
                connected: true,
                stale: false,
                error: None,
            })
            .await;

        // ------------------------------------------------------------------
        // Subscribe to WebSocket
        // ------------------------------------------------------------------
        let ws = WsClient::default();
        let stream = match ws.subscribe_last_trade_price(asset_ids) {
            Ok(s)  => s,
            Err(e) => {
                error!(%slug, error=%e, "WS subscribe failed");
                let _ = bus
                    .send(AppEvent::FeedStatusChanged {
                        window_ts,
                        connected: false,
                        stale: true,
                        error: Some(e.to_string()),
                    })
                    .await;
                return;
            }
        };
        let mut stream      = Box::pin(stream);
        let mut last_update = tokio::time::Instant::now();

        // ------------------------------------------------------------------
        // Event loop
        // ------------------------------------------------------------------
        loop {
            tokio::select! {
                biased;

                _ = shutdown.notified() => {
                    info!(%window_ts, "market feed shut down");
                    // Cleanup is handled by the reducer's WindowClosed arm.
                    return;
                }

                maybe_msg = stream.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            let ts    = msg.timestamp as u64;
                            let price = msg.price.to_f64().unwrap_or(0.0);
                            let asset = msg.asset_id.to_string();

                            // Read local snapshot to route up/down and
                            // check for out-of-order ticks before emitting.
                            let snap = prices.load();
                            if ts <= snap.last_ts {
                                continue;
                            }

                            let (up_price, down_price) = if asset == snap.up_asset_id.as_ref() {
                                (price, snap.down_price)
                            } else if asset == snap.down_asset_id.as_ref() {
                                (snap.up_price, price)
                            } else {
                                continue; // unknown asset
                            };

                            last_update = tokio::time::Instant::now();

                            let _ = bus
                                .send(AppEvent::PriceTick {
                                    window_ts,
                                    up_price,
                                    down_price,
                                    last_ts: ts,
                                })
                                .await;
                        }

                        Some(Err(e)) => {
                            warn!(%slug, error=%e, "stream error (SDK may reconnect)");
                            let _ = bus
                                .send(AppEvent::FeedStatusChanged {
                                    window_ts,
                                    connected: false,
                                    stale: true,
                                    error: Some(e.to_string()),
                                })
                                .await;
                        }

                        None => {
                            warn!(%slug, "stream ended unexpectedly");
                            let _ = bus
                                .send(AppEvent::FeedStatusChanged {
                                    window_ts,
                                    connected: false,
                                    stale: true,
                                    error: Some("stream ended".into()),
                                })
                                .await;
                        }
                    }
                }

                _ = tokio::time::sleep(STALE_CHECK_INTERVAL) => {
                    if last_update.elapsed() > STALE_TIMEOUT {
                        let snap = prices.load();
                        if !snap.stale {
                            let _ = bus
                                .send(AppEvent::FeedStatusChanged {
                                    window_ts,
                                    connected: false,
                                    stale: true,
                                    error: None,
                                })
                                .await;
                        }
                    }
                }
            }
        }
    });
}