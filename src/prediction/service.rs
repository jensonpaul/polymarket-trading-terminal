use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use std::sync::Mutex;
use std::collections::HashMap;

use arc_swap::ArcSwap;
use polymarket_client_sdk_v2::{
    gamma::Client as GammaClient,
    types::U256,
};
use tokio::sync::RwLock;
use tracing::{error, info};
use anyhow::Context;

use crate::{
    prediction::{
        btc_feed::{BtcFeed, BtcSnapshot},
        polymarket_feed::{MarketAssets, PolymarketFeed},
        PolymarketFeatures,
        PredictionContext,
        PredictionEngine,
        PredictionStore,
        strategies::{
            HypeReversionStrategy, 
            ConvictionFollowStrategy,
            ExternalBtcStrategy,
        },
        WindowState,
    },
    state::{slug_for_ts, stamp_5m},
    worker::{get_or_fetch_token_ids, get_or_fetch_market},
};

use tokio::signal;
use btc_onnx_trend_model::OnnxTrendModel;
use btc_prediction_engine::{
    engine::{EngineConfig, PredictionEngine as ExternalBtcPredictionEngine},
    feeds::{BookFeedConfig, FeedConfig},
    pipeline::PipelineConfig,
    types::{Exchange, PredictionSnapshot, Symbol, TrendDirection},
};

const MODEL_BYTES: &[u8] = include_bytes!("../../models/direction_model.onnx");

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SIGNAL_AGE_MS: u64 = 30_000;

pub struct PredictionService {
    state: Arc<PredictionStore>,
    engine: PredictionEngine,

    btc_feed: Arc<BtcFeed>,
    polymarket_feed: Arc<PolymarketFeed>,

    btc_prediction_snapshot: Arc<RwLock<Option<PredictionSnapshot>>>,
}

impl PredictionService {
    pub fn new(
        state: Arc<PredictionStore>,
        engine: PredictionEngine,
    ) -> Self {
        let btc_snapshot = Arc::new(ArcSwap::from_pointee(
            BtcSnapshot::default(),
        ));

        // The WindowState Arc is the single source of truth for all
        // window-scoped conviction data.  It is shared by both feeds and
        // reset here in the service whenever the active window changes.
        let window_state = Arc::new(RwLock::new(WindowState::default()));

        let btc_feed = Arc::new(BtcFeed::new(
            "BTC/USD",
            btc_snapshot,
            Arc::clone(&window_state),
        ));

        let polymarket_feed = Arc::new(PolymarketFeed::new(
            Arc::clone(&window_state),
        ));

        let btc_prediction_snapshot =
            Arc::new(RwLock::new(None));

        Self {
            state,
            engine,
            btc_feed,
            polymarket_feed,
            btc_prediction_snapshot,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("PredictionService starting");

        // --------------------------------------------------------------
        // External BTC snappshot prediction
        // --------------------------------------------------------------

        let onnx_model = tokio::task::spawn_blocking(move || {
            OnnxTrendModel::load_from_bytes(MODEL_BYTES)
        })
        .await
        .context("model load task panicked")?
        .context("failed to load ONNX model")?;

        /*
        let onnx_model = OnnxTrendModel::load_from_bytes(MODEL_BYTES)
            .expect("embedded direction_model.onnx failed to load — rebuild after retraining");

        let engine_config = EngineConfig {
            pipeline: PipelineConfig {
                ext_trend: Some(Box::new(onnx_model)),
                ..PipelineConfig::default()
            },
            ..EngineConfig::default()
        };

        let (btc_engine, _handles) =
            btc_prediction_engine::prelude::ExternalBtcPredictionEngine::start(
                engine_config,
            )
            .await;

        btc_engine.add_feed(
            FeedConfig::public(
                Exchange::Binance,
                Symbol::BtcUsd,
            ),
        );

        btc_engine.add_feed(
            FeedConfig::public(
                Exchange::Kraken,
                Symbol::BtcUsd,
            ),
        );

        btc_engine.add_feed(
            FeedConfig::public(
                Exchange::Bitstamp,
                Symbol::BtcUsd,
            ),
        );

        btc_engine.add_book_feed(BookFeedConfig::public(Exchange::Binance,  Symbol::BtcUsd));
        btc_engine.add_book_feed(BookFeedConfig::public(Exchange::Kraken,   Symbol::BtcUsd));
        btc_engine.add_book_feed(BookFeedConfig::public(Exchange::Bitstamp, Symbol::BtcUsd));

        let mut rx = btc_engine.subscribe();
        */

        // ── Engine configuration ──────────────────────────────────────────────────
        let pipeline_config = PipelineConfig {
            // Hand the trained model to the pipeline. It will be invoked on every
            // feature vector produced by Stage 2.
            ext_trend: Some(Box::new(onnx_model)),
    
            // Forecast horizons: 5 s × 6 steps (30 s total) and 30 s × 10 steps
            // (5 min total). These are unchanged from the default.
            forecast_steps: vec![(5, 6), (30, 10)],
    
            // All other fields (filter, fusion, broadcast_cap) use sensible
            // defaults — adjust here if needed.
            ..PipelineConfig::default()
        };
    
        let engine_config = EngineConfig {
            pipeline: pipeline_config,
            ..EngineConfig::default()
        };
    
        // ── Start engine ──────────────────────────────────────────────────────────
        info!("starting prediction engine");
        let (engine, handles) = ExternalBtcPredictionEngine::start(engine_config).await;
    
        // ── Attach trade feeds ────────────────────────────────────────────────────
        // Each feed runs as an independent tokio task with its own reconnect loop.
        for exchange in [Exchange::Binance, Exchange::Coinbase, Exchange::Kraken, Exchange::Bitstamp] {
            let config = FeedConfig::public(exchange, Symbol::BtcUsd);
            engine.add_feed(config);
            info!(?exchange, "trade feed spawned");
        }
    
        // ── Attach order-book feeds ───────────────────────────────────────────────
        // Book feeds supply the top-5 bid/ask imbalance features (book_imb5,
        // book_spread_pct, book_pressure) — the highest-signal sub-minute
        // inputs for the model.
        //
        // Coinbase does not expose a public order-book WebSocket on the same
        // endpoint, so only three exchanges are used here.
        for exchange in [Exchange::Binance, Exchange::Kraken, Exchange::Bitstamp] {
            let config = BookFeedConfig::public(exchange, Symbol::BtcUsd);
            engine.add_book_feed(config);
            info!(?exchange, "order-book feed spawned");
        }
    
        // ── Subscribe to prediction snapshots ────────────────────────────────────
        let mut rx = engine.subscribe();

        {
            let snapshot =
                Arc::clone(&self.btc_prediction_snapshot);

            tokio::spawn(async move {
                while let Ok(prediction) = rx.recv().await {
                    *snapshot.write().await = Some(prediction);
                }
            });
        }

        // --------------------------------------------------------------

        self.engine.register(
            ConvictionFollowStrategy::new()
        );

        self.engine.register(
            HypeReversionStrategy::new()
        );

        self.engine.register(
            ExternalBtcStrategy::new()
        );

        let btc_feed = Arc::clone(&self.btc_feed);
        tokio::spawn(async move {
            btc_feed.run().await;
        });

        let gamma = GammaClient::default();

        let mut active_window: Option<u64> = None;
        let mut current_token_ids: Option<Vec<String>> = None;

        loop {
            let window_ts = stamp_5m();

            // ── New market window ─────────────────────────────────────────
            if active_window != Some(window_ts) {
                // Reset window-wide conviction state immediately so BTC
                // starts accumulating origin ticks for the new window.
                let window_started_ms = window_ts * 1_000;
                self.btc_feed
                    .notify_window_start(window_started_ms)
                    .await;

                let slug = slug_for_ts(window_ts);

                match get_or_fetch_market(&gamma, &slug).await {
                    Ok(market) => {
                        // market data available
                    }
                    Err(e) => {
                        tracing::warn!("market data fetch failed!");
                    }
                }

                match get_or_fetch_token_ids(&gamma, &slug).await {
                    Ok(token_ids) if token_ids.len() >= 2 => {
                        let asset_ids = match token_ids
                            .iter()
                            .map(|v| U256::from_str(v))
                            .collect::<Result<Vec<_>, _>>()
                        {
                            Ok(v) => v,
                            Err(e) => {
                                error!(
                                    %slug,
                                    error=%e,
                                    "asset conversion failed"
                                );
                                tokio::time::sleep(POLL_INTERVAL).await;
                                continue;
                            }
                        };

                        // token_ids[0] is always UP, token_ids[1] is always DOWN.
                        let assets = MarketAssets {
                            up_asset_id: asset_ids[0],
                            down_asset_id: asset_ids[1],
                        };

                        let feed = Arc::clone(&self.polymarket_feed);

                        tokio::spawn(async move {
                            if let Err(e) = feed
                                .run_market(assets, window_ts)
                                .await
                            {
                                error!("polymarket feed error: {e:#}");
                            }
                        });

                        current_token_ids = Some(token_ids);
                        active_window = Some(window_ts);

                        info!(
                            %window_ts,
                            %slug,
                            "subscribed prediction market"
                        );
                    }

                    Ok(_) => {
                        error!(%slug, "token count < 2");
                    }

                    Err(e) => {
                        error!(%slug, error=%e, "token fetch failed");
                    }
                }
            }

            let token_ids = match &current_token_ids {
                Some(v) if v.len() >= 2 => v,
                _ => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            };

            // ── Gate: wait until BTC origin VWAP is locked ───────────────
            if !self.btc_feed.btc_origin_locked().await {
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }

            let now_sec = chrono::Utc::now().timestamp() as u64;
            let elapsed = now_sec.saturating_sub(window_ts);
            let seconds_remaining = 300u64.saturating_sub(elapsed);

            let btc = self.btc_feed.features().await;

            // token_ids[0] = UP (is_up: true), token_ids[1] = DOWN (is_up: false).
            let up = self
                .polymarket_feed
                .token_features(token_ids[0].as_str(), true)
                .await
                .unwrap_or_default();

            let down = self
                .polymarket_feed
                .token_features(token_ids[1].as_str(), false)
                .await
                .unwrap_or_default();

            let external_prediction =
                self.btc_prediction_snapshot
                    .read()
                    .await
                    .clone();

            let prediction_ctx = PredictionContext {
                timestamp_ms: chrono::Utc::now()
                    .timestamp_millis() as u64,
                btc,
                polymarket: PolymarketFeatures { up, down },
                seconds_remaining,
                external_prediction
            };

            if let Some(signal) =
                self.engine.evaluate_best(&prediction_ctx)
            {
                self.state.update_signal(
                    window_ts,
                    signal,
                    prediction_ctx.btc.clone(),
                    prediction_ctx.timestamp_ms,
                );
            } else {
                // Always keep BTC metrics current even when no signal fires.
                self.state.update_btc(
                    window_ts,
                    prediction_ctx.btc.clone(),
                    prediction_ctx.timestamp_ms,
                );

                if let Some(existing) =
                    self.state.signals.get(&window_ts)
                {
                    let age = prediction_ctx
                        .timestamp_ms
                        .saturating_sub(existing.last_updated_ms);

                    if age > MAX_SIGNAL_AGE_MS {
                        self.state.clear_signal(
                            window_ts,
                            prediction_ctx.timestamp_ms,
                        );
                    }
                }
            }

            // Evict stale signals from previous windows.
            let now_ms = prediction_ctx.timestamp_ms;

            let stale: Vec<u64> = self
                .state
                .signals
                .iter()
                .filter_map(|entry| {
                    let state = entry.value();

                    if state.active_signal.is_none() {
                        return None;
                    }

                    let age = now_ms
                        .saturating_sub(state.last_updated_ms);

                    if age > MAX_SIGNAL_AGE_MS {
                        Some(*entry.key())
                    } else {
                        None
                    }
                })
                .collect();

            for key in stale {
                self.state.clear_signal(key, now_ms);
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}