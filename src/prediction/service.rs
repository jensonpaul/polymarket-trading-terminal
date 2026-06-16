use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use polymarket_client_sdk_v2::{
    gamma::Client as GammaClient,
    types::U256,
};
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::{
    prediction::{
        btc_feed::{BtcFeed, BtcSnapshot},
        conviction::ConvictionTracker,
        trend_strength::TrendStrengthDetector,
        polymarket_feed::{MarketAssets, PolymarketFeed},
        PolymarketFeatures,
        PredictionContext,
        PredictionEngine,
        PredictionStore,
        strategies::{
            HypeReversionStrategy,
            ConvictionFollowStrategy,
            ExternalBtcStrategy,
            TrendStrengthStrategy,
        },
        WindowState,
    },
    state::{slug_for_ts, stamp_5m},
    worker::{get_or_fetch_token_ids, get_or_fetch_market},
};

use btc_prediction_engine::{
    engine::{EngineHandles, PredictionEngine as ExternalBtcPredictionEngine},
    types::PredictionSnapshot,
};

use crate::prediction::conviction::ConvictionSnapshot;
use crate::prediction::trend_strength::TrendStrengthSnapshot;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SIGNAL_AGE_MS: u64 = 30_000;

/// Shared state written by the prediction receiver task and read by the
/// main poll loop.  Wrapped in RwLock so both sides can access without
/// blocking each other for long.
struct SharedPredictionState {
    /// Latest raw snapshot from the ONNX engine.
    raw: Option<PredictionSnapshot>,
    /// Latest stable conviction snapshot derived by ConvictionTracker.
    conviction: Option<ConvictionSnapshot>,
    /// Latest trend strength snapshot derived by TrendStrengthDetector.
    /// Independent of `conviction` — neither reads the other.
    trend_strength: Option<TrendStrengthSnapshot>,
}

pub struct PredictionService {
    state:  Arc<PredictionStore>,
    engine: PredictionEngine,

    btc_feed:       Arc<BtcFeed>,
    polymarket_feed: Arc<PolymarketFeed>,

    /// Shared between the receiver task (writer) and poll loop (reader).
    prediction_state: Arc<RwLock<SharedPredictionState>>,

    /// The running BTC engine. Held here so accumulators are never reset
    /// for the lifetime of the process.
    btc_engine: ExternalBtcPredictionEngine,

    /// Feed task handles. Kept alive by being held here; dropping these
    /// would cancel all exchange WebSocket tasks.
    _btc_engine_handles: EngineHandles,
}

impl PredictionService {
    pub fn new(
        state:               Arc<PredictionStore>,
        engine:              PredictionEngine,
        btc_engine:          ExternalBtcPredictionEngine,
        btc_engine_handles:  EngineHandles,
    ) -> Self {
        let btc_snapshot = Arc::new(ArcSwap::from_pointee(
            BtcSnapshot::default(),
        ));

        let window_state = Arc::new(RwLock::new(WindowState::default()));

        let btc_feed = Arc::new(BtcFeed::new(
            "BTC/USD",
            btc_snapshot,
            Arc::clone(&window_state),
        ));

        let polymarket_feed = Arc::new(PolymarketFeed::new(
            Arc::clone(&window_state),
        ));

        let prediction_state = Arc::new(RwLock::new(SharedPredictionState {
            raw:            None,
            conviction:     None,
            trend_strength: None,
        }));

        Self {
            state,
            engine,
            btc_feed,
            polymarket_feed,
            prediction_state,
            btc_engine,
            _btc_engine_handles: btc_engine_handles,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("PredictionService starting");

        // ── Receiver task ─────────────────────────────────────────────────────
        //
        // Owns the ConvictionTracker.  Runs on every raw model tick (~100ms)
        // and:
        //   1. Pushes the raw direction + confidence into the tracker.
        //   2. Writes both the raw snapshot and the latest conviction snapshot
        //      into SharedPredictionState so the poll loop can read them.
        //
        // The tracker is reset when the window rolls over.  We detect this by
        // comparing the snapshot's timestamp to the current 5-minute boundary.
        let mut rx = self.btc_engine.subscribe();
        let pred_state = Arc::clone(&self.prediction_state);

        tokio::spawn(async move {
            let mut tracker = ConvictionTracker::new();
            // No window-rollover reset for this detector, by design — its
            // state is intentionally long-lived across market windows.
            // Compare to `tracker` above, which IS reset below.
            let mut trend_detector = TrendStrengthDetector::new();
            let mut last_window_ts: Option<u64> = None;

            while let Ok(snapshot) = rx.recv().await {
                let now_ms = chrono::Utc::now().timestamp_millis() as u64;

                // ── Window rollover: reset tracker ────────────────────────
                let window_ts = stamp_5m();
                if last_window_ts != Some(window_ts) {
                    if last_window_ts.is_some() {
                        info!(
                            %window_ts,
                            "ConvictionTracker: window rolled, resetting"
                        );
                        tracker.reset();
                    }
                    last_window_ts = Some(window_ts);
                }

                // ── Feed the tracker ──────────────────────────────────────
                //
                // Primary source: ONNX model fused direction.
                // During warm-up (return_300s = None, all vol windows equal)
                // the model outputs ~85% Sideways, so we also push the
                // heuristic signal when the model is Sideways but the
                // heuristic has high conviction.  The tracker treats both
                // identically — the caller (ExternalBtcStrategy) knows which
                // source was used via the reason string.
                let (push_dir, push_conf) =
                    if snapshot.fused_direction
                        != btc_prediction_engine::types::TrendDirection::Sideways
                    {
                        (snapshot.fused_direction, snapshot.fused_confidence)
                    } else if snapshot.heuristic.fused_direction
                        != btc_prediction_engine::types::TrendDirection::Sideways
                        && snapshot.heuristic.fused_confidence >= 0.70
                    {
                        (
                            snapshot.heuristic.fused_direction,
                            snapshot.heuristic.fused_confidence,
                        )
                    } else {
                        (
                            btc_prediction_engine::types::TrendDirection::Sideways,
                            snapshot.fused_confidence,
                        )
                    };

                tracker.push(push_dir, push_conf, now_ms);

                // Same blended input as `tracker` above, deliberately — this
                // experiment isolates lifecycle/queue semantics, not input
                // quality. trend_detector has no reset() and is never
                // touched by the window-rollover branch above.
                trend_detector.push(push_dir, push_conf, now_ms);

                let conviction_snap = tracker.snapshot();
                let trend_snap = trend_detector.snapshot(now_ms);

                tracing::debug!(
                    model_dir    = ?snapshot.fused_direction,
                    model_conf   = snapshot.fused_confidence,
                    heuristic    = ?snapshot.heuristic.fused_direction,
                    active       = ?conviction_snap.active.as_ref().map(|a| a.direction),
                    queued       = conviction_snap.queued,
                    building     = ?conviction_snap.building,
                    building_ticks = conviction_snap.building_ticks,
                    "conviction tick"
                );

                // ── Write shared state ────────────────────────────────────
                {
                    let mut guard = pred_state.write().await;
                    guard.raw            = Some(snapshot);
                    guard.conviction     = Some(conviction_snap);
                    guard.trend_strength = Some(trend_snap);
                }
            }

            tracing::warn!("prediction receiver task exited — channel closed");
        });

        // ── Strategy registration ─────────────────────────────────────────────
        self.engine.register(ConvictionFollowStrategy::new());
        self.engine.register(HypeReversionStrategy::new());
        self.engine.register(ExternalBtcStrategy::new());
        self.engine.register(TrendStrengthStrategy::new());

        // ── BTC feed task ─────────────────────────────────────────────────────
        let btc_feed = Arc::clone(&self.btc_feed);
        tokio::spawn(async move {
            btc_feed.run().await;
        });

        let gamma = GammaClient::default();

        let mut active_window:     Option<u64>        = None;
        let mut current_token_ids: Option<Vec<String>> = None;

        loop {
            let window_ts = stamp_5m();

            // ── New market window ─────────────────────────────────────────
            if active_window != Some(window_ts) {
                let window_started_ms = window_ts * 1_000;
                self.btc_feed
                    .notify_window_start(window_started_ms)
                    .await;

                let slug = slug_for_ts(window_ts);

                match get_or_fetch_market(&gamma, &slug).await {
                    Ok(_market) => {}
                    Err(_e) => {
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
                                error!(%slug, error=%e, "asset conversion failed");
                                tokio::time::sleep(POLL_INTERVAL).await;
                                continue;
                            }
                        };

                        let assets = MarketAssets {
                            up_asset_id:   asset_ids[0],
                            down_asset_id: asset_ids[1],
                        };

                        let feed = Arc::clone(&self.polymarket_feed);
                        tokio::spawn(async move {
                            if let Err(e) = feed.run_market(assets, window_ts).await {
                                error!("polymarket feed error: {e:#}");
                            }
                        });

                        current_token_ids = Some(token_ids);
                        active_window     = Some(window_ts);

                        info!(%window_ts, %slug, "subscribed prediction market");
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

            // ── Read shared prediction state (single lock, cheap) ─────────
            let (external_prediction, conviction, trend_strength) = {
                let guard = self.prediction_state.read().await;
                (guard.raw.clone(), guard.conviction.clone(), guard.trend_strength.clone())
            };

            let prediction_ctx = PredictionContext {
                timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                btc,
                polymarket: PolymarketFeatures { up, down },
                seconds_remaining,
                external_prediction,
                conviction,
                trend_strength,
            };

            // ── Run all strategies — always one signal per strategy ──────
            //
            // evaluate() never returns an empty vec: every strategy emits
            // either a Buy/Hold or a NoTrade with a descriptive reason.
            // We always call update_signals so the UI reflects the current
            // state of every strategy on every poll tick.
            let signals = self.engine.evaluate(&prediction_ctx);
            let now_ms  = prediction_ctx.timestamp_ms;

            self.state.update_signals(
                window_ts,
                signals,
                prediction_ctx.btc.clone(),
                now_ms,
            );

            // ── Evict entries from previous windows that have gone stale ──
            let stale: Vec<u64> = self
                .state
                .signals
                .iter()
                .filter_map(|entry| {
                    let age = now_ms.saturating_sub(entry.value().last_updated_ms);
                    if age > MAX_SIGNAL_AGE_MS {
                        Some(*entry.key())
                    } else {
                        None
                    }
                })
                .collect();

            for key in stale {
                self.state.clear_signals(key, now_ms);
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}