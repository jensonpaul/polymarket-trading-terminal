use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::RwLock;
use tracing::{info, warn};

use polymarket_client_sdk_v2::{
    clob::ws::Client,
    types::U256,
};

use crate::prediction::{
    OrderbookSample,
    RollingWindow,
    TokenFeatures,
    WindowState,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketAssets {
    pub up_asset_id: U256,
    pub down_asset_id: U256,
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct TokenState {
    pub vwap: f64,
    pub vwap_asks: f64,
    pub vwap_cum: f64,
    pub vwap_asks_cum: f64,
    pub vwap_dev: f64,
    pub vwap_asks_dev: f64,
    pub vwap_cum_dev: f64,
    pub vwap_asks_cum_dev: f64,
    pub orderbook_imbalance: f64,
    pub last_price: f64,
}

// ─────────────────────────────────────────────────────────────────────────────
// BtcState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct BtcState {
    pub return_pct: f64,
    pub acceleration: f64,
    pub efficiency_ratio: f64,
    pub volatility: f64,
    pub near_high_low: f64,
    pub rolling_high: f64,
    pub rolling_low: f64,
    pub path_length: f64,
    pub net_displacement: f64,
    pub prev_return_pct: f64,
    pub last_price: f64,
    pub price_history: Vec<f64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// VwapCumAccumulator
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
struct VwapCumAccumulator {
    mid_cum_pv: f64,
    mid_cum_qty: f64,
    asks_cum_pv: f64,
    asks_cum_qty: f64,
}

impl VwapCumAccumulator {
    fn ingest(&mut self, mid_price: f64, mid_qty: f64, ask_pv: f64, ask_qty: f64) {
        self.mid_cum_pv   += mid_price * mid_qty;
        self.mid_cum_qty  += mid_qty;
        self.asks_cum_pv  += ask_pv;
        self.asks_cum_qty += ask_qty;
    }

    fn vwap_cum(&self) -> Option<f64> {
        (self.mid_cum_qty > 0.0).then(|| self.mid_cum_pv / self.mid_cum_qty)
    }

    fn vwap_asks_cum(&self) -> Option<f64> {
        (self.asks_cum_qty > 0.0).then(|| self.asks_cum_pv / self.asks_cum_qty)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenWindow — owns all in-memory state for one asset over one 5-min window.
//
// `token_log` and `btc_log` accumulate formatted log lines during the window.
// They are drained into `spawn_blocking` at eviction — no I/O on the hot path.
// ─────────────────────────────────────────────────────────────────────────────

struct TokenWindow {
    window_ts: u64,
    expires_at_sec: u64,
    window: RollingWindow<OrderbookSample>,
    accum: VwapCumAccumulator,
    /// Buffered token log lines: "price,vwap_asks,imbalance\n"
    token_log: Vec<String>,
    /// Buffered BTC log lines — written here so the two sides share one buffer.
    /// Only the UP asset writes BTC lines to avoid duplicates.
    btc_log: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// OrderbookState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct OrderbookState {
    best_bid: Decimal,
    best_ask: Decimal,
    bid_depth: Decimal,
    ask_depth: Decimal,
    asks: Vec<(Decimal, Decimal)>,
    timestamp_ms: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure helpers
// ─────────────────────────────────────────────────────────────────────────────

fn compute_vwap_asks(asks: &[(Decimal, Decimal)]) -> Option<f64> {
    let mut pv  = 0.0_f64;
    let mut qty = 0.0_f64;
    for (price, size) in asks {
        let p = price.to_f64()?;
        let s = size.to_f64()?;
        pv  += p * s;
        qty += s;
    }
    (qty > 0.0).then(|| pv / qty)
}

#[inline]
fn vwap_dev(price: f64, vwap: f64) -> f64 {
    if vwap == 0.0 { 0.0 } else { (price - vwap) / vwap }
}

fn ask_ladder_sums(asks: &[(Decimal, Decimal)]) -> (f64, f64) {
    let mut pv  = 0.0_f64;
    let mut qty = 0.0_f64;
    for (price, size) in asks {
        if let (Some(p), Some(s)) = (price.to_f64(), size.to_f64()) {
            pv  += p * s;
            qty += s;
        }
    }
    (pv, qty)
}

fn rolling_volatility(prices: &[f64]) -> f64 {
    if prices.len() < 2 { return 0.0; }
    let mean     = prices.iter().sum::<f64>() / prices.len() as f64;
    let variance = prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>()
        / prices.len() as f64;
    variance.sqrt() / mean.max(1.0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure state computers
// ─────────────────────────────────────────────────────────────────────────────

fn compute_token_state(ob: &OrderbookState, accum: &VwapCumAccumulator) -> TokenState {
    let current_price = ob.best_ask.to_f64().unwrap_or(0.0);
    let best_bid_f    = ob.best_bid.to_f64().unwrap_or(0.0);
    let best_ask_f    = ob.best_ask.to_f64().unwrap_or(0.0);
    let bid_depth_f   = ob.bid_depth.to_f64().unwrap_or(0.0);
    let ask_depth_f   = ob.ask_depth.to_f64().unwrap_or(0.0);
    let total_depth   = bid_depth_f + ask_depth_f;

    let orderbook_imbalance = if total_depth > 0.0 {
        (bid_depth_f - ask_depth_f) / total_depth
    } else {
        0.0
    };

    let vwap = if total_depth > 0.0 {
        (best_bid_f * bid_depth_f + best_ask_f * ask_depth_f) / total_depth
    } else {
        best_ask_f
    };

    let vwap_asks     = compute_vwap_asks(&ob.asks).unwrap_or(best_ask_f);
    let vwap_cum      = accum.vwap_cum().unwrap_or(vwap);
    let vwap_asks_cum = accum.vwap_asks_cum().unwrap_or(vwap_asks);

    TokenState {
        vwap,
        vwap_asks,
        vwap_cum,
        vwap_asks_cum,
        vwap_dev:          vwap_dev(current_price, vwap),
        vwap_asks_dev:     vwap_dev(current_price, vwap_asks),
        vwap_cum_dev:      vwap_dev(current_price, vwap_cum),
        vwap_asks_cum_dev: vwap_dev(current_price, vwap_asks_cum),
        orderbook_imbalance,
        last_price: current_price,
    }
}

fn compute_btc_state(
    btc_price: f64,
    origin_price: f64,
    origin_locked: bool,
    prev: &BtcState,
) -> BtcState {
    if !origin_locked || origin_price == 0.0 {
        let mut next = prev.clone();
        next.last_price = btc_price;
        next.price_history.push(btc_price);
        return next;
    }

    let net_displacement = btc_price - origin_price;
    let return_pct       = net_displacement / origin_price;
    let acceleration     = return_pct - prev.prev_return_pct;
    let delta            = (btc_price - prev.last_price).abs();
    let path_length      = prev.path_length + delta;

    let efficiency_ratio = if path_length > 0.0 {
        net_displacement.abs() / path_length
    } else {
        0.0
    };

    let rolling_high = if prev.rolling_high == 0.0 { btc_price } else { prev.rolling_high.max(btc_price) };
    let rolling_low  = if prev.rolling_low  == 0.0 { btc_price } else { prev.rolling_low.min(btc_price)  };

    let near_high_low = if rolling_high > rolling_low {
        (btc_price - rolling_low) / (rolling_high - rolling_low)
    } else {
        0.5
    };

    let mut price_history = prev.price_history.clone();
    price_history.push(btc_price);
    let volatility = rolling_volatility(&price_history);

    BtcState {
        return_pct,
        acceleration,
        efficiency_ratio,
        volatility,
        near_high_low,
        rolling_high,
        rolling_low,
        path_length,
        net_displacement,
        prev_return_pct: return_pct,
        last_price: btc_price,
        price_history,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dump payload — everything moved out of the lock before spawn_blocking.
// ─────────────────────────────────────────────────────────────────────────────

struct WindowDump {
    side: &'static str,
    window_ts: u64,
    orderbook_samples: VecDeque<OrderbookSample>,
    token_log: Vec<String>,
    /// Non-empty only for the UP side (avoids writing BTC log twice).
    btc_log: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// PolymarketFeed
// ─────────────────────────────────────────────────────────────────────────────

pub struct PolymarketFeed {
    client: Client,
    windows: Arc<RwLock<HashMap<String, TokenWindow>>>,
    latest_orderbooks: Arc<RwLock<HashMap<String, OrderbookState>>>,
    token_states: Arc<RwLock<HashMap<String, TokenState>>>,
    btc_state: Arc<RwLock<BtcState>>,
    window_state: Arc<RwLock<WindowState>>,
}

impl PolymarketFeed {
    pub fn new(window_state: Arc<RwLock<WindowState>>) -> Self {
        Self {
            client: Client::default(),
            windows: Arc::new(RwLock::new(HashMap::new())),
            latest_orderbooks: Arc::new(RwLock::new(HashMap::new())),
            token_states: Arc::new(RwLock::new(HashMap::new())),
            btc_state: Arc::new(RwLock::new(BtcState::default())),
            window_state,
        }
    }

    pub async fn run_market(
        &self,
        assets: MarketAssets,
        window_ts: u64,
    ) -> anyhow::Result<()> {
        let expires_at_sec = window_ts + 300;
        let up_id   = assets.up_asset_id.to_string();
        let down_id = assets.down_asset_id.to_string();

        let orderbook_stream = self
            .client
            .subscribe_orderbook(vec![assets.up_asset_id, assets.down_asset_id])?;

        let mut orderbook_stream = Box::pin(orderbook_stream);

        info!(%window_ts, %expires_at_sec, "market websocket started");

        loop {
            let now_sec = chrono::Utc::now().timestamp() as u64;

            if now_sec >= expires_at_sec {
                info!(%window_ts, "market expired");
                self.evict_market(&assets).await;
                break;
            }

            let Some(msg) = orderbook_stream.next().await else { break };
            let book = msg?;

            let best_bid = book.bids.iter().max_by_key(|v| v.price)
                .map(|v| v.price).unwrap_or(Decimal::ZERO);
            let best_ask = book.asks.iter().min_by_key(|v| v.price)
                .map(|v| v.price).unwrap_or(Decimal::ZERO);

            let bid_depth: Decimal = book.bids.iter().map(|v| v.size).sum();
            let ask_depth: Decimal = book.asks.iter().map(|v| v.size).sum();

            let mut asks: Vec<(Decimal, Decimal)> =
                book.asks.iter().map(|v| (v.price, v.size)).collect();
            asks.sort_unstable_by_key(|(p, _)| *p);

            let timestamp_ms = chrono::Utc::now().timestamp_millis() as u64;

            self.latest_orderbooks.write().await.insert(
                book.asset_id.to_string(),
                OrderbookState { best_bid, best_ask, bid_depth, ask_depth, asks, timestamp_ms },
            );

            self.try_publish_sample(
                book.asset_id.to_string(),
                expires_at_sec,
                &up_id,
                &down_id,
            ).await;
        }

        info!(%window_ts, "market websocket closed");
        Ok(())
    }

    async fn try_publish_sample(
        &self,
        asset_id: String,
        expires_at_sec: u64,
        up_id: &str,
        down_id: &str,
    ) {
        let orderbook = {
            self.latest_orderbooks.read().await.get(&asset_id).cloned()
        };
        let Some(orderbook) = orderbook else { return };
        if orderbook.best_ask.is_zero() { return; }

        let current_price = orderbook.best_ask;

        let sample = OrderbookSample {
            timestamp_ms: orderbook.timestamp_ms,
            asset_id: asset_id.clone(),
            best_bid: orderbook.best_bid,
            best_ask: orderbook.best_ask,
            bid_depth: orderbook.bid_depth,
            ask_depth: orderbook.ask_depth,
        };

        let is_up   = asset_id == up_id;
        let is_down = asset_id == down_id;

        if !is_up && !is_down { return; }

        // Update window-scoped conviction state.
        {
            let mut ws = self.window_state.write().await;
            if is_up   { ws.ingest_up(current_price, orderbook.timestamp_ms); }
            if is_down { ws.ingest_down(current_price, orderbook.timestamp_ms); }
        }

        // Compute metrics, append log lines, update rolling window — one lock.
        {
            let mut windows = self.windows.write().await;

            let tw = windows
                .entry(asset_id.clone())
                .or_insert_with(|| TokenWindow {
                    window_ts: expires_at_sec.saturating_sub(300),
                    expires_at_sec,
                    window: RollingWindow::new(Duration::from_secs(300)),
                    accum: VwapCumAccumulator::default(),
                    token_log: Vec::new(),
                    btc_log: Vec::new(),
                });

            // Rolling window + VWAP accumulators.
            let bid_depth_f = orderbook.bid_depth.to_f64().unwrap_or(0.0);
            let ask_depth_f = orderbook.ask_depth.to_f64().unwrap_or(0.0);
            let mid_price   = orderbook.best_ask.to_f64().unwrap_or(0.0);
            let mid_qty     = bid_depth_f + ask_depth_f;
            let (ask_pv, ask_qty) = ask_ladder_sums(&orderbook.asks);

            tw.window.push(sample);
            tw.accum.ingest(mid_price, mid_qty, ask_pv, ask_qty);

            // Token metrics + log line — computed under the same lock so we
            // never hold the lock a second time for the same tick.
            let state = compute_token_state(&orderbook, &tw.accum);
            tw.token_log.push(format!(
                "{},{},{}\n",
                state.last_price,
                state.vwap_asks,
                state.orderbook_imbalance,
            ));

            // BTC log line — only for the UP asset to avoid duplicates.
            if is_up {
                let (btc_price, origin_price, origin_locked) = {
                    // window_state lock is released before we get here; re-acquire read.
                    // This is a short read — no async work inside.
                    drop(windows); // release windows write lock first to avoid ordering issues
                    let ws = self.window_state.read().await;
                    (
                        ws.btc_last_price.to_f64().unwrap_or(0.0),
                        ws.btc_origin_price.to_f64().unwrap_or(0.0),
                        ws.btc_origin_locked,
                    )
                };

                if btc_price > 0.0 {
                    let prev = self.btc_state.read().await.clone();
                    let next = compute_btc_state(btc_price, origin_price, origin_locked, &prev);

                    let btc_line = format!(
                        "{},{},{},{},{}\n",
                        btc_price,
                        next.efficiency_ratio,
                        next.volatility,
                        next.near_high_low,
                        origin_price,
                    );

                    *self.btc_state.write().await = next;

                    // Re-acquire windows to push the BTC log line into the UP window.
                    let mut windows = self.windows.write().await;
                    if let Some(tw) = windows.get_mut(&asset_id) {
                        tw.btc_log.push(btc_line);
                    }

                    return; // token_states update happens below via the re-acquired lock path
                }

                // btc_price == 0 — still need to update token_states below.
                // Fall through by re-acquiring windows.
                let mut windows = self.windows.write().await;
                let tw = match windows.get(&asset_id) {
                    Some(tw) => tw,
                    None => return,
                };
                let state = compute_token_state(&orderbook, &tw.accum);
                self.token_states.write().await.insert(asset_id, state);
                return;
            }

            // For DOWN (and UP when we didn't early-return), update token_states.
            let state = compute_token_state(&orderbook, &tw.accum);
            self.token_states.write().await.insert(asset_id, state);
        }
    }

    pub async fn token_features(&self, asset_id: &str, is_up: bool) -> Option<TokenFeatures> {
        let latest = {
            let windows = self.windows.read().await;
            let tw = windows.get(asset_id)?;
            tw.window.latest()?.clone()
        };

        let current_price = latest.best_ask;
        let spread        = latest.best_ask - latest.best_bid;
        let spread_pct    = if current_price.is_zero() {
            0.0
        } else {
            (spread / current_price).to_string().parse().unwrap_or(0.0)
        };

        let bid_depth_f = latest.bid_depth.to_f64().unwrap_or(0.0);
        let ask_depth_f = latest.ask_depth.to_f64().unwrap_or(0.0);
        let imbalance   = if (bid_depth_f + ask_depth_f) <= 0.0 {
            0.0
        } else {
            (bid_depth_f - ask_depth_f) / (bid_depth_f + ask_depth_f)
        };

        let ws = self.window_state.read().await;
        let (origin_price, distance_from_origin_pct, area_under_curve) =
            if is_up {
                (ws.up_origin_price, ws.up_distance_from_origin_pct, ws.up_area)
            } else {
                (ws.down_origin_price, ws.down_distance_from_origin_pct, ws.down_area)
            };

        Some(TokenFeatures {
            current_price,
            origin_price,
            distance_from_origin_pct,
            area_under_curve,
            bid_depth: latest.bid_depth,
            ask_depth: latest.ask_depth,
            imbalance,
            spread_pct,
        })
    }

    async fn evict_market(&self, assets: &MarketAssets) {
        let up   = assets.up_asset_id.to_string();
        let down = assets.down_asset_id.to_string();

        let mut dumps: Vec<WindowDump> = Vec::with_capacity(2);

        {
            let mut windows = self.windows.write().await;

            if let Some(tw) = windows.remove(&up) {
                dumps.push(WindowDump {
                    side: "up",
                    window_ts: tw.window_ts,
                    orderbook_samples: tw.window.samples().clone(),
                    token_log: tw.token_log,
                    btc_log: tw.btc_log,       // BTC lines live in the UP window
                });
            }
            if let Some(tw) = windows.remove(&down) {
                dumps.push(WindowDump {
                    side: "down",
                    window_ts: tw.window_ts,
                    orderbook_samples: tw.window.samples().clone(),
                    token_log: tw.token_log,
                    btc_log: vec![],            // DOWN never accumulates BTC lines
                });
            }
        }

        {
            let mut states = self.token_states.write().await;
            states.remove(&up);
            states.remove(&down);
        }
        *self.btc_state.write().await = BtcState::default();

        self.latest_orderbooks.write().await.remove(&up);
        self.latest_orderbooks.write().await.remove(&down);

        if !dumps.is_empty() {
            tokio::task::spawn_blocking(move || {
                for dump in dumps {
                    write_window_dump(&dump);
                }
            });
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Blocking I/O — runs entirely inside spawn_blocking, off the async executor.
// ─────────────────────────────────────────────────────────────────────────────

fn write_window_dump(dump: &WindowDump) {
    use std::fs;
    use std::io::Write;

    let side       = dump.side;
    let window_ts  = dump.window_ts;

    // ── Orderbook samples (existing behaviour) ────────────────────────────
    if !dump.orderbook_samples.is_empty() {
        let dir = std::path::Path::new("orderbook_dumps");
        match fs::create_dir_all(dir) {
            Err(e) => warn!(%side, error=%e, "failed to create orderbook_dumps dir"),
            Ok(()) => {
                let path = dir.join(format!("{}_{}.ndjson", window_ts, side));
                match fs::File::create(&path) {
                    Err(e) => warn!(%side, path=%path.display(), error=%e, "failed to create orderbook dump"),
                    Ok(file) => {
                        let mut w = std::io::BufWriter::new(file);
                        for sample in &dump.orderbook_samples {
                            match serde_json::to_string(sample) {
                                Ok(line) => { let _ = writeln!(w, "{}", line); }
                                Err(e)   => { warn!(%side, error=%e, "serialise error"); }
                            }
                        }
                        info!(
                            %side, %window_ts,
                            samples = dump.orderbook_samples.len(),
                            path    = %path.display(),
                            "orderbook window dumped"
                        );
                    }
                }
            }
        }
    }

    // ── Token log ─────────────────────────────────────────────────────────
    write_log_lines(
        &dump.token_log,
        "logs_token_state",
        &format!("{}_{}.log", window_ts, side),
        side,
        window_ts,
        "token",
    );

    // ── BTC log (UP side only) ────────────────────────────────────────────
    if !dump.btc_log.is_empty() {
        write_log_lines(
            &dump.btc_log,
            "logs_btc_state",
            &format!("{}.log", window_ts),
            side,
            window_ts,
            "btc",
        );
    }
}

fn write_log_lines(
    lines: &[String],
    dir: &str,
    filename: &str,
    side: &str,
    window_ts: u64,
    kind: &str,
) {
    use std::fs;
    use std::io::Write;

    if lines.is_empty() { return; }

    if let Err(e) = fs::create_dir_all(dir) {
        warn!(%side, %kind, error=%e, "failed to create log dir");
        return;
    }

    let path = std::path::Path::new(dir).join(filename);
    match fs::File::create(&path) {
        Err(e) => warn!(%side, %kind, path=%path.display(), error=%e, "failed to create log file"),
        Ok(file) => {
            let mut w = std::io::BufWriter::new(file);
            for line in lines {
                if let Err(e) = w.write_all(line.as_bytes()) {
                    warn!(%side, %kind, error=%e, "write error");
                    return;
                }
            }
            info!(
                %side, %window_ts, %kind,
                lines = lines.len(),
                path  = %path.display(),
                "log dumped"
            );
        }
    }
}