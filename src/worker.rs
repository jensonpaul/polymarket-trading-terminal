//! # Background Worker
//!
//! The worker is a pure command executor + polling engine.  It is the **sole
//! writer** of [`crate::state::AppState`] (orders, trades, rapid-sell state).
//!
//! Architecture:
//!
//! ```text
//!   ┌──────────────────────────────────────────────────┐
//!   │  PolymarketWorker::run()                         │
//!   │                                                  │
//!   │  ┌─ Task A: orders polling loop  ───────────┐   │
//!   │  │  (polls open orders, updates AppState)   │   │
//!   │  └──────────────────────────────────────────┘   │
//!   │                                                  │
//!   │  ┌─ Task B: trades polling loop  ───────────┐   │
//!   │  │  (syncs trade confirmations)             │   │
//!   │  └──────────────────────────────────────────┘   │
//!   │                                                  │
//!   │  ┌─ Task C: rapid-sell automation  ─────────┐   │
//!   │  │  (auto-posts sell on confirmed buys)     │   │
//!   │  └──────────────────────────────────────────┘   │
//!   │                                                  │
//!   │  ┌─ Main loop: UiCommand dispatcher  ───────┐   │
//!   │  │  (PlaceLimit, PlaceMarket, Cancel, …)    │   │
//!   │  └──────────────────────────────────────────┘   │
//!   └──────────────────────────────────────────────────┘
//! ```
//!
//! After every write the worker calls `state.touch()` and
//! `ctx.request_repaint()` so the UI wakes up exactly when there is new data.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, Normal};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::{
    Amount, OrderType, OrderStatusType, Side, SignatureType, TickSize, TradeStatusType,
};
use polymarket_client_sdk_v2::clob::types::request::TradesRequest;
use polymarket_client_sdk_v2::clob::types::response::{
    CancelOrdersResponse, OpenOrderResponse, PostOrderResponse, TradeResponse,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketBySlugRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market;
use polymarket_client_sdk_v2::types::{Address, U256};
pub use polymarket_client_sdk_v2::error::Error;

use crate::market_data::start_market_feed;
use crate::messages::{UiCommand, WorkerEvent};
use crate::state::{
    AppState, LocalOrderStatus, NotificationKind, RapidSellState, SharedAppState, TrackedOrder,
    slug_for_ts, stamp_5m,
};
use crate::worker_config::{Queue, SharedPollConfig};

// ---------------------------------------------------------------------------
// Module-level caches
// ---------------------------------------------------------------------------

lazy_static! {
    static ref MARKET_CACHE: std::sync::Mutex<HashMap<String, Market>> =
        std::sync::Mutex::new(HashMap::new());

    static ref CREDS_CACHE: std::sync::Mutex<HashMap<String, Credentials>> =
        std::sync::Mutex::new(HashMap::new());
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

type AuthenticatedClient = ClobClient<Authenticated<Normal>>;
type SharedClient = AuthenticatedClient;

pub struct PolymarketWorker {
    pub cmd_rx: Receiver<UiCommand>,
    pub event_tx: Sender<WorkerEvent>,
    pub ctx: egui::Context,
    pub state: SharedAppState,
    pub poll_config: SharedPollConfig,
}

impl PolymarketWorker {
    async fn init_client(&self) -> anyhow::Result<AuthenticatedClient> {
        let private_key = std::env::var("PRIVATE_KEY_VAR")?;
        let host = std::env::var("CLOB_API_URL")
            .unwrap_or_else(|_| "https://clob.polymarket.com".into());
        let deposit_wallet = Address::from_str(&std::env::var("DEPOSIT_WALLET")?)?;

        let signer = LocalSigner::from_str(&private_key)?
            .with_chain_id(Some(polymarket_client_sdk_v2::POLYGON));

        let creds =
            get_or_fetch_api_creds(private_key.clone(), host.clone()).await?;

        let client = ClobClient::new(&host, Config::default())?
            .authentication_builder(&signer)
            .funder(deposit_wallet)
            .signature_type(SignatureType::Poly1271)
            .credentials(creds)
            .authenticate()
            .await?;

        Ok(client)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("PolymarketWorker: starting");

        let client = self.init_client().await?;

        // ------------------------------------------------------------------
        // Shared helpers passed into spawned tasks
        // ------------------------------------------------------------------
        let state = self.state.clone();
        let ctx = self.ctx.clone();

        // ------------------------------------------------------------------
        // Task A: Orders polling loop
        // ------------------------------------------------------------------
        spawn_orders_polling_loop(
            client.clone(),
            Arc::clone(&state),
            ctx.clone(),
            self.poll_config.atomic(Queue::Orders),
            self.event_tx.clone(),
        );

        // ------------------------------------------------------------------
        // Task B: Trades polling loop
        // ------------------------------------------------------------------
        spawn_trades_polling_loop(
            client.clone(),
            Arc::clone(&state),
            ctx.clone(),
            self.poll_config.atomic(Queue::Trades),
        );

        // ------------------------------------------------------------------
        // Task C: Rapid-sell automation loop
        // ------------------------------------------------------------------
        spawn_rapid_sell_loop(
            client.clone(),
            Arc::clone(&state),
            ctx.clone(),
            self.poll_config.atomic(Queue::RapidSell),
            self.event_tx.clone(),
        );

        // ------------------------------------------------------------------
        // Main loop: UI command dispatcher
        // ------------------------------------------------------------------
        info!("PolymarketWorker: listening for commands");

        while let Some(cmd) = self.cmd_rx.recv().await {
            info!(?cmd, "worker received command");

            match cmd {
                UiCommand::InitializeClient { token } => {
                    info!(%token, "client already initialised at startup; token noted");
                }

                UiCommand::UpdatePollInterval { milliseconds, queue } => {
                    self.poll_config.set(queue, milliseconds);
                    info!(?queue, milliseconds, "poll interval updated");
                }

                UiCommand::PlaceLimit { side, token, price, size, rapid_price, window_ts } => {
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    let ctx = ctx.clone();
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        let slug = slug_for_ts(stamp_5m());
                        let req = LimitRequest {
                            side: side.clone(),
                            token: token.clone(),
                            price: price.clone(),
                            size: size.clone(),
                        };

                        match place_order_limit(client.clone(), &req, &slug).await {
                            Ok(resp) => match parse_response(resp) {
                                Ok(order_id) => {
                                    let order = TrackedOrder {
                                        id: order_id.clone(),
                                        side,
                                        token,
                                        price,
                                        size,
                                        executed_price: None,
                                        executed_size: None,
                                        status: LocalOrderStatus::Open,
                                        size_matched: "0".into(),
                                        //inline_sell_price: "0.10".into(),
                                        inline_sell_price: Decimal::from_str(&rapid_price)
                                            .ok()
                                            .filter(|p| *p > Decimal::ZERO)
                                            .map(|p| p.to_string())
                                            .unwrap_or_else(|| "0.10".into()),
                                        inline_sell_size: "0".into(),
                                        inline_sell_market_type: "FAK".into(),
                                        rapid_sell_price: rapid_price,
                                        rapid_sell_size: "0".into(),
                                        rapid_sell_state: RapidSellState::Idle,
                                        is_trade_fully_confirmed: false,
                                        associate_trades: vec![],
                                        open_order_response: None,
                                        window_ts,
                                    };

                                    state.orders.insert(order_id.clone(), order);
                                    state.touch();
                                    ctx.request_repaint();

                                    notify(&event_tx, "Limit Order Placed", NotificationKind::Success).await;
                                }
                                Err(e) => {
                                    notify(&event_tx, &format!("Limit rejected: {e}"), NotificationKind::Error).await;
                                }
                            },
                            Err(e) => {
                                notify(&event_tx, &format!("Limit transport error: {e}"), NotificationKind::Error).await;
                            }
                        }
                        ctx.request_repaint();
                    });
                }

                UiCommand::PlaceMarket { side, token, usdc, shares, order_type, window_ts } => {
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    let ctx = ctx.clone();
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        let slug = slug_for_ts(stamp_5m());
                        let req = MarketRequest {
                            side: side.clone(),
                            token: token.clone(),
                            usdc: usdc.clone(),
                            shares: shares.clone(),
                            order_type: order_type.clone(),
                        };

                        match place_order_market(client.clone(), &req, &slug).await {
                            Ok(resp) => match parse_response(resp) {
                                Ok(order_id) => {
                                    let order = TrackedOrder {
                                        id: order_id.clone(),
                                        side,
                                        token,
                                        price: "Market".into(),
                                        size: "Market".into(),
                                        executed_price: None,
                                        executed_size: None,
                                        status: LocalOrderStatus::Open,
                                        size_matched: "0".into(),
                                        inline_sell_price: "0.50".into(),
                                        inline_sell_size: "0".into(),
                                        inline_sell_market_type: "FAK".into(),
                                        rapid_sell_price: "0.00".into(),
                                        rapid_sell_size: "0".into(),
                                        rapid_sell_state: RapidSellState::Idle,
                                        is_trade_fully_confirmed: false,
                                        associate_trades: vec![],
                                        open_order_response: None,
                                        window_ts,
                                    };

                                    state.orders.insert(order_id.clone(), order);
                                    state.touch();
                                    ctx.request_repaint();

                                    notify(&event_tx, "Market Order Placed", NotificationKind::Success).await;
                                }
                                Err(e) => {
                                    notify(&event_tx, &format!("Market rejected: {e}"), NotificationKind::Error).await;
                                }
                            },
                            Err(e) => {
                                notify(&event_tx, &format!("Market transport error: {e}"), NotificationKind::Error).await;
                            }
                        }
                        ctx.request_repaint();
                    });
                }

                UiCommand::CheckStatus { order_id, window_ts: _ } => {
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    let ctx = ctx.clone();

                    tokio::spawn(async move {
                        if let Ok(info) = get_order_status(client.clone(), &order_id).await {
                            apply_order_status_update(&state, &order_id, &info, false);
                            state.touch();
                            ctx.request_repaint();
                        }
                    });
                }

                UiCommand::CancelIndividual { order_id, window_ts: _ } => {
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    let ctx = ctx.clone();
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        match cancel_order(client.clone(), &order_id).await {
                            Ok(resp) => {
                                if resp.canceled.contains(&order_id) {
                                    if let Some(mut o) = state.orders.get_mut(&order_id) {
                                        o.status = LocalOrderStatus::Canceled;
                                    }
                                    state.touch();
                                    ctx.request_repaint();
                                    notify(&event_tx, "Order cancelled", NotificationKind::Success).await;
                                } else {
                                    let reason = resp
                                        .not_canceled
                                        .get(&order_id)
                                        .map(|s| s.as_str())
                                        .unwrap_or("unknown reason");
                                    notify(&event_tx, &format!("Cancel rejected: {reason}"), NotificationKind::Error).await;
                                }
                            }
                            Err(e) => {
                                notify(&event_tx, &format!("Cancel transport error: {e}"), NotificationKind::Error).await;
                            }
                        }
                        ctx.request_repaint();
                    });
                }

                UiCommand::CancelAllInWindow { window_ts } => {
                    let client = client.clone();
                    let state = Arc::clone(&state);
                    let ctx = ctx.clone();
                    let event_tx = self.event_tx.clone();

                    tokio::spawn(async move {
                        let local_ids: Vec<String> = state
                            .orders
                            .iter()
                            .filter(|entry| entry.value().window_ts == window_ts)
                            .map(|entry| entry.key().clone())
                            .collect();

                        match cancel_all_orders(client.clone()).await {
                            Ok(resp) => {
                                let count = resp.canceled.len();
                                for id in &local_ids {
                                    if resp.canceled.contains(id) {
                                        if let Some(mut o) = state.orders.get_mut(id) {
                                            o.status = LocalOrderStatus::Canceled;
                                        }
                                    }
                                }
                                state.touch();
                                ctx.request_repaint();
                                notify(&event_tx, &format!("Batch cancel: {count} cancelled"), NotificationKind::Success).await;
                            }
                            Err(e) => {
                                notify(&event_tx, &format!("Batch cancel error: {e}"), NotificationKind::Error).await;
                            }
                        }
                        ctx.request_repaint();
                    });
                }

                UiCommand::StartMarketFeed { window_ts, slug } => {
                    if state.market_feeds.contains_key(&window_ts) {
                        info!(window_ts, "feed already running, skipping");
                        continue;
                    }
                    let event_tx = self.event_tx.clone();
                    start_market_feed(window_ts, slug, Arc::clone(&state), ctx.clone()).await;
                    let _ = event_tx
                        .send(WorkerEvent::MarketFeedStarted { window_ts })
                        .await;
                }

                UiCommand::StopMarketFeed { window_ts } => {
                    if let Some((_, handle)) = state.market_feeds.remove(&window_ts) {
                        handle.shutdown.notify_waiters();
                        info!(window_ts, "feed stopped");
                    }
                }

                UiCommand::CloseWindow { window_ts } => {
                    // ---------------------------------------------------------
                    // Stop feed
                    // ---------------------------------------------------------
                    if let Some((_, handle)) = state.market_feeds.remove(&window_ts) {
                        handle.shutdown.notify_waiters();
                    }

                    // ---------------------------------------------------------
                    // Remove market snapshot
                    // ---------------------------------------------------------
                    state.market_prices.remove(&window_ts);

                    // ---------------------------------------------------------
                    // Remove orders + associated trades
                    // ---------------------------------------------------------
                    let order_ids: Vec<String> = state
                        .orders
                        .iter()
                        .filter(|entry| entry.value().window_ts == window_ts)
                        .map(|entry| entry.key().clone())
                        .collect();

                    for order_id in order_ids {
                        if let Some((_, order)) = state.orders.remove(&order_id) {
                            for trade_id in &order.associate_trades {
                                state.trades.remove(trade_id);
                            }
                        }
                    }

                    state.touch();
                    ctx.request_repaint();

                    // ---------------------------------------------------------
                    // Notify UI
                    // ---------------------------------------------------------
                    let _ = self.event_tx.send(WorkerEvent::WindowClosed { window_ts }).await;
                }
            }
        }

        warn!("PolymarketWorker: command channel closed");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Polling task A: orders
// ---------------------------------------------------------------------------

fn spawn_orders_polling_loop(
    client: SharedClient,
    state: SharedAppState,
    ctx: egui::Context,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
    event_tx: Sender<WorkerEvent>,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval = make_interval(current_ms);
                info!(current_ms, "orders poll interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            interval.tick().await;

            let snapshot = state.pollable_orders();

            if snapshot.is_empty() {
                continue;
            }

            for order_id in &snapshot {
                let Ok(info) = get_order_status(client.clone(), order_id).await else {
                    continue;
                };

                apply_order_status_update(
                    &state,
                    order_id,
                    &info,
                    true,
                );
            }

            if !snapshot.is_empty() {
                state.touch();
                ctx.request_repaint();
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Polling task B: trades
// ---------------------------------------------------------------------------

/*
fn spawn_trades_polling_loop(
    client: SharedClient,
    state: SharedAppState,
    ctx: egui::Context,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval = make_interval(current_ms);
                info!(current_ms, "trades poll interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            interval.tick().await;

            if state.orders.is_empty() {
                continue;
            }

            // Deduplicate by window so we make at most one trades call per
            // active market.
            let mut seen_slugs = std::collections::HashSet::new();

            let orders: Vec<_> = state
                .orders
                .iter()
                .map(|e| e.value().window_ts)
                .collect();

            for window_ts in orders {

            for entry in state.orders.iter() {
                let order = entry.value();
                let slug = slug_for_ts(stamp_5m());

                if !seen_slugs.insert(slug.clone()) {
                    continue;
                }

                let condition_id = {
                    let cache = MARKET_CACHE.lock().unwrap();
                    cache.get(&slug).and_then(|m| m.condition_id)
                };

                let Some(condition_id) = condition_id else {
                    continue;
                };

                let mut req = TradesRequest::builder().build();
                req.market = Some(condition_id);

                let result = {
                    let guard = client.lock().await;
                    let Some(c) = guard.as_ref() else {
                        warn!("trades poll: client unavailable");
                        continue;
                    };
                    c.trades(&req, None).await
                };

                match result {
                    Ok(page) => {
                        for trade in page.data {
                            state.trades.insert(trade.id.clone(), trade);
                        }
                        state.touch();
                        ctx.request_repaint();
                    }
                    Err(e) => {
                        tracing::error!(%slug, error=%e, "trades poll failed");
                    }
                }
            }
        }
    });
}
*/
fn spawn_trades_polling_loop(
    client: SharedClient,
    state: SharedAppState,
    ctx: egui::Context,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval = make_interval(current_ms);

        loop {
            // Update interval if it has changed
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval = make_interval(current_ms);
                info!(current_ms, "trades poll interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            // Wait for the next tick
            interval.tick().await;

            // Compute the current 5-minute slug
            let slug = slug_for_ts(stamp_5m());

            // Get the market condition_id for this slug
            let condition_id = {
                let cache = MARKET_CACHE.lock().unwrap();
                cache.get(&slug).and_then(|m| m.condition_id)
            };

            let Some(condition_id) = condition_id else {
                // Market info unavailable, skip this tick
                continue;
            };

            // Build the trades request
            let mut req = TradesRequest::builder().build();
            req.market = Some(condition_id);

            // Call the client
            let result = client.trades(&req, None).await;

            // Handle the result
            match result {
                Ok(page) => {
                    for trade in page.data {
                        state.trades.insert(trade.id.clone(), trade);
                    }
                    state.touch();
                    ctx.request_repaint();
                }
                Err(e) => {
                    tracing::error!(%slug, error=%e, "trades poll failed");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Polling task C: rapid-sell automation
// ---------------------------------------------------------------------------

fn spawn_rapid_sell_loop(
    client: SharedClient,
    state: SharedAppState,
    ctx: egui::Context,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
    event_tx: Sender<WorkerEvent>,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval = make_interval(current_ms);
                info!(current_ms, "rapid-sell interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            interval.tick().await;

            // Collect candidates without holding the DashMap ref across await.
            /*
            let candidates: Vec<TrackedOrder> = state
                .orders
                .iter()
                .filter(|e| {
                    let o = e.value();
                    o.side.eq_ignore_ascii_case("buy")
                        && matches!(o.status, LocalOrderStatus::TradeConfirmed)
                        && o.is_trade_fully_confirmed
                        && matches!(
                            o.rapid_sell_state,
                            RapidSellState::Idle | RapidSellState::Failed(_)
                        )
                })
                .map(|e| e.value().clone())
                .collect();
            */
            let now = Instant::now();

            let candidates: Vec<TrackedOrder> = state
                .orders
                .iter()
                .filter_map(|entry| {
                    let order = entry.value();

                    if !order.side.eq_ignore_ascii_case("buy") {
                        return None;
                    }

                    /*
                    if !matches!(order.status, LocalOrderStatus::TradeConfirmed) {
                        return None;
                    }

                    if !order.is_trade_fully_confirmed {
                        return None;
                    }
                    */
                    match order.status {
                        LocalOrderStatus::PartiallyFilled { .. }
                        | LocalOrderStatus::FullyFilled
                        | LocalOrderStatus::TradeOpen
                        | LocalOrderStatus::TradeConfirmed => {}
                        _ => return None,
                    }

                    let matched =
                        Decimal::from_str(&order.size_matched).unwrap_or_default();

                    let already_sold =
                        Decimal::from_str(&order.rapid_sell_size).unwrap_or_default();

                    let remaining = (matched - already_sold).max(Decimal::ZERO);

                    if remaining < dec!(5) {
                        return None;
                    }

                    match &order.rapid_sell_state {
                        RapidSellState::Idle => Some(order.clone()),

                        RapidSellState::RetryScheduled { retry_at, .. }
                            if *retry_at <= now =>
                        {
                            Some(order.clone())
                        }

                        _ => None,
                    }
                })
                .collect();

            for order in candidates {
                let matched = Decimal::from_str(&order.size_matched).unwrap_or_default();
                let already_sold = Decimal::from_str(&order.rapid_sell_size).unwrap_or_default();
                let sell_price = Decimal::from_str(&order.rapid_sell_price).unwrap_or_default();
                let sell_amount = (matched - already_sold).max(Decimal::ZERO);

                if sell_price <= Decimal::ZERO || sell_amount < Decimal::from(5) {
                    continue;
                }

                // Mark pending immediately to prevent double-fire.
                /*
                if let Some(mut o) = state.orders.get_mut(&order.id) {
                    o.rapid_sell_state = RapidSellState::Pending;
                }
                */
                let acquired = {
                    if let Some(mut o) = state.orders.get_mut(&order.id) {
                        match o.rapid_sell_state {
                            RapidSellState::Idle => {
                                o.rapid_sell_state = RapidSellState::InFlight {
                                    attempt: 0,
                                    started_at: Instant::now(),
                                };
                                true
                            }

                            RapidSellState::RetryScheduled { attempt, retry_at, .. }
                                if retry_at <= Instant::now() =>
                            {
                                o.rapid_sell_state = RapidSellState::InFlight {
                                    attempt,
                                    started_at: Instant::now(),
                                };
                                true
                            }

                            _ => false,
                        }
                    } else {
                        false
                    }
                };

                if !acquired {
                    continue;
                }

                let client = client.clone();
                let state = Arc::clone(&state);
                let ctx = ctx.clone();
                let event_tx = event_tx.clone();
                let parent_id = order.id.clone();
                let token = order.token.clone();
                let window_ts = order.window_ts;
                let rapid_price = order.rapid_sell_price.clone();

                let attempt = {
                    if let Some(o) = state.orders.get(&order.id) {
                        match &o.rapid_sell_state {
                            RapidSellState::InFlight { attempt, .. } => *attempt,
                            _ => 0,
                        }
                    } else {
                        0
                    }
                };

                tokio::spawn(async move {
                    let slug = slug_for_ts(stamp_5m());
                    let req = LimitRequest {
                        side: "sell".into(),
                        token: token.clone(),
                        price: rapid_price.clone(),
                        size: sell_amount.to_string(),
                    };

                    match place_order_limit(client.clone(), &req, &slug).await {
                        Ok(resp) => match parse_response(resp) {
                            Ok(new_id) => {
                                let sell_order = TrackedOrder {
                                    id: new_id.clone(),
                                    side: "sell".into(),
                                    token: token.clone(),
                                    price: rapid_price.clone(),
                                    size: sell_amount.to_string(),
                                    executed_price: None,
                                    executed_size: None,
                                    status: LocalOrderStatus::Open,
                                    size_matched: "0".into(),
                                    inline_sell_price: "0".into(),
                                    inline_sell_size: "0".into(),
                                    inline_sell_market_type: "FAK".into(),
                                    rapid_sell_price: "0".into(),
                                    rapid_sell_size: "0".into(),
                                    rapid_sell_state: RapidSellState::Idle,
                                    is_trade_fully_confirmed: false,
                                    associate_trades: vec![],
                                    open_order_response: None,
                                    window_ts,
                                };

                                state.orders.insert(new_id.clone(), sell_order);

                                // Update parent order.
                                if let Some(mut parent) = state.orders.get_mut(&parent_id) {
                                    /*
                                    parent.rapid_sell_state = RapidSellState::Completed;
                                    parent.rapid_sell_size = matched.to_string();
                                    */
                                    let sold_before =
                                        Decimal::from_str(&parent.rapid_sell_size).unwrap_or_default();

                                    parent.rapid_sell_size =
                                        (sold_before + sell_amount).to_string();

                                    let matched =
                                        Decimal::from_str(&parent.size_matched).unwrap_or_default();

                                    let remaining =
                                        (matched
                                            - Decimal::from_str(&parent.rapid_sell_size)
                                                .unwrap_or_default())
                                        .max(Decimal::ZERO);

                                    if remaining >= dec!(5) {
                                        parent.rapid_sell_state = RapidSellState::Idle;
                                    } else {
                                        parent.rapid_sell_state = RapidSellState::Completed;
                                    }
                                }

                                state.touch();
                                ctx.request_repaint();

                                notify(
                                    &event_tx,
                                    &format!("Rapid Sell placed: {sell_amount} {token} @ {rapid_price}"),
                                    NotificationKind::Success,
                                )
                                .await;
                            }
                            Err(e) => {
                                if let Some(mut o) = state.orders.get_mut(&parent_id) {
                                    //o.rapid_sell_state = RapidSellState::Failed(e.to_string());
                                    schedule_retry(
                                        &mut o,
                                        attempt,
                                        e.to_string(),
                                    );
                                }
                                notify(&event_tx, &format!("Rapid Sell rejected: {e}"), NotificationKind::Error).await;
                            }
                        },
                        Err(e) => {
                            if let Some(mut o) = state.orders.get_mut(&parent_id) {
                                //o.rapid_sell_state = RapidSellState::Failed(e.to_string());
                                schedule_retry(
                                    &mut o,
                                    attempt,
                                    e.to_string(),
                                );
                            }
                            notify(&event_tx, &format!("Rapid Sell transport error: {e}"), NotificationKind::Error).await;
                        }
                    }
                    ctx.request_repaint();
                });
            }
        }
    });
}

// ---------------------------------------------------------------------------
// State mutation helper (shared between polling loop and CheckStatus)
// ---------------------------------------------------------------------------

/// Apply an [`OpenOrderResponse`] to `AppState::orders`.
///
/// Returns `true` if the order has reached a terminal state and should be
/// removed from the polling list.
fn apply_order_status_update(
    state: &AppState,
    order_id: &str,
    info: &OpenOrderResponse,
    check_trades: bool,
) -> bool {
    let tolerance = dec!(0.005);
    let is_fully_filled =
        info.size_matched >= info.original_size * (dec!(1.0) - tolerance);

    let is_trade_confirmed = if check_trades && !info.associate_trades.is_empty() {
        let confirmed: Decimal = info
            .associate_trades
            .iter()
            .filter_map(|tid| state.trades.get(tid))
            .filter(|t| matches!(t.status, TradeStatusType::Confirmed))
            .map(|t| t.value().size)
            .sum();
        confirmed >= info.original_size * (dec!(1.0) - tolerance)
    } else {
        false
    };

    let terminal;
    let new_status = match &info.status {
        OrderStatusType::Live => {
            terminal = false;
            if info.size_matched > Decimal::ZERO {
                LocalOrderStatus::PartiallyFilled {
                    filled: info.size_matched.to_string(),
                }
            } else {
                LocalOrderStatus::Open
            }
        }
        OrderStatusType::Matched => {
            if is_fully_filled {
                if is_trade_confirmed {
                    terminal = true;
                    LocalOrderStatus::TradeConfirmed
                } else {
                    terminal = false;
                    LocalOrderStatus::FullyFilled
                }
            } else {
                terminal = false;
                LocalOrderStatus::PartiallyFilled {
                    filled: info.size_matched.to_string(),
                }
            }
        }
        OrderStatusType::Canceled => {
            terminal = true;
            LocalOrderStatus::Canceled
        }
        OrderStatusType::Unknown(reason) => {
            terminal = true;
            if reason == "INVALID" {
                LocalOrderStatus::Canceled
            } else {
                warn!(%reason, %order_id, "unknown order status");
                LocalOrderStatus::Canceled
            }
        }
        _ => {
            terminal = true;
            warn!(%order_id, "non-exhaustive order status variant");
            LocalOrderStatus::Canceled
        }
    };

    if let Some(mut o) = state.orders.get_mut(order_id) {
        o.status = new_status;
        o.size_matched = info
            .size_matched
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
            .to_string();

        if info.size_matched > Decimal::ZERO {
            o.executed_size = Some(o.size_matched.clone());
            o.inline_sell_size = o.size_matched.clone();
            o.executed_price = Some(
                info.price
                    .round_dp_with_strategy(4, rust_decimal::RoundingStrategy::ToZero)
                    .to_string(),
            );
        }

        o.is_trade_fully_confirmed = is_trade_confirmed;
        o.associate_trades = info.associate_trades.clone();
        o.open_order_response = Some(info.clone());
    }

    terminal
}

pub const RAPID_SELL_MAX_ATTEMPTS: u32 = 8;

pub fn rapid_sell_backoff(attempt: u32) -> std::time::Duration {
    use std::time::Duration;

    match attempt {
        0 => Duration::from_secs(1),
        1 => Duration::from_secs(2),
        2 => Duration::from_secs(5),
        3 => Duration::from_secs(10),
        4 => Duration::from_secs(20),
        5 => Duration::from_secs(30),
        6 => Duration::from_secs(60),
        _ => Duration::from_secs(120),
    }
}

// ---------------------------------------------------------------------------
// Notification helper
// ---------------------------------------------------------------------------

async fn notify(tx: &Sender<WorkerEvent>, msg: &str, kind: NotificationKind) {
    let _ = tx
        .send(WorkerEvent::Notify {
            message: msg.to_owned(),
            kind,
        })
        .await;
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

fn schedule_retry(
    order: &mut TrackedOrder,
    attempt: u32,
    reason: impl Into<String>,
) {
    let reason = reason.into();

    if attempt >= RAPID_SELL_MAX_ATTEMPTS {
        order.rapid_sell_state = RapidSellState::PermanentlyFailed {
            attempts: attempt,
            reason,
        };
        return;
    }

    let retry_at = Instant::now() + rapid_sell_backoff(attempt);

    order.rapid_sell_state = RapidSellState::RetryScheduled {
        attempt: attempt + 1,
        retry_at,
        reason,
    };
}

// ---------------------------------------------------------------------------
// Interval helper
// ---------------------------------------------------------------------------

fn make_interval(ms: u64) -> tokio::time::Interval {
    tokio::time::interval(Duration::from_millis(ms.max(1)))
}

// ---------------------------------------------------------------------------
// Timer utility
// ---------------------------------------------------------------------------

struct Timer {
    label: &'static str,
    start: Instant,
}

impl Timer {
    fn start(label: &'static str) -> Self {
        Self { label, start: Instant::now() }
    }
    fn done(&self) {
        info!(label = self.label, elapsed = ?self.start.elapsed(), "step complete");
    }
}

macro_rules! timed {
    ($label:literal, $block:block) => {{
        let _t = Timer::start($label);
        let result = $block;
        _t.done();
        result
    }};
}

// ---------------------------------------------------------------------------
// Request types (internal only)
// ---------------------------------------------------------------------------

struct LimitRequest {
    side: String,
    token: String,
    price: String,
    size: String,
}

struct MarketRequest {
    side: String,
    token: String,
    usdc: Option<String>,
    shares: Option<String>,
    order_type: Option<String>,
}

// ---------------------------------------------------------------------------
// SDK helpers
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct ApiError {
    error: String,
}

fn parse_response(resp: PostOrderResponse) -> Result<String, Error> {
    if !resp.success {
        let msg = resp.error_msg.unwrap_or_else(|| "Order rejected".into());
        return Err(Error::validation(format!("Engine reject: {msg}")));
    }
    Ok(resp.order_id)
}

#[instrument(skip(client))]
pub async fn get_or_fetch_token_ids(
    client: &GammaClient,
    slug: &str,
) -> anyhow::Result<Vec<String>> {
    let market = get_or_fetch_market(client, slug).await?;
    Ok(market
        .clob_token_ids
        .as_ref()
        .map(|t| t.iter().map(|x| x.to_string()).collect())
        .unwrap_or_default())
}

#[instrument(skip(client))]
pub async fn get_or_fetch_market(client: &GammaClient, slug: &str) -> anyhow::Result<Market> {
    {
        let cache = MARKET_CACHE.lock().unwrap();
        if let Some(m) = cache.get(slug) {
            return Ok(m.clone());
        }
    }

    let req = MarketBySlugRequest::builder().slug(slug).build();
    let market = client.market_by_slug(&req).await?;

    {
        let mut cache = MARKET_CACHE.lock().unwrap();
        cache.clear();
        cache.insert(slug.to_string(), market.clone());
    }

    Ok(market)
}

pub async fn get_or_fetch_api_creds(
    private_key: String,
    host: String,
) -> anyhow::Result<Credentials> {
    let key = format!("{private_key}@{host}");
    {
        let cache = CREDS_CACHE.lock().unwrap();
        if let Some(c) = cache.get(&key) {
            return Ok(c.clone());
        }
    }

    let signer = LocalSigner::from_str(&private_key)?
        .with_chain_id(Some(polymarket_client_sdk_v2::POLYGON));
    let client = ClobClient::new(&host, Config::default())?;
    let creds: Credentials = client.create_or_derive_api_key(&signer, None).await?;

    {
        let mut cache = CREDS_CACHE.lock().unwrap();
        cache.insert(key, creds.clone());
    }
    Ok(creds)
}

async fn place_order_limit(
    client: SharedClient,
    payload: &LimitRequest,
    slug: &str,
) -> anyhow::Result<PostOrderResponse> {
    let _t = Timer::start("place_limit_total");

    let private_key = std::env::var("PRIVATE_KEY_VAR")?;
    let signer = LocalSigner::from_str(&private_key)?
        .with_chain_id(Some(polymarket_client_sdk_v2::POLYGON));

    let gamma = GammaClient::default();
    let ids = get_or_fetch_token_ids(&gamma, slug).await?;
    anyhow::ensure!(ids.len() >= 2, "no token IDs for slug {slug}");

    let token_id = if payload.token.eq_ignore_ascii_case("up") {
        U256::from_str(&ids[0])?
    } else if payload.token.eq_ignore_ascii_case("down") {
        U256::from_str(&ids[1])?
    } else {
        anyhow::bail!("invalid token '{}'; must be 'up' or 'down'", payload.token);
    };

    let price = Decimal::from_str(&payload.price)?;
    let size = Decimal::from_str(&payload.size)?;
    let side = parse_side(&payload.side)?;

    //client.set_tick_size(token_id, TickSize::Hundredth);

    let order = client.limit_order().token_id(token_id).size(size).price(price).side(side).build().await?;
    let signed = timed!("sign_limit", { client.sign(&signer, order).await? });
    let resp = timed!("post_limit", { client.post_order(signed).await? });
    Ok(resp)
}

async fn place_order_market(
    client: SharedClient,
    payload: &MarketRequest,
    slug: &str,
) -> anyhow::Result<PostOrderResponse> {
    let private_key = std::env::var("PRIVATE_KEY_VAR")?;
    let signer = LocalSigner::from_str(&private_key)?
        .with_chain_id(Some(polymarket_client_sdk_v2::POLYGON));

    let gamma = GammaClient::default();
    let ids = get_or_fetch_token_ids(&gamma, slug).await?;
    anyhow::ensure!(ids.len() >= 2, "no token IDs for slug {slug}");

    let token_id = match payload.token.to_lowercase().as_str() {
        "up" => U256::from_str(&ids[0])?,
        "down" => U256::from_str(&ids[1])?,
        _ => anyhow::bail!("invalid token '{}'", payload.token),
    };
    let side = parse_side(&payload.side)?;
    let order_type = match payload.order_type.as_deref() {
        Some("FAK") => OrderType::FAK,
        _ => OrderType::FOK,
    };

    //client.set_tick_size(token_id, TickSize::Hundredth);

    let mut builder = client.market_order().token_id(token_id).side(side).order_type(order_type);
    if let Some(u) = &payload.usdc {
        builder = builder.amount(Amount::usdc(Decimal::from_str(u)?)?);
    } else if let Some(s) = &payload.shares {
        builder = builder.amount(Amount::shares(Decimal::from_str(s)?)?);
    } else {
        anyhow::bail!("market order requires usdc or shares");
    }

    let order = builder.build().await?;
    let signed = client.sign(&signer, order).await?;
    let resp = client.post_order(signed).await?;
    Ok(resp)
}

async fn get_order_status(
    client: SharedClient,
    order_id: &str,
) -> anyhow::Result<OpenOrderResponse> {
    Ok(client.order(order_id).await?)
}

async fn cancel_order(
    client: SharedClient,
    order_id: &str,
) -> anyhow::Result<CancelOrdersResponse> {
    Ok(client.cancel_order(order_id).await?)
}

async fn cancel_all_orders(client: SharedClient) -> anyhow::Result<CancelOrdersResponse> {
    Ok(client.cancel_all_orders().await?)
}

fn parse_side(s: &str) -> anyhow::Result<Side> {
    match s.to_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => anyhow::bail!("invalid side '{s}'"),
    }
}
