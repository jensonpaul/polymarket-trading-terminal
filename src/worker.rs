//! # Background Worker
//!
//! The worker is a pure command executor + polling engine.  It is the event
//! source for [`crate::state::AppState`] — it emits [`crate::events::AppEvent`]s
//! through the [`crate::events::EventBus`] and the reducer applies them.
//!
//! Architecture:
//!
//! ```text
//!   ┌────────────────────────────────────────────────────────┐
//!   │  PolymarketWorker::run()                               │
//!   │                                                        │
//!   │  ┌─ Task A: orders polling loop  ─────────────────┐   │
//!   │  │  polls open orders → emits OrderStatusUpdated  │   │
//!   │  └────────────────────────────────────────────────┘   │
//!   │                                                        │
//!   │  ┌─ Task B: trades polling loop  ─────────────────┐   │
//!   │  │  polls trades      → emits TradeReceived       │   │
//!   │  └────────────────────────────────────────────────┘   │
//!   │                                                        │
//!   │  ┌─ Task C: rapid-sell automation  ───────────────┐   │
//!   │  │  monitors fills    → emits RapidSell*          │   │
//!   │  └────────────────────────────────────────────────┘   │
//!   │                                                        │
//!   │  ┌─ Task D: window clock  ─────────────────────────┐  │
//!   │  │  owns 5-min cycle  → emits WindowOpened        │  │
//!   │  └────────────────────────────────────────────────┘  │
//!   │                                                        │
//!   │  ┌─ Main loop: UiCommand dispatcher  ─────────────┐   │
//!   │  │  PlaceLimit, PlaceMarket, Cancel, …            │   │
//!   │  └────────────────────────────────────────────────┘   │
//!   └────────────────────────────────────────────────────────┘
//! ```
//!
//! **Single exception to "no direct state writes":** the rapid-sell loop does
//! a direct `DashMap::get_mut` CAS to transition `Idle → InFlight`.  This
//! prevents a double-fire race that would exist if the transition were routed
//! through the event bus (the bus is async; a second loop tick could claim
//! the same candidate before the first event is applied).  Every other write
//! goes through [`crate::reducer::apply`].

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::signers::local::PrivateKeySigner;
use dashmap::DashMap;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::atomic::Ordering;
use tracing::{info, instrument, warn};

use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::{Client as ClobClient};
use polymarket_client_sdk_v2::clob::types::{
    Amount, OrderType, OrderStatusType, Side, TradeStatusType,
};
use polymarket_client_sdk_v2::clob::types::request::TradesRequest;
use polymarket_client_sdk_v2::clob::types::response::{
    CancelOrdersResponse, OpenOrderResponse, PostOrderResponse,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketBySlugRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market;
use polymarket_client_sdk_v2::types::U256;
pub use polymarket_client_sdk_v2::error::Error;

use crate::events::{AppEvent, EventBus};
use crate::market_data::start_market_feed;
use crate::messages::UiCommand;
use crate::state::{
    LocalOrderStatus, NotificationKind, RapidSellState, SharedAppState, TrackedOrder,
    slug_for_ts, stamp_5m,
};
use crate::worker_config::{Queue, SharedPollConfig};

// ---------------------------------------------------------------------------
// Module-level caches
// ---------------------------------------------------------------------------

lazy_static! {
    static ref MARKET_CACHE: DashMap<String, Market> = DashMap::new();
}

// ---------------------------------------------------------------------------
// Client type aliases
// ---------------------------------------------------------------------------

pub type AuthenticatedClient = ClobClient<Authenticated<Normal>>;
pub type SharedClient        = Arc<AuthenticatedClient>;

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub struct PolymarketWorker {
    pub cmd_rx:     Receiver<UiCommand>,
    pub bus:        EventBus,
    pub state:      SharedAppState,
    pub poll_config: SharedPollConfig,
    pub client:     SharedClient,
    pub signer:     Arc<PrivateKeySigner>,
}

impl PolymarketWorker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("PolymarketWorker: starting");

        let state = self.state.clone();

        // ------------------------------------------------------------------
        // Task A: orders polling loop
        // ------------------------------------------------------------------
        spawn_orders_polling_loop(
            self.client.clone(),
            Arc::clone(&state),
            self.poll_config.atomic(Queue::Orders),
            self.bus.clone(),
        );

        // ------------------------------------------------------------------
        // Task B: trades polling loop
        // ------------------------------------------------------------------
        spawn_trades_polling_loop(
            self.client.clone(),
            Arc::clone(&state),
            self.poll_config.atomic(Queue::Trades),
            self.bus.clone(),
        );

        // ------------------------------------------------------------------
        // Task C: rapid-sell automation loop
        // ------------------------------------------------------------------
        spawn_rapid_sell_loop(
            self.client.clone(),
            (*self.signer).clone(),
            Arc::clone(&state),
            self.poll_config.atomic(Queue::RapidSell),
            self.bus.clone(),
        );

        // ------------------------------------------------------------------
        // Task D: window clock (owns feed lifecycle)
        // ------------------------------------------------------------------
        spawn_window_clock(Arc::clone(&state), self.bus.clone());

        // ------------------------------------------------------------------
        // Main loop: UI command dispatcher
        // ------------------------------------------------------------------
        info!("PolymarketWorker: listening for commands");

        while let Some(cmd) = self.cmd_rx.recv().await {
            if !matches!(cmd, UiCommand::UpdatePollInterval { .. }) {
                info!(?cmd, "worker received command");
            }

            match cmd {
                UiCommand::InitializeClient { token } => {
                    info!(%token, "client already initialised at startup; token noted");
                }

                UiCommand::UpdatePollInterval { milliseconds, queue } => {
                    self.poll_config.set(queue, milliseconds);
                    info!(?queue, milliseconds, "poll interval updated");
                }

                UiCommand::PlaceLimit { side, token, price, size, rapid_price, window_ts } => {
                    let client    = self.client.clone();
                    let signer    = (*self.signer).clone();
                    let bus       = self.bus.clone();

                    tokio::spawn(async move {
                        let slug = slug_for_ts(stamp_5m());
                        let req  = LimitRequest {
                            side:  side.clone(),
                            token: token.clone(),
                            price: price.clone(),
                            size:  size.clone(),
                        };

                        match place_order_limit(client, signer, &req, &slug).await {
                            Ok(resp) => match parse_response(resp) {
                                Ok(order_id) => {
                                    let inline_sell_price = Decimal::from_str(&rapid_price)
                                        .ok()
                                        .filter(|p| *p > Decimal::ZERO)
                                        .map(|p| p.to_string())
                                        .unwrap_or_else(|| "0.10".into());

                                    let _ = bus.send(AppEvent::OrderPlaced {
                                        order_id,
                                        window_ts,
                                        side,
                                        token,
                                        price,
                                        size,
                                        rapid_sell_price: rapid_price,
                                        inline_sell_price,
                                    }).await;
                                    let _ = bus.send(AppEvent::Notify {
                                        message: "Limit Order Placed".into(),
                                        kind: NotificationKind::Success,
                                    }).await;
                                }
                                Err(e) => {
                                    let _ = bus.send(AppEvent::Notify {
                                        message: format!("Limit rejected: {e}"),
                                        kind: NotificationKind::Error,
                                    }).await;
                                }
                            },
                            Err(e) => {
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Limit transport error: {e}"),
                                    kind: NotificationKind::Error,
                                }).await;
                            }
                        }
                    });
                }

                UiCommand::PlaceMarket { side, token, usdc, shares, order_type, window_ts } => {
                    let client = self.client.clone();
                    let signer = (*self.signer).clone();
                    let bus    = self.bus.clone();

                    tokio::spawn(async move {
                        let slug = slug_for_ts(stamp_5m());
                        let req  = MarketRequest {
                            side:       side.clone(),
                            token:      token.clone(),
                            usdc:       usdc.clone(),
                            shares:     shares.clone(),
                            order_type: order_type.clone(),
                        };

                        match place_order_market(client, signer, &req, &slug).await {
                            Ok(resp) => match parse_response(resp) {
                                Ok(order_id) => {
                                    let _ = bus.send(AppEvent::OrderPlaced {
                                        order_id,
                                        window_ts,
                                        side,
                                        token,
                                        price: "Market".into(),
                                        size:  "Market".into(),
                                        rapid_sell_price:  "0.00".into(),
                                        inline_sell_price: "0.50".into(),
                                    }).await;
                                    let _ = bus.send(AppEvent::Notify {
                                        message: "Market Order Placed".into(),
                                        kind: NotificationKind::Success,
                                    }).await;
                                }
                                Err(e) => {
                                    let _ = bus.send(AppEvent::Notify {
                                        message: format!("Market rejected: {e}"),
                                        kind: NotificationKind::Error,
                                    }).await;
                                }
                            },
                            Err(e) => {
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Market transport error: {e}"),
                                    kind: NotificationKind::Error,
                                }).await;
                            }
                        }
                    });
                }

                UiCommand::CheckStatus { order_id, window_ts: _ } => {
                    let client = self.client.clone();
                    let state  = Arc::clone(&state);
                    let bus    = self.bus.clone();

                    tokio::spawn(async move {
                        if let Ok(info) = get_order_status(client, &order_id).await {
                            if let Some(event) = build_status_event(&state, &order_id, &info, false) {
                                let _ = bus.send(event).await;
                            }
                        }
                    });
                }

                UiCommand::CancelIndividual { order_id, window_ts: _ } => {
                    let client = self.client.clone();
                    let bus    = self.bus.clone();

                    tokio::spawn(async move {
                        match cancel_order(client, &order_id).await {
                            Ok(resp) => {
                                if resp.canceled.contains(&order_id) {
                                    let _ = bus.send(AppEvent::OrderCancelled {
                                        order_id,
                                    }).await;
                                    let _ = bus.send(AppEvent::Notify {
                                        message: "Order cancelled".into(),
                                        kind: NotificationKind::Success,
                                    }).await;
                                } else {
                                    let reason = resp
                                        .not_canceled
                                        .get(&order_id)
                                        .map(|s| s.as_str())
                                        .unwrap_or("unknown reason");
                                    let _ = bus.send(AppEvent::Notify {
                                        message: format!("Cancel rejected: {reason}"),
                                        kind: NotificationKind::Error,
                                    }).await;
                                }
                            }
                            Err(e) => {
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Cancel transport error: {e}"),
                                    kind: NotificationKind::Error,
                                }).await;
                            }
                        }
                    });
                }

                UiCommand::CancelAllInWindow { window_ts } => {
                    let client = self.client.clone();
                    let state  = Arc::clone(&state);
                    let bus    = self.bus.clone();

                    tokio::spawn(async move {
                        let local_ids: Vec<String> = state
                            .orders
                            .iter()
                            .filter(|e| e.value().window_ts == window_ts)
                            .map(|e| e.key().clone())
                            .collect();

                        match cancel_all_orders(client).await {
                            Ok(resp) => {
                                let count = resp.canceled.len();
                                for id in &local_ids {
                                    if resp.canceled.contains(id) {
                                        let _ = bus.send(AppEvent::OrderCancelled {
                                            order_id: id.clone(),
                                        }).await;
                                    }
                                }
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Batch cancel: {count} cancelled"),
                                    kind: NotificationKind::Success,
                                }).await;
                            }
                            Err(e) => {
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Batch cancel error: {e}"),
                                    kind: NotificationKind::Error,
                                }).await;
                            }
                        }
                    });
                }

                UiCommand::CloseWindow { window_ts } => {
                    let _ = self.bus.send(AppEvent::WindowClosed { window_ts }).await;
                }
            }
        }

        warn!("PolymarketWorker: command channel closed");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Task D: window clock — owns feed lifecycle
// ---------------------------------------------------------------------------

fn spawn_window_clock(state: SharedAppState, bus: EventBus) {
    tokio::spawn(async move {
        let mut last_window = stamp_5m();

        // Start the initial feed immediately on launch
        let slug = slug_for_ts(last_window);
        start_market_feed(last_window, slug.clone(), Arc::clone(&state), bus.clone()).await;
        let _ = bus.send(AppEvent::WindowOpened {
            window_ts: last_window,
            slug,
        }).await;

        loop {
            // Sleep until just past the next 5-minute boundary
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let secs_until_next = 300 - (now % 300) + 2;
            tokio::time::sleep(Duration::from_secs(secs_until_next)).await;

            let current = stamp_5m();
            if current == last_window {
                continue; // early wake — boundary not crossed yet
            }

            // Stop the old feed (WindowClosed cleanup is for user-initiated
            // closes; here we just stop the feed task itself)
            if let Some((_, handle)) = state.market_feeds.remove(&last_window) {
                handle.shutdown.notify_waiters();
                info!(last_window, "window clock: old feed stopped");
            }

            // Start the new feed
            let slug = slug_for_ts(current);
            start_market_feed(current, slug.clone(), Arc::clone(&state), bus.clone()).await;
            let _ = bus.send(AppEvent::WindowOpened {
                window_ts: current,
                slug,
            }).await;

            info!(current, "window clock: new window opened");
            last_window = current;
        }
    });
}

// ---------------------------------------------------------------------------
// Task A: orders polling loop
// ---------------------------------------------------------------------------

fn spawn_orders_polling_loop(
    client: SharedClient,
    state: SharedAppState,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
    bus: EventBus,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval   = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval   = make_interval(current_ms);
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
                if let Some(event) = build_status_event(&state, order_id, &info, true) {
                    let _ = bus.send(event).await;
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Task B: trades polling loop
// ---------------------------------------------------------------------------

fn spawn_trades_polling_loop(
    client: SharedClient,
    state: SharedAppState,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
    bus: EventBus,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval   = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval   = make_interval(current_ms);
                info!(current_ms, "trades poll interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            interval.tick().await;

            let slug = slug_for_ts(stamp_5m());

            let condition_id = {
                MARKET_CACHE.get(&slug).and_then(|m| m.condition_id)
            };

            let Some(condition_id) = condition_id else {
                continue;
            };

            let mut req    = TradesRequest::builder().build();
            req.market     = Some(condition_id);

            match client.trades(&req, None).await {
                Ok(page) => {
                    for trade in page.data {
                        let _ = bus.send(AppEvent::TradeReceived { trade }).await;
                    }
                }
                Err(e) => {
                    tracing::error!(%slug, error=%e, "trades poll failed");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Task C: rapid-sell automation loop
// ---------------------------------------------------------------------------

fn spawn_rapid_sell_loop(
    client: SharedClient,
    signer: PrivateKeySigner,
    state: SharedAppState,
    interval_cell: Arc<std::sync::atomic::AtomicU64>,
    bus: EventBus,
) {
    tokio::spawn(async move {
        let mut current_ms = interval_cell.load(Ordering::Relaxed);
        let mut interval   = make_interval(current_ms);

        loop {
            let latest = interval_cell.load(Ordering::Relaxed);
            if latest != current_ms {
                current_ms = latest;
                interval   = make_interval(current_ms);
                info!(current_ms, "rapid-sell interval updated");
            }

            if current_ms == 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }

            interval.tick().await;

            let now = Instant::now();

            let candidates: Vec<TrackedOrder> = state
                .orders
                .iter()
                .filter_map(|entry| {
                    let order = entry.value();

                    if !order.side.eq_ignore_ascii_case("buy") {
                        return None;
                    }

                    match order.status {
                        LocalOrderStatus::PartiallyFilled { .. }
                        | LocalOrderStatus::FullyFilled
                        | LocalOrderStatus::TradeOpen
                        | LocalOrderStatus::TradeConfirmed => {}
                        _ => return None,
                    }

                    let matched      = Decimal::from_str(&order.size_matched).unwrap_or_default();
                    let already_sold = Decimal::from_str(&order.rapid_sell_size).unwrap_or_default();
                    let remaining    = (matched - already_sold).max(Decimal::ZERO);

                    if remaining < dec!(5) {
                        return None;
                    }

                    match &order.rapid_sell_state {
                        RapidSellState::Idle => Some(order.clone()),
                        RapidSellState::RetryScheduled { retry_at, .. }
                            if *retry_at <= now => Some(order.clone()),
                        _ => None,
                    }
                })
                .collect();

            for order in candidates {
                let matched      = Decimal::from_str(&order.size_matched).unwrap_or_default();
                let already_sold = Decimal::from_str(&order.rapid_sell_size).unwrap_or_default();
                let sell_price   = Decimal::from_str(&order.rapid_sell_price).unwrap_or_default();
                let sell_amount  = (matched - already_sold).max(Decimal::ZERO);

                if sell_price <= Decimal::ZERO || sell_amount < Decimal::from(5) {
                    continue;
                }

                // ── CAS lock: Idle / RetryScheduled → InFlight ─────────────
                // Direct write intentional — prevents double-fire race.
                // See module-level doc comment.
                let acquired = {
                    if let Some(mut o) = state.orders.get_mut(&order.id) {
                        match o.rapid_sell_state {
                            RapidSellState::Idle => {
                                o.rapid_sell_state = RapidSellState::InFlight {
                                    attempt:    0,
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

                let attempt = {
                    state.orders.get(&order.id).map(|o| match &o.rapid_sell_state {
                        RapidSellState::InFlight { attempt, .. } => *attempt,
                        _ => 0,
                    }).unwrap_or(0)
                };

                let client     = client.clone();
                let signer     = signer.clone();
                let bus        = bus.clone();
                let parent_id  = order.id.clone();
                let token      = order.token.clone();
                let window_ts  = order.window_ts;
                let rapid_price = order.rapid_sell_price.clone();

                tokio::spawn(async move {
                    let slug = slug_for_ts(stamp_5m());
                    let req  = LimitRequest {
                        side:  "sell".into(),
                        token: token.clone(),
                        price: rapid_price.clone(),
                        size:  sell_amount.to_string(),
                    };

                    match place_order_limit(client, signer, &req, &slug).await {
                        Ok(resp) => match parse_response(resp) {
                            Ok(new_id) => {
                                let _ = bus.send(AppEvent::RapidSellOrderPlaced {
                                    parent_order_id: parent_id.clone(),
                                    sell_order_id:   new_id,
                                    sell_amount:     sell_amount.to_string(),
                                    window_ts,
                                    token:           token.clone(),
                                    price:           rapid_price.clone(),
                                }).await;
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!(
                                        "Rapid Sell placed: {sell_amount} {token} @ {rapid_price}"
                                    ),
                                    kind: NotificationKind::Success,
                                }).await;
                            }
                            Err(e) => {
                                let new_state = compute_retry_state(attempt, e.to_string());
                                let _ = bus.send(AppEvent::RapidSellStateChanged {
                                    order_id:  parent_id,
                                    new_state,
                                }).await;
                                let _ = bus.send(AppEvent::Notify {
                                    message: format!("Rapid Sell rejected: {e}"),
                                    kind: NotificationKind::Error,
                                }).await;
                            }
                        },
                        Err(e) => {
                            let new_state = compute_retry_state(attempt, e.to_string());
                            let _ = bus.send(AppEvent::RapidSellStateChanged {
                                order_id:  parent_id,
                                new_state,
                            }).await;
                            let _ = bus.send(AppEvent::Notify {
                                message: format!("Rapid Sell transport error: {e}"),
                                kind: NotificationKind::Error,
                            }).await;
                        }
                    }
                });
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Status event builder
// (replaces apply_order_status_update — builds event payload, emits nothing)
// ---------------------------------------------------------------------------

/// Build an [`AppEvent::OrderStatusUpdated`] from an [`OpenOrderResponse`].
///
/// Returns `None` if the order is not in `AppState::orders` (already removed).
fn build_status_event(
    state: &crate::state::AppState,
    order_id: &str,
    info: &OpenOrderResponse,
    check_trades: bool,
) -> Option<AppEvent> {
    let tolerance      = dec!(0.005);
    let is_fully_filled = info.size_matched >= info.original_size * (dec!(1.0) - tolerance);

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

    let status = match &info.status {
        OrderStatusType::Live => {
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
                    LocalOrderStatus::TradeConfirmed
                } else {
                    LocalOrderStatus::FullyFilled
                }
            } else {
                LocalOrderStatus::PartiallyFilled {
                    filled: info.size_matched.to_string(),
                }
            }
        }
        OrderStatusType::Canceled => LocalOrderStatus::Canceled,
        OrderStatusType::Unknown(reason) => {
            if reason == "INVALID" {
                LocalOrderStatus::Canceled
            } else {
                warn!(%reason, %order_id, "unknown order status");
                LocalOrderStatus::Canceled
            }
        }
        _ => {
            warn!(%order_id, "non-exhaustive order status variant");
            LocalOrderStatus::Canceled
        }
    };

    let size_matched = info
        .size_matched
        .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
        .to_string();

    let (executed_price, executed_size) = if info.size_matched > Decimal::ZERO {
        (
            Some(
                info.price
                    .round_dp_with_strategy(4, rust_decimal::RoundingStrategy::ToZero)
                    .to_string(),
            ),
            Some(size_matched.clone()),
        )
    } else {
        (None, None)
    };

    // Only emit if the order still exists
    if !state.orders.contains_key(order_id) {
        return None;
    }

    Some(AppEvent::OrderStatusUpdated {
        order_id:                order_id.to_owned(),
        status,
        size_matched,
        executed_price,
        executed_size,
        is_trade_fully_confirmed: is_trade_confirmed,
        associate_trades:        info.associate_trades.clone(),
    })
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

pub const RAPID_SELL_MAX_ATTEMPTS: u32 = 8;

pub fn rapid_sell_backoff(attempt: u32) -> Duration {
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

fn compute_retry_state(attempt: u32, reason: String) -> RapidSellState {
    if attempt >= RAPID_SELL_MAX_ATTEMPTS {
        return RapidSellState::PermanentlyFailed { attempts: attempt, reason };
    }
    RapidSellState::RetryScheduled {
        attempt:  attempt + 1,
        retry_at: Instant::now() + rapid_sell_backoff(attempt),
        reason,
    }
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
    side:  String,
    token: String,
    price: String,
    size:  String,
}

struct MarketRequest {
    side:       String,
    token:      String,
    usdc:       Option<String>,
    shares:     Option<String>,
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
    if let Some(m) = MARKET_CACHE.get(slug) {
        return Ok(m.clone());
    }
    let req    = MarketBySlugRequest::builder().slug(slug).build();
    let market = client.market_by_slug(&req).await?;
    MARKET_CACHE.clear();
    MARKET_CACHE.insert(slug.to_string(), market.clone());
    Ok(market)
}

async fn place_order_limit(
    client: SharedClient,
    signer: PrivateKeySigner,
    payload: &LimitRequest,
    slug: &str,
) -> anyhow::Result<PostOrderResponse> {
    let _t = Timer::start("place_limit_total");

    let gamma = GammaClient::default();
    let ids   = get_or_fetch_token_ids(&gamma, slug).await?;
    anyhow::ensure!(ids.len() >= 2, "no token IDs for slug {slug}");

    let token_id = if payload.token.eq_ignore_ascii_case("up") {
        U256::from_str(&ids[0])?
    } else if payload.token.eq_ignore_ascii_case("down") {
        U256::from_str(&ids[1])?
    } else {
        anyhow::bail!("invalid token '{}'; must be 'up' or 'down'", payload.token);
    };

    let price = Decimal::from_str(&payload.price)?;
    let size  = Decimal::from_str(&payload.size)?;
    let side  = parse_side(&payload.side)?;

    let order  = client.limit_order().token_id(token_id).size(size).price(price).side(side).build().await?;
    let signed = timed!("sign_limit", { client.sign(&signer, order).await? });
    let resp   = timed!("post_limit", { client.post_order(signed).await? });
    Ok(resp)
}

async fn place_order_market(
    client: SharedClient,
    signer: PrivateKeySigner,
    payload: &MarketRequest,
    slug: &str,
) -> anyhow::Result<PostOrderResponse> {
    let gamma = GammaClient::default();
    let ids   = get_or_fetch_token_ids(&gamma, slug).await?;
    anyhow::ensure!(ids.len() >= 2, "no token IDs for slug {slug}");

    let token_id = match payload.token.to_lowercase().as_str() {
        "up"   => U256::from_str(&ids[0])?,
        "down" => U256::from_str(&ids[1])?,
        _      => anyhow::bail!("invalid token '{}'", payload.token),
    };
    let side       = parse_side(&payload.side)?;
    let order_type = match payload.order_type.as_deref() {
        Some("FAK") => OrderType::FAK,
        _           => OrderType::FOK,
    };

    let mut builder = client.market_order().token_id(token_id).side(side).order_type(order_type);
    if let Some(u) = &payload.usdc {
        builder = builder.amount(Amount::usdc(Decimal::from_str(u)?)?);
    } else if let Some(s) = &payload.shares {
        builder = builder.amount(Amount::shares(Decimal::from_str(s)?)?);
    } else {
        anyhow::bail!("market order requires usdc or shares");
    }

    let order  = builder.build().await?;
    let signed = client.sign(&signer, order).await?;
    let resp   = client.post_order(signed).await?;
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
        "buy"  => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _      => anyhow::bail!("invalid side '{s}'"),
    }
}