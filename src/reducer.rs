//! # State Reducer
//!
//! [`apply`] is the **sole writer** of [`crate::state::AppState`].
//!
//! Every [`crate::events::AppEvent`] passes through here.  Nothing else
//! mutates shared state (with the single exception of the rapid-sell CAS
//! lock in the worker — see its inline comment).
//!
//! Returns `true` if the event dirtied state and the UI should repaint.

use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use tracing::warn;

use crate::events::AppEvent;
use crate::state::{
    AppState, LocalOrderStatus, RapidSellState, TrackedOrder,
};

pub fn apply(state: &AppState, event: &AppEvent) -> bool {
    match event {
        // ------------------------------------------------------------------
        // Window lifecycle
        // ------------------------------------------------------------------

        AppEvent::WindowOpened { .. } => {
            // market_prices and market_feeds entries are already inserted by
            // start_market_feed() before this event is emitted.  We only need
            // to bump the version so the UI knows to call ensure_window().
            state.touch();
            true
        }

        AppEvent::WindowClosed { window_ts } => {
            // Stop the feed
            if let Some((_, handle)) = state.market_feeds.remove(window_ts) {
                handle.shutdown.notify_waiters();
            }

            // Remove market snapshot
            state.market_prices.remove(window_ts);

            // Remove orders + their associated trades
            let order_ids: Vec<String> = state
                .orders
                .iter()
                .filter(|e| e.value().window_ts == *window_ts)
                .map(|e| e.key().clone())
                .collect();

            for order_id in order_ids {
                if let Some((_, order)) = state.orders.remove(&order_id) {
                    for trade_id in &order.associate_trades {
                        state.trades.remove(trade_id);
                    }
                }
            }

            state.touch();
            true
        }

        // ------------------------------------------------------------------
        // Market data
        // ------------------------------------------------------------------

        AppEvent::PriceTick { window_ts, up_price, down_price, last_ts } => {
            let Some(shared) = state.market_prices.get(window_ts) else {
                return false;
            };
            let current = shared.load();
            if *last_ts <= current.last_ts {
                return false; // stale / out-of-order tick
            }
            let mut snap = current.as_ref().clone();
            snap.up_price  = *up_price;
            snap.down_price = *down_price;
            snap.last_ts   = *last_ts;
            snap.connected = true;
            snap.stale     = false;
            snap.error     = None;
            shared.store(Arc::new(snap));
            state.touch();
            true
        }

        AppEvent::FeedStatusChanged { window_ts, connected, stale, error } => {
            let Some(shared) = state.market_prices.get(window_ts) else {
                return false;
            };
            let mut snap = shared.load().as_ref().clone();
            snap.connected = *connected;
            snap.stale     = *stale;
            snap.error     = error.as_deref().map(Arc::from);
            shared.store(Arc::new(snap));
            state.touch();
            true
        }

        // ------------------------------------------------------------------
        // Order lifecycle
        // ------------------------------------------------------------------

        AppEvent::OrderPlaced {
            order_id, window_ts, side, token, price, size,
            rapid_sell_price, inline_sell_price,
        } => {
            let order = TrackedOrder {
                id:                       order_id.clone(),
                side:                     side.clone(),
                token:                    token.clone(),
                price:                    price.clone(),
                size:                     size.clone(),
                executed_price:           None,
                executed_size:            None,
                status:                   LocalOrderStatus::Open,
                size_matched:             "0".into(),
                inline_sell_price:        inline_sell_price.clone(),
                inline_sell_size:         "0".into(),
                inline_sell_market_type:  "FAK".into(),
                rapid_sell_price:         rapid_sell_price.clone(),
                rapid_sell_size:          "0".into(),
                rapid_sell_state:         RapidSellState::Idle,
                is_trade_fully_confirmed: false,
                associate_trades:         vec![],
                open_order_response:      None,
                window_ts:                *window_ts,
            };
            state.orders.insert(order_id.clone(), order);
            state.touch();
            true
        }

        AppEvent::OrderStatusUpdated {
            order_id, status, size_matched,
            executed_price, executed_size,
            is_trade_fully_confirmed, associate_trades,
        } => {
            if let Some(mut o) = state.orders.get_mut(order_id) {
                o.status                   = status.clone();
                o.size_matched             = size_matched.clone();
                o.executed_price           = executed_price.clone();
                o.executed_size            = executed_size.clone();
                o.is_trade_fully_confirmed = *is_trade_fully_confirmed;
                o.associate_trades         = associate_trades.clone();

                // Keep inline_sell_size in sync with the fill amount
                if let Some(ep) = executed_price {
                    o.inline_sell_price = ep.clone();
                }
                if let Some(es) = executed_size {
                    o.inline_sell_size = es.clone();
                }

                state.touch();
                true
            } else {
                false
            }
        }

        AppEvent::OrderCancelled { order_id } => {
            if let Some(mut o) = state.orders.get_mut(order_id) {
                o.status = LocalOrderStatus::Canceled;
                state.touch();
                true
            } else {
                false
            }
        }

        // ------------------------------------------------------------------
        // Trade lifecycle
        // ------------------------------------------------------------------

        AppEvent::TradeReceived { trade } => {
            state.trades.insert(trade.id.clone(), trade.clone());
            state.touch();
            true
        }

        // ------------------------------------------------------------------
        // Rapid-sell automation
        // ------------------------------------------------------------------

        AppEvent::RapidSellStateChanged { order_id, new_state } => {
            if let Some(mut o) = state.orders.get_mut(order_id) {
                o.rapid_sell_state = new_state.clone();
                state.touch();
                true
            } else {
                false
            }
        }

        AppEvent::RapidSellOrderPlaced {
            parent_order_id, sell_order_id,
            sell_amount, window_ts,
            token, price,
        } => {
            // Insert the child sell order
            let sell_order = TrackedOrder {
                id:                       sell_order_id.clone(),
                side:                     "sell".into(),
                token:                    token.clone(),
                price:                    price.clone(),
                size:                     sell_amount.clone(),
                executed_price:           None,
                executed_size:            None,
                status:                   LocalOrderStatus::Open,
                size_matched:             "0".into(),
                inline_sell_price:        "0".into(),
                inline_sell_size:         "0".into(),
                inline_sell_market_type:  "FAK".into(),
                rapid_sell_price:         "0".into(),
                rapid_sell_size:          "0".into(),
                rapid_sell_state:         RapidSellState::Idle,
                is_trade_fully_confirmed: false,
                associate_trades:         vec![],
                open_order_response:      None,
                window_ts:                *window_ts,
            };
            state.orders.insert(sell_order_id.clone(), sell_order);

            // Update parent's rapid_sell_size and state
            if let Some(mut parent) = state.orders.get_mut(parent_order_id) {
                let sold_before = Decimal::from_str(&parent.rapid_sell_size)
                    .unwrap_or_default();
                let added = Decimal::from_str(sell_amount).unwrap_or_default();
                parent.rapid_sell_size = (sold_before + added).to_string();

                let matched = Decimal::from_str(&parent.size_matched)
                    .unwrap_or_default();
                let remaining = (matched
                    - Decimal::from_str(&parent.rapid_sell_size).unwrap_or_default())
                .max(Decimal::ZERO);

                parent.rapid_sell_state = if remaining >= Decimal::from(5) {
                    RapidSellState::Idle
                } else {
                    RapidSellState::Completed
                };
            }

            state.touch();
            true
        }

        // ------------------------------------------------------------------
        // Ephemeral — no state change
        // ------------------------------------------------------------------

        AppEvent::Notify { .. } => false,
    }
}