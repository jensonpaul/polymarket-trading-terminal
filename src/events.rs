//! # Event Bus
//!
//! Every state-changing occurrence in the system is represented as an
//! [`AppEvent`].  The reducer [`crate::reducer::apply`] is the **only**
//! function that writes to [`crate::state::AppState`].
//!
//! ```text
//!  WS feed task   ──┐
//!  Orders poll    ──┤
//!  Trades poll    ──┤──▶  EventBus (mpsc)  ──▶  UI drain loop
//!  Rapid-sell     ──┤                               │
//!  Window clock   ──┘                               ▼
//!  Command handler                            reducer::apply()
//!                                                   │
//!                                                   ▼
//!                                             AppState (DashMap / ArcSwap)
//! ```
//!
//! The UI holds the `EventReceiver`.  Each frame it drains all pending events,
//! calls `apply()` on each one, and calls `ctx.request_repaint()` once if any
//! event was dirty.  No other code calls `request_repaint()` or `state.touch()`.

use tokio::sync::mpsc;

use crate::state::{LocalOrderStatus, NotificationKind, RapidSellState};
use polymarket_client_sdk_v2::clob::types::response::TradeResponse;

// ---------------------------------------------------------------------------
// Domain events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum AppEvent {
    // ------------------------------------------------------------------
    // Window lifecycle — emitted by spawn_window_clock
    // ------------------------------------------------------------------

    /// A new 5-minute window has opened.  The worker has already called
    /// `start_market_feed` before emitting this.
    WindowOpened { window_ts: u64, slug: String },

    /// A window was explicitly closed by the user via `UiCommand::CloseWindow`.
    WindowClosed { window_ts: u64 },

    // ------------------------------------------------------------------
    // Market data — emitted by the WS feed task
    // ------------------------------------------------------------------

    /// A price tick arrived from the WebSocket.  The reducer performs the
    /// `last_ts` deduplication check and updates the `ArcSwap<MarketPrices>`.
    PriceTick {
        window_ts: u64,
        up_price:  f64,
        down_price: f64,
        last_ts:   u64,
    },

    /// The feed's connection state changed (stale timeout, stream error,
    /// stream ended, or initial connected state after token-ID resolution).
    FeedStatusChanged {
        window_ts: u64,
        connected: bool,
        stale:     bool,
        error:     Option<String>,
    },

    // ------------------------------------------------------------------
    // Order lifecycle — emitted by command handlers and polling loop
    // ------------------------------------------------------------------

    /// An order was successfully placed (limit or market).
    OrderPlaced {
        order_id:         String,
        window_ts:        u64,
        side:             String,
        token:            String,
        price:            String,
        size:             String,
        rapid_sell_price: String,
        inline_sell_price: String,
    },

    /// An order's status changed (from the orders polling loop or CheckStatus).
    OrderStatusUpdated {
        order_id:                String,
        status:                  LocalOrderStatus,
        size_matched:            String,
        executed_price:          Option<String>,
        executed_size:           Option<String>,
        is_trade_fully_confirmed: bool,
        associate_trades:        Vec<String>,
    },

    /// An order was cancelled (individual or batch).
    OrderCancelled { order_id: String },

    // ------------------------------------------------------------------
    // Trade lifecycle — emitted by the trades polling loop
    // ------------------------------------------------------------------

    /// A trade record arrived for the current window.
    TradeReceived { trade: TradeResponse },

    // ------------------------------------------------------------------
    // Rapid-sell automation — emitted by spawn_rapid_sell_loop
    // ------------------------------------------------------------------

    /// The rapid-sell state machine transitioned for an order.
    /// Not emitted for the initial `Idle → InFlight` transition — that CAS
    /// is a direct write to prevent double-fire races (see worker.rs).
    RapidSellStateChanged {
        order_id:  String,
        new_state: RapidSellState,
    },

    /// A rapid-sell child order was placed.  The parent's `rapid_sell_size`
    /// is updated here rather than waiting for the next polling cycle.
    RapidSellOrderPlaced {
        parent_order_id: String,
        sell_order_id:   String,
        sell_amount:     String,
        window_ts:       u64,
        token:           String,
        price:           String,
    },

    // ------------------------------------------------------------------
    // Ephemeral UI notifications — forwarded to toast queue, not persisted
    // ------------------------------------------------------------------

    Notify {
        message: String,
        kind:    NotificationKind,
    },
}

// ---------------------------------------------------------------------------
// Channel types
// ---------------------------------------------------------------------------

/// Sender half — cloned into every task that produces events.
pub type EventBus = mpsc::Sender<AppEvent>;

/// Receiver half — held by the UI drain loop.
pub type EventReceiver = mpsc::Receiver<AppEvent>;

/// Construct the event channel.  Use a generous capacity; tasks are
/// non-blocking (`send().await` parks only when the channel is full).
pub fn event_channel(capacity: usize) -> (EventBus, EventReceiver) {
    mpsc::channel(capacity)
}