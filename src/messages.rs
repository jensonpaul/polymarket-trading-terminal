//! # Channel Messages
//!
//! Two one-directional channels connect the UI and the worker:
//!
//! ```text
//!   UI  ──UiCommand──▶  Worker
//!   UI  ◀─WorkerEvent──  Worker
//! ```
//!
//! **Order/trade state is NOT carried through these channels.**
//! It lives in [`crate::state::AppState`] (shared `Arc<DashMap>`).
//!
//! These messages carry:
//! - User intentions that require async I/O (`PlaceLimit`, `CancelIndividual`, …)
//! - Ephemeral notifications that are relevant only to the current frame
//!   (`WorkerEvent::Notify`)
//! - Lifecycle signals (`MarketFeedStarted`, `MarketFeedStopped`)

use crate::worker_config::Queue;

// ---------------------------------------------------------------------------
// UI → Worker
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum UiCommand {
    /// Trigger CLOB client initialization (called once after auth).
    InitializeClient { token: String },

    /// User changed a polling interval in the control panel.
    UpdatePollInterval { milliseconds: u64, queue: Queue },

    /// Place a GTC limit order.
    PlaceLimit {
        side: String,
        token: String,
        price: String,
        size: String,
        rapid_price: String,
        window_ts: u64,
    },

    /// Place a market (FOK/FAK) order.
    PlaceMarket {
        side: String,
        token: String,
        usdc: Option<String>,
        shares: Option<String>,
        order_type: Option<String>,
        window_ts: u64,
    },

    /// Manual one-shot status refresh for a single order.
    CheckStatus { order_id: String, window_ts: u64 },

    /// Cancel a single open order.
    CancelIndividual { order_id: String, window_ts: u64 },

    /// Cancel all open orders visible in a given window.
    CancelAllInWindow { window_ts: u64 },

    /// Start the live market-price feed for a 5-min window.
    EnsureFeed { window_ts: u64, slug: String },

    /// Stop and discard the feed for a window.
    ReleaseFeed { window_ts: u64 },

    CloseWindow { window_ts: u64 },
}

// ---------------------------------------------------------------------------
// Worker → UI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum WorkerEvent {
    /// Display a transient toast notification.
    Notify {
        message: String,
        kind: crate::state::NotificationKind,
    },

    /// The market feed for `window_ts` is ready; the
    /// [`crate::state::SharedMarketPrices`] has been written into
    /// `AppState::market_prices[window_ts]`.
    MarketFeedStarted { window_ts: u64 },

    WindowClosed { window_ts: u64 },
}
