//! # Channel Messages
//!
//! One channel connects the UI and the worker:
//!
//! ```text
//!   UI  ──UiCommand──▶  Worker
//! ```
//!
//! Worker → UI communication is handled by the [`crate::events::EventBus`].
//! All state changes arrive as [`crate::events::AppEvent`]s and are applied
//! by [`crate::reducer::apply`] in the UI drain loop.

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
        side:        String,
        token:       String,
        price:       String,
        size:        String,
        rapid_price: String,
        window_ts:   u64,
    },

    /// Place a market (FOK/FAK) order.
    PlaceMarket {
        side:       String,
        token:      String,
        usdc:       Option<String>,
        shares:     Option<String>,
        order_type: Option<String>,
        window_ts:  u64,
    },

    /// Manual one-shot status refresh for a single order.
    CheckStatus { order_id: String, window_ts: u64 },

    /// Cancel a single open order.
    CancelIndividual { order_id: String, window_ts: u64 },

    /// Cancel all open orders visible in a given window.
    CancelAllInWindow { window_ts: u64 },

    /// Close a window and remove all its state.
    CloseWindow { window_ts: u64 },
}