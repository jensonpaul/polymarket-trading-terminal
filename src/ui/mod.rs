//! # UI Application
//!
//! `PolymarketDashboardApp` is the `eframe::App` implementation.
//!
//! ## Repaint strategy
//!
//!The UI owns the EventReceiver and drains all pending AppEvents
//!every frame.
//!Each event is applied through reducer::apply(). If any event dirties
//!AppState, the UI calls ctx.request_repaint() exactly once.
//!When idle, the UI falls back to
//!ctx.request_repaint_after(Duration::from_millis(250))
//!so countdown timers remain live without busy-looping.
//!
//! ## UI state vs shared state
//!
//! `PolymarketDashboardApp` owns:
//! - Form field strings (ephemeral user input)
//! - `Vec<WindowGroup>` — display-only; order IDs reference `AppState`
//! - `Vec<ToastNotification>` — ephemeral notifications
//!
//! The live order/trade/price data lives in `AppState` (shared with worker).

pub mod auth_gateway;
pub mod check_interval;
pub mod order_forms;
pub mod theme;
pub mod widgets;
pub mod window_matrix;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eframe::egui;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use rust_decimal::RoundingStrategy;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::events::{AppEvent, EventReceiver};
use crate::messages::UiCommand;
use crate::reducer::apply;
use crate::state::{
    NotificationKind, SharedAppState, ToastNotification, WindowGroup, slug_for_ts, stamp_5m,
};
use crate::ui::theme::{Theme, apply_dashboard_theme};
use crate::worker_config::{Queue, SharedPollConfig};
use crate::prediction::PredictionStore;

// ---------------------------------------------------------------------------
// Interval input helpers
// ---------------------------------------------------------------------------

pub struct IntervalInputs {
    pub orders: String,
    pub trades: String,
    pub rapid_sell: String,
}

impl IntervalInputs {
    pub fn get_mut(&mut self, queue: Queue) -> &mut String {
        match queue {
            Queue::Orders => &mut self.orders,
            Queue::Trades => &mut self.trades,
            Queue::RapidSell => &mut self.rapid_sell,
        }
    }

    pub fn get(&self, queue: Queue) -> &str {
        match queue {
            Queue::Orders => &self.orders,
            Queue::Trades => &self.trades,
            Queue::RapidSell => &self.rapid_sell,
        }
    }
}

// ---------------------------------------------------------------------------
// App struct
// ---------------------------------------------------------------------------

pub struct PolymarketDashboardApp {
    // ── shared state (single source of truth) ──────────────────────────────
    pub state: SharedAppState,

    // ── prediction state ───────────────────────────────────────────────────
    pub prediction_state: Arc<PredictionStore>,

    // ── channels ───────────────────────────────────────────────────────────
    pub cmd_tx: Sender<UiCommand>,
    pub event_rx: EventReceiver,

    // ── auth ───────────────────────────────────────────────────────────────
    pub bearer_token: String,
    pub is_authenticated: bool,

    // ── display windows (keys into AppState) ───────────────────────────────
    pub windows: Vec<WindowGroup>,

    // ── transient UI state ─────────────────────────────────────────────────
    pub notifications: Vec<ToastNotification>,
    pub auto_refresh_active: bool,
    pub poll_config: SharedPollConfig,
    pub interval_inputs: IntervalInputs,

    // ── limit order form ───────────────────────────────────────────────────
    pub limit_side_buy: bool,
    pub limit_token_up: bool,
    pub limit_price: String,
    pub limit_size: String,
    pub limit_rapid_price: String,

    // ── market order form ──────────────────────────────────────────────────
    pub market_side_buy: bool,
    pub market_token_up: bool,
    pub market_usdc: String,
    pub market_shares: String,
    pub market_use_usdc: bool,
    pub market_type_fok: bool,

    pub last_seen_version: u64,
}

impl PolymarketDashboardApp {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        cmd_tx: Sender<UiCommand>,
        event_rx: EventReceiver,
        state: SharedAppState,
        prediction_state: Arc<PredictionStore>,
        poll_config: SharedPollConfig,
    ) -> Self {
        let bearer_token = std::env::var("API_BEARER_TOKEN").unwrap_or_default();

        Self {
            state,
            prediction_state,
            cmd_tx,
            event_rx,
            bearer_token,
            is_authenticated: false,
            windows: Vec::new(),
            notifications: Vec::new(),
            auto_refresh_active: true,
            interval_inputs: IntervalInputs {
                orders: poll_config.get(Queue::Orders).to_string(),
                trades: poll_config.get(Queue::Trades).to_string(),
                rapid_sell: poll_config.get(Queue::RapidSell).to_string(),
            },
            poll_config,
            limit_side_buy: true,
            limit_token_up: true,
            limit_price: "0.50".into(),
            limit_size: "10".into(),
            limit_rapid_price: "0.00".into(),
            market_side_buy: true,
            market_token_up: true,
            market_usdc: "5.00".into(),
            market_shares: "0".into(),
            market_use_usdc: true,
            market_type_fok: true,
            last_seen_version: 0,
        }
    }

    pub fn push_toast(&mut self, msg: String, kind: NotificationKind) {
        let secs = if matches!(kind, NotificationKind::Error) { 8 } else { 4 };
        self.notifications.push(ToastNotification {
            message: msg,
            kind,
            expires_at: Instant::now() + Duration::from_secs(secs),
        });
    }

    /// Ensure a `WindowGroup` entry exists for `ts`.  Returns `true` if a new
    /// window was just inserted (caller may want to start its feed).
    fn ensure_window(&mut self, ts: u64) -> bool {
        if !self.windows.iter().any(|w| w.timestamp_5m == ts) {
            self.windows.insert(
                0,
                WindowGroup {
                    timestamp_5m: ts,
                    slug: slug_for_ts(ts),
                    is_expanded: true,
                    order_ids: Vec::new(),
                    market_prices: None,
                },
            );
            true
        } else {
            false
        }
    }

    /// Synchronise `WindowGroup::order_ids` from `AppState::orders`.
    ///
    /// Called once per frame.  Cost is O(orders) which is always small for a
    /// trading terminal (dozens, not millions).
    fn sync_window_order_ids(&mut self) {
        // First, clear all ID lists.
        for w in &mut self.windows {
            w.order_ids.clear();
        }

        // Then repopulate from the source of truth.
        for entry in self.state.orders.iter() {
            let order = entry.value();
            if let Some(w) = self
                .windows
                .iter_mut()
                .find(|w| w.timestamp_5m == order.window_ts)
            {
                w.order_ids.push(order.id.clone());
            } else {
                // Order belongs to a window that hasn't been created yet
                // (e.g. restored from a previous session).  Create it.
                self.windows.push(WindowGroup {
                    timestamp_5m: order.window_ts,
                    slug: slug_for_ts(order.window_ts),
                    is_expanded: true,
                    order_ids: vec![order.id.clone()],
                    market_prices: None,
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// eframe::App
// ---------------------------------------------------------------------------

impl eframe::App for PolymarketDashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        apply_dashboard_theme(ctx);

        // ------------------------------------------------------------------
        // Drain worker events (notifications; feed-started signals)
        // ------------------------------------------------------------------
        while let Ok(event) = self.event_rx.try_recv() {
            // Apply to shared state
            let dirty = apply(&self.state, &event);

            // React to specific events for UI-local state
            match &event {
                AppEvent::WindowOpened { window_ts, .. } => {
                    self.ensure_window(*window_ts);
                }
                AppEvent::WindowClosed { window_ts } => {
                    self.windows.retain(|w| w.timestamp_5m != *window_ts);
                }
                AppEvent::Notify { message, kind } => {
                    self.push_toast(message.clone(), kind.clone());
                }
                _ => {}
            }

            if dirty {
                ctx.request_repaint();
            }
        }

        // Expire toasts.
        self.notifications.retain(|n| Instant::now() < n.expires_at);

        // ------------------------------------------------------------------
        // Auth gate
        // ------------------------------------------------------------------
        if !self.is_authenticated {
            self.render_auth_gateway(ctx);
            return;
        }

        // ------------------------------------------------------------------
        // Window management
        // ------------------------------------------------------------------
        let current_ts = stamp_5m();

        // Keep window order-ID lists in sync with shared state.
        self.sync_window_order_ids();

        // ------------------------------------------------------------------
        // Compute countdown for top bar
        // ------------------------------------------------------------------
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let time_remaining = 300 - (now_secs % 300);

        // ------------------------------------------------------------------
        // Top bar
        // ------------------------------------------------------------------
        egui::TopBottomPanel::top("top_bar")
            .exact_height(48.0)
            .frame(
                egui::Frame::none()
                    .fill(Theme::BG_ELEVATED)
                    .stroke(egui::Stroke::new(1.0, Theme::BORDER))
                    .inner_margin(egui::Margin::symmetric(16, 0)),
            )
            .show(ctx, |ui| {
                // Vertically centre everything in the 48px bar.
                ui.vertical_centered_justified(|ui| {
                    ui.set_height(48.0);
                });

                let bar_rect = ui.max_rect();
                ui.allocate_ui_at_rect(bar_rect, |ui| {
                    ui.horizontal(|ui| {
                        // ── force vertical centering ──────────────────────
                        ui.add_space(0.0);
                        let v_pad = (bar_rect.height() - 22.0) / 2.0;
                        ui.add_space(v_pad); // won't work in horizontal — use the painter trick below
                    });
                });

                // egui horizontal panels don't support vertical centering via
                // add_space, so we paint directly.
                let painter = ui.painter();
                let _ = painter; // used below via ui.with_layout

                ui.with_layout(egui::Layout::left_to_right(egui::Align::Center), |ui| {
                    // ── Branding ─────────────────────────────────────────────
                    ui.add_space(4.0);
                    ui.label(
                        egui::RichText::new("◈")
                            .size(18.0)
                            .monospace()
                            .color(Theme::BLUE),
                    );
                    ui.add_space(4.0);
                    ui.label(
                        egui::RichText::new("POLYMARKET")
                            .strong()
                            .monospace()
                            .size(15.0)
                            .color(Theme::TEXT_PRIMARY),
                    );
                    ui.label(
                        egui::RichText::new("TERMINAL")
                            .monospace()
                            .size(15.0)
                            .color(Theme::TEXT_MUTED),
                    );

                    ui.add_space(12.0);
                    ui.separator();
                    ui.add_space(12.0);

                    // ── Live indicator ────────────────────────────────────────
                    let live_color = if self.auto_refresh_active {
                        Theme::BUY_GREEN
                    } else {
                        Theme::TEXT_MUTED
                    };
                    ui.label(
                        egui::RichText::new("●")
                            .monospace()
                            .size(10.0)
                            .color(live_color),
                    );
                    ui.add_space(4.0);
                    ui.label(
                        egui::RichText::new(if self.auto_refresh_active { "LIVE" } else { "PAUSED" })
                            .monospace()
                            .size(11.0)
                            .color(live_color),
                    );

                    // ── Right-aligned controls ────────────────────────────────
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.add_space(4.0);

                        // Countdown pill
                        let urgency_color = if time_remaining <= 30 {
                            Theme::SELL_RED
                        } else if time_remaining <= 60 {
                            Theme::WARNING
                        } else {
                            Theme::TEXT_MUTED
                        };

                        let pill_frame = egui::Frame::none()
                            .fill(Theme::BG_PANEL)
                            .stroke(egui::Stroke::new(1.0, urgency_color))
                            .corner_radius(6.0)
                            .inner_margin(egui::Margin::symmetric(10, 4));

                        pill_frame.show(ui, |ui| {
                            ui.label(
                                egui::RichText::new(format!(
                                    "⟳  {:02}:{:02}",
                                    time_remaining / 60,
                                    time_remaining % 60
                                ))
                                .monospace()
                                .size(13.0)
                                .color(urgency_color),
                            );
                        });

                        ui.add_space(12.0);
                        ui.separator();
                        ui.add_space(12.0);

                        // Window count badge
                        let win_count = self.windows.len();
                        ui.label(
                            egui::RichText::new(format!("{win_count} WINDOWS"))
                                .monospace()
                                .size(11.0)
                                .color(Theme::TEXT_MUTED),
                        );

                        ui.add_space(8.0);
                        ui.separator();
                        ui.add_space(8.0);

                        // Active orders count
                        let open_orders = self.state.orders.len();
                        ui.label(
                            egui::RichText::new(format!("{open_orders} ORDERS"))
                                .monospace()
                                .size(11.0)
                                .color(Theme::TEXT_MUTED),
                        );
                    });
                });
            });

        // ------------------------------------------------------------------
        // Toast notifications overlay
        // ------------------------------------------------------------------
        if !self.notifications.is_empty() {
            egui::Window::new("##notifications")
                .anchor(egui::Align2::RIGHT_TOP, [-12.0, 56.0])
                .resizable(false)
                .collapsible(false)
                .title_bar(false)
                .frame(
                    egui::Frame::none()
                        .fill(Theme::BG_ELEVATED)
                        .stroke(egui::Stroke::new(1.0, Theme::BORDER))
                        .corner_radius(8.0)
                        .inner_margin(12.0),
                )
                .show(ctx, |ui| {
                    for toast in &self.notifications {
                        let color = match toast.kind {
                            NotificationKind::Success => Theme::BUY_GREEN,
                            NotificationKind::Error => Theme::SELL_RED,
                            NotificationKind::Warning => Theme::WARNING,
                            _ => Theme::BLUE,
                        };
                        ui.colored_label(color, &toast.message);
                    }
                });
        }

        // ------------------------------------------------------------------
        // Main panel
        // ------------------------------------------------------------------
        egui::CentralPanel::default().show(ctx, |ui| {
            self.render_order_consoles(ui, current_ts);
            ui.add_space(12.0);
            self.render_lifecycle_matrix(ui);
        });

        // ------------------------------------------------------------------
        // Repaint strategy: event-driven (worker) + 4 FPS idle fallback
        // ------------------------------------------------------------------
        if self.auto_refresh_active {
            let current_version = self.state.version();
            if current_version != self.last_seen_version {
                self.last_seen_version = current_version;
                // state changed this frame — worker already called request_repaint(),
                // so no extra call needed here
            } else {
                // nothing changed; keep the timer alive for the countdown clock only
                ctx.request_repaint_after(Duration::from_millis(250));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub fn round_to_two_dp(value: &str) -> String {
    Decimal::from_str(value)
        .map(|d| d.round_dp_with_strategy(2, RoundingStrategy::ToZero).to_string())
        .unwrap_or_else(|_| value.to_owned())
}
