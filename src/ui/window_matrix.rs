//! # Window Lifecycle Matrix
//!
//! Renders the grid of 5-minute windows and their order cards.
//!
//! The UI reads order data **directly** from `AppState::orders` (a DashMap)
//! rather than from a local copy.  Inline sell fields (`inline_sell_price`,
//! `inline_sell_size`) are mutated through `DashMap::get_mut` — safe because
//! only the UI thread writes these fields.

use eframe::egui;

use crate::messages::UiCommand;
use crate::state::{LocalOrderStatus, SharedAppState, TrackedOrder, WindowGroup};
use crate::ui::theme::Theme;
use crate::ui::widgets::{compact_input, panel_frame, themed_button};
use crate::ui::PolymarketDashboardApp;

impl PolymarketDashboardApp {
    pub fn render_lifecycle_matrix(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading(
                egui::RichText::new("📊 WINDOW LIFECYCLE MATRIX").monospace().color(Theme::TEXT_PRIMARY),
            );
            ui.separator();
            ui.checkbox(&mut self.auto_refresh_active, "LIVE");
        });

        ui.add_space(10.0);

        //let mut windows_to_remove: Vec<(usize, u64, Vec<String>)> = Vec::new();

        for (w_idx, window) in self.windows.iter_mut().enumerate() {
            panel_frame().show(ui, |ui| {
                // ----------------------------------------------------------------
                // Window header
                // ----------------------------------------------------------------
                ui.horizontal(|ui| {
                    if themed_button(
                        ui,
                        if window.is_expanded { "▼" } else { "▶" },
                        Theme::BLUE_BG,
                        Theme::BLUE,
                    )
                    .clicked()
                    {
                        window.is_expanded = !window.is_expanded;
                    }

                    let order_count = window.order_ids.len();
                    ui.label(
                        egui::RichText::new(format!(
                            "WINDOW {} | {} | ORDERS {}",
                            window.timestamp_5m, window.slug, order_count,
                        ))
                        .strong()
                        .monospace()
                        .color(Theme::TEXT_PRIMARY),
                    );

                    ui.with_layout(
                        egui::Layout::right_to_left(egui::Align::Center),
                        |ui| {
                            if themed_button(ui, "CLOSE", Theme::SELL_RED_BG, Theme::SELL_RED)
                                .clicked()
                            {
                                //windows_to_remove.push((w_idx, window.timestamp_5m, window.order_ids.clone()));
                                let _ = self.cmd_tx.try_send(UiCommand::CloseWindow {
                                    window_ts: window.timestamp_5m,
                                });
                            }

                            if themed_button(
                                ui,
                                "CANCEL ALL",
                                Theme::SELL_RED_BG,
                                Theme::SELL_RED,
                            )
                            .clicked()
                            {
                                let _ = self.cmd_tx.try_send(UiCommand::CancelAllInWindow {
                                    window_ts: window.timestamp_5m,
                                });
                            }
                        },
                    );
                });

                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    // ----------------------------------------------------------------
                    // BTC metrics ticker
                    // ----------------------------------------------------------------
                    if let Some(pred) = self.prediction_state.signals.get(&window.timestamp_5m) {
                        if let Some(btc) = pred.btc.as_ref() {
                            if !btc.origin_price.is_zero() {
                                let origin_f = btc.origin_price.to_string();
                                let current_f = btc.current_price.to_string();
                                let dist = btc.distance_from_origin_pct;
                                let (arrow, dist_color) = if dist >= 0.0 {
                                    ("▲", Theme::BUY_GREEN)
                                } else {
                                    ("▼", Theme::SELL_RED)
                                };
                                let distance_from_origin = (btc.current_price - btc.origin_price).abs();

                                panel_frame().show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(
                                            egui::RichText::new(format!("${:.2}", btc.origin_price))
                                                .monospace()
                                                .color(Theme::TEXT_PRIMARY),
                                        );
                                        ui.separator();
                                        ui.label(
                                            egui::RichText::new(format!("${:.2}", btc.current_price))
                                                .monospace()
                                                .color(Theme::WARNING),
                                        );
                                        ui.separator();
                                        ui.label(
                                            egui::RichText::new(format!("${:.2}", distance_from_origin))
                                                .monospace()
                                                .strong()
                                                .color(dist_color),
                                        );
                                        ui.separator();
                                        ui.label(
                                            egui::RichText::new(format!("{arrow} {:.4}%", dist * 100.0))
                                                .monospace()
                                                .strong()
                                                .color(dist_color),
                                        );
                                    });
                                });
                            }
                        }
                    }

                    // ----------------------------------------------------------------
                    // Market price ticker
                    // ----------------------------------------------------------------
                    if let Some(prices_arc) = self
                        .state
                        .market_prices
                        .get(&window.timestamp_5m)
                        .map(|e| e.value().clone())
                    {
                        let snap = prices_arc.load();
                        let up_c = snap.up_price * 100.0;
                        let down_c = snap.down_price * 100.0;

                        panel_frame()
                            .fill(Theme::BUY_GREEN_BG)
                            .stroke(egui::Stroke::new(1.0, Theme::BUY_GREEN))
                            .show(ui, |ui| {
                                ui.label(
                                    egui::RichText::new(format!("▲ UP {up_c:.1}¢"))
                                        .strong()
                                        .monospace()
                                        .color(Theme::TEXT_PRIMARY),
                                );
                            });

                        panel_frame()
                            .fill(Theme::SELL_RED_BG)
                            .stroke(egui::Stroke::new(1.0, Theme::SELL_RED))
                            .show(ui, |ui| {
                                ui.label(
                                    egui::RichText::new(format!("▼ DOWN {down_c:.1}¢"))
                                        .strong()
                                        .monospace()
                                        .color(Theme::TEXT_PRIMARY),
                                );
                            });

                        if snap.stale {
                            ui.colored_label(Theme::WARNING, "STALE");
                        }
                        if !snap.connected {
                            ui.colored_label(Theme::SELL_RED, "DISCONNECTED");
                        }
                    }
                });

                // ----------------------------------------------------------------
                // Strategy signals
                // ----------------------------------------------------------------
                if let Some(pred) =
                    self.prediction_state.signals.get(&window.timestamp_5m)
                {
                    if !pred.signals.is_empty() {
                        ui.add_space(6.0);

                        ui.columns(5, |cols| {
                            for (col, signal) in cols.iter_mut().zip(pred.signals.iter()) {
                                use crate::prediction::signals::{PredictionSide, SignalType};

                                let (side_label, side_color) = match signal.signal_type {
                                    SignalType::NoTrade => (
                                        "NO TRADE",
                                        Theme::TEXT_MUTED,
                                    ),
                                    _ => match signal.side {
                                        PredictionSide::Up => ("▲ UP", Theme::BUY_GREEN),
                                        PredictionSide::Down => ("▼ DOWN", Theme::SELL_RED),
                                    },
                                };

                                panel_frame().show(col, |ui| {
                                    ui.horizontal_wrapped(|ui| {
                                        // Strategy name badge
                                        ui.label(
                                            egui::RichText::new(signal.strategy_name.to_uppercase())
                                                .monospace()
                                                .strong()
                                                .color(Theme::BLUE),
                                        );

                                        ui.separator();

                                        // Side
                                        ui.label(
                                            egui::RichText::new(side_label)
                                                .monospace()
                                                .strong()
                                                .color(side_color),
                                        );

                                        ui.separator();

                                        // Confidence (omit when zero — strategy has no model yet)
                                        if signal.confidence > 0.0 {
                                            ui.label(
                                                egui::RichText::new(format!(
                                                    "CONF {:.1}%",
                                                    signal.confidence * 100.0,
                                                ))
                                                .monospace()
                                                .color(Theme::TEXT_PRIMARY),
                                            );

                                            //ui.separator();
                                        }

                                        /*
                                        // Entry price
                                        if !signal.target_entry.is_zero() {
                                            ui.label(
                                                egui::RichText::new(format!(
                                                    "ENTRY {}",
                                                    signal.target_entry,
                                                ))
                                                .monospace()
                                                .color(Theme::TEXT_PRIMARY),
                                            );
                                        }
                                        */
                                    });

                                    /*
                                    // Reason — full width, muted
                                    ui.label(
                                        egui::RichText::new(&signal.reason)
                                            .monospace()
                                            .small()
                                            .color(Theme::TEXT_MUTED),
                                    );
                                    */
                                    ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
                                        ui.label(
                                            egui::RichText::new(&signal.reason)
                                                .monospace()
                                                .strong()
                                                .color(Theme::TEXT_PRIMARY),
                                        );
                                    });
                                });
                            }
                        });
                    }
                }

                // ----------------------------------------------------------------
                // Order columns
                // ----------------------------------------------------------------
                if window.is_expanded {
                    ui.add_space(12.0);

                    let mut bought = Vec::new();
                    let mut sold = Vec::new();
                    let mut others = Vec::new();

                    for id in &window.order_ids {
                        let Some(order) = self.state.orders.get(id) else {
                            continue;
                        };
                        let terminal = matches!(
                            order.status,
                            LocalOrderStatus::Canceled | LocalOrderStatus::Failed(_)
                        );
                        if terminal {
                            others.push(id.clone());
                        } else if order.side.eq_ignore_ascii_case("buy") {
                            bought.push(id.clone());
                        } else if order.side.eq_ignore_ascii_case("sell") {
                            sold.push(id.clone());
                        } else {
                            others.push(id.clone());
                        }
                    }

                    let window_ts = window.timestamp_5m;
                    let cmd_tx = self.cmd_tx.clone();
                    let state = self.state.clone();

                    ui.columns(3, |cols| {
                        Self::render_order_column(
                            &mut cols[0],
                            //"🟢 BOUGHT",
                            "BOUGHT",
                            &bought,
                            window_ts,
                            &state,
                            &cmd_tx,
                        );
                        Self::render_order_column(
                            &mut cols[1],
                            //"🔵 SOLD",
                            "SOLD",
                            &sold,
                            window_ts,
                            &state,
                            &cmd_tx,
                        );
                        Self::render_order_column(
                            &mut cols[2],
                            //"⚫ OTHERS",
                            "OTHERS",
                            &others,
                            window_ts,
                            &state,
                            &cmd_tx,
                        );
                    });
                }
            });

            ui.add_space(10.0);
        }

        /*
        // Remove closed windows (descending index to avoid shifting).
        windows_to_remove.sort_by(|a, b| b.0.cmp(&a.0));
        for (idx, window_ts, order_ids) in windows_to_remove {
            // Stop the background market feed task.
            let _ = self.cmd_tx.try_send(UiCommand::StopMarketFeed { window_ts });
            self.feed_started_for.remove(&window_ts);

            // Remove market snapshot
            self.state.market_prices.remove(&window_ts);

            // Purge orders and their associated trades from shared state.
            for id in &order_ids {
                if let Some((_, order)) = self.state.orders.remove(id) {
                    for trade_id in &order.associate_trades {
                        self.state.trades.remove(trade_id);
                    }
                }
            }

            self.state.touch();
            self.windows.remove(idx);
        }
        */
    }

    fn render_order_column(
        ui: &mut egui::Ui,
        title: &str,
        ids: &[String],
        window_ts: u64,
        state: &SharedAppState,
        cmd_tx: &tokio::sync::mpsc::Sender<UiCommand>,
    ) {
        panel_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading(egui::RichText::new(title).monospace().color(Theme::TEXT_PRIMARY));
                ui.separator();
                ui.label(egui::RichText::new(ids.len().to_string()).monospace().color(Theme::TEXT_MUTED));
            });

            ui.add_space(8.0);

            egui::ScrollArea::vertical()
                .id_salt(format!("{title}_scroll_{window_ts}"))
                .max_height(550.0)
                .auto_shrink([false; 2])
                .show(ui, |ui| {
                    for id in ids {
                        // get_mut is safe: only the UI thread writes inline-sell
                        // fields; the worker only writes status/fill fields.
                        if let Some(mut order) = state.orders.get_mut(id) {
                            Self::render_order_card(ui, &mut order, window_ts, cmd_tx);
                            ui.add_space(8.0);
                        }
                    }
                });
        });
    }

    fn render_order_card(
        ui: &mut egui::Ui,
        order: &mut TrackedOrder,
        window_ts: u64,
        cmd_tx: &tokio::sync::mpsc::Sender<UiCommand>,
    ) {
        let (fill, border) = card_colors(order);

        panel_frame()
            .fill(fill)
            .stroke(egui::Stroke::new(1.0, border))
            .show(ui, |ui| {
                let display_price = order.executed_price.as_deref().unwrap_or(&order.price);
                let display_size = order.executed_size.as_deref().unwrap_or(&order.size);

                // Header row
                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "{} {} @ {} x {}",
                            order.side.to_uppercase(),
                            order.token.to_uppercase(),
                            display_price,
                            display_size,
                        ))
                        .monospace()
                        .strong()
                        .color(Theme::TEXT_PRIMARY),
                    );

                    ui.separator();

                    ui.label(
                        egui::RichText::new(format!("MATCHED {}", order.size_matched))
                            .monospace()
                            .color(Theme::TEXT_PRIMARY),
                    );

                    if matches!(
                        order.status,
                        LocalOrderStatus::Open | LocalOrderStatus::PartiallyFilled { .. }
                    ) {
                        if themed_button(ui, "CANCEL", Theme::SELL_RED_BG, Theme::SELL_RED)
                            .clicked()
                        {
                            let _ = cmd_tx.try_send(UiCommand::CancelIndividual {
                                order_id: order.id.clone(),
                                window_ts,
                            });
                        }
                    }
                });

                // Status label
                ui.add_space(4.0);
                ui.label(
                    egui::RichText::new(format!("{:?}", order.status))
                        .monospace()
                        .color(Theme::TEXT_MUTED),
                );

                // Inline exit desk (only for filled buys)
                if matches!(
                    order.status,
                    LocalOrderStatus::TradeConfirmed 
                    | LocalOrderStatus::TradeOpen 
                    | LocalOrderStatus::FullyFilled 
                    | LocalOrderStatus::PartiallyFilled { .. }
                ) && order.side.eq_ignore_ascii_case("buy")
                {
                    ui.add_space(8.0);
                    panel_frame().fill(Theme::BG_PANEL).show(ui, |ui| {
                        ui.vertical(|ui| {
                            ui.horizontal_wrapped(|ui| {
                                compact_input(
                                    ui,
                                    "Price",
                                    &mut order.inline_sell_price,
                                    60.0,
                                );
                                compact_input(
                                    ui,
                                    "Size",
                                    &mut order.inline_sell_size,
                                    60.0,
                                );
                            });

                            ui.add_space(6.0);

                            ui.horizontal_wrapped(|ui| {
                                if themed_button(
                                    ui,
                                    "LIMIT EXIT",
                                    Theme::SELL_RED_BG,
                                    Theme::SELL_RED,
                                )
                                .clicked()
                                {
                                    let _ = cmd_tx.try_send(UiCommand::PlaceLimit {
                                        side: "sell".into(),
                                        token: order.token.clone(),
                                        price: order.inline_sell_price.clone(),
                                        size: order.inline_sell_size.clone(),
                                        rapid_price: "0".into(),
                                        window_ts,
                                    });
                                }

                                if themed_button(
                                    ui,
                                    "MARKET EXIT",
                                    Theme::BLUE_BG,
                                    Theme::BLUE,
                                )
                                .clicked()
                                {
                                    let _ = cmd_tx.try_send(UiCommand::PlaceMarket {
                                        side: "sell".into(),
                                        token: order.token.clone(),
                                        usdc: None,
                                        shares: Some(order.inline_sell_size.clone()),
                                        order_type: Some("FAK".into()),
                                        window_ts,
                                    });
                                }
                            });
                        });
                    });
                }
            });
    }
}

fn card_colors(order: &TrackedOrder) -> (egui::Color32, egui::Color32) {
    match &order.status {
        LocalOrderStatus::Canceled => (
            egui::Color32::from_rgba_unmultiplied(90, 90, 90, 35),
            egui::Color32::from_rgb(120, 120, 120),
        ),
        LocalOrderStatus::Failed(_) => (Theme::SELL_RED_BG, Theme::SELL_RED),
        _ => match (
            order.side.to_lowercase().as_str(),
            order.token.to_lowercase().as_str(),
        ) {
            ("buy", "up") => (Theme::BUY_GREEN_BG, Theme::BUY_GREEN),
            ("buy", "down") => (Theme::SELL_RED_BG, Theme::SELL_RED),
            ("sell", _) => (Theme::BLUE_BG, Theme::BLUE),
            _ => (Theme::BG_ELEVATED, Theme::BORDER),
        },
    }
}