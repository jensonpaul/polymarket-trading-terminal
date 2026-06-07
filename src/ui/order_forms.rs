use eframe::egui;
use crate::messages::UiCommand;
use crate::ui::theme::Theme;
use crate::ui::widgets::{compact_input, panel_frame, themed_button};
use crate::ui::PolymarketDashboardApp;
use crate::worker_config::Queue;

impl PolymarketDashboardApp {
    pub fn render_order_consoles(&mut self, ui: &mut egui::Ui, current_ts: u64) {
        egui::ScrollArea::horizontal()
            .id_salt("order_consoles_scroll")
            .auto_shrink([false, true])
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 8.0;

                    // ----------------------------------------------------------------
                    // LIMIT
                    // ----------------------------------------------------------------
                    panel_frame().show(ui, |ui| {
                        ui.set_min_width(520.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("📥 LIMIT")
                                    .monospace()
                                    .strong()
                                    .color(Theme::TEXT_PRIMARY),
                            );
                            ui.separator();
                            ui.radio_value(&mut self.limit_side_buy, true, "BUY");
                            ui.radio_value(&mut self.limit_side_buy, false, "SELL");
                            ui.separator();
                            ui.radio_value(&mut self.limit_token_up, true, "UP");
                            ui.radio_value(&mut self.limit_token_up, false, "DOWN");
                            ui.separator();
                            compact_input(ui, "Price", &mut self.limit_price, 55.0);
                            compact_input(ui, "Size", &mut self.limit_size, 55.0);
                            compact_input(ui, "Rapid", &mut self.limit_rapid_price, 55.0);

                            let (fill, stroke) = if self.limit_side_buy {
                                (Theme::BUY_GREEN_BG, Theme::BUY_GREEN)
                            } else {
                                (Theme::SELL_RED_BG, Theme::SELL_RED)
                            };

                            if themed_button(ui, "EXECUTE", fill, stroke).clicked() {
                                let _ = self.cmd_tx.try_send(UiCommand::PlaceLimit {
                                    side: if self.limit_side_buy { "buy" } else { "sell" }.into(),
                                    token: if self.limit_token_up { "up" } else { "down" }.into(),
                                    price: self.limit_price.clone(),
                                    size: self.limit_size.clone(),
                                    rapid_price: self.limit_rapid_price.clone(),
                                    window_ts: current_ts,
                                });
                            }
                        });
                    });

                    // ----------------------------------------------------------------
                    // MARKET
                    // ----------------------------------------------------------------
                    panel_frame().show(ui, |ui| {
                        ui.set_min_width(500.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("⚡ MARKET")
                                    .monospace()
                                    .strong()
                                    .color(Theme::TEXT_PRIMARY),
                            );
                            ui.separator();
                            ui.radio_value(&mut self.market_token_up, true, "UP");
                            ui.radio_value(&mut self.market_token_up, false, "DOWN");
                            ui.separator();
                            ui.checkbox(&mut self.market_use_usdc, "USDC");
                            if self.market_use_usdc {
                                compact_input(ui, "Amount", &mut self.market_usdc, 60.0);
                            } else {
                                compact_input(ui, "Shares", &mut self.market_shares, 60.0);
                            }
                            ui.separator();
                            ui.radio_value(&mut self.market_type_fok, true, "FOK");
                            ui.radio_value(&mut self.market_type_fok, false, "FAK");

                            if themed_button(ui, "EXECUTE", Theme::BUY_GREEN_BG, Theme::BUY_GREEN)
                                .clicked()
                            {
                                let _ = self.cmd_tx.try_send(UiCommand::PlaceMarket {
                                    side: "buy".into(),
                                    token: if self.market_token_up { "up" } else { "down" }.into(),
                                    usdc: self.market_use_usdc.then(|| self.market_usdc.clone()),
                                    shares: (!self.market_use_usdc)
                                        .then(|| self.market_shares.clone()),
                                    order_type: Some(
                                        if self.market_type_fok { "FOK" } else { "FAK" }.into(),
                                    ),
                                    window_ts: current_ts,
                                });
                            }
                        });
                    });

                    // ----------------------------------------------------------------
                    // POLL INTERVALS
                    // ----------------------------------------------------------------
                    panel_frame().show(ui, |ui| {
                        ui.set_min_width(320.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("⏱ POLL")
                                    .monospace()
                                    .strong()
                                    .color(Theme::TEXT_PRIMARY),
                            );
                            ui.separator();
                            self.render_poll_interval_input(ui, "Orders", Queue::Orders);
                            ui.separator();
                            self.render_poll_interval_input(ui, "Trades", Queue::Trades);
                            ui.separator();
                            self.render_poll_interval_input(ui, "Rapid", Queue::RapidSell);
                        });
                    });
                });
            });
    }
}
