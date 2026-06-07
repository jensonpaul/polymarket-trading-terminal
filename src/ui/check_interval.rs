use eframe::egui;
use crate::messages::UiCommand;
use crate::state::NotificationKind;
use crate::ui::theme::Theme;
use crate::ui::PolymarketDashboardApp;
use crate::worker_config::Queue;

impl PolymarketDashboardApp {
    pub fn render_poll_interval_input(
        &mut self,
        ui: &mut egui::Ui,
        label: &str,
        queue: Queue,
    ) {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new(label).monospace().color(Theme::TEXT_MUTED));
            ui.add_space(6.0);

            let response = ui.add(
                egui::TextEdit::singleline(self.interval_inputs.get_mut(queue))
                    .desired_width(70.0),
            );

            ui.label(egui::RichText::new("ms").monospace().color(Theme::TEXT_MUTED));

            if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                let value = self.interval_inputs.get(queue).to_owned();
                match value.parse::<u64>() {
                    Ok(ms) => {
                        // try_send is fine here — the channel is bounded and this
                        // is a low-frequency user action.
                        let _ = self.cmd_tx.try_send(UiCommand::UpdatePollInterval {
                            milliseconds: ms,
                            queue,
                        });
                        self.push_toast(
                            format!("{label} updated to {ms} ms"),
                            NotificationKind::Success,
                        );
                    }
                    Err(_) => {
                        self.push_toast("Invalid interval value".into(), NotificationKind::Error);
                    }
                }
            }
        });
    }
}
