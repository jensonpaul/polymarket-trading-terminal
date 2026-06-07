use eframe::egui;
use crate::ui::theme::Theme;

pub fn compact_input(ui: &mut egui::Ui, label: &str, value: &mut String, width: f32) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new(label).monospace().color(Theme::TEXT_MUTED));
        ui.add(
            egui::TextEdit::singleline(value)
                .desired_width(width)
                .text_color(Theme::TEXT_PRIMARY)
                .background_color(Theme::BG_PANEL),
        );
    });
}

pub fn themed_button(
    ui: &mut egui::Ui,
    text: &str,
    fill: egui::Color32,
    stroke: egui::Color32,
) -> egui::Response {
    ui.add(
        egui::Button::new(egui::RichText::new(text).monospace().strong().color(Theme::TEXT_PRIMARY))
            .fill(fill)
            .stroke(egui::Stroke::new(1.0, stroke))
            .corner_radius(6.0),
    )
}

pub fn panel_frame() -> egui::Frame {
    egui::Frame::none()
        .fill(Theme::BG_ELEVATED)
        .stroke(egui::Stroke::new(1.0, Theme::BORDER))
        .corner_radius(8.0)
        .inner_margin(12.0)
}
