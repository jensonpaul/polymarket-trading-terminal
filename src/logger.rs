//! # GUI Logger
//!
//! A `tracing_subscriber::Layer` that forwards log events to the toast queue
//! via [`crate::events::AppEvent::Notify`] on the [`crate::events::EventBus`].

use tokio::sync::mpsc::Sender;
use tracing::field::Visit;
use tracing_subscriber::Layer;

use crate::events::{AppEvent, EventBus};
use crate::state::NotificationKind;

pub struct GuiLogger {
    pub tx: EventBus,
}

struct MsgVisitor {
    msg: String,
}

impl Visit for MsgVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.msg = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.msg = value.to_owned();
        }
    }
}

impl<S> Layer<S> for GuiLogger
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta   = event.metadata();
        let target = meta.target();

        // Skip framework internals to prevent recursion / noise.
        if target.starts_with("tokio")
            || target.starts_with("runtime")
            || target.starts_with("eframe")
            || target.starts_with("egui")
        {
            return;
        }

        let mut visitor = MsgVisitor { msg: String::new() };
        event.record(&mut visitor);

        if visitor.msg.is_empty() {
            return;
        }

        let kind = match *meta.level() {
            tracing::Level::ERROR => NotificationKind::Error,
            tracing::Level::WARN  => NotificationKind::Warning,
            tracing::Level::INFO  => NotificationKind::Info,
            tracing::Level::DEBUG => NotificationKind::Debug,
            tracing::Level::TRACE => NotificationKind::Trace,
        };

        // Non-blocking: drop the event if the channel is full rather than
        // stalling the tracing call-site.
        let _ = self.tx.try_send(AppEvent::Notify {
            message: visitor.msg,
            kind,
        });
    }
}