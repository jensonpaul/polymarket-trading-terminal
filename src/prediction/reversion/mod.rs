//! BTC Mean-Reversion Probability Engine — embedded as an internal module.
//!
//! This is a self-contained online inference engine that estimates the
//! probability that BTC price will mean-revert over multiple time horizons.
//!
//! ## Integration contract
//!
//! - [`ReversionEngine`] is driven by the existing [`BtcFeed`] pipeline.
//! - It consumes raw [`ExchangeTick`] data **before** the Kalman smoother,
//!   preserving the noisy micro-structure signal needed for reversion detection.
//! - [`ReversionOutput`] is published to [`PredictionContext`] every tick
//!   and consumed by [`MeanReversionStrategy`].
//! - No I/O, no persistence, no networking — the host owns all of that.

pub mod config;
pub mod engine;
pub mod errors;
pub mod features;
pub mod horizon;
pub mod output;
pub mod pipeline;
pub mod quality;
pub mod regime;

pub use config::ReversionConfig;
pub use engine::ReversionEngine;
pub use errors::ReversionError;
pub use features::{FeatureSnapshot, OrderFlowFeatures, MomentumFeatures, VolatilityState};
pub use horizon::Horizon;
pub use output::{ReversionOutput, HorizonReversion, ReversionLevels, TradeBias};
pub use quality::DataQuality;
pub use regime::{MarketRegime, RegimeDistribution};
