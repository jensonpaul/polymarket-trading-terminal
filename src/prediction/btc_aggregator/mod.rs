//! Noise-filtering pipeline for aggregated BTC order-book feeds.
//!
//! ## Module layout
//!
//! ```text
//! btc_aggregator/
//! ├── mod.rs          ← you are here; public API surface
//! ├── tick.rs         ← Exchange, Level, ExchangeTick (raw input types)
//! ├── spike_filter.rs ← Layer 1: per-exchange EMA spike gate
//! ├── outlier_gate.rs ← Layer 2: cross-exchange MAD/Z-score gate
//! ├── aggregator.rs   ← Layer 3: time-bucket OHLCV + trust-weighted VWMP
//! ├── kalman.rs       ← Layer 4: adaptive Kalman smoother
//! └── pipeline.rs     ← orchestrator: wires all 4 layers → CleanPrice
//! ```
//!
//! ## Public API
//!
//! Most callers only need three things:
//!
//! ```rust,ignore
//! use crate::prediction::btc_aggregator::{
//!     Pipeline, PipelineConfig,   // create and drive the pipeline
//!     CleanPrice,                 // output type consumed by BtcFeed
//!     Exchange, ExchangeTick,     // build ticks from raw proto levels
//! };
//! ```
//!
//! Lower-level types (`SpikeFilter`, `OutlierGate`, `Aggregator`,
//! `KalmanSmoother`) are also public for testing and future extension.

pub mod aggregator;
pub mod kalman;
pub mod outlier_gate;
pub mod pipeline;
pub mod spike_filter;
pub mod tick;

// ── Convenience re-exports ────────────────────────────────────────────────────
//
// Re-export the most-used types at the btc_aggregator crate level so that
// callers do not need to know which sub-module each type lives in.

// Input types
pub use tick::{Exchange, ExchangeTick, Level};

// Per-layer types (useful for tests and custom pipeline assemblies)
pub use spike_filter::{SpikeFilter, SpikeFilterConfig};
pub use outlier_gate::{OutlierGate, OutlierGateConfig};
pub use aggregator::{Aggregator, AggregatorConfig, Candle, ExchangeContribution};
pub use kalman::{KalmanConfig, KalmanSmoother, SmoothedPrice};

// Primary pipeline — the only type most callers need
pub use pipeline::{CleanPrice, Pipeline, PipelineConfig, PipelineStats};