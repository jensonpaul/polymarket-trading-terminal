use dashmap::DashMap;
use crate::prediction::{BtcFeatures, signals::PredictionSignal};

// ---------------------------------------------------------------------------
// Prediction
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct PredictionState {
    /// One signal per strategy that fired this cycle.  Empty means no
    /// strategy produced a signal (or the state was cleared / not yet
    /// populated).
    pub signals: Vec<PredictionSignal>,
    pub last_updated_ms: u64,
    /// Latest BTC snapshot for this window — updated every poll cycle
    /// regardless of whether any signals are emitted.
    pub btc: Option<BtcFeatures>,
}

pub struct PredictionStore {
    pub signals: DashMap<u64, PredictionState>,
}

impl Default for PredictionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PredictionStore {
    pub fn new() -> Self {
        Self {
            signals: DashMap::new(),
        }
    }

    /// Replace the full signal set for this window.
    pub fn update_signals(
        &self,
        window_ts: u64,
        signals: Vec<PredictionSignal>,
        btc: BtcFeatures,
        now_ms: u64,
    ) {
        self.signals.insert(
            window_ts,
            PredictionState {
                signals,
                last_updated_ms: now_ms,
                btc: Some(btc),
            },
        );
    }

    /// Update BTC metrics without changing the active signals.
    pub fn update_btc(
        &self,
        window_ts: u64,
        btc: BtcFeatures,
        now_ms: u64,
    ) {
        self.signals
            .entry(window_ts)
            .and_modify(|s| {
                s.btc = Some(btc.clone());
                s.last_updated_ms = now_ms;
            })
            .or_insert_with(|| PredictionState {
                signals: Vec::new(),
                last_updated_ms: now_ms,
                btc: Some(btc),
            });
    }

    /// Clear all signals for a window (stale eviction).
    pub fn clear_signals(
        &self,
        window_ts: u64,
        now_ms: u64,
    ) {
        self.signals
            .entry(window_ts)
            .and_modify(|s| {
                s.signals.clear();
                s.last_updated_ms = now_ms;
            })
            .or_insert_with(|| PredictionState {
                signals: Vec::new(),
                last_updated_ms: now_ms,
                btc: None,
            });
    }
}