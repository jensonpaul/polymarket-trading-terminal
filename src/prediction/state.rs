use dashmap::DashMap;
use crate::prediction::{BtcFeatures, signals::PredictionSignal};

// ---------------------------------------------------------------------------
// Prediction
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct PredictionState {
    pub active_signal: Option<PredictionSignal>,
    pub last_updated_ms: u64,
    /// Latest BTC snapshot for this window — updated every poll cycle
    /// regardless of whether a signal is emitted.
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

    pub fn update_signal(
        &self,
        window_ts: u64,
        signal: PredictionSignal,
        btc: BtcFeatures,
        now_ms: u64,
    ) {
        self.signals.insert(
            window_ts,
            PredictionState {
                active_signal: Some(signal),
                last_updated_ms: now_ms,
                btc: Some(btc),
            },
        );
    }

    /// Update BTC metrics without changing the active signal.
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
                active_signal: None,
                last_updated_ms: now_ms,
                btc: Some(btc),
            });
    }

    pub fn clear_signal(
        &self,
        window_ts: u64,
        now_ms: u64,
    ) {
        self.signals
            .entry(window_ts)
            .and_modify(|s| {
                s.active_signal = None;
                s.last_updated_ms = now_ms;
            })
            .or_insert_with(|| PredictionState {
                active_signal: None,
                last_updated_ms: now_ms,
                btc: None,
            });
    }
}