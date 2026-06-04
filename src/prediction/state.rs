use dashmap::DashMap;
use crate::prediction::signals::PredictionSignal;

// ---------------------------------------------------------------------------
// Prediction
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct PredictionState {
    pub active_signal: Option<PredictionSignal>,
    pub last_updated_ms: u64,
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
        now_ms: u64,
    ) {
        self.signals.insert(
            window_ts,
            PredictionState {
                active_signal: Some(signal),
                last_updated_ms: now_ms,
            },
        );
    }

    pub fn clear_signal(
        &self,
        window_ts: u64,
        now_ms: u64,
    ) {
        self.signals.insert(
            window_ts,
            PredictionState {
                active_signal: None,
                last_updated_ms: now_ms,
            },
        );
    }
}