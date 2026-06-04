use crate::prediction::features::{
    BtcFeatures,
    PolymarketFeatures,
};

#[derive(Debug, Clone, Default)]
pub struct PredictionContext {
    pub timestamp_ms: u64,

    pub btc: BtcFeatures,

    pub polymarket: PolymarketFeatures,

    pub seconds_remaining: u64,
}
