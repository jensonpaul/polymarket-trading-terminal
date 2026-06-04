use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredictionSide {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketBias {
    Bullish,
    Bearish,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecayType {
    Sudden,
    Gradual,
    Flat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Buy,
    Sell,
    Hold,
    NoTrade,
}

#[derive(Debug, Clone)]
pub struct PredictionSignal {
    pub signal_type: SignalType,

    pub side: PredictionSide,

    pub confidence: f64,

    pub target_entry: Decimal,

    pub target_exit: Decimal,

    pub stop_loss: Decimal,

    pub generated_at_ms: u64,

    pub reason: String,
}

impl PredictionSignal {
    pub fn no_trade(reason: impl Into<String>, generated_at_ms: u64) -> Self {
        Self {
            signal_type: SignalType::NoTrade,
            side: PredictionSide::Up,
            confidence: 0.0,
            target_entry: Decimal::ZERO,
            target_exit: Decimal::ZERO,
            stop_loss: Decimal::ZERO,
            generated_at_ms,
            reason: reason.into(),
        }
    }
}
