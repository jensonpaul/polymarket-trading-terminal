use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredictionSide {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Buy,
    Hold,
    NoTrade,
}

#[derive(Debug, Clone)]
pub struct PredictionSignal {
    /// Name of the strategy that produced this signal.
    pub strategy_name: &'static str,

    pub signal_type: SignalType,

    pub side: PredictionSide,

    /// Placeholder — strategies may leave this at 0.0 until a
    /// confidence model is integrated.
    pub confidence: f64,

    pub target_entry: Decimal,

    /// Reserved for future exit-price modelling.
    pub target_exit: Decimal,

    /// Reserved for future risk management.
    pub stop_loss: Decimal,

    pub generated_at_ms: u64,

    pub reason: String,
}

impl PredictionSignal {
    pub fn no_trade(
        strategy_name: &'static str,
        reason: impl Into<String>,
        generated_at_ms: u64,
    ) -> Self {
        Self {
            strategy_name,
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