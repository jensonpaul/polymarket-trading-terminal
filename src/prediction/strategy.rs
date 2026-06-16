use crate::prediction::{
    PredictionContext,
    PredictionSignal,
};

/// A prediction strategy that always produces a signal.
///
/// Strategies must never return early silently.  When preconditions are not
/// met (warm-up, missing data, explicit no-trade gate), return a signal with
/// `SignalType::NoTrade` and a human-readable `reason` explaining *why*.
/// This guarantees every registered strategy is always visible in the UI.
pub trait PredictionStrategy:
    Send
    + Sync
    + 'static
{
    fn name(&self) -> &'static str;

    fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> PredictionSignal;
}