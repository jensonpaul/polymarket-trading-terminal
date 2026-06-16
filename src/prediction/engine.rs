use crate::prediction::{
    PredictionContext,
    PredictionSignal,
    PredictionStrategy,
};

pub struct PredictionEngine {
    strategies: Vec<Box<dyn PredictionStrategy>>,
}

impl PredictionEngine {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    pub fn with_strategy(
        mut self,
        strategy: impl PredictionStrategy,
    ) -> Self {
        self.strategies.push(Box::new(strategy));
        self
    }

    pub fn register(
        &mut self,
        strategy: impl PredictionStrategy,
    ) {
        self.strategies.push(Box::new(strategy));
    }

    /// Run every registered strategy and return one signal per strategy.
    /// Signals are pre-tagged with `strategy_name` by the strategy itself.
    /// NoTrade signals are included — every strategy is always represented.
    pub fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> Vec<PredictionSignal> {
        self.strategies
            .iter()
            .map(|strategy| strategy.evaluate(ctx))
            .collect()
    }
}