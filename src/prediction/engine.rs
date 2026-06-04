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

    pub fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> Vec<PredictionSignal> {
        let mut signals = Vec::new();

        for strategy in &self.strategies {
            if let Some(signal) = strategy.evaluate(ctx) {
                signals.push(signal);
            }
        }

        signals
    }

    pub fn evaluate_best(
        &self,
        ctx: &PredictionContext,
    ) -> Option<PredictionSignal> {
        self.evaluate(ctx)
            .into_iter()
            .max_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}
