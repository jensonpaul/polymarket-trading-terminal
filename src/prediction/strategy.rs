use crate::prediction::{
    PredictionContext,
    PredictionSignal,
};

pub trait PredictionStrategy:
    Send
    + Sync
    + 'static
{
    fn name(&self) -> &'static str;

    fn evaluate(
        &self,
        ctx: &PredictionContext,
    ) -> Option<PredictionSignal>;
}
