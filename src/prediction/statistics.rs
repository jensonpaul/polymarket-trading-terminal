#[derive(Debug, Clone, Default)]
pub struct PredictionStats {
    pub signals_generated: u64,
    pub signals_won: u64,
    pub signals_lost: u64,

    pub average_confidence: f64,

    pub cumulative_confidence: f64,
}

impl PredictionStats {
    pub fn register_signal(&mut self, confidence: f64) {
        self.signals_generated += 1;
        self.cumulative_confidence += confidence;

        self.average_confidence =
            self.cumulative_confidence / self.signals_generated as f64;
    }

    pub fn register_win(&mut self) {
        self.signals_won += 1;
    }

    pub fn register_loss(&mut self) {
        self.signals_lost += 1;
    }

    pub fn win_rate(&self) -> f64 {
        if self.signals_generated == 0 {
            return 0.0;
        }

        self.signals_won as f64
            / self.signals_generated as f64
    }
}
