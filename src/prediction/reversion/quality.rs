use std::time::Instant;
use serde::{Serialize, Deserialize};

/// Data quality metadata attached to every [`FeatureSnapshot`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DataQuality {
    pub feed_age_ms:    u64,
    pub missing_fields: u8,
    /// Aggregate confidence ∈ [0, 1].
    pub confidence:     f64,
}

impl DataQuality {
    pub const MISSING_ORDER_BOOK: u8 = 0b0000_0001;
    pub const MISSING_AGGRESSOR:  u8 = 0b0000_0010;

    pub fn perfect() -> Self {
        Self { feed_age_ms: 0, missing_fields: 0, confidence: 1.0 }
    }

    pub fn compute(feed_age_ms: u64, missing_fields: u8) -> Self {
        let age_conf   = (1.0 - feed_age_ms as f64 / 5_000.0).clamp(0.0, 1.0);
        let field_conf = 1.0 - 0.15 * missing_fields.count_ones() as f64;
        let confidence = (age_conf * field_conf.max(0.0)).clamp(0.0, 1.0);
        Self { feed_age_ms, missing_fields, confidence }
    }
}

// ── Freshness monitor ─────────────────────────────────────────────────────────

pub struct FreshnessMonitor {
    last_instant:   Option<Instant>,
    missing_fields: u8,
}

impl FreshnessMonitor {
    pub fn new() -> Self {
        Self { last_instant: None, missing_fields: 0 }
    }

    pub fn record(&mut self) {
        self.last_instant = Some(Instant::now());
    }

    pub fn set_missing_fields(&mut self, mask: u8) {
        self.missing_fields = mask;
    }

    pub fn quality(&self) -> DataQuality {
        let age_ms = self.last_instant
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(u64::MAX);
        DataQuality::compute(age_ms, self.missing_fields)
    }

    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        self.last_instant
            .map(|t| t.elapsed().as_millis() as u64 > threshold_ms)
            .unwrap_or(true)
    }
}

impl Default for FreshnessMonitor {
    fn default() -> Self { Self::new() }
}
