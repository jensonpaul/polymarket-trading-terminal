//! Conviction tracker — converts noisy per-tick model outputs into stable,
//! persistent directional signals with an entry queue.
//!
//! # Design
//!
//! ## Problem
//! The ONNX model re-evaluates on every ~100 ms fused tick and its output
//! changes with each new feature vector.  Feeding raw per-tick predictions
//! directly to the strategy layer causes signals to flicker — a Bullish call
//! at tick N is overwritten by Sideways at tick N+1 even though the
//! underlying market structure has not changed.
//!
//! ## Solution: three-layer pipeline
//!
//! ```text
//! 1. Candidate accumulation
//!    Each direction (Bullish / Bearish) maintains an independent candidate
//!    that counts consecutive confirming ticks and accumulates confidence.
//!    A candidate becomes "ready" after TICKS_REQUIRED consecutive ticks
//!    exceed CONFIDENCE_THRESHOLD.
//!
//! 2. Entry queue
//!    Ready candidates are promoted into a scored queue (BinaryHeap).
//!    Score = mean_confidence over the confirmation window.
//!    - At most ONE queued entry per direction is allowed.  If a new
//!      candidate for the same direction is ready while one is already
//!      queued, it replaces the queued entry only if it has higher score.
//!    - Promotion is skipped entirely if the active signal already matches
//!      that direction (no point queuing more of the same).
//!    When the active slot is empty the highest-scored candidate wins
//!    immediately.  When the slot is occupied the queue waits.
//!
//! 3. Active slot
//!    Once a candidate wins the slot it becomes the stable signal returned
//!    to the strategy layer.  It holds until:
//!    - EXIT_TICKS_REQUIRED consecutive opposing ticks exceed
//!      EXIT_CONFIDENCE_THRESHOLD  (hard counter-signal), OR
//!    - the signal has been held longer than MAX_HOLD_MS, OR
//!    - the caller explicitly calls reset() on window rollover.
//!    On exit, the next best queued candidate (if any) immediately fills
//!    the slot.
//! ```
//!
//! ## Parameters (tunable)
//!
//! | Constant                    | Default   | Meaning                                      |
//! |-----------------------------|-----------|----------------------------------------------|
//! | `TICKS_REQUIRED`            | 8         | Consecutive confirming ticks to go "ready"   |
//! | `CONFIDENCE_THRESHOLD`      | 0.60      | Min per-tick confidence to count             |
//! | `EXIT_TICKS_REQUIRED`       | 5         | Consecutive opposing ticks to exit           |
//! | `EXIT_CONFIDENCE_THRESHOLD` | 0.65      | Min confidence for an opposing tick to count |
//! | `MAX_HOLD_MS`               | 240_000   | Hard cap: 4 min (window is 5 min)            |
//! | `CANDIDATE_EXPIRY_MS`       | 240_000   | Queued candidates expire with the hold cap   |

use std::collections::BinaryHeap;
use std::cmp::Ordering;

use btc_prediction_engine::types::TrendDirection;

// ── Tunables ──────────────────────────────────────────────────────────────────

const TICKS_REQUIRED: u32 = 8;
const CONFIDENCE_THRESHOLD: f64 = 0.60;
const EXIT_TICKS_REQUIRED: u32 = 5;
const EXIT_CONFIDENCE_THRESHOLD: f64 = 0.65;
const MAX_HOLD_MS: u64 = 240_000;

/// Queued candidates expire at the same horizon as the hold cap.
/// This ensures a reversal candidate queued early in an active signal's
/// life is still available when the signal eventually exits.
const CANDIDATE_EXPIRY_MS: u64 = MAX_HOLD_MS;

// ── Candidate ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Candidate {
    direction:             TrendDirection,
    consecutive_ticks:     u32,
    cumulative_confidence: f64,
    first_seen_ms:         u64,
    last_seen_ms:          u64,
}

impl Candidate {
    fn new(direction: TrendDirection, confidence: f64, now_ms: u64) -> Self {
        Self {
            direction,
            consecutive_ticks: 1,
            cumulative_confidence: confidence,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
        }
    }

    fn push(&mut self, confidence: f64, now_ms: u64) {
        self.consecutive_ticks     += 1;
        self.cumulative_confidence += confidence;
        self.last_seen_ms           = now_ms;
    }

    fn mean_confidence(&self) -> f64 {
        if self.consecutive_ticks == 0 { return 0.0; }
        self.cumulative_confidence / self.consecutive_ticks as f64
    }

    fn is_ready(&self) -> bool {
        self.consecutive_ticks >= TICKS_REQUIRED
    }
}

// ── Queued entry ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct QueuedEntry {
    direction:       TrendDirection,
    mean_confidence: f64,
    queued_at_ms:    u64,
}

impl PartialEq for QueuedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.mean_confidence == other.mean_confidence
    }
}
impl Eq for QueuedEntry {}

impl PartialOrd for QueuedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.mean_confidence
            .partial_cmp(&other.mean_confidence)
            .unwrap_or(Ordering::Equal)
    }
}

// ── Active signal ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ActiveSignal {
    pub direction:       TrendDirection,
    pub mean_confidence: f64,
    pub activated_at_ms: u64,
}

// ── Public output ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ConvictionSnapshot {
    /// The stable active signal, if one is held.
    pub active: Option<ActiveSignal>,

    /// Number of candidates currently queued behind the active slot.
    pub queued: usize,

    /// Both building directions and their tick counts, for full observability.
    /// First element is Bullish candidate progress, second is Bearish.
    pub building_bullish: Option<u32>,
    pub building_bearish: Option<u32>,

    // Kept for compatibility with existing reason-string formatting.
    pub building: Option<TrendDirection>,
    pub building_ticks: u32,
}

impl Default for ConvictionSnapshot {
    fn default() -> Self {
        Self {
            active:           None,
            queued:           0,
            building_bullish: None,
            building_bearish: None,
            building:         None,
            building_ticks:   0,
        }
    }
}

// ── ConvictionTracker ─────────────────────────────────────────────────────────

pub struct ConvictionTracker {
    bullish_candidate: Option<Candidate>,
    bearish_candidate: Option<Candidate>,

    /// At most one entry per direction: enforced in try_promote.
    queue: BinaryHeap<QueuedEntry>,

    active:       Option<ActiveSignal>,
    exit_counter: u32,
}

impl ConvictionTracker {
    pub fn new() -> Self {
        Self {
            bullish_candidate: None,
            bearish_candidate: None,
            queue:             BinaryHeap::new(),
            active:            None,
            exit_counter:      0,
        }
    }

    /// Reset all state on window rollover.
    pub fn reset(&mut self) {
        self.bullish_candidate = None;
        self.bearish_candidate = None;
        self.queue.clear();
        self.active       = None;
        self.exit_counter = 0;
    }

    /// Ingest one model output tick.
    pub fn push(&mut self, direction: TrendDirection, confidence: f64, now_ms: u64) {
        match direction {
            TrendDirection::Sideways => {
                // Sideways resets building candidates but does NOT exit the
                // active signal — sustained sideways alone is not a reversal.
                self.bullish_candidate = None;
                self.bearish_candidate = None;
            }

            TrendDirection::Bullish => {
                self.bearish_candidate = None;

                if confidence >= CONFIDENCE_THRESHOLD {
                    match &mut self.bullish_candidate {
                        Some(c) => c.push(confidence, now_ms),
                        None => {
                            self.bullish_candidate =
                                Some(Candidate::new(TrendDirection::Bullish, confidence, now_ms));
                        }
                    }
                    self.try_promote(TrendDirection::Bullish, now_ms);
                } else {
                    self.bullish_candidate = None;
                }

                self.update_exit_counter(TrendDirection::Bullish, confidence, now_ms);
            }

            TrendDirection::Bearish => {
                self.bullish_candidate = None;

                if confidence >= CONFIDENCE_THRESHOLD {
                    match &mut self.bearish_candidate {
                        Some(c) => c.push(confidence, now_ms),
                        None => {
                            self.bearish_candidate =
                                Some(Candidate::new(TrendDirection::Bearish, confidence, now_ms));
                        }
                    }
                    self.try_promote(TrendDirection::Bearish, now_ms);
                } else {
                    self.bearish_candidate = None;
                }

                self.update_exit_counter(TrendDirection::Bearish, confidence, now_ms);
            }
        }

        self.check_max_hold(now_ms);
        self.expire_queue(now_ms);
        self.fill_from_queue(now_ms);
    }

    /// Return the current stable state.
    pub fn snapshot(&self) -> ConvictionSnapshot {
        // Expose both building candidates independently.
        let building_bullish = self.bullish_candidate
            .as_ref()
            .map(|c| c.consecutive_ticks);
        let building_bearish = self.bearish_candidate
            .as_ref()
            .map(|c| c.consecutive_ticks);

        // Legacy fields: dominant building direction for reason strings.
        let (building, building_ticks) = match (&self.bullish_candidate, &self.bearish_candidate) {
            (Some(b), None) => (Some(TrendDirection::Bullish), b.consecutive_ticks),
            (None, Some(b)) => (Some(TrendDirection::Bearish), b.consecutive_ticks),
            (Some(b), Some(r)) => {
                // Both building (shouldn't happen, but be safe — pick higher ticks).
                if b.consecutive_ticks >= r.consecutive_ticks {
                    (Some(TrendDirection::Bullish), b.consecutive_ticks)
                } else {
                    (Some(TrendDirection::Bearish), r.consecutive_ticks)
                }
            }
            (None, None) => (None, 0),
        };

        ConvictionSnapshot {
            active: self.active.clone(),
            queued: self.queue.len(),
            building_bullish,
            building_bearish,
            building,
            building_ticks,
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────

    /// Promote a ready candidate to the queue, subject to two guards:
    ///
    /// 1. **Same-direction active**: skip entirely — no point queuing more
    ///    of what is already active.
    /// 2. **One-per-direction queue cap**: if an entry for this direction
    ///    already exists in the queue, replace it only if the new candidate
    ///    has a strictly higher mean confidence.
    fn try_promote(&mut self, direction: TrendDirection, now_ms: u64) {
        let candidate = match direction {
            TrendDirection::Bullish => self.bullish_candidate.as_ref(),
            TrendDirection::Bearish => self.bearish_candidate.as_ref(),
            TrendDirection::Sideways => return,
        };

        let candidate = match candidate {
            Some(c) if c.is_ready() => c,
            _ => return,
        };

        let mean_conf = candidate.mean_confidence();

        // Guard 1: active slot already holds this direction.
        if self.active.as_ref().map_or(false, |a| a.direction == direction) {
            tracing::debug!(
                ?direction,
                "try_promote: skipping — active slot already holds this direction"
            );
            // Reset candidate so it doesn't keep re-triggering every tick.
            match direction {
                TrendDirection::Bullish => self.bullish_candidate = None,
                TrendDirection::Bearish => self.bearish_candidate = None,
                _ => {}
            }
            return;
        }

        // Guard 2: one-per-direction queue cap.
        // Drain, check, and rebuild — BinaryHeap doesn't support in-place update.
        let existing: Vec<QueuedEntry> = self.queue.drain().collect();
        let same_dir_best = existing
            .iter()
            .filter(|e| e.direction == direction)
            .map(|e| e.mean_confidence)
            .fold(f64::NEG_INFINITY, f64::max);

        if mean_conf <= same_dir_best {
            // Existing queued entry is already better; discard new candidate.
            tracing::debug!(
                ?direction,
                mean_conf,
                same_dir_best,
                "try_promote: new candidate weaker than queued entry, skipping"
            );
            // Restore the queue as-is.
            self.queue.extend(existing);
        } else {
            // Keep all entries except the existing same-direction one, then
            // push the new (better) entry.
            self.queue.extend(
                existing.into_iter().filter(|e| e.direction != direction),
            );
            self.queue.push(QueuedEntry {
                direction,
                mean_confidence: mean_conf,
                queued_at_ms:    now_ms,
            });
            tracing::info!(
                ?direction,
                mean_conf,
                "ConvictionTracker: candidate promoted to queue"
            );
        }

        // Reset candidate either way.
        match direction {
            TrendDirection::Bullish => self.bullish_candidate = None,
            TrendDirection::Bearish => self.bearish_candidate = None,
            _ => {}
        }
    }

    fn update_exit_counter(
        &mut self,
        incoming:   TrendDirection,
        confidence: f64,
        now_ms:     u64,
    ) {
        let active_dir = match &self.active {
            Some(a) => a.direction,
            None    => return,
        };

        let is_opposing = matches!(
            (active_dir, incoming),
            (TrendDirection::Bullish, TrendDirection::Bearish)
            | (TrendDirection::Bearish, TrendDirection::Bullish)
        );

        if is_opposing && confidence >= EXIT_CONFIDENCE_THRESHOLD {
            self.exit_counter += 1;
            tracing::debug!(
                exit_counter = self.exit_counter,
                required     = EXIT_TICKS_REQUIRED,
                "conviction exit counter"
            );
            if self.exit_counter >= EXIT_TICKS_REQUIRED {
                self.evict_active(now_ms);
            }
        } else {
            self.exit_counter = 0;
        }
    }

    fn evict_active(&mut self, now_ms: u64) {
        if let Some(ref a) = self.active {
            tracing::info!(
                direction    = ?a.direction,
                held_ms      = now_ms.saturating_sub(a.activated_at_ms),
                exit_counter = self.exit_counter,
                "ConvictionTracker: evicting active signal"
            );
        }
        self.active       = None;
        self.exit_counter = 0;
        self.fill_from_queue(now_ms);
    }

    fn check_max_hold(&mut self, now_ms: u64) {
        let expired = self.active.as_ref().map_or(false, |a| {
            now_ms.saturating_sub(a.activated_at_ms) > MAX_HOLD_MS
        });
        if expired {
            tracing::info!("ConvictionTracker: MAX_HOLD_MS reached, evicting");
            self.active       = None;
            self.exit_counter = 0;
            self.fill_from_queue(now_ms);
        }
    }

    fn expire_queue(&mut self, now_ms: u64) {
        let fresh: Vec<QueuedEntry> = self
            .queue
            .drain()
            .filter(|e| now_ms.saturating_sub(e.queued_at_ms) <= CANDIDATE_EXPIRY_MS)
            .collect();
        self.queue.extend(fresh);
    }

    fn fill_from_queue(&mut self, now_ms: u64) {
        if self.active.is_some() { return; }
        if let Some(entry) = self.queue.pop() {
            tracing::info!(
                direction    = ?entry.direction,
                mean_conf    = entry.mean_confidence,
                waited_ms    = now_ms.saturating_sub(entry.queued_at_ms),
                "ConvictionTracker: activating signal from queue"
            );
            self.active = Some(ActiveSignal {
                direction:       entry.direction,
                mean_confidence: entry.mean_confidence,
                activated_at_ms: now_ms,
            });
            self.exit_counter = 0;
        }
    }
}

impl Default for ConvictionTracker {
    fn default() -> Self { Self::new() }
}