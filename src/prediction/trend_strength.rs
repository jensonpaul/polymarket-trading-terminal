//! Trend strength detector — a continuously-updated, window-independent
//! alternative to ConvictionTracker.
//!
//! # Relationship to ConvictionTracker
//!
//! ConvictionTracker models signal lifecycle as discrete state transitions:
//! build → promote → activate → exit. It resets on window rollover and
//! freezes confidence at activation time.
//!
//! This detector instead treats Bullish/Bearish as two long-lived,
//! continuously-evolving evidence streams with no window reset, no queue,
//! and no frozen "active" snapshot. There is no stored active/leader field —
//! leadership is derived fresh from `bull`/`bear` on every call to
//! `snapshot()`, so there is exactly one source of truth at all times.
//!
//! Deliberately NOT shared with ConvictionTracker:
//!   - no reset() — never reset on window rollover, by design.
//!   - no queue / no BinaryHeap — at most one stream per direction exists,
//!     so there is nothing to rank beyond a single comparison at read time.
//!   - no frozen "activated_at_ms" confidence — score is recomputed live.
//!
//! Deliberately shared with ConvictionTracker (kept identical on purpose,
//! to isolate what this experiment is actually testing — lifecycle, not
//! input quality):
//!   - same CONFIDENCE_THRESHOLD gate on incoming ticks.
//!   - same READY_TICKS bar before a stream is eligible to lead.
//!   - fed the same blended (model / heuristic-fallback) direction+confidence
//!     that `service.rs` already computes for ConvictionTracker::push.
//!
//! # Contradiction handling
//!
//! A tick for one direction strips the *other* direction's readiness
//! immediately (`consecutive_ticks = 0`), which is sufficient to guarantee
//! bull and bear can never simultaneously satisfy `ready()` and therefore
//! never co-lead / flicker. It deliberately does NOT clear
//! `ewma_confidence`, `first_seen_ms`, or `last_seen_ms` — a single
//! contradicting tick should cost a stream its eligibility to lead, not its
//! accumulated memory. The stream must reaccumulate READY_TICKS consecutive
//! confirming ticks to lead again, but its EWMA history and the duration
//! it's been observed (`held_ms`) survive untouched.
//!
//! # Recency: two distinct mechanisms, not one
//!
//! `ewma_confidence` is the primary signal: it is the only thing that
//! responds to the *content* of incoming ticks, weighting recent ticks more
//! than old ones within an actively-updating stream. It does NOT respond to
//! wall-clock time on its own.
//!
//! `current_score()` additionally applies wall-clock decay based on time
//! since `last_seen_ms`. This only does meaningful work in the gap between
//! "ticks stopped arriving" and `STALE_MS` expiry — while ticks are flowing
//! normally (sub-second gaps), decay is a near no-op (elapsed time is tiny,
//! so decay factor ≈ 1.0) and EWMA alone drives the score. Decay exists
//! purely to smooth what would otherwise be a hard cliff at the STALE_MS
//! boundary, not to add a second layer of recency-weighting on top of EWMA.
//!
//! # Tunables
//!
//! | Constant               | Default | Meaning                                          |
//! |-------------------------|---------|---------------------------------------------------|
//! | `CONFIDENCE_THRESHOLD`  | 0.60    | Min per-tick confidence to count (mirrors conviction) |
//! | `READY_TICKS`           | 8       | Consecutive confirming ticks before eligible to lead (mirrors conviction) |
//! | `EWMA_ALPHA`            | 0.05    | Weight on new evidence; TBD — see note below |
//! | `DECAY_HALFLIFE_MS`     | 15_000  | Score halves every 15s of silence (anti-cliff smoothing only) |
//! | `FRESHNESS_MS`          | 30_000  | Max silence before a stream loses eligibility to lead (2× half-life; score ≈ 25% at this point) |
//! | `STALE_MS`              | 60_000  | Hard floor: drop state entirely once decay makes it irrelevant |
//!
//! `FRESHNESS_MS` sits between normal tick flow and `STALE_MS` to give a
//! meaningful "degraded but not yet evicted" window. At 2× `DECAY_HALFLIFE_MS`
//! the score has already quartered; revoking leadership there prevents the
//! detector from acting on what is effectively historical confidence.
//! `STALE_MS` remains the sole eviction boundary — `FRESHNESS_MS` only
//! gates leadership eligibility, leaving EWMA/timestamps intact for recovery.
//!
//! `EWMA_ALPHA` is provisional. Open question: should EWMA's effective
//! memory be *longer* than READY_TICKS' confirmation window (so the gate
//! answers "is this real" on a short horizon while the score separately
//! answers "how strong has this been since, and including before, it became
//! real" on a longer one), or should it react faster than that? At
//! alpha=0.05 with ~100ms ticks, EWMA half-life is ~1.35s — close enough to
//! an 8-tick (~800ms) confirmation window that score-at-readiness is mostly
//! "mean of the last ~8 ticks", which converges toward what
//! ConvictionTracker's lifetime mean_confidence() already computes at
//! promotion. Needs deciding/tuning against real tick data before this
//! constant is trusted.

use btc_prediction_engine::types::TrendDirection;

// ── Tunables ──────────────────────────────────────────────────────────────────

const CONFIDENCE_THRESHOLD: f64 = 0.60;
const READY_TICKS: u32 = 8;
const EWMA_ALPHA: f64 = 0.05;
const DECAY_HALFLIFE_MS: u64 = 15_000;
/// Leadership eligibility gate — revoked after this much silence.
/// Distinct from `STALE_MS`: the stream's EWMA/timestamps survive
/// beyond this point; only the right to lead is suspended.
const FRESHNESS_MS: u64 = 30_000; // 2 × DECAY_HALFLIFE_MS
const STALE_MS: u64 = 60_000;

// ── DirectionState ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct DirectionState {
    direction: TrendDirection,
    ewma_confidence: f64,
    consecutive_ticks: u32,
    first_seen_ms: u64,
    last_seen_ms: u64,
}

impl DirectionState {
    fn new(direction: TrendDirection, confidence: f64, now_ms: u64) -> Self {
        Self {
            direction,
            ewma_confidence: confidence,
            consecutive_ticks: 1,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
        }
    }

    fn push(&mut self, confidence: f64, now_ms: u64) {
        self.ewma_confidence =
            EWMA_ALPHA * confidence + (1.0 - EWMA_ALPHA) * self.ewma_confidence;
        self.consecutive_ticks += 1;
        self.last_seen_ms = now_ms;
    }

    fn ready(&self, now_ms: u64) -> bool {
        self.consecutive_ticks >= READY_TICKS
            && now_ms.saturating_sub(self.last_seen_ms) <= FRESHNESS_MS
    }

    /// Time-decayed score. Decay is a no-op while ticks are flowing
    /// normally; it only matters in the silence window before STALE_MS.
    fn current_score(&self, now_ms: u64) -> f64 {
        let elapsed_ms = now_ms.saturating_sub(self.last_seen_ms) as f64;
        let decay =
            (-elapsed_ms / DECAY_HALFLIFE_MS as f64 * std::f64::consts::LN_2).exp();
        self.ewma_confidence * decay
    }

    fn is_stale(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_seen_ms) > STALE_MS
    }
}

// ── Public output ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct TrendStrengthSignal {
    pub direction: TrendDirection,
    /// Decayed score — comparable in range to ExternalBtcStrategy's
    /// mean_confidence, suitable for direct use as PredictionSignal::confidence.
    pub score: f64,
    /// Undecayed EWMA — for diagnostics / reason strings only.
    pub ewma_confidence: f64,
    pub consecutive_ticks: u32,
    /// Time since this stream was first observed. NOT an activation
    /// timestamp — there is no activation event in this design.
    pub held_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TrendStrengthSnapshot {
    /// (decayed score, consecutive_ticks) — observability/UI only.
    pub bull: Option<(f64, u32)>,
    pub bear: Option<(f64, u32)>,
    /// Derived fresh on every snapshot() call — never cached, never stored.
    pub leader: Option<TrendStrengthSignal>,
}

// ── TrendStrengthDetector ───────────────────────────────────────────────────

pub struct TrendStrengthDetector {
    bull: Option<DirectionState>,
    bear: Option<DirectionState>,
}

impl TrendStrengthDetector {
    pub fn new() -> Self {
        Self { bull: None, bear: None }
    }

    /// Ingest one model output tick. No window-rollover hook — there is no
    /// reset() method. This tracker's state is intentionally long-lived
    /// across market windows.
    pub fn push(&mut self, direction: TrendDirection, confidence: f64, now_ms: u64) {
        match direction {
            TrendDirection::Bullish => {
                // Strip the opposing stream's readiness so bull/bear can
                // never simultaneously satisfy ready() — this alone
                // prevents co-leading/flicker. EWMA and timestamps on the
                // opposing stream are deliberately left untouched.
                if let Some(bear) = &mut self.bear {
                    bear.consecutive_ticks = 0;
                }
                if confidence >= CONFIDENCE_THRESHOLD {
                    match &mut self.bull {
                        Some(s) => s.push(confidence, now_ms),
                        None => {
                            self.bull = Some(DirectionState::new(direction, confidence, now_ms))
                        }
                    }
                }
            }
            TrendDirection::Bearish => {
                if let Some(bull) = &mut self.bull {
                    bull.consecutive_ticks = 0;
                }
                if confidence >= CONFIDENCE_THRESHOLD {
                    match &mut self.bear {
                        Some(s) => s.push(confidence, now_ms),
                        None => {
                            self.bear = Some(DirectionState::new(direction, confidence, now_ms))
                        }
                    }
                }
            }
            TrendDirection::Sideways => {
                // Touches neither stream. Staleness/decay handle cleanup;
                // no explicit reset on Sideways by design (unlike
                // ConvictionTracker's candidate-building phase, which does
                // reset on Sideways).
            }
        }

        if self.bull.as_ref().is_some_and(|s| s.is_stale(now_ms)) {
            self.bull = None;
        }
        if self.bear.as_ref().is_some_and(|s| s.is_stale(now_ms)) {
            self.bear = None;
        }
    }

    /// Compute the current snapshot, including derived leadership.
    /// Takes `now_ms` because score is time-dependent (decay) — there is no
    /// way to make this side-effect-free w.r.t. time without moving that
    /// dependency somewhere else.
    pub fn snapshot(&self, now_ms: u64) -> TrendStrengthSnapshot {
        let bull_score = self
            .bull
            .as_ref()
            .map(|s| (s.current_score(now_ms), s.consecutive_ticks));
        let bear_score = self
            .bear
            .as_ref()
            .map(|s| (s.current_score(now_ms), s.consecutive_ticks));

        let leader_state = match (&self.bull, &self.bear) {
            (Some(b), Some(r)) if b.ready(now_ms) && r.ready(now_ms) => {
                // Should be unreachable given the contradiction handling
                // above (one tick always strips the other's readiness), but
                // resolved defensively rather than panicking if it ever
                // does occur (e.g. future change to push() logic).
                if b.current_score(now_ms) >= r.current_score(now_ms) {
                    Some(b)
                } else {
                    Some(r)
                }
            }
            (Some(b), _) if b.ready(now_ms) => Some(b),
            (_, Some(r)) if r.ready(now_ms) => Some(r),
            _ => None,
        };

        let leader = leader_state.map(|s| TrendStrengthSignal {
            direction: s.direction,
            score: s.current_score(now_ms),
            ewma_confidence: s.ewma_confidence,
            consecutive_ticks: s.consecutive_ticks,
            held_ms: now_ms.saturating_sub(s.first_seen_ms),
        });

        TrendStrengthSnapshot { bull: bull_score, bear: bear_score, leader }
    }
}

impl Default for TrendStrengthDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contradiction_preserves_ewma_and_timestamps_but_strips_readiness() {
        let mut d = TrendStrengthDetector::new();
        let mut t = 0u64;

        for _ in 0..READY_TICKS {
            d.push(TrendDirection::Bearish, 0.90, t);
            t += 100;
        }
        let before = d.bear.clone().unwrap();
        assert!(before.ready(t));
        assert!(before.ewma_confidence > 0.0);

        // Single contradicting tick.
        d.push(TrendDirection::Bullish, 0.65, t);

        let after = d.bear.clone().unwrap();
        assert_eq!(after.consecutive_ticks, 0);
        assert!(!after.ready(t));
        // EWMA and timestamps survive untouched.
        assert_eq!(after.ewma_confidence, before.ewma_confidence);
        assert_eq!(after.first_seen_ms, before.first_seen_ms);
        assert_eq!(after.last_seen_ms, before.last_seen_ms);
    }

    #[test]
    fn bull_and_bear_never_simultaneously_ready() {
        let mut d = TrendStrengthDetector::new();
        let mut t = 0u64;

        for _ in 0..READY_TICKS {
            d.push(TrendDirection::Bullish, 0.85, t);
            t += 100;
        }
        for _ in 0..READY_TICKS {
            d.push(TrendDirection::Bearish, 0.85, t);
            t += 100;
        }

        let bull_ready = d.bull.as_ref().is_some_and(|s| s.ready(t));
        let bear_ready = d.bear.as_ref().is_some_and(|s| s.ready(t));
        assert!(!(bull_ready && bear_ready));
    }

    #[test]
    fn freshness_gates_leadership_without_evicting_stream() {
        let mut d = TrendStrengthDetector::new();
        let mut t = 0u64;

        for _ in 0..READY_TICKS {
            d.push(TrendDirection::Bullish, 0.85, t);
            t += 100;
        }

        // Immediately after going ready: leader is present.
        let snap_fresh = d.snapshot(t);
        assert!(snap_fresh.leader.is_some(), "should lead while fresh");

        // Advance past FRESHNESS_MS but before STALE_MS.
        let stale_t = t + FRESHNESS_MS + 1;
        let snap_stale = d.snapshot(stale_t);
        assert!(
            snap_stale.leader.is_none(),
            "should lose leadership after FRESHNESS_MS of silence"
        );

        // Stream itself must still exist — EWMA/timestamps are intact.
        assert!(
            d.bull.is_some(),
            "stream must survive beyond FRESHNESS_MS; only STALE_MS evicts"
        );
    }

    fn no_reset_method_exists_state_survives_indefinitely() {
        // Compile-time check via absence of API, not a runtime assertion:
        // TrendStrengthDetector has no reset()/window-rollover hook.
        let mut d = TrendStrengthDetector::new();
        for i in 0..READY_TICKS {
            d.push(TrendDirection::Bullish, 0.80, i as u64 * 100);
        }
        let snap = d.snapshot(READY_TICKS as u64 * 100);
        assert!(snap.leader.is_some());
    }
}