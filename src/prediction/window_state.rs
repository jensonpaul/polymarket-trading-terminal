use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// All mutable state scoped to one 5-minute prediction window.
///
/// Reset on every new `window_ts` via [`WindowState::reset`].
/// Both token sides are treated symmetrically.
#[derive(Debug, Clone)]
pub struct WindowState {
    // ── BTC origin ────────────────────────────────────────────────────────
    /// VWAP computed from BTC ticks in the first 3 s of the window.
    /// Frozen once `btc_origin_locked` is true.
    pub btc_origin_price: Decimal,

    /// True once the 3-second VWAP has been frozen.
    pub btc_origin_locked: bool,

    /// Accumulator used only during the 0–3 s lock-in phase: (price_sum, count).
    pub(crate) btc_origin_accum: (Decimal, u32),

    /// Wall-clock ms at which the window started.
    pub window_started_ms: u64,

    /// Last observed BTC price — needed for per-tick return computation.
    pub btc_last_price: Decimal,

    /// Last BTC update timestamp in ms.
    pub btc_last_update_ms: u64,

    /// Signed (current − origin) / origin for BTC, updated each tick.
    pub btc_distance_from_origin_pct: f64,

    /// Time-weighted area under the BTC distance-from-origin curve.
    /// Accumulated as `distance_pct * dt_seconds` on every tick.
    pub btc_area: f64,

    // ── BTC trend metrics ─────────────────────────────────────────────────

    /// Cumulative absolute price moves since origin lock — the path length
    /// denominator for the Efficiency Ratio.
    pub btc_path_length: f64,

    /// Return computed for the previous tick: `(prev - prev_prev) / prev_prev`.
    /// Stored so the next tick can compute `acceleration = current_return - prev_return`.
    pub btc_prev_return: f64,

    /// Most recent per-tick return: `(current - prev) / prev`.
    pub btc_current_return: f64,

    /// `current_return - prev_return` — the second derivative of price.
    /// Positive means the trend is accelerating; negative means it is fading.
    pub btc_acceleration: f64,

    /// Number of elapsed seconds BTC has spent on the same side as its current
    /// direction from origin.  Used together with elapsed time to compute
    /// momentum persistence fraction.
    pub btc_same_side_seconds: f64,

    /// Total elapsed seconds since origin lock (denominator for persistence).
    pub btc_elapsed_seconds: f64,

    // ── UP token ───────────────────────────────────────────────────────────
    pub up_origin_price: Decimal,
    pub up_last_price: Decimal,
    pub up_last_update_ms: u64,
    pub up_distance_from_origin_pct: f64,
    pub up_area: f64,

    // ── DOWN token ─────────────────────────────────────────────────────────
    pub down_origin_price: Decimal,
    pub down_last_price: Decimal,
    pub down_last_update_ms: u64,
    pub down_distance_from_origin_pct: f64,
    pub down_area: f64,
}

impl Default for WindowState {
    fn default() -> Self {
        Self {
            btc_origin_price: Decimal::ZERO,
            btc_origin_locked: false,
            btc_origin_accum: (Decimal::ZERO, 0),
            window_started_ms: 0,
            btc_last_price: Decimal::ZERO,
            btc_last_update_ms: 0,
            btc_distance_from_origin_pct: 0.0,
            btc_area: 0.0,
            btc_path_length: 0.0,
            btc_prev_return: 0.0,
            btc_current_return: 0.0,
            btc_acceleration: 0.0,
            btc_same_side_seconds: 0.0,
            btc_elapsed_seconds: 0.0,

            up_origin_price: Decimal::ZERO,
            up_last_price: Decimal::ZERO,
            up_last_update_ms: 0,
            up_distance_from_origin_pct: 0.0,
            up_area: 0.0,

            down_origin_price: Decimal::ZERO,
            down_last_price: Decimal::ZERO,
            down_last_update_ms: 0,
            down_distance_from_origin_pct: 0.0,
            down_area: 0.0,
        }
    }
}

impl WindowState {
    /// Reset to a clean slate for a new window.
    pub fn reset(&mut self, window_started_ms: u64) {
        *self = Self::default();
        self.window_started_ms = window_started_ms;
    }

    // ── BTC ───────────────────────────────────────────────────────────────

    /// Ingest one BTC price tick.
    ///
    /// During the first 3 s the price is accumulated for the origin VWAP.
    /// Once the VWAP is frozen all trend metrics update each tick.
    pub fn ingest_btc(&mut self, price: Decimal, timestamp_ms: u64) {
        // ── Lock-in phase: first tick ────────────────────────────────────────
        /*
        if !self.btc_origin_locked {
            // Use the very first tick as the origin price
            self.btc_origin_price = price;
            self.btc_origin_locked = true;

            self.btc_last_price = price;
            self.btc_last_update_ms = timestamp_ms;

            // No need to accumulate anything
            return;
        }
        */

        // ── Lock-in phase (0 – 3 s) ───────────────────────────────────────
        if !self.btc_origin_locked {
            let elapsed_ms =
                timestamp_ms.saturating_sub(self.window_started_ms);

            if elapsed_ms <= 3_000 {
                self.btc_origin_accum.0 += price;
                self.btc_origin_accum.1 += 1;
                self.btc_last_price = price;
                self.btc_last_update_ms = timestamp_ms;
                return;
            }

            // 3 s have passed — freeze the VWAP.
            self.btc_origin_price = if self.btc_origin_accum.1 > 0 {
                self.btc_origin_accum.0
                    / Decimal::from(self.btc_origin_accum.1)
            } else {
                price
            };

            self.btc_origin_locked = true;
        }

        // ── Running update ────────────────────────────────────────────────
        let origin_f = match self.btc_origin_price.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return,
        };
        let current_f = match price.to_f64() {
            Some(v) => v,
            None => return,
        };

        let dt = timestamp_ms
            .saturating_sub(self.btc_last_update_ms) as f64
            / 1_000.0;

        // Distance from origin (signed).
        let distance_pct = (current_f - origin_f) / origin_f;
        self.btc_distance_from_origin_pct = distance_pct;

        // Time-weighted AUC.
        self.btc_area += distance_pct * dt;

        // Per-tick return and derived metrics.
        if !self.btc_last_price.is_zero() {
            if let Some(prev_f) = self.btc_last_price.to_f64() {
                if prev_f > 0.0 {
                    let tick_return = (current_f - prev_f) / prev_f;

                    // Path length for Efficiency Ratio.
                    self.btc_path_length += tick_return.abs();

                    // Acceleration = second derivative of price.
                    self.btc_acceleration =
                        tick_return - self.btc_prev_return;

                    self.btc_prev_return = self.btc_current_return;
                    self.btc_current_return = tick_return;
                }
            }
        }

        // Momentum persistence: fraction of elapsed time spent on the same
        // side as current direction.
        self.btc_elapsed_seconds += dt;
        if distance_pct >= 0.0 {
            // BTC above origin
            if self.btc_area >= 0.0 {
                // Average position also above origin → consistent uptrend
                self.btc_same_side_seconds += dt;
            }
        } else {
            // BTC below origin
            if self.btc_area <= 0.0 {
                self.btc_same_side_seconds += dt;
            }
        }

        self.btc_last_price = price;
        self.btc_last_update_ms = timestamp_ms;
    }

    // ── Token helpers (symmetric) ─────────────────────────────────────────

    pub fn ingest_up(&mut self, price: Decimal, timestamp_ms: u64) {
        Self::ingest_token(
            price,
            timestamp_ms,
            &mut self.up_origin_price,
            &mut self.up_last_price,
            &mut self.up_last_update_ms,
            &mut self.up_distance_from_origin_pct,
            &mut self.up_area,
        );
    }

    pub fn ingest_down(&mut self, price: Decimal, timestamp_ms: u64) {
        Self::ingest_token(
            price,
            timestamp_ms,
            &mut self.down_origin_price,
            &mut self.down_last_price,
            &mut self.down_last_update_ms,
            &mut self.down_distance_from_origin_pct,
            &mut self.down_area,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn ingest_token(
        price: Decimal,
        timestamp_ms: u64,
        origin_price: &mut Decimal,
        last_price: &mut Decimal,
        last_update_ms: &mut u64,
        distance_from_origin_pct: &mut f64,
        area: &mut f64,
    ) {
        if origin_price.is_zero() {
            *origin_price = price;
            *last_price = price;
            *last_update_ms = timestamp_ms;
            return;
        }

        let origin_f = match origin_price.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return,
        };
        let current_f = match price.to_f64() {
            Some(v) => v,
            None => return,
        };

        let dist = (current_f - origin_f) / origin_f;
        *distance_from_origin_pct = dist;

        let dt = timestamp_ms.saturating_sub(*last_update_ms) as f64
            / 1_000.0;
        *area += dist * dt;

        *last_price = price;
        *last_update_ms = timestamp_ms;
    }
}
