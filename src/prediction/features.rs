use rust_decimal::Decimal;

/// The complete characterisation of BTC's behaviour in the current window.
///
/// Computed once per evaluation cycle from [`WindowState`] + the rolling
/// price buffer in [`BtcFeed`].  Consumers should read these fields directly
/// — there are no thresholds or classifications baked in.
#[derive(Debug, Clone, Default)]
pub struct BtcFeatures {
    pub current_price: Decimal,

    /// VWAP frozen from the first 3 s of the window.  Zero until locked.
    pub origin_price: Decimal,

    // ── Direction ─────────────────────────────────────────────────────────

    /// `(current − origin) / origin` — signed fraction.
    /// Positive → BTC above its window origin (up-trend).
    /// Negative → BTC below its window origin (down-trend).
    pub distance_from_origin_pct: f64,

    // ── Trend quality (Efficiency Ratio) ──────────────────────────────────

    /// `|net_displacement| / path_length` in [0, 1].
    ///
    /// 1.0 = perfectly straight-line move (clean, trustworthy trend).
    /// 0.0 = all noise, no net progress (choppy, unreliable).
    ///
    /// Computed as `|distance_from_origin_pct| / btc_path_length`.
    pub efficiency_ratio: f64,

    // ── Volatility ────────────────────────────────────────────────────────

    /// Rolling std dev of log-returns over the last 30 s, normalised by mean
    /// price (coefficient of variation).  Measures absolute noise level.
    pub volatility_30s: f64,

    /// Same over the last 60 s.
    pub volatility_60s: f64,

    /// `(current_price − rolling_mean) / rolling_std` over the 5-minute
    /// window.  Self-normalising: tells you how extreme the current price is
    /// relative to its own distribution this window.
    /// A large positive z-score means BTC is unusually high right now.
    pub z_score: f64,

    // ── Acceleration ──────────────────────────────────────────────────────

    /// `current_return − prev_return` — the second derivative of price.
    ///
    /// Positive + up-trend  → trend is strengthening.
    /// Negative + up-trend  → trend is fading / reversing.
    /// Positive + down-trend → down-move is accelerating.
    pub acceleration: f64,

    // ── Momentum persistence ──────────────────────────────────────────────

    /// `btc_same_side_seconds / btc_elapsed_seconds` in [0, 1].
    ///
    /// Fraction of elapsed window time BTC has spent on the same side as its
    /// current direction from origin.  1.0 = completely consistent trend;
    /// 0.5 = equally split (no persistence); < 0.5 = mean-reverting.
    pub momentum_persistence: f64,

    /// `btc_area / btc_elapsed_seconds` — the time-averaged signed distance
    /// from origin.  Equivalent to the signed area under the distance curve
    /// normalised by elapsed time.  Non-zero when BTC has spent more time
    /// above (positive) or below (negative) its origin.
    pub avg_distance_from_origin: f64,

    // ── Range context ─────────────────────────────────────────────────────

    pub high_5m: Decimal,
    pub low_5m: Decimal,

    /// Where BTC's current price sits within its 5-minute high/low range.
    /// 0.0 = at the 5m low, 1.0 = at the 5m high.
    pub range_position: f64,
}

#[derive(Debug, Clone, Default)]
pub struct TokenFeatures {
    pub current_price: Decimal,

    /// Price at the first trade of this token in the current window.
    pub origin_price: Decimal,

    /// `(current − origin) / origin` — signed fraction.
    pub distance_from_origin_pct: f64,

    /// Time-weighted AUC: Σ(distance_from_origin_pct * dt_seconds).
    pub area_under_curve: f64,

    // Orderbook.
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub imbalance: f64,
    pub spread_pct: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PolymarketFeatures {
    pub up: TokenFeatures,
    pub down: TokenFeatures,
}
