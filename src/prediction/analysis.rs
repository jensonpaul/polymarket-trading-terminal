use crate::prediction::{PredictionContext, PredictionSide};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BtcTrend — the complete picture of what BTC is doing right now.
//
// All dimensions are orthogonal; no thresholds or classifications are baked
// in.  Consumers (strategies) interpret these values freely.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone)]
pub struct BtcTrend {
    // ── 1. Direction ──────────────────────────────────────────────────────

    /// Which way BTC is moving relative to its window origin.
    pub side: PredictionSide,

    /// Signed `(current − origin) / origin`.  Magnitude indicates how far
    /// BTC has moved; sign mirrors `side`.
    pub distance_from_origin_pct: f64,

    // ── 2. Trend quality (Efficiency Ratio) ───────────────────────────────

    /// `|net_displacement| / path_length` in [0, 1].
    ///
    /// 1.0 → BTC moved in a straight line (clean, trustworthy trend).
    /// 0.0 → all noise, no net progress (choppy, unreliable).
    pub efficiency_ratio: f64,

    // ── 3. Volatility / Z-score ───────────────────────────────────────────

    /// Rolling coefficient-of-variation over the last 30 s.
    /// Absolute noise level on a short horizon.
    pub volatility_30s: f64,

    /// Same over the last 60 s.  Higher than `volatility_30s` after a
    /// recent calm; lower after a recent spike.
    pub volatility_60s: f64,

    /// How extreme the current price is within its own 5-minute distribution.
    /// Self-normalising.  Large positive → BTC unusually high right now.
    pub z_score: f64,

    // ── 4. Acceleration ───────────────────────────────────────────────────

    /// `current_tick_return − prev_tick_return` — second derivative of price.
    ///
    /// Positive + up-trend  → momentum is building.
    /// Negative + up-trend  → momentum is fading or reversing.
    pub acceleration: f64,

    // ── 5. Momentum persistence ───────────────────────────────────────────

    /// Fraction of elapsed window time BTC has spent on the same side as its
    /// current direction from origin.
    ///
    /// 1.0 → completely consistent trend since origin lock.
    /// 0.5 → equally split; no persistence.
    /// < 0.5 → mean-reverting character.
    pub momentum_persistence: f64,

    /// `Σ(distance_pct * dt) / elapsed_seconds` — the time-averaged signed
    /// distance from origin.  Captures both duration and magnitude of the
    /// trend in a single number.
    pub avg_distance_from_origin: f64,

    // ── Range context ─────────────────────────────────────────────────────

    /// Where BTC sits within its 5-minute high/low band.
    /// 0.0 = at the 5m low, 1.0 = at the 5m high.
    pub range_position: f64,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TokenTrend — symmetric summary for one Polymarket token side.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Clone)]
pub struct TokenTrend {
    /// Signed distance from the token's first trade price this window.
    pub distance_from_origin_pct: f64,

    /// Time-weighted AUC — the token's time-averaged signed drift.
    pub avg_distance_from_origin: f64,

    /// Orderbook imbalance: positive → buy pressure; negative → sell pressure.
    pub imbalance: f64,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MarketAnalyzer
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct MarketAnalyzer;

impl MarketAnalyzer {
    /// Compute the [`BtcTrend`] from the current context.
    ///
    /// Returns `None` if the BTC origin has not yet been locked
    /// (i.e., the 3-second warm-up has not completed).
    pub fn btc_trend(ctx: &PredictionContext) -> Option<BtcTrend> {
        let btc = &ctx.btc;

        if btc.origin_price.is_zero() {
            return None;
        }

        let side = if btc.distance_from_origin_pct >= 0.0 {
            PredictionSide::Up
        } else {
            PredictionSide::Down
        };

        Some(BtcTrend {
            side,
            distance_from_origin_pct: btc.distance_from_origin_pct,
            efficiency_ratio: btc.efficiency_ratio,
            volatility_30s: btc.volatility_30s,
            volatility_60s: btc.volatility_60s,
            z_score: btc.z_score,
            acceleration: btc.acceleration,
            momentum_persistence: btc.momentum_persistence,
            avg_distance_from_origin: btc.avg_distance_from_origin,
            range_position: btc.range_position,
        })
    }

    /// Summarise one Polymarket token side relative to its window origin.
    pub fn token_trend(
        ctx: &PredictionContext,
        side: PredictionSide,
        elapsed_seconds: f64,
    ) -> Option<TokenTrend> {
        let token = match side {
            PredictionSide::Up => &ctx.polymarket.up,
            PredictionSide::Down => &ctx.polymarket.down,
        };

        if token.origin_price.is_zero() {
            return None;
        }

        let avg_distance = if elapsed_seconds > 0.0 {
            token.area_under_curve / elapsed_seconds
        } else {
            0.0
        };

        Some(TokenTrend {
            distance_from_origin_pct: token.distance_from_origin_pct,
            avg_distance_from_origin: avg_distance,
            imbalance: token.imbalance,
        })
    }
}
