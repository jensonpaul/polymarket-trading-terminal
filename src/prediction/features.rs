use rust_decimal::Decimal;

#[derive(Debug, Clone, Default)]
pub struct BtcFeatures {
    pub current_price: Decimal,

    pub high_5m: Decimal,
    pub low_5m: Decimal,

    pub vwap_5m: Decimal,

    pub momentum_30s: f64,
    pub momentum_60s: f64,

    pub volatility_30s: f64,
    pub volatility_60s: f64,

    pub range_position: f64,

    pub distance_from_high_pct: f64,
    pub distance_from_low_pct: f64,
}

#[derive(Debug, Clone, Default)]
pub struct TokenFeatures {
    pub current_price: Decimal,

    pub velocity: f64,

    pub acceleration: f64,

    pub decay_rate: f64,

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
