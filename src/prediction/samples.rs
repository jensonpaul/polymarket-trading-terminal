use rust_decimal::Decimal;

use crate::prediction::storage::Timestamped;

#[derive(Debug, Clone)]
pub struct BtcSample {
    pub timestamp_ms: u64,
    pub price: Decimal,
}

impl Timestamped for BtcSample {
    #[inline]
    fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

#[derive(Debug, Clone)]
pub struct OrderbookSample {
    pub timestamp_ms: u64,

    pub asset_id: String,

    pub best_bid: Decimal,
    pub best_ask: Decimal,

    pub last_trade_price: Decimal,

    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
}

impl Timestamped for OrderbookSample {
    #[inline]
    fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}
