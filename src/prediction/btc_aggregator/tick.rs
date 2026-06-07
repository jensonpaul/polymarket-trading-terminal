//! Raw tick types ingested directly from exchange WebSocket feeds.
//!
//! Each [`ExchangeTick`] carries the full top-10 order book from one
//! exchange.  The pipeline processes ticks through three layers:
//!
//! 1. **Per-exchange spike gate** — rejects individual ticks that jump
//!    too far from the exchange's own recent price.
//! 2. **Cross-exchange outlier gate** — rejects ticks whose VWAP deviates
//!    beyond `threshold` MADs from the cross-exchange median.
//! 3. **Kalman smoother** — removes residual noise from the aggregated
//!    clean price.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ── Exchange identity ─────────────────────────────────────────────────────────

/// Canonical exchange identifiers.
///
/// Add new variants here when a new feed is introduced; every match
/// in the pipeline will produce a compile-time warning reminding you
/// to handle the new case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Coinbase,
    Kraken,
    Bitstamp,
}

impl Exchange {
    /// Human-readable label used in logs and metrics.
    pub fn label(self) -> &'static str {
        match self {
            Self::Binance  => "binance",
            Self::Coinbase => "coinbase",
            Self::Kraken   => "kraken",
            Self::Bitstamp => "bitstamp",
        }
    }

    /// Relative trust weight used in cross-exchange VWAP aggregation.
    ///
    /// These reflect approximate market-share and data reliability.
    /// Tune based on your observations of each feed's latency and
    /// data quality.
    pub fn trust_weight(self) -> f64 {
        match self {
            Self::Binance  => 1.0,
            Self::Coinbase => 0.9,
            Self::Kraken   => 0.85,
            Self::Bitstamp => 0.75,
        }
    }
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

// ── Level ─────────────────────────────────────────────────────────────────────

/// One price level in an order book (bid or ask).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price:  Decimal,
    pub amount: Decimal,
}

impl Level {
    #[inline]
    pub fn notional(&self) -> Decimal {
        self.price * self.amount
    }
}

// ── ExchangeTick ──────────────────────────────────────────────────────────────

/// A single order-book snapshot received from one exchange.
///
/// Contains the top-N bids and asks exactly as they arrived from the
/// WebSocket feed.  No transformation is applied before this point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeTick {
    /// Which exchange emitted this snapshot.
    pub exchange: Exchange,

    /// Wall-clock milliseconds when we received the message.
    pub received_ms: u64,

    /// Top bids, best first (highest price first).
    pub bids: Vec<Level>,

    /// Top asks, best first (lowest price first).
    pub asks: Vec<Level>,
}

impl ExchangeTick {
    /// Volume-weighted average bid price across all provided levels.
    ///
    /// Returns `None` if the bid list is empty or total size is zero.
    pub fn bid_vwap(&self) -> Option<f64> {
        vwap_of(&self.bids)
    }

    /// Volume-weighted average ask price across all provided levels.
    pub fn ask_vwap(&self) -> Option<f64> {
        vwap_of(&self.asks)
    }

    /// Mid-price computed from the VWAP of bids and asks.
    ///
    /// This is more robust than a simple best-bid / best-ask mid because
    /// it incorporates depth — a thin best level barely moves the VWAP.
    pub fn mid_vwap(&self) -> Option<f64> {
        let bid = self.bid_vwap()?;
        let ask = self.ask_vwap()?;
        Some((bid + ask) / 2.0)
    }

    /// Best bid price (highest bid).
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.price)
    }

    /// Best ask price (lowest ask).
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }

    /// Total bid-side depth in base currency.
    pub fn total_bid_size(&self) -> Decimal {
        self.bids.iter().map(|l| l.amount).sum()
    }

    /// Total ask-side depth in base currency.
    pub fn total_ask_size(&self) -> Decimal {
        self.asks.iter().map(|l| l.amount).sum()
    }

    /// Order-book imbalance in `[-1, 1]`.
    ///
    /// `+1` means all depth is on the bid side (strong buy pressure).
    /// `-1` means all depth is on the ask side (strong sell pressure).
    pub fn imbalance(&self) -> f64 {
        let bid = self.total_bid_size();
        let ask = self.total_ask_size();
        let total = bid + ask;

        if total.is_zero() {
            return 0.0;
        }

        use rust_decimal::prelude::ToPrimitive;
        let bid_f = bid.to_f64().unwrap_or(0.0);
        let ask_f = ask.to_f64().unwrap_or(0.0);
        let total_f = bid_f + ask_f;

        if total_f == 0.0 {
            0.0
        } else {
            (bid_f - ask_f) / total_f
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn vwap_of(levels: &[Level]) -> Option<f64> {
    if levels.is_empty() {
        return None;
    }

    use rust_decimal::prelude::ToPrimitive;

    let (notional_sum, size_sum) =
        levels.iter().fold((0.0_f64, 0.0_f64), |(ns, ss), lvl| {
            let p = lvl.price.to_f64().unwrap_or(0.0);
            let a = lvl.amount.to_f64().unwrap_or(0.0);
            (ns + p * a, ss + a)
        });

    if size_sum == 0.0 {
        None
    } else {
        Some(notional_sum / size_sum)
    }
}
