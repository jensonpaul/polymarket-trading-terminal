use crate::prediction::{
    PredictionContext,
    PredictionSide,
    DecayType,
};

#[derive(Debug, Clone)]
pub struct BtcBias {
    pub side: PredictionSide,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct HypeAnalysis {
    pub hyped_side: PredictionSide,
    pub trending_side: PredictionSide,

    pub confidence: f64,

    pub btc_alignment: f64,

    pub decay_type: DecayType,

    pub hype_score: f64,
}

pub struct MarketAnalyzer;

impl MarketAnalyzer {
    pub fn btc_bias(ctx: &PredictionContext) -> BtcBias {
        let btc = &ctx.btc;

        let mut up_score = 0.0;
        let mut down_score = 0.0;

        if btc.range_position <= 0.15 {
            up_score += 40.0;
        }

        if btc.range_position >= 0.85 {
            down_score += 40.0;
        }

        if btc.momentum_30s > 0.0 {
            up_score += btc.momentum_30s.abs() * 1000.0;
        } else {
            down_score += btc.momentum_30s.abs() * 1000.0;
        }

        if btc.momentum_60s > 0.0 {
            up_score += btc.momentum_60s.abs() * 1000.0;
        } else {
            down_score += btc.momentum_60s.abs() * 1000.0;
        }

        if up_score >= down_score {
            BtcBias {
                side: PredictionSide::Up,
                confidence: (up_score - down_score).clamp(0.0, 100.0),
            }
        } else {
            BtcBias {
                side: PredictionSide::Down,
                confidence: (down_score - up_score).clamp(0.0, 100.0),
            }
        }
    }

    pub fn hype_analysis(ctx: &PredictionContext) -> Option<HypeAnalysis> {
        let btc_bias = Self::btc_bias(ctx);

        let up_strength = Self::token_strength(
            ctx.polymarket.up.velocity,
            ctx.polymarket.up.acceleration,
            ctx.polymarket.up.imbalance,
        );

        let down_strength = Self::token_strength(
            ctx.polymarket.down.velocity,
            ctx.polymarket.down.acceleration,
            ctx.polymarket.down.imbalance,
        );

        let (hyped_side, trending_side, token_delta) = if up_strength > down_strength {
            (
                PredictionSide::Up,
                PredictionSide::Down,
                up_strength - down_strength,
            )
        } else {
            (
                PredictionSide::Down,
                PredictionSide::Up,
                down_strength - up_strength,
            )
        };

        let btc_alignment = match (btc_bias.side, hyped_side) {
            (PredictionSide::Up, PredictionSide::Up) => 1.0,
            (PredictionSide::Down, PredictionSide::Down) => 1.0,
            _ => -1.0,
        };

        let decay_rate = match hyped_side {
            PredictionSide::Up => ctx.polymarket.down.decay_rate,
            PredictionSide::Down => ctx.polymarket.up.decay_rate,
        };

        let decay_type = if decay_rate <= -0.30 {
            DecayType::Sudden
        } else if decay_rate <= -0.10 {
            DecayType::Gradual
        } else {
            DecayType::Flat
        };

        Some(HypeAnalysis {
            hyped_side,
            trending_side,
            confidence: token_delta.clamp(0.0, 100.0),
            btc_alignment,
            decay_type,
            hype_score: token_delta,
        })
    }

    #[inline]
    fn token_strength(
        velocity: f64,
        acceleration: f64,
        imbalance: f64,
    ) -> f64 {
        velocity.abs() * 100.0
            + acceleration.abs() * 100.0
            + imbalance.abs() * 25.0
    }
}
