//! Integration tests for the BTC price cleaning pipeline.
//!
//! Run with: `cargo test --test pipeline_integration -- --nocapture`
//!
//! These tests simulate realistic exchange data patterns:
//! - Normal ticks from multiple exchanges
//! - Spike ticks that should be filtered
//! - Cross-exchange outliers
//! - Noisy but bounded data that Kalman should smooth

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;
    use rust_decimal::Decimal;

    // ── helpers ───────────────────────────────────────────────────────────

    use crate::prediction::btc_aggregator::{
        aggregator::{Aggregator, AggregatorConfig},
        kalman::{KalmanConfig, KalmanSmoother},
        outlier_gate::{OutlierGate, OutlierGateConfig},
        pipeline::{Pipeline, PipelineConfig},
        spike_filter::{SpikeFilter, SpikeFilterConfig},
        tick::{Exchange, ExchangeTick, Level},
    };

    fn make_level(price: f64, amount: f64) -> Level {
        Level {
            price:  Decimal::from_f64(price).unwrap(),
            amount: Decimal::from_f64(amount).unwrap(),
        }
    }

    /// Constructs a realistic tick: 10 bid levels descending, 10 ask levels ascending.
    fn make_tick(exchange: Exchange, ms: u64, mid: f64) -> ExchangeTick {
        let spread = 1.0; // $1 spread

        let bids: Vec<Level> = (0..10)
            .map(|i| make_level(mid - spread / 2.0 - i as f64 * 0.5, 0.1 + i as f64 * 0.05))
            .collect();

        let asks: Vec<Level> = (0..10)
            .map(|i| make_level(mid + spread / 2.0 + i as f64 * 0.5, 0.1 + i as f64 * 0.05))
            .collect();

        ExchangeTick { exchange, received_ms: ms, bids, asks }
    }

    // ── spike filter tests ────────────────────────────────────────────────

    #[test]
    fn spike_filter_rejects_large_jump() {
        let mut filter = SpikeFilter::new(SpikeFilterConfig {
            max_change_frac: 0.003,
            warmup_ticks: 1,
            ..Default::default()
        });

        let t1 = make_tick(Exchange::Binance, 1_000, 105_000.0);
        let t2 = make_tick(Exchange::Binance, 1_100, 105_001.0);
        // Spike: +$600 from $105_001 ≈ 0.57% > 0.3%
        let t3 = make_tick(Exchange::Binance, 1_200, 105_600.0);
        // Normal follow-up
        let t4 = make_tick(Exchange::Binance, 1_300, 105_002.0);

        assert!(filter.accept(&t1), "first tick always passes");
        assert!(filter.accept(&t2), "normal tick passes");
        assert!(!filter.accept(&t3), "spike rejected");
        assert!(filter.accept(&t4), "normal tick after spike passes");
    }

    #[test]
    fn spike_filter_independent_per_exchange() {
        let mut filter = SpikeFilter::new(SpikeFilterConfig::default());

        // Seed Binance.
        assert!(filter.accept(&make_tick(Exchange::Binance, 1_000, 105_000.0)));
        // Kraken has no history — first tick always passes regardless of value.
        assert!(filter.accept(&make_tick(Exchange::Kraken, 1_000, 106_000.0)));
    }

    // ── outlier gate tests ────────────────────────────────────────────────

    #[test]
    fn outlier_gate_rejects_divergent_exchange() {
        let mut gate = OutlierGate::new(OutlierGateConfig {
            z_threshold:    3.5,
            min_exchanges:  2,
            max_age_ms:     5_000,
        });

        // Three exchanges with similar prices.
        assert!(gate.accept(Exchange::Binance,  105_000.0, 1_000));
        assert!(gate.accept(Exchange::Coinbase, 105_002.0, 1_001));
        assert!(gate.accept(Exchange::Kraken,   104_998.0, 1_002));

        // Bitstamp reports $200 higher — outlier.
        assert!(!gate.accept(Exchange::Bitstamp, 105_300.0, 1_003));

        // But a normal Bitstamp price passes.
        assert!(gate.accept(Exchange::Bitstamp, 105_001.0, 1_004));
    }

    #[test]
    fn outlier_gate_stale_peers_excluded() {
        let mut gate = OutlierGate::new(OutlierGateConfig {
            z_threshold:   3.5,
            min_exchanges: 2,
            max_age_ms:    500, // very tight: 500 ms
        });

        // Seed two exchanges at t=0.
        assert!(gate.accept(Exchange::Binance,  105_000.0, 0));
        assert!(gate.accept(Exchange::Coinbase, 105_010.0, 0));

        // At t=1000 ms those peers are stale — gate falls back to permissive.
        // min_exchanges=2 but only 1 fresh peer → passes.
        assert!(gate.accept(Exchange::Kraken, 108_000.0, 1_000));
    }

    // ── aggregator tests ──────────────────────────────────────────────────

    #[test]
    fn aggregator_vwmp_weighted_by_volume_and_trust() {
        let mut agg = Aggregator::new(AggregatorConfig {
            bucket_ms:        250,
            min_ticks:        1,
            use_trust_weight: true,
        });

        // Two ticks in bucket 0.
        // Binance (trust=1.0, heavier volume) at 105_000
        // Bitstamp (trust=0.75, lighter volume) at 106_000
        agg.ingest(&make_tick(Exchange::Binance,  100, 105_000.0));
        agg.ingest(&make_tick(Exchange::Bitstamp, 200, 106_000.0));

        // Tick in bucket 1 closes bucket 0.
        let candle = agg.ingest(&make_tick(Exchange::Binance, 300, 105_000.0))
            .expect("candle should be emitted");

        // VWMP should be pulled toward Binance (higher trust + higher volume).
        assert!(candle.vwmp < 105_500.0, "Binance should dominate: {}", candle.vwmp);
        println!("VWMP: {:.2} (Binance=105000, Bitstamp=106000)", candle.vwmp);
    }

    #[test]
    fn aggregator_flush_returns_partial_bucket() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        agg.ingest(&make_tick(Exchange::Binance, 0, 105_000.0));

        let candle = agg.flush().expect("flush should return partial bucket");
        assert_eq!(candle.tick_count, 1);
    }

    // ── Kalman smoother tests ─────────────────────────────────────────────

    #[test]
    fn kalman_reduces_variance_on_noisy_signal() {
        let mut kf = KalmanSmoother::new(KalmanConfig {
            process_noise:  1e-4,
            measure_noise:  1e-2,
            adaptive:       false,
            adaptive_scale: 0.0,
        });

        // Simulate flat true price with ±50 noise.
        let true_price = 105_000.0_f64;
        let noise: [f64; 10] = [
            50.0, -40.0, 30.0, -20.0, 10.0,
            -30.0, 45.0, -15.0, 25.0, -5.0,
        ];

        let mut raw_mse = 0.0_f64;
        let mut kal_mse = 0.0_f64;

        kf.update(true_price, 0.0); // seed

        for &n in &noise {
            let measured = true_price + n;
            let out = kf.update(measured, 0.0);
            raw_mse += n * n;
            kal_mse += (out.estimate - true_price).powi(2);
        }

        println!("raw MSE: {raw_mse:.1}  kalman MSE: {kal_mse:.1}");
        assert!(kal_mse < raw_mse, "Kalman should reduce MSE");
    }

    #[test]
    fn kalman_adaptive_increases_smoothing_during_volatile_candle() {
        let mut kf_fixed    = KalmanSmoother::new(KalmanConfig { adaptive: false, ..Default::default() });
        let mut kf_adaptive = KalmanSmoother::new(KalmanConfig { adaptive: true, adaptive_scale: 10.0, ..Default::default() });

        kf_fixed.update(105_000.0, 0.0);
        kf_adaptive.update(105_000.0, 0.0);

        // Volatile candle (high/low range = 200 on a 105_000 mid → vol_proxy ≈ 0.0019).
        let vol_proxy = 200.0 / 105_000.0;
        let measurement = 105_100.0;

        let fixed_out    = kf_fixed.update(measurement, vol_proxy);
        let adaptive_out = kf_adaptive.update(measurement, vol_proxy);

        // Adaptive should be more conservative (smaller gain) during volatility.
        println!("fixed gain: {:.4}  adaptive gain: {:.4}", fixed_out.gain, adaptive_out.gain);
        assert!(
            adaptive_out.gain < fixed_out.gain,
            "adaptive gain should be lower during volatile candle"
        );
    }

    // ── full pipeline tests ───────────────────────────────────────────────

    #[test]
    fn full_pipeline_emits_candles_and_cleans_spikes() {
        let cfg = PipelineConfig {
            spike: SpikeFilterConfig {
                max_change_frac: 0.003,
                warmup_ticks:    3,
                ema_alpha:       0.1,
            },
            outlier: OutlierGateConfig {
                z_threshold:   3.5,
                min_exchanges: 1,
                max_age_ms:    5_000,
            },
            aggregator: AggregatorConfig {
                bucket_ms:        250,
                min_ticks:        1,
                use_trust_weight: true,
            },
            kalman: KalmanConfig {
                process_noise:  1e-4,
                measure_noise:  1e-2,
                adaptive:       true,
                adaptive_scale: 10.0,
            },
        };

        let mut pipeline = Pipeline::new(cfg);
        let mut candles_emitted = 0usize;

        // Feed 20 clean ticks spread across 3 bucket periods.
        for i in 0..20_u64 {
            let ms   = i * 50; // every 50 ms → 4 ticks per 250 ms bucket
            let price = 105_000.0 + (i as f64 * 0.5).sin() * 10.0; // gentle sine
            let tick  = make_tick(Exchange::Binance, ms, price);

            if let Some(clean) = pipeline.ingest(&tick) {
                candles_emitted += 1;
                // Kalman smoothed price should be within a few dollars of truth.
                assert!(
                    (clean.smoothed - 105_000.0).abs() < 100.0,
                    "smoothed price wildly off: {}",
                    clean.smoothed
                );
            }
        }

        // Inject a spike — should be silently dropped.
        let spike = make_tick(Exchange::Binance, 1_500, 108_000.0);
        let result = pipeline.ingest(&spike);
        assert!(result.is_none() || {
            // If it happened to close a bucket, the smoothed price
            // should still not jump $3000.
            result.map(|c| (c.smoothed - 105_000.0).abs() < 500.0).unwrap_or(true)
        });

        println!(
            "Pipeline stats: {:?}",
            pipeline.stats()
        );
        println!("Rejection rate: {:.1}%", pipeline.rejection_rate() * 100.0);
        assert!(candles_emitted >= 2, "should have emitted at least 2 candles");
    }

    #[test]
    fn pipeline_reset_smoother_clears_kalman_state() {
        let mut pipeline = Pipeline::new(PipelineConfig::default());

        // Warm up at $105_000.
        for i in 0..10_u64 {
            pipeline.ingest(&make_tick(Exchange::Binance, i * 30, 105_000.0));
        }

        // Reset (new window starting at a different price level).
        pipeline.reset_smoother();

        // First clean price after reset should seed at the new level,
        // not interpolate from $105_000.
        let tick = make_tick(Exchange::Binance, 400, 99_000.0);
        if let Some(clean) = pipeline.ingest(&tick) {
            // After reset, Kalman seeds exactly at measurement.
            assert_eq!(clean.smoothed, clean.raw_vwmp, "first post-reset price = measurement");
        }
    }
}
