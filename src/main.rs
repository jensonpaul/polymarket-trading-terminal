mod events;
mod logger;
mod market_data;
mod messages;
mod prediction;
mod reducer;
mod state;
mod ui;
mod worker;
mod worker_config;

use std::fs::OpenOptions;

use tracing_subscriber::{
    fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

use alloy::signers::Signer as _;
use std::str::FromStr;
use std::sync::Arc;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk_v2::types::{Address, U256};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::SignatureType;
use polymarket_client_sdk_v2::POLYGON;

use events::event_channel;
use logger::GuiLogger;
use state::AppState;
use crate::worker::{AuthenticatedClient, PolymarketWorker};
use worker_config::PollConfig;
use ui::PolymarketDashboardApp;
use prediction::{
    PredictionStore,
    PredictionEngine,
    PredictionService,
};

use btc_onnx_trend_model::{OnnxTrendModel, HotReloadOnnxTrendModel};
use btc_prediction_engine::{
    engine::{EngineConfig, PredictionEngine as ExternalBtcPredictionEngine},
    feeds::{BookFeedConfig, FeedConfig},
    models::TrendModelExt,
    pipeline::PipelineConfig,
    types::{Exchange, Symbol},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Ignore a missing .env file.
    let _ = dotenv::dotenv();

    let private_key = std::env::var("PRIVATE_KEY_VAR")?;
    let host = std::env::var("CLOB_API_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".into());
    let deposit_wallet = Address::from_str(&std::env::var("DEPOSIT_WALLET")?)?;

    let signer = Arc::new(
        LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON))
    );

    let client: Arc<AuthenticatedClient> = Arc::new(
        ClobClient::new(&host, Config::default())?
            .authentication_builder(signer.as_ref())
            .funder(deposit_wallet)
            .signature_type(SignatureType::Poly1271)
            .authenticate()
            .await?
    );

    // ------------------------------------------------------------------
    // Shared state (single source of truth)
    // ------------------------------------------------------------------
    let app_state = Arc::new(AppState::new());

    // ------------------------------------------------------------------
    // Prediction state
    // ------------------------------------------------------------------
    let prediction_state = Arc::new(PredictionStore::new());

    // ------------------------------------------------------------------
    // Communication channels
    // ------------------------------------------------------------------
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<messages::UiCommand>(128);
    let (event_bus, event_rx) = event_channel(512);

    // ------------------------------------------------------------------
    // Poll intervals
    // ------------------------------------------------------------------
    let poll_config = Arc::new(PollConfig::new());

    // ------------------------------------------------------------------
    // Telemetry
    // ------------------------------------------------------------------
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,polymarket_node=trace"));

    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("polymarket.log")?;

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_file.with_max_level(tracing::Level::TRACE))
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::rfc_3339());

    let stdout_layer = tracing_subscriber::fmt::layer().with_level(true);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    tracing::info!("Polymarket Trading Terminal starting");

    // ------------------------------------------------------------------
    // BTC prediction engine — initialised once here, for the lifetime of
    // the process. The FeatureState accumulators (vol_1800s, ret_300s,
    // OFI windows, etc.) must never be reset, so the engine must not be
    // owned by any view or per-window scope.
    // ------------------------------------------------------------------
    let model_path = std::env::var("ONNX_MODEL_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::current_exe()
                .expect("failed to get current exe path")
                .parent()
                .expect("exe has no parent dir")
                .join("../../models/direction_model.onnx")
        });

    tracing::info!(path = %model_path.display(), "loading ONNX model");
    let onnx_model = tokio::task::spawn_blocking(move || {
        HotReloadOnnxTrendModel::load(&model_path)
    })
    .await
    .expect("model load task panicked")
    .expect("failed to load ONNX model");
    let _guard = onnx_model.watch()?;  // keep alive — dropping it stops the watcher

    let engine_config = EngineConfig {
        pipeline: PipelineConfig {
            ext_trend: Some(Box::new(onnx_model) as Box<dyn TrendModelExt>),
            forecast_steps: vec![(5, 6), (30, 10)],
            ..PipelineConfig::default()
        },
        ..EngineConfig::default()
    };

    tracing::info!("starting BTC prediction engine");
    let (btc_engine, btc_engine_handles) =
        ExternalBtcPredictionEngine::start(engine_config).await;

    for exchange in [Exchange::Binance, Exchange::Coinbase, Exchange::Kraken, Exchange::Bitstamp] {
        btc_engine.add_feed(FeedConfig::public(exchange, Symbol::BtcUsd));
        tracing::info!(?exchange, "trade feed spawned");
    }

    for exchange in [Exchange::Binance, Exchange::Kraken, Exchange::Bitstamp] {
        btc_engine.add_book_feed(BookFeedConfig::public(exchange, Symbol::BtcUsd));
        tracing::info!(?exchange, "order-book feed spawned");
    }

    // ------------------------------------------------------------------
    // Worker
    // ------------------------------------------------------------------
    let mut worker = PolymarketWorker {
        cmd_rx,
        bus: event_bus,
        state: Arc::clone(&app_state),
        poll_config: Arc::clone(&poll_config),
        client: client.clone(),
        signer: signer.clone(),
    };

    // ------------------------------------------------------------------
    // Native window options
    // ------------------------------------------------------------------
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1600.0, 950.0])
            .with_min_inner_size([1200.0, 700.0]),
        ..Default::default()
    };

    // ------------------------------------------------------------------
    // Start eframe
    // ------------------------------------------------------------------
    eframe::run_native(
        "Polymarket Trading Terminal",
        native_options,
        Box::new(move |cc| {
            tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    tracing::error!("Worker exited with error: {e:#}");
                }
            });

            // Spawn prediction service, passing the already-running engine.
            // btc_engine_handles is moved here to keep the feed tasks alive
            // for the entire process lifetime.
            let prediction_store = Arc::clone(&prediction_state);
            tokio::spawn(async move {
                let engine = PredictionEngine::new();
                let service = PredictionService::new(
                    prediction_store,
                    engine,
                    btc_engine,
                    btc_engine_handles,
                );
                if let Err(e) = service.run().await {
                    tracing::error!("Prediction service exited: {e:#}");
                }
            });

            Ok(Box::new(PolymarketDashboardApp::new(
                cc,
                cmd_tx,
                event_rx,
                app_state,
                prediction_state,
                poll_config,
            )))
        }),
    )
    .map_err(|e| anyhow::anyhow!("eframe error: {e:?}"))?;

    Ok(())
}