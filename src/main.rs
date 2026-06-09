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
use prediction::strategies::HypeReversionStrategy;

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

    //let creds = get_or_fetch_api_creds(private_key, host.clone()).await?;

    let client: Arc<AuthenticatedClient> = Arc::new(
        ClobClient::new(&host, Config::default())?
            .authentication_builder(signer.as_ref())
            .funder(deposit_wallet)
            .signature_type(SignatureType::Poly1271)
            //.credentials(creds)
            .authenticate()
            .await?
    );

    // ------------------------------------------------------------------
    // Shared state (single source of truth)
    // ------------------------------------------------------------------
    let app_state = std::sync::Arc::new(AppState::new());

    // ------------------------------------------------------------------
    // Prediction state
    // ------------------------------------------------------------------
    let prediction_state = std::sync::Arc::new(
        PredictionStore::new()
    );

    // ------------------------------------------------------------------
    // Communication channels
    //
    // cmd_tx/cmd_rx   : UI → Worker  (user intentions requiring async I/O)
    // event_bus/event_rx : Worker/tasks → UI  (all AppEvents)
    // ------------------------------------------------------------------
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<messages::UiCommand>(128);
    let (event_bus, event_rx) = event_channel(512);

    // ------------------------------------------------------------------
    // Poll intervals (shared atomically; no message passing needed for reads)
    // ------------------------------------------------------------------
    let poll_config = std::sync::Arc::new(PollConfig::new());

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

    // GuiLogger forwards ERROR/WARN events to the toast queue.
    let gui_layer = GuiLogger { tx: event_bus.clone() };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        //.with(gui_layer)
        .init();

    tracing::info!("Polymarket Trading Terminal starting");

    // ------------------------------------------------------------------
    // Build the worker (ctx will be injected inside eframe callback)
    // ------------------------------------------------------------------
    let worker_state = std::sync::Arc::clone(&app_state);
    let worker_poll_config = std::sync::Arc::clone(&poll_config);

    let mut worker = PolymarketWorker {
        cmd_rx,
        bus: event_bus,
        state: worker_state,
        poll_config: worker_poll_config,
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
    // Start eframe; inject the egui context into the worker, then spawn it
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

            // ------------------------------------------------------------------
            // Start prediction subsystem
            // ------------------------------------------------------------------
            let prediction_store = std::sync::Arc::clone(&prediction_state);

            tokio::spawn(async move {
                let engine = PredictionEngine::new()
                    .with_strategy(HypeReversionStrategy::new());

                let service = PredictionService::new(
                    prediction_store,
                    engine,
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
