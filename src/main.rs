mod api;
mod config;
mod db;
mod monitor;
mod price;
mod pump;
mod rpc;
mod store;
mod types;

use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::api::ApiState;
use crate::config::build_runtime;
use crate::monitor::TokenState;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_config()?;
    let (runtime, stream_mode, rpc_backfill) = build_runtime(&config);

    let client = Client::builder()
        .user_agent("token-monitor/0.4")
        .timeout(Duration::from_secs(45))
        .build()?;

    eprintln!("Using RPC endpoint: {}", runtime.rpc_url);
    eprintln!("Using Pump program: {}", runtime.pump_program_id);
    eprintln!("Stream mode: {}", stream_mode);
    eprintln!(
        "Postgres: {}",
        runtime.database_url.as_deref().map(|_| "enabled").unwrap_or("disabled")
    );
    eprintln!("HTTP API bind: {}", runtime.http_bind);

    let runtime = Arc::new(runtime);
    let db = db::init_db(runtime.database_url.as_deref()).await?;

    let state: TokenState = Arc::new(Mutex::new(HashMap::new()));

    // HTTP API server runs alongside monitor loop.
    let api_state = ApiState {
        runtime: runtime.clone(),
        db: db.clone(),
        mem: state.clone(),
    };
    let app = api::router(api_state);

    let listener = tokio::net::TcpListener::bind(&runtime.http_bind).await?;
    let api_task = tokio::spawn(async move {
        axum::serve(listener, app).await
    });

    let price_task = if let Some(db) = db.clone() {
        let runtime = runtime.clone();
        let client = Client::builder()
            .user_agent("token-monitor/0.4")
            .timeout(Duration::from_secs(45))
            .build()?;
        Some(tokio::spawn(async move {
            run_price_cron(client, runtime, db).await
        }))
    } else {
        None
    };

    let monitor_task = tokio::spawn({
        let stream_mode = stream_mode.clone();
        async move {
            monitor::run_stream_mode(client, runtime, state, db, &stream_mode, rpc_backfill).await
        }
    });

    if let Some(price_task) = price_task {
        let (mon, api, price) = tokio::join!(monitor_task, api_task, price_task);
        mon??;
        api??;
        price??;
        return Ok(());
    }

    let (mon, api) = tokio::join!(monitor_task, api_task);
    mon??;
    api??;
    Ok(())
}

async fn run_price_cron(client: Client, runtime: Arc<config::AppRuntime>, db: db::Db) -> Result<()> {
    // Run immediately at startup, then every 30 minutes.
    let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        eprintln!("[price-cron] updating prices...");
        let ts = Utc::now();

        let mut offset = 0i64;
        let page = 2000i64;
        let batch_size = 50usize;
        let mut updated = 0u64;

        loop {
            let mints = db::list_mints(&db, page, offset).await?;
            if mints.is_empty() {
                break;
            }
            offset += page;

            for chunk in mints.chunks(batch_size) {
                let prices = price::fetch_token_prices_usd(&client, &runtime, chunk).await?;
                for (mint, p) in prices {
                    db::update_token_price(&db, &mint, p).await?;
                    db::insert_price_point(&db, &mint, ts, p).await?;
                    updated += 1;
                }
            }
        }

        eprintln!("[price-cron] done. updated={}", updated);
        interval.tick().await;
    }
}