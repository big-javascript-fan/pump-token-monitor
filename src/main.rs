mod api;
mod config;
mod db;
mod dead_tokens;
mod jupiter_tokens;
mod monitor;
mod price;
mod pump;
mod rpc;
mod telegram;
mod trading;
mod types;

use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    if runtime.telegram_bot_token.is_some() != runtime.telegram_chat_id.is_some() {
        eprintln!(
            "Warning: set both telegram_bot_token and telegram_chat_id for Telegram alerts (one is missing)."
        );
    }

    let runtime = Arc::new(runtime);
    let db = db::init_db(runtime.database_url.as_deref()).await?;
    if let Some(ref pool) = db {
        trading::init_trade_tables(&pool.pool).await?;
    }
    let wallet = trading::parse_keypair(runtime.wallet_secret_key_base58.as_deref())?;

    let state: TokenState = Arc::new(Mutex::new(HashMap::new()));

    // HTTP API server runs alongside monitor loop.
    let api_state = ApiState {
        runtime: runtime.clone(),
        db: db.clone(),
        mem: state.clone(),
        http: client.clone(),
        wallet,
    };
    let app = api::router(api_state);

    let listener = tokio::net::TcpListener::bind(&runtime.http_bind).await?;
    let api_task = tokio::spawn(async move {
        axum::serve(listener, app).await
    });

    let telegram_notify = telegram::TelegramNotifier::from_runtime(
        client.clone(),
        runtime.telegram_bot_token.as_deref(),
        runtime.telegram_chat_id.as_deref(),
    );
    if let Some(ref tg) = telegram_notify {
        match tg.validate_on_startup().await {
            Ok(()) => eprintln!("Telegram: destination OK (getMe + getChat)."),
            Err(e) => eprintln!(
                "Warning: Telegram check failed — alerts will keep failing until this is fixed.\n       {:#}",
                e
            ),
        }
    }

    let price_task = if let Some(db) = db.clone() {
        let runtime = runtime.clone();
        let tg = telegram_notify.clone();
        let client = Client::builder()
            .user_agent("token-monitor/0.4")
            .timeout(Duration::from_secs(45))
            .build()?;
        Some(tokio::spawn(async move {
            run_price_cron(client, runtime, db, tg).await
        }))
    } else {
        None
    };

    let dead_task = db.clone().map(|db_dead| {
        let tg = telegram_notify.clone();
        tokio::spawn(async move {
            if let Err(e) = dead_tokens::run_dead_token_cron(db_dead, tg).await {
                eprintln!("dead-token cron exited: {:#}", e);
            }
        })
    });

    let monitor_task = tokio::spawn({
        let stream_mode = stream_mode.clone();
        async move {
            monitor::run_stream_mode(client, runtime, state, db, &stream_mode, rpc_backfill).await
        }
    });

    match (price_task, dead_task) {
        (Some(price_task), Some(dead_task)) => {
            let (mon, api, price, dead) = tokio::join!(monitor_task, api_task, price_task, dead_task);
            mon??;
            api??;
            price??;
            dead?;
        }
        (Some(price_task), None) => {
            let (mon, api, price) = tokio::join!(monitor_task, api_task, price_task);
            mon??;
            api??;
            price??;
        }
        (None, Some(dead_task)) => {
            let (mon, api, dead) = tokio::join!(monitor_task, api_task, dead_task);
            mon??;
            api??;
            dead?;
        }
        (None, None) => {
            let (mon, api) = tokio::join!(monitor_task, api_task);
            mon??;
            api??;
        }
    }

    Ok(())
}

const PRICE_ALERT_PCT_THRESHOLD: f64 = 20.0;
const PRICE_ALERT_COOLDOWN: Duration = Duration::from_secs(60 * 60);

async fn run_price_cron(
    client: Client,
    runtime: Arc<config::AppRuntime>,
    db: db::Db,
    telegram: Option<crate::telegram::TelegramNotifier>,
) -> Result<()> {
    // Run immediately at startup, then every 5 minutes.
    let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut alert_cooldown: HashMap<String, Instant> = HashMap::new();

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
                // Space out Jupiter Price vs Tokens calls and between cron batches to avoid 429.
                if runtime.jupiter_api_key.as_deref().is_some() {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                let jup_map = crate::jupiter_tokens::search_tokens_by_mints(
                    &client,
                    runtime.jupiter_api_key.as_deref(),
                    chunk,
                )
                .await
                .unwrap_or_else(|e| {
                    eprintln!(
                        "[price-cron] Jupiter tokens/v2/search failed for batch after retries: {:#}",
                        e
                    );
                    Default::default()
                });
                if runtime.jupiter_api_key.as_deref().is_some() {
                    tokio::time::sleep(Duration::from_millis(280)).await;
                }

                for (mint, p) in prices {
                    let row_before = db::get_token(&db, &mint).await?;

                    let jup = jup_map.get(&mint);
                    db::update_token_price_from_cron(&db, &mint, p, jup).await?;
                    db::insert_price_point(&db, &mint, ts, p).await?;
                    updated += 1;

                    let Some(ref tg) = telegram else {
                        continue;
                    };

                    let Some(old_last) = row_before
                        .as_ref()
                        .and_then(|r| r.price_usd)
                        .filter(|x| x.is_finite() && *x > 0.0)
                    else {
                        continue;
                    };
                    if !p.is_finite() || p <= 0.0 {
                        continue;
                    }

                    let pct = (p - old_last) / old_last * 100.0;
                    if !pct.is_finite() || pct.abs() < PRICE_ALERT_PCT_THRESHOLD {
                        continue;
                    }

                    let now = Instant::now();
                    if let Some(prev) = alert_cooldown.get(&mint) {
                        if now.duration_since(*prev) < PRICE_ALERT_COOLDOWN {
                            continue;
                        }
                    }

                    let name = row_before
                        .as_ref()
                        .map(|r| r.name.as_str())
                        .filter(|s| !s.trim().is_empty())
                        .unwrap_or(&mint);
                    let first_px = row_before.as_ref().and_then(|r| r.first_price_usd);

                    let msg = format!(
                        "Price move ≥{}% (vs prior cron quote)\n\
                         Name: {}\n\
                         Contract: {}\n\
                         First price USD: {}\n\
                         Previous USD: {}\n\
                         New USD: {}\n\
                         Change vs prior: {:+.2}%",
                        PRICE_ALERT_PCT_THRESHOLD as i32,
                        name,
                        mint,
                        crate::telegram::fmt_usd_label(first_px),
                        crate::telegram::fmt_usd_label(Some(old_last)),
                        crate::telegram::fmt_usd_label(Some(p)),
                        pct,
                    );

                    // match tg.send_plain(&msg).await {
                    //     Ok(()) => {
                    //         alert_cooldown.insert(mint.clone(), now);
                    //     }
                    //     Err(e) => {
                    //         eprintln!("[price-cron] telegram send failed for {}: {:#}", mint, e);
                    //     }
                    // }
                }
            }
        }

        eprintln!("[price-cron] done. updated={}", updated);
        interval.tick().await;
    }
}