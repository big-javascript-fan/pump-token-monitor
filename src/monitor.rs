use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{AppRuntime, RpcBackfillParams, OUTPUT_PATH};
use crate::db;
use crate::db::Db;
use crate::price;
use crate::pump;
use crate::rpc;
use crate::store;
use crate::types::TokenRecord;

const DEFAULT_RPC_URL_SENTINEL: &str = "https://mainnet.helius-rpc.com/?api-key=";
const ENHANCED_POLL_LIMIT: usize = 1000;

pub type TokenState = Arc<Mutex<HashMap<String, TokenRecord>>>;

pub async fn run_stream_mode(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: TokenState,
    db: Option<Db>,
    stream_mode: &str,
    rpc_backfill: RpcBackfillParams,
) -> Result<()> {
    match stream_mode {
        "logs" => run_logs_subscribe(client, runtime, state, db).await,
        "backfill" => run_rpc_backfill(client, runtime, state, db, rpc_backfill).await,
        "enhanced" => run_enhanced_backfill(client, runtime, state, db).await,
        "both" => {
            let c1 = client.clone();
            let c2 = client;
            let r1 = runtime.clone();
            let r2 = runtime;
            let s1 = state.clone();
            let s2 = state;
            let d1 = db.clone();
            let d2 = db;
            let p = rpc_backfill.clone();

            tokio::select! {
                res = run_logs_subscribe(c1, r1, s1, d1) => res,
                res = run_rpc_backfill(c2, r2, s2, d2, p) => res,
            }
        }
        other => Err(anyhow!("unsupported stream_mode in config.toml: {}", other)),
    }
}

async fn run_logs_subscribe(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: TokenState,
    dbi: Option<Db>,
) -> Result<()> {
    let ws_url = http_to_ws(&runtime.rpc_url)?;
    eprintln!("[logsSubscribe] connecting to {}", ws_url);
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed connecting websocket: {}", ws_url))?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe = serde_json::json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"logsSubscribe",
        "params":[
            { "mentions":[runtime.pump_program_id] },
            { "commitment":"confirmed" }
        ]
    });
    write
        .send(Message::Text(subscribe.to_string()))
        .await
        .context("failed sending logsSubscribe")?;
    eprintln!("[logsSubscribe] subscription request sent");

    let create_discriminator = pump::anchor_discriminator("create");
    let create_v2_discriminator = pump::anchor_discriminator("create_v2");

    while let Some(msg) = read.next().await {
        let msg = msg.context("websocket read failed")?;
        if !msg.is_text() {
            continue;
        }
        let text = msg.to_text().context("failed to read websocket text")?;
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let signature = value
            .get("params")
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("value"))
            .and_then(|v| v.get("signature"))
            .and_then(Value::as_str);

        let Some(signature) = signature else { continue };
        eprintln!("[logsSubscribe] received signature {}", pump::short_sig(signature));

        let tx = rpc::get_transaction(&client, &runtime.rpc_url, signature).await?;
        if let Some(extracted) = pump::extract_create_mint(
            &tx,
            &runtime.pump_program_id,
            &create_discriminator,
            &create_v2_discriminator,
        ) {
            let slot = tx.get("slot").and_then(Value::as_u64).unwrap_or_default();
            upsert_token(
                &state,
                &client,
                &runtime,
                dbi.as_ref(),
                TokenRecord {
                    slot,
                    name: extracted.name,
                    mint: extracted.mint,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_rpc_backfill(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: TokenState,
    dbi: Option<Db>,
    params: RpcBackfillParams,
) -> Result<()> {
    if runtime.rpc_url == DEFAULT_RPC_URL_SENTINEL {
        return Err(anyhow!(
            "set helius_api_key or rpc_url in config.toml for JSON-RPC backfill"
        ));
    }

    let create_discriminator = pump::anchor_discriminator("create");
    let create_v2_discriminator = pump::anchor_discriminator("create_v2");

    let mut before: Option<String> = None;
    let mut scanned = 0usize;

    loop {
        if scanned >= params.max_signatures {
            eprintln!(
                "[rpc:backfill] reached max_signatures limit ({})",
                params.max_signatures
            );
            break;
        }

        let page_limit = (params.max_signatures - scanned).min(params.page_size);
        eprintln!(
            "[rpc:backfill] getSignaturesForAddress page (before={}, limit={}, scanned={})",
            before.as_deref().unwrap_or("<none>"),
            page_limit,
            scanned
        );

        let signatures = rpc::get_signatures_page(
            &client,
            &runtime.rpc_url,
            &runtime.pump_program_id,
            before.as_deref(),
            page_limit,
        )
        .await?;

        if signatures.is_empty() {
            eprintln!("[rpc:backfill] no more signatures; done");
            break;
        }

        for sig_info in &signatures {
            if scanned >= params.max_signatures {
                break;
            }
            scanned += 1;

            let tx_result =
                rpc::get_transaction(&client, &runtime.rpc_url, &sig_info.signature).await;
            let tx = match tx_result {
                Ok(t) => t,
                Err(e) => {
                    eprintln!(
                        "[rpc:backfill] skip sig {} — {}",
                        pump::short_sig(&sig_info.signature),
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(params.tx_delay_ms)).await;
                    continue;
                }
            };

            if let Some(extracted) = pump::extract_create_mint(
                &tx,
                &runtime.pump_program_id,
                &create_discriminator,
                &create_v2_discriminator,
            ) {
                let slot = tx
                    .get("slot")
                    .and_then(Value::as_u64)
                    .unwrap_or_else(|| sig_info.slot.unwrap_or(0));

                upsert_token(
                    &state,
                    &client,
                    &runtime,
                    dbi.as_ref(),
                    TokenRecord {
                        slot,
                        name: extracted.name,
                        mint: extracted.mint,
                    },
                )
                .await?;
            }

            tokio::time::sleep(Duration::from_millis(params.tx_delay_ms)).await;
        }

        before = signatures.last().map(|s| s.signature.clone());
        eprintln!(
            "[rpc:backfill] progress: scanned={}, unique_tokens={}",
            scanned,
            state.lock().await.len()
        );
    }

    let total = state.lock().await.len();
    eprintln!("[rpc:backfill] finished. total unique mints: {}", total);
    Ok(())
}

async fn run_enhanced_backfill(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: TokenState,
    dbi: Option<Db>,
) -> Result<()> {
    let Some(api_key) = runtime.helius_api_key.clone() else {
        return Err(anyhow!(
            "helius_api_key is required in config.toml for enhanced mode"
        ));
    };

    let mut before: Option<String> = None;
    let create_discriminator = pump::anchor_discriminator("create");
    let create_v2_discriminator = pump::anchor_discriminator("create_v2");

    loop {
        let url = pump::enhanced_url(
            &runtime.helius_enhanced_base_url,
            &runtime.pump_program_id,
            &api_key,
            before.as_deref(),
            ENHANCED_POLL_LIMIT,
        );
        eprintln!("[enhanced] polling {}", url);
        let payload: Vec<Value> = client
            .get(&url)
            .send()
            .await
            .context("enhanced API request failed")?
            .error_for_status()
            .context("enhanced API returned error status")?
            .json()
            .await
            .context("failed to parse enhanced API response")?;
        eprintln!("[enhanced] got {} transactions", payload.len());
        if payload.is_empty() {
            eprintln!("[enhanced] backfill complete (no more transactions)");
            break;
        }

        for tx in payload.iter().rev() {
            if tx.get("signature").and_then(Value::as_str).is_none() {
                continue;
            }

            if let Some(extracted) = pump::extract_create_mint(
                tx,
                &runtime.pump_program_id,
                &create_discriminator,
                &create_v2_discriminator,
            ) {
                let slot = tx.get("slot").and_then(Value::as_u64).unwrap_or_default();
                upsert_token(
                    &state,
                    &client,
                    &runtime,
                    dbi.as_ref(),
                    TokenRecord {
                        slot,
                        name: extracted.name,
                        mint: extracted.mint,
                    },
                )
                .await?;
            }
        }

        before = payload
            .last()
            .and_then(|tx| tx.get("signature"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
    }

    let total = state.lock().await.len();
    eprintln!("[enhanced] historical backfill finished. total unique mints: {}", total);
    Ok(())
}

async fn upsert_token(
    state: &TokenState,
    client: &Client,
    runtime: &AppRuntime,
    dbi: Option<&Db>,
    record: TokenRecord,
) -> Result<()> {
    let mut guard = state.lock().await;
    if guard.contains_key(&record.mint) {
        return Ok(());
    }
    eprintln!(
        "[token] new mint={} name={:?} slot={}",
        record.mint, record.name, record.slot
    );
    let mint = record.mint.clone();
    guard.insert(mint.clone(), record);
    store::persist_tokens(OUTPUT_PATH, &guard)?;
    eprintln!("[persist] updated {}", OUTPUT_PATH);
    let inserted = guard.get(&mint).cloned().expect("just inserted token record");
    drop(guard);

    if let Some(db) = dbi {
        db::upsert_token(db, &inserted).await?;
        if let Some(price) = price::fetch_token_price_usd(client, runtime, &inserted.mint).await? {
            db::update_token_price(db, &inserted.mint, price).await?;
            eprintln!("[price] mint={} usd={}", inserted.mint, price);
        }
    }

    Ok(())
}

fn http_to_ws(http_url: &str) -> Result<String> {
    if let Some(rest) = http_url.strip_prefix("https://") {
        return Ok(format!("wss://{}", rest));
    }
    if let Some(rest) = http_url.strip_prefix("http://") {
        return Ok(format!("ws://{}", rest));
    }
    Err(anyhow!("unsupported RPC URL for websocket: {}", http_url))
}

