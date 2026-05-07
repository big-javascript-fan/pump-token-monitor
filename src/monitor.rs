use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{AppRuntime, RpcBackfillParams};
use crate::db;
use crate::db::Db;
use crate::price;
use crate::pump;
use crate::rpc;
use crate::types::TokenRecord;

const DEFAULT_RPC_URL_SENTINEL: &str = "https://mainnet.helius-rpc.com/?api-key=";
const ENHANCED_POLL_LIMIT: usize = 1000;
/// WSS can hang indefinitely if outbound 443 is blocked or the host never completes TLS.
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

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

fn redact_sensitive_ws_url(url: &str) -> String {
    const NEEDLE: &str = "api-key=";
    if let Some(i) = url.find(NEEDLE) {
        let start_val = i + NEEDLE.len();
        let val_end = url[start_val..]
            .find('&')
            .map(|p| start_val + p)
            .unwrap_or(url.len());
        format!("{}<redacted>{}", &url[..start_val], &url[val_end..])
    } else {
        url.to_string()
    }
}

fn json_rpc_request_id(value: &Value) -> Option<i64> {
    value.get("id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
    })
}

async fn run_logs_subscribe(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: TokenState,
    dbi: Option<Db>,
) -> Result<()> {
    let ws_url = http_to_ws(&runtime.rpc_url)?;
    let ws_url_log = redact_sensitive_ws_url(&ws_url);
    eprintln!(
        "[logsSubscribe] connecting WebSocket for pump program {}\n[logsSubscribe] ws endpoint: {}",
        runtime.pump_program_id, ws_url_log
    );

    if runtime.rpc_url == DEFAULT_RPC_URL_SENTINEL {
        eprintln!(
            "[logsSubscribe] warning: rpc_url looks like an empty Helius placeholder — websocket may fail; set helius_api_key or a full rpc_url"
        );
    }

    eprintln!(
        "[logsSubscribe] dialing WebSocket (TCP → TLS → HTTP upgrade); timeout {:?}…",
        WS_CONNECT_TIMEOUT
    );
    let connect_result =
        tokio::time::timeout(WS_CONNECT_TIMEOUT, connect_async(ws_url.as_str())).await;

    let (ws_stream, response) = match connect_result {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            return Err(anyhow::Error::from(e).context(format!(
                "WebSocket connect_async failed (TLS/DNS/upgrade rejected?). Endpoint: {}.\n\
                 Common causes: wrong rpc_url for WSS, missing api-key on provider URL, \
                 provider disallows websocket on this tier, or certificate/proxy issues.",
                ws_url_log
            )));
        }
        Err(_elapsed) => {
            return Err(anyhow!(
                "WebSocket connect timed out after {:?} — never finished TCP/TLS/WebSocket upgrade.\n\
                 Endpoint: {}.\n\
                 This usually means: outbound HTTPS (443) blocked by firewall/security group; \
                 host unreachable from this server; DNS stuck; or RPC allows HTTP POST only (no WSS on same host).\n\
                 Try from the server: `curl -sI \"{}\"` (replace ws→http if needed) and confirm provider docs for websocket URL.",
                WS_CONNECT_TIMEOUT,
                ws_url_log,
                ws_url_log.replace("wss://", "https://").replace("ws://", "http://")
            ));
        }
    };

    eprintln!(
        "[logsSubscribe] WebSocket handshake OK (HTTP status {})",
        response.status()
    );
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
    eprintln!("[logsSubscribe] sent logsSubscribe JSON-RPC request (id=1), waiting for ack…");

    let subscription_ack = Arc::new(AtomicBool::new(false));
    let logs_notification_count = Arc::new(AtomicU64::new(0));

    let ack_hb = Arc::clone(&subscription_ack);
    let count_hb = Arc::clone(&logs_notification_count);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(120)).await;
            let ack = ack_hb.load(Ordering::Relaxed);
            let n = count_hb.load(Ordering::Relaxed);
            if ack && n > 0 {
                eprintln!(
                    "[logsSubscribe] heartbeat (every 120s): OK — subscription active, logs_notifications total={}",
                    n
                );
            } else if !ack {
                eprintln!(
                    "[logsSubscribe] heartbeat (every 120s): subscription_acknowledged=false notifications_seen={}\n\
                     → RPC never acknowledged logsSubscribe (id=1): wrong WS URL, plan without websocket, or unsupported.",
                    n
                );
            } else {
                eprintln!(
                    "[logsSubscribe] heartbeat (every 120s): subscribed OK but logs_notifications=0\n\
                     → No logs notifications yet — wrong pump_program_id filter, or RPC lag / idle slice."
                );
            }
        }
    });

    let create_discriminator = pump::anchor_discriminator("create");
    let create_v2_discriminator = pump::anchor_discriminator("create_v2");

    let mut json_parse_warn_remaining: u8 = 8;
    let mut non_text_logged: u8 = 0;

    while let Some(msg) = read.next().await {
        let msg = msg.context("websocket read failed")?;
        if !msg.is_text() {
            if non_text_logged < 6 {
                non_text_logged += 1;
                eprintln!(
                    "[logsSubscribe] non-text websocket frame (#{}): {:?} (often Ping/Pong — ok)",
                    non_text_logged, msg
                );
            }
            continue;
        }
        let text = msg.to_text().context("failed to read websocket text")?;
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                if json_parse_warn_remaining > 0 {
                    json_parse_warn_remaining -= 1;
                    let preview: String = text.chars().take(220).collect();
                    eprintln!(
                        "[logsSubscribe] skipped invalid JSON ({} remaining samples): {} — preview {:?}",
                        json_parse_warn_remaining, e, preview
                    );
                }
                continue;
            }
        };

        if json_rpc_request_id(&value) == Some(1) {
            if let Some(err) = value.get("error") {
                eprintln!(
                    "[logsSubscribe] logsSubscribe FAILED (RPC error on id=1): {}\
                     \n    Fix: use an RPC that supports Solana websocket logsSubscribe on this URL (many HTTP-only endpoints do not).",
                    err
                );
                continue;
            }
            if value.get("result").is_some() {
                subscription_ack.store(true, Ordering::Relaxed);
                eprintln!(
                    "[logsSubscribe] subscription ACK OK from RPC: result={:?}",
                    value.get("result")
                );
                continue;
            }
        }

        if value
            .get("method")
            .and_then(|m| m.as_str())
            .is_some_and(|m| m == "logsNotification")
        {
            let n = logs_notification_count.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 {
                eprintln!(
                    "[logsSubscribe] first logsNotification received — logsSubscribe stream is LIVE"
                );
            } else if n > 0 && n % 1000 == 0 {
                eprintln!(
                    "[logsSubscribe] logsNotification running total={}",
                    n
                );
            }
        }

        let signature = value
            .get("params")
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("value"))
            .and_then(|v| v.get("signature"))
            .and_then(Value::as_str);

        let Some(signature) = signature else { continue };

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

    eprintln!(
        "[logsSubscribe] websocket read stream ended (connection closed or dropped)"
    );
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

