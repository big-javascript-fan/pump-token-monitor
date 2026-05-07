use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

const MAX_RPC_RETRIES: usize = 7;
const INITIAL_RETRY_BACKOFF_MS: u64 = 500;

#[derive(Debug, Deserialize)]
pub struct SignatureInfo {
    pub signature: String,
    #[serde(default)]
    pub slot: Option<u64>,
    #[serde(rename = "blockTime")]
    #[allow(dead_code)]
    pub block_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

pub async fn get_signatures_page(
    client: &Client,
    rpc_url: &str,
    program_id: &str,
    before: Option<&str>,
    limit: usize,
) -> Result<Vec<SignatureInfo>> {
    let mut cfg = json!({
        "limit": limit,
        "commitment": "confirmed",
    });
    if let Some(b) = before {
        cfg["before"] = json!(b);
    }

    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [program_id, cfg],
    });

    let response: RpcResponse<Vec<SignatureInfo>> = post_rpc(client, rpc_url, payload).await?;
    if let Some(err) = response.error {
        return Err(anyhow!("RPC error {}: {}", err.code, err.message));
    }
    Ok(response.result.unwrap_or_default())
}

pub async fn get_transaction(client: &Client, rpc_url: &str, signature: &str) -> Result<Value> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }
        ]
    });

    let response: RpcResponse<Value> = post_rpc(client, rpc_url, payload).await?;
    if let Some(err) = response.error {
        return Err(anyhow!("RPC error {}: {}", err.code, err.message));
    }
    response
        .result
        .ok_or_else(|| anyhow!("missing transaction for signature {}", signature))
}

async fn post_rpc<T: for<'de> Deserialize<'de>>(
    client: &Client,
    rpc_url: &str,
    payload: Value,
) -> Result<T> {
    let mut backoff_ms = INITIAL_RETRY_BACKOFF_MS;
    let method_name = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");

    for attempt in 1..=MAX_RPC_RETRIES {
        eprintln!("[rpc:http] {} attempt {}/{}", method_name, attempt, MAX_RPC_RETRIES);
        let response = client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC call failed: {}", method_name))?;

        let status = response.status();
        let body = response.text().await.context("failed reading RPC body")?;
        if status.as_u16() == 429 && attempt < MAX_RPC_RETRIES {
            eprintln!("[rpc:http] 429 on {}, backing off {}ms", method_name, backoff_ms);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(15_000);
            continue;
        }
        if !status.is_success() {
            return Err(anyhow!("RPC HTTP {}: {}", status, body));
        }
        return serde_json::from_str(&body).context("failed parsing RPC response JSON");
    }

    Err(anyhow!("exhausted HTTP retries for RPC method {}", method_name))
}

