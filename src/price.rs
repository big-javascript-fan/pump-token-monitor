//! Jupiter **Price API v3** (`GET …/price/v3?ids=…`) for USD quotes.
//! Matches [How to get token prices](https://developers.jup.ag/docs/guides/how-to-get-token-price):
//! batches up to 50 mints, optional `x-api-key` header.
//!
//! Cron merges results with [Tokens API](https://developers.jup.ag/docs/guides/how-to-get-token-information)
//! metadata in `jupiter_tokens` — separate endpoints.

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use serde_json::Map;

use crate::config::AppRuntime;

fn price_payload_object(v: &Value) -> Option<&Map<String, Value>> {
    // Jupiter Price API responses have historically been either:
    // 1) { "data": { "<mint>": { "usdPrice": ... } } }
    // 2) { "<mint>": { "usdPrice": ... } }  (some callers/logging paths surface only the inner `data`)
    if let Some(obj) = v.get("data").and_then(Value::as_object) {
        return Some(obj);
    }
    v.as_object()
}

pub async fn fetch_token_price_usd(
    client: &Client,
    runtime: &AppRuntime,
    mint: &str,
) -> Result<Option<f64>> {
    let url = format!(
        "{}?ids={}",
        runtime.price_api_base_url.trim_end_matches('/'),
        mint
    );
    let mut req = client.get(&url);
    if let Some(key) = runtime.jupiter_api_key.as_deref() {
        req = req.header("x-api-key", key);
    }
    let v: Value = req
        .send()
        .await
        .context("price API request failed")?
        .error_for_status()
        .context("price API returned error status")?
        .json()
        .await
        .context("failed to parse price API JSON")?;

    let price = price_payload_object(&v)
        .and_then(|d| d.get(mint))
        .and_then(|obj| obj.get("usdPrice"))
        .and_then(Value::as_f64);

    Ok(price)
}

pub async fn fetch_token_prices_usd(
    client: &Client,
    runtime: &AppRuntime,
    mints: &[String],
) -> Result<std::collections::HashMap<String, f64>> {
    if mints.is_empty() {
        return Ok(Default::default());
    }

    let ids = mints.join(",");
    let url = format!("{}?ids={}", runtime.price_api_base_url.trim_end_matches('/'), ids);
    let mut req = client.get(&url);
    if let Some(key) = runtime.jupiter_api_key.as_deref() {
        req = req.header("x-api-key", key);
    }
    let v: Value = req
        .send()
        .await
        .context("price API request failed")?
        .error_for_status()
        .context("price API returned error status")?
        .json()
        .await
        .context("failed to parse price API JSON")?;

    let mut out = std::collections::HashMap::new();
    let Some(data) = price_payload_object(&v) else {
        return Ok(out);
    };
    for (mint, obj) in data {
        if let Some(p) = obj.get("usdPrice").and_then(Value::as_f64) {
            out.insert(mint.to_string(), p);
        }
    }
    Ok(out)
}

