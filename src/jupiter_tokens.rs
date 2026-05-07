//! Jupiter Tokens API v2 — metadata (`usdPrice`, `mcap`, `icon`, `isVerified`, etc.).
//! See <https://developers.jup.ag/docs/guides/how-to-get-token-information>

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;

/// Same host as documented Tokens API (`GET /tokens/v2/search`).
const DEFAULT_JUPITER_API_ORIGIN: &str = "https://api.jup.ag";

#[derive(Debug, Clone, Default)]
pub struct TokenJupiterMeta {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub icon: Option<String>,
    pub decimals: Option<i32>,
    pub is_verified: Option<bool>,
    pub mcap_usd: Option<f64>,
    pub organic_score: Option<f64>,
    pub stats_24h_price_change_pct: Option<f64>,
}

fn opt_str(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn opt_f64(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64().or_else(|| x.as_str()?.parse().ok()))
}

fn opt_bool(v: &Value, key: &str) -> Option<bool> {
    v.get(key).and_then(Value::as_bool)
}

fn opt_i32(v: &Value, key: &str) -> Option<i32> {
    v.get(key).and_then(|x| {
        x.as_i64()
            .or_else(|| x.as_u64().map(|u| u as i64))
            .and_then(|i| i32::try_from(i).ok())
    })
}

fn parse_token_entry(v: &Value) -> Option<(String, TokenJupiterMeta)> {
    let id = opt_str(v, "id")?;
    let stats24 = v.get("stats24h");
    let pct = stats24.and_then(|s| opt_f64(s, "priceChange"));

    let meta = TokenJupiterMeta {
        name: opt_str(v, "name"),
        symbol: opt_str(v, "symbol"),
        icon: opt_str(v, "icon"),
        decimals: opt_i32(v, "decimals"),
        is_verified: opt_bool(v, "isVerified"),
        mcap_usd: opt_f64(v, "mcap"),
        organic_score: opt_f64(v, "organicScore"),
        stats_24h_price_change_pct: pct,
    };
    Some((id, meta))
}

/// Batch lookup by mint addresses using `GET /tokens/v2/search?query=mint1,mint2,...` (max 100 per Jupiter docs).
/// Requires [`crate::config::AppRuntime::jupiter_api_key`] (`x-api-key` header).
pub async fn search_tokens_by_mints(
    client: &Client,
    jupiter_api_key: Option<&str>,
    mints: &[String],
) -> Result<HashMap<String, TokenJupiterMeta>> {
    let mut out = HashMap::new();
    if mints.is_empty() {
        return Ok(out);
    }
    let Some(api_key) = jupiter_api_key.filter(|k| !k.trim().is_empty()) else {
        return Ok(out);
    };

    let query = mints.join(",");
    let base = format!(
        "{}/tokens/v2/search",
        DEFAULT_JUPITER_API_ORIGIN.trim_end_matches('/')
    );

    let v: Value = client
        .get(base)
        .query(&[("query", query.as_str())])
        .header("x-api-key", api_key.trim())
        .send()
        .await
        .context("jupiter tokens/v2/search request failed")?
        .error_for_status()
        .context("jupiter tokens/v2/search HTTP error")?
        .json()
        .await
        .context("jupiter tokens/v2/search JSON")?;

    let Some(arr) = v.as_array() else {
        return Ok(out);
    };

    for item in arr {
        if let Some((id, meta)) = parse_token_entry(item) {
            out.insert(id, meta);
        }
    }

    Ok(out)
}
