//! Jupiter Tokens API v2 — metadata (`usdPrice`, `mcap`, `icon`, `isVerified`, etc.).
//! See <https://developers.jup.ag/docs/guides/how-to-get-token-information>
//!
//! Retries on **429** / **503** with exponential backoff and optional `Retry-After` header,
//! so price-cron can run large backlogs without tripping Jupiter rate limits.

use anyhow::{bail, Context, Result};
use reqwest::header::RETRY_AFTER;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

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

const JUPITER_SEARCH_MAX_ATTEMPTS: u32 = 8;
const JUPITER_SEARCH_BASE_BACKOFF_MS: u64 = 400;
const JUPITER_SEARCH_MAX_BACKOFF_MS: u64 = 25_000;

fn backoff_after_rate_limit(attempt: u32) -> Duration {
    let exp = (JUPITER_SEARCH_BASE_BACKOFF_MS).saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(exp.min(JUPITER_SEARCH_MAX_BACKOFF_MS).max(JUPITER_SEARCH_BASE_BACKOFF_MS))
}

fn retry_after_from_response(resp: &reqwest::Response, attempt: u32) -> Duration {
    if let Some(h) = resp.headers().get(RETRY_AFTER) {
        if let Ok(s) = h.to_str() {
            if let Ok(secs) = s.parse::<u64>() {
                return Duration::from_secs(secs.clamp(1, 120));
            }
        }
    }
    backoff_after_rate_limit(attempt)
}

async fn fetch_search_json(
    client: &Client,
    base: &str,
    query: &str,
    api_key: &str,
) -> Result<Value> {
    let mut last_status: Option<StatusCode> = None;

    for attempt in 0..JUPITER_SEARCH_MAX_ATTEMPTS {
        let resp = client
            .get(base)
            .query(&[("query", query)])
            .header("x-api-key", api_key.trim())
            .send()
            .await
            .with_context(|| {
                format!(
                    "jupiter tokens/v2/search request failed (attempt {}/{})",
                    attempt + 1,
                    JUPITER_SEARCH_MAX_ATTEMPTS
                )
            })?;

        let status = resp.status();
        last_status = Some(status);

        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
            let wait = retry_after_from_response(&resp, attempt);
            drop(resp);
            sleep(wait).await;
            continue;
        }

        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| String::from("(body unavailable)"));
            let snippet: String = body.chars().take(280).collect();
            bail!(
                "jupiter tokens/v2/search HTTP {}: {}",
                status.as_u16(),
                snippet
            );
        }

        return resp
            .json()
            .await
            .context("jupiter tokens/v2/search JSON parse failed");
    }

    bail!(
        "jupiter tokens/v2/search: still rate-limited after {} attempts (last HTTP {:?})",
        JUPITER_SEARCH_MAX_ATTEMPTS,
        last_status
    )
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

    let v: Value = fetch_search_json(client, &base, &query, api_key).await?;

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
