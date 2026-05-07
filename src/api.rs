use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::config::AppRuntime;
use crate::db::{self, Db};
use crate::monitor::TokenState;
use crate::types::TokenRecord;

#[derive(Clone)]
pub struct ApiState {
    pub runtime: Arc<AppRuntime>,
    pub db: Option<Db>,
    pub mem: TokenState,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct TokenResponse {
    mint: String,
    name: String,
    first_slot: i64,
    last_slot: i64,
    first_price_usd: Option<f64>,
    price_usd: Option<f64>,
    price_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/tokens", get(list_tokens))
        .route("/tokens/:mint", get(get_token))
        .route("/tokens/:mint/prices", get(get_token_prices))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .with_state(state)
}

async fn health(State(st): State<ApiState>) -> impl IntoResponse {
    let db = st.db.as_ref().map(|_| "enabled").unwrap_or("disabled");
    Json(serde_json::json!({
        "ok": true,
        "db": db,
        "pump_program_id": st.runtime.pump_program_id,
    }))
}

async fn list_tokens(
    State(st): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(100);
    let offset = q.offset.unwrap_or(0);

    if let Some(db) = st.db.as_ref() {
        let rows = db::list_tokens(db, limit, offset)
            .await
            .map_err(internal)?;
        let out: Vec<TokenResponse> = rows
            .into_iter()
            .map(
                |(
                    mint,
                    name,
                    first_slot,
                    last_slot,
                    first_price_usd,
                    _first_price_at,
                    price_usd,
                    price_updated_at,
                )| TokenResponse {
                mint,
                name,
                first_slot,
                last_slot,
                first_price_usd,
                price_usd,
                price_updated_at,
            },
            )
            .collect();
        return Ok(Json(out));
    }

    let guard = st.mem.lock().await;
    let mut tokens: Vec<TokenRecord> = guard.values().cloned().collect();
    drop(guard);
    tokens.sort_by(|a, b| b.slot.cmp(&a.slot));

    let start = offset.max(0) as usize;
    let end = start.saturating_add(limit.max(1).min(500) as usize);
    let slice = tokens.get(start..tokens.len().min(end)).unwrap_or(&[]);

    let out: Vec<TokenResponse> = slice
        .iter()
        .map(|t| TokenResponse {
            mint: t.mint.clone(),
            name: t.name.clone(),
            first_slot: t.slot as i64,
            last_slot: t.slot as i64,
            first_price_usd: None,
            price_usd: None,
            price_updated_at: None,
        })
        .collect();

    Ok(Json(out))
}

async fn get_token(
    State(st): State<ApiState>,
    Path(mint): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if let Some(db) = st.db.as_ref() {
        let row = db::get_token(db, &mint).await.map_err(internal)?;
        if let Some((
            mint,
            name,
            first_slot,
            last_slot,
            first_price_usd,
            _first_price_at,
            price_usd,
            price_updated_at,
        )) = row
        {
            return Ok(Json(TokenResponse {
                mint,
                name,
                first_slot,
                last_slot,
                first_price_usd,
                price_usd,
                price_updated_at,
            }));
        }
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }

    let guard = st.mem.lock().await;
    let Some(t) = guard.get(&mint).cloned() else {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    };
    Ok(Json(TokenResponse {
        mint: t.mint,
        name: t.name,
        first_slot: t.slot as i64,
        last_slot: t.slot as i64,
        first_price_usd: None,
        price_usd: None,
        price_updated_at: None,
    }))
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[derive(Debug, Deserialize)]
struct PricesQuery {
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct PricePointResponse {
    ts: chrono::DateTime<chrono::Utc>,
    price_usd: f64,
}

async fn get_token_prices(
    State(st): State<ApiState>,
    Path(mint): Path<String>,
    Query(q): Query<PricesQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(db) = st.db.as_ref() else {
        return Err((StatusCode::BAD_REQUEST, "db disabled".to_string()));
    };
    let limit = q.limit.unwrap_or(240);
    let rows = db::list_price_points(db, &mint, limit).await.map_err(internal)?;
    // return ascending for charting
    let mut out: Vec<PricePointResponse> = rows
        .into_iter()
        .map(|(ts, price_usd)| PricePointResponse { ts, price_usd })
        .collect();
    out.reverse();
    Ok(Json(out))
}

