use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_keypair::Keypair;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::config::AppRuntime;
use crate::db::{self, Db};
use crate::monitor::TokenState;
use crate::trading;
use crate::types::TokenRecord;

const DEFAULT_TRADE_SLIPPAGE_BPS: u16 = 100;

#[derive(Clone)]
pub struct ApiState {
    pub runtime: Arc<AppRuntime>,
    pub db: Option<Db>,
    pub mem: TokenState,
    pub http: Client,
    pub wallet: Option<Arc<Keypair>>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    /// Sorting: `first_seen` / `last_seen` / `change_desc` / `change_asc` (aliases in `TokenListSort::parse`).
    sort: Option<String>,
    /// Case-insensitive substring match on token name or mint.
    search: Option<String>,
}

#[derive(Debug, Serialize)]
struct TokenResponse {
    mint: String,
    name: String,
    first_slot: i64,
    last_slot: i64,
    first_seen: Option<chrono::DateTime<chrono::Utc>>,
    last_seen: Option<chrono::DateTime<chrono::Utc>>,
    first_price_usd: Option<f64>,
    price_usd: Option<f64>,
    /// `(price − first_price) / first_price * 100` when both sides exist and first ≠ 0.
    price_change_pct: Option<f64>,
    price_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    dead_token: bool,
    dead_marked_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn price_change_pct(first_price_usd: Option<f64>, price_usd: Option<f64>) -> Option<f64> {
    let f = first_price_usd?;
    let p = price_usd?;
    if f == 0.0 || !f.is_finite() || !p.is_finite() {
        return None;
    }
    Some((p - f) / f * 100.0)
}

fn token_row_to_response(
    (
        mint,
        name,
        first_slot,
        last_slot,
        first_price_usd,
        _first_price_at,
        price_usd,
        price_updated_at,
        first_seen,
        last_seen,
        dead_token,
        dead_marked_at,
    ): db::DbTokenRow,
) -> TokenResponse {
    TokenResponse {
        mint,
        name,
        first_slot,
        last_slot,
        first_seen: Some(first_seen),
        last_seen: Some(last_seen),
        first_price_usd,
        price_usd,
        price_change_pct: price_change_pct(first_price_usd, price_usd),
        price_updated_at,
        dead_token,
        dead_marked_at,
    }
}

fn validate_solana_mint(mint: &str) -> Result<(), String> {
    let t = mint.trim();
    if t.is_empty() {
        return Err("mint is required".to_string());
    }
    let raw = bs58::decode(t)
        .into_vec()
        .map_err(|_| "mint must be valid base58".to_string())?;
    if raw.len() != 32 {
        return Err(format!(
            "mint must be a Solana address (32 bytes when decoded; got {} bytes)",
            raw.len()
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RegisterTokenBody {
    mint: String,
    name: String,
}

async fn register_token(
    State(st): State<ApiState>,
    Json(body): Json<RegisterTokenBody>,
) -> Result<Json<TokenResponse>, (StatusCode, String)> {
    let mint = body.mint.trim().to_string();
    validate_solana_mint(&mint).map_err(|m| (StatusCode::BAD_REQUEST, m))?;

    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name is required".to_string()));
    }
    if name.len() > 512 {
        return Err((StatusCode::BAD_REQUEST, "name too long (max 512 chars)".to_string()));
    }

    let record = TokenRecord {
        slot: 0,
        name: name.clone(),
        mint: mint.clone(),
    };

    if let Some(db) = st.db.as_ref() {
        db::upsert_token(db, &record).await.map_err(internal)?;
        db::clear_dead_flag(db, &mint).await.map_err(internal)?;
        match crate::price::fetch_token_price_usd(&st.http, &st.runtime, &mint).await {
            Ok(Some(px)) => {
                let _ = db::update_token_price(db, &mint, px).await;
            }
            _ => {}
        }
        let row = db::get_token(db, &mint).await.map_err(internal)?;
        let Some(row) = row else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "token missing after insert".to_string(),
            ));
        };
        let resp = token_row_to_response(row);
        {
            let mut g = st.mem.lock().await;
            g.insert(mint.clone(), record);
        }
        return Ok(Json(resp));
    }

    {
        let mut g = st.mem.lock().await;
        g.insert(mint.clone(), record.clone());
    }

    Ok(Json(TokenResponse {
        mint: record.mint,
        name: record.name,
        first_slot: 0,
        last_slot: 0,
        first_seen: None,
        last_seen: None,
        first_price_usd: None,
        price_usd: None,
        price_change_pct: None,
        price_updated_at: None,
        dead_token: false,
        dead_marked_at: None,
    }))
}

#[derive(Debug, Deserialize)]
struct BatchQuery {
    /// Comma-separated mint addresses (max 80).
    mints: String,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/tokens", get(list_tokens).post(register_token))
        .route("/tokens-batch", get(list_tokens_batch))
        .route("/tokens/:mint", get(get_token))
        .route("/tokens/:mint/prices", get(get_token_prices))
        .route("/trades/positions", get(list_trade_positions))
        .route("/trades/buy", post(trade_buy))
        .route("/trades/sell", post(trade_sell))
        .route("/trades/estimate-buy", get(estimate_buy))
        .route("/trades/estimate-sell", get(estimate_sell))
        .route("/trades/sell-history", get(sell_history_bundle))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .with_state(state)
}

async fn health(State(st): State<ApiState>) -> impl IntoResponse {
    let db = st.db.as_ref().map(|_| "enabled").unwrap_or("disabled");
    let wallet = if st.wallet.is_some() {
        "configured"
    } else {
        "missing_wallet_secret_key_base58"
    };
    Json(serde_json::json!({
        "ok": true,
        "db": db,
        "wallet": wallet,
        "pump_program_id": st.runtime.pump_program_id,
        "tokens_sort": serde_json::json!([
            "first_seen", "last_seen", "change_desc", "change_asc"
        ]),
    }))
}

async fn list_tokens_batch(
    State(st): State<ApiState>,
    Query(q): Query<BatchQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(pool) = st.db.as_ref() else {
        return Err((StatusCode::BAD_REQUEST, "database disabled".to_string()));
    };

    let mints: Vec<String> = q
        .mints
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .take(80)
        .collect();

    if mints.is_empty() {
        return Ok(Json(Vec::<TokenResponse>::new()));
    }

    let mut rows = db::list_tokens_by_mints(pool, &mints).await.map_err(internal)?;
    rows.sort_by_key(|row| {
        mints
            .iter()
            .position(|m| m == &row.0)
            .unwrap_or(usize::MAX)
    });

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
                first_seen,
                last_seen,
                dead_token,
                dead_marked_at,
            )| TokenResponse {
                mint,
                name,
                first_slot,
                last_slot,
                first_seen: Some(first_seen),
                last_seen: Some(last_seen),
                first_price_usd,
                price_usd,
                price_change_pct: price_change_pct(first_price_usd, price_usd),
                price_updated_at,
                dead_token,
                dead_marked_at,
            },
        )
        .collect();
    Ok(Json(out))
}

async fn list_tokens(
    State(st): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(100);
    let offset = q.offset.unwrap_or(0);
    if let Some(pool) = st.db.as_ref() {
        let sort = db::TokenListSort::parse(q.sort.as_deref());
        let rows = db::list_tokens(pool, limit, offset, sort, q.search.as_deref())
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
                    first_seen,
                    last_seen,
                    dead_token,
                    dead_marked_at,
                )| TokenResponse {
                mint,
                name,
                first_slot,
                last_slot,
                first_seen: Some(first_seen),
                last_seen: Some(last_seen),
                first_price_usd,
                price_usd,
                price_change_pct: price_change_pct(first_price_usd, price_usd),
                price_updated_at,
                dead_token,
                dead_marked_at,
            },
            )
            .collect();
        return Ok(Json(out));
    }

    let needle = q
        .search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let guard = st.mem.lock().await;
    let mut tokens: Vec<TokenRecord> = guard.values().cloned().collect();
    drop(guard);

    if let Some(n) = needle {
        let nl = n.to_lowercase();
        tokens.retain(|t| {
            t.name.to_lowercase().contains(&nl) || t.mint.to_lowercase().contains(&nl)
        });
    }

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
            first_seen: None,
            last_seen: None,
            first_price_usd: None,
            price_usd: None,
            price_change_pct: None,
            price_updated_at: None,
            dead_token: false,
            dead_marked_at: None,
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
            first_seen,
            last_seen,
            dead_token,
            dead_marked_at,
        )) = row
        {
            return Ok(Json(TokenResponse {
                mint,
                name,
                first_slot,
                last_slot,
                first_seen: Some(first_seen),
                last_seen: Some(last_seen),
                first_price_usd,
                price_usd,
                price_change_pct: price_change_pct(first_price_usd, price_usd),
                price_updated_at,
                dead_token,
                dead_marked_at,
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
        first_seen: None,
        last_seen: None,
        first_price_usd: None,
        price_usd: None,
        price_change_pct: None,
        price_updated_at: None,
        dead_token: false,
        dead_marked_at: None,
    }))
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[derive(Debug, Deserialize)]
struct PricesQuery {
    limit: Option<i64>,
    /// RFC3339 / ISO-8601 (e.g. `2026-05-07T12:00:00Z`): only rows with `ts >= from` are returned.
    from: Option<String>,
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
    let from_ts = match &q.from {
        None => None,
        Some(s) if s.is_empty() => None,
        Some(s) => match chrono::DateTime::parse_from_rfc3339(s) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid `from`: expected RFC3339 timestamp".to_string(),
                ));
            }
        },
    };
    let rows = db::list_price_points(db, &mint, limit, from_ts)
        .await
        .map_err(internal)?;
    // return ascending for charting
    let mut out: Vec<PricePointResponse> = rows
        .into_iter()
        .map(|(ts, price_usd)| PricePointResponse { ts, price_usd })
        .collect();
    out.reverse();
    Ok(Json(out))
}

#[derive(Debug, Deserialize)]
struct TradeBuyBody {
    mint: String,
    sol_amount: f64,
}

#[derive(Debug, Deserialize)]
struct TradeSellBody {
    position_id: i64,
    token_amount: String,
}

#[derive(Debug, Serialize)]
struct PositionDto {
    id: i64,
    mint: String,
    token_name: String,
    buy_price_usd: f64,
    buy_at: chrono::DateTime<chrono::Utc>,
    token_amount_remaining: f64,
    current_price_usd: Option<f64>,
    unrealized_profit_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SellHistoryQuery {
    from: Option<String>,
    to: Option<String>,
}

#[derive(Debug, Serialize)]
struct MintProfitSlice {
    mint: String,
    token_name: String,
    profit_usd: f64,
}

#[derive(Debug, Serialize)]
struct DayProfitBar {
    day: String,
    profit_usd: f64,
}

#[derive(Debug, Serialize)]
struct SellHistoryRowResponse {
    id: i64,
    position_id: Option<i64>,
    mint: String,
    token_name: String,
    sell_tx_signature: Option<String>,
    sold_at: chrono::DateTime<chrono::Utc>,
    buy_price_usd: f64,
    sell_price_usd: f64,
    token_decimals: i16,
    tokens_sold_raw: String,
    amount_sold_human: f64,
    sol_received_lamports: Option<i64>,
    profit_usd: f64,
    closed_position: bool,
}

fn map_sell_history_row(r: trading::SellRow) -> SellHistoryRowResponse {
    let amount_sold_human = trading::sell_qty_human(&r);
    let profit_usd = trading::sell_realized_profit_usd(&r);
    SellHistoryRowResponse {
        id: r.id,
        position_id: r.position_id,
        mint: r.mint,
        token_name: r.token_name,
        sell_tx_signature: r.sell_tx_signature,
        sold_at: r.sold_at,
        buy_price_usd: r.buy_price_usd,
        sell_price_usd: r.sell_price_usd,
        token_decimals: r.token_decimals,
        tokens_sold_raw: r.tokens_sold_raw,
        amount_sold_human,
        sol_received_lamports: r.sol_received_lamports,
        profit_usd,
        closed_position: r.closed_position,
    }
}

#[derive(Debug, Serialize)]
struct SellHistoryBundle {
    items: Vec<SellHistoryRowResponse>,
    profit_by_mint: Vec<MintProfitSlice>,
    profit_by_day: Vec<DayProfitBar>,
    total_profit_usd: f64,
    trades_winning: usize,
    trades_losing: usize,
}

async fn list_trade_positions(
    State(st): State<ApiState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(db) = st.db.as_ref() else {
        return Err((StatusCode::BAD_REQUEST, "database disabled".to_string()));
    };
    let rows = trading::open_positions_with_pnl(&st.http, &st.runtime, db)
        .await
        .map_err(internal)?;
    let out: Vec<PositionDto> = rows
        .into_iter()
        .map(|p| {
            let factor = 10_f64.powi(p.row.token_decimals as i32);
            let rem: u128 = p.row.tokens_remaining_raw.parse().unwrap_or(0);
            PositionDto {
                id: p.row.id,
                mint: p.row.mint,
                token_name: p.row.token_name,
                buy_price_usd: p.row.buy_price_usd,
                buy_at: p.row.buy_at,
                token_amount_remaining: rem as f64 / factor,
                current_price_usd: p.current_price_usd,
                unrealized_profit_usd: p.unrealized_profit_usd,
            }
        })
        .collect();
    Ok(Json(out))
}

async fn trade_buy(
    State(st): State<ApiState>,
    Json(body): Json<TradeBuyBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(wallet) = st.wallet.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "set wallet_secret_key_base58 in config.toml".to_string(),
        ));
    };
    let Some(db) = st.db.clone() else {
        return Err((StatusCode::BAD_REQUEST, "database disabled".to_string()));
    };

    let mint = body.mint.trim();
    if mint.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "mint required".to_string()));
    }

    let token_name = db::get_token(&db, mint)
        .await
        .map_err(internal)?
        .map(|r| r.1)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| mint.to_string());

    let row = trading::buy_and_open_position(
        &st.http,
        &st.runtime,
        &db,
        wallet.as_ref(),
        mint,
        &token_name,
        body.sol_amount,
        DEFAULT_TRADE_SLIPPAGE_BPS,
    )
    .await
    .map_err(internal)?;

    let px_live = crate::price::fetch_token_price_usd(&st.http, &st.runtime, mint)
        .await
        .ok()
        .flatten();

    let factor = 10_f64.powi(row.token_decimals as i32);
    let rem: u128 = row.tokens_remaining_raw.parse().unwrap_or(0);
    Ok(Json(PositionDto {
        id: row.id,
        mint: row.mint,
        token_name: row.token_name,
        buy_price_usd: row.buy_price_usd,
        buy_at: row.buy_at,
        token_amount_remaining: rem as f64 / factor,
        current_price_usd: px_live.or(Some(row.buy_price_usd)),
        unrealized_profit_usd: Some(0.0),
    }))
}

async fn trade_sell(
    State(st): State<ApiState>,
    Json(body): Json<TradeSellBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(wallet) = st.wallet.clone() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "set wallet_secret_key_base58 in config.toml".to_string(),
        ));
    };
    let Some(db) = st.db.clone() else {
        return Err((StatusCode::BAD_REQUEST, "database disabled".to_string()));
    };

    let amt = rust_decimal::Decimal::from_str_exact(body.token_amount.trim()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "token_amount must be a decimal number".to_string(),
        )
    })?;

    trading::apply_sell_position(
        &st.http,
        &st.runtime,
        wallet.as_ref(),
        &db,
        body.position_id,
        amt,
        DEFAULT_TRADE_SLIPPAGE_BPS,
    )
    .await
    .map_err(internal)?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
struct EstimateBuyQuery {
    mint: String,
    sol_amount: f64,
}

#[derive(Debug, Serialize)]
struct EstimateBuyResponse {
    sol_amount: f64,
    sol_price_usd: Option<f64>,
    token_price_usd: Option<f64>,
    estimated_tokens: Option<f64>,
    estimated_usd_spent: Option<f64>,
}

async fn estimate_buy(
    State(st): State<ApiState>,
    Query(q): Query<EstimateBuyQuery>,
) -> Result<Json<EstimateBuyResponse>, (StatusCode, String)> {
    let mint = q.mint.trim();
    if mint.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "mint required".to_string()));
    }
    if !q.sol_amount.is_finite() || q.sol_amount <= 0.0 {
        return Err((StatusCode::BAD_REQUEST, "sol_amount must be positive".to_string()));
    }

    let sol_px =
        crate::price::fetch_token_price_usd(&st.http, &st.runtime, trading::SOL_MINT)
            .await
            .map_err(internal)?;
    let token_px = crate::price::fetch_token_price_usd(&st.http, &st.runtime, mint)
        .await
        .map_err(internal)?;

    let estimated_usd_spent = match sol_px {
        Some(sp) if sp.is_finite() && sp > 0.0 => Some(q.sol_amount * sp),
        _ => None,
    };
    let estimated_tokens = match (estimated_usd_spent, token_px) {
        (Some(usd), Some(tp)) if tp.is_finite() && tp > 0.0 => Some(usd / tp),
        _ => None,
    };

    Ok(Json(EstimateBuyResponse {
        sol_amount: q.sol_amount,
        sol_price_usd: sol_px,
        token_price_usd: token_px,
        estimated_tokens,
        estimated_usd_spent,
    }))
}

#[derive(Debug, Deserialize)]
struct EstimateSellQuery {
    mint: String,
    token_amount: f64,
}

#[derive(Debug, Serialize)]
struct EstimateSellResponse {
    token_amount: f64,
    token_price_usd: Option<f64>,
    estimated_usd: Option<f64>,
    sol_price_usd: Option<f64>,
    estimated_sol: Option<f64>,
}

async fn estimate_sell(
    State(st): State<ApiState>,
    Query(q): Query<EstimateSellQuery>,
) -> Result<Json<EstimateSellResponse>, (StatusCode, String)> {
    let mint = q.mint.trim();
    if mint.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "mint required".to_string()));
    }
    if !q.token_amount.is_finite() || q.token_amount <= 0.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "token_amount must be positive".to_string(),
        ));
    }

    let sol_px =
        crate::price::fetch_token_price_usd(&st.http, &st.runtime, trading::SOL_MINT)
            .await
            .map_err(internal)?;
    let token_px = crate::price::fetch_token_price_usd(&st.http, &st.runtime, mint)
        .await
        .map_err(internal)?;

    let estimated_usd = match token_px {
        Some(tp) if tp.is_finite() && tp > 0.0 => Some(q.token_amount * tp),
        _ => None,
    };
    let estimated_sol = match (estimated_usd, sol_px) {
        (Some(usd), Some(sp)) if sp.is_finite() && sp > 0.0 => Some(usd / sp),
        _ => None,
    };

    Ok(Json(EstimateSellResponse {
        token_amount: q.token_amount,
        token_price_usd: token_px,
        estimated_usd,
        sol_price_usd: sol_px,
        estimated_sol,
    }))
}

async fn sell_history_bundle(
    State(st): State<ApiState>,
    Query(q): Query<SellHistoryQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(db) = st.db.as_ref() else {
        return Err((StatusCode::BAD_REQUEST, "database disabled".to_string()));
    };

    let to = match q.to.as_ref().filter(|s| !s.is_empty()) {
        None => chrono::Utc::now(),
        Some(s) => chrono::DateTime::parse_from_rfc3339(s)
            .map(|d| d.with_timezone(&chrono::Utc))
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid `to`: use RFC3339".to_string(),
                )
            })?,
    };
    let from = match q.from.as_ref().filter(|s| !s.is_empty()) {
        None => to - chrono::Duration::days(30),
        Some(s) => chrono::DateTime::parse_from_rfc3339(s)
            .map(|d| d.with_timezone(&chrono::Utc))
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid `from`: use RFC3339".to_string(),
                )
            })?,
    };
    if from > to {
        return Err((StatusCode::BAD_REQUEST, "`from` must be <= `to`".to_string()));
    }

    let rows = trading::fetch_sells_between(db, from, to).await.map_err(internal)?;

    use std::collections::BTreeMap;
    let mut mint_acc: std::collections::HashMap<(String, String), f64> = std::collections::HashMap::new();
    let mut day_acc: BTreeMap<String, f64> = BTreeMap::new();

    let mut total = 0.0_f64;
    let mut trades_winning = 0usize;
    let mut trades_losing = 0usize;

    for r in &rows {
        let p = trading::sell_realized_profit_usd(r);
        total += p;
        match trading::sell_trade_price_win(r) {
            Some(true) => trades_winning += 1,
            Some(false) => trades_losing += 1,
            None => {
                if p > 0.0 {
                    trades_winning += 1;
                } else if p < 0.0 {
                    trades_losing += 1;
                }
            }
        }
        *mint_acc.entry((r.mint.clone(), r.token_name.clone())).or_insert(0.0) += p;
        let day = r.sold_at.naive_utc().date().to_string();
        *day_acc.entry(day).or_insert(0.0) += p;
    }

    let mut profit_by_mint: Vec<MintProfitSlice> = mint_acc
        .into_iter()
        .map(|((mint, token_name), profit_usd)| MintProfitSlice {
            mint,
            token_name,
            profit_usd,
        })
        .collect();
    profit_by_mint.sort_by(|a, b| b.profit_usd.partial_cmp(&a.profit_usd).unwrap_or(std::cmp::Ordering::Equal));

    let profit_by_day: Vec<DayProfitBar> = day_acc
        .into_iter()
        .map(|(day, profit_usd)| DayProfitBar { day, profit_usd })
        .collect();

    let items: Vec<SellHistoryRowResponse> = rows.into_iter().map(map_sell_history_row).collect();

    Ok(Json(SellHistoryBundle {
        items,
        profit_by_mint,
        profit_by_day,
        total_profit_usd: total,
        trades_winning,
        trades_losing,
    }))
}

