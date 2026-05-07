//! Jupiter Swap API v2 (`/order` + `/execute`) and trade bookkeeping.
//!
//! Uses the Meta-Aggregator flow documented at:
//! https://developers.jup.ag/docs/swap/order-and-execute
//!
//! Requires `jupiter_api_key` in config (`x-api-key` header).
//!
//! Configure `wallet_secret_key_base58` in `config.toml` (Solana CLI keypair as base58-encoded 64 bytes).
//! Test only — protect your keys.

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use bincode::Options;
use reqwest::Client;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use solana_keypair::Keypair;
use solana_signer::Signer;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use std::str::FromStr;
use std::sync::Arc;

use crate::config::AppRuntime;
use crate::db::Db;

pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Jupiter Swap v2 (order + execute). See <https://developers.jup.ag/docs/swap/order-and-execute>.
const JUP_SWAP_V2_BASE: &str = "https://api.jup.ag/swap/v2";

fn require_jupiter_api_key(runtime: &AppRuntime) -> Result<&str> {
    runtime
        .jupiter_api_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "jupiter_api_key is required for Jupiter Swap v2 (GET /order + POST /execute). \
                 Set it in config.toml — see https://developers.jup.ag/docs/swap/order-and-execute"
            )
        })
}

pub fn parse_keypair(secret_b58: Option<&str>) -> Result<Option<Arc<Keypair>>> {
    let Some(s) = secret_b58.map(str::trim).filter(|x| !x.is_empty()) else {
        return Ok(None);
    };
    let bytes = bs58::decode(s)
        .into_vec()
        .context("wallet_secret_key_base58 must be valid base58")?;
    match bytes.len() {
        64 => {
            let arr: [u8; 64] = bytes
                .try_into()
                .map_err(|_| anyhow!("wallet key must decode to exactly 64 bytes"))?;
            Ok(Some(Arc::new(
                Keypair::try_from(&arr[..]).context("invalid Solana secret keypair bytes")?,
            )))
        }
        _ => Err(anyhow!(
            "wallet_secret_key_base58: expected 64 bytes after base58 decode, got {}",
            bytes.len()
        )),
    }
}

pub async fn fetch_mint_decimals(http: &Client, rpc_url: &str, mint: &str) -> Result<u8> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint, {"encoding": "base64"}]
    });
    let v: Value = http
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("rpc getAccountInfo send")?
        .json()
        .await
        .context("rpc getAccountInfo parse")?;

    let b64 = v["result"]["value"]["data"]
        .get(0)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("mint account missing or malformed"))?;

    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .context("mint data base64")?;
    raw.get(44).copied().ok_or_else(|| anyhow!("mint data too short"))
}

/// Partial sign only slots belonging to `keypair` (needed when JupiterZ leaves MM signer slots empty).
fn partial_sign_versioned_tx(vtx: &mut VersionedTransaction, keypair: &Keypair) -> Result<()> {
    let msg_bytes = vtx.message.serialize();
    let sig = keypair
        .try_sign_message(&msg_bytes)
        .map_err(|e| anyhow!("swap tx sign-message failed: {:?}", e))?;
    let keys = vtx.message.static_account_keys();
    let n = vtx.message.header().num_required_signatures as usize;
    if keys.len() < n {
        return Err(anyhow!("swap tx message: not enough account keys for header"));
    }
    if vtx.signatures.len() < n {
        return Err(anyhow!(
            "swap tx: expected at least {} signatures, got {}",
            n,
            vtx.signatures.len()
        ));
    }
    let pk = keypair.pubkey();
    let mut wrote = false;
    for i in 0..n {
        if keys[i] == pk {
            vtx.signatures[i] = sig;
            wrote = true;
        }
    }
    if !wrote {
        return Err(anyhow!(
            "wallet {} is not among the first {} required signers for this swap",
            pk,
            n
        ));
    }
    Ok(())
}

fn deserialize_vtx(tx_b64: &str) -> Result<VersionedTransaction> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(tx_b64.trim())?;
    Ok(bincode::options()
        .with_fixint_encoding()
        .with_little_endian()
        .allow_trailing_bytes()
        .deserialize::<VersionedTransaction>(&bytes)
        .context("deserialize VersionedTransaction (bincode)")?)
}

fn serialize_vtx_b64(vtx: &VersionedTransaction) -> Result<String> {
    let wired = bincode::options()
        .with_fixint_encoding()
        .with_little_endian()
        .allow_trailing_bytes()
        .serialize(vtx)
        .context("serialize signed VersionedTransaction")?;
    Ok(base64::engine::general_purpose::STANDARD.encode(wired))
}

/// `GET /swap/v2/order` — assembled tx + `requestId` for `/execute`.
async fn jupiter_swap_v2_order(
    http: &Client,
    runtime: &AppRuntime,
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    taker: &str,
    slippage_bps: u16,
) -> Result<Value> {
    let api_key = require_jupiter_api_key(runtime)?;
    let mut url =
        reqwest::Url::parse(&format!("{JUP_SWAP_V2_BASE}/order")).context("parse Jupiter /order URL")?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("inputMint", input_mint);
        q.append_pair("outputMint", output_mint);
        q.append_pair("amount", &amount_raw.to_string());
        q.append_pair("taker", taker);
        if slippage_bps > 0 {
            q.append_pair("slippageBps", &slippage_bps.to_string());
        }
    }

    let resp = http
        .get(url)
        .header("x-api-key", api_key)
        .send()
        .await
        .context("Jupiter /order send")?
        .error_for_status()
        .context("Jupiter /order HTTP status")?;

    let v = resp.json::<Value>().await.context("Jupiter /order JSON")?;
    Ok(v)
}

/// Sign locally and `POST /swap/v2/execute` — Jupiter lands the transaction.
async fn jupiter_swap_v2_execute(
    http: &Client,
    runtime: &AppRuntime,
    keypair: &Keypair,
    order: &Value,
) -> Result<(Signature, Value)> {
    let api_key = require_jupiter_api_key(runtime)?;

    let tx_b64 = order["transaction"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Jupiter /order returned no `transaction` (need `taker`; check API key and params): {}",
                serde_json::to_string(order).unwrap_or_default()
            )
        })?;

    let request_id = order["requestId"]
        .as_str()
        .ok_or_else(|| anyhow!("Jupiter /order missing requestId"))?;

    let mut vtx = deserialize_vtx(tx_b64)?;
    partial_sign_versioned_tx(&mut vtx, keypair)?;
    let signed_b64 = serialize_vtx_b64(&vtx)?;

    let body = json!({
        "signedTransaction": signed_b64,
        "requestId": request_id,
    });

    let exec = http
        .post(format!("{JUP_SWAP_V2_BASE}/execute"))
        .header("x-api-key", api_key)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("Jupiter /execute send")?
        .error_for_status()
        .context("Jupiter /execute HTTP status")?
        .json::<Value>()
        .await
        .context("Jupiter /execute JSON")?;

    let status = exec["status"].as_str().unwrap_or("");
    let sig_str = exec["signature"]
        .as_str()
        .ok_or_else(|| anyhow!("Jupiter /execute missing signature: {}", exec))?;

    let sig = Signature::from_str(sig_str.trim()).map_err(|_| anyhow!("invalid signature from /execute"))?;

    if status != "Success" {
        let code = &exec["code"];
        let err = exec["error"].as_str().unwrap_or("");
        return Err(anyhow!(
            "Jupiter /execute failed: status={status} code={code} error={err} full={exec}"
        ));
    }

    Ok((sig, exec))
}

async fn jupiter_v2_swap_exact_in(
    http: &Client,
    runtime: &AppRuntime,
    keypair: &Keypair,
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
) -> Result<(Signature, Value, Value)> {
    let taker = keypair.pubkey().to_string();
    let order = jupiter_swap_v2_order(
        http,
        runtime,
        input_mint,
        output_mint,
        amount_raw,
        &taker,
        slippage_bps,
    )
    .await
    .context("Jupiter swap v2 order")?;

    eprintln!("[jupiter_v2_swap_exact_in] order: {:?}", order);
    let (sig, exec) = jupiter_swap_v2_execute(http, runtime, keypair, &order)
        .await
        .context("Jupiter swap v2 execute")?;

    Ok((sig, order, exec))
}

fn parse_jupiter_amount_field(v: &Value, key: &str) -> Option<u128> {
    v.get(key).and_then(|x| x.as_str()).and_then(|s| s.parse().ok())
}

pub struct BuyResult {
    pub signature: String,
    pub tokens_bought_raw: u128,
    pub sol_spent_lamports: u64,
    pub buy_price_token_usd: f64,
    pub buy_cost_usd_estimate: f64,
    #[allow(dead_code)]
    pub quote: Value,
}

pub async fn buy_with_sol(
    http: &Client,
    runtime: &AppRuntime,
    keypair: &Keypair,
    output_mint: &str,
    sol_amount: f64,
    slippage_bps: u16,
) -> Result<BuyResult> {
    if sol_amount <= 0.0 || !sol_amount.is_finite() {
        return Err(anyhow!("invalid SOL amount"));
    }
    let lamports = (sol_amount * 1_000_000_000_f64).round().max(1.0) as u64;

    let (sig, order, exec) = jupiter_v2_swap_exact_in(
        http,
        runtime,
        keypair,
        SOL_MINT,
        output_mint,
        lamports,
        slippage_bps,
    )
    .await
    .context("Jupiter swap v2 (buy)")?;

    let out_raw = parse_jupiter_amount_field(&exec, "outputAmountResult")
        .or_else(|| parse_jupiter_amount_field(&order, "outAmount"))
        .ok_or_else(|| anyhow!("Jupiter response missing output amount (outputAmountResult / outAmount)"))?;

    let quote = json!({ "order": order, "execute": exec });

    let buy_price_token_usd = crate::price::fetch_token_price_usd(http, runtime, output_mint)
        .await
        .unwrap_or(None)
        .unwrap_or(0.0);

    let sol_px = crate::price::fetch_token_price_usd(http, runtime, SOL_MINT)
        .await
        .unwrap_or(None)
        .unwrap_or(0.0);

    let sol_human = lamports as f64 / 1e9_f64;
    let buy_cost_usd_estimate = sol_human * sol_px;

    Ok(BuyResult {
        signature: sig.to_string(),
        tokens_bought_raw: out_raw,
        sol_spent_lamports: lamports,
        buy_price_token_usd,
        buy_cost_usd_estimate,
        quote,
    })
}

pub async fn sell_tokens_for_sol(
    http: &Client,
    runtime: &AppRuntime,
    keypair: &Keypair,
    input_mint: &str,
    decimals: u8,
    token_amount_human: Decimal,
    slippage_bps: u16,
    cap_raw_atoms: Option<u128>,
) -> Result<(String, u64, Value)> {
    let atoms: u128 = if token_amount_human > Decimal::ZERO {
        let factor = Decimal::from(10u64.pow(decimals as u32));
        let scaled = (token_amount_human * factor).trunc();
        let raw = scaled
            .to_u128()
            .ok_or_else(|| anyhow!("sell amount overflows u128"))?;
        match cap_raw_atoms {
            Some(cap) => raw.min(cap),
            None => raw,
        }
    } else if let Some(cap) = cap_raw_atoms {
        cap
    } else {
        return Err(anyhow!("sell amount must be positive"));
    };

    if atoms == 0 {
        return Err(anyhow!("sell amount resolves to zero"));
    }
    if atoms > u64::MAX as u128 {
        return Err(anyhow!(
            "raw sell amount exceeds u64 (Jupiter); reduce size."
        ));
    }
    let amount_u64 = atoms as u64;

    let (sig, order, exec) = jupiter_v2_swap_exact_in(
        http,
        runtime,
        keypair,
        input_mint,
        SOL_MINT,
        amount_u64,
        slippage_bps,
    )
    .await
    .context("Jupiter swap v2 (sell)")?;

    let out_lamports_u128 = parse_jupiter_amount_field(&exec, "outputAmountResult")
        .or_else(|| parse_jupiter_amount_field(&order, "outAmount"));

    let out_lamports = out_lamports_u128
        .map(|x| u64::try_from(x).unwrap_or(u64::MAX))
        .unwrap_or(0);

    let quote = json!({ "order": order, "execute": exec });

    Ok((sig.to_string(), out_lamports, quote))
}

// --- Persistence ------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct PositionRow {
    pub id: i64,
    pub mint: String,
    pub token_name: String,
    pub buy_tx_signature: String,
    pub buy_at: chrono::DateTime<chrono::Utc>,
    pub buy_price_usd: f64,
    pub token_decimals: i16,
    pub tokens_remaining_raw: String,
    pub tokens_bought_raw: String,
    pub sol_spent_lamports: i64,
    pub buy_cost_usd_est: f64,
}

#[derive(Clone, Serialize)]
pub struct PositionWithPnl {
    #[serde(flatten)]
    pub row: PositionRow,
    pub current_price_usd: Option<f64>,
    pub holdings_usd: Option<f64>,
    pub unrealized_profit_usd: Option<f64>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SellRow {
    pub id: i64,
    pub position_id: Option<i64>,
    pub mint: String,
    pub token_name: String,
    pub sell_tx_signature: Option<String>,
    pub sold_at: chrono::DateTime<chrono::Utc>,
    pub buy_price_usd: f64,
    pub sell_price_usd: f64,
    pub token_decimals: i16,
    pub tokens_sold_raw: String,
    pub sol_received_lamports: Option<i64>,
    pub profit_usd: f64,
    pub closed_position: bool,
}

/// Human token amount sold for one ledger row.
pub fn sell_qty_human(row: &SellRow) -> f64 {
    let dec = row.token_decimals.clamp(0, 24) as i32;
    let raw: u128 = row.tokens_sold_raw.parse().unwrap_or(0);
    let q = raw as f64 / 10_f64.powi(dec);
    if q.is_finite() { q } else { 0.0 }
}

/// Realized P/L: `(sell_price - buy_price) * qty` when buy price is known; otherwise stored `profit_usd`.
pub fn sell_realized_profit_usd(row: &SellRow) -> f64 {
    let qty = sell_qty_human(row);
    if qty <= 0.0 || !qty.is_finite() {
        return row.profit_usd;
    }
    if row.buy_price_usd.is_finite()
        && row.buy_price_usd > 0.0
        && row.sell_price_usd.is_finite()
    {
        (row.sell_price_usd - row.buy_price_usd) * qty
    } else {
        row.profit_usd
    }
}

/// Win / loss from prices when baseline exists (`sell > buy` ⇒ win). `None` ⇒ infer from [`sell_realized_profit_usd`].
pub fn sell_trade_price_win(row: &SellRow) -> Option<bool> {
    if row.buy_price_usd > 0.0 && row.buy_price_usd.is_finite() && row.sell_price_usd.is_finite() {
        if row.sell_price_usd > row.buy_price_usd {
            Some(true)
        } else if row.sell_price_usd < row.buy_price_usd {
            Some(false)
        } else {
            None
        }
    } else {
        None
    }
}

pub async fn init_trade_tables(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query(
        r#"
        create table if not exists trade_positions (
            id bigserial primary key,
            mint text not null,
            token_name text not null default '',
            buy_tx_signature text not null unique,
            buy_at timestamptz not null default now(),
            buy_price_usd double precision not null default 0,
            token_decimals smallint not null default 9,
            tokens_bought_raw text not null,
            tokens_remaining_raw text not null,
            sol_spent_lamports bigint not null,
            buy_cost_usd_est double precision not null default 0
        );
        "#,
    )
    .execute(pool)
    .await
    .context("trade_positions DDL")?;

    sqlx::query(
        r#"
        create table if not exists sell_history (
            id bigserial primary key,
            position_id bigint references trade_positions(id) on delete set null,
            mint text not null,
            token_name text not null,
            sell_tx_signature text,
            sold_at timestamptz not null default now(),
            buy_price_usd double precision not null default 0,
            sell_price_usd double precision not null default 0,
            token_decimals smallint not null default 9,
            tokens_sold_raw text not null,
            sol_received_lamports bigint,
            profit_usd double precision not null default 0,
            closed_position boolean not null default false
        );
        "#,
    )
    .execute(pool)
    .await
    .context("sell_history DDL")?;

    sqlx::query(r#"alter table sell_history add column if not exists buy_price_usd double precision not null default 0"#)
        .execute(pool)
        .await
        .context("sell_history buy_price_usd migration")?;
    sqlx::query(r#"alter table sell_history add column if not exists token_decimals smallint not null default 9"#)
        .execute(pool)
        .await
        .context("sell_history token_decimals migration")?;

    Ok(())
}

pub async fn insert_position_row<'e, E>(
    pool: E,
    mint: &str,
    token_name: &str,
    buy_sig: &str,
    buy_price_usd: f64,
    decimals: i16,
    bought_raw: u128,
    remaining_raw: u128,
    sol_lamports: i64,
    buy_cost_est: f64,
) -> Result<i64>
where
    E: sqlx::PgExecutor<'e>,
{
    let id: i64 = sqlx::query_scalar(
        r#"
        insert into trade_positions (
            mint, token_name, buy_tx_signature, buy_price_usd, token_decimals,
            tokens_bought_raw, tokens_remaining_raw, sol_spent_lamports, buy_cost_usd_est
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        returning id;
        "#,
    )
    .bind(mint)
    .bind(token_name)
    .bind(buy_sig)
    .bind(buy_price_usd)
    .bind(decimals)
    .bind(bought_raw.to_string())
    .bind(remaining_raw.to_string())
    .bind(sol_lamports)
    .bind(buy_cost_est)
    .fetch_one(pool)
    .await
    .context("insert trade_positions")?;

    Ok(id)
}

/// Buy on-chain, then insert an open row in `trade_positions`.
pub async fn buy_and_open_position(
    http: &Client,
    runtime: &AppRuntime,
    db: &Db,
    keypair: &Keypair,
    mint: &str,
    token_name: &str,
    sol_amount: f64,
    slippage_bps: u16,
) -> Result<PositionRow> {
    let decimals = fetch_mint_decimals(http, &runtime.rpc_url, mint).await?;
    let res = buy_with_sol(http, runtime, keypair, mint, sol_amount, slippage_bps).await?;
    let lamports_i =
        i64::try_from(res.sol_spent_lamports).map_err(|_| anyhow!("SOL spent lamports overflow i64"))?;

    let factor_dec = 10_f64.powi(i32::from(decimals));
    let qty_human = (res.tokens_bought_raw as f64 / factor_dec).max(f64::EPSILON);
    let baseline_usd = if res.buy_price_token_usd.is_finite() && res.buy_price_token_usd > 0.0 {
        res.buy_price_token_usd
    } else if res.buy_cost_usd_estimate.is_finite() && res.buy_cost_usd_estimate > 0.0 {
        res.buy_cost_usd_estimate / qty_human
    } else {
        0.0
    };

    let latest_usd = crate::price::fetch_token_price_usd(http, runtime, mint)
        .await
        .unwrap_or(None)
        .filter(|p| p.is_finite() && *p > 0.0)
        .unwrap_or(baseline_usd);

    let pid = insert_position_row(
        &db.pool,
        mint,
        token_name.trim(),
        &res.signature,
        res.buy_price_token_usd,
        i16::from(decimals),
        res.tokens_bought_raw,
        res.tokens_bought_raw,
        lamports_i,
        res.buy_cost_usd_estimate,
    )
    .await?;

    if baseline_usd > 0.0 && baseline_usd.is_finite() {
        crate::db::apply_buy_price_baseline(db, mint, baseline_usd, latest_usd).await?;
    }

    let row = sqlx::query_as::<_, PositionRow>(
        r#"
        select id, mint, token_name, buy_tx_signature, buy_at, buy_price_usd, token_decimals,
               tokens_remaining_raw, tokens_bought_raw, sol_spent_lamports, buy_cost_usd_est
        from trade_positions where id = $1;
        "#,
    )
    .bind(pid)
    .fetch_one(&db.pool)
    .await
    .context("fetch new trade position")?;
    Ok(row)
}

pub async fn open_positions_with_pnl(
    http: &Client,
    runtime: &AppRuntime,
    db: &Db,
) -> Result<Vec<PositionWithPnl>> {
    let rows = fetch_open_positions(db).await?;
    let mut out = Vec::with_capacity(rows.len());
    for p in rows {
        let bought_raw: u128 = p.tokens_bought_raw.parse().unwrap_or(1);
        let rem_raw: u128 = p.tokens_remaining_raw.parse().unwrap_or(0);
        let factor = 10_f64.powi(p.token_decimals as i32);
        let qty_human = rem_raw as f64 / factor;
        let current = crate::price::fetch_token_price_usd(http, runtime, &p.mint)
            .await
            .unwrap_or(None);
        let holdings = current.map(|px| px * qty_human);
        let cost_ratio = (rem_raw as f64) / (bought_raw as f64).max(f64::EPSILON);
        let cost_remaining = cost_ratio * p.buy_cost_usd_est;
        let unrealized = holdings.map(|h| h - cost_remaining);
        out.push(PositionWithPnl {
            row: p,
            current_price_usd: current,
            holdings_usd: holdings,
            unrealized_profit_usd: unrealized,
        });
    }
    Ok(out)
}

pub async fn fetch_open_positions(db: &Db) -> Result<Vec<PositionRow>> {
    let rows = sqlx::query_as::<_, PositionRow>(
        r#"
        select id, mint, token_name, buy_tx_signature, buy_at, buy_price_usd, token_decimals,
               tokens_remaining_raw, tokens_bought_raw, sol_spent_lamports, buy_cost_usd_est
        from trade_positions
        where tokens_remaining_raw::numeric > 0
        order by buy_at desc;
        "#,
    )
    .fetch_all(&db.pool)
    .await
    .context("list positions")?;

    Ok(rows)
}

pub async fn fetch_sells_between(
    db: &Db,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<SellRow>> {
    let rows = sqlx::query_as::<_, SellRow>(
        r#"
        select id, position_id, mint, token_name, sell_tx_signature, sold_at,
               buy_price_usd, sell_price_usd, token_decimals,
               tokens_sold_raw, sol_received_lamports, profit_usd, closed_position
        from sell_history
        where sold_at >= $1 and sold_at <= $2
        order by sold_at desc;
        "#,
    )
    .bind(from)
    .bind(to)
    .fetch_all(&db.pool)
    .await
    .context("list sells")?;

    Ok(rows)
}

#[allow(dead_code)]
pub fn position_human_remaining_atoms(p: &PositionRow) -> Result<Decimal> {
    Decimal::from_str_exact(&p.tokens_remaining_raw).context("remaining atoms decimal parse")
}

/// Execute on-chain swap then update Postgres in one transaction (sell ledger + optionally remove position).
pub async fn apply_sell_position(
    http: &Client,
    runtime: &AppRuntime,
    keypair: &Keypair,
    db: &Db,
    position_id: i64,
    token_amount_human: Decimal,
    slippage_bps: u16,
) -> Result<()> {
    let pos: PositionRow = sqlx::query_as(
        r#"
        select id, mint, token_name, buy_tx_signature, buy_at, buy_price_usd, token_decimals,
               tokens_remaining_raw, tokens_bought_raw, sol_spent_lamports, buy_cost_usd_est
        from trade_positions where id = $1;
        "#,
    )
    .bind(position_id)
    .fetch_optional(&db.pool)
    .await
    .context("load position")?
    .ok_or_else(|| anyhow!("position {} not found", position_id))?;

    let rem_raw: u128 = pos.tokens_remaining_raw.parse().context("parse remaining raw")?;
    let bought_raw: u128 = pos.tokens_bought_raw.parse().context("parse bought raw")?;
    let dec = pos.token_decimals as u8;

    let factor = Decimal::from(10u64.pow(dec as u32));
    let scaled = (token_amount_human * factor).trunc();
    let mut want_raw = scaled.to_u128().unwrap_or(0);
    want_raw = want_raw.min(rem_raw);
    if want_raw == 0 {
        return Err(anyhow!("nothing to sell (check token amount decimals)"));
    }

    let human_for_quote = Decimal::from(want_raw) / factor;

    let (sell_sig, out_lams, _) = sell_tokens_for_sol(
        http,
        runtime,
        keypair,
        &pos.mint,
        dec,
        human_for_quote,
        slippage_bps,
        Some(want_raw),
    )
    .await?;

    let sell_raw_atoms = want_raw;

    let sol_usd = crate::price::fetch_token_price_usd(http, runtime, SOL_MINT)
        .await
        .unwrap_or(None)
        .unwrap_or(0.0);
    let proceeds_usd = (out_lams as f64 / 1e9) * sol_usd;

    let basis_ratio = (sell_raw_atoms as f64) / (bought_raw as f64).max(1e-18);
    let basis_usd = basis_ratio * pos.buy_cost_usd_est;

    let sell_price_token = crate::price::fetch_token_price_usd(http, runtime, &pos.mint)
        .await
        .unwrap_or(None)
        .unwrap_or(0.0);

    let qty_human = sell_raw_atoms as f64 / 10_f64.powi(dec as i32);
    let profit_usd = if pos.buy_price_usd.is_finite()
        && pos.buy_price_usd > 0.0
        && sell_price_token.is_finite()
        && qty_human > 0.0
    {
        (sell_price_token - pos.buy_price_usd) * qty_human
    } else {
        proceeds_usd - basis_usd
    };

    let mut tx = db.pool.begin().await?;

    let new_rem: Option<String> = sqlx::query_scalar(
        r#"
        update trade_positions
        set tokens_remaining_raw =
            (tokens_remaining_raw::numeric - $2::numeric)::text
        where id = $1
          and tokens_remaining_raw::numeric >= $2::numeric
        returning tokens_remaining_raw;
        "#,
    )
    .bind(position_id)
    .bind(sell_raw_atoms.to_string())
    .fetch_optional(&mut *tx)
    .await?;

    let Some(new_remaining_str) = new_rem else {
        tx.rollback().await?;
        return Err(anyhow!(
            "position balances changed — not enough remaining to record this sell after swap (check wallet + DB)"
        ));
    };

    let remaining_after: u128 = new_remaining_str.parse().context("parse new remaining")?;
    let closed = remaining_after == 0;

    sqlx::query(
        r#"
        insert into sell_history (
            position_id, mint, token_name, sell_tx_signature,
            buy_price_usd, sell_price_usd, token_decimals,
            tokens_sold_raw, sol_received_lamports, profit_usd, closed_position
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);
        "#,
    )
    .bind(Some(position_id))
    .bind(&pos.mint)
    .bind(&pos.token_name)
    .bind(Some(sell_sig))
    .bind(pos.buy_price_usd)
    .bind(sell_price_token)
    .bind(pos.token_decimals)
    .bind(sell_raw_atoms.to_string())
    .bind(Some(out_lams as i64))
    .bind(profit_usd)
    .bind(closed)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}
