use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;

use crate::types::TokenRecord;

#[derive(Clone)]
pub struct Db {
    pub pool: PgPool,
}

pub type DbTokenRow = (
    String,
    String,
    i64,
    i64,
    Option<f64>,
    Option<DateTime<Utc>>,
    Option<f64>,
    Option<DateTime<Utc>>,
);

pub async fn init_db(database_url: Option<&str>) -> Result<Option<Db>> {
    let Some(url) = database_url else {
        return Ok(None);
    };

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await
        .context("failed to connect to Postgres (database_url)")?;

    sqlx::query(
        r#"
        create table if not exists pump_tokens (
            mint text primary key,
            name text not null,
            first_slot bigint not null,
            last_slot bigint not null,
            first_seen timestamptz not null default now(),
            last_seen timestamptz not null default now(),
            first_price_usd double precision null,
            first_price_at timestamptz null,
            price_usd double precision null,
            price_updated_at timestamptz null
        );
        "#,
    )
    .execute(&pool)
    .await
    .context("failed to init pump_tokens table")?;

    // Backwards-compatible migrations if table existed before new columns.
    sqlx::query(r#"alter table pump_tokens add column if not exists first_price_usd double precision null;"#)
        .execute(&pool)
        .await
        .context("failed to add first_price_usd column")?;
    sqlx::query(r#"alter table pump_tokens add column if not exists first_price_at timestamptz null;"#)
        .execute(&pool)
        .await
        .context("failed to add first_price_at column")?;

    sqlx::query(
        r#"
        create table if not exists token_prices (
            mint text not null,
            ts timestamptz not null,
            price_usd double precision not null,
            primary key (mint, ts)
        );
        "#,
    )
    .execute(&pool)
    .await
    .context("failed to init token_prices table")?;

    sqlx::query(
        r#"
        create index if not exists token_prices_mint_ts_idx
        on token_prices (mint, ts desc);
        "#,
    )
    .execute(&pool)
    .await
    .context("failed to init token_prices index")?;

    Ok(Some(Db { pool }))
}

pub async fn upsert_token(db: &Db, token: &TokenRecord) -> Result<()> {
    sqlx::query(
        r#"
        insert into pump_tokens (mint, name, first_slot, last_slot, first_seen, last_seen)
        values ($1, $2, $3, $3, now(), now())
        on conflict (mint) do update set
            name = excluded.name,
            last_slot = greatest(pump_tokens.last_slot, excluded.last_slot),
            last_seen = now();
        "#,
    )
    .bind(&token.mint)
    .bind(&token.name)
    .bind(token.slot as i64)
    .execute(&db.pool)
    .await
    .context("failed to upsert token into Postgres")?;
    Ok(())
}

pub async fn update_token_price(db: &Db, mint: &str, price_usd: f64) -> Result<()> {
    sqlx::query(
        r#"
        update pump_tokens
        set first_price_usd = coalesce(first_price_usd, $2),
            first_price_at = case when first_price_usd is null then now() else first_price_at end,
            price_usd = $2,
            price_updated_at = now(),
            last_seen = now()
        where mint = $1;
        "#,
    )
    .bind(mint)
    .bind(price_usd)
    .execute(&db.pool)
    .await
    .context("failed to update token price in Postgres")?;
    Ok(())
}

pub async fn get_token(db: &Db, mint: &str) -> Result<Option<DbTokenRow>> {
    let row = sqlx::query_as::<_, DbTokenRow>(
        r#"
        select mint, name, first_slot, last_slot, first_price_usd, first_price_at, price_usd, price_updated_at
        from pump_tokens
        where mint = $1;
        "#,
    )
    .bind(mint)
    .fetch_optional(&db.pool)
    .await
    .context("failed to fetch token from Postgres")?;
    Ok(row)
}

pub async fn list_tokens(db: &Db, limit: i64, offset: i64) -> Result<Vec<DbTokenRow>> {
    let rows = sqlx::query_as::<_, DbTokenRow>(
        r#"
        select mint, name, first_slot, last_slot, first_price_usd, first_price_at, price_usd, price_updated_at
        from pump_tokens
        order by last_seen desc
        limit $1
        offset $2;
        "#,
    )
    .bind(limit.max(1).min(500))
    .bind(offset.max(0))
    .fetch_all(&db.pool)
    .await
    .context("failed to list tokens from Postgres")?;
    Ok(rows)
}

pub async fn list_mints(db: &Db, limit: i64, offset: i64) -> Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        select mint
        from pump_tokens
        order by last_seen desc
        limit $1
        offset $2;
        "#,
    )
    .bind(limit.max(1).min(2000))
    .bind(offset.max(0))
    .fetch_all(&db.pool)
    .await
    .context("failed to list mints from Postgres")?;
    Ok(rows)
}

pub async fn insert_price_point(db: &Db, mint: &str, ts: DateTime<Utc>, price_usd: f64) -> Result<()> {
    sqlx::query(
        r#"
        insert into token_prices (mint, ts, price_usd)
        values ($1, $2, $3)
        on conflict (mint, ts) do nothing;
        "#,
    )
    .bind(mint)
    .bind(ts)
    .bind(price_usd)
    .execute(&db.pool)
    .await
    .context("failed to insert token price point")?;
    Ok(())
}

pub type DbPricePoint = (DateTime<Utc>, f64);

pub async fn list_price_points(db: &Db, mint: &str, limit: i64) -> Result<Vec<DbPricePoint>> {
    let rows = sqlx::query_as::<_, DbPricePoint>(
        r#"
        select ts, price_usd
        from token_prices
        where mint = $1
        order by ts desc
        limit $2;
        "#,
    )
    .bind(mint)
    .bind(limit.max(1).min(2000))
    .fetch_all(&db.pool)
    .await
    .context("failed to list token price points")?;
    Ok(rows)
}

