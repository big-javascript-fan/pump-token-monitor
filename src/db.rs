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
    DateTime<Utc>,
    DateTime<Utc>,
    bool,
    Option<DateTime<Utc>>,
);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TokenListSort {
    /// Newly discovered tokens first (`first_seen` desc).
    #[default]
    FirstSeenDesc,
    /// Tokens with recent updates first (`last_seen` desc).
    LastSeenDesc,
    /// Largest positive % change `(price − first_price) / first_price` first.
    ChangePctDesc,
    /// Largest negative % change first.
    ChangePctAsc,
}

impl TokenListSort {
    pub fn parse(raw: Option<&str>) -> Self {
        let r = raw.map(str::trim).filter(|s| !s.is_empty()).map(|s| s.to_ascii_lowercase());

        match r.as_deref() {
            None | Some("") | Some("first_seen") | Some("new") | Some("newest") => Self::FirstSeenDesc,
            Some("last_seen") | Some("recent") | Some("active") => Self::LastSeenDesc,
            Some("change_desc") | Some("pct_desc") | Some("gainers") | Some("+") => Self::ChangePctDesc,
            Some("change_asc") | Some("pct_asc") | Some("losers") | Some("-") => Self::ChangePctAsc,
            _ => Self::FirstSeenDesc,
        }
    }

    fn order_sql(&self) -> &'static str {
        match self {
            TokenListSort::FirstSeenDesc => "order by first_seen desc nulls last, mint asc",
            TokenListSort::LastSeenDesc => "order by last_seen desc nulls last, mint asc",
            TokenListSort::ChangePctDesc => {
                "order by (case \
                 when first_price_usd is not null and price_usd is not null and first_price_usd <> 0::float8 \
                 then (price_usd - first_price_usd) / first_price_usd * 100::float8 end) \
                 desc nulls last, first_seen desc nulls last, mint asc"
            }
            TokenListSort::ChangePctAsc => {
                "order by (case \
                 when first_price_usd is not null and price_usd is not null and first_price_usd <> 0::float8 \
                 then (price_usd - first_price_usd) / first_price_usd * 100::float8 end) \
                 asc nulls last, first_seen desc nulls last, mint asc"
            }
        }
    }
}

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

    sqlx::query(r#"alter table pump_tokens add column if not exists dead_token boolean not null default false"#)
        .execute(&pool)
        .await
        .context("failed to add dead_token column")?;
    sqlx::query(r#"alter table pump_tokens add column if not exists dead_marked_at timestamptz null"#)
        .execute(&pool)
        .await
        .context("failed to add dead_marked_at column")?;

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

pub async fn clear_dead_flag(db: &Db, mint: &str) -> Result<()> {
    sqlx::query(
        r#"
        update pump_tokens
        set dead_token = false, dead_marked_at = null, last_seen = now()
        where mint = $1;
        "#,
    )
    .bind(mint)
    .execute(&db.pool)
    .await
    .context("clear_dead_flag")?;
    Ok(())
}

/// After a user buy: set `first_price_usd` to the execution baseline and refresh `price_usd`
/// to the latest quote so dashboard % change reads as (market − buy) / buy.
pub async fn apply_buy_price_baseline(db: &Db, mint: &str, buy_baseline_usd: f64, latest_market_usd: f64) -> Result<()> {
    if !buy_baseline_usd.is_finite() || buy_baseline_usd <= 0.0 {
        return Ok(());
    }
    let px = if latest_market_usd.is_finite() && latest_market_usd > 0.0 {
        latest_market_usd
    } else {
        buy_baseline_usd
    };
    sqlx::query(
        r#"
        update pump_tokens
        set first_price_usd = $2,
            first_price_at = now(),
            price_usd = $3,
            price_updated_at = now(),
            last_seen = now(),
            dead_token = false,
            dead_marked_at = null
        where mint = $1;
        "#,
    )
    .bind(mint)
    .bind(buy_baseline_usd)
    .bind(px)
    .execute(&db.pool)
    .await
    .context("apply_buy_price_baseline")?;
    let ts = Utc::now();
    insert_price_point(db, mint, ts, px).await?;
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
        where mint = $1
          and coalesce(dead_token, false) = false;
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
        select mint, name, first_slot, last_slot, first_price_usd, first_price_at,
               price_usd, price_updated_at, first_seen, last_seen, dead_token, dead_marked_at
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

pub async fn list_tokens_by_mints(db: &Db, mints: &[String]) -> Result<Vec<DbTokenRow>> {
    const MAX: usize = 80;
    if mints.is_empty() {
        return Ok(vec![]);
    }
    let cap = mints.len().min(MAX);
    let slice = &mints[..cap];

    let rows = sqlx::query_as::<_, DbTokenRow>(
        r#"
        select mint, name, first_slot, last_slot, first_price_usd, first_price_at,
               price_usd, price_updated_at, first_seen, last_seen, dead_token, dead_marked_at
        from pump_tokens
        where mint = any($1);
        "#,
    )
    .bind(slice)
    .fetch_all(&db.pool)
    .await
    .context("list_tokens_by_mints")?;

    Ok(rows)
}

pub async fn list_tokens(
    db: &Db,
    limit: i64,
    offset: i64,
    sort: TokenListSort,
    search: Option<&str>,
) -> Result<Vec<DbTokenRow>> {
    let lim = limit.max(1).min(500);
    let off = offset.max(0);
    let needle = search.map(str::trim).filter(|s| !s.is_empty());

    let sql = if needle.is_some() {
        format!(
            r#"
            select mint, name, first_slot, last_slot, first_price_usd, first_price_at,
                   price_usd, price_updated_at, first_seen, last_seen, dead_token, dead_marked_at
            from pump_tokens
            where coalesce(dead_token, false) = false
              and (
                    strpos(lower(name), lower($3::text)) > 0
                 or strpos(lower(mint), lower($3::text)) > 0
              )
            {}
            limit $1 offset $2;
            "#,
            sort.order_sql()
        )
    } else {
        format!(
            r#"
            select mint, name, first_slot, last_slot, first_price_usd, first_price_at,
                   price_usd, price_updated_at, first_seen, last_seen, dead_token, dead_marked_at
            from pump_tokens
            where coalesce(dead_token, false) = false
            {}
            limit $1 offset $2;
            "#,
            sort.order_sql()
        )
    };

    let rows = if let Some(n) = needle {
        sqlx::query_as::<_, DbTokenRow>(&sql)
            .bind(lim)
            .bind(off)
            .bind(n)
            .fetch_all(&db.pool)
            .await
    } else {
        sqlx::query_as::<_, DbTokenRow>(&sql)
            .bind(lim)
            .bind(off)
            .fetch_all(&db.pool)
            .await
    }
    .context("failed to list tokens from Postgres")?;
    Ok(rows)
}

pub async fn list_mints(db: &Db, limit: i64, offset: i64) -> Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        select mint
        from pump_tokens
        where coalesce(dead_token, false) = false
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

pub async fn list_price_points(
    db: &Db,
    mint: &str,
    limit: i64,
    from_ts: Option<DateTime<Utc>>,
) -> Result<Vec<DbPricePoint>> {
    let limit = limit.max(1).min(2000);

    let rows = if let Some(from) = from_ts {
        sqlx::query_as::<_, DbPricePoint>(
            r#"
            select ts, price_usd
            from token_prices
            where mint = $1 and ts >= $2
            order by ts desc
            limit $3;
            "#,
        )
        .bind(mint)
        .bind(from)
        .bind(limit)
        .fetch_all(&db.pool)
        .await
    } else {
        sqlx::query_as::<_, DbPricePoint>(
            r#"
            select ts, price_usd
            from token_prices
            where mint = $1
            order by ts desc
            limit $2;
            "#,
        )
        .bind(mint)
        .bind(limit)
        .fetch_all(&db.pool)
        .await
    }
    .context("failed to list token price points")?;
    Ok(rows)
}

/// Mark mint as dead (no longer priced by cron / hidden from main token list).
pub async fn mark_token_dead(db: &Db, mint: &str) -> Result<()> {
    sqlx::query(
        r#"
        update pump_tokens
        set dead_token = true,
            dead_marked_at = now()
        where mint = $1
          and coalesce(dead_token, false) = false;
        "#,
    )
    .bind(mint)
    .execute(&db.pool)
    .await
    .context("mark_token_dead")?;
    Ok(())
}

/// Mints with ≥ `min_points` price samples in the last 24h (excluding already-dead tokens).
pub async fn list_mints_for_dead_scan(db: &Db, min_points: i64) -> Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        select tp.mint
        from token_prices tp
        inner join pump_tokens pt on pt.mint = tp.mint
        where tp.ts >= now() - interval '24 hours'
          and coalesce(pt.dead_token, false) = false
        group by tp.mint
        having count(*) >= $1
        "#,
    )
    .bind(min_points.max(4))
    .fetch_all(&db.pool)
    .await
    .context("list_mints_for_dead_scan")?;
    Ok(rows)
}

pub async fn list_price_points_since_asc(
    db: &Db,
    mint: &str,
    since: DateTime<Utc>,
) -> Result<Vec<DbPricePoint>> {
    let rows = sqlx::query_as::<_, DbPricePoint>(
        r#"
        select ts, price_usd
        from token_prices
        where mint = $1 and ts >= $2
        order by ts asc;
        "#,
    )
    .bind(mint)
    .bind(since)
    .fetch_all(&db.pool)
    .await
    .context("list_price_points_since_asc")?;
    Ok(rows)
}

