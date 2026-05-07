use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::fs;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct ImportTokenRecord {
    slot: u64,
    name: String,
    #[serde(rename = "token_contract_address")]
    mint: String,
}

#[derive(Debug, Default, Deserialize)]
struct MinimalConfig {
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Usage:
    //   cargo run --bin import_token_json --release
    //   cargo run --bin import_token_json --release -- path/to/token.json
    //   cargo run --bin import_token_json --release -- path/to/token.json path/to/config.toml
    let args: Vec<String> = std::env::args().collect();
    let token_path = args.get(1).map(String::as_str).unwrap_or("token.json");
    let config_path = args.get(2).map(String::as_str).unwrap_or("config.toml");

    let database_url = resolve_database_url(config_path)?;
    let pool = connect_db(&database_url).await?;

    let bytes = fs::read(token_path).with_context(|| format!("failed reading {}", token_path))?;
    let tokens: Vec<ImportTokenRecord> =
        serde_json::from_slice(&bytes).context("failed parsing token.json")?;

    eprintln!("[import] loaded {} tokens from {}", tokens.len(), token_path);

    let mut imported = 0u64;
    for t in tokens {
        upsert_token(&pool, &t).await?;
        imported += 1;
        if imported % 1000 == 0 {
            eprintln!("[import] upserted {}", imported);
        }
    }

    eprintln!("[import] done. upserted {}", imported);
    Ok(())
}

fn resolve_database_url(config_path: &str) -> Result<String> {
    // Optional override via env var.
    if let Ok(v) = std::env::var("DATABASE_URL") {
        let t = v.trim().to_string();
        if !t.is_empty() {
            return Ok(t);
        }
    }

    let content = fs::read_to_string(config_path)
        .with_context(|| format!("failed reading {}", config_path))?;
    let cfg = toml::from_str::<MinimalConfig>(&content)
        .with_context(|| format!("failed parsing {}", config_path))?;

    cfg.database_url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("database_url is missing in {} (or set DATABASE_URL)", config_path))
}

async fn connect_db(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
        .context("failed to connect to Postgres") // expects pump_tokens table already exists
}

async fn upsert_token(pool: &PgPool, t: &ImportTokenRecord) -> Result<()> {
    let mint = sanitize_pg_text(&t.mint, 64);
    let name = sanitize_pg_text(&t.name, 256);
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
    .bind(mint)
    .bind(name)
    .bind(t.slot as i64)
    .execute(pool)
    .await
    .context("failed to upsert token")?;
    Ok(())
}

fn sanitize_pg_text(s: &str, max_len: usize) -> String {
    // Postgres text/varchar disallows NUL (0x00). Some on-chain strings can contain it.
    let mut out: String = s.chars().filter(|&c| c != '\0').collect();
    out = out.trim().to_string();
    if out.len() > max_len {
        out.truncate(max_len);
    }
    out
}

