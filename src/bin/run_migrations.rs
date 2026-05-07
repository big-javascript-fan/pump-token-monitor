//! Apply idempotent SQL files from `./migrations` (additive `IF NOT EXISTS` style).
//!
//! Usage:
//!   DATABASE_URL=... cargo run --bin run_migrations
//!   cargo run --bin run_migrations -- path/to/config.toml
//!
//! Each `.sql` file may contain multiple `;`-terminated statements. Lines starting with `--` are skipped.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Default, Deserialize)]
struct MinimalConfig {
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args.get(1).map(String::as_str).unwrap_or("config.toml");
    let database_url = resolve_database_url(config_path)?;
    let migrations_dir = env_migrations_dir();
    apply_migrations(&database_url, &migrations_dir).await?;
    eprintln!(
        "[migrations] OK (applied all statements under {})",
        migrations_dir.display()
    );
    Ok(())
}

fn env_migrations_dir() -> PathBuf {
    if let Ok(p) = std::env::var("MIGRATIONS_DIR") {
        let q = PathBuf::from(p.trim());
        if !q.as_os_str().is_empty() {
            return q;
        }
    }
    PathBuf::from("migrations")
}

async fn apply_migrations(database_url: &str, migrations_dir: &Path) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
        .context("failed connecting to Postgres")?;

    if !migrations_dir.is_dir() {
        eprintln!(
            "[migrations] skip: directory not found {}",
            migrations_dir.display()
        );
        return Ok(());
    }

    let mut paths: Vec<PathBuf> = fs::read_dir(migrations_dir)
        .with_context(|| format!("read_dir {}", migrations_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    paths.sort();

    if paths.is_empty() {
        eprintln!("[migrations] no *.sql files in {}", migrations_dir.display());
        return Ok(());
    }

    for p in paths {
        let rel = p.display();
        let text = fs::read_to_string(&p).with_context(|| format!("read {}", rel))?;
        for stmt in split_sql_statements(&text) {
            eprintln!("[migrations] {} ...", truncate(&stmt, 72));
            sqlx::query(&stmt).execute(&pool).await.with_context(|| {
                anyhow!("failed executing statement from {}", rel)
            })?;
        }
    }

    pool.close().await;
    Ok(())
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed.starts_with("--")
            || trimmed.to_ascii_lowercase().starts_with("comment ")
        {
            continue;
        }
        cur.push_str(trimmed);
        cur.push('\n');
    }
    for part in cur.split(';') {
        let s = part.trim().to_string();
        if !s.is_empty() {
            out.push(s);
        }
    }
    out
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    format!("{}…", &s[..max])
}

fn resolve_database_url(config_path: &str) -> Result<String> {
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
        .ok_or_else(|| anyhow!("set DATABASE_URL or database_url in {}", config_path))
}
