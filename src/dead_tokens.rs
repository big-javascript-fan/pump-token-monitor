//! Marks tokens as dead when price collapses (>50% below the 24h peak) and keeps falling.

use anyhow::Result;
use chrono::{Duration, Utc};

use crate::db::Db;
use crate::telegram::{fmt_usd_label, TelegramNotifier};

pub fn qualifies_dead_token(points: &[(chrono::DateTime<chrono::Utc>, f64)]) -> bool {
    if points.len() < 4 {
        return false;
    }

    let mut peak = 0.0_f64;
    for (_, px) in points {
        if px.is_finite() && *px > peak {
            peak = *px;
        }
    }
    if peak <= 0.0 {
        return false;
    }

    let last = points.last().unwrap().1;
    if !last.is_finite() || last <= 0.0 {
        return false;
    }

    // More than 50% drawdown from the peak observed in this window.
    if last > peak * 0.5 {
        return false;
    }

    let n = points.len();
    // Still drifting down: last three samples strictly decrease.
    points[n - 3].1 > points[n - 2].1 && points[n - 2].1 > points[n - 1].1
}

#[derive(Debug)]
pub struct DeadTokenAlert {
    pub mint: String,
    pub name: String,
    pub first_price_usd: Option<f64>,
    pub last_price_usd: Option<f64>,
}

pub async fn scan_and_mark_dead(db: &Db) -> Result<Vec<DeadTokenAlert>> {
    let mints = crate::db::list_mints_for_dead_scan(db, 4).await?;
    let since = Utc::now() - Duration::hours(24);
    let mut alerts = Vec::new();

    for mint in mints {
        let pts = crate::db::list_price_points_since_asc(db, &mint, since).await?;
        if !qualifies_dead_token(&pts) {
            continue;
        }
        let row_opt = crate::db::get_token(db, &mint).await?;
        let Some(row) = row_opt else {
            continue;
        };
        let alert = DeadTokenAlert {
            mint: row.0.clone(),
            name: row.1.clone(),
            first_price_usd: row.4,
            last_price_usd: row.6,
        };
        crate::db::mark_token_dead(db, &mint).await?;
        alerts.push(alert);
    }

    Ok(alerts)
}

async fn notify_dead_tokens(tg: Option<&TelegramNotifier>, alerts: &[DeadTokenAlert]) {
    let Some(bot) = tg else {
        return;
    };
    for a in alerts {
        let pct = match (a.first_price_usd, a.last_price_usd) {
            (Some(f), Some(l)) if f.is_finite() && l.is_finite() && f > 0.0 => {
                Some((l - f) / f * 100.0)
            }
            _ => None,
        };
        let pct_str = pct
            .filter(|p| p.is_finite())
            .map(|p| format!("{p:+.2}% vs first"))
            .unwrap_or_else(|| "—".to_string());
        let msg = format!(
            "Dead token flagged\n\
             Name: {}\n\
             Contract: {}\n\
             First price USD: {}\n\
             Last price USD: {}\n\
             Change vs first: {}",
            a.name,
            a.mint,
            fmt_usd_label(a.first_price_usd),
            fmt_usd_label(a.last_price_usd),
            pct_str,
        );
        if let Err(e) = bot.send_plain(&msg).await {
            eprintln!("[dead-token-cron] telegram send failed: {:#}", e);
        }
    }
}

pub async fn run_dead_token_cron(db: Db, telegram: Option<TelegramNotifier>) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60 * 60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        eprintln!("[dead-token-cron] scanning...");
        match scan_and_mark_dead(&db).await {
            Ok(alerts) => {
                eprintln!("[dead-token-cron] newly marked dead: {}", alerts.len());
                notify_dead_tokens(telegram.as_ref(), &alerts).await;
            }
            Err(e) => eprintln!("[dead-token-cron] error: {:#}", e),
        }
        interval.tick().await;
    }
}
