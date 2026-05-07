//! Optional Telegram Bot API notifications (`sendMessage`).

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

#[derive(Clone)]
pub struct TelegramNotifier {
    client: Client,
    bot_token: String,
    chat_id: String,
}

/// Normalize chat id from config (quotes, unicode minus, whitespace).
pub fn normalize_telegram_chat_id_raw(s: &str) -> String {
    s.trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .replace('\u{2212}', "-")
        .trim()
        .to_string()
}

/// Telegram accepts integer or string `chat_id`; numeric strings as JSON numbers avoid edge cases.
fn chat_id_json_field(chat_id: &str) -> Value {
    let s = normalize_telegram_chat_id_raw(chat_id);
    if let Ok(n) = s.parse::<i64>() {
        return serde_json::Number::from(n).into();
    }
    Value::String(s)
}

impl TelegramNotifier {
    pub fn new(client: Client, bot_token: String, chat_id: String) -> Self {
        Self {
            client,
            bot_token,
            chat_id,
        }
    }

    /// Returns `None` if token or chat id is missing / empty.
    pub fn from_runtime(client: Client, bot_token: Option<&str>, chat_id: Option<&str>) -> Option<Self> {
        let token = normalize_bot_token(bot_token?);
        let chat = normalize_telegram_chat_id_raw(chat_id?);
        if token.is_empty() || chat.is_empty() {
            return None;
        }
        Some(Self::new(client, token, chat))
    }

    /// Verifies token (`getMe`) and that the bot can reach `chat_id` (`getChat`). Call once at startup for actionable errors.
    pub async fn validate_on_startup(&self) -> Result<()> {
        let me_url = format!(
            "https://api.telegram.org/bot{}/getMe",
            self.bot_token
        );
        let me_res = self
            .client
            .get(me_url)
            .send()
            .await
            .context("telegram getMe request failed")?;
        if !me_res.status().is_success() {
            let status = me_res.status();
            let body = me_res.text().await.unwrap_or_default();
            anyhow::bail!("telegram getMe HTTP {}: {}", status, body);
        }

        let chat_url = format!(
            "https://api.telegram.org/bot{}/getChat",
            self.bot_token
        );
        let body = serde_json::json!({
            "chat_id": chat_id_json_field(&self.chat_id),
        });
        let chat_res = self
            .client
            .post(chat_url)
            .json(&body)
            .send()
            .await
            .context("telegram getChat request failed")?;
        if !chat_res.status().is_success() {
            let status = chat_res.status();
            let body = chat_res.text().await.unwrap_or_default();
            anyhow::bail!(
                "telegram getChat HTTP {}: {}\n\
                 Hint: \"chat not found\" usually means wrong chat_id, or the bot has no access yet. \
                 For a private DM use your numeric user id after sending /start to the bot. \
                 For a channel/supergroup add the bot as admin and use the numeric id (often -100…) or @channelusername.",
                status,
                body
            );
        }
        Ok(())
    }

    pub async fn send_plain(&self, text: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );
        let body = serde_json::json!({
            "chat_id": chat_id_json_field(&self.chat_id),
            "text": text,
            "disable_web_page_preview": true,
        });
        let res = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("telegram sendMessage request failed")?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(send_message_failure(status, body));
        }
        Ok(())
    }

    /// Telegram Bot API `parse_mode: HTML` (subset). Caller must escape untrusted text with [`escape_html`].
    pub async fn send_html(&self, html: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );
        let body = serde_json::json!({
            "chat_id": chat_id_json_field(&self.chat_id),
            "text": html,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        let res = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("telegram sendMessage (HTML) request failed")?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(send_message_failure(status, body));
        }
        Ok(())
    }
}

fn send_message_failure(status: reqwest::StatusCode, body: String) -> anyhow::Error {
    let hint = telegram_send_failure_hint(status.as_u16(), &body);
    if hint.is_empty() {
        anyhow::anyhow!("telegram sendMessage HTTP {}: {}", status, body)
    } else {
        anyhow::anyhow!("telegram sendMessage HTTP {}: {}\n{}", status, body, hint)
    }
}

fn normalize_bot_token(s: &str) -> String {
    s.trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .to_string()
}

fn telegram_send_failure_hint(status: u16, body: &str) -> &'static str {
    if status == 400 && body.contains("chat not found") {
        "Hint: bot cannot see this chat yet — open a private chat with the bot and send /start (DM), or add the bot as an admin to the channel/group and use the correct numeric chat id (-100… for channels)."
    } else {
        ""
    }
}

pub fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn solscan_account_url(mint: &str) -> String {
    format!("https://solscan.io/account/{}", mint.trim())
}

pub fn fmt_usd_label(v: Option<f64>) -> String {
    match v {
        None => "—".to_string(),
        Some(x) if !x.is_finite() => "—".to_string(),
        Some(x) if x >= 1.0 => format!("${:.6}", x),
        Some(x) if x >= 1e-6 => format!("${:.8}", x),
        Some(x) => format!("${:.12}", x),
    }
}
