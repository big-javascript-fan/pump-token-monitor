use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer};
use std::fs;

fn deserialize_optional_telegram_chat_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ChatIdToml {
        Int(i64),
        Str(String),
    }

    let opt = Option::<ChatIdToml>::deserialize(deserializer)?;
    Ok(opt
        .map(|v| match v {
            ChatIdToml::Int(n) => n.to_string(),
            ChatIdToml::Str(s) => s,
        })
        .map(|raw| crate::telegram::normalize_telegram_chat_id_raw(&raw))
        .filter(|s| !s.is_empty()))
}

const CONFIG_PATH: &str = "config.toml";

const DEFAULT_RPC_URL: &str = "https://mainnet.helius-rpc.com/?api-key=";
const DEFAULT_PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const DEFAULT_HELIUS_ENHANCED_BASE_URL: &str = "https://api.helius.xyz";
const DEFAULT_PRICE_API_BASE_URL: &str = "https://api.jup.ag/price/v3";
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8080";

pub const DEFAULT_SIGNATURES_PAGE_SIZE: usize = 1000;
pub const DEFAULT_MAX_SIGNATURES: usize = 500_000;
pub const DEFAULT_TX_REQUEST_DELAY_MS: u64 = 40;

#[derive(Debug, Default, Deserialize)]
pub struct AppConfig {
    /// Base58-encoded 64-byte Solana CLI keypair **secret** — test wallets only (keep out of Git).
    pub wallet_secret_key_base58: Option<String>,
    pub helius_api_key: Option<String>,
    pub rpc_url: Option<String>,
    pub pump_program_id: Option<String>,
    pub stream_mode: Option<String>,
    pub helius_enhanced_base_url: Option<String>,
    pub database_url: Option<String>,
    /// Required for on-chain swaps: `x-api-key` on Jupiter Swap v2 `/order` + `/execute`. See https://developers.jup.ag/docs/swap/order-and-execute
    pub jupiter_api_key: Option<String>,
    pub price_api_base_url: Option<String>,
    pub http_bind: Option<String>,
    pub max_signatures: Option<usize>,
    pub tx_request_delay_ms: Option<u64>,
    pub signatures_page_size: Option<usize>,
    /// Telegram Bot API token (`sendMessage`). Needs `telegram_chat_id`.
    pub telegram_bot_token: Option<String>,
    /// Channel id (e.g. `-100…`) or `@channelusername` where the bot may post.
    /// Unquoted negative integers in TOML are accepted (e.g. `-1001234567890`).
    #[serde(default, deserialize_with = "deserialize_optional_telegram_chat_id")]
    pub telegram_chat_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RpcBackfillParams {
    pub max_signatures: usize,
    pub tx_delay_ms: u64,
    pub page_size: usize,
}

#[derive(Debug, Clone)]
pub struct AppRuntime {
    /// Presence is reflected in `/health` as `wallet`; the raw value is never logged.
    #[allow(dead_code)]
    pub wallet_secret_key_base58: Option<String>,
    pub rpc_url: String,
    pub pump_program_id: String,
    pub helius_api_key: Option<String>,
    pub helius_enhanced_base_url: String,
    pub database_url: Option<String>,
    pub jupiter_api_key: Option<String>,
    pub price_api_base_url: String,
    pub http_bind: String,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

pub fn load_config() -> Result<AppConfig> {
    let content = fs::read_to_string(CONFIG_PATH)
        .with_context(|| format!("config required: place settings in {}", CONFIG_PATH))?;
    toml::from_str::<AppConfig>(&content).with_context(|| format!("failed parsing {}", CONFIG_PATH))
}

pub fn build_runtime(config: &AppConfig) -> (AppRuntime, String, RpcBackfillParams) {
    let stream_mode = resolve_stream_mode(config);
    let rpc_backfill = RpcBackfillParams {
        max_signatures: config.max_signatures.unwrap_or(DEFAULT_MAX_SIGNATURES),
        tx_delay_ms: config.tx_request_delay_ms.unwrap_or(DEFAULT_TX_REQUEST_DELAY_MS),
        page_size: config
            .signatures_page_size
            .unwrap_or(DEFAULT_SIGNATURES_PAGE_SIZE)
            .min(1000)
            .max(1),
    };

    let runtime = AppRuntime {
        wallet_secret_key_base58: config
            .wallet_secret_key_base58
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        rpc_url: resolve_rpc_url(config),
        pump_program_id: resolve_pump_program_id(config),
        helius_api_key: resolve_helius_api_key(config),
        helius_enhanced_base_url: resolve_helius_enhanced_base_url(config),
        database_url: resolve_database_url(config),
        jupiter_api_key: resolve_jupiter_api_key(config),
        price_api_base_url: resolve_price_api_base_url(config),
        http_bind: resolve_http_bind(config),
        telegram_bot_token: resolve_telegram_bot_token(config),
        telegram_chat_id: resolve_telegram_chat_id(config),
    };

    (runtime, stream_mode, rpc_backfill)
}

fn resolve_rpc_url(config: &AppConfig) -> String {
    if let Some(url) = config.rpc_url.as_deref() {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(api_key) = resolve_helius_api_key(config) {
        return format!("{DEFAULT_RPC_URL}{api_key}");
    }
    DEFAULT_RPC_URL.to_string()
}

fn resolve_helius_api_key(config: &AppConfig) -> Option<String> {
    config
        .helius_api_key
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn resolve_pump_program_id(config: &AppConfig) -> String {
    config
        .pump_program_id
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_PUMP_PROGRAM_ID.to_string())
}

fn resolve_stream_mode(config: &AppConfig) -> String {
    config
        .stream_mode
        .as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "backfill".to_string())
}

fn resolve_helius_enhanced_base_url(config: &AppConfig) -> String {
    config
        .helius_enhanced_base_url
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_HELIUS_ENHANCED_BASE_URL.to_string())
}

fn resolve_database_url(config: &AppConfig) -> Option<String> {
    config
        .database_url
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn resolve_jupiter_api_key(config: &AppConfig) -> Option<String> {
    config
        .jupiter_api_key
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn resolve_price_api_base_url(config: &AppConfig) -> String {
    config
        .price_api_base_url
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_PRICE_API_BASE_URL.to_string())
}

fn resolve_http_bind(config: &AppConfig) -> String {
    config
        .http_bind
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_HTTP_BIND.to_string())
}

fn resolve_telegram_bot_token(config: &AppConfig) -> Option<String> {
    config
        .telegram_bot_token
        .as_ref()
        .map(|s| {
            s.trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .to_string()
        })
        .filter(|s| !s.is_empty())
}

fn resolve_telegram_chat_id(config: &AppConfig) -> Option<String> {
    config
        .telegram_chat_id
        .as_ref()
        .map(|s| crate::telegram::normalize_telegram_chat_id_raw(s))
        .filter(|s| !s.is_empty())
}

