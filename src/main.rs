use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_RPC_URL: &str = "https://mainnet.helius-rpc.com/?api-key=";
const DEFAULT_PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const DEFAULT_HELIUS_ENHANCED_BASE_URL: &str = "https://api.helius.xyz";
/// Metaplex Token Metadata program (names on-chain for many SPL mints).
const METAPLEX_METADATA_PROGRAM: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
const MAX_RPC_RETRIES: usize = 7;
const INITIAL_RETRY_BACKOFF_MS: u64 = 500;
const CONFIG_PATH: &str = "config.toml";
const OUTPUT_PATH: &str = "token.json";
const ENHANCED_POLL_LIMIT: usize = 1000;
const DEFAULT_SIGNATURES_PAGE_SIZE: usize = 1000;
const DEFAULT_MAX_SIGNATURES: usize = 500_000;
const DEFAULT_TX_REQUEST_DELAY_MS: u64 = 40;

#[derive(Debug, Deserialize)]
struct SignatureInfo {
    signature: String,
    #[serde(default)]
    slot: Option<u64>,
    #[serde(rename = "blockTime")]
    #[allow(dead_code)]
    block_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize)]
struct TokenRecord {
    slot: u64,
    /// Human-readable name from Pump instruction args or Metaplex metadata in the same tx.
    name: String,
    #[serde(rename = "token_contract_address")]
    mint: String,
}

/// Parsed create / create_v2 hit (used before building `TokenRecord`).
struct CreateMintExtract {
    mint: String,
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct AppConfig {
    helius_api_key: Option<String>,
    rpc_url: Option<String>,
    pump_program_id: Option<String>,
    stream_mode: Option<String>,
    helius_enhanced_base_url: Option<String>,
    /// Limit how many signatures to scan during RPC backfill (default: large cap).
    max_signatures: Option<usize>,
    /// Delay between getTransaction calls in backfill (rate limiting).
    tx_request_delay_ms: Option<u64>,
    /// Page size for getSignaturesForAddress (max 1000 on most RPCs).
    signatures_page_size: Option<usize>,
}

#[derive(Debug, Clone)]
struct RpcBackfillParams {
    max_signatures: usize,
    tx_delay_ms: u64,
    page_size: usize,
}

#[derive(Debug, Clone)]
struct AppRuntime {
    rpc_url: String,
    pump_program_id: String,
    helius_api_key: Option<String>,
    helius_enhanced_base_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config().context("failed to load config.toml")?;
    let runtime = AppRuntime {
        rpc_url: resolve_rpc_url(&config),
        pump_program_id: resolve_pump_program_id(&config),
        helius_api_key: resolve_helius_api_key(&config),
        helius_enhanced_base_url: resolve_helius_enhanced_base_url(&config),
    };

    let stream_mode = resolve_stream_mode(&config);
    let rpc_backfill = RpcBackfillParams {
        max_signatures: config.max_signatures.unwrap_or(DEFAULT_MAX_SIGNATURES),
        tx_delay_ms: config.tx_request_delay_ms.unwrap_or(DEFAULT_TX_REQUEST_DELAY_MS),
        page_size: config
            .signatures_page_size
            .unwrap_or(DEFAULT_SIGNATURES_PAGE_SIZE)
            .min(1000)
            .max(1),
    };
    let client = Client::builder()
        .user_agent("token-monitor/0.3")
        .timeout(Duration::from_secs(45))
        .build()
        .context("failed to create HTTP client")?;

    eprintln!("Using RPC endpoint: {}", runtime.rpc_url);
    eprintln!("Using Pump program: {}", runtime.pump_program_id);
    eprintln!("Stream mode: {}", stream_mode);

    let state = Arc::new(Mutex::new(HashMap::<String, TokenRecord>::new()));
    let runtime = Arc::new(runtime);

    match stream_mode.as_str() {
        "logs" => run_logs_subscribe(client, runtime, state).await?,
        "backfill" => run_rpc_backfill(client, runtime, state, rpc_backfill).await?,
        "enhanced" => run_enhanced_backfill(client, runtime, state).await?,
        "both" => {
            let c1 = client.clone();
            let c2 = client;
            let r1 = runtime.clone();
            let r2 = runtime;
            let s1 = state.clone();
            let s2 = state;
            let p = rpc_backfill.clone();

            tokio::select! {
                res = run_logs_subscribe(c1, r1, s1) => res?,
                res = run_rpc_backfill(c2, r2, s2, p) => res?,
            }
        }
        other => return Err(anyhow!("unsupported stream_mode in config.toml: {}", other)),
    }

    Ok(())
}

async fn run_logs_subscribe(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: Arc<Mutex<HashMap<String, TokenRecord>>>,
) -> Result<()> {
    let ws_url = http_to_ws(&runtime.rpc_url)?;
    eprintln!("[logsSubscribe] connecting to {}", ws_url);
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed connecting websocket: {}", ws_url))?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe = json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"logsSubscribe",
        "params":[
            { "mentions":[runtime.pump_program_id] },
            { "commitment":"confirmed" }
        ]
    });
    write
        .send(Message::Text(subscribe.to_string()))
        .await
        .context("failed sending logsSubscribe")?;
    eprintln!("[logsSubscribe] subscription request sent");

    let create_discriminator = anchor_discriminator("create");
    let create_v2_discriminator = anchor_discriminator("create_v2");

    while let Some(msg) = read.next().await {
        let msg = msg.context("websocket read failed")?;
        if !msg.is_text() {
            continue;
        }
        let text = msg.to_text().context("failed to read websocket text")?;
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let signature = value
            .get("params")
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("value"))
            .and_then(|v| v.get("signature"))
            .and_then(Value::as_str);

        let Some(signature) = signature else { continue };
        eprintln!("[logsSubscribe] received signature {}", short_sig(signature));

        let tx = get_transaction(&client, &runtime.rpc_url, signature).await?;
        if let Some(extracted) = extract_create_mint(
            &tx,
            &runtime.pump_program_id,
            &create_discriminator,
            &create_v2_discriminator,
        ) {
            let slot = tx.get("slot").and_then(Value::as_u64).unwrap_or_default();
            upsert_token(
                &state,
                TokenRecord {
                    slot,
                    name: extracted.name,
                    mint: extracted.mint,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_rpc_backfill(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: Arc<Mutex<HashMap<String, TokenRecord>>>,
    params: RpcBackfillParams,
) -> Result<()> {
    if runtime.rpc_url == DEFAULT_RPC_URL {
        return Err(anyhow!(
            "set helius_api_key or rpc_url in config.toml for JSON-RPC backfill"
        ));
    }

    let create_discriminator = anchor_discriminator("create");
    let create_v2_discriminator = anchor_discriminator("create_v2");

    let mut before: Option<String> = None;
    let mut scanned = 0usize;

    loop {
        if scanned >= params.max_signatures {
            eprintln!("[rpc:backfill] reached max_signatures limit ({})", params.max_signatures);
            break;
        }

        let page_limit = (params.max_signatures - scanned).min(params.page_size);
        eprintln!(
            "[rpc:backfill] getSignaturesForAddress page (before={}, limit={}, scanned={})",
            before.as_deref().unwrap_or("<none>"),
            page_limit,
            scanned
        );

        let signatures = get_signatures_page(
            &client,
            &runtime.rpc_url,
            &runtime.pump_program_id,
            before.as_deref(),
            page_limit,
        )
        .await?;

        if signatures.is_empty() {
            eprintln!("[rpc:backfill] no more signatures; done");
            break;
        }

        for sig_info in &signatures {
            if scanned >= params.max_signatures {
                break;
            }
            scanned += 1;

            let tx_result = get_transaction(&client, &runtime.rpc_url, &sig_info.signature).await;
            let tx = match tx_result {
                Ok(t) => t,
                Err(e) => {
                    eprintln!(
                        "[rpc:backfill] skip sig {} — {}",
                        short_sig(&sig_info.signature),
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(params.tx_delay_ms)).await;
                    continue;
                }
            };

            if let Some(extracted) = extract_create_mint(
                &tx,
                &runtime.pump_program_id,
                &create_discriminator,
                &create_v2_discriminator,
            ) {
                let slot = tx.get("slot").and_then(Value::as_u64).unwrap_or_else(|| {
                    sig_info.slot.unwrap_or(0)
                });
                upsert_token(
                    &state,
                    TokenRecord {
                        slot,
                        name: extracted.name,
                        mint: extracted.mint,
                    },
                )
                .await?;
            }

            tokio::time::sleep(Duration::from_millis(params.tx_delay_ms)).await;
        }

        before = signatures.last().map(|s| s.signature.clone());
        eprintln!(
            "[rpc:backfill] progress: scanned={}, unique_tokens={}",
            scanned,
            state.lock().await.len()
        );
    }

    let total = state.lock().await.len();
    eprintln!(
        "[rpc:backfill] finished. total unique mints: {}",
        total
    );
    Ok(())
}

async fn get_signatures_page(
    client: &Client,
    rpc_url: &str,
    program_id: &str,
    before: Option<&str>,
    limit: usize,
) -> Result<Vec<SignatureInfo>> {
    let mut cfg = json!({
        "limit": limit,
        "commitment": "confirmed",
    });
    if let Some(b) = before {
        cfg["before"] = json!(b);
    }

    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [program_id, cfg],
    });

    let response: RpcResponse<Vec<SignatureInfo>> = post_rpc(client, rpc_url, payload).await?;
    if let Some(err) = response.error {
        return Err(anyhow!("RPC error {}: {}", err.code, err.message));
    }
    Ok(response.result.unwrap_or_default())
}

async fn run_enhanced_backfill(
    client: Client,
    runtime: Arc<AppRuntime>,
    state: Arc<Mutex<HashMap<String, TokenRecord>>>,
) -> Result<()> {
    let Some(api_key) = runtime.helius_api_key.clone() else {
        return Err(anyhow!(
            "helius_api_key is required in config.toml for enhanced mode"
        ));
    };

    let mut before: Option<String> = None;
    let create_discriminator = anchor_discriminator("create");
    let create_v2_discriminator = anchor_discriminator("create_v2");

    loop {
        let url = enhanced_url(
            &runtime.helius_enhanced_base_url,
            &runtime.pump_program_id,
            &api_key,
            before.as_deref(),
        );
        eprintln!("[enhanced] polling {}", url);
        let payload: Vec<Value> = client
            .get(&url)
            .send()
            .await
            .context("enhanced API request failed")?
            .error_for_status()
            .context("enhanced API returned error status")?
            .json()
            .await
            .context("failed to parse enhanced API response")?;
        eprintln!("[enhanced] got {} transactions", payload.len());
        if payload.is_empty() {
            eprintln!("[enhanced] backfill complete (no more transactions)");
            break;
        }

        for tx in payload.iter().rev() {
            if tx.get("signature").and_then(Value::as_str).is_none() {
                continue;
            }

            if let Some(extracted) = extract_create_mint(
                tx,
                &runtime.pump_program_id,
                &create_discriminator,
                &create_v2_discriminator,
            ) {
                let slot = tx.get("slot").and_then(Value::as_u64).unwrap_or_default();
                upsert_token(
                    &state,
                    TokenRecord {
                        slot,
                        name: extracted.name,
                        mint: extracted.mint,
                    },
                )
                .await?;
            }
        }

        before = payload
            .last()
            .and_then(|tx| tx.get("signature"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
    }

    let total = state.lock().await.len();
    eprintln!("[enhanced] historical backfill finished. total unique mints: {}", total);
    Ok(())
}

async fn upsert_token(
    state: &Arc<Mutex<HashMap<String, TokenRecord>>>,
    record: TokenRecord,
) -> Result<()> {
    let mut guard = state.lock().await;
    if guard.contains_key(&record.mint) {
        return Ok(());
    }
    eprintln!(
        "[token] new mint={} name={:?} slot={}",
        record.mint, record.name, record.slot
    );
    guard.insert(record.mint.clone(), record);
    persist_tokens(OUTPUT_PATH, &guard)?;
    eprintln!("[persist] updated {}", OUTPUT_PATH);
    Ok(())
}

fn persist_tokens(output_path: &str, tokens_by_mint: &HashMap<String, TokenRecord>) -> Result<()> {
    let mut tokens: Vec<TokenRecord> = tokens_by_mint.values().cloned().collect();
    tokens.sort_by(|a, b| b.slot.cmp(&a.slot));

    let mut file = File::create(output_path)
        .with_context(|| format!("failed to create {}", output_path))?;
    let output = serde_json::to_string_pretty(&tokens).context("failed to serialize tokens")?;
    file.write_all(output.as_bytes())
        .with_context(|| format!("failed writing {}", output_path))?;
    Ok(())
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

fn load_config() -> Result<AppConfig> {
    let content = fs::read_to_string(CONFIG_PATH)
        .with_context(|| format!("config required: place settings in {}", CONFIG_PATH))?;

    toml::from_str::<AppConfig>(&content).with_context(|| format!("failed parsing {}", CONFIG_PATH))
}

async fn get_transaction(client: &Client, rpc_url: &str, signature: &str) -> Result<Value> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }
        ]
    });
    let response: RpcResponse<Value> = post_rpc(client, rpc_url, payload).await?;
    if let Some(err) = response.error {
        return Err(anyhow!("RPC error {}: {}", err.code, err.message));
    }
    response
        .result
        .ok_or_else(|| anyhow!("missing transaction for signature {}", signature))
}

async fn post_rpc<T: for<'de> Deserialize<'de>>(
    client: &Client,
    rpc_url: &str,
    payload: Value,
) -> Result<T> {
    let mut backoff_ms = INITIAL_RETRY_BACKOFF_MS;
    let method_name = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");

    for attempt in 1..=MAX_RPC_RETRIES {
        eprintln!("[rpc:http] {} attempt {}/{}", method_name, attempt, MAX_RPC_RETRIES);
        let response = client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC call failed: {}", method_name))?;

        let status = response.status();
        let body = response.text().await.context("failed reading RPC body")?;
        if status.as_u16() == 429 && attempt < MAX_RPC_RETRIES {
            eprintln!(
                "[rpc:http] 429 on {}, backing off {}ms",
                method_name, backoff_ms
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(15_000);
            continue;
        }
        if !status.is_success() {
            return Err(anyhow!("RPC HTTP {}: {}", status, body));
        }
        return serde_json::from_str(&body).context("failed parsing RPC response JSON");
    }

    Err(anyhow!("exhausted HTTP retries for RPC method {}", method_name))
}

fn extract_create_mint(
    tx: &Value,
    program_id: &str,
    create_discriminator: &[u8; 8],
    create_v2_discriminator: &[u8; 8],
) -> Option<CreateMintExtract> {
    let log_hint = infer_instruction_from_logs(tx);
    let instructions = tx
        .get("transaction")?
        .get("message")?
        .get("instructions")?
        .as_array()?;

    for ix in instructions {
        let ix_program_id = ix.get("programId")?.as_str()?;
        if ix_program_id != program_id {
            continue;
        }

        let accounts = ix.get("accounts")?.as_array()?;
        let mint = accounts.first()?.as_str()?.to_string();
        let data_str = ix.get("data").and_then(|value| value.as_str()).unwrap_or("");
        let decoded = bs58::decode(data_str).into_vec().ok();
        classify_instruction(
            decoded.as_deref(),
            create_discriminator,
            create_v2_discriminator,
            log_hint,
        )?;

        let name = resolve_token_display_name(tx, &mint, decoded.as_deref());
        return Some(CreateMintExtract { mint, name });
    }

    None
}

/// Prefer Pump Anchor args (first Borsh `String` after 8-byte discriminator = name), else Metaplex `info.name` for this mint.
fn resolve_token_display_name(tx: &Value, mint: &str, pump_ix_data: Option<&[u8]>) -> String {
    if let Some(data) = pump_ix_data {
        if let Some(n) = parse_first_borsh_string_after_discriminator(data) {
            let t = n.trim();
            if !t.is_empty() && t.len() <= 256 {
                return t.to_string();
            }
        }
    }
    metaplex_name_for_mint_in_tx(tx, mint).unwrap_or_default()
}

fn parse_first_borsh_string_after_discriminator(data: &[u8]) -> Option<String> {
    if data.len() < 8 + 4 {
        return None;
    }
    let mut off = 8;
    read_borsh_string(data, &mut off)
}

fn read_borsh_string(data: &[u8], offset: &mut usize) -> Option<String> {
    if *offset + 4 > data.len() {
        return None;
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().ok()?) as usize;
    *offset += 4;
    if len > 512 || *offset + len > data.len() {
        return None;
    }
    let slice = &data[*offset..*offset + len];
    *offset += len;
    std::str::from_utf8(slice).ok().map(str::to_owned)
}

fn metaplex_name_for_mint_in_tx(tx: &Value, mint: &str) -> Option<String> {
    let inner = tx.get("meta")?.get("innerInstructions")?.as_array()?;
    for group in inner {
        let ixs = group.get("instructions")?.as_array()?;
        for ix in ixs {
            let pid = ix.get("programId").and_then(Value::as_str)?;
            if pid != METAPLEX_METADATA_PROGRAM {
                continue;
            }
            let parsed = ix.get("parsed")?;
            let info = parsed.get("info")?;
            let m = info.get("mint").and_then(Value::as_str)?;
            if m != mint {
                continue;
            }
            if let Some(name) = info.get("name").and_then(Value::as_str) {
                let t = name.trim();
                if !t.is_empty() {
                    return Some(t.to_string());
                }
            }
        }
    }
    None
}

fn infer_instruction_from_logs(tx: &Value) -> Option<&'static str> {
    let logs = tx
        .get("meta")?
        .get("logMessages")?
        .as_array()?
        .iter()
        .filter_map(|line| line.as_str());

    for line in logs {
        if line.contains("Instruction: CreateV2") || line.contains("Instruction: Create_V2") {
            return Some("create_v2");
        }
        if line.contains("Instruction: Create") {
            return Some("create");
        }
    }
    None
}

fn classify_instruction<'a>(
    decoded_data: Option<&[u8]>,
    create_discriminator: &[u8; 8],
    create_v2_discriminator: &[u8; 8],
    log_hint: Option<&'a str>,
) -> Option<&'a str> {
    if let Some(data) = decoded_data {
        if data.len() >= 8 {
            if &data[..8] == create_discriminator {
                return Some("create");
            }
            if &data[..8] == create_v2_discriminator {
                return Some("create_v2");
            }
        }
    }

    log_hint
}

fn anchor_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{name}").as_bytes());
    let hash = hasher.finalize();

    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

fn short_sig(signature: &str) -> String {
    const PREFIX: usize = 8;
    const SUFFIX: usize = 8;
    if signature.len() <= PREFIX + SUFFIX + 3 {
        return signature.to_string();
    }
    format!(
        "{}...{}",
        &signature[..PREFIX],
        &signature[signature.len() - SUFFIX..]
    )
}

fn enhanced_url(base: &str, address: &str, api_key: &str, before: Option<&str>) -> String {
    let mut url = format!(
        "{}/v0/addresses/{}/transactions?api-key={}&limit={}",
        base.trim_end_matches('/'),
        address,
        api_key,
        ENHANCED_POLL_LIMIT
    );
    if let Some(before_sig) = before {
        url.push_str("&before=");
        url.push_str(before_sig);
    }
    url
}

fn http_to_ws(http_url: &str) -> Result<String> {
    if let Some(rest) = http_url.strip_prefix("https://") {
        return Ok(format!("wss://{}", rest));
    }
    if let Some(rest) = http_url.strip_prefix("http://") {
        return Ok(format!("ws://{}", rest));
    }
    Err(anyhow!("unsupported RPC URL for websocket: {}", http_url))
}
