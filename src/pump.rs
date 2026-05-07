use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::types::CreateMintExtract;

/// Metaplex Token Metadata program (names on-chain for many SPL mints).
const METAPLEX_METADATA_PROGRAM: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";

pub fn anchor_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{name}").as_bytes());
    let hash = hasher.finalize();

    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

pub fn extract_create_mint(
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

pub fn short_sig(signature: &str) -> String {
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

pub fn enhanced_url(base: &str, address: &str, api_key: &str, before: Option<&str>, limit: usize) -> String {
    let mut url = format!(
        "{}/v0/addresses/{}/transactions?api-key={}&limit={}",
        base.trim_end_matches('/'),
        address,
        api_key,
        limit
    );
    if let Some(before_sig) = before {
        url.push_str("&before=");
        url.push_str(before_sig);
    }
    url
}

