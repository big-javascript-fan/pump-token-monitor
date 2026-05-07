use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

use crate::types::TokenRecord;

pub fn persist_tokens(output_path: &str, tokens_by_mint: &HashMap<String, TokenRecord>) -> Result<()> {
    let mut tokens: Vec<TokenRecord> = tokens_by_mint.values().cloned().collect();
    tokens.sort_by(|a, b| b.slot.cmp(&a.slot));

    let mut file = File::create(output_path)
        .with_context(|| format!("failed to create {}", output_path))?;
    let output = serde_json::to_string_pretty(&tokens).context("failed to serialize tokens")?;
    file.write_all(output.as_bytes())
        .with_context(|| format!("failed writing {}", output_path))?;
    Ok(())
}

