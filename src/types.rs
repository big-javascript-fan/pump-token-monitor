use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub slot: u64,
    /// Human-readable name from Pump instruction args or Metaplex metadata in the same tx.
    pub name: String,
    #[serde(rename = "token_contract_address")]
    pub mint: String,
}

/// Parsed create / create_v2 hit (used before building `TokenRecord`).
pub struct CreateMintExtract {
    pub mint: String,
    pub name: String,
}

