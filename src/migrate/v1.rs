//! V1 config schema — mirrors the original Java YAML format exactly.
//! Used only for deserialization during migration; never used at runtime.

use serde::Deserialize;
use anyhow::Result;
use crate::error::DbToolsError;

#[derive(Debug, Deserialize)]
pub struct V1Config {
    pub databases: V1Databases,
    pub tables: Vec<V1TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct V1Databases {
    pub source: V1DbConfig,
    pub target: Option<V1DbConfig>,
}

/// v1 used `jdbcUrl` + separate username/password
#[derive(Debug, Deserialize)]
pub struct V1DbConfig {
    #[serde(rename = "jdbcUrl")]
    pub jdbc_url: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct V1TableConfig {
    pub name: String,
    pub order: Option<String>,
    pub primary_key: Option<Vec<String>>,
    pub columns: Vec<V1ColumnConfig>,
}

/// v1 used bare string "true"/"false" for is_track, and a loose `type` field
#[derive(Debug, Deserialize)]
pub struct V1ColumnConfig {
    pub column_name: String,
    /// "true" or "false" as a plain string
    pub is_track: String,
    #[serde(rename = "type")]
    pub col_type: Option<String>,
    pub default: Option<String>,
}

// ─── Loader ───────────────────────────────────────────────────────────────────

pub fn load(path: &str) -> Result<V1Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| DbToolsError::Io { path: path.to_string(), source: e })?;
    serde_yaml::from_str(&content)
        .map_err(|e| DbToolsError::Config(format!("v1 parse error: {}", e)).into())
}
