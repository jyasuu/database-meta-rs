use crate::error::{DbToolsError, Result};
use serde::{Deserialize, Serialize};
use std::fs;

// ─── Top-level ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub databases: Databases,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Databases {
    pub source: DbConfig,
    pub target: Option<DbConfig>,
}

// ─── Database connection ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbConfig {
    /// PostgreSQL connection string.
    /// Accepts both JDBC format  (jdbc:postgresql://host/db)
    /// and native format         (postgresql://user:pass@host/db  or  host=... dbname=...)
    pub url: String,

    /// Separate credentials — optional if already embedded in url
    pub username: Option<String>,
    pub password: Option<String>,
}

impl DbConfig {
    /// Return a tokio-postgres compatible connection string
    pub fn connection_string(&self) -> String {
        // Strip JDBC prefix if present
        let url = self.url.strip_prefix("jdbc:").unwrap_or(&self.url);

        // If credentials are provided separately, inject them
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            if let Some(rest) = url.strip_prefix("postgresql://") {
                // Avoid double-embedding if user already in URL
                if !rest.contains('@') {
                    return format!("postgresql://{}:{}@{}", user, pass, rest);
                }
            }
        }
        url.to_string()
    }
}

// ─── Table config ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableConfig {
    pub name: String,

    /// Optional schema qualifier, e.g. "public"
    pub schema: Option<String>,

    /// Comma-separated column names to sort output by
    pub order: Option<String>,

    /// Primary key columns (required for sync command)
    pub primary_key: Option<Vec<String>>,

    pub columns: Vec<ColumnConfig>,
}

impl TableConfig {
    /// Qualified table name, e.g. `"public"."users"`
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("\"{}\".\"{}\"", schema, self.name),
            None => format!("\"{}\"", self.name),
        }
    }

    /// Parsed and trimmed order columns, empty strings removed
    pub fn order_columns(&self) -> Vec<String> {
        self.order
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    /// Validated primary keys
    pub fn required_primary_keys(&self) -> Result<&Vec<String>> {
        self.primary_key
            .as_ref()
            .ok_or_else(|| DbToolsError::NoPrimaryKey(self.name.clone()))
    }
}

// ─── Column config ────────────────────────────────────────────────────────────

/// Whether a column value is read from the DB or replaced with a default
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TrackMode {
    /// Read the actual DB value
    True,
    /// Substitute a configured default instead
    False,
}

impl TrackMode {
    pub fn is_tracked(&self) -> bool {
        *self == TrackMode::True
    }
}

/// Supported column value types
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Numeric,
    #[default]
    String,
    Bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnConfig {
    pub column_name: String,

    /// "true" → read from DB, "false" → substitute default
    pub is_track: TrackMode,

    #[serde(rename = "type", default)]
    pub col_type: ColumnType,

    /// Default value used when is_track = false
    pub default: Option<String>,
}

impl ColumnConfig {
    pub fn is_tracked(&self) -> bool {
        self.is_track.is_tracked()
    }
}

// ─── Loader ───────────────────────────────────────────────────────────────────

pub fn load(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path).map_err(|e| DbToolsError::Io {
        path: path.to_string(),
        source: e,
    })?;

    serde_yaml::from_str(&content).map_err(|e| DbToolsError::Config(e.to_string()))
}
