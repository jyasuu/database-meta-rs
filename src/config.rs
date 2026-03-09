use serde::Deserialize;

/// Top-level YAML config structure
#[derive(Debug, Deserialize)]
pub struct Config {
    pub databases: Databases,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct Databases {
    pub source: DbConfig,
    pub target: Option<DbConfig>,
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    #[serde(rename = "jdbcUrl")]
    pub jdbc_url: String,
    pub username: String,
    pub password: String,
}

impl DbConfig {
    /// Convert JDBC URL (jdbc:postgresql://host:port/db) to postgres connection string
    pub fn to_pg_connection_string(&self) -> String {
        // Strip "jdbc:" prefix if present
        let url = self.jdbc_url
            .strip_prefix("jdbc:")
            .unwrap_or(&self.jdbc_url);
        // url is now like: postgresql://host:port/dbname
        // postgres crate accepts: postgresql://user:pass@host:port/dbname
        // We inject credentials into the URL
        if let Some(rest) = url.strip_prefix("postgresql://") {
            format!(
                "postgresql://{}:{}@{}",
                self.username, self.password, rest
            )
        } else {
            // Fallback: return as-is
            url.to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub name: String,
    /// Comma-separated column names to order by
    pub order: Option<String>,
    pub columns: Vec<ColumnConfig>,
    /// Primary key columns (used in compare-tables)
    pub primary_key: Option<Vec<String>>,
}

impl TableConfig {
    /// Returns the list of order columns, filtering empty strings
    pub fn order_columns(&self) -> Vec<String> {
        match &self.order {
            None => vec![],
            Some(s) => s
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnConfig {
    pub column_name: String,
    pub is_track: String, // "true" / "false"  (matches Java's string comparison)
    #[serde(rename = "type")]
    pub col_type: Option<String>,
    pub default: Option<String>,
}

impl ColumnConfig {
    pub fn is_tracked(&self) -> bool {
        self.is_track == "true"
    }
}

/// Load and parse the YAML config file
pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}
