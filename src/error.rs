use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbToolsError {
    #[error("Config error: {0}")]
    Config(String),

    // #[error("Database connection failed for '{url}': {source}")]
    // Connection {
    //     url: String,
    //     #[source]
    //     source: tokio_postgres::Error,
    // },
    #[error("Query failed on table '{table}': {source}")]
    Query {
        table: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("Table '{0}' has no primary_key defined in config")]
    NoPrimaryKey(String),

    #[error("No target database configured (required for sync)")]
    NoTargetDb,

    // #[error("Unsupported column type '{col_type}' for column '{column}'")]
    // UnsupportedType { col_type: String, column: String },
    #[error("IO error writing '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Serialization error: {0}")]
    Serialize(String),
    // #[error("Migration error: {0}")]
    // Migration(String),
}

/// Convenience alias
pub type Result<T, E = DbToolsError> = std::result::Result<T, E>;
