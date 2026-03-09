use anyhow::{Context, Result};
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use tokio_postgres::NoTls;
use tracing::info;

use crate::config::DbConfig;

/// Thin wrapper so callers don't need to import deadpool types directly
pub struct DbPool(Pool);

impl DbPool {
    /// Build a pool from a DbConfig.
    /// Uses a pool size of 4 — enough for parallel table fetches.
    pub async fn new(db_config: &DbConfig) -> Result<Self> {
        let conn_str = db_config.connection_string();
        info!("Connecting to: {}", redact_password(&conn_str));

        let mut cfg = PoolConfig::new();
        cfg.url = Some(conn_str.clone());
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: 4,
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .with_context(|| format!("Failed to create pool for: {}", redact_password(&conn_str)))?;

        // Eagerly verify connectivity
        pool.get().await
            .with_context(|| format!("Cannot connect to: {}", redact_password(&conn_str)))?;

        Ok(Self(pool))
    }

    pub fn inner(&self) -> &Pool {
        &self.0
    }
}

/// Redact password in connection strings for log output
fn redact_password(conn_str: &str) -> String {
    // postgresql://user:PASS@host/db  →  postgresql://user:***@host/db
    if let Some(at_pos) = conn_str.find('@') {
        if let Some(colon_pos) = conn_str[..at_pos].rfind(':') {
            let prefix = &conn_str[..=colon_pos];
            let suffix = &conn_str[at_pos..];
            return format!("{}***{}", prefix, suffix);
        }
    }
    conn_str.to_string()
}
