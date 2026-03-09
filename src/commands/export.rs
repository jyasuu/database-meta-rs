use std::path::Path;
use anyhow::Result;
use tokio::fs;
use tracing::info;

use crate::config;
use crate::db::{DbPool, fetch_rows_with_defaults};
use crate::format::{self, OutputFormat};

pub async fn run(config_path: &str, format: OutputFormat, out_dir: &str) -> Result<()> {
    let cfg = config::load(config_path)?;

    fs::create_dir_all(out_dir).await?;

    let pool = DbPool::new(&cfg.databases.source).await?;

    for table in &cfg.tables {
        let order_cols = table.order_columns();
        let qualified  = table.qualified_name();

        let mut rows = fetch_rows_with_defaults(
            pool.inner(),
            &table.name,
            &qualified,
            &table.columns,
        ).await?;

        // Sort rows in Rust after fetch (ORDER BY is pushed post-select
        // so defaults don't interfere with the DB planner)
        if !order_cols.is_empty() {
            rows.sort_by(|a, b| {
                for col in &order_cols {
                    let va = a.get(col);
                    let vb = b.get(col);
                    let ord = va.partial_cmp(vb)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let col_names: Vec<&str> = table.columns.iter()
            .map(|c| c.column_name.as_str())
            .collect();

        let content = format::render(&format, &rows, &col_names, &table.name)?;

        let path = Path::new(out_dir)
            .join(format!("{}{}", table.name, format.extension()));

        fs::write(&path, content).await?;
        info!("Written: {}", path.display());
    }

    Ok(())
}
