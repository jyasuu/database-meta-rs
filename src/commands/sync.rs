use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use tracing::info;

use crate::config::{self, ColumnConfig};
use crate::db::row::values_equal;
use crate::db::{fetch_rows, DbPool, DbRow, Value};
use crate::error::DbToolsError;

// ─── Entry point ─────────────────────────────────────────────────────────────

pub async fn run(config_path: &str, out_path: &str, dry_run: bool) -> Result<()> {
    let cfg = config::load(config_path)?;

    let target_cfg = cfg
        .databases
        .target
        .as_ref()
        .ok_or(DbToolsError::NoTargetDb)?;

    let source_pool = DbPool::new(&cfg.databases.source).await?;
    let target_pool = DbPool::new(target_cfg).await?;

    let mut all_sql = String::new();

    let pb = ProgressBar::new(cfg.tables.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
        )?
        .progress_chars("=>-"),
    );

    for table in &cfg.tables {
        pb.set_message(table.name.clone());

        let primary_keys = table.required_primary_keys()?;
        let qualified = table.qualified_name();

        let source_rows =
            fetch_rows(source_pool.inner(), &table.name, &qualified, &table.columns).await?;
        let target_rows =
            fetch_rows(target_pool.inner(), &table.name, &qualified, &table.columns).await?;

        let DiffResult {
            inserts,
            updates,
            deletes,
        } = diff_tables(
            &table.name,
            &source_rows,
            &target_rows,
            &table.columns,
            primary_keys,
        );

        info!(
            table = %table.name,
            inserts = inserts.len(),
            updates = updates.len(),
            deletes = deletes.len(),
            "diff complete"
        );

        for sql in inserts.iter().chain(updates.iter()).chain(deletes.iter()) {
            all_sql.push_str(sql);
            all_sql.push_str(";\n");
        }

        pb.inc(1);
    }

    pb.finish_with_message("done");

    if dry_run {
        println!("{}", all_sql);
    } else {
        tokio::fs::write(out_path, &all_sql).await?;
        println!("SQL written to: {}", out_path);
    }

    Ok(())
}

// ─── Diff logic ──────────────────────────────────────────────────────────────

struct DiffResult {
    inserts: Vec<String>,
    updates: Vec<String>,
    deletes: Vec<String>,
}

/// Pure function: compare source vs target rows and emit SQL strings.
///
/// Semantics (identical to Java original):
///   row in target but not source  → INSERT  (bring into source)
///   row in both, tracked cols differ → UPDATE  (update source to match target)
///   row in source but not target  → DELETE  (remove from source)
fn diff_tables(
    table_name: &str,
    source_rows: &[DbRow],
    target_rows: &[DbRow],
    columns: &[ColumnConfig],
    primary_keys: &[String],
) -> DiffResult {
    let source_map: HashMap<String, &DbRow> = source_rows
        .iter()
        .map(|r| (r.pk_key(primary_keys), r))
        .collect();
    let target_map: HashMap<String, &DbRow> = target_rows
        .iter()
        .map(|r| (r.pk_key(primary_keys), r))
        .collect();

    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    let mut deletes = Vec::new();

    // INSERTs and UPDATEs
    for target_row in target_rows {
        let key = target_row.pk_key(primary_keys);
        match source_map.get(&key) {
            None => inserts.push(build_insert(table_name, target_row)),
            Some(source_row) => {
                let changed: Vec<(&str, &Value)> = columns
                    .iter()
                    .filter(|c| c.is_tracked())
                    .filter_map(|c| {
                        let src = source_row.get(&c.column_name);
                        let tgt = target_row.get(&c.column_name);
                        if !values_equal(src, tgt) {
                            Some((c.column_name.as_str(), tgt))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !changed.is_empty() {
                    updates.push(build_update(table_name, primary_keys, target_row, &changed));
                }
            }
        }
    }

    // DELETEs
    for source_row in source_rows {
        let key = source_row.pk_key(primary_keys);
        if !target_map.contains_key(&key) {
            deletes.push(build_delete(table_name, primary_keys, source_row));
        }
    }

    DiffResult {
        inserts,
        updates,
        deletes,
    }
}

// ─── SQL builders ─────────────────────────────────────────────────────────────

fn build_insert(table_name: &str, row: &DbRow) -> String {
    let cols: Vec<String> = row.columns.iter().map(|c| format!("\"{}\"", c)).collect();
    let vals: Vec<String> = row
        .columns
        .iter()
        .map(|c| row.get(c).to_sql_literal())
        .collect();
    format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        table_name,
        cols.join(", "),
        vals.join(", ")
    )
}

fn build_update(
    table_name: &str,
    primary_keys: &[String],
    row: &DbRow,
    changed: &[(&str, &Value)],
) -> String {
    let set: Vec<String> = changed
        .iter()
        .map(|(col, val)| format!("\"{}\" = {}", col, val.to_sql_literal()))
        .collect();
    let where_clause = build_where(primary_keys, row);
    format!(
        "UPDATE \"{}\" SET {} WHERE {}",
        table_name,
        set.join(", "),
        where_clause
    )
}

fn build_delete(table_name: &str, primary_keys: &[String], row: &DbRow) -> String {
    format!(
        "DELETE FROM \"{}\" WHERE {}",
        table_name,
        build_where(primary_keys, row)
    )
}

fn build_where(primary_keys: &[String], row: &DbRow) -> String {
    primary_keys
        .iter()
        .map(|k| format!("\"{}\" = {}", k, row.get(k).to_sql_literal()))
        .collect::<Vec<_>>()
        .join(" AND ")
}
