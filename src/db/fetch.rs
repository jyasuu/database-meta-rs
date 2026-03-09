use std::collections::HashMap;
use anyhow::Result;
use deadpool_postgres::Pool;
use tokio_postgres::types::Type;
use tracing::debug;

use crate::config::ColumnConfig;
use crate::db::row::{DbRow, Value};
use crate::error::DbToolsError;

// ─── Value reader ─────────────────────────────────────────────────────────────

/// Read one cell from a tokio_postgres Row by column index.
/// Handles all common PostgreSQL types; falls back to text.
fn read_value(row: &tokio_postgres::Row, idx: usize) -> Value {
    let col  = &row.columns()[idx];
    let type_ = col.type_();

    macro_rules! try_get {
        ($T:ty, $variant:expr) => {{
            let v: Option<$T> = row.try_get(idx).unwrap_or(None);
            v.map($variant).unwrap_or(Value::Null)
        }};
    }

    match *type_ {
        Type::BOOL    => try_get!(bool,                   Value::Bool),
        Type::INT2    => try_get!(i16,  |x| Value::Int(x as i64)),
        Type::INT4    => try_get!(i32,  |x| Value::Int(x as i64)),
        Type::INT8    => try_get!(i64,                    Value::Int),
        Type::FLOAT4  => try_get!(f32,  |x| Value::Float(x as f64)),
        Type::FLOAT8  => try_get!(f64,                    Value::Float),
        Type::BYTEA   => try_get!(Vec<u8>,                Value::Bytes),
        Type::JSON | Type::JSONB => try_get!(serde_json::Value, Value::Json),
        Type::UUID    => try_get!(uuid::Uuid,             Value::Uuid),
        Type::DATE    => try_get!(chrono::NaiveDate,      Value::Date),
        Type::TIMESTAMP  => try_get!(chrono::NaiveDateTime,   Value::Timestamp),
        Type::TIMESTAMPTZ => try_get!(chrono::DateTime<chrono::Utc>, Value::TimestampTz),
        _ => {
            // Default: treat as text (covers VARCHAR, TEXT, NUMERIC, ENUM, …)
            let v: Option<String> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Text).unwrap_or(Value::Null)
        }
    }
}

// ─── Public fetch functions ───────────────────────────────────────────────────

/// Fetch all rows from `table_name`, reading every column from the DB.
pub async fn fetch_rows(
    pool: &Pool,
    table_name: &str,
    qualified_name: &str,
    columns: &[ColumnConfig],
) -> Result<Vec<DbRow>> {
    let client = pool.get().await
        .map_err(|e| DbToolsError::Config(format!("Pool error: {}", e)))?;

    let col_list: Vec<String> = columns.iter()
        .map(|c| format!("\"{}\"", c.column_name))
        .collect();

    let query = format!("SELECT {} FROM {}", col_list.join(", "), qualified_name);
    debug!("fetch_rows: {}", query);

    let rows = client.query(&query, &[]).await
        .map_err(|e| DbToolsError::Query { table: table_name.to_string(), source: e })?;

    let col_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();
    Ok(build_rows(&rows, &col_names))
}

/// Fetch rows, substituting non-tracked columns with their configured defaults.
/// Non-tracked columns are projected as SQL literals, so we still get one unified
/// result set with consistent column order.
pub async fn fetch_rows_with_defaults(
    pool: &Pool,
    table_name: &str,
    qualified_name: &str,
    columns: &[ColumnConfig],
) -> Result<Vec<DbRow>> {
    let client = pool.get().await
        .map_err(|e| DbToolsError::Config(format!("Pool error: {}", e)))?;

    let select_parts: Vec<String> = columns.iter().map(|col| {
        if col.is_tracked() {
            format!("\"{}\"", col.column_name)
        } else {
            // Emit a literal default value cast to text
            use crate::config::ColumnType;
            let default_expr = match col.col_type {
                ColumnType::Numeric => {
                    let n = col.default.as_deref().unwrap_or("0");
                    n.to_string()   // raw numeric literal
                }
                _ => {
                    let s = col.default.as_deref().unwrap_or("");
                    format!("'{}'", s.replace('\'', "''"))
                }
            };
            format!("{} AS \"{}\"", default_expr, col.column_name)
        }
    }).collect();

    let query = format!("SELECT {} FROM {}", select_parts.join(", "), qualified_name);
    debug!("fetch_rows_with_defaults: {}", query);

    let rows = client.query(&query, &[]).await
        .map_err(|e| DbToolsError::Query { table: table_name.to_string(), source: e })?;

    let col_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();
    Ok(build_rows(&rows, &col_names))
}

fn build_rows(rows: &[tokio_postgres::Row], col_names: &[String]) -> Vec<DbRow> {
    rows.iter().map(|row| {
        let mut data = HashMap::with_capacity(col_names.len());
        for (idx, col) in col_names.iter().enumerate() {
            data.insert(col.clone(), read_value(row, idx));
        }
        DbRow::new(col_names.to_vec(), data)
    }).collect()
}
