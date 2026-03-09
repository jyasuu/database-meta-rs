use std::collections::HashMap;
use anyhow::{Result, Context};
use postgres::{Client, NoTls, Row};

use crate::config::{ColumnConfig, DbConfig};

/// A generic cell value that can hold different PostgreSQL types
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl Value {
    /// Try to extract as a string representation for SQL generation
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => {
                // Escape single quotes
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
            Value::Bytes(b) => format!("'\\x{}'", hex::encode(b)),
            Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        }
    }

    /// Returns a comparable form (for sorting)
    pub fn partial_cmp_with(&self, other: &Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Display value for CSV/text output
    pub fn display(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bytes(b) => hex::encode(b),
            Value::Json(j) => j.to_string(),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display())
    }
}

/// A single table row as an ordered list of (column_name, value) pairs
#[derive(Debug, Clone)]
pub struct DbRow {
    pub columns: Vec<String>,
    pub data: HashMap<String, Value>,
}

impl DbRow {
    pub fn get(&self, col: &str) -> &Value {
        self.data.get(col).unwrap_or(&Value::Null)
    }
}

/// Open a connection to a PostgreSQL database
pub fn connect(db_config: &DbConfig) -> Result<Client> {
    let conn_str = db_config.to_pg_connection_string();
    Client::connect(&conn_str, NoTls)
        .with_context(|| format!("Failed to connect to database: {}", db_config.jdbc_url))
}

/// Read a value from a postgres Row by column index and type hint
fn read_value(row: &Row, idx: usize) -> Value {
    use postgres::types::Type;

    let col = &row.columns()[idx];
    let type_ = col.type_();

    // Try common types. postgres crate uses Option<T> for nullable columns.
    match *type_ {
        Type::BOOL => {
            let v: Option<bool> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Bool).unwrap_or(Value::Null)
        }
        Type::INT2 => {
            let v: Option<i16> = row.try_get(idx).unwrap_or(None);
            v.map(|x| Value::Int(x as i64)).unwrap_or(Value::Null)
        }
        Type::INT4 => {
            let v: Option<i32> = row.try_get(idx).unwrap_or(None);
            v.map(|x| Value::Int(x as i64)).unwrap_or(Value::Null)
        }
        Type::INT8 => {
            let v: Option<i64> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Int).unwrap_or(Value::Null)
        }
        Type::FLOAT4 => {
            let v: Option<f32> = row.try_get(idx).unwrap_or(None);
            v.map(|x| Value::Float(x as f64)).unwrap_or(Value::Null)
        }
        Type::FLOAT8 => {
            let v: Option<f64> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Float).unwrap_or(Value::Null)
        }
        Type::BYTEA => {
            let v: Option<Vec<u8>> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Bytes).unwrap_or(Value::Null)
        }
        Type::JSON | Type::JSONB => {
            let v: Option<serde_json::Value> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Json).unwrap_or(Value::Null)
        }
        _ => {
            // Default: treat as text
            let v: Option<String> = row.try_get(idx).unwrap_or(None);
            v.map(Value::Text).unwrap_or(Value::Null)
        }
    }
}

/// Fetch rows from a table, selecting only the specified columns
pub fn fetch_table_data(
    client: &mut Client,
    table_name: &str,
    columns: &[ColumnConfig],
) -> Result<Vec<DbRow>> {
    let col_names: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"", c.column_name))
        .collect();
    let query = format!(
        "SELECT {} FROM \"{}\"",
        col_names.join(", "),
        table_name
    );

    let rows = client.query(&query, &[])
        .with_context(|| format!("Failed to query table: {}", table_name))?;

    let result = rows
        .iter()
        .map(|row| {
            let mut data = HashMap::new();
            let col_list: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();
            for (idx, col) in col_list.iter().enumerate() {
                data.insert(col.clone(), read_value(row, idx));
            }
            DbRow {
                columns: col_list,
                data,
            }
        })
        .collect();

    Ok(result)
}

/// Fetch rows with optional ordering, respecting tracked/untracked columns
/// (for database-meta: non-tracked columns replaced by their default value)
pub fn fetch_table_data_with_defaults(
    client: &mut Client,
    table_name: &str,
    columns: &[ColumnConfig],
) -> Result<Vec<DbRow>> {
    // Build SELECT expressions:
    //   tracked columns -> actual column
    //   non-tracked columns -> literal default value AS column_name
    let mut select_parts: Vec<String> = Vec::new();

    for col in columns {
        if col.is_tracked() {
            select_parts.push(format!("\"{}\"", col.column_name));
        } else {
            let default_expr = match col.col_type.as_deref() {
                Some("numeric") => {
                    let n = col.default.as_deref().unwrap_or("0");
                    format!("{} AS \"{}\"", n, col.column_name)
                }
                _ => {
                    let s = col.default.as_deref().unwrap_or("");
                    format!("'{}' AS \"{}\"", s.replace('\'', "''"), col.column_name)
                }
            };
            select_parts.push(default_expr);
            continue;
        }
    }

    let query = format!(
        "SELECT {} FROM \"{}\"",
        select_parts.join(", "),
        table_name
    );

    let rows = client.query(&query, &[])
        .with_context(|| format!("Failed to query table: {}", table_name))?;

    let col_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();

    let result = rows
        .iter()
        .map(|row| {
            let mut data = HashMap::new();
            for (idx, col_name) in col_names.iter().enumerate() {
                data.insert(col_name.clone(), read_value(row, idx));
            }
            DbRow {
                columns: col_names.clone(),
                data,
            }
        })
        .collect();

    Ok(result)
}
