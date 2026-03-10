use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json;
use std::cmp::Ordering;
use std::collections::HashMap;
use uuid::Uuid;

// ─── Value ────────────────────────────────────────────────────────────────────

/// Typed cell value covering the most common PostgreSQL types.
/// Derives PartialEq so value comparison in sync is straightforward.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Uuid(Uuid),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Produce a valid SQL literal (for DML generation).
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string().to_uppercase(), // TRUE / FALSE
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => sql_quote(s),
            Value::Bytes(b) => format!("'\\x{}'", hex::encode(b)),
            Value::Json(j) => sql_quote(&j.to_string()),
            Value::Uuid(u) => sql_quote(&u.to_string()),
            Value::Date(d) => sql_quote(&d.format("%Y-%m-%d").to_string()),
            Value::Timestamp(ts) => sql_quote(&ts.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
            Value::TimestampTz(ts) => sql_quote(&ts.format("%Y-%m-%d %H:%M:%S%.f%z").to_string()),
        }
    }

    /// Human-readable display (used for CSV, XML, HTML, YAML, JSON output)
    pub fn as_display(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bytes(b) => hex::encode(b),
            Value::Json(j) => j.to_string(),
            Value::Uuid(u) => u.to_string(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::TimestampTz(ts) => ts.format("%Y-%m-%d %H:%M:%S%z").to_string(),
        }
    }

    /// Convert to serde_json::Value for JSON/YAML output
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::json!(f),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => serde_json::Value::String(hex::encode(b)),
            Value::Json(j) => j.clone(),
            Value::Uuid(u) => serde_json::Value::String(u.to_string()),
            Value::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
            Value::Timestamp(ts) => {
                serde_json::Value::String(ts.format("%Y-%m-%dT%H:%M:%S").to_string())
            }
            Value::TimestampTz(ts) => serde_json::Value::String(ts.to_rfc3339()),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_display())
    }
}

/// Implement PartialOrd so sort comparisons are idiomatic
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::TimestampTz(a), Value::TimestampTz(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// Compare values for sync equality.
/// Strips \r from strings to match the Java behaviour.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Text(s1), Value::Text(s2)) => s1.replace('\r', "") == s2.replace('\r', ""),
        _ => a == b,
    }
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

// ─── DbRow ────────────────────────────────────────────────────────────────────

/// An ordered set of (column → value) pairs for one DB row.
/// `columns` preserves insertion order; `data` allows O(1) lookup.
#[derive(Debug, Clone)]
pub struct DbRow {
    /// Column names in SELECT order
    pub columns: Vec<String>,
    pub data: HashMap<String, Value>,
}

impl DbRow {
    pub fn new(columns: Vec<String>, data: HashMap<String, Value>) -> Self {
        Self { columns, data }
    }

    /// Get a value by column name, returning Null for missing columns
    pub fn get(&self, col: &str) -> &Value {
        self.data.get(col).unwrap_or(&Value::Null)
    }

    /// Build a composite primary-key string for use as a HashMap key.
    /// Values are joined with '\0' to avoid collisions (e.g. "1|2" vs "12|").
    pub fn pk_key(&self, primary_keys: &[String]) -> String {
        primary_keys
            .iter()
            .map(|k| self.get(k).as_display())
            .collect::<Vec<_>>()
            .join("\x00")
    }
}
