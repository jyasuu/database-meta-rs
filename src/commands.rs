use std::collections::HashMap;
use std::fs;
use std::path::Path;
use anyhow::{Result, Context};

use crate::config::{load_config, ColumnConfig};
use crate::db::{connect, fetch_table_data, fetch_table_data_with_defaults, DbRow, Value};

// ─── database-meta ────────────────────────────────────────────────────────────

pub fn database_meta(format: &str, config_path: &str, out_dir: &str) -> Result<()> {
    let config = load_config(config_path)?;

    // Ensure output directory exists
    fs::create_dir_all(out_dir)
        .with_context(|| format!("Cannot create output directory: {}", out_dir))?;

    let mut source_client = connect(&config.databases.source)?;

    for table_config in &config.tables {
        let table_name = &table_config.name;
        let order_columns = table_config.order_columns();

        // Fetch rows, replacing non-tracked columns with defaults
        let mut rows = fetch_table_data_with_defaults(
            &mut source_client,
            table_name,
            &table_config.columns,
        )?;

        // Sort in Rust (mirrors the Java Collections.sort approach)
        if !order_columns.is_empty() {
            rows.sort_by(|r1, r2| {
                for col in &order_columns {
                    let v1 = r1.get(col);
                    let v2 = r2.get(col);
                    match (v1.is_null(), v2.is_null()) {
                        (true, true) => continue,
                        (true, false) => return std::cmp::Ordering::Less,
                        (false, true) => return std::cmp::Ordering::Greater,
                        (false, false) => {
                            let ord = v1.partial_cmp_with(v2);
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let col_names: Vec<&str> = table_config
            .columns
            .iter()
            .map(|c| c.column_name.as_str())
            .collect();

        let (file_content, ext) = match format {
            "csv" => (format_csv(&rows, &col_names), ".csv"),
            "xml" => (format_xml(&rows, &col_names, table_name), ".xml"),
            "html" => (format_html(&rows, &col_names), ".html"),
            "insert" => (format_insert(&rows, &col_names, table_name), ".sql"),
            "yaml" => (format_yaml(&rows, &col_names)?, ".yaml"),
            _ => (format_json(&rows, &col_names)?, ".json"),
        };

        let out_path = Path::new(out_dir).join(format!("{}{}", table_name, ext));
        fs::write(&out_path, &file_content)
            .with_context(|| format!("Failed to write file: {}", out_path.display()))?;

        println!("Written: {}", out_path.display());
    }

    Ok(())
}

// ─── compare-tables ───────────────────────────────────────────────────────────

pub fn compare_tables(config_path: &str) -> Result<String> {
    let config = load_config(config_path)?;

    let target_config = config
        .databases
        .target
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No target database configured"))?;

    let mut source_client = connect(&config.databases.source)?;
    let mut target_client = connect(target_config)?;

    let mut all_sql = String::new();

    for table_config in &config.tables {
        let table_name = &table_config.name;
        let primary_keys = table_config
            .primary_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Table '{}' has no primary_key defined", table_name))?;

        let source_rows = fetch_table_data(&mut source_client, table_name, &table_config.columns)?;
        let target_rows = fetch_table_data(&mut target_client, table_name, &table_config.columns)?;

        let (inserts, updates, deletes) = compare_and_generate_sql(
            table_name,
            &source_rows,
            &target_rows,
            &table_config.columns,
            primary_keys,
        );

        for sql in &inserts {
            all_sql.push_str(sql);
            all_sql.push_str(";\n");
        }
        for sql in &updates {
            all_sql.push_str(sql);
            all_sql.push_str(";\n");
        }
        for sql in &deletes {
            all_sql.push_str(sql);
            all_sql.push_str(";\n");
        }
    }

    fs::write("dml.sql", &all_sql)
        .context("Failed to write dml.sql")?;

    Ok("Comparison completed and SQL generated.".to_string())
}

/// Compare source and target rows, generating INSERT/UPDATE/DELETE SQL strings.
/// Logic mirrors the Java compareAndGenerateSQL method:
///   - Row in target but NOT in source  → INSERT into target
///   - Row in both but differs          → UPDATE target
///   - Row in source but NOT in target  → DELETE from target
fn compare_and_generate_sql(
    table_name: &str,
    source_rows: &[DbRow],
    target_rows: &[DbRow],
    columns: &[ColumnConfig],
    primary_keys: &[String],
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    let mut deletes = Vec::new();

    // Build lookup maps keyed by primary key composite string
    let source_map: HashMap<String, &DbRow> = source_rows
        .iter()
        .map(|r| (pk_key(primary_keys, r), r))
        .collect();

    let target_map: HashMap<String, &DbRow> = target_rows
        .iter()
        .map(|r| (pk_key(primary_keys, r), r))
        .collect();

    // INSERT / UPDATE
    for target_row in target_rows {
        let key = pk_key(primary_keys, target_row);

        match source_map.get(&key) {
            None => {
                // In target but not source → INSERT
                inserts.push(build_insert(table_name, target_row));
            }
            Some(source_row) => {
                // Both exist → check tracked columns for differences
                let mut changed_cols: Vec<(&str, &Value)> = Vec::new();

                for col in columns {
                    if !col.is_tracked() {
                        continue;
                    }
                    let src_val = source_row.get(&col.column_name);
                    let tgt_val = target_row.get(&col.column_name);

                    if !values_equal(src_val, tgt_val) {
                        changed_cols.push((&col.column_name, tgt_val));
                    }
                }

                if !changed_cols.is_empty() {
                    updates.push(build_update(table_name, primary_keys, target_row, &changed_cols));
                }
            }
        }
    }

    // DELETE
    for source_row in source_rows {
        let key = pk_key(primary_keys, source_row);
        if !target_map.contains_key(&key) {
            deletes.push(build_delete(table_name, primary_keys, source_row));
        }
    }

    (inserts, updates, deletes)
}

/// Compare two values; for strings, strip \r before comparing (mirrors Java logic)
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Text(s1), Value::Text(s2)) => {
            let s1 = s1.replace('\r', "");
            let s2 = s2.replace('\r', "");
            s1 == s2
        }
        _ => a == b,
    }
}

/// Build a composite primary key string for use as a HashMap key
fn pk_key(primary_keys: &[String], row: &DbRow) -> String {
    primary_keys
        .iter()
        .map(|k| row.get(k).display())
        .collect::<Vec<_>>()
        .join("|")
}

fn build_where_clause(primary_keys: &[String], row: &DbRow) -> String {
    primary_keys
        .iter()
        .map(|k| format!("\"{}\" = {}", k, row.get(k).to_sql_literal()))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn build_insert(table_name: &str, row: &DbRow) -> String {
    let cols: Vec<String> = row.columns.iter().map(|c| format!("\"{}\"", c)).collect();
    let vals: Vec<String> = row.columns.iter().map(|c| row.get(c).to_sql_literal()).collect();
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
    let set_clause: Vec<String> = changed
        .iter()
        .map(|(col, val)| format!("\"{}\" = {}", col, val.to_sql_literal()))
        .collect();
    let where_clause = build_where_clause(primary_keys, row);
    format!(
        "UPDATE \"{}\" SET {} WHERE {}",
        table_name,
        set_clause.join(", "),
        where_clause
    )
}

fn build_delete(table_name: &str, primary_keys: &[String], row: &DbRow) -> String {
    let where_clause = build_where_clause(primary_keys, row);
    format!("DELETE FROM \"{}\" WHERE {}", table_name, where_clause)
}

// ─── Formatters ───────────────────────────────────────────────────────────────

fn format_csv(rows: &[DbRow], cols: &[&str]) -> String {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record(cols).unwrap();
    for row in rows {
        let record: Vec<String> = cols.iter().map(|c| row.get(c).display()).collect();
        wtr.write_record(&record).unwrap();
    }
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
}

fn format_json(rows: &[DbRow], cols: &[&str]) -> Result<String> {
    let list: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for col in cols {
                let v = match row.get(col) {
                    Value::Null => serde_json::Value::Null,
                    Value::Bool(b) => serde_json::Value::Bool(*b),
                    Value::Int(i) => serde_json::Value::Number((*i).into()),
                    Value::Float(f) => serde_json::json!(f),
                    Value::Text(s) => serde_json::Value::String(s.clone()),
                    Value::Bytes(b) => serde_json::Value::String(hex::encode(b)),
                    Value::Json(j) => j.clone(),
                };
                map.insert(col.to_string(), v);
            }
            serde_json::Value::Object(map)
        })
        .collect();
    Ok(serde_json::to_string_pretty(&list)?)
}

fn format_yaml(rows: &[DbRow], cols: &[&str]) -> Result<String> {
    let list: Vec<serde_yaml::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_yaml::Mapping::new();
            for col in cols {
                let v = match row.get(col) {
                    Value::Null => serde_yaml::Value::Null,
                    Value::Bool(b) => serde_yaml::Value::Bool(*b),
                    Value::Int(i) => serde_yaml::Value::Number((*i).into()),
                    Value::Float(f) => serde_yaml::Value::Number(
                        serde_yaml::Number::from(*f as f64)
                    ),
                    Value::Text(s) => serde_yaml::Value::String(s.clone()),
                    Value::Bytes(b) => serde_yaml::Value::String(hex::encode(b)),
                    Value::Json(j) => serde_yaml::Value::String(j.to_string()),
                };
                map.insert(serde_yaml::Value::String(col.to_string()), v);
            }
            serde_yaml::Value::Mapping(map)
        })
        .collect();
    Ok(serde_yaml::to_string(&list)?)
}

fn format_xml(rows: &[DbRow], cols: &[&str], table_name: &str) -> String {
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<result>\n");
    for row in rows {
        out.push_str(&format!("  <{}>\n", table_name));
        for col in cols {
            let val = xml_escape(&row.get(col).display());
            out.push_str(&format!("    <{}>{}</{}>\n", col, val, col));
        }
        out.push_str(&format!("  </{}>\n", table_name));
    }
    out.push_str("</result>\n");
    out
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn format_html(rows: &[DbRow], cols: &[&str]) -> String {
    let mut out = String::from("<table>\n  <thead>\n    <tr>");
    for col in cols {
        out.push_str(&format!("<th>{}</th>", col));
    }
    out.push_str("</tr>\n  </thead>\n  <tbody>\n");
    for row in rows {
        out.push_str("    <tr>");
        for col in cols {
            out.push_str(&format!("<td>{}</td>", xml_escape(&row.get(col).display())));
        }
        out.push_str("</tr>\n");
    }
    out.push_str("  </tbody>\n</table>\n");
    out
}

fn format_insert(rows: &[DbRow], cols: &[&str], table_name: &str) -> String {
    rows.iter()
        .map(|row| build_insert(table_name, row))
        .map(|s| format!("{};\n", s))
        .collect()
}
