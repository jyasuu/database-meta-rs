use anyhow::Result;
use crate::db::DbRow;

pub fn render(rows: &[DbRow], cols: &[&str]) -> Result<String> {
    // Reuse JSON serialization then convert to YAML — avoids duplicating
    // all the type-mapping logic.
    let json_list: Vec<serde_json::Value> = rows.iter().map(|row| {
        let obj: serde_json::Map<String, serde_json::Value> = cols.iter()
            .map(|&col| (col.to_string(), row.get(col).to_json()))
            .collect();
        serde_json::Value::Object(obj)
    }).collect();

    Ok(serde_yaml::to_string(&json_list)?)
}
