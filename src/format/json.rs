use crate::db::DbRow;
use anyhow::Result;

pub fn render(rows: &[DbRow], cols: &[&str]) -> Result<String> {
    let list: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = cols
                .iter()
                .map(|&col| (col.to_string(), row.get(col).to_json()))
                .collect();
            serde_json::Value::Object(obj)
        })
        .collect();

    Ok(serde_json::to_string_pretty(&list)?)
}
