use crate::db::DbRow;

pub fn render(rows: &[DbRow], table_name: &str) -> String {
    rows.iter().map(|row| {
        let cols: Vec<String> = row.columns.iter()
            .map(|c| format!("\"{}\"", c))
            .collect();
        let vals: Vec<String> = row.columns.iter()
            .map(|c| row.get(c).to_sql_literal())
            .collect();
        format!(
            "INSERT INTO \"{}\" ({}) VALUES ({});\n",
            table_name,
            cols.join(", "),
            vals.join(", "),
        )
    }).collect()
}
