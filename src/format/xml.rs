use crate::db::DbRow;

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn render(rows: &[DbRow], cols: &[&str], table_name: &str) -> String {
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<results>\n");
    for row in rows {
        out.push_str(&format!("  <{}>\n", table_name));
        for &col in cols {
            let val = escape(&row.get(col).as_display());
            out.push_str(&format!("    <{col}>{val}</{col}>\n"));
        }
        out.push_str(&format!("  </{}>\n", table_name));
    }
    out.push_str("</results>\n");
    out
}
