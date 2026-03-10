use crate::db::DbRow;

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn render(rows: &[DbRow], cols: &[&str]) -> String {
    let mut out = String::from("<table>\n  <thead>\n    <tr>");
    for &col in cols {
        out.push_str(&format!("<th>{}</th>", escape(col)));
    }
    out.push_str("</tr>\n  </thead>\n  <tbody>\n");
    for row in rows {
        out.push_str("    <tr>");
        for &col in cols {
            out.push_str(&format!("<td>{}</td>", escape(&row.get(col).as_display())));
        }
        out.push_str("</tr>\n");
    }
    out.push_str("  </tbody>\n</table>\n");
    out
}
