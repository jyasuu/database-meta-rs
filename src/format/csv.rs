use crate::db::DbRow;

pub fn render(rows: &[DbRow], cols: &[&str]) -> String {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record(cols).expect("write header");
    for row in rows {
        let record: Vec<String> = cols.iter().map(|&c| row.get(c).as_display()).collect();
        wtr.write_record(&record).expect("write row");
    }
    String::from_utf8(wtr.into_inner().expect("flush csv")).expect("utf8")
}
