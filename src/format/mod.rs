mod csv;
mod html;
mod insert;
mod json;
mod xml;
mod yaml;

use anyhow::Result;
use clap::ValueEnum;

use crate::db::DbRow;

// ─── Format enum (used by clap) ───────────────────────────────────────────────

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Json,
    Csv,
    Xml,
    Html,
    Yaml,
    /// SQL INSERT statements
    Insert,
}

impl OutputFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            OutputFormat::Json   => ".json",
            OutputFormat::Csv    => ".csv",
            OutputFormat::Xml    => ".xml",
            OutputFormat::Html   => ".html",
            OutputFormat::Yaml   => ".yaml",
            OutputFormat::Insert => ".sql",
        }
    }
}

// ─── Dispatcher ───────────────────────────────────────────────────────────────

pub fn render(
    format: &OutputFormat,
    rows: &[DbRow],
    cols: &[&str],
    table_name: &str,
) -> Result<String> {
    match format {
        OutputFormat::Json   => json::render(rows, cols),
        OutputFormat::Csv    => Ok(csv::render(rows, cols)),
        OutputFormat::Xml    => Ok(xml::render(rows, cols, table_name)),
        OutputFormat::Html   => Ok(html::render(rows, cols)),
        OutputFormat::Yaml   => yaml::render(rows, cols),
        OutputFormat::Insert => Ok(insert::render(rows, table_name)),
    }
}
