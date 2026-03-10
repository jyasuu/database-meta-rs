//! Upgrade a v1 config to v2, with optional diff output.

use crate::config::{
    ColumnConfig, ColumnType, Config, Databases, DbConfig, TableConfig, TrackMode,
};
use crate::error::DbToolsError;
use crate::migrate::v1::{V1ColumnConfig, V1Config, V1DbConfig};
use anyhow::Result;

// ─── Entry point ─────────────────────────────────────────────────────────────

pub fn run(input_path: &str, output_path: Option<&str>, show_diff: bool) -> Result<()> {
    // 1. Load v1
    let v1 = crate::migrate::v1::load(input_path)?;

    // 2. Collect warnings
    let mut warnings: Vec<String> = Vec::new();

    // 3. Convert
    let v2 = convert_config(&v1, &mut warnings)?;

    // 4. Serialize v2 to YAML string
    let v2_yaml = serde_yaml::to_string(&v2).map_err(|e| DbToolsError::Serialize(e.to_string()))?;

    // 5. Show diff if requested
    if show_diff {
        let v1_yaml = std::fs::read_to_string(input_path)?;
        print_diff(&v1_yaml, &v2_yaml);
    }

    // 6. Print warnings
    if !warnings.is_empty() {
        eprintln!("\n⚠️  Migration warnings:");
        for w in &warnings {
            eprintln!("   • {}", w);
        }
        eprintln!();
    }

    // 7. Write output
    let out = output_path
        .map(|s| s.to_string())
        .unwrap_or_else(|| derive_output_path(input_path));

    std::fs::write(&out, &v2_yaml).map_err(|e| DbToolsError::Io {
        path: out.clone(),
        source: e,
    })?;

    println!("✓ Migrated config written to: {}", out);
    Ok(())
}

// ─── Conversion ──────────────────────────────────────────────────────────────

fn convert_config(v1: &V1Config, warnings: &mut Vec<String>) -> Result<Config> {
    let source = convert_db_config(&v1.databases.source);
    let target = v1.databases.target.as_ref().map(convert_db_config);

    let tables = v1
        .tables
        .iter()
        .map(|t| convert_table(t, warnings))
        .collect::<Result<Vec<_>>>()?;

    Ok(Config {
        databases: Databases { source, target },
        tables,
    })
}

fn convert_db_config(v1: &V1DbConfig) -> DbConfig {
    // Strip jdbc: prefix and embed credentials into the URL
    let base = v1.jdbc_url.strip_prefix("jdbc:").unwrap_or(&v1.jdbc_url);

    let url = if let Some(rest) = base.strip_prefix("postgresql://") {
        // Only embed if credentials not already present
        if !rest.contains('@') {
            format!("postgresql://{}:{}@{}", v1.username, v1.password, rest)
        } else {
            base.to_string()
        }
    } else {
        base.to_string()
    };

    DbConfig {
        url,
        // We embed creds in the URL above, so these become redundant.
        // Keep them as None to avoid duplication.
        username: None,
        password: None,
    }
}

fn convert_table(
    v1: &crate::migrate::v1::V1TableConfig,
    warnings: &mut Vec<String>,
) -> Result<TableConfig> {
    if v1.primary_key.is_none() {
        warnings.push(format!(
            "Table '{}' has no primary_key. You must add one before using the 'sync' command.",
            v1.name
        ));
    }

    let columns = v1
        .columns
        .iter()
        .map(|c| convert_column(c, &v1.name, warnings))
        .collect::<Result<Vec<_>>>()?;

    Ok(TableConfig {
        name: v1.name.clone(),
        schema: None, // v1 had no schema concept
        order: v1.order.clone(),
        primary_key: v1.primary_key.clone(),
        columns,
    })
}

fn convert_column(
    v1: &V1ColumnConfig,
    table_name: &str,
    warnings: &mut Vec<String>,
) -> Result<ColumnConfig> {
    // is_track: "true"/"false" string → enum
    let is_track = match v1.is_track.trim() {
        "true" => TrackMode::True,
        "false" => TrackMode::False,
        other => {
            warnings.push(format!(
                "Table '{}', column '{}': unknown is_track value '{}', defaulting to false.",
                table_name, v1.column_name, other
            ));
            TrackMode::False
        }
    };

    // col_type: loose string → enum
    let col_type = match v1.col_type.as_deref().unwrap_or("string") {
        "numeric" => ColumnType::Numeric,
        "bool" => ColumnType::Bool,
        "string" => ColumnType::String,
        other => {
            warnings.push(format!(
                "Table '{}', column '{}': unknown type '{}', defaulting to string.",
                table_name, v1.column_name, other
            ));
            ColumnType::String
        }
    };

    // Validate: non-tracked columns should have a default
    if is_track == TrackMode::False && v1.default.is_none() {
        warnings.push(format!(
            "Table '{}', column '{}': is_track=false but no default value set.",
            table_name, v1.column_name
        ));
    }

    Ok(ColumnConfig {
        column_name: v1.column_name.clone(),
        is_track,
        col_type,
        default: v1.default.clone(),
    })
}

// ─── Diff display ─────────────────────────────────────────────────────────────

fn print_diff(before: &str, after: &str) {
    println!("\n{}", "=".repeat(60));
    println!("  CONFIG DIFF  (- before  /  + after)");
    println!("{}\n", "=".repeat(60));

    // Simple line-by-line diff — no external dep needed
    let before_lines: Vec<&str> = before.lines().collect();
    let after_lines: Vec<&str> = after.lines().collect();

    // Use a naive LCS-based approach for clarity
    let lcs = lcs_diff(&before_lines, &after_lines);

    for entry in lcs {
        match entry {
            DiffEntry::Same(line) => println!("  {}", line),
            DiffEntry::Removed(line) => println!("\x1b[31m- {}\x1b[0m", line),
            DiffEntry::Added(line) => println!("\x1b[32m+ {}\x1b[0m", line),
        }
    }
    println!();
}

// ─── Minimal LCS differ ───────────────────────────────────────────────────────

enum DiffEntry<'a> {
    Same(&'a str),
    Removed(&'a str),
    Added(&'a str),
}

fn lcs_diff<'a>(a: &[&'a str], b: &[&'a str]) -> Vec<DiffEntry<'a>> {
    let m = a.len();
    let n = b.len();
    // Build LCS table
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    // Backtrack
    let mut result = Vec::new();
    let (mut i, mut j) = (m, n);
    while i > 0 || j > 0 {
        if i > 0 && j > 0 && a[i - 1] == b[j - 1] {
            result.push(DiffEntry::Same(a[i - 1]));
            i -= 1;
            j -= 1;
        } else if j > 0 && (i == 0 || dp[i][j - 1] >= dp[i - 1][j]) {
            result.push(DiffEntry::Added(b[j - 1]));
            j -= 1;
        } else {
            result.push(DiffEntry::Removed(a[i - 1]));
            i -= 1;
        }
    }
    result.reverse();
    result
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn derive_output_path(input: &str) -> String {
    // config.yaml → config.v2.yaml
    if let Some(stem) = input.strip_suffix(".yaml") {
        return format!("{}.v2.yaml", stem);
    }
    if let Some(stem) = input.strip_suffix(".yml") {
        return format!("{}.v2.yml", stem);
    }
    format!("{}.v2.yaml", input)
}
