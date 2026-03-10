//! Config migration utilities.
//!
//! v1 config (Java / original Rust port):
//!   - `databases.source.jdbcUrl` (JDBC prefix)
//!   - `databases.source.username` / `.password` (top-level, not in URL)
//!   - `columns[].is_track` is a bare string "true"/"false"
//!   - No `schema` field on tables
//!   - No `url` field (was `jdbcUrl`)
//!
//! v2 config (this codebase):
//!   - `databases.source.url` (plain postgres:// URL with creds embedded, OR separate username/password)
//!   - `columns[].is_track` is an enum: `true` | `false`
//!   - Optional `schema` field on tables
//!   - Optional `primary_key` promoted to required for sync

pub mod upgrade;
pub mod v1;

use anyhow::Result;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum MigrateAction {
    /// Upgrade a v1 config file to v2 format
    Upgrade {
        /// Input v1 config path
        #[arg(short = 'i', long = "input")]
        input: String,

        /// Output v2 config path (defaults to <input>.v2.yaml)
        #[arg(short = 'o', long = "output")]
        output: Option<String>,

        /// Print diff of changes to stdout
        #[arg(long)]
        diff: bool,
    },

    /// Validate a config file (works for both v1 and v2)
    Validate {
        /// Config file path
        #[arg(short = 'c', long = "config")]
        config: String,

        /// Config version to validate against
        #[arg(long, value_enum, default_value_t = ConfigVersion::V2)]
        version: ConfigVersion,
    },

    /// Show a summary of what changed between v1 and v2 schemas
    Changelog,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ConfigVersion {
    V1,
    V2,
}

pub fn run(action: MigrateAction) -> Result<()> {
    match action {
        MigrateAction::Upgrade {
            input,
            output,
            diff,
        } => upgrade::run(&input, output.as_deref(), diff),
        MigrateAction::Validate { config, version } => validate(&config, &version),
        MigrateAction::Changelog => {
            print_changelog();
            Ok(())
        }
    }
}

fn validate(config_path: &str, version: &ConfigVersion) -> Result<()> {
    match version {
        ConfigVersion::V1 => {
            let cfg = v1::load(config_path)?;
            println!("✓ Valid v1 config");
            println!("  Source DB: {}", cfg.databases.source.jdbc_url);
            println!("  Tables:    {}", cfg.tables.len());
            for t in &cfg.tables {
                let pk = t
                    .primary_key
                    .as_ref()
                    .map(|k| k.join(", "))
                    .unwrap_or_else(|| "(none)".to_string());
                let tracked: usize = t.columns.iter().filter(|c| c.is_track == "true").count();
                println!(
                    "    - {} ({} cols, {} tracked, pk: {})",
                    t.name,
                    t.columns.len(),
                    tracked,
                    pk
                );
            }
        }
        ConfigVersion::V2 => {
            let cfg = crate::config::load(config_path)?;
            println!("✓ Valid v2 config");
            println!("  Source DB: {}", cfg.databases.source.url);
            if cfg.databases.target.is_some() {
                println!("  Target DB: configured");
            }
            println!("  Tables:    {}", cfg.tables.len());
            for t in &cfg.tables {
                let pk = t
                    .primary_key
                    .as_ref()
                    .map(|k| k.join(", "))
                    .unwrap_or_else(|| "(none)".to_string());
                let tracked: usize = t.columns.iter().filter(|c| c.is_tracked()).count();
                println!(
                    "    - {} ({} cols, {} tracked, pk: {})",
                    t.name,
                    t.columns.len(),
                    tracked,
                    pk
                );
            }
        }
    }
    Ok(())
}

fn print_changelog() {
    println!(
        r#"
╔══════════════════════════════════════════════════════════════════╗
║              Config schema: v1 → v2 changelog                   ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  databases.<role>                                                ║
║    RENAMED  jdbcUrl        → url                                 ║
║    CHANGED  url format     jdbc:postgresql://  → postgresql://   ║
║    NEW      username / password can now be embedded in url       ║
║             OR kept as separate fields (both styles supported)   ║
║                                                                  ║
║  tables[]                                                        ║
║    NEW      schema         optional schema qualifier             ║
║                                                                  ║
║  tables[].columns[]                                              ║
║    CHANGED  is_track       "true"/"false" string                 ║
║                            → true/false YAML boolean             ║
║    CHANGED  type default   (implicit string)                     ║
║                            → explicit `string` enum value        ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
"#
    );
}
