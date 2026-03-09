mod commands;
mod config;
mod db;
mod error;
mod format;
mod migrate;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

// ─── CLI definition ───────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "db-tools",
    version,
    about = "Database export and sync tools",
    long_about = None,
)]
struct Cli {
    /// Increase verbosity (-v info, -vv debug, -vvv trace)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export table data to files (json/csv/xml/html/yaml/insert)
    Export {
        /// Output format
        #[arg(value_enum)]
        format: format::OutputFormat,

        /// Path to YAML config file
        #[arg(short = 'c', long = "config")]
        config: String,

        /// Output directory
        #[arg(short = 'o', long = "out", default_value = "./out")]
        out: String,
    },

    /// Compare source → target and emit DML (INSERT/UPDATE/DELETE)
    Sync {
        /// Path to YAML config file
        #[arg(short = 'c', long = "config")]
        config: String,

        /// Output SQL file path
        #[arg(short = 'o', long = "out", default_value = "dml.sql")]
        out: String,

        /// Dry-run: print SQL but do not write file
        #[arg(long)]
        dry_run: bool,
    },

    /// Config migration utilities
    Migrate {
        #[command(subcommand)]
        action: migrate::MigrateAction,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Map -v count to log level
    let level = match cli.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(level)),
        )
        .with_target(false)
        .compact()
        .init();

    match cli.command {
        Commands::Export { format, config, out } => {
            commands::export::run(&config, format, &out).await?;
        }
        Commands::Sync { config, out, dry_run } => {
            commands::sync::run(&config, &out, dry_run).await?;
        }
        Commands::Migrate { action } => {
            migrate::run(action)?;
        }
    }

    Ok(())
}
