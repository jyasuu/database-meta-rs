mod config;
mod db;
mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "db-tools", about = "Database export and comparison tools")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Export database table data to a file
    DatabaseMeta {
        /// Output format: json, csv, xml, html, insert, yaml
        format: String,

        /// Path to YAML config file
        #[arg(short = 'c', long = "config")]
        config: String,

        /// Output directory
        #[arg(short = 'o', long = "out", default_value = "./out")]
        out: String,
    },

    /// Compare two tables from two different databases and generate SQL
    CompareTables {
        /// Path to YAML config file
        #[arg(short = 'c', long = "config")]
        config: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::DatabaseMeta { format, config, out } => {
            commands::database_meta(&format, &config, &out)?;
        }
        Commands::CompareTables { config } => {
            let msg = commands::compare_tables(&config)?;
            println!("{}", msg);
        }
    }

    Ok(())
}
