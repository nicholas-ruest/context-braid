#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use context_braid::{append, initialize, snapshot, verify, write_snapshot, AppendRequest, Stance};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "context-braid")]
#[command(about = "Keep multi-agent context append-only, conflict-aware, and verifiable")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create an empty ledger without overwriting an existing file.
    Init { ledger: PathBuf },
    /// Append one sourced claim under an exclusive writer lock.
    Append {
        ledger: PathBuf,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        stance: Stance,
        #[arg(long)]
        claim: String,
        #[arg(long)]
        source: String,
        #[arg(long)]
        author: String,
        #[arg(long = "supersedes")]
        supersedes: Vec<String>,
        #[arg(long = "expect-head")]
        expected_head: Option<String>,
    },
    /// Verify the full hash chain and semantic invariants.
    Verify { ledger: PathBuf },
    /// Write a compact view that retains every unresolved claim.
    Snapshot {
        ledger: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Init { ledger } => {
            initialize(&ledger)?;
            println!("initialized {}", ledger.display());
        }
        Command::Append {
            ledger,
            topic,
            stance,
            claim,
            source,
            author,
            supersedes,
            expected_head,
        } => {
            let event = append(
                ledger,
                AppendRequest {
                    topic,
                    stance,
                    claim,
                    source,
                    author,
                    supersedes,
                    expected_head,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&event)?);
        }
        Command::Verify { ledger } => {
            let result = verify(ledger)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Snapshot { ledger, output } => {
            let result = snapshot(ledger)?;
            write_snapshot(&result, &output)?;
            println!(
                "wrote {} topics at head {} to {}",
                result.topics.len(),
                result.ledger_head,
                output.display()
            );
        }
    }
    Ok(())
}
