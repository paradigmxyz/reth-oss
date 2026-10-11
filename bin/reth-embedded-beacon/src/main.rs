//! Additive experimental launcher; normal execution startup remains separately gated.

#[cfg(feature = "embedded-beacon")]
mod reth;

#[cfg(not(feature = "embedded-beacon"))]
mod logging;

#[cfg(not(feature = "embedded-beacon"))]
use clap::{Parser, Subcommand};
#[cfg(not(feature = "embedded-beacon"))]
use reth_embedded_beacon_node::Readiness;

#[cfg(not(feature = "embedded-beacon"))]
#[derive(Debug, Parser)]
#[command(name = "reth-embedded-beacon", about = "Experimental embedded beacon targeting Gloas")]
struct Cli {
    #[command(flatten)]
    logs: logging::LoggingArgs,
    #[command(subcommand)]
    command: Command,
}
#[cfg(not(feature = "embedded-beacon"))]
#[derive(Debug, Subcommand)]
enum Command {
    /// Print implemented readiness; never implies full consensus conformance.
    Status,
    /// Refuse live sync while the full consensus implementation is incomplete.
    Node,
}

#[cfg(not(feature = "embedded-beacon"))]
fn main() {
    let cli = Cli::parse();
    if let Err(error) = cli.logs.init() {
        eprintln!("{error}");
        std::process::exit(1);
    }
    let result = match cli.command {
        Command::Status => serde_json::to_string_pretty(&Readiness::current())
            .map(|json| println!("{json}"))
            .map_err(eyre::Report::from),
        Command::Node => {
            tracing::info!(target: "beacon::node", "[beacon] Embedded beacon startup requested");
            let readiness = Readiness::current();
            tracing::debug!(target: "beacon::node", ?readiness, "[beacon] Checked compiled consensus capabilities");
            readiness.require_live_sync().map_err(eyre::Report::from)
        }
    };
    if let Err(err) = result {
        tracing::error!(target: "beacon::node", error = %err, "[beacon] Embedded beacon startup failed");
        std::process::exit(1);
    }
}

#[cfg(feature = "embedded-beacon")]
fn main() {
    if let Err(err) = reth::run() {
        if tracing::dispatcher::has_been_set() {
            tracing::error!(target: "beacon::node", error = %err, "[beacon] Embedded launcher failed");
        } else {
            // Preflight can reject startup before the native CLI initializes its subscriber.
            eprintln!("{err:?}");
        }
        std::process::exit(1);
    }
}
