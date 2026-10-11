//! Portable launcher logs share the full launcher's verbosity convention and use stderr.
//! The feature-enabled launcher continues to use its existing tracing configuration.

use clap::{ArgAction, Args};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// Display controls for the portable status/readiness executable.
#[derive(Debug, Args)]
pub(super) struct LoggingArgs {
    /// Minimum log level: -v errors, -vv warnings, -vvv info, -vvvv debug, -vvvvv trace.
    #[arg(short = 'v', long, action = ArgAction::Count, default_value_t = 3, global = true)]
    verbosity: u8,
    /// Silence logging while preserving command output and exit status.
    #[arg(short = 'q', long, alias = "silent", global = true)]
    quiet: bool,
    /// Override the console filter; otherwise use `RUST_LOG` or the verbosity level.
    #[arg(long = "log.stdout.filter", global = true, value_name = "FILTER")]
    filter: Option<String>,
}

impl LoggingArgs {
    /// Install only console logging; no database, log directory or telemetry endpoint is opened.
    pub(super) fn init(&self) -> eyre::Result<()> {
        let filter = if self.quiet {
            EnvFilter::new("off")
        } else if let Some(filter) = &self.filter {
            EnvFilter::try_new(filter)?
        } else {
            let level = match self.verbosity {
                0 | 1 => LevelFilter::ERROR,
                2 => LevelFilter::WARN,
                3 => LevelFilter::INFO,
                4 => LevelFilter::DEBUG,
                _ => LevelFilter::TRACE,
            };
            EnvFilter::builder().with_default_directive(level.into()).from_env()?
        };
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .try_init()
            .map_err(|error| eyre::eyre!("console logger initialization failed: {error}"))?;
        Ok(())
    }
}
