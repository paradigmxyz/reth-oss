//! clap [`Args`] for static files configuration.

use clap::Args;
use reth_config::config::{BlocksPerFileConfig, StaticFilesConfig};

/// Blocks per header file when running a `--minimal` node.
pub const MINIMAL_BLOCKS_PER_FILE: u64 = 10000;

/// Bound the unpruneable tail of history and transaction files without changing retention.
pub const MINIMAL_HISTORY_BLOCKS_PER_FILE: u64 = 1000;

/// Receipts retain a short reorg window, so seal files more frequently than history.
pub const MINIMAL_RECEIPTS_BLOCKS_PER_FILE: u64 = 128;

/// Parameters for static files configuration
#[derive(Debug, Args, PartialEq, Eq, Clone, Copy, Default)]
#[command(next_help_heading = "Static Files")]
pub struct StaticFilesArgs {
    /// Number of blocks per file for the headers segment.
    #[arg(long = "static-files.blocks-per-file.headers")]
    pub blocks_per_file_headers: Option<u64>,

    /// Number of blocks per file for the transactions segment.
    #[arg(long = "static-files.blocks-per-file.transactions")]
    pub blocks_per_file_transactions: Option<u64>,

    /// Number of blocks per file for the receipts segment.
    #[arg(long = "static-files.blocks-per-file.receipts")]
    pub blocks_per_file_receipts: Option<u64>,

    /// Number of blocks per file for the transaction senders segment.
    #[arg(long = "static-files.blocks-per-file.transaction-senders")]
    pub blocks_per_file_transaction_senders: Option<u64>,

    /// Number of blocks per file for the account changesets segment.
    #[arg(long = "static-files.blocks-per-file.account-change-sets")]
    pub blocks_per_file_account_change_sets: Option<u64>,

    /// Number of blocks per file for the storage changesets segment.
    #[arg(long = "static-files.blocks-per-file.storage-change-sets")]
    pub blocks_per_file_storage_change_sets: Option<u64>,
}

impl StaticFilesArgs {
    /// Merges the CLI arguments with an existing [`StaticFilesConfig`], giving priority to CLI
    /// args.
    ///
    /// Minimal defaults use smaller pruneable segments. Explicit CLI and configuration values
    /// take precedence over defaults, and retention distances remain unchanged.
    pub fn merge_with_config(&self, config: StaticFilesConfig, minimal: bool) -> StaticFilesConfig {
        let minimal_blocks_per_file = minimal.then_some(MINIMAL_BLOCKS_PER_FILE);
        let minimal_history_blocks = minimal.then_some(MINIMAL_HISTORY_BLOCKS_PER_FILE);
        let minimal_receipts_blocks = minimal.then_some(MINIMAL_RECEIPTS_BLOCKS_PER_FILE);
        StaticFilesConfig {
            blocks_per_file: BlocksPerFileConfig {
                headers: self
                    .blocks_per_file_headers
                    .or(config.blocks_per_file.headers)
                    .or(minimal_blocks_per_file),
                transactions: self
                    .blocks_per_file_transactions
                    .or(config.blocks_per_file.transactions)
                    .or(minimal_history_blocks),
                receipts: self
                    .blocks_per_file_receipts
                    .or(config.blocks_per_file.receipts)
                    .or(minimal_receipts_blocks),
                transaction_senders: self
                    .blocks_per_file_transaction_senders
                    .or(config.blocks_per_file.transaction_senders)
                    .or(minimal_history_blocks),
                account_change_sets: self
                    .blocks_per_file_account_change_sets
                    .or(config.blocks_per_file.account_change_sets)
                    .or(minimal_history_blocks),
                storage_change_sets: self
                    .blocks_per_file_storage_change_sets
                    .or(config.blocks_per_file.storage_change_sets)
                    .or(minimal_history_blocks),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_segments_bound_pruning_slack() {
        let config =
            StaticFilesArgs::default().merge_with_config(StaticFilesConfig::default(), true);
        assert_eq!(config.blocks_per_file.headers, Some(MINIMAL_BLOCKS_PER_FILE));
        assert_eq!(config.blocks_per_file.receipts, Some(MINIMAL_RECEIPTS_BLOCKS_PER_FILE));
        assert_eq!(config.blocks_per_file.transactions, Some(MINIMAL_HISTORY_BLOCKS_PER_FILE));
        assert_eq!(
            config.blocks_per_file.account_change_sets,
            Some(MINIMAL_HISTORY_BLOCKS_PER_FILE)
        );
        assert_eq!(
            config.blocks_per_file.storage_change_sets,
            Some(MINIMAL_HISTORY_BLOCKS_PER_FILE)
        );
    }

    #[test]
    fn explicit_segment_sizes_override_minimal_defaults() {
        let configured = StaticFilesConfig {
            blocks_per_file: BlocksPerFileConfig {
                receipts: Some(256),
                transactions: Some(500),
                ..Default::default()
            },
        };
        let args = StaticFilesArgs { blocks_per_file_receipts: Some(192), ..Default::default() };
        let merged = args.merge_with_config(configured, true);
        assert_eq!(merged.blocks_per_file.receipts, Some(192));
        assert_eq!(merged.blocks_per_file.transactions, Some(500));
        let normal = StaticFilesArgs::default().merge_with_config(configured, false);
        assert_eq!(normal, configured);
    }
}
