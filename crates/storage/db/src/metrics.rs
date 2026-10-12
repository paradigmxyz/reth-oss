#[cfg(feature = "mdbx")]
use crate::implementation::mdbx::packing::{PackingMode, ADAPTIVE_TRIE_DEPTH, FULL_TRIE_DEPTH};
use crate::Tables;
use metrics::Histogram;
use reth_metrics::{metrics::Counter, Metrics};
use reth_primitives_traits::FastInstant as Instant;
use rustc_hash::FxHashMap;
use std::{array, sync::Arc, time::Duration};
use strum::{EnumCount, EnumIter, IntoEnumIterator};

const LARGE_VALUE_THRESHOLD_BYTES: usize = 4096;

/// Caches metric handles for database environment to make sure handles are not re-created
/// on every operation.
///
/// Requires a metric recorder to be registered before creating an instance of this struct.
/// Otherwise, metric recording will no-op.
#[derive(Debug)]
pub(crate) struct DatabaseEnvMetrics {
    /// Caches per-table operation metric handles for all database operation metrics.
    operations: FxHashMap<&'static str, TableOperationMetrics>,
    /// Caches `TransactionMetrics` handles for counters grouped by only transaction mode.
    /// Updated both at tx open and close.
    transactions: FxHashMap<TransactionMode, TransactionMetrics>,
    /// Caches `TransactionOutcomeMetrics` handles for counters grouped by transaction mode and
    /// outcome. Can only be updated at tx close, as outcome is only known at that point.
    transaction_outcomes:
        FxHashMap<(TransactionMode, TransactionOutcome), TransactionOutcomeMetrics>,
    /// Pre-bound handles for each packed storage and trie retention layout.
    #[cfg(feature = "mdbx")]
    packing_flush: FxHashMap<(&'static str, &'static str), PackingFlushMetrics>,
}

/// Per-table operation metric handles cached for hot cursor paths.
pub(crate) type TableOperationMetrics = Arc<[OperationMetrics; Operation::COUNT]>;

impl DatabaseEnvMetrics {
    pub(crate) fn new() -> Self {
        // Pre-populate metric handle maps with all possible combinations of labels
        // to avoid runtime locks on the map when recording metrics.
        Self {
            operations: Self::generate_operation_handles(),
            transactions: Self::generate_transaction_handles(),
            transaction_outcomes: Self::generate_transaction_outcome_handles(),
            #[cfg(feature = "mdbx")]
            packing_flush: Self::generate_packing_flush_handles(),
        }
    }

    /// Generate a map of pre-bound operation handles for each table.
    fn generate_operation_handles() -> FxHashMap<&'static str, TableOperationMetrics> {
        let mut operations = FxHashMap::with_capacity_and_hasher(Tables::COUNT, Default::default());

        for table in Tables::ALL {
            let table_name = table.name();
            let metrics = array::from_fn(|index| {
                let operation = Operation::from_index(index);
                OperationMetrics::new_with_labels(&[
                    (Labels::Table.as_str(), table_name),
                    (Labels::Operation.as_str(), operation.as_str()),
                ])
            });

            operations.insert(table_name, Arc::new(metrics));
        }

        operations
    }

    /// Generate a map of all possible transaction modes to metric handles.
    /// Used for tracking a counter of open transactions.
    fn generate_transaction_handles() -> FxHashMap<TransactionMode, TransactionMetrics> {
        TransactionMode::iter()
            .map(|mode| {
                (
                    mode,
                    TransactionMetrics::new_with_labels(&[(
                        Labels::TransactionMode.as_str(),
                        mode.as_str(),
                    )]),
                )
            })
            .collect()
    }

    /// Generate a map of all possible transaction mode and outcome handles.
    /// Used for tracking various stats for finished transactions (e.g. commit duration).
    fn generate_transaction_outcome_handles(
    ) -> FxHashMap<(TransactionMode, TransactionOutcome), TransactionOutcomeMetrics> {
        let mut transaction_outcomes = FxHashMap::with_capacity_and_hasher(
            TransactionMode::COUNT * TransactionOutcome::COUNT,
            Default::default(),
        );
        for mode in TransactionMode::iter() {
            for outcome in TransactionOutcome::iter() {
                transaction_outcomes.insert(
                    (mode, outcome),
                    TransactionOutcomeMetrics::new_with_labels(&[
                        (Labels::TransactionMode.as_str(), mode.as_str()),
                        (Labels::TransactionOutcome.as_str(), outcome.as_str()),
                    ]),
                );
            }
        }
        transaction_outcomes
    }

    /// Record a metric for database operation executed in `f`.
    /// Panics if a metric recorder is not found for the given table and operation.
    pub(crate) fn record_operation<R>(
        &self,
        table: &'static str,
        operation: Operation,
        value_size: Option<usize>,
        f: impl FnOnce() -> R,
    ) -> R {
        if let Some(metrics) = self.operations.get(table) {
            metrics[operation.index()].record(value_size, f)
        } else {
            f()
        }
    }

    /// Returns pre-bound operation metric handles for a single table.
    pub(crate) fn table_operation_metrics(&self, table: &'static str) -> TableOperationMetrics {
        self.operations.get(table).expect("table operation metric handles not found").clone()
    }

    #[cfg(feature = "mdbx")]
    fn generate_packing_flush_handles(
    ) -> FxHashMap<(&'static str, &'static str), PackingFlushMetrics> {
        ["dense", "integer32"]
            .into_iter()
            .flat_map(|mode| {
                ["cutoff", "full", "adaptive"].into_iter().map(move |retention| {
                    (
                        (mode, retention),
                        PackingFlushMetrics::new_with_labels(&[
                            ("packing_mode", mode),
                            ("retention", retention),
                        ]),
                    )
                })
            })
            .collect()
    }

    /// Returns cached flush handles without registering metrics in the transaction path.
    #[cfg(feature = "mdbx")]
    pub(crate) fn packing_flush_metrics(
        &self,
        mode: PackingMode,
        depth: usize,
    ) -> PackingFlushMetrics {
        let mode = match mode {
            PackingMode::Dense => "dense",
            PackingMode::Integer32 => "integer32",
        };
        let retention = match depth {
            FULL_TRIE_DEPTH => "full",
            ADAPTIVE_TRIE_DEPTH => "adaptive",
            _ => "cutoff",
        };
        self.packing_flush
            .get(&(mode, retention))
            .expect("packing metric handles not found")
            .clone()
    }

    /// Record metrics for opening a database transaction.
    pub(crate) fn record_opened_transaction(&self, mode: TransactionMode) {
        self.transactions
            .get(&mode)
            .expect("transaction mode metric handle not found")
            .record_open();
    }

    /// Record metrics for closing a database transactions.
    #[cfg(feature = "mdbx")]
    pub(crate) fn record_closed_transaction(
        &self,
        mode: TransactionMode,
        outcome: TransactionOutcome,
        open_duration: Duration,
        close_duration: Option<Duration>,
        commit_latency: Option<reth_libmdbx::CommitLatency>,
    ) {
        self.transactions
            .get(&mode)
            .expect("transaction mode metric handle not found")
            .record_close();

        self.transaction_outcomes
            .get(&(mode, outcome))
            .expect("transaction outcome metric handle not found")
            .record(open_duration, close_duration, commit_latency);
    }
}

/// Physical packed work completed by one database transaction.
#[cfg(feature = "mdbx")]
#[derive(Debug)]
pub(crate) struct PackingFlushStats {
    pub(crate) total_duration: Duration,
    pub(crate) encoding_duration: Duration,
    pub(crate) persistence_duration: Duration,
    pub(crate) reconstruction_duration: Duration,
    pub(crate) blobs: u64,
    pub(crate) encoded_bytes: u64,
    pub(crate) repaired_contracts: u64,
    pub(crate) repair_leaves: u64,
    pub(crate) completed_contracts: u64,
}

/// Cached metrics recorded once per successful packed flush.
#[cfg(feature = "mdbx")]
#[derive(Metrics, Clone)]
#[metrics(scope = "database.packing")]
pub(crate) struct PackingFlushMetrics {
    /// Total packed flush time, including preparation.
    flush_duration_seconds: Histogram,
    /// Time encoding changed blobs.
    encoding_duration_seconds: Histogram,
    /// Time writing changed blobs.
    persistence_duration_seconds: Histogram,
    /// Time repairing retained trie nodes and persisting frontier metadata.
    reconstruction_duration_seconds: Histogram,
    /// Number of encoded blobs.
    blobs_total: Counter,
    /// Encoded blob bytes submitted to MDBX.
    encoded_bytes_total: Counter,
    /// Contracts that required trie repair.
    repaired_contracts_total: Counter,
    /// Storage leaves visited by trie repair.
    repair_leaves_total: Counter,
    /// Contracts that completed without trie repair.
    completed_contracts_total: Counter,
}

#[cfg(feature = "mdbx")]
impl PackingFlushMetrics {
    pub(crate) fn record(&self, stats: PackingFlushStats) {
        self.flush_duration_seconds.record(stats.total_duration);
        self.encoding_duration_seconds.record(stats.encoding_duration);
        self.persistence_duration_seconds.record(stats.persistence_duration);
        self.reconstruction_duration_seconds.record(stats.reconstruction_duration);
        self.blobs_total.increment(stats.blobs);
        self.encoded_bytes_total.increment(stats.encoded_bytes);
        self.repaired_contracts_total.increment(stats.repaired_contracts);
        self.repair_leaves_total.increment(stats.repair_leaves);
        self.completed_contracts_total.increment(stats.completed_contracts);
    }
}

/// Transaction mode for the database, either read-only or read-write.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, EnumCount, EnumIter)]
pub(crate) enum TransactionMode {
    /// Read-only transaction mode.
    ReadOnly,
    /// Read-write transaction mode.
    ReadWrite,
}

impl TransactionMode {
    /// Returns the transaction mode as a string.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::ReadWrite => "read-write",
        }
    }

    /// Returns `true` if the transaction mode is read-only.
    pub(crate) const fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

/// Transaction outcome after a database operation - commit, abort, or drop.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, EnumCount, EnumIter)]
pub(crate) enum TransactionOutcome {
    /// Successful commit of the transaction.
    Commit,
    /// Aborted transaction.
    Abort,
    /// Dropped transaction.
    Drop,
}

impl TransactionOutcome {
    /// Returns the transaction outcome as a string.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Abort => "abort",
            Self::Drop => "drop",
        }
    }

    /// Returns `true` if the transaction outcome is a commit.
    pub(crate) const fn is_commit(&self) -> bool {
        matches!(self, Self::Commit)
    }
}

/// Types of operations conducted on the database: get, put, delete, and various cursor operations.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, EnumCount, EnumIter)]
pub(crate) enum Operation {
    /// Database get operation.
    Get,
    /// Database put upsert operation.
    PutUpsert,
    /// Database put append operation.
    PutAppend,
    /// Database delete operation.
    Delete,
    /// Database cursor upsert operation.
    CursorUpsert,
    /// Database cursor insert operation.
    CursorInsert,
    /// Database cursor append operation.
    CursorAppend,
    /// Database cursor append duplicates operation.
    CursorAppendDup,
    /// Database cursor delete current operation.
    CursorDeleteCurrent,
    /// Database cursor delete current duplicates operation.
    CursorDeleteCurrentDuplicates,
}

impl Operation {
    /// Returns the index of the operation in the cached per-table operation array.
    pub(crate) const fn index(&self) -> usize {
        match self {
            Self::Get => 0,
            Self::PutUpsert => 1,
            Self::PutAppend => 2,
            Self::Delete => 3,
            Self::CursorUpsert => 4,
            Self::CursorInsert => 5,
            Self::CursorAppend => 6,
            Self::CursorAppendDup => 7,
            Self::CursorDeleteCurrent => 8,
            Self::CursorDeleteCurrentDuplicates => 9,
        }
    }

    /// Returns the operation for the given index in the cached per-table operation array.
    const fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Get,
            1 => Self::PutUpsert,
            2 => Self::PutAppend,
            3 => Self::Delete,
            4 => Self::CursorUpsert,
            5 => Self::CursorInsert,
            6 => Self::CursorAppend,
            7 => Self::CursorAppendDup,
            8 => Self::CursorDeleteCurrent,
            9 => Self::CursorDeleteCurrentDuplicates,
            _ => panic!("invalid operation index"),
        }
    }

    /// Returns the operation as a string.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::PutUpsert => "put-upsert",
            Self::PutAppend => "put-append",
            Self::Delete => "delete",
            Self::CursorUpsert => "cursor-upsert",
            Self::CursorInsert => "cursor-insert",
            Self::CursorAppend => "cursor-append",
            Self::CursorAppendDup => "cursor-append-dup",
            Self::CursorDeleteCurrent => "cursor-delete-current",
            Self::CursorDeleteCurrentDuplicates => "cursor-delete-current-duplicates",
        }
    }
}

/// Enum defining labels for various aspects used in metrics.
enum Labels {
    /// Label representing a table.
    Table,
    /// Label representing a transaction mode.
    TransactionMode,
    /// Label representing a transaction outcome.
    TransactionOutcome,
    /// Label representing a database operation.
    Operation,
}

impl Labels {
    /// Converts each label variant into its corresponding string representation.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TransactionMode => "mode",
            Self::TransactionOutcome => "outcome",
            Self::Operation => "operation",
        }
    }
}

#[derive(Metrics, Clone)]
#[metrics(scope = "database.transaction")]
pub(crate) struct TransactionMetrics {
    /// Total number of opened database transactions (cumulative)
    opened_total: Counter,
    /// Total number of closed database transactions (cumulative)
    closed_total: Counter,
}

impl TransactionMetrics {
    pub(crate) fn record_open(&self) {
        self.opened_total.increment(1);
    }

    pub(crate) fn record_close(&self) {
        self.closed_total.increment(1);
    }
}

#[derive(Metrics, Clone)]
#[metrics(scope = "database.transaction")]
pub(crate) struct TransactionOutcomeMetrics {
    /// The time a database transaction has been open
    open_duration_seconds: Histogram,
    /// The time it took to close a database transaction
    close_duration_seconds: Histogram,
    /// The time it took to prepare a transaction commit
    commit_preparation_duration_seconds: Histogram,
    /// Duration of GC update during transaction commit by wall clock
    commit_gc_wallclock_duration_seconds: Histogram,
    /// The time it took to conduct audit of a transaction commit
    commit_audit_duration_seconds: Histogram,
    /// The time it took to write dirty/modified data pages to a filesystem during transaction
    /// commit
    commit_write_duration_seconds: Histogram,
    /// The time it took to sync written data to the disk/storage during transaction commit
    commit_sync_duration_seconds: Histogram,
    /// The time it took to release resources during transaction commit
    commit_ending_duration_seconds: Histogram,
    /// The total duration of a transaction commit
    commit_whole_duration_seconds: Histogram,
    /// User-mode CPU time spent on GC update during transaction commit
    commit_gc_cputime_duration_seconds: Histogram,
}

impl TransactionOutcomeMetrics {
    /// Record transaction closing with the duration it was open and the duration it took to close
    /// it.
    #[cfg(feature = "mdbx")]
    pub(crate) fn record(
        &self,
        open_duration: Duration,
        close_duration: Option<Duration>,
        commit_latency: Option<reth_libmdbx::CommitLatency>,
    ) {
        self.open_duration_seconds.record(open_duration);

        if let Some(close_duration) = close_duration {
            self.close_duration_seconds.record(close_duration)
        }

        if let Some(commit_latency) = commit_latency {
            self.commit_preparation_duration_seconds.record(commit_latency.preparation());
            self.commit_gc_wallclock_duration_seconds.record(commit_latency.gc_wallclock());
            self.commit_audit_duration_seconds.record(commit_latency.audit());
            self.commit_write_duration_seconds.record(commit_latency.write());
            self.commit_sync_duration_seconds.record(commit_latency.sync());
            self.commit_ending_duration_seconds.record(commit_latency.ending());
            self.commit_whole_duration_seconds.record(commit_latency.whole());
            self.commit_gc_cputime_duration_seconds.record(commit_latency.gc_cputime());
        }
    }
}

#[derive(Metrics, Clone)]
#[metrics(scope = "database.operation")]
pub(crate) struct OperationMetrics {
    /// Total number of database operations made
    calls_total: Counter,
    /// The time it took to execute a database operation (`put/upsert/insert/append/append_dup`)
    /// with value larger than [`LARGE_VALUE_THRESHOLD_BYTES`] bytes.
    large_value_duration_seconds: Histogram,
}

impl OperationMetrics {
    /// Record operation metric.
    ///
    /// The duration it took to execute the closure is recorded only if the provided `value_size` is
    /// larger than [`LARGE_VALUE_THRESHOLD_BYTES`].
    pub(crate) fn record<R>(&self, value_size: Option<usize>, f: impl FnOnce() -> R) -> R {
        self.calls_total.increment(1);

        // Record duration only for large values to prevent the performance hit of clock syscall
        // on small operations
        if value_size.is_some_and(|size| size > LARGE_VALUE_THRESHOLD_BYTES) {
            let start = Instant::now();
            let result = f();
            self.large_value_duration_seconds.record(start.elapsed());
            result
        } else {
            f()
        }
    }
}
