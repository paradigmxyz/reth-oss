//! Canonical execution measurements from the launched provider.

use reth_ethereum_primitives::EthPrimitives;
use reth_metrics::{
    metrics::{self, Counter, Gauge, Histogram},
    Metrics,
};
use reth_node_api::{FullNodeComponents, NodeAddOns, NodeTypes};
use reth_node_builder::FullNode;
use reth_provider::CanonStateSubscriptions;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::error::RecvError;

/// Fixed-name counters have no per-root, per-peer or transaction labels.
#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.execution")]
struct CanonicalMetrics {
    /// Number of actual committed canonical blocks observed by this subscription.
    canonical_blocks: Counter,
    /// Blocks removed by observed canonical reorgs/reverts.
    reverted_blocks: Counter,
    /// Observed canonical reorg/revert notifications.
    reorganizations: Counter,
    /// Canonical notifications missed because the observer fell behind.
    missed_notifications: Counter,
    /// Gas consumed by observed canonical blocks.
    gas_used: Counter,
    /// Blob gas consumed by observed canonical blocks.
    blob_gas_used: Counter,
    /// Last observed canonical block number; decreases are allowed during reorgs.
    latest_block_number: Gauge,
    /// Gas limit of the last committed canonical block.
    latest_gas_limit: Gauge,
    /// Gas used by the last committed canonical block.
    latest_gas_used: Gauge,
    /// Transactions in the last committed canonical block.
    latest_transactions: Gauge,
    /// Gas utilization of the last committed canonical block.
    latest_gas_utilization: Gauge,
    /// Clock seconds since the last committed block timestamp, clamped at zero.
    latest_timestamp_lag_seconds: Gauge,
    /// Transactions observed in committed canonical blocks, including later reverted blocks.
    transactions_total: Counter,
    /// Time spent inspecting one already validated canonical notification.
    notification_processing_seconds: Histogram,
}

/// Register a shutdown-bound observer against the actual launched execution provider.
pub(in crate::reth) fn install<N, A>(node: &FullNode<N, A>)
where
    N: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
    A: NodeAddOns<N>,
{
    let mut notifications = node.provider.subscribe_to_canonical_state();
    let metrics = CanonicalMetrics::default();
    node.task_executor.spawn_task(async move {
        loop {
            let notification = match notifications.recv().await {
                Ok(notification) => notification,
                Err(RecvError::Lagged(missed)) => {
                    metrics.missed_notifications.increment(missed);
                    tracing::warn!(target: "beacon::execution", missed,
                        "[beacon] Canonical telemetry subscription missed notifications");
                    continue;
                }
                Err(RecvError::Closed) => {
                    tracing::debug!(target: "beacon::execution", "[beacon] Canonical telemetry subscription closed");
                    break;
                }
            };
            let started = Instant::now();
            if let Some(old) = notification.reverted() {
                if notification.tip_checked().is_none() && !old.is_empty() {
                    metrics.latest_block_number.set(old.first().header().number.saturating_sub(1) as f64);
                }
                metrics.reorganizations.increment(1);
                metrics.reverted_blocks.increment(old.len() as u64);
                tracing::info!(target: "beacon::execution", reverted_blocks = old.len(),
                    "[beacon] Execution canonical chain reorganized");
            }
            let committed = notification.committed();
            for (index, block) in committed.blocks().values().enumerate() {
                let header = block.header();
                let blob_gas_used = header.blob_gas_used.unwrap_or_default();
                metrics.canonical_blocks.increment(1);
                metrics.gas_used.increment(header.gas_used);
                metrics.blob_gas_used.increment(blob_gas_used);
                metrics.latest_block_number.set(header.number as f64);
                metrics.latest_gas_limit.set(header.gas_limit as f64);
                metrics.latest_gas_used.set(header.gas_used as f64);
                metrics.latest_transactions.set(block.body().transactions.len() as f64);
                metrics.latest_gas_utilization.set(if header.gas_limit == 0 { f64::NAN } else { header.gas_used as f64 / header.gas_limit as f64 });
                metrics.latest_timestamp_lag_seconds.set(SystemTime::now().duration_since(UNIX_EPOCH).map(|time| time.as_secs().saturating_sub(header.timestamp) as f64).unwrap_or(f64::NAN));
                metrics.transactions_total.increment(block.body().transactions.len() as u64);
                // Every metric is recorded. Bound a large historical segment's console output;
                // the actual live head is always included even when a segment is truncated.
                if index < 128 || index + 1 == committed.len() {
                    tracing::info!(target: "beacon::execution",
                        number = header.number, hash = ?block.hash(), parent = ?header.parent_hash,
                        gas_used = header.gas_used, gas_limit = header.gas_limit,
                        transactions = block.body().transactions.len(), timestamp = header.timestamp,
                        blob_gas_used = ?header.blob_gas_used,
                        // One blob data-gas unit corresponds to one nominal blob data byte.
                        // This is not a measurement of compressed or shared bytes on disk.
                        blob_data_bytes = ?header.blob_gas_used,
                        "[beacon] Verified execution block became canonical");
                    let timestamp_lag_seconds = SystemTime::now().duration_since(UNIX_EPOCH)
                        .ok().map(|now| now.as_secs().saturating_sub(header.timestamp));
                    tracing::debug!(target: "beacon::execution",
                        number = header.number, timestamp_lag_seconds = ?timestamp_lag_seconds,
                        excess_blob_gas = ?header.excess_blob_gas,
                        "[beacon] Canonical execution block diagnostics");
                }
            }
            if committed.len() > 129 {
                tracing::info!(target: "beacon::execution",
                    segment_blocks = committed.len(), omitted_block_logs = committed.len() - 129,
                    "[beacon] Historical canonical segment summaries bounded");
            }
            metrics.notification_processing_seconds.record(started.elapsed().as_secs_f64());
            tracing::debug!(target: "beacon::execution",
                committed_blocks = committed.len(), elapsed = ?started.elapsed(),
                "[beacon] Processed canonical execution telemetry");
        }
    });
}
