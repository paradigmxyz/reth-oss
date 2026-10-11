//! Measurements of the same typed execution proxy used by consensus.

use reth_embedded_beacon_execution_proxy::{ExecutionEngine, ExecutionProxy};
use reth_metrics::{
    metrics::{self, Counter, Gauge},
    Metrics,
};
use std::time::Duration;

#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.execution_proxy")]
struct TransportMetrics {
    /// Cumulative calls since this owner started.
    calls_total: Counter,
    /// Cumulative failures since this owner started.
    failures_total: Counter,
    /// Cumulative timeouts since this owner started.
    timeouts_total: Counter,
    /// Cumulative typed execution call time including admission wait, in nanoseconds.
    elapsed_nanos_total: Counter,
    /// Whether the actual typed execution proxy has closed admission.
    closed: Gauge,
}

pub(in crate::reth) async fn observe_transport<E: ExecutionEngine>(proxy: ExecutionProxy<E>) {
    let metrics = TransportMetrics::default();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let stats = proxy.stats();
        metrics.calls_total.absolute(stats.calls);
        metrics.failures_total.absolute(stats.failures);
        metrics.timeouts_total.absolute(stats.timeouts);
        metrics.elapsed_nanos_total.absolute(stats.elapsed_nanos);
        metrics.closed.set(u8::from(proxy.is_closed()) as f64);
    }
}
