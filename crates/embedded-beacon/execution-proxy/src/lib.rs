//! Serialized in-process execution calls with deadlines and unchanged engine verdicts.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, TryAcquireError};

#[cfg(feature = "beacon-types")]
pub mod payload;

#[cfg(feature = "reth")]
mod reth;

#[cfg(feature = "reth")]
pub use reth::RethTypedEngine;

#[cfg(any(feature = "reth", feature = "beacon-types"))]
mod reply;
#[cfg(any(feature = "reth", feature = "beacon-types"))]
pub use reply::{ForkchoiceReply, PayloadReply};

/// Serializes execution imports and forkchoice updates without JSON encoding.
#[derive(Debug)]
pub struct ExecutionProxy<E> {
    engine: E,
    admission: Arc<Semaphore>,
    queue: Arc<Semaphore>,
    timeout: Duration,
    metrics: Arc<Metrics>,
}

impl<E: Clone> Clone for ExecutionProxy<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            admission: self.admission.clone(),
            queue: self.queue.clone(),
            timeout: self.timeout,
            metrics: self.metrics.clone(),
        }
    }
}

impl<E: ExecutionEngine> ExecutionProxy<E> {
    /// Creates an ordered proxy with a positive total admission/response deadline.
    pub fn new(engine: E, timeout: Duration) -> Result<Self, ProxyError<E::Error>> {
        if timeout.is_zero() {
            return Err(ProxyError::ZeroTimeout);
        }
        Ok(Self {
            engine,
            admission: Arc::new(Semaphore::new(1)),
            queue: Arc::new(Semaphore::new(128)),
            timeout,
            metrics: Arc::default(),
        })
    }

    /// Sends a payload through the versioned validation API.
    pub async fn new_payload(
        &self,
        version: PayloadVersion,
        payload: E::Payload,
    ) -> Result<E::PayloadReply, ProxyError<E::Error>> {
        self.execute(|| self.engine.new_payload(version, payload)).await
    }

    /// Updates head/safe/finalized without proposer attributes.
    pub async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: E::Forkchoice,
        custody: Option<[u8; 16]>,
    ) -> Result<E::ForkchoiceReply, ProxyError<E::Error>> {
        if custody.is_some() &&
            matches!(
                version,
                ForkchoiceVersion::V1 | ForkchoiceVersion::V2 | ForkchoiceVersion::V3
            )
        {
            return Err(ProxyError::CustodyVersion);
        }
        self.execute(|| self.engine.forkchoice_updated(version, state, custody)).await
    }

    /// Closes admission. An admitted operation may finish.
    pub fn close(&self) {
        self.admission.close();
        self.queue.close();
    }

    /// Whether uncertainty, cancellation or explicit shutdown has closed actual admission.
    /// Reopening requires a newly captured and reconciled engine proxy, never a flag override.
    pub fn is_closed(&self) -> bool {
        self.admission.is_closed() || self.queue.is_closed()
    }

    /// Returns monotonic transport statistics.
    pub fn stats(&self) -> ProxyStats {
        ProxyStats {
            calls: self.metrics.calls.load(Ordering::Relaxed),
            failures: self.metrics.failures.load(Ordering::Relaxed),
            timeouts: self.metrics.timeouts.load(Ordering::Relaxed),
            elapsed_nanos: self.metrics.elapsed_nanos.load(Ordering::Relaxed),
        }
    }

    async fn execute<T, F: Future<Output = Result<T, E::Error>>>(
        &self,
        call: impl FnOnce() -> F,
    ) -> Result<T, ProxyError<E::Error>> {
        let _queue_permit = self.queue.try_acquire().map_err(|error| match error {
            TryAcquireError::Closed => ProxyError::Closed,
            TryAcquireError::NoPermits => ProxyError::QueueFull,
        })?;
        let start = Instant::now();
        self.metrics.calls.fetch_add(1, Ordering::Relaxed);
        let result = tokio::time::timeout(self.timeout, async {
            let _permit = self.admission.acquire().await.map_err(|_| ProxyError::Closed)?;
            let mut cancellation = CancellationGuard {
                admission: self.admission.clone(),
                queue: self.queue.clone(),
                armed: true,
            };
            let response = call().await;
            cancellation.armed = false;
            response.map_err(ProxyError::Engine)
        })
        .await;
        self.metrics.elapsed_nanos.fetch_add(
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        match result {
            Ok(result) => {
                if result.is_err() {
                    self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
            Err(_) => {
                // A timed-out admitted engine call may still execute. Halt admission until
                // restart/reconciliation.
                self.admission.close();
                self.queue.close();
                self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                Err(ProxyError::Timeout)
            }
        }
    }
}

#[derive(Debug)]
struct CancellationGuard {
    admission: Arc<Semaphore>,
    queue: Arc<Semaphore>,
    armed: bool,
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        // Dropping an admitted future does not cancel a request already queued inside execution.
        if self.armed {
            self.admission.close();
            self.queue.close();
        }
    }
}

/// Typed execution transport. Version validation is mandatory for implementations.
pub trait ExecutionEngine: Send + Sync {
    /// Owned execution payload.
    type Payload: Send;
    /// Owned head, safe and finalized hashes.
    type Forkchoice: Send;
    /// Complete payload status.
    type PayloadReply: Send;
    /// Complete forkchoice result.
    type ForkchoiceReply: Send;
    /// Original engine error.
    type Error: Send;

    /// Validates method-version fields, then submits to execution.
    fn new_payload(
        &self,
        version: PayloadVersion,
        payload: Self::Payload,
    ) -> impl Future<Output = Result<Self::PayloadReply, Self::Error>> + Send;
    /// Preserves custody side effects and sends no proposer attributes.
    fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: Self::Forkchoice,
        custody: Option<[u8; 16]>,
    ) -> impl Future<Output = Result<Self::ForkchoiceReply, Self::Error>> + Send;
}

/// Explicit payload method versions; consensus fork names must not infer EL versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadVersion {
    /// Post-merge payload.
    V1,
    /// Withdrawals.
    V2,
    /// Blob fields.
    V3,
    /// Execution requests.
    V4,
    /// Slot-aware payload.
    V5,
    /// Extended response.
    V6,
}

/// Explicit forkchoice method versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkchoiceVersion {
    /// Original method.
    V1,
    /// Withdrawals.
    V2,
    /// Blob-aware attributes.
    V3,
    /// Slot and custody.
    V4,
    /// Extended response.
    V5,
}

/// Failures that must not be interpreted as execution validity.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError<E> {
    /// The bounded waiting queue is full.
    #[error("execution admission queue full")]
    QueueFull,
    /// Custody has no field in the selected historical method.
    #[error("custody requires forkchoice method V4 or later")]
    CustodyVersion,
    /// Invalid timeout.
    #[error("execution deadline must be positive")]
    ZeroTimeout,
    /// Shutdown or unknown prior execution outcome.
    #[error("execution admission closed")]
    Closed,
    /// Unknown execution outcome; no further imports are admitted.
    #[error("execution timed out; restart and reconcile before committing consensus state")]
    Timeout,
    /// Original engine error.
    #[error("execution API error: {0}")]
    Engine(E),
}

/// Measurements include admission wait.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProxyStats {
    /// Number of calls.
    pub calls: u64,
    /// Engine errors and closed admission attempts.
    pub failures: u64,
    /// Deadline expirations.
    pub timeouts: u64,
    /// Total nanoseconds elapsed.
    pub elapsed_nanos: u64,
}

#[derive(Debug, Default)]
struct Metrics {
    calls: AtomicU64,
    failures: AtomicU64,
    timeouts: AtomicU64,
    elapsed_nanos: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Clone, Default)]
    struct MockEngine(Arc<Mutex<Vec<u64>>>);

    impl ExecutionEngine for MockEngine {
        type Payload = u64;
        type Forkchoice = u64;
        type PayloadReply = u64;
        type ForkchoiceReply = (u64, Option<[u8; 16]>);
        type Error = &'static str;

        fn new_payload(
            &self,
            _: PayloadVersion,
            payload: u64,
        ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            // This adapter deliberately dispatches synchronously when the future is constructed.
            let result = if payload == 0 {
                Err("INVALID")
            } else {
                self.0.lock().unwrap().push(payload);
                Ok(payload)
            };
            std::future::ready(result)
        }
        async fn forkchoice_updated(
            &self,
            _: ForkchoiceVersion,
            state: u64,
            custody: Option<[u8; 16]>,
        ) -> Result<Self::ForkchoiceReply, Self::Error> {
            self.0.lock().unwrap().push(state);
            Ok((state, custody))
        }
    }

    #[tokio::test]
    async fn preserves_verdict_errors_and_custody() {
        let proxy = ExecutionProxy::new(MockEngine::default(), Duration::from_secs(1)).unwrap();
        assert_eq!(proxy.new_payload(PayloadVersion::V4, 5).await.unwrap(), 5);
        assert_eq!(
            proxy.forkchoice_updated(ForkchoiceVersion::V4, 5, Some([1; 16])).await.unwrap(),
            (5, Some([1; 16]))
        );
        assert!(matches!(
            proxy.new_payload(PayloadVersion::V4, 0).await,
            Err(ProxyError::Engine("INVALID"))
        ));
        assert_eq!(proxy.stats().calls, 3);
        assert_eq!(proxy.stats().failures, 1);
        proxy.close();
        assert!(matches!(proxy.new_payload(PayloadVersion::V4, 9).await, Err(ProxyError::Closed)));
    }

    #[derive(Debug, Clone)]
    struct HangingEngine(Arc<tokio::sync::Notify>);

    impl ExecutionEngine for HangingEngine {
        type Payload = u64;
        type Forkchoice = u64;
        type PayloadReply = u64;
        type ForkchoiceReply = u64;
        type Error = &'static str;
        async fn new_payload(&self, _: PayloadVersion, _: u64) -> Result<u64, Self::Error> {
            self.0.notify_one();
            std::future::pending().await
        }
        async fn forkchoice_updated(
            &self,
            _: ForkchoiceVersion,
            state: u64,
            _: Option<[u8; 16]>,
        ) -> Result<u64, Self::Error> {
            Ok(state)
        }
    }

    #[tokio::test]
    async fn aborting_an_admitted_call_closes_future_admission() {
        let started = Arc::new(tokio::sync::Notify::new());
        let proxy =
            ExecutionProxy::new(HangingEngine(started.clone()), Duration::from_secs(30)).unwrap();
        let other = proxy.clone();
        let task = tokio::spawn(async move { other.new_payload(PayloadVersion::V4, 1).await });
        started.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(proxy.new_payload(PayloadVersion::V4, 2).await, Err(ProxyError::Closed)));
    }

    #[tokio::test]
    async fn historical_methods_reject_custody_without_dispatch() {
        let proxy = ExecutionProxy::new(MockEngine::default(), Duration::from_secs(1)).unwrap();
        assert!(matches!(
            proxy.forkchoice_updated(ForkchoiceVersion::V3, 2, Some([1; 16])).await,
            Err(ProxyError::CustodyVersion)
        ));
        assert!(proxy.engine.0.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_closes_proxy_and_never_submits_queued_payload() {
        let proxy = ExecutionProxy::new(MockEngine::default(), Duration::from_secs(1)).unwrap();
        let permit = proxy.admission.acquire().await.unwrap();
        assert!(matches!(proxy.new_payload(PayloadVersion::V4, 9).await, Err(ProxyError::Timeout)));
        assert!(proxy.engine.0.lock().unwrap().is_empty());
        assert_eq!(proxy.stats().timeouts, 1);
        drop(permit);
        assert!(matches!(proxy.new_payload(PayloadVersion::V4, 10).await, Err(ProxyError::Closed)));
    }
}
