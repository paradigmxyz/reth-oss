//! Private typed payload feedback grants retry permission only, never execution validity.
//! Feedback is reset for each exact original input owned by the serialized import worker.

use alloy_rpc_types_engine::PayloadStatusEnum;
use reth_embedded_beacon_primitives::Root;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingPayloadStatus {
    Syncing,
    Accepted,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingPayload {
    pub(crate) hash: Root,
    pub(crate) status: PendingPayloadStatus,
}
#[derive(Debug, Default)]
struct Attempt {
    input: Option<Root>,
    feedback: Option<PendingPayload>,
    timing_owner: Arc<()>,
    execution_micros: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PendingFeedback(Arc<Mutex<Attempt>>);
impl PendingFeedback {
    pub(crate) fn begin(&self, input: Root) -> Result<(), ()> {
        let mut attempt = self.0.lock().map_err(|_| ())?;
        *attempt = Attempt { input: Some(input), ..Attempt::default() };
        Ok(())
    }

    pub(crate) fn observe(&self, status: &PayloadStatusEnum, hash: Root) {
        let pending = match status {
            PayloadStatusEnum::Syncing => Some(PendingPayloadStatus::Syncing),
            PayloadStatusEnum::Accepted => Some(PendingPayloadStatus::Accepted),
            _ => None,
        };
        if let Ok(mut attempt) = self.0.lock() {
            // Standalone adapters without a currently owned input cannot mint import feedback.
            attempt.feedback =
                attempt.input.and(pending).map(|status| PendingPayload { hash, status });
        }
    }

    pub(crate) fn take(&self, input: Root) -> Result<Option<PendingPayload>, ()> {
        let mut attempt = self.0.lock().map_err(|_| ())?;
        if attempt.input != Some(input) {
            return Ok(None)
        }
        Ok(attempt.feedback.take())
    }

    /// Measure only a payload API await belonging to the currently serialized import attempt.
    pub(crate) fn measure_execution(&self) -> Option<PayloadTimer> {
        let attempt = self.0.lock().ok()?;
        attempt.input?;
        Some(PayloadTimer {
            feedback: self.clone(),
            owner: attempt.timing_owner.clone(),
            started: Instant::now(),
        })
    }

    /// Read telemetry without granting retry, execution or consensus authority.
    #[cfg(any(test, all(feature = "peer-runtime", feature = "gossip-validation")))]
    pub(crate) fn execution_micros(&self, input: Root) -> Option<u64> {
        let attempt = self.0.lock().ok()?;
        (attempt.input == Some(input)).then_some(attempt.execution_micros)
    }
}

/// A late payload completion cannot charge a replacement import attempt.
#[derive(Debug)]
pub(crate) struct PayloadTimer {
    feedback: PendingFeedback,
    owner: Arc<()>,
    started: Instant,
}

impl Drop for PayloadTimer {
    fn drop(&mut self) {
        let micros = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        if let Ok(mut attempt) = self.feedback.0.lock() &&
            Arc::ptr_eq(&attempt.timing_owner, &self.owner)
        {
            attempt.execution_micros = attempt.execution_micros.saturating_add(micros);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_payload_timer_does_not_charge_replacement_attempt() {
        let feedback = PendingFeedback::default();
        assert!(feedback.measure_execution().is_none());
        feedback.begin([1; 32]).unwrap();
        let timer = feedback.measure_execution().unwrap();
        feedback.begin([1; 32]).unwrap();
        drop(timer);
        assert_eq!(feedback.execution_micros([1; 32]), Some(0));
        assert_eq!(feedback.execution_micros([2; 32]), None);
    }
}
