//! Deterministic startup scheduling preserves real execution acknowledgement ownership.

use super::*;
use std::{cell::Cell, future::poll_fn, task::Poll};
use tokio::sync::oneshot;

#[test]
fn optimistic_admission_is_limited_to_payload_bearing_classic_anchors() {
    for fork in [Fork::Bellatrix, Fork::Capella, Fork::Deneb, Fork::Electra, Fork::Fulu] {
        assert_eq!(CheckpointAdmission::for_fork(fork), CheckpointAdmission::Optimistic);
    }
    for fork in [Fork::Phase0, Fork::Altair, Fork::Gloas] {
        assert_eq!(CheckpointAdmission::for_fork(fork), CheckpointAdmission::Strict);
    }
}

#[tokio::test]
async fn optimistic_owner_startup_does_not_poll_a_blocked_execution_reconciliation() {
    let (_acknowledge, acknowledged) = oneshot::channel::<u8>();
    let reconciliation_polled = Cell::new(false);
    let reconciliation = async {
        reconciliation_polled.set(true);
        Ok(acknowledged.await?)
    };
    let startup = CheckpointAdmission::Optimistic.receipt(reconciliation);
    tokio::pin!(startup);
    let Poll::Ready(result) = poll_fn(|cx| Poll::Ready(startup.as_mut().poll(cx))).await else {
        panic!("authenticated optimistic owner startup waited for blocked execution");
    };
    assert_eq!(result.unwrap(), None);
    assert!(!reconciliation_polled.get());
}

#[tokio::test]
async fn strict_owner_startup_waits_for_actual_execution_receipt() {
    let (acknowledge, acknowledged) = oneshot::channel::<u8>();
    let reconciliation_polled = Cell::new(false);
    let reconciliation = async {
        reconciliation_polled.set(true);
        Ok(acknowledged.await?)
    };
    let startup = CheckpointAdmission::Strict.receipt(reconciliation);
    tokio::pin!(startup);
    assert!(poll_fn(|cx| Poll::Ready(startup.as_mut().poll(cx))).await.is_pending());
    assert!(reconciliation_polled.get());
    acknowledge.send(7).unwrap();
    assert_eq!(startup.await.unwrap(), Some(7));
}

#[tokio::test]
async fn failed_strict_reconciliation_does_not_turn_into_optimistic_admission() {
    let error = CheckpointAdmission::Strict
        .receipt(std::future::ready(Err::<u8, _>(eyre::eyre!("execution rejected target"))))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "execution rejected target");
}
