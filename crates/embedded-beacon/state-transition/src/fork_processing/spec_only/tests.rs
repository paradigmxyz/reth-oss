//! Isolation and exact post-root regressions for artificial fixture replay.
use super::*;
use crate::ssz::gloas;
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

#[derive(Debug, thiserror::Error)]
#[error("test engine rejects payload authority")]
struct FixtureError;
struct RejectEveryEngine;
macro_rules! rejecting {
    ($module:ident, $schema:ident) => {
        impl crate::$module::ExecutionEngine for RejectEveryEngine {
            type Error = FixtureError;
            async fn verify_terminal_block(
                &self,
                _: &crate::ssz::$schema::ExecutionPayload,
                _: crate::$module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(FixtureError)
            }
            async fn verify_payload(
                &self,
                _: &crate::ssz::$schema::ExecutionPayload,
                _: crate::$module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(FixtureError)
            }
        }
    };
}
rejecting!(bellatrix_processing, bellatrix);
rejecting!(capella_processing, capella);
rejecting!(deneb_processing, deneb);
rejecting!(electra_processing, electra);
rejecting!(fulu_processing, fulu);
impl crate::gloas_processing::ExecutionEngine for RejectEveryEngine {
    type Error = FixtureError;
    async fn verify_payload(
        &self,
        _: &crate::ssz::gloas::ExecutionPayload,
        _: crate::gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(FixtureError)
    }
}
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture test unexpectedly suspended"),
    }
}
#[test]
fn artificial_signed_dispatch_cannot_relax_production_or_post_root_checks() {
    let (state, _, params) = crate::gloas_processing::blocks::tests::state_and_keys();
    let fork_epoch = state.fork.epoch;
    let mut schedule = TransitionParameters::for_preset(Preset::Minimal).schedule;
    for entry in &mut schedule {
        entry.epoch = if entry.fork == Fork::Gloas { fork_epoch } else { 0 };
    }
    let parameters = TransitionParameters::new(params, schedule).unwrap();
    let original = BeaconState::Gloas(state, Preset::Minimal);
    let mut candidate = original.clone();
    process_slots(&mut candidate, original.slot() + 1, &parameters).unwrap();
    let value = match &mut candidate {
        BeaconState::Gloas(value, _) => value,
        _ => unreachable!(),
    };
    let constants = crate::gloas_processing::checked_constants(value, &parameters.gloas).unwrap();
    let body = crate::gloas_processing::blocks::tests::body(value, &parameters.gloas);
    let mut message = gloas::BeaconBlock {
        slot: value.slot,
        proposer_index: crate::gloas_processing::get_beacon_proposer_index(
            value,
            &parameters.gloas,
        )
        .unwrap(),
        parent_root: value.latest_block_header.hash_tree_root(),
        state_root: [0; 32],
        body,
    };
    crate::gloas_processing::blocks::artificial::process_block(
        value,
        &message,
        &parameters.gloas,
        &constants,
    )
    .unwrap();
    message.state_root = value.hash_tree_root(Preset::Minimal).unwrap();
    let signed = SignedBeaconBlock::Gloas(
        gloas::SignedBeaconBlock { message, signature: [0; 96] },
        Preset::Minimal,
    );
    let mut production = original.clone();
    assert!(ready(super::super::state_transition(
        &mut production,
        &signed,
        &parameters,
        &RejectEveryEngine
    ))
    .is_err());
    assert_eq!(production, original);
    let mut artificial = original.clone();
    ready(state_transition(&mut artificial, &signed, &parameters, &RejectEveryEngine)).unwrap();
    assert_eq!(artificial, candidate);
    let mut wrong_root = signed;
    if let SignedBeaconBlock::Gloas(value, _) = &mut wrong_root {
        value.message.state_root = [55; 32];
    }
    let mut artificial = original.clone();
    assert!(ready(state_transition(&mut artificial, &wrong_root, &parameters, &RejectEveryEngine))
        .is_err());
    assert_eq!(artificial, original);
    assert!(!crate::supports_full_transition(Fork::Gloas));
}
