//! Owned transaction parity, mandatory callback rejection and cancelled public import rollback.

use super::*;
use crate::ssz::{self, gloas};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

struct Engine {
    allowed: bool,
    pending: bool,
    calls: AtomicUsize,
}

impl Engine {
    fn new(allowed: bool, pending: bool) -> Self {
        Self { allowed, pending, calls: AtomicUsize::new(0) }
    }
}

macro_rules! strict_engine {
    ($module:ident, $schema:ident) => {
        impl $module::ExecutionEngine for Engine {
            type Error = std::io::Error;
            async fn verify_terminal_block(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(std::io::Error::other("terminal callback outside this post-merge fixture"))
            }
            async fn verify_payload(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.pending {
                    std::future::pending::<()>().await;
                }
                if self.allowed {
                    Ok(())
                } else {
                    Err(std::io::Error::other("exact fixture execution rejection"))
                }
            }
        }
    };
}
strict_engine!(bellatrix_processing, bellatrix);
strict_engine!(capella_processing, capella);
strict_engine!(deneb_processing, deneb);
strict_engine!(electra_processing, electra);
strict_engine!(fulu_processing, fulu);

impl gloas_processing::ExecutionEngine for Engine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        _: &gloas::ExecutionPayload,
        _: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("beacon import cannot admit an execution envelope"))
    }
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("immediate fixture engine expected"),
    }
}

fn trusted(raw: gloas_processing::GloasParameters, gloas_epoch: u64) -> TransitionParameters {
    TransitionParameters::new(raw, parameters(0, gloas_epoch).schedule).unwrap()
}

#[test]
fn owned_fulu_matches_public_state_and_exact_rejections_without_publishing_partial_state() {
    let (pre, keys, fulu_parameters) = crate::fulu_processing::tests::state_and_keys();
    let (block, expected) = crate::fulu_processing::tests::candidate(&pre, &keys, &fulu_parameters);
    let mut raw = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    raw.fulu = fulu_parameters;
    let parameters = trusted(raw, u64::MAX);
    let pre = BeaconState::Fulu(pre, Preset::Minimal);
    let block = SignedBeaconBlock::Fulu(block, Preset::Minimal);
    let engine = Engine::new(true, false);
    let owned = ready(state_transition_owned(pre.clone(), &block, &parameters, &engine)).unwrap();
    let mut public = pre.clone();
    ready(state_transition(&mut public, &block, &parameters, &engine)).unwrap();
    assert_eq!(owned, BeaconState::Fulu(expected.clone(), Preset::Minimal));
    assert_eq!(public, owned);
    assert_eq!(engine.calls.load(Ordering::SeqCst), 2);

    for variant in 0..6 {
        let mut invalid = block.clone();
        let mut input = pre.clone();
        let mut configuration = parameters.clone();
        match variant {
            0 => {
                let SignedBeaconBlock::Fulu(value, _) = &mut invalid else { unreachable!() };
                value.signature = [0; 96];
            }
            1 => {
                let SignedBeaconBlock::Fulu(value, _) = &mut invalid else { unreachable!() };
                value.message.state_root[0] ^= 1;
                let root = compute_signing_root(
                    value.message.hash_tree_root(Preset::Minimal).unwrap(),
                    compute_domain(
                        [0; 4],
                        expected.fork.current_version,
                        expected.genesis_validators_root,
                    ),
                );
                value.signature = keys[value.message.proposer_index as usize]
                    .sign(&root, ETHEREUM_BLS_DST, &[])
                    .to_bytes();
            }
            2 => {}
            3 => configuration.schedule[6].version = [99; 4],
            4 => {
                let BeaconState::Fulu(value, _) = &mut input else { unreachable!() };
                value.balances.values.pop();
            }
            _ => {
                let BeaconState::Fulu(value, _) = &mut input else { unreachable!() };
                value.finalized_checkpoint.epoch = u64::MAX;
                let SignedBeaconBlock::Fulu(value, _) = &mut invalid else { unreachable!() };
                value.message.slot = 16;
            }
        }
        let before = input.clone();
        let public_engine = Engine::new(variant != 2, false);
        let owned_engine = Engine::new(variant != 2, false);
        let public_error =
            ready(state_transition(&mut input, &invalid, &configuration, &public_engine))
                .unwrap_err();
        let owned_error =
            ready(state_transition_owned(before.clone(), &invalid, &configuration, &owned_engine))
                .unwrap_err();
        assert_eq!(format!("{public_error:?}"), format!("{owned_error:?}"));
        assert_eq!(input, before);
        assert_eq!(
            public_engine.calls.load(Ordering::SeqCst),
            owned_engine.calls.load(Ordering::SeqCst)
        );
        if variant == 1 || variant == 2 {
            assert_eq!(public_engine.calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(public_engine.calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn owned_gloas_native_and_dispatcher_preserve_signed_progressive_roots_and_errors() {
    let (pre, keys, raw) = crate::gloas_processing::blocks::tests::state_and_keys();
    let (block, expected) = crate::gloas_processing::blocks::tests::candidate(&pre, &keys, &raw);
    let parameters = trusted(raw.clone(), 0);
    let mut native = pre.clone();
    gloas_processing::state_transition(&mut native, &block, &raw).unwrap();
    let owned = gloas_processing::state_transition_owned(pre.clone(), &block, &raw).unwrap();
    assert_eq!(owned, expected);
    assert_eq!(native, owned);
    let signed = SignedBeaconBlock::Gloas(block.clone(), Preset::Minimal);
    let engine = Engine::new(false, false);
    let mut public = BeaconState::Gloas(pre.clone(), Preset::Minimal);
    ready(state_transition(&mut public, &signed, &parameters, &engine)).unwrap();
    let dispatched = ready(state_transition_owned(
        BeaconState::Gloas(pre.clone(), Preset::Minimal),
        &signed,
        &parameters,
        &engine,
    ))
    .unwrap();
    assert_eq!(public, dispatched);
    assert_eq!(public, BeaconState::Gloas(expected, Preset::Minimal));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    for late in [false, true] {
        let mut invalid = block.clone();
        if late {
            invalid.message.state_root[0] ^= 1;
            let root = compute_signing_root(
                invalid.message.hash_tree_root(Preset::Minimal).unwrap(),
                compute_domain([0; 4], pre.fork.current_version, pre.genesis_validators_root),
            );
            invalid.signature = keys[invalid.message.proposer_index as usize]
                .sign(&root, ETHEREUM_BLS_DST, &[])
                .to_bytes();
        } else {
            invalid.signature = [0; 96];
        }
        let mut public = pre.clone();
        let public_error =
            gloas_processing::state_transition(&mut public, &invalid, &raw).unwrap_err();
        let owned_error =
            gloas_processing::state_transition_owned(pre.clone(), &invalid, &raw).unwrap_err();
        assert_eq!(format!("{public_error:?}"), format!("{owned_error:?}"));
        assert_eq!(public, pre);
        let signed = SignedBeaconBlock::Gloas(invalid, Preset::Minimal);
        let before = BeaconState::Gloas(pre.clone(), Preset::Minimal);
        let mut public = before.clone();
        let public_error =
            ready(state_transition(&mut public, &signed, &parameters, &engine)).unwrap_err();
        let owned_error =
            ready(state_transition_owned(before.clone(), &signed, &parameters, &engine))
                .unwrap_err();
        assert_eq!(format!("{public_error:?}"), format!("{owned_error:?}"));
        assert_eq!(public, before);
    }
}

#[test]
fn owned_dispatcher_crosses_the_real_fulu_gloas_boundary_before_signed_import() {
    let (mut fulu, keys, fulu_parameters) = crate::fulu_processing::tests::state_and_keys();
    let mut raw = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    raw.fulu = fulu_parameters;
    let parameters = trusted(raw, 1);
    fulu_processing::process_slots(&mut fulu, 7, &parameters.gloas.fulu).unwrap();
    let before = BeaconState::Fulu(fulu, Preset::Minimal);
    let mut boundary = before.clone();
    process_slots(&mut boundary, 8, &parameters).unwrap();
    let BeaconState::Gloas(boundary, _) = boundary else {
        panic!("scheduled Gloas boundary expected")
    };
    let (block, expected) =
        crate::gloas_processing::blocks::tests::candidate(&boundary, &keys, &parameters.gloas);
    let block = SignedBeaconBlock::Gloas(block, Preset::Minimal);
    let engine = Engine::new(false, false);
    let mut public = before.clone();
    ready(state_transition(&mut public, &block, &parameters, &engine)).unwrap();
    let owned = ready(state_transition_owned(before, &block, &parameters, &engine)).unwrap();
    assert_eq!(owned, public);
    assert_eq!(owned, BeaconState::Gloas(expected, Preset::Minimal));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn cancelled_engine_await_leaves_public_parent_unchanged_and_returns_no_owned_state() {
    let (pre, keys, fulu_parameters) = crate::fulu_processing::tests::state_and_keys();
    let (block, _) = crate::fulu_processing::tests::candidate(&pre, &keys, &fulu_parameters);
    let mut raw = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    raw.fulu = fulu_parameters;
    let parameters = trusted(raw, u64::MAX);
    let before = BeaconState::Fulu(pre, Preset::Minimal);
    let block = SignedBeaconBlock::Fulu(block, Preset::Minimal);
    let engine = Engine::new(true, true);
    let mut public = before.clone();
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(state_transition(&mut public, &block, &parameters, &engine));
    assert!(future.as_mut().poll(&mut context).is_pending());
    drop(future);
    assert_eq!(public, before);
    let mut future = Box::pin(state_transition_owned(before.clone(), &block, &parameters, &engine));
    assert!(future.as_mut().poll(&mut context).is_pending());
    drop(future);
    assert_eq!(public, before);
    assert_eq!(engine.calls.load(Ordering::SeqCst), 2);
}
