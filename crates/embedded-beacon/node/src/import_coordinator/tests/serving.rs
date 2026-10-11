//! Genuine retained competing blocks serve by root, while ranges follow actual graph selection.

use super::*;
use crate::import_coordinator::{ServingBatch, ServingRequest};
use reth_embedded_beacon_network::block_rpc::BlockRequest;
use reth_embedded_beacon_state_transition::phase0_processing;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn originals_on_competing_branches_never_turn_root_inventory_into_canonical_ranges() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let mut competing = signed.clone();
    competing.message.body.graffiti = [44; 32];
    let parent = store.block_state(store.get_head().unwrap().root).unwrap();
    let BeaconState::Phase0(mut state, preset) =
        fork_processing::replay_slots(parent, 1, &parameters, 1).unwrap()
    else {
        panic!("genuine Phase0 parent")
    };
    phase0_processing::process_block(
        &mut state,
        &competing.message,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    competing.message.state_root = state.hash_tree_root(preset).unwrap();
    competing.signature = keys[competing.message.proposer_index as usize]
        .sign(
            &signing_root(
                competing.message.hash_tree_root(preset).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let (setup, _) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        let a = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let b = WireBlock::from_ssz(competing.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        assert_ne!(a.root(), b.root());
        worker.import(Input::Block(a.clone()), 0).unwrap();
        worker.import(Input::Block(b.clone()), 0).unwrap();
        let selected = worker.graph.snapshot().unwrap();
        let Some(ServingBatch::Blocks(roots)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![b.root(), a.root(), [199; 32]])))
            .unwrap()
        else {
            panic!("actual signed roots")
        };
        assert_eq!(roots, vec![b, a.clone()]);
        let Some(ServingBatch::Blocks(range)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Range { start_slot: 1, count: 1 }))
            .unwrap()
        else {
            panic!("actual canonical range")
        };
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].root(), selected.head.root);
        assert_eq!(
            range[0].ssz().as_ref(),
            worker.repository.original_block(selected.head.root).unwrap().unwrap().1.as_ref()
        );
        assert_eq!(worker.graph.snapshot().unwrap(), selected);
        assert!(worker
            .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![a.root(); 17])))
            .unwrap()
            .is_none());
        let Some(ServingBatch::Blocks(anchor)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![a.parent_root()])))
            .unwrap()
        else {
            panic!("unsigned anchor is known, signed original absent")
        };
        assert!(anchor.is_empty());
        assert_eq!(worker.repository.stats().unwrap().retained_states, 3);
    })
    .await
    .unwrap();
}
