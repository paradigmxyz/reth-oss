//! Real signed import samples exercise propagation atomicity independently of gossip fixtures.

use super::*;
use crate::Store;
use reth_embedded_beacon_state_transition::ssz::{phase0, BeaconBlock};
use std::path::Path;

fn store_and_blocks() -> (Store, SignedBeaconBlock, SignedBeaconBlock) {
    let preset = Preset::Minimal;
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/official-phase0-import");
    let state =
        phase0::BeaconState::from_ssz(&std::fs::read(base.join("genesis.ssz")).unwrap(), preset)
            .unwrap();
    let body = phase0::BeaconBlockBody::default_value(preset).unwrap();
    assert_eq!(body.hash_tree_root(preset).unwrap(), state.latest_block_header.body_root);
    let block = phase0::BeaconBlock {
        slot: state.slot,
        proposer_index: state.latest_block_header.proposer_index,
        parent_root: state.latest_block_header.parent_root,
        state_root: state.hash_tree_root(preset).unwrap(),
        body,
    };
    let mut store = Store::get_store(
        BeaconState::Phase0(state, preset),
        BeaconBlock::Phase0(block, preset),
        StoreParameters::for_preset(preset),
    )
    .unwrap();
    store.set_transition_parameters(TransitionParameters::for_preset(preset)).unwrap();
    let first = SignedBeaconBlock::Phase0(
        phase0::SignedBeaconBlock::from_ssz(
            &std::fs::read(base.join("blocks_0.ssz")).unwrap(),
            preset,
        )
        .unwrap(),
        preset,
    );
    let second = SignedBeaconBlock::Phase0(
        phase0::SignedBeaconBlock::from_ssz(
            &std::fs::read(base.join("blocks_1.ssz")).unwrap(),
            preset,
        )
        .unwrap(),
        preset,
    );
    (store, first, second)
}
fn time(store: &Store, signed: &SignedBeaconBlock) -> u64 {
    store.genesis_time_ms() + candidate(signed).slot * ReadGossipView::slot_duration_ms(store)
}

#[test]
fn real_signatures_parent_admission_and_capacity_do_not_mutate_graph_or_partial_seen() {
    let (mut store, first, second) = store_and_blocks();
    let original_head = store.get_head().unwrap();
    let original = store.block_state(original_head.root).unwrap().to_ssz().unwrap();
    let mut seen = SeenBeaconBlocks::new(&store, 1).unwrap();
    let mut invalid = first.clone();
    let SignedBeaconBlock::Phase0(block, _) = &mut invalid else { unreachable!() };
    block.signature = [0; 96];
    assert!(matches!(
        validate(&store, &mut seen, &invalid, time(&store, &first)),
        Err(GossipBlockError::Reject("invalid proposer signature"))
    ));
    assert_eq!(seen.entries(), 0);
    assert!(matches!(
        validate(&store, &mut seen, &second, time(&store, &second)),
        Err(GossipBlockError::Ignore("block's parent has not been seen"))
    ));
    assert_eq!(seen.entries(), 0);
    validate(&store, &mut seen, &first, time(&store, &first)).unwrap();
    let accepted = seen.clone();
    assert!(matches!(
        validate(&store, &mut seen, &invalid, time(&store, &first)),
        Err(GossipBlockError::Ignore(
            "block is not the first valid block for this slot and proposer"
        ))
    ));
    assert_eq!(seen, accepted);
    assert_eq!(store.get_head().unwrap(), original_head);
    assert_eq!(store.block_state(original_head.root).unwrap().to_ssz().unwrap(), original);
    store.on_tick(time(&store, &first)).unwrap();
    store.on_block_and_contents(&first).unwrap();
    let head = store.get_head().unwrap();
    let post = store.block_state(head.root).unwrap().to_ssz().unwrap();
    assert!(matches!(
        validate(&store, &mut seen, &second, time(&store, &second)),
        Err(GossipBlockError::Capacity)
    ));
    assert_eq!(seen, accepted);
    assert_eq!(store.get_head().unwrap(), head);
    assert_eq!(store.block_state(head.root).unwrap().to_ssz().unwrap(), post);
    seen.prune_before_slot(candidate(&second).slot);
    assert_eq!(seen.entries(), 0);
    validate(&store, &mut seen, &second, time(&store, &second)).unwrap();
}

#[test]
fn seen_context_rejects_another_genesis_and_scalar_clock_boundaries_are_exact() {
    let (store, first, _) = store_and_blocks();
    let mut seen = SeenBeaconBlocks::new(&store, 4).unwrap();
    let start = time(&store, &first);
    assert!(matches!(
        validate(&store, &mut seen, &first, start - 501),
        Err(GossipBlockError::Ignore("block is from a future slot"))
    ));
    assert_eq!(seen.entries(), 0);
    validate(&store, &mut seen, &first, start - 500).unwrap();
    let mut changed = seen.clone();
    changed.genesis_validators_root[0] ^= 1;
    assert!(matches!(
        validate(&store, &mut changed, &first, start),
        Err(GossipBlockError::Store(StoreError::InvalidParameters))
    ));
    assert_eq!(changed.entries(), 1);
    assert!(prefix(&seen, (u64::MAX, 0), u64::MAX, u64::MAX, u64::MAX, 0, 8).is_err());
}
