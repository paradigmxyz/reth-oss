//! Bounded generalized-index proofs over complete declared SSZ container trees.

use crate::ssz::{BeaconState, Preset, Ssz};
use reth_embedded_beacon_primitives::{Root, SszError};

/// Read an exact committed node. Unsupported nested collection paths return an error.
pub fn tree_node<T: Ssz>(value: &T, gindex: u64, preset: Preset) -> Result<Root, SszError> {
    value.tree_node(gindex, preset)
}
/// Derive every sibling from the actual SSZ object, verifying the requested leaf exists.
pub fn compute_merkle_proof<T: Ssz>(
    value: &T,
    gindex: u64,
    preset: Preset,
) -> Result<Vec<Root>, SszError> {
    value.tree_node(gindex, preset)?;
    let mut index = gindex;
    let mut branch = Vec::with_capacity((63 - gindex.leading_zeros()) as usize);
    while index > 1 {
        branch.push(value.tree_node(index ^ 1, preset)?);
        index /= 2
    }
    Ok(branch)
}
/// Derive a real state proof under its own complete regular or progressive fork schema.
pub fn compute_state_merkle_proof(state: &BeaconState, gindex: u64) -> Result<Vec<Root>, SszError> {
    match state {
        BeaconState::Phase0(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Altair(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Bellatrix(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Capella(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Deneb(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Electra(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Fulu(value, preset) => compute_merkle_proof(value, gindex, *preset),
        BeaconState::Gloas(value, preset) => compute_merkle_proof(value, gindex, *preset),
    }
}
/// Read a node from an exact complete fork-aware state without projecting or copying it.
pub fn state_tree_node(state: &BeaconState, gindex: u64) -> Result<Root, SszError> {
    match state {
        BeaconState::Phase0(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Altair(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Bellatrix(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Capella(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Deneb(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Electra(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Fulu(value, preset) => value.tree_node(gindex, *preset),
        BeaconState::Gloas(value, preset) => value.tree_node(gindex, *preset),
    }
}
