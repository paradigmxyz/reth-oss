//! Shared strict gossip test anchors built from signed deposits and real fork upgrades.

use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_fork_choice::{gloas::GloasStore, Store, StoreParameters};
use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root, Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::{self, TransitionParameters},
    gloas_processing,
    phase0_processing::{
        self,
        genesis::{initialize_beacon_state_from_eth1, is_valid_genesis_state, GenesisParameters},
    },
    ssz::{
        altair, bellatrix, capella, deneb, electra, fulu, gloas, phase0, BeaconBlock, BeaconState,
        Preset, ProgressiveList, SignedBeaconBlock, Ssz,
    },
};

/// Build a real-key Minimal genesis and attach its exact selected-fork schedule.
pub(crate) fn store_and_keys(fork: Fork) -> (Store, Vec<SecretKey>, TransitionParameters) {
    assert!(fork < Fork::Gloas, "use the dedicated Gloas graph helper");
    let (mut state, keys, parameters) = state_and_keys(fork);
    let block = anchor_block(&mut state);
    let mut store = Store::get_store(state, block, StoreParameters::for_preset(Preset::Minimal))
        .expect("complete trusted test anchor");
    store.set_transition_parameters(parameters.clone()).expect("exact trusted test schedule");
    (store, keys, parameters)
}

/// Build the actual Gloas graph from the same signed genesis and upgrade sequence.
pub(crate) fn gloas_store_and_keys() -> (GloasStore, Vec<SecretKey>, TransitionParameters) {
    let (mut state, keys, parameters) = state_and_keys(Fork::Gloas);
    let block = anchor_block(&mut state);
    let (BeaconState::Gloas(state, _), BeaconBlock::Gloas(block, _)) = (state, block) else {
        panic!("selected Gloas genesis schema");
    };
    let store = GloasStore::get_store(
        state,
        block,
        StoreParameters::for_preset(Preset::Minimal),
        parameters.gloas.clone(),
    )
    .expect("complete trusted Gloas test anchor");
    (store, keys, parameters)
}

/// Compute the exact Ethereum BLS domain and object signing root.
pub(crate) fn signing_root(
    object: Root,
    domain_type: [u8; 4],
    fork_version: [u8; 4],
    genesis_validators_root: Root,
) -> Root {
    compute_signing_root(object, compute_domain(domain_type, fork_version, genesis_validators_root))
}

/// Build an actual signed Phase0 child with real RANDAO and complete committed post-state.
pub(crate) fn phase0_signed_child(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> phase0::SignedBeaconBlock {
    let head = store.get_head().expect("real retained head");
    let parent = store.block_state(head.root).expect("real retained state");
    let slot = parent.slot().checked_add(1).expect("bounded unit slot");
    let BeaconState::Phase0(mut state, preset) =
        fork_processing::replay_slots(parent, slot, parameters, 1).expect("real slot replay")
    else {
        panic!("selected Phase0 helper");
    };
    let processing = &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0;
    let proposer = phase0_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[usize::try_from(proposer).unwrap()];
    let epoch = slot / processing.slots_per_epoch();
    let version = if epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    let mut message = phase0::BeaconBlock::default_value(preset).unwrap();
    message.slot = slot;
    message.proposer_index = proposer;
    message.parent_root = head.root;
    message.body.eth1_data = state.eth1_data.clone();
    message.body.randao_reveal = key
        .sign(
            &signing_root(uint64_root(epoch), [2, 0, 0, 0], version, state.genesis_validators_root),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    phase0_processing::process_block(&mut state, &message, processing)
        .expect("mandatory real RANDAO and full body processing");
    message.state_root = state.hash_tree_root(preset).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(preset).unwrap(),
                [0; 4],
                version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    phase0::SignedBeaconBlock { message, signature }
}

/// Build a genuine Fulu anchor and the signed first Gloas block under an epoch-one upgrade.
/// The block is generated with real public processing and BLS; callers independently admit it.
pub(crate) fn fulu_gloas_boundary(
) -> (Store, Vec<SecretKey>, TransitionParameters, SignedBeaconBlock) {
    classic_gloas_boundary(Fork::Fulu)
}

/// Preserve a genuine pre-execution Phase0 history through all configured epoch-one upgrades.
pub(crate) fn phase0_gloas_boundary(
) -> (Store, Vec<SecretKey>, TransitionParameters, SignedBeaconBlock) {
    classic_gloas_boundary(Fork::Phase0)
}

fn classic_gloas_boundary(
    selected: Fork,
) -> (Store, Vec<SecretKey>, TransitionParameters, SignedBeaconBlock) {
    let (original, keys, original_parameters) = store_and_keys(selected);
    let mut schedule = original_parameters.schedule.clone();
    for activation in &mut schedule {
        if activation.fork > selected {
            activation.epoch = 1;
        }
    }
    let parameters = TransitionParameters::new(original_parameters.gloas, schedule).unwrap();
    let root = original.get_head().unwrap().root;
    let mut store = Store::get_store(
        original.block_state(root).unwrap().clone(),
        original.block(root).unwrap().clone(),
        StoreParameters::for_preset(Preset::Minimal),
    )
    .unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    let signed = classic_to_gloas_child(&store, &keys, &parameters);
    (store, keys, parameters, signed)
}

/// Produce a genuine signed first Gloas block from the supplied original classic graph.
/// Complete configured slot upgrades and all native body processing preserve its exact origin.
pub(crate) fn classic_to_gloas_child(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> SignedBeaconBlock {
    classic_to_gloas_child_with_commitments(store, keys, parameters, Vec::new())
}

/// Include real blob commitments before native post-state processing and block signing.
pub(crate) fn classic_to_gloas_child_with_commitments(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    commitments: Vec<[u8; 48]>,
) -> SignedBeaconBlock {
    let root = store.get_head().unwrap().root;
    let slot = parameters.slots_per_epoch();
    let BeaconState::Gloas(mut state, preset) =
        fork_processing::replay_slots(store.block_state(root).unwrap(), slot, parameters, slot)
            .unwrap()
    else {
        panic!("real configured Gloas upgrade");
    };
    let processing = &parameters.gloas;
    let proposer = gloas_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[usize::try_from(proposer).unwrap()];
    let epoch = slot / processing.slots_per_epoch();
    let version = state.fork.current_version;
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let requests = gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    };
    let mut body = gloas::BeaconBlockBody::default_value(preset).unwrap();
    body.eth1_data = state.eth1_data.clone();
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: state.latest_block_hash,
        parent_block_root: root,
        block_hash: [9; 32],
        prev_randao: state.randao_mixes.values[(epoch % 64) as usize],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::from_vec(commitments),
        execution_requests_root: requests.hash_tree_root(preset).unwrap(),
    };
    body.randao_reveal = key
        .sign(
            &signing_root(uint64_root(epoch), [2, 0, 0, 0], version, state.genesis_validators_root),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = gloas::BeaconBlock {
        slot,
        proposer_index: proposer,
        parent_root: root,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    gloas_processing::process_parent_execution_payload(&mut state, &message, processing).unwrap();
    gloas_processing::process_block_header(&mut state, &message, processing).unwrap();
    gloas_processing::process_withdrawals(&mut state, processing).unwrap();
    gloas_processing::process_execution_payload_bid(
        &mut state,
        &message.body.signed_execution_payload_bid,
        processing,
    )
    .unwrap();
    gloas_processing::process_randao(&mut state, &message.body, processing).unwrap();
    gloas_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    gloas_processing::process_operations(&mut state, &message.body, parent_slot, processing)
        .unwrap();
    gloas_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, processing)
        .unwrap();
    message.state_root = state.hash_tree_root(preset).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(preset).unwrap(),
                [0; 4],
                version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    SignedBeaconBlock::Gloas(gloas::SignedBeaconBlock { message, signature }, preset)
}

fn state_and_keys(fork: Fork) -> (BeaconState, Vec<SecretKey>, TransitionParameters) {
    let keys = (1u8..=32)
        .map(|index| SecretKey::key_gen(&[index; 32], &[]).expect("deterministic valid BLS key"))
        .collect::<Vec<_>>();
    let mut genesis = GenesisParameters::for_preset(Preset::Minimal);
    genesis.genesis_delay = 0;
    genesis.min_genesis_time = 0;
    genesis.min_genesis_active_validator_count = 32;
    let deposits = deposits(&keys, genesis.phase0.genesis_fork_version);
    let state = initialize_beacon_state_from_eth1([7; 32], 0, &deposits, &genesis)
        .expect("real deposit proofs and mandatory proof-of-possession signatures");
    assert!(is_valid_genesis_state(&state, &genesis).expect("valid genesis policy"));
    assert_eq!(state.validators.values.len(), keys.len());
    let defaults = TransitionParameters::for_preset(Preset::Minimal);
    let schedule = defaults
        .schedule
        .into_iter()
        .map(|mut activation| {
            activation.epoch = if activation.fork <= fork { 0 } else { u64::MAX };
            activation
        })
        .collect();
    let parameters = TransitionParameters::new(defaults.gloas, schedule)
        .expect("ordered selected-fork genesis schedule");
    let mut state = BeaconState::Phase0(state, Preset::Minimal);
    fork_processing::upgrade_state_at_slot(&mut state, &parameters)
        .expect("production consecutive genesis fork upgrades");
    assert_eq!(state.schema_fork(), fork);
    (state, keys, parameters)
}

fn deposits(keys: &[SecretKey], version: [u8; 4]) -> Vec<phase0::Deposit> {
    let mut leaves = Vec::new();
    keys.iter()
        .map(|key| {
            let pubkey = key.sk_to_pk().to_bytes();
            let mut withdrawal_credentials = reth_embedded_beacon_primitives::sha256(&pubkey);
            withdrawal_credentials[0] = 0;
            let mut data = phase0::DepositData {
                pubkey,
                withdrawal_credentials,
                amount: 32_000_000_000,
                signature: [0; 96],
            };
            let message = merkleize(
                &[
                    bytes_root(&data.pubkey, 48, false).expect("complete BLS key root"),
                    data.withdrawal_credentials,
                    uint64_root(data.amount),
                ],
                3,
            )
            .expect("deposit message root");
            data.signature = key
                .sign(&signing_root(message, [3, 0, 0, 0], version, [0; 32]), ETHEREUM_BLS_DST, &[])
                .to_bytes();
            let index = leaves.len();
            leaves.push(data.hash_tree_root(Preset::Minimal).expect("complete signed deposit"));
            let mut proof = Vec::with_capacity(33);
            for height in 0..32 {
                let width = 1usize << height;
                let start = ((index >> height) ^ 1) << height;
                let sibling = if start < leaves.len() {
                    &leaves[start..(start + width).min(leaves.len())]
                } else {
                    &[]
                };
                proof.push(merkleize(sibling, width).expect("exact prefix deposit proof"));
            }
            proof.push(uint64_root(leaves.len() as u64));
            phase0::Deposit { proof: phase0::DepositProof::from_vec(proof), data }
        })
        .collect()
}

fn anchor_block(state: &mut BeaconState) -> BeaconBlock {
    let preset = Preset::Minimal;
    // The trusted test anchor is constructed after the genuine upgrades. Its typed empty
    // body must also be bound in the latest header before hashing the complete anchor state.
    match state {
        BeaconState::Phase0(value, _) => {
            let mut block = phase0::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Phase0(block, preset)
        }
        BeaconState::Altair(value, _) => {
            let mut block = altair::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Altair(block, preset)
        }
        BeaconState::Bellatrix(value, _) => {
            let mut block = bellatrix::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Bellatrix(block, preset)
        }
        BeaconState::Capella(value, _) => {
            let mut block = capella::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Capella(block, preset)
        }
        BeaconState::Deneb(value, _) => {
            let mut block = deneb::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Deneb(block, preset)
        }
        BeaconState::Electra(value, _) => {
            let mut block = electra::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Electra(block, preset)
        }
        BeaconState::Fulu(value, _) => {
            let mut block = fulu::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Fulu(block, preset)
        }
        BeaconState::Gloas(value, _) => {
            let mut block = gloas::BeaconBlock::default_value(preset).unwrap();
            block.body.eth1_data = value.eth1_data.clone();
            block.body.signed_execution_payload_bid.message =
                value.latest_execution_payload_bid.clone();
            value.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
            block.state_root = value.hash_tree_root(preset).unwrap();
            BeaconBlock::Gloas(block, preset)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_crypto::verify_signature;
    use reth_embedded_beacon_shared_data::{SharedDataStore, StoreIdentity};
    use reth_embedded_beacon_storage::{CompactStateLimits, CompactStateStore};
    use tempfile::TempDir;

    #[test]
    fn shared_factory_retains_complete_real_key_anchors_for_every_fork() {
        for fork in [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
        ] {
            let (store, keys, parameters) = store_and_keys(fork);
            let head = store.get_head().unwrap();
            let state = store.block_state(head.root).unwrap();
            let block = store.block(head.root).unwrap();
            assert_eq!(state.schema_fork(), fork);
            assert_eq!(state.slot(), 0);
            assert_eq!(state.genesis_time(), 0);
            assert_eq!(block.header().unwrap().state_root, state.hash_tree_root().unwrap());
            let mut normalized = state.latest_block_header();
            normalized.state_root = state.hash_tree_root().unwrap();
            assert_eq!(normalized.hash_tree_root(), head.root);
            assert_eq!(parameters.fork_at_slot(0).unwrap(), fork);
            assert_eq!(keys.len(), 32);
            let root = signing_root(
                [5; 32],
                [1, 0, 0, 0],
                state.fork_info().current_version,
                state.genesis_validators_root(),
            );
            let signature = keys[0].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
            verify_signature(&keys[0].sk_to_pk().to_bytes(), &root, &signature).unwrap();
        }
        let (store, keys, parameters) = gloas_store_and_keys();
        let head = store.get_head().unwrap();
        let state = store.block_state(head.root).unwrap();
        assert_eq!(state.validators.values.len(), 32);
        assert_eq!(keys.len(), 32);
        assert_eq!(parameters.fork_at_slot(0).unwrap(), Fork::Gloas);
        assert_eq!(state.fork.current_version, parameters.gloas.fork_version);
        assert_eq!(state.validators.values[0].pubkey, keys[0].sk_to_pk().to_bytes());
        assert!(state
            .validators
            .values
            .iter()
            .all(|validator| validator.activation_epoch == 0 && validator.exit_epoch == u64::MAX));
    }
    #[test]
    fn compact_persistence_roundtrips_genuine_signed_genesis_and_slot_replay_in_every_fork() {
        for fork in [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
            Fork::Gloas,
        ] {
            let (state, keys, parameters) = if fork == Fork::Gloas {
                let (store, keys, parameters) = gloas_store_and_keys();
                let head = store.get_head().unwrap();
                (
                    BeaconState::Gloas(
                        store.block_state(head.root).unwrap().clone(),
                        Preset::Minimal,
                    ),
                    keys,
                    parameters,
                )
            } else {
                let (store, keys, parameters) = store_and_keys(fork);
                let head = store.get_head().unwrap();
                (store.block_state(head.root).unwrap().clone(), keys, parameters)
            };
            assert_eq!(keys.len(), 32);
            let next = fork_processing::replay_slots(&state, 1, &parameters, 1).unwrap();
            let identity = StoreIdentity {
                genesis_root: state.genesis_validators_root(),
                config_digest: [fork as u8; 32],
            };
            let directory = TempDir::new().unwrap();
            let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
            let compact =
                CompactStateStore::open(shared.clone(), CompactStateLimits::default()).unwrap();
            let root = state.hash_tree_root().unwrap();
            let next_root = next.hash_tree_root().unwrap();
            compact.persist_state(&state, root).unwrap();
            compact.persist_state(&next, next_root).unwrap();
            drop(compact);
            drop(shared);
            let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
            let compact = CompactStateStore::open(shared, CompactStateLimits::default()).unwrap();
            assert_eq!(
                compact
                    .lease(root, fork, Preset::Minimal)
                    .unwrap()
                    .unwrap()
                    .state()
                    .to_ssz()
                    .unwrap(),
                state.to_ssz().unwrap()
            );
            assert_eq!(
                compact
                    .lease(next_root, fork, Preset::Minimal)
                    .unwrap()
                    .unwrap()
                    .state()
                    .to_ssz()
                    .unwrap(),
                next.to_ssz().unwrap()
            );
            assert_eq!(compact.stats().unwrap().retained_states, 2);
        }
    }
}
