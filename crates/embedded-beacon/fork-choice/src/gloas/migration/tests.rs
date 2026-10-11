//! Genuine signed migration, execution-contract and finality regressions.
//! Signed deposit genesis, mandatory BLS and complete state processing are real. Controlled
//! Engine/empty-custody callbacks test exact contracts; they do not establish live EL validity.

use super::*;
use crate::fast_confirmation::{FastConfirmation, FastConfirmationView, Parameters, StateRef};
use blst::min_pk::{AggregateSignature, SecretKey};
use reth_embedded_beacon_crypto::{CryptoError, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root};
use reth_embedded_beacon_state_transition::{
    bellatrix_processing, capella_processing, deneb_processing, electra_processing,
    fork_processing::TransitionParameters,
    fulu_processing,
    phase0_processing::{
        self,
        genesis::{initialize_beacon_state_from_eth1, is_valid_genesis_state, GenesisParameters},
    },
    ssz::{
        phase0, BeaconBlock as AnyBlock, BeaconState as AnyState, List, ProgressiveList, Vector,
    },
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
};

struct UnavailableEngine(AtomicUsize);
macro_rules! unavailable_classic_engine {
    ($module:ident, $schema:ident) => {
        impl $module::ExecutionEngine for UnavailableEngine {
            type Error = std::io::Error;
            async fn verify_terminal_block(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(std::io::Error::other("test supplies no terminal proof"))
            }
            async fn verify_payload(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(std::io::Error::other("test supplies no execution validation"))
            }
        }
    };
}
unavailable_classic_engine!(bellatrix_processing, bellatrix);
unavailable_classic_engine!(capella_processing, capella);
unavailable_classic_engine!(deneb_processing, deneb);
unavailable_classic_engine!(electra_processing, electra);
unavailable_classic_engine!(fulu_processing, fulu);
impl gloas_processing::ExecutionEngine for UnavailableEngine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        _: &gloas::ExecutionPayload,
        _: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("test supplies no envelope validation"))
    }
}
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("no external callbacks are expected during beacon migration"),
    }
}
fn signing_root(object: Root, domain_type: [u8; 4], version: [u8; 4], genesis: Root) -> Root {
    compute_signing_root(object, compute_domain(domain_type, version, genesis))
}
fn real_anchor(selected: Fork) -> (Store, Vec<SecretKey>, TransitionParameters) {
    real_anchor_with_limits(selected, StoreParameters::for_preset(Preset::Minimal))
}
fn real_anchor_with_limits(
    selected: Fork,
    limits: StoreParameters,
) -> (Store, Vec<SecretKey>, TransitionParameters) {
    assert!(matches!(selected, Fork::Phase0 | Fork::Fulu));
    let keys =
        (1u8..=32).map(|index| SecretKey::key_gen(&[index; 32], &[]).unwrap()).collect::<Vec<_>>();
    let mut genesis = GenesisParameters::for_preset(Preset::Minimal);
    genesis.genesis_delay = 0;
    genesis.min_genesis_time = 0;
    genesis.min_genesis_active_validator_count = 32;
    let state = initialize_beacon_state_from_eth1(
        [7; 32],
        0,
        &deposits(&keys, genesis.phase0.genesis_fork_version),
        &genesis,
    )
    .unwrap();
    assert!(is_valid_genesis_state(&state, &genesis).unwrap());
    let defaults = TransitionParameters::for_preset(Preset::Minimal);
    let schedule = defaults
        .schedule
        .into_iter()
        .map(|mut activation| {
            activation.epoch = if activation.fork <= selected { 0 } else { 1 };
            activation
        })
        .collect();
    let parameters = TransitionParameters::new(defaults.gloas, schedule).unwrap();
    let mut state = AnyState::Phase0(state, Preset::Minimal);
    fork_processing::upgrade_state_at_slot(&mut state, &parameters).unwrap();
    let block = match &mut state {
        AnyState::Phase0(state, _) => {
            let mut block = phase0::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.body.eth1_data = state.eth1_data.clone();
            state.latest_block_header.body_root =
                block.body.hash_tree_root(Preset::Minimal).unwrap();
            block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
            AnyBlock::Phase0(block, Preset::Minimal)
        }
        AnyState::Fulu(state, _) => {
            let mut block = fulu::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.body.eth1_data = state.eth1_data.clone();
            state.latest_block_header.body_root =
                block.body.hash_tree_root(Preset::Minimal).unwrap();
            block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
            AnyBlock::Fulu(block, Preset::Minimal)
        }
        _ => panic!("requested exact selected historical anchor"),
    };
    let mut store = Store::get_store(state, block, limits).unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    (store, keys, parameters)
}
fn requests() -> gloas::ExecutionRequests {
    gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    }
}
fn signed_first(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> ssz::SignedBeaconBlock {
    signed_first_at(store, store.get_head().unwrap().root, keys, parameters, 8)
}
fn signed_first_at(
    store: &Store,
    parent: Root,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    target: u64,
) -> ssz::SignedBeaconBlock {
    let AnyState::Gloas(mut state, _) = fork_processing::replay_slots(
        store.block_state(parent).unwrap(),
        target,
        parameters,
        target,
    )
    .unwrap() else {
        panic!("exact real scheduled upgrade to Gloas")
    };
    let proposer = gloas_processing::get_beacon_proposer_index(&state, &parameters.gloas).unwrap();
    let epoch = state.slot / 8;
    let version = state.fork.current_version;
    let key = &keys[proposer as usize];
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: state.latest_block_hash,
        parent_block_root: parent,
        block_hash: [11; 32],
        prev_randao: state.randao_mixes.values[epoch as usize % 64],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot: state.slot,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests().hash_tree_root(Preset::Minimal).unwrap(),
    };
    body.randao_reveal = key
        .sign(
            &signing_root(uint64_root(epoch), [2, 0, 0, 0], version, state.genesis_validators_root),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = BeaconBlock {
        slot: state.slot,
        proposer_index: proposer,
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    let processing = &parameters.gloas;
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
    message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    ssz::SignedBeaconBlock::Gloas(SignedBeaconBlock { message, signature }, Preset::Minimal)
}
fn all_bits(length: usize) -> Vec<u8> {
    let mut bytes = vec![0; (length + 1).div_ceil(8)];
    for index in 0..=length {
        bytes[index / 8] |= 1 << (index % 8);
    }
    bytes
}
fn aggregate(indices: &[u64], keys: &[SecretKey], root: Root) -> [u8; 96] {
    let signatures = indices
        .iter()
        .map(|&index| keys[index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
        .unwrap()
        .to_signature()
        .to_bytes()
}
fn first_with_phase0_history(
) -> (Store, Vec<SecretKey>, TransitionParameters, ssz::SignedBeaconBlock) {
    let (mut store, keys, parameters) = real_anchor(Fork::Phase0);
    for slot in 1..=2 {
        store.on_tick(slot * store.slot_duration_ms()).unwrap();
        let child = phase0_signed_child(&store, &keys, &parameters);
        store.on_block(&ssz::SignedBeaconBlock::Phase0(child, Preset::Minimal)).unwrap();
    }
    store.on_tick(8 * store.slot_duration_ms()).unwrap();
    let signed = signed_first(&store, &keys, &parameters);
    (store, keys, parameters, signed)
}

#[test]
fn real_signed_upgrade_preserves_typed_shared_ancestry_and_checkpoints() {
    let (store, _, parameters, signed) = first_with_phase0_history();
    let roots = store.migration_roots().collect::<Vec<_>>();
    assert_eq!(roots.len(), 3);
    let engine = UnavailableEngine(AtomicUsize::new(0));
    let migrated = ready(GloasStore::migrate_from_classic(&store, &signed, &engine)).unwrap();
    assert_eq!(engine.0.load(Ordering::SeqCst), 0);
    assert_eq!(migrated.checkpoints(), store.migration_checkpoints());
    assert_eq!(migrated.time_ms(), store.time_ms());
    assert_eq!(migrated.genesis_time_ms(), store.genesis_time_ms());
    for root in roots {
        assert!(Arc::ptr_eq(
            &store.block_arc(root).unwrap(),
            &migrated.historical_block_arc(root).unwrap()
        ));
        assert!(Arc::ptr_eq(
            &store.block_state_arc(root).unwrap(),
            &migrated.historical_block_state_arc(root).unwrap()
        ));
        assert_eq!(migrated.historical_block_state_arc(root).unwrap().schema_fork(), Fork::Phase0);
        assert!(migrated.block_state_arc(root).is_none());
        assert_eq!(migrated.root_node(root, PayloadStatus::Pending).unwrap().payload_status, None);
    }
    let new_root = signed.message_root().unwrap();
    let head = migrated.get_head().unwrap();
    assert_eq!(head.root, new_root);
    assert_eq!(migrated.get_ancestor(head, 2).unwrap(), store.get_head().unwrap());
    assert_eq!(migrated.get_checkpoint_block(new_root, 0).unwrap(), store.checkpoints().0.root);
    assert!(!migrated.is_payload_verified(new_root));
    let mut expected = store.block_state(store.get_head().unwrap().root).unwrap().clone();
    ready(fork_processing::state_transition(&mut expected, &signed, &parameters, &engine)).unwrap();
    let AnyState::Gloas(expected, _) = expected else { panic!("actual Gloas schema") };
    assert_eq!(migrated.block_state(new_root).unwrap(), &expected);
    assert!(migrated.retained_schema_bytes().unwrap() > store.retained_schema_bytes().unwrap());
}

#[test]
fn historical_finality_initializes_fcr_without_fabricated_payload_branch() {
    let (store, _, _, signed) = first_with_phase0_history();
    let migrated = ready(GloasStore::migrate_from_classic(
        &store,
        &signed,
        &UnavailableEngine(AtomicUsize::new(0)),
    ))
    .unwrap();
    let finalized = store.checkpoints().1;
    assert!(!FastConfirmationView::has_payload_branches(&migrated, finalized.root));
    assert!(matches!(
        FastConfirmationView::state(&migrated, finalized.root).unwrap(),
        StateRef::Classic(AnyState::Phase0(..))
    ));
    assert!(matches!(
        FastConfirmationView::checkpoint_state(&migrated, finalized).unwrap(),
        StateRef::Classic(AnyState::Phase0(..))
    ));
    let mut confirmation = FastConfirmation::new(&migrated, Parameters::default()).unwrap();
    assert_eq!(confirmation.snapshot().confirmed_root, finalized.root);
    confirmation.update_variables(&migrated).unwrap();
    assert!(confirmation.latest_confirmed(&migrated).is_ok());
}

#[test]
fn missing_envelopes_and_historical_schema_cannot_assert_execution_hashes() {
    let (store, _, _, signed) = first_with_phase0_history();
    let migrated = ready(GloasStore::migrate_from_classic(
        &store,
        &signed,
        &UnavailableEngine(AtomicUsize::new(0)),
    ))
    .unwrap();
    let root = signed.message_root().unwrap();
    assert_eq!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Pending)).unwrap(),
        None
    );
    assert_eq!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Empty)).unwrap(),
        None
    );
    assert!(matches!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert!(matches!(
        migrated.verified_execution_block_hash(node(
            store.get_head().unwrap().root,
            PayloadStatus::Full
        )),
        Err(StoreError::InvalidAttestationTarget)
    ));
    assert_eq!(migrated.finalized_execution_block_hash().unwrap(), None);
}

#[test]
fn real_previous_fulu_vote_uses_original_checkpoint_registry_and_domain() {
    let (mut store, keys, parameters) = real_anchor(Fork::Fulu);
    let root = store.get_head().unwrap().root;
    let AnyState::Fulu(state, _) = store.block_state(root).unwrap() else { panic!("Fulu anchor") };
    let committee =
        fulu_processing::get_beacon_committee(state, 0, 0, &parameters.gloas.fulu).unwrap();
    let data = phase0::AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: root,
        source: state.current_justified_checkpoint,
        target: Checkpoint { epoch: 0, root },
    };
    let mut attestation = gloas::Attestation {
        aggregation_bits: gloas::AggregationBits::from_ssz(
            &all_bits(committee.len()),
            Preset::Minimal,
        )
        .unwrap(),
        data: data.clone(),
        signature: aggregate(
            &committee,
            &keys,
            signing_root(
                data.hash_tree_root(Preset::Minimal).unwrap(),
                [1, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
        ),
        committee_bits: gloas::CommitteeBits::from_ssz(&[1], Preset::Minimal).unwrap(),
    };
    store.on_tick(8 * store.slot_duration_ms()).unwrap();
    let signed = signed_first(&store, &keys, &parameters);
    let mut migrated = ready(GloasStore::migrate_from_classic(
        &store,
        &signed,
        &UnavailableEngine(AtomicUsize::new(0)),
    ))
    .unwrap();
    migrated.on_attestation(&attestation).unwrap();
    for index in &committee {
        let message = migrated.latest_message(*index).unwrap();
        assert_eq!(message.slot, 0);
        assert_eq!(
            migrated.get_supported_node(message).unwrap(),
            ForkChoiceNode { root, payload_status: None }
        );
    }
    let before = migrated.snapshot().unwrap();
    let retained = migrated.retained_schema_bytes().unwrap();
    let AnyState::Fulu(state, _) = store.block_state(root).unwrap() else { panic!("Fulu anchor") };
    attestation.signature = aggregate(
        &committee,
        &keys,
        signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            parameters.gloas.fork_version,
            state.genesis_validators_root,
        ),
    );
    assert!(migrated.on_attestation(&attestation).is_err());
    assert_eq!(migrated.snapshot().unwrap(), before);
    assert_eq!(migrated.retained_schema_bytes().unwrap(), retained);
}

#[test]
fn real_classic_latest_message_keeps_selected_signed_slot_and_equivocation() {
    let (mut store, keys, parameters, signed) = first_with_phase0_history();
    let anchor = store.checkpoints().0.root;
    let AnyState::Phase0(state, _) = store.block_state(anchor).unwrap() else {
        panic!("Phase0 context")
    };
    let voted = store.get_head().unwrap().root;
    let committee = phase0_processing::get_beacon_committee(
        state,
        2,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    let data = phase0::AttestationData {
        slot: 2,
        index: 0,
        beacon_block_root: voted,
        source: state.current_justified_checkpoint,
        target: Checkpoint { epoch: 0, root: anchor },
    };
    let signature = aggregate(
        &committee,
        &keys,
        signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            state.fork.current_version,
            state.genesis_validators_root,
        ),
    );
    let attestation = phase0::Attestation {
        aggregation_bits: phase0::AggregationBits::from_ssz(
            &all_bits(committee.len()),
            Preset::Minimal,
        )
        .unwrap(),
        data: data.clone(),
        signature,
    };
    store.on_attestation(&attestation).unwrap();
    let index = committee[0];
    let mut first = phase0::IndexedAttestation::default_value(Preset::Minimal).unwrap();
    first.attesting_indices = ssz::List::from_vec(vec![index]);
    first.data = data;
    let AnyState::Phase0(state, _) = store.block_state(anchor).unwrap() else {
        panic!("original context")
    };
    first.signature = aggregate(
        &[index],
        &keys,
        signing_root(
            first.data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            state.fork.current_version,
            state.genesis_validators_root,
        ),
    );
    let mut second = first.clone();
    second.data.beacon_block_root = anchor;
    second.signature = aggregate(
        &[index],
        &keys,
        signing_root(
            second.data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            state.fork.current_version,
            state.genesis_validators_root,
        ),
    );
    store
        .on_attester_slashing(&phase0::AttesterSlashing {
            attestation_1: first,
            attestation_2: second,
        })
        .unwrap();
    let migrated = ready(GloasStore::migrate_from_classic(
        &store,
        &signed,
        &UnavailableEngine(AtomicUsize::new(0)),
    ))
    .unwrap();
    assert!(migrated.equivocating.contains(&index));
    for index in committee {
        if let Some(message) = migrated.latest_message(index) {
            assert_eq!(message.slot, 2);
            assert_eq!(message.root, voted);
        }
    }
}

#[test]
fn signature_state_root_unknown_parent_and_clock_fail_transactionally() {
    let (store, keys, parameters, signed) = first_with_phase0_history();
    let snapshot = store.snapshot().unwrap();
    let bytes = store.retained_schema_bytes().unwrap();
    let parent = store.block_state_arc(snapshot.head.root).unwrap();
    let ssz::SignedBeaconBlock::Gloas(original, preset) = signed else {
        panic!("Gloas signed child")
    };
    for change in 0..4 {
        let mut invalid = original.clone();
        match change {
            0 => invalid.signature = [0; 96],
            1 => {
                invalid.message.state_root = [1; 32];
                let state = store.block_state(snapshot.head.root).unwrap();
                invalid.signature = keys[invalid.message.proposer_index as usize]
                    .sign(
                        &signing_root(
                            invalid.message.hash_tree_root(preset).unwrap(),
                            [0; 4],
                            parameters.gloas.fork_version,
                            genesis_root(state),
                        ),
                        ETHEREUM_BLS_DST,
                        &[],
                    )
                    .to_bytes();
            }
            2 => invalid.message.parent_root = [2; 32],
            _ => {
                let ssz::SignedBeaconBlock::Gloas(future, _) =
                    signed_first_at(&store, snapshot.head.root, &keys, &parameters, 9)
                else {
                    panic!("signed future Gloas block")
                };
                invalid = future;
            }
        }
        let error = ready(GloasStore::migrate_from_classic(
            &store,
            &ssz::SignedBeaconBlock::Gloas(invalid, preset),
            &UnavailableEngine(AtomicUsize::new(0)),
        ))
        .unwrap_err();
        match change {
            0 => assert!(matches!(error, StoreError::ExecutionImport(message)
                if message == "invalid BLS signature")),
            1 => assert!(matches!(error, StoreError::ExecutionImport(message)
                if message == "invalid consensus state: block post-state root mismatch")),
            2 => assert!(matches!(error, StoreError::UnknownRoot)),
            _ => assert!(matches!(error, StoreError::InvalidBlockTime)),
        }
        assert_eq!(store.snapshot().unwrap(), snapshot);
        assert_eq!(store.retained_schema_bytes().unwrap(), bytes);
        assert!(Arc::ptr_eq(&parent, &store.block_state_arc(snapshot.head.root).unwrap()));
    }
}

#[test]
fn missing_schedule_and_wrong_preset_cannot_be_migration_authority() {
    let (store, _, _, signed) = first_with_phase0_history();
    let root = store.checkpoints().0.root;
    let detached = Store::get_store(
        store.block_state(root).unwrap().clone(),
        store.block(root).unwrap().clone(),
        StoreParameters::for_preset(Preset::Minimal),
    )
    .unwrap();
    assert!(matches!(
        ready(GloasStore::migrate_from_classic(
            &detached,
            &signed,
            &UnavailableEngine(AtomicUsize::new(0))
        )),
        Err(StoreError::ValidationUnavailable(Fork::Gloas))
    ));
    let ssz::SignedBeaconBlock::Gloas(signed, _) = signed else { panic!("Gloas schema") };
    assert!(matches!(
        ready(GloasStore::migrate_from_classic(
            &store,
            &ssz::SignedBeaconBlock::Gloas(signed, Preset::Mainnet),
            &UnavailableEngine(AtomicUsize::new(0))
        )),
        Err(StoreError::AnchorMismatch)
    ));
}

#[test]
fn migration_rejects_budget_before_admitting_new_graph() {
    let (store, keys, parameters, _) = first_with_phase0_history();
    let root = store.checkpoints().0.root;
    let mut limits = StoreParameters::for_preset(Preset::Minimal);
    limits.max_blocks = 1;
    let mut bounded = Store::get_store(
        store.block_state(root).unwrap().clone(),
        store.block(root).unwrap().clone(),
        limits,
    )
    .unwrap();
    bounded.set_transition_parameters(parameters.clone()).unwrap();
    bounded.on_tick(8 * bounded.slot_duration_ms()).unwrap();
    let signed = signed_first(&bounded, &keys, &parameters);
    let before = bounded.snapshot().unwrap();
    assert!(matches!(
        ready(GloasStore::migrate_from_classic(
            &bounded,
            &signed,
            &UnavailableEngine(AtomicUsize::new(0))
        )),
        Err(StoreError::ResourceLimit)
    ));
    assert_eq!(bounded.snapshot().unwrap(), before);
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

fn phase0_signed_child(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> phase0::SignedBeaconBlock {
    let head = store.get_head().expect("real retained head");
    let parent = store.block_state(head.root).expect("real retained state");
    let slot = parent.slot().checked_add(1).expect("bounded unit slot");
    let AnyState::Phase0(mut state, preset) =
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

fn genesis_root(state: &AnyState) -> Root {
    match state {
        AnyState::Phase0(s, _) => s.genesis_validators_root,
        AnyState::Altair(s, _) => s.genesis_validators_root,
        AnyState::Bellatrix(s, _) => s.genesis_validators_root,
        AnyState::Capella(s, _) => s.genesis_validators_root,
        AnyState::Deneb(s, _) => s.genesis_validators_root,
        AnyState::Electra(s, _) => s.genesis_validators_root,
        AnyState::Fulu(s, _) => s.genesis_validators_root,
        AnyState::Gloas(s, _) => s.genesis_validators_root,
    }
}

// Exact controlled callback contracts; these do not calculate or validate an EL block.
struct BoundaryEngine {
    fulu_payload: fulu::ExecutionPayload,
    fulu_parent: Root,
    fulu_slot: u64,
    terminal_calls: AtomicUsize,
    fulu_calls: AtomicUsize,
    gloas_calls: AtomicUsize,
    expected_gloas: Mutex<Option<(gloas::ExecutionPayload, gloas_processing::ExecutionContext)>>,
}
impl BoundaryEngine {
    fn new(signed: &fulu::SignedBeaconBlock) -> Self {
        Self {
            fulu_payload: signed.message.body.execution_payload.clone(),
            fulu_parent: signed.message.parent_root,
            fulu_slot: signed.message.slot,
            terminal_calls: AtomicUsize::new(0),
            fulu_calls: AtomicUsize::new(0),
            gloas_calls: AtomicUsize::new(0),
            expected_gloas: Mutex::new(None),
        }
    }
    fn check_fulu(
        &self,
        payload: &fulu::ExecutionPayload,
        context: &fulu_processing::ExecutionContext,
    ) {
        assert_eq!(payload, &self.fulu_payload);
        assert_eq!(context.slot, self.fulu_slot);
        assert_eq!(context.parent_beacon_block_root, self.fulu_parent);
        assert_eq!(context.previous_execution_payload_hash, None);
        assert!(context.expected_blob_versioned_hashes.is_empty());
        assert!(context.execution_requests_list.is_empty());
    }
}
macro_rules! unavailable_boundary_engine {
    ($module:ident, $schema:ident) => {
        impl $module::ExecutionEngine for BoundaryEngine {
            type Error = std::io::Error;
            async fn verify_terminal_block(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(std::io::Error::other("earlier callback is outside this controlled contract"))
            }
            async fn verify_payload(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(std::io::Error::other("earlier callback is outside this controlled contract"))
            }
        }
    };
}
unavailable_boundary_engine!(bellatrix_processing, bellatrix);
unavailable_boundary_engine!(capella_processing, capella);
unavailable_boundary_engine!(deneb_processing, deneb);
unavailable_boundary_engine!(electra_processing, electra);
impl fulu_processing::ExecutionEngine for BoundaryEngine {
    type Error = std::io::Error;
    async fn verify_terminal_block(
        &self,
        payload: &fulu::ExecutionPayload,
        context: fulu_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.check_fulu(payload, &context);
        assert_eq!(self.fulu_calls.load(Ordering::SeqCst), 0);
        self.terminal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn verify_payload(
        &self,
        payload: &fulu::ExecutionPayload,
        context: fulu_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.check_fulu(payload, &context);
        assert_eq!(self.terminal_calls.load(Ordering::SeqCst), 1);
        self.fulu_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl gloas_processing::ExecutionEngine for BoundaryEngine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        payload: &gloas::ExecutionPayload,
        context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        let guard = self.expected_gloas.lock().unwrap();
        let (expected, expected_context) =
            guard.as_ref().expect("exact envelope contract installed");
        assert_eq!(payload, expected);
        assert_eq!(context.slot, expected_context.slot);
        assert_eq!(context.parent_beacon_block_root, expected_context.parent_beacon_block_root);
        assert_eq!(
            context.previous_execution_payload_hash,
            expected_context.previous_execution_payload_hash
        );
        assert_eq!(
            context.expected_blob_versioned_hashes,
            expected_context.expected_blob_versioned_hashes
        );
        assert_eq!(context.execution_requests_list, expected_context.execution_requests_list);
        self.gloas_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct EmptyBoundaryData(AtomicUsize);
impl crate::data_availability::DataAvailability for EmptyBoundaryData {
    type Error = std::io::Error;
    async fn verify(&self, root: Root, block: &AnyBlock) -> Result<(), Self::Error> {
        let AnyBlock::Fulu(block, preset) = block else {
            return Err(std::io::Error::other("controlled data contract only accepts Fulu"));
        };
        assert_eq!(*preset, Preset::Minimal);
        assert_eq!(root, block.hash_tree_root(*preset).unwrap());
        assert!(block.body.blob_kzg_commitments.values.is_empty());
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl DataAvailability for EmptyBoundaryData {
    type Error = std::io::Error;
    async fn verify(&self, root: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        assert_eq!(root, block.hash_tree_root(Preset::Minimal).unwrap());
        assert!(block
            .body
            .signed_execution_payload_bid
            .message
            .blob_kzg_commitments
            .values
            .is_empty());
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn fulu_signed_child(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    hash: u8,
) -> fulu::SignedBeaconBlock {
    let parent = store.get_head().unwrap().root;
    let pre = store.block_state(parent).unwrap();
    let target = pre.slot() + 1;
    let AnyState::Fulu(mut state, _) =
        fork_processing::replay_slots(pre, target, parameters, 1).unwrap()
    else {
        panic!("real Fulu child context")
    };
    let processing = &parameters.gloas.fulu;
    let proposer = fulu_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let mut body = fulu::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    body.sync_aggregate.sync_committee_signature = infinity;
    body.execution_payload = fulu::ExecutionPayload {
        parent_hash: state.latest_execution_payload_header.block_hash,
        fee_recipient: [8; 20],
        state_root: [1; 32],
        receipts_root: [2; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: state.randao_mixes.values[(state.slot / 8 % 64) as usize],
        block_number: state.latest_execution_payload_header.block_number + 1,
        gas_limit: 30_000_000,
        gas_used: 0,
        timestamp: state.genesis_time + state.slot * 6,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [hash; 32],
        transactions: List::default(),
        withdrawals: List::default(),
        blob_gas_used: 0,
        excess_blob_gas: 0,
    };
    body.randao_reveal = keys[proposer as usize]
        .sign(
            &signing_root(
                uint64_root(target / 8),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = fulu::BeaconBlock {
        slot: target,
        proposer_index: proposer,
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    fulu_processing::process_block_header(&mut state, &message, processing).unwrap();
    fulu_processing::process_withdrawals(&mut state, &message.body.execution_payload, processing)
        .unwrap();
    state.latest_execution_payload_header =
        fulu_processing::validate_execution_payload_consensus(&state, &message.body, processing)
            .unwrap();
    fulu_processing::process_randao(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_operations(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, processing)
        .unwrap();
    message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = keys[proposer as usize]
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    fulu::SignedBeaconBlock { message, signature }
}
fn fulu_store_with_payload() -> (Store, Vec<SecretKey>, TransitionParameters, BoundaryEngine) {
    let (mut store, keys, parameters) = real_anchor(Fork::Fulu);
    let signed = fulu_signed_child(&store, &keys, &parameters, 41);
    let engine = BoundaryEngine::new(&signed);
    let availability = EmptyBoundaryData(AtomicUsize::new(0));
    store.on_tick(store.slot_duration_ms()).unwrap();
    ready(store.on_block_with_engine(
        &ssz::SignedBeaconBlock::Fulu(signed, Preset::Minimal),
        &engine,
        &availability,
    ))
    .unwrap();
    assert_eq!(engine.terminal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.fulu_calls.load(Ordering::SeqCst), 1);
    assert_eq!(availability.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.verified_execution_block_hash(store.get_head().unwrap().root).unwrap(),
        Some([41; 32])
    );
    (store, keys, parameters, engine)
}
fn signed_gloas_child(
    store: &GloasStore,
    keys: &[SecretKey],
    include_parent: bool,
    hash: u8,
    attest: bool,
) -> SignedBeaconBlock {
    signed_gloas_child_with_slashing(store, keys, include_parent, hash, attest, None)
}
fn signed_gloas_child_with_slashing(
    store: &GloasStore,
    keys: &[SecretKey],
    include_parent: bool,
    hash: u8,
    attest: bool,
    slashing: Option<gloas::AttesterSlashing>,
) -> SignedBeaconBlock {
    let attested_slots = if attest {
        vec![store.block_state(store.get_head().unwrap().root).unwrap().slot]
    } else {
        Vec::new()
    };
    signed_gloas_child_with_attestations(
        store,
        keys,
        include_parent,
        hash,
        &attested_slots,
        slashing,
    )
}
fn signed_gloas_child_with_attestations(
    store: &GloasStore,
    keys: &[SecretKey],
    include_parent: bool,
    hash: u8,
    attested_slots: &[u64],
    slashing: Option<gloas::AttesterSlashing>,
) -> SignedBeaconBlock {
    let parent = store.get_head().unwrap().root;
    let mut state = store.block_state(parent).unwrap().clone();
    let target = state.slot + 1;
    gloas_processing::process_slots(&mut state, target, &store.processing).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, &store.processing).unwrap();
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    if let Some(slashing) = slashing {
        body.attester_slashings = ProgressiveList::from_vec(vec![slashing]);
    }
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: if include_parent {
            state.latest_execution_payload_bid.block_hash
        } else {
            state.latest_block_hash
        },
        parent_block_root: parent,
        block_hash: [hash; 32],
        prev_randao: state.randao_mixes.values[(target / 8 % 64) as usize],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot: target,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests().hash_tree_root(Preset::Minimal).unwrap(),
    };
    for &slot in attested_slots {
        let epoch = slot / 8;
        let committee =
            gloas_processing::get_beacon_committee(&state, slot, 0, &store.processing).unwrap();
        let data = phase0::AttestationData {
            slot,
            index: 0,
            beacon_block_root: state.block_roots.values[(slot % 64) as usize],
            source: if epoch == target / 8 {
                state.current_justified_checkpoint
            } else {
                state.previous_justified_checkpoint
            },
            target: Checkpoint { epoch, root: state.block_roots.values[(epoch * 8 % 64) as usize] },
        };
        let version = if epoch < state.fork.epoch {
            state.fork.previous_version
        } else {
            state.fork.current_version
        };
        body.attestations.values.push(gloas::Attestation {
            aggregation_bits: gloas::AggregationBits::from_ssz(
                &all_bits(committee.len()),
                Preset::Minimal,
            )
            .unwrap(),
            signature: aggregate(
                &committee,
                keys,
                signing_root(
                    data.hash_tree_root(Preset::Minimal).unwrap(),
                    [1, 0, 0, 0],
                    version,
                    state.genesis_validators_root,
                ),
            ),
            data,
            committee_bits: gloas::CommitteeBits::from_ssz(&[1], Preset::Minimal).unwrap(),
        });
    }
    body.randao_reveal = keys[proposer as usize]
        .sign(
            &signing_root(
                uint64_root(target / 8),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = BeaconBlock {
        slot: target,
        proposer_index: proposer,
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    let processing = &store.processing;
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
    message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = keys[proposer as usize]
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    SignedBeaconBlock { message, signature }
}
// The genesis guard prevents justification during epochs zero and one. Let epoch one
// become the actual justified checkpoint at slot24 instead of simultaneously justifying
// epoch two and skipping the historical root. Afterwards both previous-epoch-two and
// current-epoch-three committees genuinely vote, providing Casper's required finality bits.
fn finalizing_gloas_child(store: &GloasStore, keys: &[SecretKey], slot: u64) -> SignedBeaconBlock {
    let attested_slots = match slot {
        17..=23 => Vec::new(),
        25..=31 => vec![slot - 9, slot - 1],
        _ => vec![slot - 1],
    };
    let child = signed_gloas_child_with_attestations(
        store,
        keys,
        false,
        100 + slot as u8,
        &attested_slots,
        None,
    );
    assert_eq!(child.message.slot, slot);
    if (17..=23).contains(&slot) {
        assert!(child.message.body.attestations.values.is_empty());
    }
    child
}
fn assert_finalizing_participation(store: &GloasStore, historical: Root, slot: u64) {
    let state = store.block_state(store.get_head().unwrap().root).unwrap();
    let target_count = |flags: &[u8]| flags.iter().filter(|&&flag| flag & 2 != 0).count();
    match slot {
        23 => {
            assert_eq!(target_count(&state.current_epoch_participation.values), 0);
            assert_eq!(target_count(&state.previous_epoch_participation.values), 28);
            assert_eq!(state.current_justified_checkpoint.epoch, 0);
        }
        24 => {
            assert_eq!(
                state.current_justified_checkpoint,
                Checkpoint { epoch: 1, root: historical }
            );
            assert_eq!(target_count(&state.previous_epoch_participation.values), 4);
            assert_eq!(target_count(&state.current_epoch_participation.values), 0);
        }
        31 => {
            assert_eq!(
                state.current_justified_checkpoint,
                Checkpoint { epoch: 1, root: historical }
            );
            assert_eq!(target_count(&state.previous_epoch_participation.values), 32);
            assert_eq!(target_count(&state.current_epoch_participation.values), 28);
        }
        _ => {}
    }
}

fn admit_gloas_envelope(
    store: &mut GloasStore,
    root: Root,
    keys: &[SecretKey],
    engine: &BoundaryEngine,
) {
    let state = store.block_state(root).unwrap();
    let bid = &state.latest_execution_payload_bid;
    let message = gloas::ExecutionPayloadEnvelope {
        payload: gloas::ExecutionPayload {
            parent_hash: state.latest_block_hash,
            fee_recipient: bid.fee_recipient,
            state_root: [1; 32],
            receipts_root: [2; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: bid.prev_randao,
            block_number: state.slot + 1,
            gas_limit: bid.gas_limit,
            gas_used: 0,
            timestamp: state.genesis_time + state.slot * 6,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: bid.block_hash,
            transactions: ProgressiveList::default(),
            withdrawals: state.payload_expected_withdrawals.clone(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
            block_access_list: ProgressiveList::default(),
            slot_number: state.slot,
        },
        execution_requests: requests(),
        builder_index: bid.builder_index,
        beacon_block_root: root,
        parent_beacon_block_root: state.latest_block_header.parent_root,
    };
    let context = gloas_processing::ExecutionContext {
        slot: state.slot,
        parent_beacon_block_root: state.latest_block_header.parent_root,
        previous_execution_payload_hash: state.latest_block_hash,
        expected_blob_versioned_hashes: Vec::new(),
        execution_requests_list: Vec::new(),
    };
    *engine.expected_gloas.lock().unwrap() = Some((message.payload.clone(), context));
    let signature = keys[state.latest_block_header.proposer_index as usize]
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [11, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let availability = EmptyBoundaryData(AtomicUsize::new(0));
    ready(store.on_execution_payload_envelope(
        &gloas::SignedExecutionPayloadEnvelope { message, signature },
        engine,
        &availability,
    ))
    .unwrap();
    assert_eq!(availability.0.load(Ordering::SeqCst), 1);
}

#[test]
fn nonzero_fulu_parent_and_signed_gloas_full_empty_ancestry_keep_exact_contracts() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let parent = classic.get_head().unwrap().root;
    classic.on_tick(8 * classic.slot_duration_ms()).unwrap();
    let signed = signed_first(&classic, &keys, &parameters);
    let root = signed.message_root().unwrap();
    let mut migrated = ready(GloasStore::migrate_from_classic(&classic, &signed, &engine)).unwrap();
    assert_eq!(engine.gloas_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Empty)).unwrap(),
        Some([41; 32])
    );
    assert_eq!(
        migrated
            .verified_execution_block_hash(ForkChoiceNode { root: parent, payload_status: None })
            .unwrap(),
        Some([41; 32])
    );
    assert!(matches!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(_))
    ));
    admit_gloas_envelope(&mut migrated, root, &keys, &engine);
    assert_eq!(engine.gloas_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        migrated.verified_execution_block_hash(node(root, PayloadStatus::Full)).unwrap(),
        Some([11; 32])
    );
    for (include, hash) in [(true, 43), (false, 44), (false, 45)] {
        let child = signed_gloas_child(&migrated, &keys, include, hash, false);
        migrated.on_tick(child.message.slot * migrated.slot_duration_ms()).unwrap();
        migrated.on_block(&child).unwrap();
        let child_root = child.message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(
            migrated.verified_execution_block_hash(node(child_root, PayloadStatus::Empty)).unwrap(),
            Some([11; 32])
        );
    }
    let current = migrated.get_head().unwrap();
    let before = migrated.snapshot().unwrap();
    let bytes = migrated.retained_schema_bytes().unwrap();
    Arc::make_mut(migrated.payloads.get_mut(&root).unwrap()).payload.block_hash = [0xee; 32];
    assert!(matches!(
        migrated.verified_execution_block_hash(current),
        Err(StoreError::ExecutionImport(_))
    ));
    assert_eq!(migrated.snapshot().unwrap(), before);
    assert_eq!(migrated.retained_schema_bytes().unwrap(), bytes);
}

#[test]
fn real_attestations_advance_gloas_checkpoints_and_finalize_a_historical_root() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let parent = classic.get_head().unwrap().root;
    classic.on_tick(9 * classic.slot_duration_ms()).unwrap();
    let first = signed_first_at(&classic, parent, &keys, &parameters, 9);
    let first_root = first.message_root().unwrap();
    let mut migrated = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let mut observed_historical_finalization = false;
    for slot in 10..=32 {
        let child = finalizing_gloas_child(&migrated, &keys, slot);
        migrated.on_tick(slot * migrated.slot_duration_ms()).unwrap();
        migrated.on_block_and_contents(&child).unwrap();
        assert_finalizing_participation(&migrated, parent, slot);
        if migrated.checkpoints().1 == (Checkpoint { epoch: 1, root: parent }) {
            observed_historical_finalization = true;
        }
    }
    assert!(
        observed_historical_finalization,
        "real finalization must visit the retained Fulu root"
    );
    let (justified, finalized, _, _) = migrated.checkpoints();
    assert!(
        justified.epoch >= 2,
        "genuine previous-epoch committee participation must justify later epochs"
    );
    assert!(finalized.epoch >= 1, "genuine participation must finalize the historical checkpoint");
    let historical = Checkpoint { epoch: 1, root: parent };
    assert_eq!(migrated.get_checkpoint_block(first_root, 1).unwrap(), parent);
    assert!(matches!(
        migrated.checkpoint_view(historical).unwrap().view(),
        StateRef::Classic(AnyState::Gloas(..))
    ));
    assert!(migrated
        .historical_block_state_arc(parent)
        .is_some_and(|state| state.schema_fork() == Fork::Fulu));
    assert!(migrated.block_state_arc(parent).is_none());
    assert_eq!(migrated.finalized_execution_block_hash().unwrap(), Some([41; 32]));
    assert_eq!(engine.gloas_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn new_previous_fulu_and_current_gloas_slashings_use_exact_scheduled_domains() {
    let (mut classic, keys, parameters) = real_anchor(Fork::Fulu);
    let anchor = classic.get_head().unwrap().root;
    classic.on_tick(8 * classic.slot_duration_ms()).unwrap();
    let first = signed_first(&classic, &keys, &parameters);
    let migrated = ready(GloasStore::migrate_from_classic(
        &classic,
        &first,
        &UnavailableEngine(AtomicUsize::new(0)),
    ))
    .unwrap();
    let network = genesis_root(classic.block_state(anchor).unwrap());
    for (epoch, version, index) in
        [(0, parameters.gloas.fulu.fork_version, 0), (1, parameters.gloas.fork_version, 1)]
    {
        let mut store = migrated.clone();
        let data = phase0::AttestationData {
            slot: epoch * 8,
            index: 0,
            beacon_block_root: anchor,
            source: Checkpoint::default(),
            target: Checkpoint { epoch, root: anchor },
        };
        let mut a = gloas::IndexedAttestation {
            attesting_indices: ProgressiveList::from_vec(vec![index]),
            data,
            signature: [0; 96],
        };
        let mut b = a.clone();
        b.data.beacon_block_root = [0x77; 32];
        for indexed in [&mut a, &mut b] {
            indexed.signature = aggregate(
                &[index],
                &keys,
                signing_root(
                    indexed.data.hash_tree_root(Preset::Minimal).unwrap(),
                    [1, 0, 0, 0],
                    version,
                    network,
                ),
            );
        }
        let valid = gloas::AttesterSlashing { attestation_1: a, attestation_2: b };
        let before = store.snapshot().unwrap();
        let bytes = store.retained_schema_bytes().unwrap();
        let excluded = store.equivocating.clone();
        let mut wrong = valid.clone();
        wrong.attestation_2.signature = aggregate(
            &[index],
            &keys,
            signing_root(
                wrong.attestation_2.data.hash_tree_root(Preset::Minimal).unwrap(),
                [1, 0, 0, 0],
                if epoch == 0 {
                    parameters.gloas.fork_version
                } else {
                    parameters.gloas.fulu.fork_version
                },
                network,
            ),
        );
        assert!(matches!(
            store.on_attester_slashing(&wrong),
            Err(StoreError::Processing(phase0_processing::ProcessingError::Crypto(
                CryptoError::VerificationFailed
            )))
        ));
        wrong.attestation_2.signature = [0; 96];
        assert!(matches!(
            store.on_attester_slashing(&wrong),
            Err(StoreError::Processing(phase0_processing::ProcessingError::Crypto(
                CryptoError::InvalidSignature
            )))
        ));
        assert_eq!(store.equivocating, excluded);
        assert_eq!(store.snapshot().unwrap(), before);
        assert_eq!(store.retained_schema_bytes().unwrap(), bytes);
        store.on_attester_slashing(&valid).unwrap();
        assert!(store.equivocating.contains(&index));
        assert_eq!(store.snapshot().unwrap(), before);
    }
}

#[test]
fn combined_historical_and_gloas_schema_allocations_enforce_the_joint_byte_budget() {
    let (mut original, keys, parameters) = real_anchor(Fork::Fulu);
    original.on_tick(8 * original.slot_duration_ms()).unwrap();
    let signed = signed_first(&original, &keys, &parameters);
    let engine = UnavailableEngine(AtomicUsize::new(0));
    let migrated = ready(GloasStore::migrate_from_classic(&original, &signed, &engine)).unwrap();
    let historical_bytes = original.retained_schema_bytes().unwrap();
    let total = migrated.retained_schema_bytes().unwrap();
    let gloas_bytes = total - historical_bytes;
    let mut limits = StoreParameters::for_preset(Preset::Minimal);
    limits.max_retained_schema_bytes = historical_bytes.max(gloas_bytes) + 1;
    assert!(historical_bytes < limits.max_retained_schema_bytes);
    assert!(gloas_bytes < limits.max_retained_schema_bytes);
    assert!(total > limits.max_retained_schema_bytes);
    let (mut bounded, bounded_keys, bounded_parameters) =
        real_anchor_with_limits(Fork::Fulu, limits);
    bounded.on_tick(8 * bounded.slot_duration_ms()).unwrap();
    let child = signed_first(&bounded, &bounded_keys, &bounded_parameters);
    let snapshot = bounded.snapshot().unwrap();
    let bytes = bounded.retained_schema_bytes().unwrap();
    assert!(matches!(
        ready(GloasStore::migrate_from_classic(&bounded, &child, &engine)),
        Err(StoreError::ResourceLimit)
    ));
    assert_eq!(bounded.snapshot().unwrap(), snapshot);
    assert_eq!(bounded.retained_schema_bytes().unwrap(), bytes);
    assert_eq!(engine.0.load(Ordering::SeqCst), 0);
}

#[test]
fn finalized_signed_chain_rejects_a_genuine_competing_historical_parent_transactionally() {
    let (mut classic, keys, parameters) = real_anchor(Fork::Fulu);
    let canonical = fulu_signed_child(&classic, &keys, &parameters, 41);
    let sibling = fulu_signed_child(&classic, &keys, &parameters, 42);
    let parent = canonical.message.hash_tree_root(Preset::Minimal).unwrap();
    let competitor = sibling.message.hash_tree_root(Preset::Minimal).unwrap();
    assert_ne!(parent, competitor);
    let engine = BoundaryEngine::new(&canonical);
    let side_engine = BoundaryEngine::new(&sibling);
    let availability = EmptyBoundaryData(AtomicUsize::new(0));
    classic.on_tick(classic.slot_duration_ms()).unwrap();
    for (signed, contract) in [(canonical, &engine), (sibling, &side_engine)] {
        ready(classic.on_block_with_engine(
            &ssz::SignedBeaconBlock::Fulu(signed, Preset::Minimal),
            contract,
            &availability,
        ))
        .unwrap();
    }
    assert_eq!(availability.0.load(Ordering::SeqCst), 2);
    classic.on_tick(9 * classic.slot_duration_ms()).unwrap();
    let first = signed_first_at(&classic, parent, &keys, &parameters, 9);
    let ssz::SignedBeaconBlock::Gloas(conflicting, _) =
        signed_first_at(&classic, competitor, &keys, &parameters, 9)
    else {
        panic!("genuine competing signed fork transition")
    };
    let mut migrated = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    // Finality of the first post-genesis checkpoint needs the next real epoch boundary.
    for slot in 10..=32 {
        let child = finalizing_gloas_child(&migrated, &keys, slot);
        migrated.on_tick(slot * migrated.slot_duration_ms()).unwrap();
        migrated.on_block_and_contents(&child).unwrap();
        assert_finalizing_participation(&migrated, parent, slot);
    }
    assert_eq!(migrated.checkpoints().1, Checkpoint { epoch: 1, root: parent });
    let before = migrated.snapshot().unwrap();
    let bytes = migrated.retained_schema_bytes().unwrap();
    let messages = migrated.latest_messages.clone();
    let checkpoints = migrated.checkpoint_states.clone();
    assert!(matches!(migrated.on_block(&conflicting), Err(StoreError::ConflictsWithFinality)));
    assert_eq!(migrated.snapshot().unwrap(), before);
    assert_eq!(migrated.retained_schema_bytes().unwrap(), bytes);
    assert_eq!(migrated.latest_messages, messages);
    assert_eq!(migrated.checkpoint_states, checkpoints);
    assert!(!migrated
        .blocks
        .contains_key(&conflicting.message.hash_tree_root(Preset::Minimal).unwrap()));
}

#[test]
fn retained_signed_body_observations_authenticate_exact_items_and_reject_bad_indexes() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    classic.on_tick(8 * classic.slot_duration_ms()).unwrap();
    let first = signed_first(&classic, &keys, &parameters);
    let mut migrated = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let child = signed_gloas_child(&migrated, &keys, false, 43, true);
    let root = child.message.hash_tree_root(Preset::Minimal).unwrap();
    migrated.on_tick(child.message.slot * migrated.slot_duration_ms()).unwrap();
    migrated.on_block(&child).unwrap();
    assert!(migrated.latest_messages.is_empty());
    let before = migrated.snapshot().unwrap();
    let bytes = migrated.retained_schema_bytes().unwrap();
    let cached = migrated.checkpoint_states.clone();
    assert!(matches!(
        migrated.on_retained_block_attestation(root, 1),
        Err(StoreError::InvalidAttestationTarget)
    ));
    assert!(matches!(
        migrated.on_retained_block_slashing(root, 0),
        Err(StoreError::InvalidSlashing)
    ));
    assert!(matches!(
        migrated.on_retained_block_attestation([0xff; 32], 0),
        Err(StoreError::UnknownRoot)
    ));
    assert_eq!(migrated.checkpoint_states, cached);
    assert_eq!(migrated.snapshot().unwrap(), before);
    assert_eq!(migrated.retained_schema_bytes().unwrap(), bytes);
    assert!(migrated.latest_messages.is_empty());
    migrated.on_retained_block_attestation(root, 0).unwrap();
    assert!(!migrated.latest_messages.is_empty());
    for message in migrated.latest_messages.values() {
        assert_eq!(message.slot, 8);
        assert_eq!(message.root, first.message_root().unwrap());
    }
    let votes = migrated.latest_messages.clone();
    let cached = migrated.checkpoint_states.clone();
    migrated.on_retained_block_attestation(root, 0).unwrap();
    assert_eq!(migrated.latest_messages, votes);
    assert_eq!(migrated.checkpoint_states, cached);
}

#[test]
fn retained_genuine_signed_body_slashing_marks_only_authenticated_equivocators() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    classic.on_tick(8 * classic.slot_duration_ms()).unwrap();
    let first = signed_first(&classic, &keys, &parameters);
    let first_root = first.message_root().unwrap();
    let mut migrated = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let network = genesis_root(classic.block_state(classic.get_head().unwrap().root).unwrap());
    let mut a = gloas::IndexedAttestation {
        attesting_indices: ProgressiveList::from_vec(vec![0]),
        data: phase0::AttestationData {
            slot: 8,
            index: 0,
            beacon_block_root: first_root,
            source: Checkpoint::default(),
            target: Checkpoint { epoch: 1, root: first_root },
        },
        signature: [0; 96],
    };
    let mut b = a.clone();
    b.data.beacon_block_root = [0x77; 32];
    for indexed in [&mut a, &mut b] {
        indexed.signature = aggregate(
            &[0],
            &keys,
            signing_root(
                indexed.data.hash_tree_root(Preset::Minimal).unwrap(),
                [1, 0, 0, 0],
                parameters.gloas.fork_version,
                network,
            ),
        );
    }
    let child = signed_gloas_child_with_slashing(
        &migrated,
        &keys,
        false,
        43,
        false,
        Some(gloas::AttesterSlashing { attestation_1: a, attestation_2: b }),
    );
    let root = child.message.hash_tree_root(Preset::Minimal).unwrap();
    migrated.on_tick(child.message.slot * migrated.slot_duration_ms()).unwrap();
    migrated.on_block(&child).unwrap();
    assert!(migrated.block_state(root).unwrap().validators.values[0].slashed);
    assert!(
        migrated.equivocating.is_empty(),
        "normative transition and LMD observation remain separate"
    );
    let before = migrated.snapshot().unwrap();
    let bytes = migrated.retained_schema_bytes().unwrap();
    let cached = migrated.checkpoint_states.clone();
    assert!(matches!(
        migrated.on_retained_block_slashing(root, 1),
        Err(StoreError::InvalidSlashing)
    ));
    assert!(migrated.equivocating.is_empty());
    assert_eq!(migrated.checkpoint_states, cached);
    migrated.on_retained_block_slashing(root, 0).unwrap();
    assert_eq!(migrated.equivocating, BTreeSet::from([0]));
    assert_eq!(migrated.snapshot().unwrap(), before);
    assert_eq!(migrated.retained_schema_bytes().unwrap(), bytes);
    migrated.on_retained_block_slashing(root, 0).unwrap();
    assert_eq!(migrated.equivocating, BTreeSet::from([0]));
}

#[path = "checkpoint_tests.rs"]
mod checkpoint_tests;

#[path = "compact_tests.rs"]
mod compact_tests;
