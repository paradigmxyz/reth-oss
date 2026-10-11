//! Real signed genesis archives retain actual prefixes through rejection and cold resume.
//! Controlled unused Engine adapters cannot manufacture payload or canonical acknowledgements.

use super::*;
use crate::{
    bootstrap::genesis::tests::{genuine, genuine_phase0_gloas},
    compact_persistence::PersistenceNetwork,
    execution_bootstrap::tests::{adapter, Backend, BackendHandle},
    gossip_tests::{classic_to_gloas_child, phase0_signed_child},
    import_coordinator::{
        genesis::{GenesisImportAnchor, GenesisSetup},
        tests::genuine_empty_gloas_child,
    },
    VerifiedGenesis,
};
use reth_embedded_beacon_fork_choice::{Store, StoreParameters};
use reth_embedded_beacon_primitives::sha256;
use reth_embedded_beacon_shared_data::SharedDataStore;
use reth_embedded_beacon_state_transition::ssz::{phase0, SignedBeaconBlock};
use reth_embedded_beacon_storage::CompactStateLimits;
use reth_embedded_beacon_sync::{ArchiveEntry, ArchiveManifest};
use reth_fs_util as fs;
use std::{path::Path, sync::atomic::AtomicU64};
use tempfile::TempDir;

fn write_archive(directory: &Path, config: &ChainConfig, blocks: &[phase0::SignedBeaconBlock]) {
    let mut entries = Vec::new();
    for block in blocks {
        let bytes = block.to_ssz(Preset::Minimal).unwrap();
        let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
        let name = root.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        fs::write(directory.join(format!("{name}.ssz")), &bytes).unwrap();
        entries.push(ArchiveEntry {
            root,
            slot: block.message.slot,
            ssz_sha256: sha256(&bytes),
            bytes: bytes.len() as u64,
        });
    }
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&ArchiveManifest {
            version: 1,
            genesis_validators_root: config.genesis_validators_root,
            entries,
        })
        .unwrap(),
    )
    .unwrap();
}

fn worker(
    directory: &Path,
    config: ChainConfig,
    genesis: VerifiedGenesis,
    runtime: Handle,
    now: u64,
) -> (
    Worker<BackendHandle, RetrievalAvailability, RetrievalAvailability>,
    RetrievalAvailability,
    Arc<Backend>,
    crate::live_sync::availability::RetrievalBroker,
) {
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory, network.identity(), 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap();
    let (engine, calls) = adapter(config.clone(), Vec::new(), Duration::from_secs(1));
    let (availability, broker) = RetrievalAvailability::new(config.clone()).unwrap();
    let setup = GenesisSetup {
        config,
        anchor: GenesisImportAnchor { genesis, decode_limits: BootstrapDecodeLimits::default() },
        repository,
        engine,
        classic_availability: availability.clone(),
        gloas_availability: availability.clone(),
        limits: ImportLimits { max_tick_slots: 1, ..ImportLimits::default() },
        runtime,
        clock: OwnerClock::Controlled(Arc::new(AtomicU64::new(now))),
    };
    let worker = Worker::initialize_genesis(setup, Ok(now), true).unwrap();
    (worker, availability, calls, broker)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_archive_prefix_survives_bad_bls_and_resumes_from_signed_history() {
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let durable = TempDir::new().unwrap();
        let archive_directory = TempDir::new().unwrap();
        let next_directory = TempDir::new().unwrap();
        let (config, state, keys, parameters) = genuine(Fork::Phase0);
        let genesis = VerifiedGenesis::authenticate(
            config.clone(),
            state.to_ssz().unwrap().into(),
            BootstrapDecodeLimits::default(),
        )
        .unwrap();
        let original = genesis.original_state_ssz();
        let (state, block) = genesis.clone().into_import_inputs(&config).unwrap();
        let mut producer = Store::get_store(
            state.as_ref().clone(),
            block.as_ref().clone(),
            StoreParameters::for_preset(Preset::Minimal),
        )
        .unwrap();
        producer.set_transition_parameters(parameters.clone()).unwrap();
        let mut signed = Vec::new();
        for _ in 0..132 {
            let child = phase0_signed_child(&producer, &keys, &parameters);
            let late = config.clock().unwrap().start_of_slot(child.message.slot + 1).unwrap() - 1;
            producer.on_tick(late).unwrap();
            producer.on_block(&SignedBeaconBlock::Phase0(child.clone(), Preset::Minimal)).unwrap();
            signed.push(child);
        }
        let now = config.clock().unwrap().start_of_slot(10_000).unwrap();
        let (ready, _pending) = oneshot::channel();
        let mut damaged = signed[..130].to_vec();
        damaged[129].signature[0] ^= 1;
        write_archive(archive_directory.path(), &config, &damaged);
        let archive =
            HistoricalArchive::open(archive_directory.path(), config.genesis_validators_root)
                .unwrap();
        let (mut first, availability, calls, _broker) =
            worker(durable.path(), config.clone(), genesis.clone(), runtime.clone(), now);
        assert!(matches!(
            first.replay_archive(&archive, None, &availability, &ready),
            Err(ImportError::Consensus(_))
        ));
        let prefix = first.repository.current_snapshot().unwrap().unwrap();
        let root128 = signed[127].message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(prefix.head().root, root128);
        let unpublished = signed[128].message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(first.graph.snapshot().unwrap().head.root, unpublished);
        assert!(first.repository.original_block(unpublished).unwrap().is_some());
        assert_ne!(prefix.head().root, unpublished);
        assert_eq!(prefix.head().payload_status, None);
        assert_eq!(calls.forkchoice_call_count(), 0);
        assert_eq!(
            &*first
                .repository
                .recover_genesis(BootstrapDecodeLimits::default())
                .unwrap()
                .unwrap()
                .original_state_ssz(),
            &*original
        );
        drop(first);
        drop(availability);

        // Reopen actual originals; a manifest cursor never seeds or replaces this graph.
        write_archive(archive_directory.path(), &config, &signed[..130]);
        let archive =
            HistoricalArchive::open(archive_directory.path(), config.genesis_validators_root)
                .unwrap();
        let (mut restored, availability, calls, _broker) =
            worker(durable.path(), config.clone(), genesis.clone(), runtime.clone(), now);
        assert_eq!(restored.graph.snapshot().unwrap().head.root, genesis.block_root());
        assert_eq!(restored.repository.current_snapshot().unwrap().unwrap().head().root, root128);
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &config,
            genesis.block_root(),
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        assert_eq!(plan.blocks.len(), 128);
        assert!(matches!(
            restored.replay_archive(&archive, Some(&plan), &availability, &ready),
            Err(ImportError::Bounds)
        ));
        let root130 = signed[129].message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(restored.repository.current_snapshot().unwrap().unwrap().head().root, root130);
        assert_eq!(restored.graph.snapshot().unwrap().head.root, root130);
        assert_eq!(calls.forkchoice_call_count(), 0);
        drop(restored);
        drop(availability);

        // A separate bounded segment proves exact parent continuity from the recovered head.
        write_archive(next_directory.path(), &config, &signed[130..]);
        let next =
            HistoricalArchive::open(next_directory.path(), config.genesis_validators_root).unwrap();
        let (mut continued, availability, calls, _broker) =
            worker(durable.path(), config.clone(), genesis.clone(), runtime, now);
        let plan = recovery::ReplayPlan::prepare(
            &continued.repository,
            &config,
            genesis.block_root(),
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            continued.replay_archive(&next, Some(&plan), &availability, &ready),
            Err(ImportError::Bounds)
        ));
        let root132 = signed[131].message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(continued.repository.current_snapshot().unwrap().unwrap().head().root, root132);
        assert_eq!(continued.graph.snapshot().unwrap().head.root, root132);
        assert_eq!(calls.forkchoice_call_count(), 0);
        assert_eq!(
            &*continued
                .repository
                .recover_genesis(BootstrapDecodeLimits::default())
                .unwrap()
                .unwrap()
                .original_state_ssz(),
            &*original
        );
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_parent_archive_guidance_cannot_bypass_native_signed_admission() {
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let durable = TempDir::new().unwrap();
        let archive_directory = TempDir::new().unwrap();
        let (config, state, keys, parameters) = genuine_phase0_gloas();
        let genesis = VerifiedGenesis::authenticate(
            config.clone(),
            state.to_ssz().unwrap().into(),
            BootstrapDecodeLimits::default(),
        )
        .unwrap();
        let now = config.clock().unwrap().start_of_slot(10_000).unwrap();
        let (mut owner, _availability, calls, _broker) =
            worker(durable.path(), config.clone(), genesis.clone(), runtime, now);
        let (state, block) = genesis.into_import_inputs(&config).unwrap();
        let mut producer = Store::get_store(
            state.as_ref().clone(),
            block.as_ref().clone(),
            StoreParameters::for_preset(Preset::Minimal),
        )
        .unwrap();
        producer.set_transition_parameters(parameters.clone()).unwrap();
        let first = classic_to_gloas_child(&producer, &keys, &parameters);
        let first = WireBlock::from_ssz(first.to_ssz().unwrap(), &config).unwrap();
        owner.limits.max_tick_slots = parameters.slots_per_epoch();
        owner.advance_archive_clock(first.slot()).unwrap();
        owner.import(Input::Block(first.clone()), 0).unwrap();
        let Graph::Gloas { store, .. } = &owner.graph else { panic!("native Gloas graph") };
        let second = genuine_empty_gloas_child(store, first.root(), &keys, &parameters);
        let second_wire =
            WireBlock::from_ssz(second.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let mut damaged = second;
        damaged.signature[0] ^= 1;
        let bytes = damaged.to_ssz(Preset::Minimal).unwrap();
        let name = second_wire.root().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        fs::write(archive_directory.path().join(format!("{name}.ssz")), &bytes).unwrap();
        fs::write(
            archive_directory.path().join("manifest.json"),
            serde_json::to_vec(&ArchiveManifest {
                version: 1,
                genesis_validators_root: config.genesis_validators_root,
                entries: vec![ArchiveEntry {
                    root: second_wire.root(),
                    slot: second_wire.slot(),
                    ssz_sha256: sha256(&bytes),
                    bytes: bytes.len() as u64,
                }],
            })
            .unwrap(),
        )
        .unwrap();
        let archive =
            HistoricalArchive::open(archive_directory.path(), config.genesis_validators_root)
                .unwrap();
        // Even a bad signature can guide optional retrieval; it cannot admit its own child.
        assert!(!owner
            .archive_envelope_required(&archive, &first, archive.entries().first())
            .unwrap());
        let actual_first = owner.graph.snapshot().unwrap();
        owner.advance_archive_clock(second_wire.slot()).unwrap();
        let damaged = WireBlock::from_ssz(bytes, &config).unwrap();
        assert!(matches!(owner.import(Input::Block(damaged), 0), Err(ImportError::Consensus(_))));
        assert_eq!(owner.graph.snapshot().unwrap().head, actual_first.head);
        assert!(owner.repository.original_envelope(first.root()).unwrap().is_none());
        assert_eq!(calls.forkchoice_call_count(), 0);
        owner.import(Input::Block(second_wire.clone()), 0).unwrap();
        owner.publish_archive_prefix().unwrap();
        let actual = owner.graph.snapshot().unwrap();
        assert_eq!(actual.head.root, second_wire.root());
        assert_eq!(actual.head.payload_status, Some(PayloadStatus::Empty));
        assert!(owner.repository.original_envelope(second_wire.root()).unwrap().is_none());
        assert_eq!(owner.repository.current_snapshot().unwrap().unwrap().head(), actual.head);
        assert_eq!(calls.forkchoice_call_count(), 0);
    })
    .await
    .unwrap();
}
