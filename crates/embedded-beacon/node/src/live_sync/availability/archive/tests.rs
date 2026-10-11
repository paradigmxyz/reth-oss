//! Native custody selection vectors and immutable proof-record namespace isolation.

use super::*;
use tempfile::TempDir;

#[test]
fn pinned_node_id_integer_endianness_wrap_and_distinct_groups_match_das_vectors() {
    assert_eq!(
        custody_columns([0; 32]).unwrap().into_iter().collect::<Vec<_>>(),
        vec![1, 17, 87, 102]
    );
    let mut one = [0; 32];
    one[31] = 1;
    assert_eq!(custody_columns(one).unwrap().into_iter().collect::<Vec<_>>(), vec![1, 17, 75, 87]);
    assert_eq!(
        custody_columns([255; 32]).unwrap().into_iter().collect::<Vec<_>>(),
        vec![1, 47, 87, 102]
    );
    let mut prefix = [0; 32];
    prefix[..2].copy_from_slice(&[0xe3, 0x43]);
    assert_eq!(
        custody_columns(prefix).unwrap().into_iter().collect::<Vec<_>>(),
        vec![21, 57, 68, 83]
    );
}

#[test]
fn shared_writer_identity_preserves_broader_policy_and_rejects_changed_consensus_network() {
    let directory = TempDir::new().unwrap();
    let config = Arc::new(ChainConfig::hoodi());
    let identity =
        StoreIdentity { genesis_root: config.genesis_validators_root, config_digest: [201; 32] };
    let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
    let archive = SidecarArchive::new(
        config.clone(),
        checked_config_digest(&config).unwrap(),
        shared.clone(),
    )
    .unwrap();
    assert_eq!(archive.shared.identity(), identity);
    archive.bind_identity([0; 32]).unwrap();
    assert!(matches!(archive.bind_identity([1; 32]), Err(RetrievalError::Storage)));
    let mut foreign = config.as_ref().clone();
    foreign.genesis_validators_root = [202; 32];
    assert!(matches!(
        SidecarArchive::new(Arc::new(foreign), [0; 32], shared),
        Err(RetrievalError::Configuration)
    ));
}

#[test]
fn checksum_and_namespace_identity_are_integrity_only_and_cannot_authorize_stored_data() {
    let directory = TempDir::new().unwrap();
    let config = Arc::new(ChainConfig::hoodi());
    let shared = SharedDataStore::open(
        directory.path(),
        StoreIdentity { genesis_root: config.genesis_validators_root, config_digest: [203; 32] },
        0,
    )
    .unwrap();
    let archive =
        SidecarArchive::new(config.clone(), checked_config_digest(&config).unwrap(), shared)
            .unwrap();
    let mut record = Record {
        schema: 1,
        identity: archive.shared.identity(),
        config_digest: archive.config_digest,
        root: [3; 32],
        slot: 1,
        index: 0,
        context: config.fork_digest(0).unwrap(),
        column: false,
        object: ObjectId::new(
            ObjectKind::BeaconDataSidecar,
            Encoding::Ssz,
            b"structural marker only",
        ),
        checksum: [0; 32],
    };
    record.checksum = sha256(&serde_json::to_vec(&record).unwrap());
    let path = directory.path().join("controlled-record.json");
    publish_file(&path, &serde_json::to_vec(&record).unwrap()).unwrap();
    assert_eq!(archive.read_record(&path).unwrap().root, [3; 32]);
    record.root[0] ^= 1;
    publish_file(&path, &serde_json::to_vec(&record).unwrap()).unwrap();
    assert!(matches!(archive.read_record(&path), Err(RetrievalError::Storage)));
    // This test deliberately stores no proof data and mints no availability or custody.
    assert!(archive.read([3; 32], 1, 0, false).unwrap().is_none());
}
