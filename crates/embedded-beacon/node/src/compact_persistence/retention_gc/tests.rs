//! Real shared-owner crash-window recovery, without consensus or custody authority.

use super::*;
use reth_embedded_beacon_shared_data::{Encoding, ObjectKind};
use tempfile::TempDir;

fn interrupted(domain: RetentionDomain, shared: &SharedDataStore, epoch: u64, root: Root) {
    let mut descriptor = Deletion {
        schema: 1,
        identity: shared.identity(),
        consensus_digest: [19; 32],
        domain,
        epoch,
        roots: vec![root],
        checksum: [0; 32],
    };
    descriptor.checksum = sha256(&serde_json::to_vec(&descriptor).unwrap());
    publish_file(
        &domain.directory(shared).join("pruning.json"),
        &serde_json::to_vec(&descriptor).unwrap(),
    )
    .unwrap();
}
#[test]
fn interrupted_unlink_before_pin_release_resumes_idempotently_preserving_other_domain() {
    for domain in [RetentionDomain::Contexts, RetentionDomain::Sidecars] {
        let temporary = TempDir::new().unwrap();
        let shared = SharedDataStore::open(
            temporary.path(),
            StoreIdentity { genesis_root: [17; 32], config_digest: [18; 32] },
            0,
        )
        .unwrap();
        let root = [21; 32];
        let other = Owner::Consensus([22; 32]);
        let object = shared
            .put(
                ObjectKind::BeaconBlock,
                Encoding::Ssz,
                b"immutable proof-input bytes",
                domain.owner(root),
            )
            .unwrap();
        shared.pin(other, object).unwrap();
        let epoch = domain.directory(&shared).join(format!("{:020}", 9));
        fs::create_dir_all(&epoch).unwrap();
        interrupted(domain, &shared, 9, root);
        fs::remove_dir_all(&epoch).unwrap();
        // This is the interrupted process boundary: the pin still exists after metadata unlink.
        assert_eq!(shared.owner_objects(domain.owner(root)), vec![object]);
        domain.resume(&shared, [19; 32]).unwrap();
        assert!(shared.owner_objects(domain.owner(root)).is_empty());
        assert_eq!(shared.owner_objects(other), vec![object]);
        assert!(!domain.directory(&shared).join("pruning.json").exists());
        domain.resume(&shared, [19; 32]).unwrap();
        assert_eq!(shared.owner_objects(other), vec![object]);
    }
}
#[test]
fn interrupted_descriptor_before_unlink_resumes_and_wrong_identity_never_releases_pins() {
    let temporary = TempDir::new().unwrap();
    let shared = SharedDataStore::open(
        temporary.path(),
        StoreIdentity { genesis_root: [17; 32], config_digest: [18; 32] },
        0,
    )
    .unwrap();
    let domain = RetentionDomain::Contexts;
    let root = [21; 32];
    let object = shared
        .put(ObjectKind::BeaconBlock, Encoding::Ssz, b"immutable original", domain.owner(root))
        .unwrap();
    let epoch = domain.directory(&shared).join(format!("{:020}", 9));
    fs::create_dir_all(&epoch).unwrap();
    interrupted(domain, &shared, 9, root);
    assert!(matches!(domain.resume(&shared, [20; 32]), Err(StoreError::CorruptJournal)));
    assert_eq!(shared.owner_objects(domain.owner(root)), vec![object]);
    assert!(epoch.exists());
    domain.resume(&shared, [19; 32]).unwrap();
    assert!(shared.owner_objects(domain.owner(root)).is_empty());
    assert!(!epoch.exists());
}
