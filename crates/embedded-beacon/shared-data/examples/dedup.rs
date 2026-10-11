//! Measures duplicate immutable storage bytes for identical deterministic input.

use reth_embedded_beacon_shared_data::{
    Encoding, ObjectKind, Owner, SharedDataStore, StoreIdentity,
};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TempDir::new()?;
    let identity = StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] };
    let execution = SharedDataStore::open(directory.path().join("execution"), identity, 0)?;
    let consensus = SharedDataStore::open(directory.path().join("consensus"), identity, 0)?;
    let shared = SharedDataStore::open(directory.path().join("shared"), identity, 0)?;
    let pool_owner = Owner::Pool([3; 32]);
    let consensus_owner = Owner::Consensus([4; 32]);
    let blob = vec![17u8; 131_072];
    let proof = vec![23u8; 48];
    for (kind, bytes) in
        [(ObjectKind::Blob, blob.as_slice()), (ObjectKind::Proof, proof.as_slice())]
    {
        execution.put(kind, Encoding::Raw, bytes, pool_owner)?;
        consensus.put(kind, Encoding::Raw, bytes, consensus_owner)?;
        shared.put(kind, Encoding::Raw, bytes, pool_owner)?;
        shared.put(kind, Encoding::Raw, bytes, consensus_owner)?;
    }
    let a = execution.stats()?;
    let b = consensus.stats()?;
    let c = shared.stats()?;
    let separate_bytes = a.physical_bytes + b.physical_bytes;
    let separate_objects = a.physical_objects + b.physical_objects;
    assert_eq!(separate_bytes, 2 * c.physical_bytes);
    assert_eq!(c.logical_bytes, separate_bytes);
    let result = serde_json::json!({
        "workload": "identical_raw_blob_and_proof_bytes_with_two_owners",
        "separate_physical_object_bytes": separate_bytes,
        "shared_physical_object_bytes": c.physical_bytes,
        "saved_physical_object_bytes": separate_bytes - c.physical_bytes,
        "separate_immutable_object_files": separate_objects,
        "shared_immutable_object_files": c.physical_objects,
        "logical_owner_bytes": c.logical_bytes,
        "owner_pins": c.owner_pins,
        "metadata_and_active_reader_memory_included": false,
        "live_sync_performance_measured": false
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
