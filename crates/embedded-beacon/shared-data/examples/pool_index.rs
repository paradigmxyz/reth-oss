//! Measures pool metadata queries and indexed lookup over deterministic synthetic sidecars.

use alloy_eips::{
    eip4844::{kzg_to_versioned_hash, Blob, BlobTransactionSidecar, Bytes48},
    eip7594::BlobTransactionSidecarVariant,
};
use alloy_primitives::B256;
use reth_embedded_beacon_shared_data::{
    SharedBlobStore, SharedDataStore, StoreIdentity, DEFAULT_CACHE_BYTES,
};
use reth_transaction_pool::blobstore::BlobStore;
use std::{hint::black_box, time::Instant};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    for count in [16u64, 64, 256] {
        let directory = TempDir::new()?;
        let shared = SharedDataStore::open(
            directory.path(),
            StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] },
            DEFAULT_CACHE_BYTES,
        )?;
        let store = SharedBlobStore::new(shared.clone());
        let mut txs = Vec::new();
        let mut last_hash = B256::ZERO;
        for index in 0..count {
            let mut tx = [0; 32];
            tx[24..].copy_from_slice(&index.to_be_bytes());
            let mut commitment = [0; 48];
            commitment[40..].copy_from_slice(&index.to_be_bytes());
            last_hash = kzg_to_versioned_hash(&commitment);
            txs.push((
                B256::from(tx),
                BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
                    blobs: vec![Blob::default()],
                    commitments: vec![Bytes48::from(commitment)],
                    proofs: vec![Bytes48::default()],
                })
                .into(),
            ));
        }
        let before = shared.try_observe().unwrap();
        let started = Instant::now();
        store.insert_all(txs)?;
        let insertion_micros = started.elapsed().as_micros();
        let after = shared.try_observe().unwrap();
        assert_eq!(store.blobs_len(), count as usize);
        let expected_size = count as usize * (alloy_eips::eip4844::BYTES_PER_BLOB + 96);
        assert_eq!(store.data_size_hint(), Some(expected_size));
        let metrics_before = shared.try_observe().unwrap();
        let metric_iterations = 1024;
        let started = Instant::now();
        for _ in 0..metric_iterations {
            black_box(store.blobs_len());
            black_box(store.data_size_hint());
        }
        let metric_micros = started.elapsed().as_micros();
        let metrics_after = shared.try_observe().unwrap();
        let lookup_iterations = 16;
        let started = Instant::now();
        for _ in 0..lookup_iterations {
            let values = store.get_by_versioned_hashes_v1(black_box(&[last_hash]))?;
            assert!(values[0].is_some());
            black_box(values);
        }
        results.push(serde_json::json!({
            "transactions": count,
            "batch_insertion_micros": insertion_micros,
            "reference_commits": after.journal_commits_total - before.journal_commits_total,
            "metric_iterations": metric_iterations,
            "metric_query_micros": metric_micros,
            "metric_object_reads": metrics_after.cache_hits_total + metrics_after.cache_misses_total
                - metrics_before.cache_hits_total - metrics_before.cache_misses_total,
            "lookup_iterations": lookup_iterations,
            "single_hash_lookup_micros": started.elapsed().as_micros(),
        }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "workload": "shape_valid_synthetic_single_blob_sidecars",
            "debug_assertions": cfg!(debug_assertions),
            "live_sync_performance_measured": false,
            "results": results,
        }))?
    );
    Ok(())
}
