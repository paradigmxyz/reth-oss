# Shared data storage

`SharedDataStore` stores exact immutable bytes once per semantic kind, encoding and SHA-256 digest. `Pool`, `Consensus` and `Recovery` owners hold independent durable pins. Pool eviction releases its pins only. Garbage collection commits removal of references before unlinking unused files; bytes already returned in an `Arc` remain available to readers.

Objects are published before reference generations. A complete snapshot becomes visible through a rename; partially written `.pending` generations are ignored during recovery. Snapshots carry schema, genesis root, consensus configuration digest, generation and checksum. Corrupt committed generations fail closed. An operating-system file lock prevents two independently opened writers. Clones share one writer. File contents are synced before publication; Unix also syncs the parent directory. Windows syncs the published file but power-loss directory durability has not been tested.

The default byte cache is 256 MiB. This bound excludes bytes held by callers and filesystem metadata. Methods are blocking and should run in blocking tasks when called from async code. Reference metadata uses complete generation snapshots, bounded to 64 MiB, so this first implementation requires a more scalable journal/index backend before large archival deployment.

The optional `reth` feature exports `SharedBlobStore::new(shared)`, implementing the execution pool's existing blob-store interface. It stores raw blobs, commitments and proofs separately and records their composition in a small descriptor. Sparse cells are independent `Cell` objects and are never exposed as complete sidecars. Shape validation does not establish signature, KZG or beacon contextual validity; existing pool and consensus validation remain mandatory.

Physical object bytes include uncollected orphans. Logical owner bytes count each immutable object once per owner pin; `BlobStore::data_size_hint` separately reports execution-pool sidecar sizes. Metadata and caller-owned cache allocations are excluded from physical object statistics.

Run the deterministic duplicate-input measurement with:

```text
cargo run -p reth-embedded-beacon-shared-data --example dedup
```

The JSON compares identical blob and proof bytes kept in two independent stores against two owners in one shared store. It measures object files and bytes, without claiming live synchronization throughput or total-datadir savings.
The measured deterministic run used 262,240 object bytes in four independent-store files and 131,120 bytes in two shared-store files, saving 131,120 object bytes (50%). Both owners still account for 262,240 logical bytes and four pins. The result is saved in `../../../testing/embedded-beacon/artifacts/dedup.json` and applies only to identical immutable inputs.

Pool pins intentionally survive restart. Before enabling this facade for a running node, startup must reconcile persistent pool owners with the reconstructed in-memory pool so abandoned transactions do not retain objects indefinitely. The current pool facade performs durable writes synchronously; pool integration needs a measured bounded blocking path before live performance or storage-lifecycle acceptance.

Pool sidecar lookups use a derived versioned-hash index and incremental logical byte
and transaction counts. Indexes rebuild from durable descriptors when pool ownership
changes outside the facade; consensus pins do not invalidate them. Poisoned writers
still reject lookups. Multiple sidecar insertions replace their owners in one atomic
reference generation, after all immutable files are published.

Measure synthetic batch insertion, metadata queries and single-hash lookup with:

```text
cargo run -p reth-embedded-beacon-shared-data --features reth --example pool_index
```

The measurement reports operation times, reference commits and object reads for
metadata queries at several pool sizes. Synthetic shape validation does not
establish KZG validity or public-network performance.
