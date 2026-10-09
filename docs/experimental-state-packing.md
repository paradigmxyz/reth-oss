# Experimental packed state for minimal nodes

This branch adds an opt-in database layout. A normal `reth node --minimal` keeps the native MDBX schema and encodings. The experimental blob table is created only when the new flag is present. Account state, account trie, history, receipts, code, static files, and RocksDB retain their existing layouts.

## Run

Use a fresh, dedicated data directory and storage V2 (the default):

```sh
# Approach 1: dense native Compact values plus storage-trie reconstruction.
reth node --chain sepolia --minimal --storage.v2=true \
  --datadir ./sepolia-packed-dense \
  --db.experimental-state-packing dense

# Approach 2: the same provider plus adaptive groups of 32 integers.
reth node --chain sepolia --minimal --storage.v2=true \
  --datadir ./sepolia-packed-integer \
  --db.experimental-state-packing integer32
```

The persisted `--db.experimental-trie-depth` defaults to 3 and accepts 1 through 8. Nodes with paths shorter than the depth remain on disk. Other storage-trie branch records are reconstructed. Greater depth retains more records and makes reconstruction regions smaller. Reopening requires the same mode and depth. The node command rejects this flag without `--minimal` or with storage V2 disabled.

Existing native databases and snapshots cannot be converted in place. Neither mode is a decoder for the snapshots at snapshots.reth.rs. Native databases cannot be opened using this flag; experimental databases cannot be opened without it. Dense and integer formats use dedicated database versions 10001 and 10002 and an `experimental-packing.config` marker. These numbers are local experimental formats, not upstream Reth schema versions. Preserve the marker with backups.

## Layout and provider

`ExperimentalPackedStoragesV1` is an ordinary MDBX table. Its key consists of the 32-byte contract hash and the first 32-byte slot hash in its blob. Blobs contain up to 512 ordered storage rows. Splits distribute rows evenly. Hashed slot keys remain full width: cryptographic hashes offer little useful delta compression.

Every blob has a magic value, encoding tag, count, payload length, offset width, and 64-bit corruption checksum. Keys form a contiguous array. Dense mode uses an offset per value and Reth's canonical Compact U256 bytes. Integer mode uses an offset per group of at most 32 values. Each group independently chooses raw Compact values or a minimum base and bit-packed deltas, whichever is smaller. It supports all 256-bit values, constant groups, and partial final groups. Sixteen-bit offsets are used when possible, otherwise 32-bit offsets. The checksum detects ordinary corruption; Ethereum state roots provide canonical state integrity.

The MDBX transaction/cursor adapter presents the existing typed `HashedStorages` and `PackedStoragesTrie` interfaces. Reth's database hashed cursor factory, trie cursor factory, root calculator, and proof generator therefore consume logical rows without a separate RPC implementation. The physical native `HashedStorages` table is empty in experimental databases. The default cursor forwards to the preserved native MDBX implementation.

Writers buffer storage mutations in a transaction-local ordered map. All cursors in that transaction see those mutations. Commit rewrites affected blobs atomically within the same MDBX transaction. Readers use the original MDBX snapshot; abort discards pending changes. A newly committed state must have matching upper trie records. If the normal trie writer did not supply a root after the last state mutation, commit regenerates the affected contracts' retained upper nodes. No lower trie records are persisted.

Trie reconstruction streams storage leaves in hashed-key order through the same Ethereum HashBuilder. It preserves full 64-nibble paths and emits canonical branch records and masks. It combines retained upper nodes with one regenerated prefix region in global path order. Each cursor keeps one validated storage blob, one decoded integer group, and one reconstructed region. Random dense lookups decode one value; integer lookups decode at most 32 values. Sequential scans reuse the group cache. Caches are scoped to a transaction and invalidated by its storage mutation generation; clean upper caches additionally observe a shared retained-trie epoch. Dirty upper caches survive trie persistence while storage is unchanged. There is no cross-snapshot cache. The legacy 65-byte trie adapter is rejected for this layout.

Failed cursor seeks retain a boundary or explicit missing state, so iteration cannot wrap to the beginning. See the [audit response](experimental-state-packing-audit-response.md) for the cursor fixes, differential regressions, and follow-up designs.

## Current limits

This is an experimental storage backend, not a production release or a demonstrated 25% whole-node saving. The cutoff is a fixed nibble depth, rather than the adaptive leaf-count cuts described in the earlier plan. A lower region exceeding 262,144 leaves returns an error. There is no shared LRU, reconstruction-task coalescing, or adaptive large-contract fallback yet.

A dirty contract can require a complete streaming hash pass to supply current upper trie nodes. A state-only commit can also require such a pass. These costs can dominate execution for large, frequently modified contracts. Blob updates amplify writes, and deletion-heavy workloads can leave small blobs. Pending writes scale with transaction size. The reconstruction limit bounds leaves per lower region, not total process memory or total proof work.

Account packing, node-wide pruning changes, migration from native snapshots, and a production recovery/migration tool are outside this implementation. Use the V2 trie adapter when accessing these databases programmatically. Native-format repair tools are not interchangeable with this backend.

The pinned baseline also has a short-chain minimal V2 restart issue before its full-prune sender checkpoint exists. A native control reproduced the same startup unwind-to-zero assertion. The node integration test uses pruning interval 1 and minimum distance 0 on its two-block chain to establish that checkpoint; those are test overrides, not changes to the normal minimal preset. See the [validation report](experimental-state-packing-report.md) for the exact tested scope.

## Validate and measure

```sh
cargo test -p reth-db
cargo test -p reth-trie-db --test experimental_packing
cargo test -p reth-node-core args::database
cargo test -p reth-node-ethereum --test e2e experimental_packing

# Explicit DB-backed synthetic benchmark; optimization matters for timing.
cargo test -p reth-trie-db --test experimental_packing --release \
  benchmark_packed_storage -- --ignored --nocapture --test-threads=1
```

The benchmark uses one 65,536-slot contract, hashed keys, and a mixture of one-byte, small-integer, and full-width values. It prints live allocated MDBX table pages (including overflow pages), initial population plus commit time, random lookups, sequential scans, a multiproof, and state-only update plus trie repair time. The native comparison also rebuilds its trie for the state-only update; that row is not a benchmark of native incremental block execution. These results cannot be averaged into node throughput or extrapolated directly to Sepolia disk size.

Whole-node reduction must be computed from an actual minimal-mode table census: sum the saved bytes in affected tables and divide by total allocated node storage. Before production use, benchmark release builds against real Sepolia state, normal block execution, sync, reorgs, contract wipes, proofs, long-lived readers, database growth, and crash/restart workloads.
