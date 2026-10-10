# Packed-state reconstruction and blob optimizations

Historical measurements for `38b70ff`. The subsequent [PR 127 reaudit response](experimental-state-packing-reaudit-response.md) adds an inline record tier and new experimental versions; this report's compatibility and timing statements describe the earlier revision.

This patch follows audit-fix commit `0a66cde013ecb8935632aa12df7d15c05dc443c3` on `feat/experimental-state-packing`. It applies only when `--db.experimental-state-packing dense|integer32` is enabled. Native minimal storage, pruning settings, codec IDs, experimental database versions and the persisted fixed-depth marker remain unchanged. Existing experimental directories can reopen without conversion.

## Implementation

- Upper-record invalidation is per contract; lower-region invalidation is per contract/prefix. Contract wipes and table clears provide broader invalidation boundaries. Retained-trie epochs are scoped to the affected contract, except for table clears.
- A transaction-local LRU shares reconstructed records between cursors, admitting at most 64 entries and an estimated 8 MiB. Each cursor can retain references to its active records; evicted references and temporary rebuild memory are outside the shared admission budget. Independent transactions never share this cache. Concurrent misses can still compute the same region independently.
- A writer captures immutable retained records before the first storage mutation of a contract, with a separate estimated 8-MiB transaction-wide budget. Unchanged hashed children are fed to the canonical HashBuilder; only uncovered storage ranges are hashed as leaves. Existing upper records inside skipped subtrees are retained and affected records are regenerated. State-only commit repair uses the same algorithm. Roots supplied by the production writer are tracked against that contract's generation, so subsequent writes to other contracts do not force redundant repair.
- Missing/oversized summaries, contract/table wipes and trie-only edits before baseline capture select full reconstruction. Long-prefix contracts with no retained upper records can still require a complete scan. The fixed-depth 262,144-leaf lower-region limit remains.
- Read-only snapshots bypass overlay/generation locks because their overlay and generations are immutable and empty.
- Deletion-time merging considers adjacent blobs of the same contract when the affected blob has at most 128 rows and the combined result has at most 384 rows. A full predecessor does not prevent trying an eligible successor. Successors with remaining pending mutations are not consumed. Re-encoding and removal/replacement of anchors occur in the existing write transaction. The 512-row split target provides hysteresis. Inserts before the first contract anchor extend the first blob rather than creating a one-row prefix blob.

Persisted adaptive cutoff maps are a separate format milestone. This patch does not implement adaptive retention, change the format, or remove its large-region error.

## Validation and measurements

The focused sparse-update regression has 8,192 hashed slots, changes one slot and performs 100 production-style trie seek/delete/upsert operations. It checks one upper reconstruction and fewer than 256 hashed upper leaves instead of the previous full 8,192-leaf pass. The validated run hashed **5 upper leaves**, a **99.94% reduction in upper-leaf hashing** for this sparse fixture. It measured 49.17 ms for dense and 52.13 ms for integer32 in an unoptimized build. The count excludes lower-region reconstruction and storage reads used to navigate skipped ranges; it is not a reduction in all node work. Complete canonical records, including masks and hashes, are checked rather than only the root.

Additional regressions cover cross-cursor sharing, alternating regions, unrelated-contract writes, same-contract writes in another region, contract/table clearing and reinsertion, LRU eviction and oversized admission rejection. Mutation tests compare complete tries before and after commit, across snapshots and after reopening, including long common prefixes. Blob tests cover abort, old snapshots, first-anchor removal, reinsertion, reopening and successor merging when a predecessor is full. Production root/multiproof differential tests additionally exercise sparse updates, insertions and deletions in both modes.

The manual provider benchmark uses one 65,536-slot contract, full hashed keys and a mix of full-width and small values. The update changes 64 slots and commits a state-only repair. Its native control explicitly rebuilds the full trie, so this row does **not** compare packed writes against native incremental block persistence. It also measures live affected table pages, initial population, random reads, sequential reads and a multiproof.

The blob-target benchmark compares 128/256/512 rows across three rounds per codec. It measures live pages, random reads, a 64-slot sparse update and deletion of seven eighths of the rows. The targets are test-only controls; the normal backend remains at 512 rows.

| Check | Result |
| --- | --- |
| Database unit/property suite | 58 passed; manual blob benchmark ignored |
| Production roots/multiproofs | 2 passed; manual provider benchmark ignored |
| Strict changed-crate Clippy | Passed with `--all-targets --no-deps -- -D warnings`; six existing dependency warnings remain |
| Formatting and whitespace | Focused nightly formatter check and `git diff --check` passed |
| Node execution/proof/reorg/restart | Passed across native, dense and integer32; 1 test, 5.85 seconds |

Native dependencies use the bundled RocksDB 10.4.2 source/version. The final runner reused the successfully built exact-version archive through its supported `ROCKSDB_LIB_DIR`/`ROCKSDB_STATIC` options, while Snappy and Rust code were compiled normally.

The first node test binary could not start: ELF inspection found 2,729 zeroed entries inside its `DT_RELACOUNT` range. Copying it to Linux `/tmp` preserved its SHA-256 and the same loader failure. Regenerating the test binary with GNU ld produced zero invalid relative-relocation entries and the node test passed. No storage-source workaround was needed. The node test retains the previously documented short-chain pruning overrides; this is not a steady-state minimal pruning experiment.

## Matched debug benchmark results

Measured 10 October 2026. Both provider executables use Rust 1.95.0, LLD 22.1.2 and an unoptimized development profile. Three before/after rounds alternated their order in the second round; medians are shown. Compilation was idle and the prior devnet EL/CL containers were stopped during timing. Every benchmark retained its correctness assertions. Raw outputs, compiler/binary hashes and `optimize-measurements.json` are in `experiments/reth-packing-devnet/` outside the source repository.

| Measurement | Dense before | Dense after | Integer32 before | Integer32 after |
| --- | ---: | ---: | ---: | ---: |
| Affected live tables (MiB) | 3.301 | 3.305 | 3.297 | 3.301 |
| Initial population + commit (ms) | 769.337 | 1374.199 | 743.302 | 1304.486 |
| Random storage lookup (microseconds) | 106.148 | 103.125 | 79.159 | 83.751 |
| Sequential row (microseconds) | 3.930 | 3.890 | 3.686 | 3.958 |
| Multiproof (ms) | 22.881 | 24.300 | 22.129 | 26.670 |
| 64-slot state-only update + repair (ms) | 3707.765 | 193.224 | 3657.948 | 211.679 |

The sparse-update result isolates an important benefit: unchanged retained child hashes replace the full-contract upper hash pass. It does not establish release node capacity or native incremental persistence performance.

Dense: sparse state-only update elapsed time decreased **94.79%**. Initial population elapsed time increased **78.62%**, and multiproof time increased **6.20%**. These initial-population/proof regressions are material; this patch should not be described as faster for every workload. Per-write generation bookkeeping and the new reconstruction/cache path require optimized profiling before choosing further changes.

Integer32: sparse state-only update elapsed time decreased **94.21%**. Initial population elapsed time increased **75.50%**, and multiproof time increased **20.52%**. These initial-population/proof regressions are material; this patch should not be described as faster for every workload. Per-write generation bookkeeping and the new reconstruction/cache path require optimized profiling before choosing further changes.

The native full-rebuild control measured 3416.290 ms before and 3382.255 ms after. Packed live table medians changed by one 4-KiB page; there is no demonstrated extra initial-state compression from this patch. HashMap insertion order and MDBX page occupancy can vary between fresh databases. The codec/retention layout is unchanged.

## Blob target medians

| Mode | Target rows | Live MiB | Random read microseconds | Sparse update ms | Delete-heavy commit ms | Remaining blobs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dense | 128 | 4.227 | 32.191 | 133.770 | 2671.541 | 86 |
| Dense | 256 | 3.207 | 54.097 | 153.202 | 2533.975 | 43 |
| Dense | 512 | 3.199 | 100.563 | 187.299 | 2243.078 | 22 |
| Integer32 | 128 | 4.227 | 30.384 | 129.365 | 2672.248 | 86 |
| Integer32 | 256 | 3.207 | 46.859 | 149.963 | 2358.739 | 43 |
| Integer32 | 512 | 3.195 | 82.571 | 184.981 | 2448.040 | 22 |

The smaller targets improve random reads and sparse updates on this fixture. The 128-row target uses about 32% more live pages than 512; 256 uses less than 0.4% more. The 512-row target has the smallest live footprint, fewer residual blobs and generally better deletion-heavy commits here, so it remains the default. A configurable 256-row performance option is a possible follow-up after optimized tests on representative values; this test does not justify silently changing the storage-focused default.

The deletion phase leaves 8,192 logical rows from 65,536. With the 512-row target, merging leaves 22 blobs; retaining each original range independently would leave 128 undersized blobs. This is an occupancy benefit in a deliberately deletion-heavy experiment, not a measured whole-node byte reduction. Freed MDBX pages remain reusable allocation.

No new release devnet TPS, sync throughput, long-term pruning, maximum throughput or whole-node storage reduction was measured. The previous 30-minute devnet figures remain baseline results. Persisted adaptive retention is still outside this compatible patch.


## Reproduce

```sh
cargo test --locked -p reth-db --lib -- --test-threads=1
cargo test --locked -p reth-trie-db --features reth-provider/jemalloc --test experimental_packing
cargo clippy --locked -p reth-db --all-targets --no-deps -- -D warnings
cargo test --locked -p reth-node-ethereum --features reth-provider/jemalloc --test e2e experimental_packing
cargo test --locked -p reth-db benchmark_blob_targets -- --ignored --nocapture --test-threads=1
cargo test --locked -p reth-trie-db --features reth-provider/jemalloc --test experimental_packing benchmark_packed_storage -- --ignored --nocapture --test-threads=1
```

On this Windows-mounted WSL target, the successful node test executable was built with:

```sh
cargo rustc --locked -p reth-node-ethereum --features reth-provider/jemalloc --test e2e -- \
  -C linker=cc -C link-arg=-fuse-ld=bfd
# Run the produced e2e executable with: experimental_packing --nocapture.
```

Run optimized builds for throughput conclusions. Debug-build smoke timings and hashed-leaf counts cannot establish new node TPS or a percentage change in release block persistence. The earlier 30-minute Kurtosis results describe the audit-fixed baseline and must not be presented as measurements of this patch. Whole-node savings still require a mature, pruned workload and an allocation census, including unchanged history/static files.
