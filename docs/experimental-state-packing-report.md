# Packed-state implementation and measured smoke results

The implementation is on `feat/experimental-state-packing`, based on reth-oss commit `4e64212dff9d7f60a6214fa518b53b6e1b00121d` (Reth 2.7.0). It supplies dense and integer32 modes behind `--db.experimental-state-packing`, for fresh minimal-mode V2 databases. The default minimal database format is preserved.

The subsequent [audit response](experimental-state-packing-audit-response.md) fixes repeated upper-trie hashing during writes and iteration after failed seeks. Its focused writer measurement is separate from the original fixture below; the disk/read/proof measurements here have not been rerun or extrapolated into new throughput claims.

## Scope

Both modes pack hashed contract storage and reconstruct omitted lower storage-trie branches. Account state, account trie, code, history, static files, and RocksDB use their native formats. The provider supports MDBX snapshots, pending writes visible across cursors, abort, commit, ordering, and the production hashed/trie cursor factories. See [usage and design](experimental-state-packing.md).

## Measured fixture

One contract with 65,536 storage slots, full hashed keys, and a mixture of U256::MAX, one-byte values, and small integers. Retained trie depth: 3. Blob target: 512 rows; integer group: at most 32 values. MDBX page size: 4,096 bytes. Tests ran using Rust 1.95 in WSL Ubuntu, on a Windows-mounted workspace. Temporary databases lived in the Linux temporary directory. These are **unoptimized development-build timings**, not release throughput predictions. RocksDB was linked from the matching static library produced by the node build; the measured tables are MDBX tables.

| Measurement | Native | Dense | Integer32 |
|---|---:|---:|---:|
| Live allocated pages, affected tables | 4,943,872 B | 3,444,736 B | 3,440,640 B |
| Reduction in affected tables | - | 30.32% | 30.41% |
| Populate and commit | 190.400 ms | 1,323.505 ms | 1,366.110 ms |
| Random storage lookup | 1.426 us | 175.013 us | 146.645 us |
| Sequential scan, per row | 0.637 us | 7.357 us | 6.881 us |
| Multiproof | 13.068 ms | 39.694 ms | 40.750 ms |
| State-only update plus full trie repair | 5,189.987 ms | 6,212.700 ms | 6,318.164 ms |

Allocated bytes include live MDBX branch, leaf, and overflow pages of hashed storage, storage trie, and the experimental blob table. They exclude MDBX free pages and preallocation, account tables, and all other node storage. Population includes storage and precomputed trie-record writes. Random reads use 4,096 deterministic dispersed lookups. The scan reads all slots. Proof generation targets dispersed leaves. The update changes 64 slots; native mode also performs a full-contract trie rebuild for this measurement, so that last row does not compare against native incremental block execution.

## Interpretation

The synthetic affected-table reduction exceeds the requested 25% target. This does **not** establish a 25% reduction in a complete minimal Sepolia node. At 30.32% savings in affected tables, those tables would need to represent approximately 82.5% of the node's total allocated storage to achieve a 25% whole-node reduction. An actual table census is required.

Integer32 adds only about 0.08 percentage points of reduction on this fixture. Hash ordering mixes full-width and small values inside each group, frequently selecting the raw fallback. It may help more on contracts with consistently narrow integers or nearly constant values; those workloads need separate measurement.

The debug timing penalties are substantial: approximately 123x/103x slower random lookups, 11.6x/10.8x slower scans, and 3.0x/3.1x slower multiproofs for dense/integer32. Expressed as per-operation throughput losses, those are about 99.2%/99.0%, 91.3%/90.7%, and 67.1%/67.9%, respectively. They cannot be averaged into node throughput. Debug checksum/index validation over a blob and logical cursor serialization are significant costs. A dirty-contract upper-trie rebuild can also dominate block processing. This version should remain experimental.

Before recommending deployment, run the release benchmark, replace the one-blob cache with a memory-budgeted cache if justified, profile validation/serialization, and measure incremental block execution and real-state contract distributions. Adaptive cuts and a bounded shared reconstruction cache remain further work. The implementation deliberately fails when a lower region exceeds 262,144 leaves rather than silently returning an incomplete trie.

## Reproduce

```sh
cargo test -p reth-db
cargo test -p reth-trie-db --test experimental_packing
cargo test -p reth-node-core args::database
cargo test -p reth-node-ethereum --test e2e experimental_packing
cargo test -p reth-trie-db --test experimental_packing --release \
  benchmark_packed_storage -- --ignored --nocapture --test-threads=1
```

Using `--features reth-provider/jemalloc` for the provider/node tests matches the standard node's native allocator configuration. The smoke numbers above were collected without `--release`; use that same command without `--release` to reproduce the development-build experiment.

## Completed validation

| Check | Result |
|---|---|
| `cargo test -p reth-db` | 48 passed, including codec properties, malformed inputs, snapshots, abort, ordering, updates, clearing, and directory guards |
| Production trie factory differential test | Passed for dense and integer32; native-equivalent roots and multiproofs before/after updates and reopening |
| Database argument tests | 20 passed |
| Node execution/proof/reorg/restart test | Passed in 10.84 seconds; native, dense, and integer32 each deployed 512 storage slots, updated state, read proofs, persisted a competing block, and reopened the node |
| DB-backed synthetic benchmark | Passed; measurements above |
| Clippy | Passed without warnings for `reth-db`, `reth-node-core`, and `reth-cli-commands`, including all targets |
| Formatting and dependency checks | Formatting, `zepter run check`, and `make SHELL=/bin/bash lint-toml` passed |
| CLI integration | Standard node built, flag help/startup rejection checked, and CLI reference pages regenerated |

The node test enables the minimal preset and V2, with pruning interval 1 and minimum pruning distance 0 for its two-block fixture. The preset's distance-based account/storage history retention remains 10,064 blocks. Transaction inclusion is checked directly because transaction-hash lookup can already be pruned. The test waits for the fully-pruned sender checkpoint and the replacement block's exact persisted hash before restarting.

A control run exposed a restart issue in the pinned baseline: on a two-block minimal V2 chain with the default pruning safety distance, startup requested an unwind to zero because the sender static-file segment was absent without a full-prune checkpoint. The native backend reproduced it. This change preserves that baseline behavior; the test's explicit pruning settings permit a checkpoint on the short chain. This passing test is not evidence that the baseline issue is fixed or that arbitrary crash recovery, real Sepolia replay, or default-preset restart on a short chain has been validated.
