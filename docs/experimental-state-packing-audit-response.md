# Experimental state packing audit response

Audited baseline: `6ae848ff2141f4de2d7bdaea18bb3a098c841cd6`, branch `feat/experimental-state-packing`.

## Confirmed issues

### P1: repeated full-contract hashing during trie persistence

Fixed. The upper-node cache now distinguishes the storage mutation generation from an independent retained-trie epoch. While a contract is dirty, its upper nodes come from the current canonical storage view. Retained-node replacements, deletions, and discarded lower-node writes therefore preserve that cache until storage changes. A storage mutation still invalidates both upper and lower reconstruction caches.

For a clean contract, the cache can contain physical retained records. Successful physical replacements/deletions and table clearing increment a transaction-shared trie epoch, so reads through other cursors also see those changes. Discarded lower-node writes do not increment it. All epochs and caches remain transaction-local; no cross-snapshot cache was introduced.

The regression uses 8,192 persisted slots, changes one slot, then performs 100 production-style seek/delete/upsert replacements over canonical branch paths in each mode. Test-only counters assert exactly one upper reconstruction and 8,192 upper-pass leaves after every replacement. Deleting all trie duplicates also preserves the derived dirty-state cache. A subsequent storage write must produce a second reconstruction and a new canonical root.

The first post-fix development-build run measured 566 ms for dense and 521 ms for integer32 for the 100 replacements, excluding initial population and commit. These timings are an additional focused smoke measurement, not a matched rerun of the auditor's source, a release-throughput result, or a whole-node benchmark. The deterministic reconstruction-count assertions are the regression threshold; elapsed time is not asserted. Lower-prefix reconstruction and a full-contract pass when the storage generation changes remain possible costs.

### P2: iteration wrapping after an unsuccessful seek

Fixed. A logical cursor now has explicit uninitialized, positioned-row, failed-range-boundary, and missing/exhausted states. A failed range seek retains its requested boundary, preventing forward iteration from returning earlier rows. Failed exact/scoped seeks and ordinary iteration exhaustion do not act like fresh cursors. Explicit `first`, `last`, a successful seek, or a write can position the cursor again.

Differential tests compare native, dense, and integer32 for both hashed-storage and storage-trie cursors: walks past the final contract, repeated forward calls, backward positioning after a failed range seek, failed exact seeks, end-of-contract iteration, missing duplicate-walk contracts, and failed subkey seeks within an existing contract. Retained-trie replacement/deletion/clear visibility is separately tested across cursors.

The changes do not alter blobs, codec IDs, database versions, the configured retention depth, or native MDBX operations. Existing experimental directories reopen with their original flags. Lower trie records remain omitted.

## Validation results

| Check | Result |
| --- | --- |
| `reth-db` unit/property tests | 52 passed, including four new audit regressions |
| Production provider root/proof differential | 1 passed; the existing manual benchmark remains ignored |
| Node execution/proof/reorg/restart test | Passed across native, dense, and integer32 |
| `cargo +nightly fmt --check -p reth-db` | Passed |
| `cargo clippy -p reth-db --all-targets` | Passed with no warnings in `reth-db`; existing dependency warnings remain |

A second full database-test run measured 748 ms for dense and 572 ms for integer32 for the same writer regression. Across these development-build runs, the focused 100-replacement loop took 0.52–0.75 seconds; both modes still reconstructed the upper nodes exactly once. These concurrent-suite timings are not a controlled comparison with the audit's 120-second reproduction.

## Additional suggestions

### Adaptive retention: separate format milestone

The suggestion is sound, but is deferred from the cursor fixes. It requires persisted cut decisions and atomic coordination with storage, retained nodes, and transaction overlays. Inferring different cuts on reopening would make the physical layout ambiguous.

Proposed implementation:

1. Introduce a versioned contract/prefix cutoff directory and new experimental format versions. Keep the existing fixed-depth format readable; require an explicit offline rebuild to switch an existing directory.
2. Partition each contract into nonoverlapping prefixes. Compare thresholds of 64, 128, and 256 leaves; subdivide overloaded prefixes and retain the additional canonical branches required above those cuts. Use original full nibble paths and preserve the full logical masks.
3. Apply cut splits/merges, upper-node updates, storage changes, and metadata in the same MDBX transaction. Include a cutoff epoch in reconstruction cache validation, and provide a pending-cut view during writes and reverts.
4. Differentially verify complete cursor order, roots, proofs, storage clearing, long common prefixes, rollback, restart, and metadata corruption. Measure directory/retained-node pages and p95/p99 reconstruction latency before choosing the threshold.

The current fixed cutoff and 262,144-leaf lower-region error remain in place. This response does not claim that adaptive retention or removal of that limit is implemented.

### Blob merging: compatible follow-up

Merging can reuse the current format, but is deferred so its extra reads and write amplification can be measured independently of the correctness fixes.

Proposed implementation:

1. During deletion batches, inspect only the affected blob's immediate same-contract neighbors. Avoid a whole-contract compaction scan.
2. Start with a low-water occupancy of 128 rows and a merged maximum of 384 rows, below the 512-row split target. Measure these thresholds; the gap provides hysteresis against immediate splitting on small subsequent inserts.
3. Apply all pending changes for the selected blobs before encoding their merged rows. Atomically remove both old anchors and write the new first-slot anchor in the existing MDBX transaction. Never merge across contracts or change logical ordering or codec interpretation.
4. Test clustered/random deletion, first-slot removal, empty blobs, inserts into merged ranges, clearing, both codecs, old read snapshots, abort, commit, and reopening. Compare live table pages and bytes rewritten per logical deletion. Reclaimed MDBX free pages do not by themselves shrink the database file.

Neither suggestion is needed to fix P1 or P2. Both remain explicit follow-up work rather than implicit changes to the existing format or pruning preset.

## Reproduce the regressions

```sh
cargo test -p reth-db
cargo test -p reth-db dirty_trie_writer_hashes_upper_once_per_storage_generation -- --nocapture --test-threads=1
cargo test -p reth-trie-db --features reth-provider/jemalloc --test experimental_packing
cargo test -p reth-node-ethereum --features reth-provider/jemalloc --test e2e experimental_packing
```

The node test retains the previously documented short-chain pruning overrides. See the [implementation report](experimental-state-packing-report.md) for its baseline restart limitation and the scope of the original storage measurements. The original storage/read/proof measurements have not been replaced by this focused trie-writer timing.
