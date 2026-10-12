# Trie retention strategy for packed state

## Decision and implementation status

Prefer dense storage packing with the full native storage trie when pursuing a
practical latency/storage tradeoff. Grouping storage rows can save MDBX pages
without requiring missing trie branches to be reconstructed during root and proof
reads. Full retention is implemented as an explicit option; its performance has
not been measured in the four-node comparison below. It is not a production
recommendation or a claim that the remaining blob-update overhead disappears.

The current default remains reconstruction with a fixed nibble-depth cutoff of
3. This document records the recommended direction, explains the implemented
alternative and the separately versioned adaptive implementation. It does not change
defaults, migrate existing databases, or switch any running node's layout.

| Strategy | Status | Storage representation | Storage trie |
| --- | --- | --- | --- |
| Native | Existing control | Native MDBX duplicate rows | Native complete trie |
| Dense or integer32, cutoff depth | Existing experimental default | Packed blobs | Retained upper branches and reconstructed lower regions |
| Dense or integer32, full retention | Implemented opt-in | Packed blobs | Native complete trie and native trie cursors |
| Adaptive retention v1 | Implemented opt-in; unbenchmarked | Packed blobs | Persisted cuts, bounded regions and full-trie fallback |

Accounts, the account trie, bytecode, history indices, receipts and static files
are outside this packing change. Storage V2 is required. Packing supports archive,
full and minimal history-retention modes; history retention and storage-trie
retention are separate decisions.

## Why full retention is the first direction

A release-build comparison used four minimal Reth/Lighthouse pairs, six-second
slots, Gloas/Amsterdam and a 200,000,000 block gas limit. Storage spam targeted four
storage scopes with full-width values. These are small-state measurements on a
shared host, not measurements of mainnet or maximum node capacity.

The same 100 nonempty canonical hashes, blocks 101 through 200, produced:

| Measure | Local native | Dense, depth 3 | Integer32, depth 3 |
| --- | --- | --- | --- |
| Canonical import mean | 81.69 ms | 165.34 ms | 164.93 ms |
| Canonical import p95 | 122.22 ms | 226.74 ms | 231.65 ms |
| State-root job mean | 54.54 ms | 128.65 ms | 128.33 ms |
| Execution telemetry mean | 23.80 ms | 31.44 ms | 30.88 ms |
| Persistence mean per 16-block batch | 260.70 ms | 944.69 ms | 914.78 ms |

Root jobs account for about 74 ms of the 83–84 ms additional mean import time.
These durations overlap and must not be summed as an exclusive execution
breakdown. The observations identify the root path as the main measured latency
increase; they do not identify an individual source function through a profile.
Keeping the native trie removes the need to regenerate omitted regions. Reads
and updates of packed storage can still cost more, and the persistence slowdown
also needs attention.

At a later read-only page census, all four Finish records matched at database
frontier 270 and state/trie frontier 240, with 256,169 logical storage slots:

| Live physical table bytes | Native | Dense, depth 3 | Integer32, depth 3 |
| --- | --- | --- | --- |
| Storage values | 28,606,464 | 18,751,488 | 18,587,648 |
| Storage trie | 2,826,240 | 208,896 | 208,896 |
| All current-state tables | 31,485,952 | 19,013,632 | 18,849,792 |

Dense saves 12,472,320 current-state bytes: 9,854,976 from storage pages and
2,617,344 from trie omission. Approximately 79% of this saving therefore comes
from the storage layout. Restoring the measured native trie pages while leaving
the dense storage pages unchanged would yield 21,630,976 state bytes, an
**estimated 31.3% reduction**. This is page arithmetic, not an observed full-trie
run; page allocation and occupancy can change under that mode.

Dense serialized storage records were 585,074 bytes larger than the native raw
records in that census. The physical reduction came from fewer B-tree headers,
better page occupancy and fewer retained trie nodes. Integer32 used 8,414 raw
groups and no delta groups; it saved only 163,840 additional physical bytes over
dense. Full-width hashed keys and values are not a promising source of generic
integer compression. Mainnet has a different contract/value distribution, so
neither this saving ratio nor the measured latency ratio transfers automatically.

## Implemented full-retention option

Use a fresh dedicated directory:

```sh
reth node --chain mainnet --minimal --storage.v2=true \
  --datadir ./mainnet-packed-dense-full-trie \
  --db.experimental-state-packing dense \
  --db.experimental-retain-full-trie
```

`--full`, or the normal archive retention configuration, can replace `--minimal`.
The example describes the storage choice; it does not establish production
readiness or provide a snapshot conversion path.

The full-trie flag requires an explicit packing mode and conflicts with
`--db.experimental-trie-depth`. Internally, `FULL_TRIE_DEPTH = 65` is a persisted
sentinel. It is not a 65-nibble query prefix or a user-facing cutoff: hashed slot
paths have at most 64 nibbles. Existing depth cutoffs remain 1 through 8.

The important implementation boundaries are:

1. `Tx::new_cursor` routes storage-value cursors through packed blobs, but routes
   `PackedStoragesTrie` through the native MDBX cursor in full-retention mode.
2. Normal provider trie writes call `DbTxMut::finish_storage_trie_updates` after
   writing a contract's canonical trie updates. The native backend's default is
   a no-op. The packed full-retention backend records completion for the current
   storage generation, preventing a redundant state-only repair at commit.
3. A later storage mutation invalidates completion for that generation. A commit
   without completed canonical trie writes still repairs the trie, preserving
   the state-only writer contract. This repair can scan substantial storage.
4. The empty storage-root path is internal metadata, not a native storage-trie
   row. Repair must omit it. Reconstructed cursors also omit it so cached roots
   cannot hide newer branches supplied by a pending trie overlay.
5. Region invalidation bounds prefixes to actual hashed-key length, including
   under the full-retention sentinel. Contract/table wipes and aborts preserve
   their existing atomicity and visibility rules.

Completed full-retention updates and contract wipes do not capture retained-node
baselines or create per-slot reconstruction generations. State-only repair streams
finalized canonical nodes into MDBX in batches rather than accumulating a complete
contract node map. It still hashes every leaf of the repaired contract. A whole
storage-table clear forces repair of newly populated scopes, even after provider
completion, because the table clear is applied during flush.

The `storage::db::packing` debug event `Packed state prepared for commit` reports
preparation, blob encoding, blob persistence and reconstruction durations, encoded
bytes, blobs, repaired scopes/leaves and completed scopes. These are buffered-flush
durations before the final MDBX commit, not exclusive whole-block timings. Preparation
includes decoding, sorted merges, bookkeeping, clears and adjacent-blob merges;
reconstruction includes its trie/metadata writes. Adaptive boundary maintenance and
fallback adoption happen during mutation and are outside these flush timings.

The packed blob encoding is unchanged. The directory marker includes the
retention sentinel, and reopening requires the same layout. A reconstructed
packed directory cannot be switched to full retention by adding a flag. A
native snapshot cannot be opened as packed by changing a version file or marker.
No converter is implemented by these fixes.

Relevant code is in `crates/storage/db/src/implementation/mdbx/packing/`,
`crates/storage/db/src/implementation/mdbx/tx.rs`,
`crates/storage/db-api/src/transaction.rs`,
`crates/storage/provider/src/providers/database/provider.rs` and
`crates/node/core/src/args/database.rs`.

## Implemented adaptive retention v1

Use a fresh directory with an explicit packing mode:

```sh
reth node --chain mainnet --minimal --storage.v2=true \
  --datadir ./mainnet-packed-dense-adaptive \
  --db.experimental-state-packing dense \
  --db.experimental-adaptive-trie
```

Integer32, archive and full history retention are also supported. The adaptive
flag requires packing and conflicts with fixed depth and full-trie retention.
Internal sentinel 66 selects the policy; it is never used as a nibble length.
Dense adaptive databases use version 10005 and integer32 adaptive databases use
10006, with marker `<version>:adaptive:1`. Versions 10003/10004 and their existing
cutoff/full markers keep their interpretation. Reopening with another retention
policy is rejected before opening MDBX. No migration/converter exists: resync into
a separate directory. Changing policy budgets requires another policy version.

`ExperimentalPackedTrieFrontiersV1` contains one checksummed, contract-bound record
per tracked scope. It stores sorted, nonoverlapping nibble prefixes and leaf counts,
or an explicit full-trie fallback tag. Only occupied regions are represented;
unrepresented children are empty. Cuts are loaded from the transaction's snapshot,
not rediscovered by scanning the whole contract. Invalid versions, ordering, overlaps,
counts, padding, checksums and oversized records are rejected by the bounded parser.

The policy is:

- A reconstructed region contains at most **512 leaves**, or **32 KiB** of decoded
  key/value payload. Adding the 513th leaf scans that region and subdivides occupied
  children recursively. Even a long common prefix terminates at unique 64-nibble keys.
- Region branch maps have a conservative **1 MiB estimated allocation budget**.
  At most 511 branching nodes and one active hash-builder frontier are needed for
  512 leaves. The estimate includes map/child-hash overhead; it is not allocator RSS
  accounting or a process-wide limit. One validated storage blob and cursor/cache
  references are additional memory.
- After deletion, descendant cuts merge when their combined occupancy is at most
  **256 leaves**. This hysteresis avoids splitting and merging around 512.
- At most **4,096 cuts** are persisted per contract. Exceeding that budget adopts
  complete canonical trie records using a streaming storage scan inside the writer
  transaction. Full fallback is persisted and remains until a contract wipe. This
  can cost substantial adoption time and disk space, but valid large contracts do
  not fail merely because a reconstruction region is oversized.

Retained records are canonical branch summaries outside the cuts, with canonical
child hashes/masks. An internal empty-path summary also links first-nibble cuts to
reusable child hashes; public cursors omit it. Dirty scopes regenerate changed ancestors
using immutable baseline child hashes when available. A split or merge forces final
retained-record repair even if a provider completion was recorded. Retained records
and the internal root summary are refreshed from current derived nodes or incremental
repair after every sparse adaptive storage generation; provider updates alone omit
that summary. Storage blobs, retained records and metadata publish atomically.
Aborts, contract/table wipes and older readers
keep their existing MDBX snapshot interpretation. Full-fallback scopes use physical
native trie seeks; other scopes expose canonical omitted nodes through bounded
region reconstruction. Empty root pseudo-records remain absent from the public view.

Multiproofs may touch many regions. They reconstruct regions sequentially and reuse
snapshot-local derived data through bounded cache admission; total CPU and proof
output grow with the request. There is no arbitrary total-leaf rejection for a valid
proof, nor a claim of constant total proof latency or memory. Upper summaries and
temporary cursor-held maps are separate from the existing 8 MiB blob and 8 MiB trie
cache admission budgets. Adaptive frontier metadata has its own 8 MiB/64-entry
admission cache; active cursors can retain references beyond that budget.
Dirty contract generations invalidate all its adaptive
regions, conservatively; other contracts and older snapshots remain isolated.

Initial trie construction, full-fallback adoption, table-clear repair and repair
without a usable baseline can still stream the entire affected contract. The region
limit bounds an omitted-region reconstruction, not all write/initialization work.
No coldness heuristic or migration tool is implemented. Native/full-retention results
are the correctness oracle. Storage savings and release/node performance of adaptive
v1 have not been measured; the historical devnet estimates above do not establish
its tradeoff.

Adaptive size comparisons must include the frontier metadata table as well as packed
storage blobs and retained trie pages. A full-fallback contract loses trie-omission
savings; its storage-value packing remains enabled. Per-contract metadata adds
overhead, so small contracts can expand relative to native storage.

## Rationale and requirements

Regression coverage includes native-versus-packed canonical roots and multiproofs,
the production provider's sorted trie writer, pending overlays, sparse updates,
completion followed by later mutation, state-only repair, contract/table wipes,
reopen, abort, old snapshots and cursor positioning/reversal. Adaptive tests also
exercise deep common prefixes, split/merge hysteresis, bounded malformed-metadata
parsing, and forced full fallback. The real-node test runs native, both fixed-depth
codecs, both full-trie codecs and both adaptive codecs through execution, persistence,
RPC proofs, reorg and restart with a 513-slot contract. These are correctness tests,
not release throughput or storage measurements.

If trie omission remains desirable after storage-only packing, select retained
boundaries by reconstruction work rather than by one global nibble depth.
Fixed depth treats a tiny contract and a very large contract alike. The current
262,144-leaf region limit rejects an oversized region; it is not a complete
memory or proof-work bound.

An adaptive design should retain a canonical branch summary whenever its
subtree would exceed a reconstruction budget. Large or frequently changed
subtrees keep more native branches. Small cold regions can be reconstructed.
Cold regions can still be requested by validation or proofs, so coldness alone
is not permission to discard an unbounded subtree.

Requirements addressed by the separate adaptive policy:

- Persist explicit per-contract retained/reconstructed boundaries. Do not infer
  them from a single depth flag or rebuild the whole contract to rediscover them.
- Bound work by leaves, decoded bytes and temporary memory. Define the policy for
  a proof touching many individually bounded regions. Oversized work must have a
  documented retained-data or bounded fallback path rather than an unexpected
  validation failure.
- Keep canonical child hashes, branch masks and paths at retained boundaries.
  Incremental writes must regenerate changed ancestors while preserving
  unaffected children and overlay precedence.
- Publish storage changes, trie records and boundary metadata atomically in one
  writer transaction. Readers of older snapshots must retain the old boundary
  interpretation. Abort, deletes, contract wipes and table clears must leave no
  partially adopted frontier.
- Separate on-disk retention from cache admission. Cache immutable derived
  regions by snapshot identity and contract/region generation; never reuse a
  stale hash across snapshots or let a cache entry override pending updates.
  Coalesce concurrent reconstruction only when callers share compatible state.
- Use hysteresis for retained/reconstructed transitions to avoid repeatedly
  rewriting the same frontier around a threshold.
- Assign an explicit layout/policy version and a migration path. Do not silently
  reinterpret depth-3, full-retention or existing marker versions.

Full native trie retention remains the correctness and latency baseline.
Adaptive omission remains an explicit experimental mode.

## Detailed implementation prompt

> Improve packed-state latency while retaining the grouped storage-page benefit.
> Start from the existing dense backend and implemented
> `--db.experimental-retain-full-trie` option. Keep the default and existing
> persisted layouts unchanged unless the task explicitly requests a versioned
> transition. Do not alter an active devnet, workload or build profile as part of
> implementing the storage changes.
>
> Trace normal block persistence end to end. Native storage-trie cursors must be
> used under full retention. Mark canonical trie completion only after the
> provider has written every update for that contract and current storage
> generation; later mutations must require another completion or repair. Avoid
> redundant full-contract hashing during normal commits, but preserve repair for
> state-only writes, including storage deletes and complete contract wipes. Keep
> the empty root path out of the logical/native trie rows. Pending overlays must
> remain authoritative over cached committed state.
>
> Preserve ordered range seeks, transaction-local validated blob sharing and
> sorted vector merges. The blob and derived-trie caches have separate bounded
> admission budgets; cursor-held references and temporary buffers are additional
> memory. Do not describe admission limits as a process-wide memory bound. Keep
> stale-snapshot isolation, bounded allocation and invalidation semantics when
> modifying these caches. Instrument preparation, encoding and reconstruction
> separately if changing the persistence path, since commit time alone does not
> explain the observed batch slowdown.
>
> Extend existing native-versus-packed regression coverage for full-retention
> roots, proofs, multiproofs, sparse incremental updates, pending overlays,
> reopen, abort, deletes, wipes, later mutations after completion and state-only
> repair. Ensure marker mismatches, missing packing mode and conflicting cutoff
> arguments are rejected before mutating an incompatible directory. Keep all
> history-retention modes available with storage V2 and native storage behavior
> unchanged. Use existing test helpers and the provider's production update path
> rather than only a synthetic state writer.
>
> If snapshot conversion is requested separately, read a stable, checkpointed
> source into a new destination and keep the source intact. Preserve accounts,
> bytecode, the complete native trie, retained history/static files and metadata;
> convert only storage-value rows. Respect both header and durable state/trie
> frontiers when state masking is active. Compare logical account/storage
> counts and checksums and the canonical root at the represented state frontier.
> Publish the destination marker only through a recoverable completion protocol.
> Do not implement conversion by relabeling a native or depth-cutoff database.
>
> Treat adaptive retention as a separate versioned feature. Implement explicit
> retained boundaries with bounded reconstruction work and atomic publication;
> use canonical native/full-retention results as the oracle. Do not ship a
> mainnet-space or performance claim by multiplying a synthetic whole-state
> percentage across all node data. Document implemented behavior separately from
> proposed policy and remaining limitations.

## Mainnet sizing interpretation

The saving applies to `HashedStorages` and, when omitted, `StoragesTrie`.
Accounts, bytecode, account trie, historical data, RocksDB indices and static
files do not inherit the same percentage. Whole-node savings depend on the
selected history-retention profile and physical page/file allocation.

A historical [Ethereum mainnet table census from March 2026](https://github.com/paradigmxyz/reth/issues/22804)
reported 108.1 GiB of hashed storage and 35.6 GiB of storage trie. Applying this
devnet's approximately 34.45% storage-page reduction to that hashed-storage
table gives about 37 GiB saved with full trie retention. Applying the additional
observed trie reduction gives about 70 GiB total with trie omission. These are
conditional projections from an older node's current-state tables, not measured
mainnet conversions or current disk requirements. Contract sizes, value widths,
trie shapes and page occupancy can change the outcome substantially.

Freed MDBX pages can be reused without reducing an existing file's allocation.
Keep live table bytes, reusable/retired pages, allocated filesystem bytes and
compressed snapshot download sizes separate in every report.
