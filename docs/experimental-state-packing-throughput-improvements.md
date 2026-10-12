# Packed storage throughput and disk amplification improvements

This implementation follows the branch audit at `5a26d7e52df6b8450dafd8c6211780b73fdab150`. It targets repeated reconstruction, allocation and unnecessary physical writes in the experimental packed backend, together with storage outside the state tables. It contains no snapshot conversion stage.

The latest targeted regression tests and 100-block devnet comparison are reported below. Packing reduces live state, while root validation remains roughly twice native latency.

## Packed write and reconstruction paths

- The optional duplicate-cursor replacement hook lets packed storage and trie writers replace a known subkey without first reconstructing logical trie rows to seek and delete it. Native cursors retain the existing fallback. Subkeys and encoded values are checked before using this path.
- Pending no-op updates do not advance generations. Repeated writes use already known pending membership. Full trie retention avoids occupancy bookkeeping that cannot change its layout.
- Frontier queries use bounded ordered ranges. Layout boundaries have their own epoch and cache, separate from changing occupancy counts. Read-only snapshots bypass mutable overlay locks.
- The blob, frontier and reconstructed-trie caches use indexed bounded LRU lists, avoiding a full cache scan on each eviction or touch. Admission limits remain bounded; active cursor references can outlive shared-cache eviction.
- Completed root builders carry an ephemeral canonical empty-path branch summary through classic and sparse trie updates. Packed writers can use a current-generation summary to avoid a redundant commit-time repair. Missing summaries, dirty or extension roots, boundary changes, wipes and replayed serialized updates keep the conservative reconstruction fallback. `None` means unknown, not an empty trie.
- Repair reconciles changed retained records instead of deleting and reinserting every retained record. Ordered prefix searches avoid scanning all skipped ranges for each rebuilt record.
- Exact logical storage counts use validated blob row counts and pending membership deltas instead of decoding every value through a logical cursor. The initial count still scans physical records; this is not persisted constant-time cardinality metadata.

Root summaries are omitted from serde and bincode update serialization. Existing update payloads remain readable and cannot supply a stale persisted summary. Empty-path branch summaries remain absent from native trie tables. These optimizations do not change state roots, proof semantics or pruning retention.

## Encoding and physical layout

- Encoding removes temporary payload/offset allocations and reuses output, row and merge vectors across blobs. Integer32 selects its group encoding once, uses byte-oriented packing rather than individual-bit loops, and has constant-group encode/decode paths. Canonical value checks, record tags and checksums remain in place.
- Validated cached blobs retain the original owned byte vector rather than copying it into a second allocation. Cache accounting includes that vector's capacity.
- Blob splitting observes both the existing 512-row ceiling and an eight-page encoded-size budget derived from MDBX's actual page size. This mostly affects the widest rows; it does not imply a large reduction for already compact blobs. Existing records are readable and are only rewritten when touched.

No packed codec ID, experimental database version or layout marker changes. Existing matching experimental directories can reopen without a migration. Stock Reth still cannot open these experimental packed directories.

## Storage outside current state

Storage changeset static files sample up to 32 serialized rows from the first batch. Streaming writes sample the first row. A new empty jar selects the existing per-row LZ4 compressor only when the sample saves at least 20 percent of encoded bytes. Incompressible and small samples stay uncompressed. Point-read offsets remain independent, and existing NippyJar readers use the persisted compressor metadata.

The compression choice is frozen before the first row is appended. Existing populated jars retain their compressor. Compression metadata is persisted using the committed empty-jar header without advancing a pending changeset header. A favorable sample is a selection policy, not a guarantee for subsequent rows. Compression introduces encode/decode work; this patch does not claim that cost is zero.

For minimal-storage defaults, newly configured receipt segments contain 128 blocks, while transactions, senders and account/storage changeset segments contain 1,000 blocks. Headers retain 10,000-block segments. Smaller sealed segments reduce file-granularity retention slack; protocol and unwind retention distances are unchanged. Explicit saved configuration and CLI overrides retain precedence. Existing files are not repacked, and their discovered ranges remain valid. More frequent rotation adds metadata and file activity.

The block-access-list RocksDB column family enables blob garbage collection and uses a 64 MiB target for new blob files instead of 256 MiB. This allows obsolete payload space to be reclaimed more granularly during compaction without shortening BAL retention. Live payload relocation adds background writes, and existing files are not immediately resized. Other column families retain their policy.

## Accounting

Packed logical writes now participate in the existing operation counters. Transaction close timing includes packed flush work. Cached `database.packing` handles record flush, encoding, blob persistence and reconstruction durations, encoded bytes, blob counts and repair/completion counts once per successful flush. These timings separate costs; blob persistence time is not the final MDBX commit latency.

Database page/count inventories include the physical packed and adaptive frontier tables. CLI table counts are labeled physical entries, which must not be interpreted as logical storage slot counts. Read-operation latency is not comprehensively instrumented by these changes.

## Validation and remaining limits

Regression source covers direct replacement and pending mutations, generation and snapshot isolation, range seeks and cache eviction, codec byte compatibility, chunk budgets, current versus stale root summaries, serialization, compression selection, jar reopening/rotation/unwind and minimal-setting precedence. Independent subagent reviews cover root-summary propagation and compression crash ordering.

Targeted regression testing completed with 80 passing tests, two ignored and zero failures across 12 suites. Coverage includes packing codecs and cursors, snapshot isolation, root summaries, batched reconstruction, compression selection and reopen/rotation/unwind. The devnet comparison does not demonstrate a root-validation or import-latency improvement.

Snapshot conversion is deferred at the user's request. New trie-path, account/history and address-run formats are not introduced here: their complete reader, recovery and migration paths remain architectural work. The current cutoff/full/adaptive retention choices remain available; this patch does not silently switch an existing database to another layout. Sparse updates without a usable canonical branch summary and adaptive boundary changes can still incur reconstruction. Large-state proof and initialization costs are not eliminated by these changes.

## Follow-up: reuse trie work across validation and persistence

Synchronous root validation opens a new read provider per block. The following
methods reuse reconstructed nodes and completed writer generations without
changing persisted formats or retaining additional native trie records.

Read-only reconstructed trie maps now have an environment-owned cache. Its key
includes the actual MDBX snapshot transaction ID, packing version/depth and
contract/region; independently opened environments own separate caches. One
aggregate admission budget covers all snapshots: an estimated 8 MiB and at most
1,024 entries. Old readers can reuse their original snapshot after a commit, but
a newer snapshot cannot use its nodes. Writer overlays and transaction-local
generations stay private. Cursor-held references can outlive eviction, so this
admission budget does not bound total process memory. Blob validation remains
transaction-local, and concurrent reconstruction misses are not coalesced.

Commit repair reuses an upper-node map already derived from the final storage
generation for both fixed-cutoff and adaptive layouts. Reconciliation borrows
the immutable map rather than copying its tree. Missing or stale maps still
select reconstruction; mutations, contract/table clears and layout revisions
retain their existing invalidation semantics. This helps only when the final
writer generation has already populated that cache.

Masked root-summary reuse requires a joint state/trie proof. Showing that this
batch loses no deltas is insufficient: older deferred writes can still make the
database baseline differ from the canonical batch state. A candidate must also
cover deferred storage and trie deltas with identical retained values, with
conservative fallback for deletions, unknown summaries and genuinely hybrid
snapshots. The ordinary trie-only merge remains conservative. No canonical head
root is substituted for a hybrid persisted snapshot.
The provider runs this joint proof only when the backend requests omitted-trie
root summaries; native and fixed full-trie retention keep the existing merge
path. The new transaction capability defaults to disabled for other backends.

These changes preserve the packed disk format, markers, retention policy and
public trie view. No additional storage formats or snapshot converter are added.
New regression source covers snapshot isolation/cache bounds, writer isolation,
final-generation commit reuse and safe versus unsafe masked-summary reuse.
The follow-up regression tests passed as part of the 80-test validation. The
latest measured comparison is summarized below.

## Follow-up: required repairs and subtree reuse

The previous 100-block comparison still recorded
15 repaired contracts and zero completed-without-repair contracts in each packed
mode. These counters count contract work across transaction flushes, not failed
blocks. All measured canonical hashes and roots matched. Packed root-validation
means remained about twice native, and reconstruction accounted for about
28 ms of a 37 ms packed flush. The counts alone do not identify a stale-root bug.
They are live-scrape deltas and exclude the final shutdown flush. A full catch-up
with an empty suffix can use a valid canonical summary without appearing in
these recorded completion counters.

Engine persistence deliberately omits batch writes overwritten by its newer
in-memory suffix. The physical database can therefore contain a hybrid of older
values and retained batch updates, rather than the batch's canonical state.
Fresh-slot spam introduces suffix keys absent from the persisted batch; existing
deferred holes also have to be covered. A canonical storage root summary cannot
certify that hybrid database, so repair remains required. The current sparse and
regular producers already propagate separate root summaries through sorting and
the physical cursor hook. Serialization and changeset-only calculations can lack
that metadata and intentionally retain reconstruction.

The joint coverage proof now checks the newest suffix value for each storage
slot and trie path. A historical suffix write that is later restored no longer
rejects an otherwise exact summary. Every suffix key must still be present and
equal in retained updates; destruction, zero values, node removals, unknown roots
and inherited holes retain conservative fallback. This helps repeated/restored
writes, not genuinely new suffix storage.

The root-read optimization targets unchanged omitted regions. Read-only region
maps can be keyed by a hashed child commitment from retained branch nodes read
in that exact database snapshot, along with the child path, contract, region and
packing layout. An unchanged cryptographic commitment can then reuse immutable
reconstruction after a different MDBX transaction commits. Canonical block/header
roots are not used to certify a hybrid persisted database. Missing commitments
retain exact-snapshot keys; mutable writer overlays retain private generation
keys. Snapshot and commitment entries share the existing aggregate admission
budget, so this does not introduce an unbounded cache or a persistent trie table.

Required upper-trie reconstruction drains emitted branch updates in bounded
batches instead of after every builder input. Final root updates are always
drained. This reduces repeated map operations while preserving the retained-node
selection and bounding temporary update accumulation.
Provider completion also returns immediately when the exact storage generation
already has a valid root and requires no repair. Whole-table/contract clears and
adaptive boundary changes preserve their existing repair checks.

The packed codec, database versions, markers, cutoff depths and adaptive frontier
policy are unchanged. Additional native trie retention is not enabled. Source
regressions cover restored suffix values, changed and unchanged region commitments,
old read snapshots, writer isolation and batched reconstruction boundaries.
The regression tests passed and the devnet comparison is recorded below.
Necessary hybrid-state repairs remain nonzero during live persistence.

## Devnet validation and results

The release-profile devnet used four execution/consensus pairs with the same Reth
source: native minimal, dense cutoff at depth 3, integer32 cutoff at depth 3, and
dense adaptive. Lighthouse v8.3.0-rc.0 ran with Gloas/Fulu at genesis, Amsterdam on
the execution layer, six-second slots and a 200,000,000 gas limit. Each execution
node had four CPU cores and a 4 GB memory limit. Spamoor offered 512 transactions
per slot, with 200,000 compute-gas storage calls producing fresh slots and wide
values.

The measured window contained 100 successful storage-spam blocks, 52,641
successful transactions and 105,282 storage write loops, or 87.735 included TPS.
No six-second intervals were missed. All four nodes agreed on canonical block
hashes and state roots. Independent stopped database copies at the final measured
block contained 800 accounts and 105,637 distinct storage slots, with matching
account/storage checksums.

| Mode | Live state MiB | EL allocated MiB | State saving | Import mean / p95 ms | Root mean ms | CPU/native |
|---|---:|---:|---:|---:|---:|---:|
| Native minimal | 14.57 | 58.60 | 0.0% | 52.30 / 76.76 | 27.51 | 1.00 |
| Dense cutoff | 7.75 | 51.27 | 46.9% | 87.29 / 120.97 | 57.70 | 0.98 |
| Integer32 cutoff | 7.53 | 50.88 | 48.3% | 87.61 / 119.00 | 57.23 | 0.90 |
| Dense adaptive | 7.58 | 50.88 | 48.0% | 87.55 / 127.63 | 57.89 | 0.89 |

Compared with the previous 100-block run, packed/native import ratios changed
from 1.636 to 1.669 for dense, 1.664 to 1.675 for integer32, and 1.618 to 1.674 for
adaptive. Root validation remained 2.08–2.10 times native; no root or import
latency improvement was demonstrated. Whole-process CPU ratios are observational
and include proposer work. Included TPS is shared-chain throughput, not each
implementation's independently measured maximum throughput.

Each packed node recorded 18 live repaired contracts and zero live completions
without repair, versus 15 and zero previously. The current live window included
five persistence batches and ten packing flushes, versus four and eight
previously. Reconstruction consumed 75–77% of packing flush time. Full runtime
logs separately showed three contracts completed without repair per packed node
during shutdown, with zero reconstructed leaves. These shutdown completions are
outside the live counter window. The completion path works; live hybrid-state
persistence remains the expensive path.

The 100 canonical import samples per role are distinct from the wider live
root/CPU window, which contained 112 root validations versus 107 previously.
The current run contained 1.79% more distinct slots. Fresh-chain page occupancy,
proposer roles and narrow packing diagnostics also differ, so small cross-run
changes are not controlled regressions. Whole allocated disk retains free-page
and offline-unwind effects; live-state table pages are the primary storage
comparison. The short, small-state workload does not establish mainnet savings
or sustained performance with GiB-scale state.
