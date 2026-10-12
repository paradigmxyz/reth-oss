# PR 127 state packing reaudit response

Reviewed head: `38b70ff02a9e7b77e2a4f71c61d700cd7505cff4`. The 10 October 2026 reaudit confirmed two issues. This response fixes directional cursor exhaustion and adds a smaller physical tier for singleton/small storage records. It does not change the native minimal storage path.

## Directional exhaustion

Ordinary iteration exhaustion is now separate from an uninitialized cursor, a failed range-seek boundary and a missing exact seek. The exhausted position keeps the last contract/subkey and direction. Repeated movement in that direction returns no row. Reversal moves to the adjacent outer contract, matching MDBX instead of restarting or discarding the position.

MDBX also distinguishes a singleton leaf from an inner duplicate cursor: after outer exhaustion, `current()` still returns a singleton but returns no value for an exhausted duplicate cursor. The logical cursor follows that distinction. Duplicate-only exhaustion continues to retain its scope without turning into outer exhaustion.

The differential regression uses three contracts with 1, 3 and 64 slots per contract, and compares both hashed storage and canonical storage-trie cursor traces against native MDBX. It includes repeated end operations, `current`, duplicate navigation, direction reversal and reversal of exhausted forward/backward walkers. The earlier failed-seek regression remains intact.

## Inline storage tier

Singleton values use `[mode/tag:1][checksum:8][Compact U256:0..32]`. The slot comes from the physical 64-byte contract/slot anchor, which is included in the checksum. There is no duplicated slot, count or offsets. Tags `0x80/0x81` select dense/integer32 singletons.

Small multi-row records use tags `0x82/0x83`, an eight-byte checksum, a one-byte count, subsequent 32-byte slot keys, one-byte Compact lengths and values. The first slot again comes from the anchor. This tier is bounded by **256 complete encoded record-value bytes**, allowing at most eight rows. Integer32 retains a full-blob candidate when it is smaller; larger records retain the existing dense/integer blob codecs. Decoding remains independent of neighboring records.

For a one-byte singleton value, serialized physical key/value bytes change from **127 to 74** in dense mode and **129 to 74** in integer32, saving 53/55 bytes. Native remains 65 bytes: the inline representation still has nine more serialized bytes. It reduces the identified overhead but does not guarantee native-sized allocation for every small-contract workload. A native exception tier is not implemented.

Inline parsing verifies the tag/mode, anchor checksum, encoded size, count, ordered keys, lengths and canonical Compact values. Property tests exercise arbitrary input, valid-checksum malformed headers, full/inline round trips and owned borrowed views. Tests also cover every-byte singleton corruption, anchor corruption, truncation, tier transitions, abort, old snapshots and reopen. Splitting/merging/rewriting remain within the existing MDBX transaction.

## Format compatibility

Dense/integer32 now use experimental database versions **10003/10004**. Versions 10001/10002 are rejected without modification, including when the flag is omitted; this is tested. Older binaries likewise reject the new version/marker. Use a fresh experimental directory. Editing the marker/version is not a supported migration. The full-blob decoder remains available within the new format for mixed physical tiers. Native version 2 and its storage tables are unchanged.

## Validation

The full database suite passed **64 tests**, with the two manual experiments ignored. The singleton allocation experiment was run separately and passed. Production-provider roots/multiproofs passed **two tests**, with its manual benchmark ignored; coverage includes singleton, small inline and full-blob contracts before/after mutation and reopen. Strict changed-crate Clippy passed with `--all-targets --no-deps -- -D warnings`; six existing dependency warnings remain. Focused nightly formatting and `git diff --check` passed. The cursor regression passed again after the lint-only corrections. The previous optimization report describes the earlier head; its timing and compatibility claims must not be attributed to this format revision. No whole-node storage, release throughput or new devnet measurements are claimed here.

The allocation census uses 4,096 contracts with one hashed slot each and `U256::ONE`, fresh databases and 4-KiB pages. All layouts insert the same contract/slot records in sorted contract order. The full-blob control writes the previous codec directly into the physical experimental table in a fresh transaction; it does not expand an already populated inline database. The logical row count is verified for every layout.

| Representation | Live storage table bytes |
| --- | ---: |
| Native | 323,584 |
| Dense full-blob control | 598,016 |
| Dense inline | 364,544 |
| Integer32 full-blob control | 598,016 |
| Integer32 inline | 364,544 |

Inline records reduce live storage pages **39.04%** relative to full blobs on this fixture. They still use **12.66% more** live space than the sorted native control. The audit's native allocation was 454,656 bytes; its allocation and this sorted-control allocation must not be mixed to claim parity or a native-relative saving. Key distribution and insertion order affect page occupancy. Avoiding all small-contract expansion would require the separate native exception tier suggested by the audit. These measurements describe one storage table, not total MDBX allocation, filesystem size or a mature minimal node.

Raw check and census logs are saved in `experiments/reth-packing-devnet/reaudit-*.log` outside the source repository. Reproduce the census with `cargo test --locked -p reth-db --lib benchmark_singleton_allocation -- --ignored --nocapture --test-threads=1`.

An additional diagnostic calling `last_dup()` after outer exhaustion aborted in the native MDBX control with its `tree_search` root assertion. That sequence is excluded from the differential trace; the audit's `last/next/prev` and `first/prev/next` sequences are covered. No vendored MDBX source was changed. Node/e2e tests are not rerun for this response.

## Further suggestions

| Suggestion | Disposition |
| --- | --- |
| Variable-length/grouped trie paths | Deferred to a physical-format/provider change with ordering, prefix and range-seek differential tests. Trimming padding directly is unsafe. |
| Group changesets by contract | Deferred to an undo/history format project. Historical lookup and the configured undo window must be preserved. |
| Integer dictionaries and exceptions | Candidate for a later codec version, selected by complete encoded size and benchmarked on representative values. Current raw/delta fallbacks remain. |
| Dense packed lengths/checkpoints | Candidate for a later versioned full-blob codec. Existing full offsets remain; the new small tier uses bounded one-byte lengths. |
| Byte/page-based full-blob sizing | Deferred until overflow-page occupancy is measured. Row limits and merge hysteresis remain; only the inline tier is selected by encoded bytes. |
| Smaller static-file pruning segments | Existing configuration can be benchmarked separately. This patch changes neither minimal defaults nor undo distance. |
| Persisted adaptive trie cutoffs | Separate format milestone; fixed depth and the 262,144-leaf region limit remain. |

These are additional research opportunities, not fixes silently applied to the main minimal backend. Aggregate savings still depend on the affected tables' share of total allocated node storage; accounts, account tries, bytecode, undo/history and static-file layouts remain unchanged.
