# Partitioned Binary Tree

Experimental Rust implementation of [EIP-8297](https://eips.ethereum.org/EIPS/eip-8297)
and the artifact/replay rules of [EIP-8347](https://eips.ethereum.org/EIPS/eip-8347).
The normative EIPs take precedence over the planning repository. Both proposals
are drafts. BLAKE3 is the reference/devnet configuration, not a final hash decision.

## Implemented

- A persistent binary tree with full-key leaves, compressed bit prefixes, cached
  commitments, canonical insertion/deletion, sorted construction, ordered iteration,
  and membership/non-membership proofs. Clones retain independent parent snapshots.
- Ethereum account headers, full-width storage keys, inline slots 0–63, code
  groups, PUSH continuation bytes, delegation, zero deletion, and shared-code cleanup.
  Bytecode buffers are shared when snapshots or decoded accounts reuse them.
  Account, storage, and code-reference indexes use persistent ordered maps, so a
  child update copies paths instead of cloning the complete parent indexes.
- The `execution` feature applies authoritative Reth `BundleState` output using
  plain address and slot keys. It handles storage wipes and rejects missing code.
- The `migration` feature produces and verifies the **tagged-record** snapshot
  format and hash-ordered preimage file. Verification reconstructs both PBT and
  anchor MPT roots, checks code hashes and PUSH metadata, and rejects noncanonical
  or surplus records. It does not use the older RLP snapshot format.
- The `conversion` feature reads a pinned Reth MPT range provider and bytecode
  provider with sequential, hash-ordered preimages. It checks exact account/slot
  coverage and the reconstructed MPT root before returning canonical artifacts.
  Source reads are paged; the output state still uses this crate's in-memory backend.
- The `bal` feature authenticates access lists against a caller-supplied header
  commitment and replays post-values. It rejects noncanonical read/write lists
  and nonzero storage writes without a surviving account. `ReplayChain` retains
  competing branches by block hash and discards incompatible layers when finality
  advances. Its default budget is 1024 retained layers, including the anchor;
  `with_limit` selects another operator budget. Reaching the limit rejects new
  layers without evicting viable branches. Finalization releases capacity. This
  bounds layer count rather than total memory.
- The `persistence` feature atomically checkpoints complete canonical artifacts
  and publishes block-to-PBT-root references after the state is saved. Reopening
  verifies the checkpoint against its root. Reads are bounded before allocation.
  `import_anchor` dual-verifies bounded local artifacts against a caller-trusted
  finalized MPT root before publishing them. `prune_finalized` deletes only
  explicitly obsolete references and candidate states unused by retained blocks;
  the caller must prove finality and ancestry. Shared roots and retained branches
  survive pruning.

`BinaryTree` can store a zero value. `StateTree` maps zero writes to deletion.
`set_account` exposes the standalone embedding, including explicitly supplied
empty accounts; `apply_account` performs execution state clearing. Migration
artifacts reject empty accounts under the EIP-7523 anchor invariant required by EIP-8347.

## Running

```sh
cargo +nightly test -p reth-trie-pbt --all-features
cargo +nightly clippy -p reth-trie-pbt --all-targets --all-features -- -D warnings
cargo +nightly run -p example-pbt-node
cargo +nightly nextest run -p example-pbt-node
```

Use a Rust toolchain satisfying the workspace MSRV in a supported Unix
environment. Persistence uses Reth's atomic writes, including parent-directory
sync, without a platform fallback.

See [`testdata/README.md`](testdata/README.md) for the pinned EELS vectors.

## Engine integration

Enable `reth-engine-tree/pbt` and install `PbtStateRootStrategy` through
`BasicEngineValidator::with_state_root_strategy`. Initialize its shared
`SnapshotStore` with genesis or a verified finalized migration anchor.

Before activation, accepted blocks retain MPT header roots while the strategy
checkpoints shadow PBT roots. At activation, the first PBT block obtains its parent
state through the pre-fork block reference. Afterwards, checkpoints can be restored
directly against the parent's header root. Validation continues producing MPT trie
updates, so reorgs across the activation boundary retain both commitments.

Payload building uses finalized execution output through `PayloadStateRootHandle`.
A failure of this backend fails building instead of falling back to an MPT root.
The example disables `eth_getProof` on all configured RPC transports because the
stock response is an MPT proof. The crate's internal PBT proofs are not an RPC format.
The example supplies a separate, versioned `pbt_getProof` method on authenticated
RPC only; see its [wire format and lifecycle configuration](../../../examples/pbt-node/README.md).

## Current operational boundary

This is a devnet/reference backend. Account indexes, artifact conversion, and
verification are in memory; checkpoints rewrite the full state. It is not a
mainnet-scale incremental database implementation. The cache holds at most 64
states and eviction does not remove durable checkpoints. Disk usage grows until
the operator explicitly prunes checkpoints proven obsolete under finality.

The engine strategy covers live validation, payload construction, shadow roots,
and fork selection. Staged/backfill sync and snap protocol paths still use MPT.
The dev-node example explicitly refuses staged/backfill sync before the MPT
pipeline can run; it must stay within live engine processing.
External users must integrate or disable those paths before running a PBT network
that enters backfill. The example supports durable restart, verified local anchor
import, retained-MPT self-conversion, bounded BAL catch-up with authenticated peer
retrieval, and explicit finalized-ancestor pruning.
Automatic anchor discovery, remote snapshot distribution, background onboarding
alongside a moving chain, automatic branch pruning, and a standardized public
proof RPC remain unimplemented. The replay API expects hashes from an already validated header
chain; it does not establish consensus finality or validate execution itself.

No mainnet activation timestamp or gas schedule is selected by this crate.
