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
- The `execution` feature applies authoritative Reth `BundleState` output using
  plain address and slot keys. It handles storage wipes and rejects missing code.
- The `migration` feature produces and verifies the **tagged-record** snapshot
  format and hash-ordered preimage file. Verification reconstructs both PBT and
  anchor MPT roots, checks code hashes and PUSH metadata, and rejects noncanonical
  or surplus records. It does not use the older RLP snapshot format.
- The `bal` feature authenticates access lists against a caller-supplied header
  commitment and replays post-values. It rejects noncanonical read/write lists
  and nonzero storage writes without a surviving account. `ReplayChain` retains
  competing branches by block hash and discards incompatible layers when finality
  advances.
- The `persistence` feature atomically checkpoints complete canonical artifacts
  and publishes block-to-PBT-root references after the state is saved. Reopening
  verifies the checkpoint against its root. All checkpoints remain on disk.

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

## Current operational boundary

This is a devnet/reference backend. Account indexes, artifact conversion, and
verification are in memory; checkpoints rewrite the full state. It is not a
mainnet-scale incremental database implementation. The cache holds at most 64
states and eviction does not remove durable checkpoints. Disk usage grows with
every distinct state until operators remove an obsolete devnet directory.

The engine strategy covers live validation, payload construction, shadow roots,
and fork selection. Staged/backfill sync and snap protocol paths still use MPT.
The dev-node example explicitly refuses staged/backfill sync before the MPT
pipeline can run; it must stay within live engine processing.
External users must integrate or disable those paths before running a PBT network
that enters backfill. Automatic anchor discovery, downloading snapshots, fetching
missing BAL ranges, finalized checkpoint pruning, and a public PBT proof RPC are
not implemented. The replay API expects hashes from an already validated header
chain; it does not establish consensus finality or validate execution itself.

No mainnet activation timestamp or gas schedule is selected by this crate.
