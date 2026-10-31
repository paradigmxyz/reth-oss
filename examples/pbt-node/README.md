# PBT dev node

```sh
cargo +nightly run -p example-pbt-node
```

Launches a temporary Ethereum dev node with a PBT genesis root and the BLAKE3
reference configuration. The normal EVM, transaction pool, state tables, and
Engine API are retained. PBT handles state commitment; MPT updates are also
maintained. The node prints its RPC addresses. By default its database and
checkpoints are temporary. Set `PBT_DATADIR` to retain both across restarts.

`eth_getProof` is disabled because MPT proofs cannot authenticate the PBT root.
Internal PBT proof generation/verification is available in `reth-trie-pbt`, and
the example exposes the versioned `pbt_getProof` method described below on
authenticated RPC only.

## Durable operation and migration

```sh
PBT_DATADIR=./pbt-data cargo +nightly run -p example-pbt-node
```

Use Ctrl+C or a graceful termination signal to stop the node and flush its
persisted state. Restart with the same directory and activation configuration.
Startup authenticates the persisted tip against its checkpoint and fails if the
checkpoint is missing or inconsistent. The directory pins both genesis and the
activation timestamp, so changing either requires a separate directory.
An OS lock allows one node process per directory. A populated checkpoint store
with a missing `chain-config` marker is rejected rather than assigned a new chain.

| Environment variable | Meaning |
|---|---|
| `PBT_DATADIR` | Persistent node directory; unset creates a temporary directory. |
| `PBT_ACTIVATION_TIMESTAMP` | Unsigned fork timestamp; defaults to `0` for PBT from genesis. |
| `PBT_MIGRATION_SNAPSHOT`, `PBT_MIGRATION_PREIMAGES`, and `PBT_MIGRATION_ANCHOR` | Supply all three together: local artifact paths and the anchor's full block hash. The node checks that the selected header is canonical, finalized, and before activation, then dual-verifies against that header's MPT root. The anchor may precede the current finalized head, including when finality has crossed activation. |
| `PBT_SELF_CONVERT` | Set to `true` to produce the snapshot at `PBT_MIGRATION_SNAPSHOT` from the retained anchor MPT and supplied preimage file before importing it. Missing, surplus, or unordered preimages fail conversion. The source state must still be retained. Snapshot output and preimage input must name different files. |
| `PBT_PRUNE_BLOCKS` | Optional comma-separated block hashes to prune at startup. Every candidate must be a strict ancestor of the locally finalized header. Finalized, sibling and unrelated blocks are rejected before deletion. |
| `PBT_CATCHUP_MAX_BLOCKS` | Nonzero budget for local anchor-to-tip BAL catch-up; defaults to 1024 blocks. |
| `PBT_CATCHUP_MAX_BAL_BYTES` | Nonzero aggregate raw BAL-byte budget for catch-up; defaults to 67108864 bytes (64 MiB). |

After migration import, the node walks its trusted local headers from the
selected finalized anchor to the persisted tip and replays BALs. Missing local
BALs are fetched from connected `eth/71` peers, with up to three 10-second attempts
per block and cancellation on shutdown. Only header-authenticated, byte-budgeted
responses enter the local BAL store. Every
parent link, BAL commitment and resulting MPT/PBT root is checked before the
corresponding checkpoint is published. Unavailable BAL ranges, invalid commitments,
bad roots and exceeded budgets stop startup. The node does not download artifacts.
Explicit pruning preserves the
finalized checkpoint and state files shared with every retained block reference.
It does not automatically enumerate or discard orphaned branches.

## Experimental authenticated proof RPC

`pbt_getProof(blockHash, keys)` accepts a block hash and an array of raw,
hex-encoded tree keys. Derive account/storage/code keys with `reth-trie-pbt::key`.
The method is available only on the JWT-authenticated RPC server, using the same
authentication as the Engine API. It is absent from ordinary HTTP/WS/IPC RPC.

The response is a versioned experimental format, not a replacement for the
standard `eth_getProof` schema:

```json
{
  "version": 1,
  "blockHash": "0x...",
  "pbtStateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "proofs": [{
    "key": "0x...",
    "value": null,
    "steps": [],
    "terminal": {"type": "empty"}
  }]
}
```

`steps` run from root to terminal. Each prefix is a sequence of relative path
bits, followed by the `right` selector and sibling hash. The terminal is
`empty`, `leaf` (full hex `key` and 32-byte `value`), or `branch` (relative
`prefix`, `left` and `right` hashes). A `null` value proves absence; otherwise
`value` is the raw 32-byte leaf. Reconstruct `Proof`, `ProofStep` and
`ProofTerminal` to verify with `Blake3Hasher`. Clients must compare
`pbtStateRoot` with a separately trusted commitment; before activation it is a
shadow root rather than the header's MPT root.

Requests accept 1–16 nonempty keys of at most 8192 bytes each, with a raw
parameter budget of 263168 bytes. Four blocking proof jobs may run at once;
additional requests fail immediately with error `-32005`. Invalid parameters
return `-32602`, and unavailable/corrupt checkpoints return `-32603`.

The tests cover transfers, contract deployment and overflow storage, competing
branches, reorgs, timestamp activation, BAL replay, early empty payloads,
missing-checkpoint failures, anchor onboarding, durable restart, finalized
pruning and proof responses. The example refuses staged/backfill sync with an
explicit error before the stock MPT pipeline can execute. It does not support
PBT-native snap sync. See the [PBT crate](../../crates/trie/pbt/README.md) for the
implemented surfaces and remaining production integration work.
