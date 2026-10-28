# PBT dev node

```sh
cargo +nightly run -p example-pbt-node
```

Launches a temporary Ethereum dev node with a PBT genesis root and the BLAKE3
reference configuration. The normal EVM, transaction pool, state tables, and
Engine API are retained. PBT handles state commitment; MPT updates are also
maintained. The node prints its HTTP RPC address. Both the node database and PBT
checkpoints are temporary.

`eth_getProof` is disabled because MPT proofs cannot authenticate the PBT root.
Internal PBT proof generation/verification is available in `reth-trie-pbt`.

The tests cover transfers, contract deployment and overflow storage, competing
branches, reorgs, timestamp activation, BAL replay, early empty payloads, and
missing-checkpoint failures. This example does not support staged/backfill
or PBT-native snap sync. See the [PBT crate](../../crates/trie/pbt/README.md) for the
implemented surfaces and remaining production integration work.
