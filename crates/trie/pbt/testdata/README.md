# PBT conformance vectors

`binary_trie_vectors.json` is vendored unchanged from
[`ethereum/execution-specs`, `projects/binary-trie`](https://github.com/ethereum/execution-specs/blob/09d2088cf9f338e660cb04f948653baf544b7ba6/tests/binary_trie/vectors/binary_trie_vectors.json)
at checkout `09d2088cf9f338e660cb04f948653baf544b7ba6` (CC0-1.0).
The file records generator implementation commit
`8d258bc1d07fd0563e8eea5c0c732ed5fb803f4e`.

It contains nine tree-root cases, key-derivation vectors, four code-chunking
cases, three basic-data encodings, and 21 whole-state roots. Tests consume
every section. BLAKE3 matches these vectors; it is not a final Ethereum hash decision.
