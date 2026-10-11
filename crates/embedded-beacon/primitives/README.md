# Beacon primitives

Canonical SSZ scalar, root, header, bitlist, bitvector, container-offset decoding and virtual-padding Merkleization are implemented. Progressive trees grow through 1, 4, 16, 64, ... leaf subtrees, with length or active-field mixing according to the schema.

The Gloas module implements the pinned v1.7.0-beta.3 payload bid, signed bid, PTC vote data and aggregates, execution payload, five execution request kinds, payload envelope, and signed envelope. Its SSZ parsing and roots are structural operations. Builder funding, membership, signature, data availability, and execution validity require separate contextual checks. Context-specific limits must be supplied by the caller.

Complete BeaconState, BeaconBlockBody and historical fork-specific schemas are covered by the authenticated consensus fixture run. Structural tests remain a subset of consensus validation and do not establish public synchronization.

Eleven generated structural fixtures under testdata/generated-gloas verify exact roots against the pinned independent eth-ssz-specs 0.1.0 package. They include mainnet and minimal PTC representations and bounded indexed PTC roots. These fixtures also pass the spec runner's canonical byte roundtrip and root checks; they are separate from published consensus transition suites.

Regenerate the structural vectors from the repository root:

```sh
python3 testing/embedded-beacon/scripts/generate_reference_vectors.py
```
