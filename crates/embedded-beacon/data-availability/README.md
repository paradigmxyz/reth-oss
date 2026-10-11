# Embedded beacon data availability

`VerifiedCells` acquires cells only after a complete KZG batch succeeds, retains shared `Arc<Cell>` allocations, and reports partial custody until all 128 extended cells are present. Recovery requires at least 64 verified cells; generated cells and proofs are verified again before publication. Invalid proofs and malformed batches leave existing custody unchanged.

`ColumnSidecar` decodes the Gloas SSZ structure with canonical offsets and a caller-supplied blob bound. It binds each sidecar to the validated block root and slot, obtains its commitments from that block's execution payload bid, and checks all KZG cell proofs. A verified column is immutable. The caller remains responsible for validated consensus block provenance, fork timing, correct subnet, duplicate suppression and fork-specific sampling requirements.

Proof verification and recovery are synchronous crypto operations. Runtime integration must schedule them on bounded blocking workers and share one trusted setup. Cell verification alone does not justify optimistic execution becoming finalized or establish availability of a complete block.
