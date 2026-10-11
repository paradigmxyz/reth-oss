# Execution proxy

The proxy bounds admission, serializes execution requests, and closes admission after cancellation
or a timeout leaves an execution outcome unknown. It preserves the complete engine response and
never treats an admission failure as a valid payload.

The `reth` feature routes typed requests through the public Engine API server methods in process.
This retains the server's version validation, requests-hash policy, custody propagation and error
objects without encoding JSON or opening an HTTP connection. Payload and sidecar fields must be
exactly representable by the chosen method; a mismatch returns invalid parameters instead of
silently dropping fields. The adapter's error type is `jsonrpsee_types::ErrorObjectOwned`.

The `integration-tests` feature enables `tests/real_engine_parity.rs`. It launches independent real
execution nodes with temporary databases and authenticated HTTP endpoints, using the shared node
test harness. Run on a supported execution-node build host:

```sh
cargo test -p reth-embedded-beacon-execution-proxy --features integration-tests --test real_engine_parity -- --test-threads=1
```

The test compares full responses and exact errors for newPayload V1 through V6 and
forkchoiceUpdated V1 through V5. Its cases include executed transfers, an invalid state root with a
recomputed block hash, malformed extra data, later-method timestamp rejection, disabled and
explicitly enabled requests-hash policy, canonical head selection, unknown-head SYNCING, and
custody bitvector propagation. Execution forks covered are Paris, Shanghai, Cancun, Prague, Osaka,
Amsterdam and Bogota. A separate test drives the same pair of nodes through all seven successive
execution activation timestamps on one chain. Nodes wait on conditions rather than elapsed sleeps.

These are execution-boundary parity tests. They do not validate beacon fork upgrades, full beacon
state transitions, the Gloas fork-choice event handlers, or inclusion-list consensus rules. Live
beacon sync remains gated on the unimplemented full transition pipeline. Gloas targets a consensus
fork; the execution method selected for that target must come from its separately pinned schedule.
