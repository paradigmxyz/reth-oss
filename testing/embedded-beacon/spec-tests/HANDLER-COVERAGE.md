# Manifest inspection and complete execution coverage

`inspect` authenticates the supplied manifest SHA-256 and groups every unique original case by preset, fork, runner, handler and suite. It calls the same declared-dispatch predicate used by execution. It opens no fixture input, invokes no handler, and grants no conformance or sync credit. A declared handler can still reject an unsupported metadata context during real execution; that distinction is preserved.

After the current runner is compiled, inspect the two immutable original manifests without restarting full fixtures:

```sh
cargo run --locked -p reth-embedded-beacon-spec-tests -- inspect \
  --manifest testing/embedded-beacon/spec-tests/work/published-all-manifest.json \
  --manifest-sha256 0x513216f9ca4c07092e35dff6a6ce6eab383823c36dd232949d7a839192f7bc0f \
  --json testing/embedded-beacon/artifacts/primary-handler-inventory.json
cargo run --locked -p reth-embedded-beacon-spec-tests -- inspect \
  --manifest testing/embedded-beacon/spec-tests/work/published-compliance-full-manifest.json \
  --manifest-sha256 0xe7fd977bcec49da3533634bdbf7a4ad9295d7bcfdcaa0efcad63d452c32f17cc \
  --json testing/embedded-beacon/artifacts/compliance-handler-inventory.json
```

The expected distinct counts are 63,744 primary cases and 10,248 compliance cases. The full execution gate independently binds their unique 73,992-case union and all 1,296,962 original files. Neither manifest inspection, expected-failure acceptance, old report unions, stopped runs nor reported booleans replace that fresh execution gate.

The complete gate's `handler_coverage` records exact result counts and artificial assumptions for each published suite. BLS-disabled cases, private execution and data-availability mocks, and asserted graph contexts remain visible. These fixture assumptions are not production verification evidence. Missing or non-boolean markers, duplicate JSON fields, non-finite durations, substituted cases, unexpected source pins and nonzero runner exits fail the gate.

The subset diagnostic gate uses the reviewed explicit unsupported-case list. It reports all four assumption totals, rejects malformed result metadata, and always keeps `complete_conformance` and `runtime_synchronization_verified` false. A listed case that now passes is reported as a stale expectation; no expectations were expanded in this implementation.

The complete authenticated local fixture run has passed. CI repeats complete source authentication and execution; manifest-only inspection cannot substitute for that fresh result. Focused policy tests use small synthetic reports and do not read the fixture corpus:

```sh
python -m unittest discover -s testing/embedded-beacon/scripts -p 'test_*consensus*fixtures.py' -v
```
