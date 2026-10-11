# Scoped transport regression verification

`scoped-pubsub-tests.py` prepares a uniquely owned temporary package from the
network-owned transport snapshot at `crates/embedded-beacon/gossipsub/`
and runs its complete metrics-enabled library suite.
Python 3.11 or newer and Cargo are required for execution. Preparation and the
Python contract tests require no dependency downloads or Rust builds.

```sh
python3 -B testing/embedded-beacon/scripts/scoped-pubsub-tests.py prepare \
  --repository "$PWD" --temp-root "${TMPDIR:-/tmp}" \
  --artifacts testing/embedded-beacon/artifacts/pubsub-local
python3 -B testing/embedded-beacon/scripts/scoped-pubsub-tests.py run \
  --preparation testing/embedded-beacon/artifacts/pubsub-local/preparation.json
```

Choose a fresh evidence directory. Existing evidence is never overwritten, and
the helper never deletes directories. The preparation record names the unique
owned temporary directory for inspection and later manual cleanup. Temporary
copies and evidence cannot be inside the source snapshot. Links and Windows
reparse points are rejected before following paths or copying files.

Preparation verifies every original, patched, added and auxiliary file against
`SOURCE-PROVENANCE.json`, preserves the MIT notice, and hashes the production
workspace manifest and lockfile. Only the copied `Cargo.toml` gains the exact
verification-only `quickcheck = "=1.0.3"` dev dependency and empty `[workspace]`.
The published registry manifest omits the original workspace test dependency;
`Cargo.toml.orig` records it. Production dependency edges and lockfiles are untouched.

Execution resolves a lockfile inside the independent copy, then builds only the
library tests with `metrics` and `--locked`. Cargo JSON must identify exactly one
library test executable belonging to that copy. That same executable supplies the
unfiltered test inventory and runs all tests. The gate requires a nonempty unique
inventory, one passing terminal outcome for every listed test, matching complete
counts, zero ignored/measured/filtered/failed tests, success exit codes, and an
unchanged executable hash. Counts come from the binary inventory so new source
regressions join the suite automatically.

Evidence includes original provenance, license and snapshot notes, exact source
hashes, normalized manifest, isolated resolved lockfile, compiler log, binary hash,
full named inventory, execution log, command vectors/statuses and `ci-gate.json`.
After success or a command failure, the helper checks that all copied sources and
production manifests/lock remain unchanged. No expected-failure exception can
satisfy this gate. A source preparation or a mocked Python contract test is not
Rust regression execution evidence.

The separate `pubsub` job runs automatically on relevant pull requests, merge-queue
checks and main pushes, or through manual dispatch. It caches registry dependencies
but keeps each copied test target isolated. The job runs this gate and preserves
its evidence even when execution fails. It does not download consensus fixtures
or run a node, and does not modify the ordinary execution workflow.

```sh
python3 -B -m unittest discover -s testing/embedded-beacon/scripts \
  -p 'test_scoped_pubsub_tests.py' -v
```
