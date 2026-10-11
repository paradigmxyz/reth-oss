# Execution fixture CI

The execution workflow resolves the newest published `tests@` fixture release, including
prereleases, and requires its published SHA-256 digest. Extraction rejects unsafe paths,
links and duplicate files. The provenance artifact records the exact release, archive
hash, and each selected JSON file's size and SHA-256. CI verifies those bytes before and
after execution and checks the provenance against the download's selection digest.

The native EF harness executes every downloaded `blockchain_test` JSON file with two
bounded workers. Build failures remain fatal. The result gate requires one result for
every authenticated file, consistent counters and the matching runner exit status.
Missing results, crashes, new failures, unlisted skips and changed error messages fail CI.

[known-failures.json](known-failures.json) contains two failures observed in preserved,
incomplete execution runs. Each entry binds an exact release, archive, fixture hash,
outcome and error. It does not predict other files' outcomes. A newly published archive
does not inherit these exceptions. Fixes appear as stale expectations in the gate report
and can be removed without blocking the improved run. Add an expectation only after
reviewing an actual failure and recording all of those exact fields; wildcard exceptions
and automatically generated exceptions are not accepted.

CI publishes the selection, full provenance inventory, build record, run log, per-file
results and gate decision even when a step fails. A green gate with known failures is
explicitly named `passed_with_known_failures`; the two files are still reported as failed.
Coverage is per JSON file. `state_test`, `blockchain_test_engine`,
`blockchain_test_engine_x` and `transaction_test` formats remain outside this harness.
Complete execution conformance and public network synchronization are not inferred.

The workflow is configured for branch changes and manual dispatch. Adding it does not
start a run, download fixtures locally, or change execution code.
