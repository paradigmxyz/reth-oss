# Current execution specification fixtures

The execution runner consumes the published `blockchain_test` format. Fixture absence, empty directories, malformed files, conflicting configuration, unexpected acceptance/rejection, incorrect final roots and incorrect last-valid hashes fail. One file is decoded at a time; its cases use two workers with 16 MiB stacks. Override `RETH_EF_TEST_THREADS` with an integer from 1 to 8 when resources permit.

The fixture configuration applies its chain ID and hex-encoded blob schedule without changing the shared fork configuration. Rejected candidates preserve the last valid parent and state, and the remaining candidates and final expectations are still checked. The runner requires complete RLP consumption.

Run `make eest-tests` to resolve the newest published fixture release, authenticate its asset SHA-256 and size, reverify every extracted JSON file, and execute it using the optimized test profile. Set `EEST_TESTS_TAG=tests@v21.0.0` to reproduce an exact release. An existing directory without matching provenance fails; use a fresh destination for another release. The download does not extract state-test, Engine, Engine X or transaction-test formats. This is blockchain fixture coverage, not complete execution conformance.

The dedicated Docker target `execution-fixtures` runs runner regressions before producing an executable test image. Use `--ulimit nofile=65536:65536` and mount the authenticated fixture directory read-only at `/fixtures`. Set `RETH_EF_TEST_REPORT=/reports/results.json` with a writable report mount to preserve per-file outcomes even when assertions fail. The source changes and test harness remain isolated from ordinary node startup.
