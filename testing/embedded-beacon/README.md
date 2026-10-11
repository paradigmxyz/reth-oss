# Embedded beacon testing

Run commands from the repository root. Consensus libraries live in
`../../crates/embedded-beacon/`, the optional launcher in
`../../bin/reth-embedded-beacon/`, and the Rust fixture runner in `spec-tests/`.
This directory also contains authenticated fixture helpers, execution-test
expectations, the Dockerfile and independent devnet tooling.

```sh
python3 -B -m unittest discover -s testing/embedded-beacon/scripts -p 'test_*.py'
bash testing/embedded-beacon/scripts/docker-build.sh
bash testing/embedded-beacon/scripts/docker-check.sh launcher-profile
```

The consensus and execution fixture workflows run on relevant pull requests,
merge-queue checks, main pushes and manual dispatch. Consensus CI also checks the
optional production node/launcher wiring. Independent devnet CI remains manual.

Generated reports are stored in ignored `artifacts/` directories; downloads and
private devnet tools use ignored `work/` directories. Public synchronization is a
separate acceptance step and has not been demonstrated.

[Build, configure and test the embedded beacon](../../docs/vocs/docs/pages/run/embedded-beacon.mdx).
