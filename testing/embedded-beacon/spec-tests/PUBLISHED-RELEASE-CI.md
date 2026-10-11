# Authenticated published-release conformance

This CI mode resolves the newest published release once, including prereleases. It refuses an unreviewed schema release before acquiring fixtures. The reviewed schema remains `v1.7.0-beta.3`; changing it requires source/schema review. The frozen local `run-complete` proof and its original byte/count pins remain unchanged.

The resolver retains the complete original release metadata, immutable release and asset IDs, archive URLs, published SHA-256 digests, and independently reviewed fallback digests. Original cached archive bytes must match size and SHA-256. Primary and compliance roles remain separate. Preparation includes every original target-fork case member, including static `value.yaml`, without supported-handler filtering. The pre-execution source record binds both original manifest bytes, all file identities, every distinct case identity, and the complete eight-fork Mainnet/Minimal matrix plus General.

Use one resolution throughout the job. Each command prints a small JSON result to stdout; save the returned checksum values as job outputs before invoking the runner. Never take source checksums or expected counts from a result report.

```sh
python testing/embedded-beacon/scripts/published-consensus-fixtures.py resolve \
  --release latest --output artifacts/release-resolution.json > artifacts/resolution-result.json

python testing/embedded-beacon/scripts/published-consensus-fixtures.py prepare \
  --resolution artifacts/release-resolution.json \
  --resolution-sha256 "$RESOLUTION_SHA256" \
  --work archive-cache --output prepared > artifacts/preparation-result.json

cargo run --release -p reth-embedded-beacon-spec-tests -- run-authenticated-release \
  --source-record prepared/source-record.json \
  --source-record-sha256 "$SOURCE_RECORD_SHA256" \
  --primary-fixtures prepared/primary \
  --primary-manifest prepared/primary-manifest.json \
  --primary-manifest-sha256 "$PRIMARY_MANIFEST_SHA256" \
  --compliance-fixtures prepared/compliance \
  --compliance-manifest prepared/compliance-manifest.json \
  --compliance-manifest-sha256 "$COMPLIANCE_MANIFEST_SHA256" \
  --authentication-workers 4 --progress-every 1024 \
  --json artifacts/published-release-results.json \
  --junit artifacts/published-release-results.xml

python testing/embedded-beacon/scripts/published-consensus-fixtures.py check \
  --resolution artifacts/release-resolution.json \
  --resolution-sha256 "$RESOLUTION_SHA256" \
  --source-record prepared/source-record.json \
  --source-record-sha256 "$SOURCE_RECORD_SHA256" \
  --work archive-cache \
  --primary-manifest prepared/primary-manifest.json \
  --compliance-manifest prepared/compliance-manifest.json \
  --report artifacts/published-release-results.json \
  --runner-exit-code "$RUNNER_EXIT_CODE" \
  --output artifacts/published-release-gate.json
```

Capture the actual runner exit status even when it fails, then execute `check` so CI retains a rejection artifact. Do not coerce the runner status to zero. No expected-failure list can satisfy this complete gate. Both original inventories authenticate before any handler executes; unsupported cases, failures, setup errors, omissions, duplicate results, substitutions, and nonzero exits reject conformance.

The terminal gate independently hashes the original archives again and streams their members to reconstruct the complete manifests without creating another fixture copy. It checks those exact original manifests against the preparation source record and the fresh report. Counts are derived from the independently authenticated archive members rather than frozen totals or test outcomes. Preserve resolution metadata, source record, manifest checksums, JSON/JUnit results, the runner's actual status and the gate artifact together.

Extraction is bounded to 2,000,000 selected members per role, 1 GiB per member, 2 GiB per archive, and 40 GiB extracted bytes per role by default. `--max-extracted-bytes` can raise the reviewed CI disk budget up to 200 GiB. Original fixture values are retained for source authentication; the static handler continues to perform original byte/root assertions under its existing bounded execution policy.

Passed published fixtures establish fixture conformance only. Per-handler coverage and explicit BLS-disabled, mock execution, mock availability and asserted-graph totals remain in the gate. They grant no live synchronization or recovery credit. Real devnet/public-network acceptance stays behind this complete gate and the remaining runtime implementation gates.

Offline policy controls:

```sh
python -W error::ResourceWarning -m unittest discover \
  -s testing/embedded-beacon/scripts -p 'test_published_consensus_fixtures.py' -v
```

These synthetic archives and outcomes test provenance/gate behavior only. They are never published fixture execution evidence.
