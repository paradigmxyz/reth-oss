# Consensus fixture runner

This crate accepts consensus-specs **v1.7.0-beta.3** fixtures and executes every discovered case under `minimal/`, `mainnet/` and `general/`. Full mode requires both presets across Phase0, Altair, Bellatrix, Capella, Deneb, Electra, Fulu and Gloas. Missing required suites, empty inputs, corrupted files and unsupported handlers fail. JSON and JUnit reports contain every case; unsupported handlers are failures, never skipped tests.

The complete authenticated local run passed all 73,992 original cases: 63,744 primary and 10,248 compliance cases, with no failures or unsupported handlers. The result retains explicit signature, execution, availability and graph fixture assumptions; it does not establish independent runtime synchronization.

All 49,536 published static SSZ cases pass across 387 fork/container suites, using complete typed decoding, canonical byte re-encoding and the original published roots. Signed blocks independently verify both signed-container and unsigned-message roots. This includes all eight forks and both presets. YAML decoding of static containers is not implemented; the official permitted canonical-byte mode is used.

The processing adapters run published upgrades, operations, signed blocks, slots and epoch components, comparing complete post-state bytes and roots. The completed processing counts are:

| Fork | Published processing passes | Artificial BLS-disabled fixtures | Private execution fixture mocks |
| --- | ---: | ---: | ---: |
| Phase0 | 474 | 6 | 0 |
| Altair | 637 | 6 | 0 |
| Bellatrix | 705 | 6 | 157 |
| Capella | 869 | 6 | 183 |
| Deneb | 915 | 6 | 227 |
| Electra | 1,319 | 6 | 268 |
| Fulu | 1,233 | 6 | 238 |
| Gloas | 1,487 | 6 | 0 |

The execution mocks belong only to the consensus fixture context and do not establish runtime execution validity. Gloas beacon processing does not verify execution envelopes: those require the separate mandatory engine and availability paths. Other completed subsets include all 742 reward fixtures, all 122 light-client fixtures (59 synchronization/ranking, 52 Merkle proofs and 11 data collection), all 377 cross-fork transitions, all 623 primary fork-choice fixtures, and 64 custody fixtures. The data-collection subset uses 10 private execution mocks; transitions use 329. Gloas fork-choice uses both private execution and availability mocks in the 58 cases containing envelope events. All mandatory compliance and remaining categories passed in the complete run; unsupported cases remain failures.

The runner also implements shuffling and the published BLS handlers. Generic SSZ codecs have separate regression coverage. Generated structural fixtures remain separately labeled and do not increase published counts.

SSZ fixtures use raw Snappy blocks; consensus network streams use framed Snappy in the network crate. Static SSZ uses canonical byte re-encoding and original published roots; YAML parameters and event assertions are parsed separately. Case metadata identities are validated when supplied. Shuffle round overrides are resolved from enclosing configuration files.

A local inventory proves file integrity after it is created. Complete fixture acceptance requires a fresh run of both pinned original source corpora: 63,744 primary cases and 10,248 compliance cases. Both exact symlink-free inventories and every original file checksum must pass before any handler executes. The complete gate requires the exact 73,992 unique canonical case IDs, all preset/fork/general and declared-suite coverage, the pinned role-qualified input-file digest, and every passing outcome. Historical or focused reports have no full-corpus authentication evidence and cannot establish complete conformance. Official signature, execution, availability and graph fixture assumptions remain explicit in each result; independent runtime synchronization remains a separate gate.

## Run a verified fixture directory

The fixture root is the directory directly containing the preset folders. Store the manifest and reports outside that root, otherwise the inventory will change.

```powershell
cargo run -p reth-embedded-beacon-spec-tests -- inventory --fixtures testing/embedded-beacon/spec-tests/testdata/official-general --output testing/embedded-beacon/spec-tests/work/general-manifest.json
cargo run -p reth-embedded-beacon-spec-tests -- run --fixtures testing/embedded-beacon/spec-tests/testdata/official-general --manifest testing/embedded-beacon/spec-tests/work/general-manifest.json --manifest-sha256 <printed-digest> --json testing/embedded-beacon/spec-tests/work/general-results.json --junit testing/embedded-beacon/spec-tests/work/general-results.xml --focused
```

Obtain the release archive and expected archive digest independently, verify the digest, then extract without absolute paths, symlinks or parent-directory traversal. The CLI requires a separately supplied manifest digest so replacing both a fixture and its checksum during a run is detectable. Integrity hashing uses a fixed 64 KiB buffer per worker and one through 32 native workers. Every original file is checked independently, including repeated config files in separate source roots. The earliest manifest-index failure is deterministic. YAML and decompressed case data have a 64 MiB default limit.

All three run commands accept `--case-workers` from one through 32, defaulting to one. Whole cases share only immutable inputs and KZG settings; events and fresh state/store contexts stay local to each case. Worker threads have explicit 8 MiB stacks, and CI also sets `RUST_MIN_STACK=8388608` for Rust test threads. At most the selected worker count, capped at 32, executes concurrently; the completion queue has the same bounded capacity. The 64 MiB input limit applies to each file, not total decoded state or process memory; no total-memory benchmark is claimed. Results return in original canonical order after every worker joins.

A handler panic produces an explicit failed original case and preserves later case accounting. Its fallback reports fixture-assumption metadata as unavailable in the failure message; false marker fields on that failed row do not establish absence of fixture assumptions. Panic rows receive no passing, conformance or runtime-verification credit. Ordinary completed rows retain the existing exact assumption inference.

## Included official regression data

`testdata/official-general/` contains the 20 Ethereum-specific Altair BLS vectors in the pinned general release archive: 8 aggregate-public-key cases and 12 fast-aggregate-verification cases. These are the two handlers present in that archive; this does not cover all BLS handlers or complete beacon conformance. The published archive SHA-256 is `c4f395f71f606c28093500e32067990e430cc73a2277be6f3bf5090adcce3f67`. The regression test runs these exact vectors through the implemented crypto handlers. These vectors establish this subset's behavior only.

The pinned component archive has also been downloaded and checked into the local ignored `work/archives/` directory for further development. Its verified SHA-256 is `eaef466f1434dfad72e50c1e52449109f430c920e51181c0c19bff28e2aeb67d`. It contains additional compliance vectors and source material; this component archive alone does not establish complete source-union conformance.

The static-handler regression additionally runs 11 independently generated Gloas structural fixtures from the primitives crate, including both 16-member and 512-member PTC presets. These fixtures are marked `generated_structural` and their pinned reference package is recorded in their own manifest. They are separate from published conformance vectors and do not increase the published fixture coverage.

## Published complete containers

`testdata/official-phase0/` retains six unmodified published cases: BeaconState, BeaconBlock and SignedBeaconBlock for each preset. Every case includes the original serialized SSZ-Snappy bytes, expected root and identity manifest. The runner decodes all container fields, computes the complete schema tree root, and verifies canonical byte re-encoding. This is structural SSZ coverage; it does not establish transition validity or full consensus conformance.

`testdata/official-phase0-provenance.json` records the exact pinned release archives and selection. Mainnet archive SHA-256 is `a50c3eae743fcf69e17e3510b84e94dcabf96977655788c66bdc43a96590fdf5`; minimal archive SHA-256 is `b26832555fa5a67cc3964bf140ae99cb662f8045d6d205993e3fa27feb39ca6f`. Their size and digest were authenticated before bounded extraction. The six-case integrity manifest SHA-256 is `50b8dcdbcd96f6f9d5c54193fcce973e0af6bca4f745dc1e30f56213212146eb`.

```powershell
cargo run --locked -p reth-embedded-beacon-spec-tests -- run --fixtures testing/embedded-beacon/spec-tests/testdata/official-phase0 --manifest testing/embedded-beacon/spec-tests/testdata/official-phase0-manifest.json --manifest-sha256 0x50b8dcdbcd96f6f9d5c54193fcce973e0af6bca4f745dc1e30f56213212146eb --json testing/embedded-beacon/spec-tests/work/published-phase0-results.json --junit testing/embedded-beacon/spec-tests/work/published-phase0-results.xml --focused
```

The runner has passed all 3,072 published complete-state, unsigned-block and signed-block static cases from Phase0 through Gloas: 120 mainnet and 2,952 minimal cases. The authenticated corpus, integrity manifest, archive provenance and JSON/JUnit results remain in ignored work and artifacts directories. Each signed case independently checks its unsigned message root, complete body-derived header root and signed container root. These are structural results, not proof of valid state transitions.

`testdata/official-minimal-upgrades/` retains 21 additional unmodified published static cases, covering the three complete containers for each later fork. Its separately supplied manifest digest is `fbf97597761985ae182b5a6a2ec7851a50670afbb8667712b357ffee6b0e9e45`. The regression suite runs both retained published subsets. Independently generated structural fixtures remain separately labeled.

Processing adapters invoke the real fork-specific kernels through Gloas, applying exact published configuration overrides and comparing entire resulting states with original post-state bytes. Unsupported suites continue to fail explicitly. A successful focused structural or processing run always reports incomplete conformance.


## Latest published fixture profile

The downloader resolves the newest published release including prereleases, requires its asset SHA-256 digests, verifies cached archives, and extracts bounded regular files with no symlinks or traversal. An unknown newer release fails explicitly until the runner's schema pin is updated and validated. The implemented profile includes proven all-fork structural and processing handlers, rewards, light-client verification, custody, BLS, generic SSZ and shuffling suites. Its provenance also records every target-fork suite in the downloaded archives, including unimplemented counts. `--profile full` extracts the entire target-fork corpus, including the separately published mandatory compliance archive when Minimal is selected; unsupported handlers remain failures. The complete authenticated inventory has 73,992 cases: 63,744 from the preset archives plus 10,248 compliance cases. The same exact source union passed the complete authenticated local run. Each new release or implementation change still requires its own fresh complete result.

```bash
python3 testing/embedded-beacon/scripts/download-consensus-fixtures.py --release latest --profile implemented --presets mainnet minimal general --fixtures testing/embedded-beacon/spec-tests/work/latest --work testing/embedded-beacon/spec-tests/work/archives --manifest testing/embedded-beacon/spec-tests/work/latest-manifest.json --provenance testing/embedded-beacon/artifacts/latest-provenance.json
```

Supply the printed manifest digest to the fixture runner with `--focused`. A focused result is not complete conformance. The runner's state-transition dependency enables an isolated `spec-tests` namespace for original fixtures explicitly declaring `bls_setting: 2`; their result has `spec_only_bls_disabled: true`. Isolated gossip contexts that use a fixed artificial signature policy receive the same label even when the original metadata does not disable signatures. `spec_only_asserted_graph` separately identifies fixture checkpoint or graph observations, including isolated contexts that retain real signature verification. Private execution and availability providers have their own result fields. These namespaces cannot create runtime validation proofs or change production signature checks.

`testdata/official-minimal-phase0-processing/` retains all 63 published Minimal Phase0 epoch/slot cases and seven published operation/block cases, with unchanged original files and archive provenance. The 70-case manifest digest is `e1525386e7c93c050823345a849ef72f07ee7eea8f1e20c2498be506d299d287`. All 122 published Phase0 epoch/slot cases and all 352 operation/block cases have passed complete post-state byte comparisons; their separate reports preserve the exact artificial-BLS accounting.


## Fresh complete source invocation

Use the two original directories directly; no merged copy or prior result union is required. The complete command accepts both independently supplied manifest digests and rejects a source pin that differs from the identities embedded in the runner.

```powershell
cargo run --locked -p reth-embedded-beacon-spec-tests -- run-complete --primary-fixtures testing/embedded-beacon/spec-tests/work/published-all --primary-manifest testing/embedded-beacon/spec-tests/work/published-all-manifest.json --primary-manifest-sha256 0x513216f9ca4c07092e35dff6a6ce6eab383823c36dd232949d7a839192f7bc0f --compliance-fixtures testing/embedded-beacon/spec-tests/work/published-compliance-full --compliance-manifest testing/embedded-beacon/spec-tests/work/published-compliance-full-manifest.json --compliance-manifest-sha256 0xe7fd977bcec49da3533634bdbf7a4ad9295d7bcfdcaa0efcad63d452c32f17cc --json testing/embedded-beacon/artifacts/complete-results.json --junit testing/embedded-beacon/artifacts/complete-results.xml --authentication-workers 4 --case-workers 4
```

An immutable optimized library test executable can run the exact ignored `tests::authenticated_complete_published_corpus` entry instead of building a second CLI binary. Set `BEACON_FIXTURE_COMPLETE=true`, `BEACON_PRIMARY_FIXTURE_ROOT`, `BEACON_PRIMARY_FIXTURE_MANIFEST`, `BEACON_PRIMARY_FIXTURE_SHA256`, and the corresponding three `BEACON_COMPLIANCE_FIXTURE_*` values. Set `BEACON_FIXTURE_REPORT` to one report stem outside the source roots; optional `BEACON_FIXTURE_PROGRESS_EVERY` and `BEACON_FIXTURE_AUTHENTICATION_WORKERS` control bounded authentication and progress. The entry writes one terminal JSON and JUnit report and requires complete acceptance. The existing `tests::authenticated_published_corpus` entry remains focused and subset-only.

`testdata/full-corpus-case-ids.txt.snappy` is a compact policy-test input derived from the two pinned manifests, with independent provenance in the adjacent JSON. Synthetic outcomes over that identity list exercise acceptance and rejection guards only; they are not fixture execution results or runtime proof.


## Strict complete fixture CI

The complete workflow prepares the original source roles separately with `--profile full --corpus-role primary --presets mainnet minimal general` and `--profile full --corpus-role compliance --presets minimal`. Archive digests, every selected original member, exact source counts and the pinned original manifest bytes remain bound by `full-corpus-pins.json`. The latest-release resolver fails if a newer schema release differs from the validated Gloas pin; it cannot silently run that release under older assumptions.

The workflow invokes `run-complete` once and verifies the terminal report with `check-complete-consensus-fixture-results.py`. Success requires exit zero, both exact source inventories, the canonical full input and case identities, every unique passing result, no setup errors, and explicit fixture assumption markers. It preserves source provenance, the full execution log, JSON and JUnit results on failure as well as success. No expected failure can grant complete acceptance.

`expected-failures.json` and `check-consensus-fixture-results.py` remain optional subset diagnostics. Their accepted result always reports incomplete conformance, including when their expected list is empty. The complete CI job does not use that allowlist. Published fixture conformance and independent runtime synchronization remain separate milestones.


Independent fixture cases can use `--case-workers 1` through `--case-workers 32`
on every run command. The default is one because mainnet states can be large;
the complete CI run explicitly selects four. Select a higher count when the host
has sufficient CPU and memory; each active case can allocate a large mainnet state. Workers draw the next available case
from a bounded shared queue, keeping uneven cases from blocking a fixed batch.
Each handler creates its own state, store and fixture validation context, and
processes its internal events in their original order. Source authentication and
both complete-corpus identity checks still finish before execution starts.

Completed outcomes are restored to the original canonical case order before JSON
and JUnit are written. Per-case elapsed times naturally vary with concurrency;
all identities, outcomes, messages and explicit fixture assumptions must match
serial execution. A handler panic becomes a failed outcome for its original case;
worker failures, missing results or duplicates cannot grant a successful report.
The report schema and original zero-omission acceptance gate are unchanged.
