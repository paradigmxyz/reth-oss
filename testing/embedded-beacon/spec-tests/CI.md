# Published consensus fixture CI

The complete consensus workflow resolves the latest compatible published release
once, rejects an unreviewed schema, authenticates every original primary and
compliance archive, and executes the exact unique case union in one fresh run.
Its independent terminal gate reconstructs archive inventories and preserves
source metadata plus JSON/JUnit results. The authenticated local run has passed
all 73,992 original cases; no hosted workflow run is claimed.

CI runs on relevant pull requests, merge-queue checks, main pushes and manual
dispatch. The workflow uses pinned actions, the repository's secure-runner setup,
job-scoped read permissions, bounded Cargo jobs and separate native caches. Its
optional runtime job tests production node features, shared-pool ownership,
feature-disabled logging, launcher profiles and real Engine API dispatch parity.
These checks do not change compiled readiness or perform public synchronization.
The independent devnet workflow remains manually gated.

Every original case must have exactly one passing result. Missing, duplicate,
unsupported or failed cases, changed source identity, setup/integrity errors and
nonzero infrastructure exits fail complete acceptance. All signature, execution,
availability and asserted-graph fixture assumptions remain explicit in each
result. Fixture mocks cannot grant runtime execution validity.

`expected-failures.json` retains exact reviewed identities for the separate
historical diagnostic mode. That mode accepts only listed `unsupported` outcomes,
rejects every unexpected `failed` outcome, and reports listed cases that now pass
as stale expectations. It cannot satisfy complete consensus acceptance. Never add
a wildcard or blanket exception. The execution fixture workflow likewise gates
its own exact reviewed expectations and rejects unlisted or incomplete results.

Downloader and terminal-gate contract tests run before fixture acquisition.
The diagnostic gate requires finite bounded durations and explicit assumption
markers, rejecting duplicate JSON fields. The complete gate preserves handler
coverage and assumption totals. [Manifest-only inspection](HANDLER-COVERAGE.md)
audits dispatch without granting fixture pass or runtime readiness credit.

[Authenticated published-release mode](PUBLISHED-RELEASE-CI.md) resolves metadata
once and derives source identities from the authenticated original archives.
New releases require explicit schema/provenance review. The frozen local proof
retains its exact original totals and manifest pins.
