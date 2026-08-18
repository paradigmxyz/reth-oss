# Hive partial-persistence differential report

This report records the same Hive differential matrix in two states:

1. **Before the fixes**: the feature-enabled image hit a confirmed storage-wipe persistence panic.
2. **After the fixes**: the panic is absent, RPC and most Engine comparisons are clean, and one non-reproducing Engine API difference remains recorded as a possible flake.

Both runs compare a baseline client with partial persistence compiled in but disabled at runtime against the same image with aggressive partial-persistence flags enabled. The primary comparison is baseline versus partial behavior, not absolute Hive pass count.

## Shared test design

| Setting | Value |
| --- | --- |
| Feature build | One Reth binary/image with `partial-persistence` compiled in |
| Baseline | No partial-persistence engine flags |
| Partial | `--engine.persistence-threshold 2 --engine.persistence-backpressure-threshold 4 --engine.memory-block-buffer-target 0 --engine.num-state-masking-blocks 1` |
| Seed | `424242` |
| Parallelism | `16` |
| Hive log level | `4` |
| RPC CI filter | Shown below outside the table so its regex delimiters render literally |
| Complete RPC filter | `/` |
| Engine fallback filters | `engine-api`, `cancun`, `engine-withdrawals` |

Exact RPC CI filter:

```text
/(eth_blockNumber|eth_call|eth_chainId|eth_createAccessList|eth_estimateGas|eth_feeHistory|eth_getBalance|eth_getBlockBy|eth_getBlockTransactionCountBy|eth_getCode|eth_getProof|eth_getStorage|eth_getTransactionBy|eth_getTransactionCount|eth_getTransactionReceipt|eth_sendRawTransaction|eth_syncing|debug_)
```

For each pair, Hive used the client name `reth`, rebuilt the Hive Reth client wrapper without Docker cache, preserved raw JSON/log output, normalized suite/test/pass-state records, compared expected-failure policy, compared serious-error signatures, and retained focused reruns for result-set differences.

## Reproducibility and evidence contract

The CI run uses a workspace-local lab directory, retained and uploaded as the `hive-partial-persistence-lab` artifact:

```text
LAB_DIR=$GITHUB_WORKSPACE/hive-partial-persistence-lab
HIVE_DIR=$LAB_DIR/hive
RESULTS_DIR=$LAB_DIR/results
```

It clones the pinned Hive fork once, records its commit, and does not change or pull Hive between baseline and partial. Both modes use the locally built, feature-enabled `ghcr.io/paradigmxyz/reth:latest` image; its image ID is recorded before the batches. `--docker.pull` is intentionally absent. Only `clients/reth/reth.sh` differs at runtime: the partial launcher appends the four engine flags immediately before the node launch, rather than passing them to `reth init` or `reth import`.

Every batch uses this command shape, with only `SIMULATOR`, `FILTER`, `LABEL`, and `MODE` changing:

```bash
./hive --sim "$SIMULATOR" --sim.limit "$FILTER" --sim.limit.exact=false \
  --sim.parallelism 16 --sim.randomseed 424242 --sim.loglevel 4 \
  --client reth --docker.nocache 'hive/clients/reth'
```

The executed batches were: RPC CI subset baseline/partial; complete unmodified `ethereum/rpc-compat` baseline/partial with filter `/`; then, because neither RPC run emitted `Returning save input`, `ethereum/engine` baseline/partial pairs for `engine-api`, `cancun`, and `engine-withdrawals`.

Representative recorded client launch commands show the control has no masking flag and the partial command is the same base node command with exactly this suffix:

```text
--engine.persistence-threshold 2 --engine.persistence-backpressure-threshold 4 \
--engine.memory-block-buffer-target 0 --engine.num-state-masking-blocks 1
```

The complete commands are retained as `results/*-{baseline,partial}/client-launch-commands.txt`; raw Hive JSON reports, console logs, client logs, normalized TSVs, counts, diffs, and focused rerun logs are under the same `results/` directory. The RPC CI expected-failure policy is applied to every report with Reth's `.github/scripts/hive/parse.py`, `expected_failures.yaml`, and `ignored_tests.yaml`.

---

# Part 1 - Before the fixes

## Run identity

| Item | Value |
| --- | --- |
| Workflow run | [31590880801](https://github.com/paradigmxyz/reth-oss/actions/runs/31590880801) |
| Reth commit | [`ff3bb7a59988de6a45cbdc930819ec72083aa27f`](https://github.com/paradigmxyz/reth-oss/commit/ff3bb7a59988de6a45cbdc930819ec72083aa27f) |
| Reth version | `2.4.1 (ff3bb7a)` |
| Reth image | `sha256:1754fdab786179a092e71ead905534284f9b106a8fcdf1ebd3f077c3da1d59f5` |
| Hive commit | [`2b2813967e8262963a40d7891d337e4dcd67b7e9`](https://github.com/ethereum/hive/commit/2b2813967e8262963a40d7891d337e4dcd67b7e9) |
| RPC fixture revision | execution-apis [`742d45db810b31265c8d3c075af324953330d1ed`](https://github.com/ethereum/execution-apis/commit/742d45db810b31265c8d3c075af324953330d1ed) |
| Host | GitHub-hosted Ubuntu 24.04, Docker Engine, `linux/amd64` |

Baseline launch logs had no masking flag. Partial launch logs contained all four partial-persistence flags. The paired Reth and simulator image identities matched.

The complete pre-fix lab, including raw Hive JSON reports and baseline/partial client logs for the reproduced failures, is attached to the workflow as [hive-partial-persistence-lab](https://github.com/paradigmxyz/reth-oss/actions/runs/31590880801/artifacts/9142306201).

## Before-fix results

| Suite | Baseline cases | Partial cases | Baseline unexpected failures | Partial unexpected failures | Result set | Outcome |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| RPC CI subset | 113 | 113 | 1 | 1 | clean | Same outcomes |
| Full `rpc-compat` | 235 | 235 | 18 | 18 | clean | Same outcomes |
| Engine API | 129 | 129 | 0 | 4 | changed | Partial regression |
| Engine Cancun | 226 | 226 | 1 | 1 | changed | Not consistently reproduced |
| Engine withdrawals | 35 | 35 | 2 | 2 | clean | Same outcomes; diagnostic text differed |

The complete `ethereum/rpc-compat` simulator was run unmodified, not only the CI method filter. The RPC CI and complete RPC normalized diffs were empty: each paired run has the same named cases and pass/fail state. The RPC CI expected-failure parser outcome was the same in both modes. Neither standard RPC run activated a persistence cycle, so all three standard Engine suites were run.

## Before-fix persistence activation

The partial Engine logs proved actual masking/persistence activity:

| Partial suite | Activation events | First recorded frontier |
| --- | ---: | --- |
| Engine API | 381 | `new_partial_state_trie=2`, `new_db_tip=3` |
| Engine Cancun | 475 | `new_partial_state_trie=2`, `new_db_tip=3` |
| Engine withdrawals | 242 | `new_partial_state_trie=2`, `new_db_tip=3` |

Every recorded event had `new_partial_state_trie < new_db_tip`. The partial-state frontier was therefore genuinely behind the database frontier; this was not only flag-plumbing coverage.

## Confirmed pre-fix failure

Two Engine API cases passed in baseline, failed in partial mode, and reproduced in all three required focused reruns (partial first, baseline second):

1. `Invalid Missing Ancestor ReOrg, StateRoot, EmptyTxs=False, Invalid P10 (Paris) (reth)`
2. `Re-org to Previously Validated Sidechain Payload (Paris) (reth)`

The partial persistence service reached a masked frontier, then panicked while merging a storage wipe:

```text
Returning save input ... canonical_head_number=11
new_partial_state_trie=10 new_db_tip=11 target=Threshold

thread 'persistence' panicked at crates/trie/common/src/updates.rs:750:17:
storage wipes are not supported by disjointed_merge_batch

ERROR engine::tree: Channel disconnected
ERROR reth::cli: Fatal error in consensus engine
ERROR reth::cli: shutting down due to error
```

The relevant stack path was:

```text
TrieUpdatesSorted::disjointed_merge_batch
DatabaseProvider::save_blocks_inner
DatabaseProvider::save_blocks
PersistenceService::run
```

The following Engine API request then observed JSON-RPC `-32603` with `beacon consensus engine task stopped`. This was a partial-persistence runtime regression, not a missing test or client-launch issue.

---

# Part 2 - After the fixes

## Code changes included

The post-fix image contains both code fixes below.

### 1. Engine configuration construction

The post-fix workflow used [`ff3bb7a599`](https://github.com/paradigmxyz/reth-oss/commit/ff3bb7a59988de6a45cbdc930819ec72083aa27f), `fix(engine): construct partial persistence config safely`. The clean fix-only branch contains the equivalent commit [`b8f4f54c1`](https://github.com/paradigmxyz/reth-oss/commit/b8f4f54c149435a560c18c5520273503e09dbf05). It changes `EngineArgs::tree_config` so the persistence threshold is applied before the backpressure threshold:

```rust
.with_persistence_threshold(self.persistence_threshold)
.with_persistence_backpressure_threshold(self.persistence_backpressure_threshold())
```

This order matters because the `TreeConfig` setters enforce the backpressure/persistence invariant on each call. The earlier order applied backpressure while the config still held its default persistence threshold. The commit also adds a feature-gated regression test for the exact aggressive setting tuple: threshold `2`, backpressure `4`, memory buffer `0`, masking blocks `1`.

### 2. Storage-wipe merge support

The post-fix workflow used [`f8e18ee799`](https://github.com/paradigmxyz/reth-oss/commit/f8e18ee799ac6e14912b4a75b821f3013564e4a5), `fix(trie): support storage wipes in masked persistence`. The clean fix-only branch contains the equivalent commit [`c355445d6`](https://github.com/paradigmxyz/reth-oss/commit/c355445d6b5a08fb4c498bdcd065a051cf06e69f). It changes both:

- `TrieUpdatesSorted::disjointed_merge_batch` in `crates/trie/common/src/updates.rs`
- `HashedPostStateSorted::disjointed_merge_batch` in `crates/trie/common/src/hashed_state.rs`

The merge now processes the batch newest-to-oldest, seals storage state after a wipe so pre-wipe entries cannot be merged back, treats a masking-range wipe as covering the older persisted update, and preserves an unmasked wipe in the merge output. It adds four focused unit tests: batch-wipe and masking-wipe behavior for both trie updates and hashed state.

## Run identity

| Item | Value |
| --- | --- |
| Workflow run | [31702655553](https://github.com/paradigmxyz/reth-oss/actions/runs/31702655553) |
| Reth commit / workflow SHA | [`f8e18ee799ac6e14912b4a75b821f3013564e4a5`](https://github.com/paradigmxyz/reth-oss/commit/f8e18ee799ac6e14912b4a75b821f3013564e4a5) |
| Reth version | `2.5.0` |
| Shared Reth image | `sha256:1e5e28343f26eeb783875633d1139e2e1c2a084ae7c4794dc9ed5b61d97aa9a4` |
| Hive fork commit | [`3e3aeecc98c094357659cc5913c778aa063e1702`](https://github.com/Soubhik-10/hive/commit/3e3aeecc98c094357659cc5913c778aa063e1702) |
| Pinned upstream Hive commit | [`2b2813967e8262963a40d7891d337e4dcd67b7e9`](https://github.com/ethereum/hive/commit/2b2813967e8262963a40d7891d337e4dcd67b7e9) |
| RPC fixture revision | execution-apis [`742d45db810b31265c8d3c075af324953330d1ed`](https://github.com/ethereum/execution-apis/commit/742d45db810b31265c8d3c075af324953330d1ed) |
| Host | Ubuntu 24.04.4, Docker Engine 28.0.4, Buildx 0.35.0, Go 1.24.0, Python 3.12.13, PyYAML 6.0.2 |

The post-fix baseline launch commands have no masking flag. Every post-fix partial launch command has the four configured flags. The paired client and simulator image IDs match. The `--engine.num-state-masking-blocks` CLI flag was present in the feature-enabled image's `reth node --help` output before the comparison began.

## After-fix results

| Suite | Baseline cases | Partial cases | Baseline unexpected failures | Partial unexpected failures | Result set | Failure reasons | Serious errors | Outcome |
| --- | ---: | ---: | ---: | --- | --- | --- | --- | --- |
| RPC CI subset | 113 | 113 | 1 | 1 | clean | clean | clean | Same outcomes |
| Full `rpc-compat` | 235 | 235 | 18 | 18 | clean | clean | clean | Same outcomes |
| Engine API | 129 | 129 | 1 | 0 | changed | changed | clean | One non-reproducing baseline fail / partial pass |
| Engine Cancun | 226 | 226 | 0 | 0 | clean | clean | clean | Same outcomes |
| Engine withdrawals | 35 | 35 | 2 | 2 | clean | changed | clean | Same pass/fail outcomes; diagnostic text differed |

## Post-fix acceptance evidence

| Requirement from the test design | Evidence | Status |
| --- | --- | --- |
| One feature-enabled Reth image for both modes | One recorded Reth commit and shared image ID; baseline/partial differ only by the four launcher flags | Met |
| Same pinned Hive, simulator source/images, seed, parallelism, and log level | One Hive checkout and recorded revisions; seed `424242`, parallelism `16`, log level `4` | Met |
| Baseline has no masking flag; partial has all four exact flags | Saved launcher command extraction and partial launcher flag file | Met |
| Current Reth CI RPC subset | 113 baseline and 113 partial normalized case records; `rpc-ci.diff` empty | Met |
| Complete unmodified `ethereum/rpc-compat` | 235 baseline and 235 partial normalized case records; `rpc-full.diff` empty | Met |
| Expected-failure and ignored-test policy | Same RPC CI parser result in baseline and partial; expected-result outputs retained | Met |
| No partial-only startup, panic, fatal, or serious storage/RPC error | RPC serious-error and startup diffs empty; post-fix Engine scan has no storage-wipe persistence panic or consensus-engine shutdown | Met for the recorded run |
| At least one real partial-persistence cycle | Engine API, Cancun, and withdrawals partial logs show `new_partial_state_trie < new_db_tip` | Met |
| Investigate result-set differences | Pre-fix Engine API regressions reproduced three times; post-fix Engine API baseline-fail/partial-pass difference rerun three times partial-first and did not reproduce | Met; initial post-fix difference remains preserved |

The initial post-fix Engine API pair still has a recorded normalized difference, so this report does not erase it. The three controlled reruns produced no recurrence and the run therefore provides no consistent partial-persistence regression after the storage-wipe fix.

### RPC after-fix result

`rpc-ci.diff`, `rpc-full.diff`, both expected-failure diffs, both RPC transcript diffs, and both RPC serious-error diffs are empty. No RPC case was added or removed, no RPC pass became a failure, and no client-startup difference was recorded. The serious-error comparison covers client launch/startup failures and partial-only panic, fatal, database, state-root, missing-trie-node, and overlay-error signatures; it found no new partial-mode signature in either RPC pair.

### Engine API after-fix difference

The initial pair differed only for:

```text
Invalid Missing Ancestor Syncing ReOrg, Incomplete Transactions,
EmptyTxs=False, CanonicalReOrg=True, Invalid P9 (Paris) (reth)
```

The initial baseline result was `false`; the partial result was `true`. The harness derived the exact filter and ran it three times with the same seed, partial first, baseline second. Its saved focused summary records:

```text
reproduced_attempts: 0
classification: not-consistently-reproduced-possible-flake
```

No partial-mode failure, panic, startup problem, or serious-error difference was recorded for the three focused pairs. The initial result-set diff remains preserved as required by the comparison policy.

### Engine withdrawals after-fix diagnostic difference

The 35 named cases and their pass/fail states are identical in baseline and partial. The expected-failure outcome and serious-error signature are also identical. The failure-detail comparison differs because the raw simulator diagnostic transcripts contain different dynamic request/response content for the two existing failures. This did not create a test-case outcome change or a partial-only runtime error.

## After-fix persistence activation and crash scan

Neither post-fix RPC run logged `Returning save input`, so the RPC result remains configuration/RPC compatibility coverage. All three post-fix Engine partial runs emitted persistence events. One recorded Cancun event is:

```text
Returning save input ... canonical_head_number=3
new_partial_state_trie=2 new_db_tip=3 target=Threshold
```

Equivalent events are present in partial `engine-api` and `engine-withdrawals` logs. The partial state-trie frontier is behind the database tip in these events.

The post-fix partial logs contain no occurrence of:

```text
storage wipes are not supported by disjointed_merge_batch
thread 'persistence'
Fatal error in consensus engine
beacon consensus engine task stopped
```

The pre-fix storage-wipe panic and its consensus-engine shutdown are absent from the exercised post-fix Engine runs.

## Attached post-fix artifact

The complete post-fix lab artifact is [hive-partial-persistence-lab](https://github.com/paradigmxyz/reth-oss/actions/runs/31702655553/artifacts/9184308868). It contains the raw Hive JSON reports, all baseline/partial client logs, normalized TSVs, expected-failure parser output, activation extracts, diffs, and focused-rerun evidence cited in this report.

The artifact is attached to the workflow run and can be downloaded by repository members from the link above; no local path is required to review this report.

Key saved evidence:

- `results/summary.md`
- `results/rpc-ci-filter-from-workflow.txt`, `results/rpc-ci-filter.txt`, `results/rpc-full-filter.txt`, `results/random-seed.txt`, and `results/parallelism.txt`
- `results/partial-launcher-flags.txt`, `results/*-{baseline,partial}/client-launch-commands.txt`, and `results/*-{baseline,partial}/exit-code.txt`
- `results/rpc-ci.diff` and `results/rpc-full.diff`
- `results/rpc-ci-expected.diff`, `results/rpc-full-expected.diff`, and per-run `expected-results.txt`
- `results/engine-api.diff` and `results/engine-api-expected.diff`
- `results/focused-engine-api/summary.tsv`
- `results/*-partial/persistence-activation.tsv`
- `results/*-partial/logs/reth/*.log`
- `results/*-serious-errors.diff`, `results/*-failure-reasons.diff`, and `results/engine-withdrawals-failure-reasons.diff`
