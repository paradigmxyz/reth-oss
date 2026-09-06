# Frame audit fix handoff — 2026-09-05

Conservative fixes prepared for the frame branch at the user's request to conserve usage.
Full integration is unfinished; this is not a verified merge-ready patch set.
Unintegrated frame inspector/state modules and sibling dependency drafts remain local and are
excluded from this commit. Do not enable public frame gossip based on these partial fixes.

## Deliberately preserved

- Amsterdam activates Bogota for this devnet.
- Older Engine API methods remain accepted for this devnet; do not restore the Bogota checks.
- Original Desktop/SReth checkouts were not switched or overwritten.

## Implemented locally

- Removed extra BalWorkerOutput reservation fields while frames retain the serial path.
- Made serial-frame fallback consistent for payload and decoded-block inputs.
- Removed added hot-path execution/receipt diagnostics; restored DEBUG-only BAL divergence work.
- Blobless frames no longer require a sidecar during payload building.
- Frame payload gas admission delegates to the executor's active gas schedule.
- Pool admission uses separate frame gas reservations instead of the combined limit in both lanes.
- RPC normalization preserves frame gas/nonce/kind; estimation validates the canonical envelope once instead of scalar gas search. Verification remains pending below.
- Borrowed receipt encoding and consuming log/envelope conversions avoid redundant standard-receipt clones; standard Compact encoding delegates directly.
- ERE export rejects unsupported frame slim receipts instead of dropping payer/frame outcomes.
- Full-width frame fees forwarded through EthPooledTransaction, cost calculation, ordering and replacement checks.
- Consensus-to-pooled frame conversion rejects missing required blob sidecars.

## Must finish before claiming full support

1. **Public frame mempool:** not integrated. Existing admission/gossip policy remains unchanged, including the known local-RPC validation gap. Do not enable public gossip until all checks are connected.
   - Draft `crates/transaction-pool/src/validate/frame_state.rs`: payer reservation and dependency tracker; nine standalone tests passed. Not declared as a module.
   - Draft `frame_inspector.rs`: trace policy; uncompiled and not integrated. SETDELEGATE currently fails closed. Verify canonical paymaster runtime independently, approvals, prefix limits, and trace rules.
   - Add validated metadata to pool transactions, atomic admission/reservation checks, release on every removal/replacement/inclusion, dependency/expiry withdrawal on head changes, reorg handling and asynchronous revalidation with stale-head protection.
   - Validate all signatures against the original full transaction; execute only the validation prefix, not arbitrary suffix frames.
   - Recheck blob-count admission for frames and payer/sender interactions with ordinary pending transactions.
2. **Revm prefix execution draft:** isolated `../revm-validation-prefix`, based on a72606f. Uncommitted edits in handler eip8141, new validation_prefix module, inspector handler, handler Cargo.toml and Cargo.lock. Tests currently have compile errors (FrameLimits arguments, error-type inference, boxed comparison, stale alloy_consensus test reference). Formatting and inspector parity tests remain. Do not pin this draft yet.
3. **Alloy pooled consolidation:** isolated `../alloy-pooled-consolidation`, branch feat/eip8141-transaction at c395a28, contains reverse pooled-to-consensus conversion and tests. Not committed/pushed/pinned. Reth's alias migration was deferred to avoid requiring an unavailable dependency revision; the compatible local enum remains. Finish Alloy conversion, update pins coherently, then migrate the enum and API call sites. Infallible sidecar-envelope conversion still needs attention.
4. **Frame BAL parallelism:** still serial. Restore only after reservation replay and ordered validation/error handling are verified. The removed output fields may become necessary then; do not simply delete the eligibility guard.
5. **RPC verification:** canonical call/simulate acceptance, missing-gas global simulation cap, dimensional accounting, conservative estimate behavior, trait imports and conversion error handling need targeted tests.
6. **Remaining cleanup:** remove unused reth-evm tracing manifest dependency and update lockfile coherently; check remaining newly added diagnostics outside the cleaned engine/receipt paths. Review public Priority::Overflow compatibility and memory-size impact. Review payload fee reporting for full-width values.
7. **Verification/integration:** nightly formatting and diff checks; targeted pool/RPC/BAL/history tests, then Linux CI. Earlier checks encountered pooled-conversion errors during the now-deferred alias migration; rerun after final integration. Do not assume a full build passed.

## Verification so far

- Receipt worker reports 14 standalone receipt-module tests passed.
- Frame reservation tracker worker reports nine standalone tests passed.
- ERE, RPC, full pool and BAL integration tests have not been established as passing.
- No dependency pins changed. `../evm-frame-validation` is a clean isolated worktree at afcad03, reserved for a future bridge; no bridge was implemented.

Main fix worktree: `C:/Users/soubh/Documents/Codex/2026-09-03/https-github-com-alloy-rs-alloy/work/reth-frame-audit-fixes`.
All sibling worktrees and drafts above should be preserved until explicitly integrated or discarded.

## Continuation audit — 2026-09-06

Fetched `origin/feat/eip8141-frame-transactions`: it still points to `a801a389229f524e9a415b923e3fb13777921106`.
The checkout was clean before editing. No dependency drafts were available or adopted and nothing
has been pushed. The Amsterdam/Bogota mapping, Engine API compatibility, serial frame execution,
public gossip gate and local pooled enum remain unchanged.

### Specification evidence

- User-selected [EIP-8141 revision](https://github.com/ethereum/EIPs/blob/b75cbe61150f09a44c38843be916417283d5b7bf/EIPS/eip-8141.md).
- User-selected execution-specs `devnets/frames/0` resolves to
  [`92bd4d2e1da307158dbe868c551262318efc870d`](https://github.com/ethereum/execution-specs/tree/92bd4d2e1da307158dbe868c551262318efc870d).
  Its frame tests' `spec.py` pins the same EIP revision above.
- The prefix ends when payment approval succeeds; signatures cover the complete original
  transaction. Prefix execution budgets plus signature cost are capped at 100,000 execution gas,
  and prefix state budgets at 500,000. Later VERIFY frames are forbidden for public admission.
- The EIP requires an exact canonical-paymaster runtime match and delayed-withdrawal accounting,
  but supplies neither a runtime artifact nor a storage layout. The inspected devnet frame
  implementation, fixture helpers and admission tests do not supply them either. Do not invent a
  runtime hash or exempt an arbitrary contract from trace restrictions. The EIP also has a
  canonical-only propagation sentence alongside non-canonical paymaster admission rules; this
  policy ambiguity remains unresolved.

### Additional fixes in this checkout

- Import `NetworkTransactionBuilder` where call and estimate use `output_tx_type`.
- Frame estimation no longer retries failures with an ordinary scalar gas limit.
- Simulation resolves the canonical transaction and active-schedule EVM environment before
  checking frame gas. Omitted outer gas cannot bypass the global cap, supplied gas is not clamped,
  and execution/state block reservations are checked separately. Ordinary request normalization
  remains on its existing path. Added a budget and dimensional-boundary regression test.
- Frame blob admission now checks the active blob-count limit. Added blobless, at-limit and
  over-limit cases. Frame blob replacement uses the configured blob bump, with full-width
  fee, priority-fee and blob-fee boundary tests.
- Removed unused `reth-evm` tracing dependency and its lockfile entry.

### Verification and remaining integration

- Current GitHub checks for the handoff report build, Clippy and test failures; Hive was skipped.
  Formatting passed. Public check annotations only expose exit statuses; the log download API
  rejected unauthenticated access with HTTP 403. These CI failures are not claimed resolved.
- Windows targeted `cargo +nightly check -p reth-transaction-pool -p reth-rpc-eth-types --locked`
  passed using a workspace-local target directory. The wider RPC API check encounters pre-existing
  Unix-only `ChangesetOffsetReader`/`ChangesetOffsetWriter` imports in the provider crate.
- Linux checking and regression test results are recorded below when completed.
- `zepter` passed. `make lint-toml` cannot run with the Windows shell; its `dprint fmt` command
  was run directly (formatting completed, with an unwritable incremental-cache warning).
- Full public validation, local-RPC prefix validation, atomic payer reservations and lifecycle
  integration remain unfinished. No admission or gossip enablement follows from these fixes.
- Revm prefix API, its EVM bridge and matching published dependency revisions remain required.
  Alloy consolidation remains deferred until its reverse conversions exist at an available pin.
- `Priority::Overflow` retains the `u128` associated priority type, but adds an exhaustive public
  enum variant and embeds a U256 in every priority value. Downstream source compatibility and
  memory/performance review remain necessary; this audit does not claim them resolved.
