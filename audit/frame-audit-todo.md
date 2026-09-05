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
