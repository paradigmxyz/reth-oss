# Bounded Hoodi smoke source

This harness prepares one direct Linux process, with `--minimal --embedded-beacon --chain hoodi`, and stops after observing five new validated canonical beacon additions. The resource profile keeps Hoodi's mainnet consensus preset, 32 slots per epoch and its actual 12-second slots. The six-second private-network clock is not used on Hoodi.

No smoke run has been executed. The complete authenticated 73,992-case fixture run, its actual successful terminal exit, and compiled full readiness must pass first. The script invokes the existing strict original-source checker; an expected-failure list cannot satisfy it. Local source contract tests use synthetic API replies and mocked process handles, and grant no live acceptance.

Checkpoint startup needs an **explicitly selected trusted epoch and block root**, an HTTPS artifact provider, an explicit file of unchanged published signed seeds, and a literal public advertised IP. Provider responses supply bytes only. The smoke script has no default trust root. An explicitly selected provider-finalized checkpoint may be used for a controlled experiment when its trust basis is recorded; it remains provider trust, without independent finality proof. The native launcher independently authenticates complete original checkpoint bytes, network identity, state-dependent weak-subjectivity age, scheduled replay and actual execution reconciliation. Genesis mode currently needs an authenticated local archive and is not substituted for this test.

The metadata helper reads only the official Hoodi configuration, registry root and original bootstrap records at one exact source commit. It bounds and hashes the originals, preserves signed ENR bytes, and records no checkpoint trust. The native routing-seed owner verifies every ENR's signature, secp256k1 identity and permitted nonzero UDP endpoints. The explicit seed file may select a nonempty subset of the checked original inventory, without editing any record. UDP-only or historical-fork bootstrap records provide discovery routing only. The typed routing admission cannot place them in the consensus dial queue; each returned peer still needs a fresh current fork advertisement, supported transport and authenticated status handshake. The original strict peer parser remains unchanged. Five genuine signed-record routing controls are written; their native compilation and execution remain pending. Published source identity: [network configuration](https://github.com/eth-clients/hoodi/blob/main/metadata/config.yaml), [genesis identity](https://github.com/eth-clients/hoodi/blob/main/README.md), and [signed discovery inventory](https://github.com/eth-clients/hoodi/blob/main/metadata/bootstrap_nodes.yaml).

The official resource page publishes [this checkpoint artifact endpoint](https://checkpoint-sync.hoodi.ethpandaops.io). Its genesis reply matches Hoodi's configured identity. Exact finalized block, post-state and boundary-state GET routes are available. Block SSZ omits the consensus-version header; its exact-root JSON reply supplies the version. State SSZ includes the version header. The native downloader must authenticate all original SSZ and compare provider version metadata with the locally scheduled fork before importing anything.

Prepare or verify metadata without starting a node:

```sh
python3 -B testing/embedded-beacon/devnet/public/hoodi_metadata.py \
  testing/embedded-beacon/devnet/work/hoodi-official-metadata --fetch
python3 -B testing/embedded-beacon/devnet/public/hoodi_metadata.py \
  testing/embedded-beacon/devnet/work/hoodi-official-metadata
```

After the complete gate and a reviewed trust selection, the concrete command is:

```sh
python3 -B testing/embedded-beacon/devnet/public/hoodi_smoke.py \
  --binary /absolute/path/to/reth-embedded-beacon \
  --metadata-dir testing/embedded-beacon/devnet/work/hoodi-official-metadata \
  --bootnodes-file testing/embedded-beacon/devnet/work/hoodi-official-metadata/bootnodes.txt \
  --trusted-checkpoint EPOCH:0xINDEPENDENTLY_TRUSTED_BLOCK_ROOT \
  --trust-basis 'Reviewed origin and independent basis for this exact epoch/root' \
  --checkpoint-url https://REVIEWED_ARTIFACT_PROVIDER \
  --advertised-ip ACTUAL_PUBLIC_LITERAL_IP \
  --fixture-report /absolute/path/to/complete-report.json \
  --fixture-exit-file /absolute/path/to/actual-runner-exit.txt \
  --primary-manifest /absolute/path/to/original-primary-manifest.json \
  --primary-provenance /absolute/path/to/original-primary-provenance.json \
  --compliance-manifest /absolute/path/to/original-compliance-manifest.json \
  --compliance-provenance /absolute/path/to/original-compliance-provenance.json
```

The placeholders deliberately cannot launch. The peer bind wildcard matches the advertised family (`0.0.0.0` for IPv4, `::` for IPv6); the advertised address remains the explicit public literal. Both observer APIs remain IPv4 loopback. Execution TCP and discovery v4 use the explicit execution port; discovery v5 uses its checked successor, so the command does not inherit another node's default discovery ports. Select original routing seeds with UDP addresses usable by that local discovery family; the native discovery owner rejects a seed with no compatible route. The runner-exit file must contain the actual successful terminal exit `0`; a partial report is refused. The default deadline is one hour, with a maximum two hours. Fresh EL checkpoint download may consume most or all of that time before any beacon observation; timeout preserves failure diagnostics and stops the owned process. The launcher uses the same captured typed Engine owner rather than external consensus control.

The observer attributes both loopback API sockets to its exact child PID through Linux `/proc`, verifies original Hoodi EL/CL genesis and fork metadata, then anchors one non-optimistic canonical baseline. It walks actual retained signed block parents, verifies increasing beacon slots and exact execution-height/hash parent links, compares every payload's hash/state/receipts/gas limit/gas used/timestamp/blob fields with the canonical same-height Reth block, and rechecks all counted roots remain canonical. The actual EL head must advance and match the accepted beacon head payload. A five-slot jump by itself earns no block count. This is evidence from the native authenticated owner; the Python observer does not separately recompute SSZ roots or BLS signatures.

Only the `Popen`-owned PID receives SIGINT. Graceful zero exit is required for a passing report; timeout escalation is reported as failure. Logs, actual argv/PID, executable and fixture input hashes, official metadata provenance, baseline and five original block/hash/gas observations remain under a fresh directory in `testing/embedded-beacon/artifacts/hoodi-smoke/`. Databases are retained for inspection. The stop is triggered after five verified observations; polling and shutdown permit additional concurrent imports. There is no exact five-import kernel limit flag.

A passing bounded observation does not mark complete public sync, finality/restart acceptance, Gloas activation on Hoodi, or mainnet sync as passed. Exact checkpoint trust and public address inputs, full fixtures, compiled readiness and eventual real EL reconciliation remain launch prerequisites.


## Public network input files

`network-sources.json` records mainnet, Hoodi and Sepolia identities, the requested provider and canonical registry metadata sources. The fetch helper saves original configuration, signed discovery records, genesis identity replies and exact finalized checkpoint descriptors with timestamps, source commits and SHA-256 checksums. Checkpoint selection requires an explicit trust basis:

```sh
python3 -B testing/embedded-beacon/devnet/public/fetch_network_inputs.py \
  --output testing/embedded-beacon/devnet/work/network-inputs/first-hoodi \
  --trust-provider-finalized 'Provider explicitly selected for this controlled sync experiment'
```

The output directory must be new. Add `--network hoodi` to prepare only Hoodi. Downloaded inputs remain ignored under `devnet/work/network-inputs/`; the source catalog and fetch helper are permanent test tooling. The native checkpoint downloader acquires and authenticates the full state, avoiding a second large state download in the helper. Genesis SSZ source URLs are recorded without downloading unused genesis archives.

Mainnet's provider API requires an API key, so unauthenticated preparation saves its registry configuration and bootstrap records and records `checkpoint: null`. The helper does not invent a checkpoint or silently substitute a different provider. Hoodi and Sepolia expose public checkpoint APIs. A provider-finalized descriptor records that experimental trust explicitly and grants no independent synchronization acceptance.
