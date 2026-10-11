# Independent devnet verification

The required topology is one reference execution/beacon pair and one actual embedded SUT. Both SUT APIs must belong to the same running container launched with `--embedded-beacon`. An external beacon service for the SUT cannot receive independent acceptance.

The requested profiles use the Mainnet preset, 32 slots per epoch and 6000 ms slots. Historical activation epochs are Phase0 through Gloas at epochs 0 through 7; the transition profile activates Gloas at epoch 1. The complete deployed schedule must match the supplied plan, whose `consensus_mode` is `independent_embedded`.

Readiness is mandatory by default. Each of `full_state_transition`, `full_fork_choice`, `peer_runtime`, `checkpoint_bootstrap`, `verified_state_recovery`, and `full_spec_conformance` must be an explicit true boolean, with `target_fork: "gloas"`. While capabilities are incomplete, the checker writes `status: "blocked"`, exits 2, and makes no HTTP or Docker requests. This is a blocked deployment, not a passing chain test.

```sh
python testing/embedded-beacon/devnet/checks/verify.py \
  --preflight-only \
  --schedule testing/embedded-beacon/devnet/package/schedules/historical.json \
  --readiness-file testing/embedded-beacon/artifacts/docker-readiness.json \
  --embedded-launch-probe testing/embedded-beacon/artifacts/embedded-launch-probe.json \
  --output-dir testing/embedded-beacon/artifacts/devnet/historical
```

Once all capabilities are implemented, preflight returns 0 with `status: "ready_not_deployed"` and `embedded_runtime_acceptance: false`. It does not deploy services or grant acceptance. A launch probe supplied while readiness is false must show an integer nonzero `exit_code`, `datadir_initialized: false`, and `readiness_error: true`. An unrelated command error receives no readiness-guard credit.

Full verification additionally requires the four actual published endpoint URLs and `--independent-runtime-provenance PATH` (alias `--provenance-file`). The file describes the expected deployment; asserted JSON does not establish that it exists:

```json
{
  "schema_version": 1,
  "enclave_uuid": "0123456789abcdef0123456789abcdef",
  "roles": {
    "reference_execution": "reference-el",
    "reference_beacon": "reference-beacon",
    "sut": "embedded-beacon-sut"
  },
  "allowed_services": ["reference-el", "reference-beacon", "embedded-beacon-sut", "reference-validator", "workload"],
  "images": {
    "reference_execution": "sha256:<64 lowercase hex digits>",
    "reference_beacon": "sha256:<64 lowercase hex digits>",
    "sut": "sha256:<64 lowercase hex digits>"
  },
  "ports": {
    "reference_execution": "8545/tcp",
    "reference_beacon": "5052/tcp",
    "sut_execution": "8545/tcp",
    "sut_beacon": "5052/tcp"
  }
}
```

`allowed_services` must include the exact expected user-service set, including normal helpers, the reference validator, and the workload. The checker independently queries live Docker using the runner's Docker environment. It enumerates the enclave's `user-service` containers, verifies the immutable image IDs, running participant state, deployed arguments, and agreement of the actual `com.kurtosistech.id` and `kurtosis_service_name` labels. Both SUT URLs must map to ports published by that same SUT container. URLs require literal loopback IPs and reject credentials, query parameters, fragments, and proxy paths. The deployment is inspected again before acceptance. Inspection errors never expose Docker output or environment data.

For each required fork, the checker reads a finalized full block from its first epoch and a finalized block from the preceding epoch where applicable. Skipped slots are allowed. Both roles must agree on version, beacon root, state root, and execution payload. State-root lookup uses the fixed slot, with a root-addressed known-state fallback only after a 404. Both forms require finalized, non-optimistic metadata and exact root equality; a second 404 earns no coverage. Reports record the lookup form and `independent_ssz_root_computation: false`.

The historical plan's `merge_transition` identifies the shared execution genesis terminal block and Bellatrix epoch. Advertised terminal configuration must match both. The first accepted Bellatrix payload must be finalized execution height 1 with that exact parent hash. An empty Bellatrix payload cannot count as an accepted transition. Other profiles require no terminal override fields.

Capella requires withdrawals; Deneb and later require blob gas fields; Electra and Fulu require execution requests. Gloas requires a payload bid and a FULL envelope from `/eth/v1/beacon/execution_payload_envelopes/{block_id}`, linked to the same beacon block and execution hash. Missing envelopes receive no FULL payload credit.

Execution comparisons use the same numeric height finalized on both participants. Canonical hash, parent hash, state root, receipts root, gas limit/usage, timestamp, base fee, blob gas fields, withdrawals, transaction hashes, and each receipt's block identity/order/fields/logs must agree. Beacon payload parent hashes must match that same fixed execution block. Ordered receipt gas usage must equal each cumulative receipt total and the block gas total. For a beacon payload comparison, original transaction bytes are retrieved from both execution endpoints and compared in order; reports include their SHA-256 digests and explicitly disclaim independent transaction-hash computation. Block hashes are checked again after receipt collection. Advancing finality and at least one finalized transaction receipt are required by default.

`verification.json` uses schema version 2 and is atomically replaced. Actual independent devnet acceptance can be true only after readiness, live provenance, complete fork coverage, advancing finality, and execution/receipt checks pass. `public_network_sync_acceptance` remains false: devnet success does not prove public-network synchronization. Contract failures or deadline expiration exit 1. These checks do not independently recompute consensus SSZ roots or replace full specification conformance.

Earlier external-consensus reports are retired diagnostics. Their originals remain unchanged; `../../artifacts/devnet/topology-change-checkpoint-20261008.json` preserves the latest partial observations before the required topology changed. None establish independent embedded acceptance.

```sh
python -W error::ResourceWarning -m unittest discover -s testing/embedded-beacon/devnet/checks -p 'test_*.py' -v
```

The focused regression suite uses HTTP contract fixtures and mocked Docker inspection responses. It covers malformed/sanitized errors, schedule and fork boundaries, known-state availability, Gloas envelopes, canonical merge linkage, fixed-height execution/receipt agreement, mandatory readiness, non-accepting preflight, substituted consensus services, missing embedded flags, wrong images/roles, and incorrect endpoint bindings. Fixture success is not a live deployment result.

## Public synchronization and restart observation

`observe_public.py` is a read-only acceptance observer for Hoodi or mainnet. It requires the same live immutable-container provenance as the devnet verifier, both APIs mapped to the actual embedded participant, and independently supplied consensus/execution genesis identifiers. Native-process provenance remains unsupported; manually asserted process IDs or logs cannot receive embedded acceptance.

```sh
python testing/embedded-beacon/devnet/checks/observe_public.py \
  --network hoodi \
  --sut-el-url http://127.0.0.1:8545 \
  --sut-beacon-url http://127.0.0.1:5052 \
  --reference-el-url http://127.0.0.1:18545 \
  --reference-beacon-url http://127.0.0.1:15052 \
  --expected-execution-genesis-hash "$EXECUTION_GENESIS_HASH" \
  --expected-genesis-validators-root "$GENESIS_VALIDATORS_ROOT" \
  --readiness-file testing/embedded-beacon/artifacts/runtime-readiness.json \
  --independent-runtime-provenance testing/embedded-beacon/artifacts/public-deployment.json \
  --output-dir testing/embedded-beacon/artifacts/public-hoodi
```

Use the checked network configuration for the two genesis identities. The observer independently reads both endpoint genesis/schedule/chain identities, rejects optimistic/offline/syncing status, checks that the beacon and execution heads are recent, and compares a fixed common finalized signed block/state/payload and its actual execution block, gas/data and ordered receipts. Both live heads and finalized consensus/execution must advance during the observation. Connected peers alone earn no credit. The optional `--expected-live-fork gloas` requires the actual observed chain to be in Gloas; it does not modify the network schedule.

To verify a real restart, preserve the completed `verification.json` outside the next output directory and add `--resume-report PATH --resume-report-sha256 0x<exact SHA-256>`. The restarted node must preserve the original genesis/schedule identity, retrieve and agree on both saved finalized canonical anchors, then advance again. A changed/missing anchor or stale report receives no restart acceptance. The script observes a restart performed by the operator; it never stops or restarts services itself.

Public reports preserve `independent_ssz_root_computation: false` and do not assert complete historical genesis replay. Full fixture conformance and two successive finalized-cut recovery tests remain separate prerequisites. HTTP contract tests are synthetic; their success is never reported as actual public synchronization.
