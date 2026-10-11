# Independent fork devnet

The required setup has two logical nodes: one reference execution and consensus pair, and one custom embedded beacon node. The custom node must serve its own execution and beacon APIs from the same container. Validators and the transaction generator are auxiliary test services; validators belong only to the reference pair. The public-network follower does not need a validator.

**Deployment remains gated.** Complete authenticated consensus fixture conformance and native Linux launcher/Engine integration checks have passed. Independent runtime synchronization and sustained chain advancement have not been demonstrated. Every deployment still requires the compiled readiness gate before creating an enclave; fixture passes cannot authorize a live node.

All profiles use the mainnet preset, 32 slots per epoch and 6-second slots. Consecutive historical forks are exactly one epoch apart. Schedules include all eight forks, their versions, required observations and service endpoints. Earlier forks superseded at genesis do not count as observed transitions.

| Profile | Genesis | Required observed forks | Deadline |
| --- | --- | --- | --- |
| `historical` | Phase0 | Phase0 0; Altair 1; Bellatrix 2; Capella 3; Deneb 4; Electra 5; Fulu 6; Gloas 7 | 2400 seconds |
| `gloas-transition` | Fulu | Fulu 0; Gloas 1 | 900 seconds |
| `gloas-genesis` | Gloas | Gloas 0 | 900 seconds |

The historical profile also requires a real live Merge edge. The selected reference consensus image has removed that transition path. That limitation must produce a failed observation with retained diagnostics; neither a substitute reference image nor a skipped Bellatrix boundary is accepted. Gloas remains mandatory in each applicable profile.

The service catalog is a required design, not evidence of deployed services. It maps the reference execution API and consensus API to distinct reference services, and both custom APIs to `embedded-beacon-sut` with ports `rpc` and `beacon`. Container provenance must show the actual `--embedded-beacon` launch, one shared SUT container and the exact allowed service set. No additional consensus container is allowed on the custom side.

The deployment must generate execution genesis, consensus genesis, validator keys and network configuration once. Both logical nodes consume that same generated identity and schedule. The reference consensus node needs archive mode to retain historical state queries. The custom node uses the bounded, strict private-network input below:

```sh
node --chain /network-configs/genesis.json \
  --embedded-beacon \
  --beacon.devnet-config /network-configs/embedded-beacon.json \
  --beacon.sync-mode genesis \
  --beacon.genesis /network-configs/genesis.ssz \
  --datadir /data/embedded-beacon
```

The launcher implements these inputs and wires the same execution engine, shared storage, importer, peer runtime and optional read-only Beacon API. Native Linux integration checks have passed; the isolated deployment still requires independent runtime acceptance. Add `--beacon.http --beacon.http-addr 0.0.0.0`, a checked literal `--beacon.listen-ip`, optional translated `--beacon.bind-ip`, and `--beacon.bootnodes-file` containing the generated signed discovery records. These services cannot bypass the compiled acceptance gate. Generated private JSON must use schema 1, the actual execution genesis hash for both `execution_genesis_hash` and `terminal_block_hash`, the profile's Bellatrix epoch for `terminal_block_hash_activation_epoch`, and the complete `consensus` network subset accepted by `DevnetConfig`. Roots and versions use byte arrays. The generator must bind actual genesis time, validator root and chain ID 3151908, and derive execution fork timestamps from the same schedule. The launcher checks this binding; JSON metadata alone does not authenticate the full consensus genesis state.

Run from the repository root on Linux with Docker and Python 3.10 or newer:

```sh
bash testing/embedded-beacon/scripts/docker-build.sh
bash testing/embedded-beacon/scripts/docker-check.sh component-tests
bash testing/embedded-beacon/scripts/docker-check.sh engine-parity
bash testing/embedded-beacon/scripts/install-kurtosis-local.sh
bash testing/embedded-beacon/devnet/run.sh historical
bash testing/embedded-beacon/devnet/run.sh gloas-transition
bash testing/embedded-beacon/devnet/run.sh gloas-genesis
```

Each deployment command requires passing compiled readiness. `BEACON_IMAGE` selects the locally built image; `KURTOSIS_BIN` selects the localized CLI; `BEACON_ENCLAVE` selects the isolated namespace. A readiness environment override cannot authorize an external consensus baseline. Component and Engine integration checks remain useful, but do not establish independent beacon acceptance.

`package/pins.json` records the immutable reference execution image and its attested source revision, the selected reference consensus image, orchestration CLI 1.20.0, the package commit and archive SHA-256, and generator/helper/workload image digests. There is no alternate historical provider in the active configuration. The custom image is identified by its actual local image ID and build revision.

`prepare_package.py` verifies the archive before publication, cleans its own incomplete downloads, rejects unsafe archive paths and enforces resource bounds. Its additive adaptations pin helper images and relocate template paths under the ignored `package/vendor/` directory. It does not change reference consensus logic, Engine requests or launch providers. The prepared `embedded-lock.json` records those adaptations.

The workflow remains manual; automatic PR/merge-queue/main checks cover fixtures and optional native runtime wiring. The devnet workflow must keep every mandatory profile strict, retain reports and logs on failure, and require actual independent readiness. Earlier external-consensus runs use different topology and timing. Their retained artifacts are historical execution-integration evidence and cannot establish the required custom-node deployment. Public Hoodi and mainnet sync remain gated; mainnet acceptance requires independent public-network validation after Hoodi and complete conformance coverage.
