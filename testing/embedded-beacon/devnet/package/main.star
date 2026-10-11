"""Two logical nodes sharing generated identity, with independent embedded SUT consensus."""

reference_package = import_module("./vendor/main.star")
pins = json.decode(read_file("./pins.json"))
FORK_NAMES = ["phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"]
FORK_SCHEMAS = ["Phase0", "Altair", "Bellatrix", "Capella", "Deneb", "Electra", "Fulu", "Gloas"]
FAR_FUTURE_EPOCH = 18446744073709551615


def checked_schedule(profile):
    if profile not in ["historical", "gloas-transition", "gloas-genesis"]:
        fail("Unknown profile: {0}".format(profile))
    schedule = json.decode(read_file("./schedules/{0}.json".format(profile)))
    if schedule["slots_per_epoch"] != 32 or schedule["slot_duration_ms"] != 6000 or schedule["seconds_per_slot"] != 6:
        fail("The isolated network requires 32 slots per epoch and 6-second slots")
    if schedule["logical_node_count"] != 2 or schedule["sut_consensus_mode"] != "embedded_only":
        fail("The topology requires one reference pair and one independent embedded node")
    expected_epochs = list(range(8)) if profile == "historical" else [0, 0, 0, 0, 0, 0, 0, 1 if profile == "gloas-transition" else 0]
    for index in range(len(FORK_NAMES)):
        if schedule["fork_epochs"][FORK_NAMES[index]] != expected_epochs[index]:
            fail("Fork schedule differs from the requested one-epoch boundaries")
        expected_version = "0x{0}0000038".format(index + 1)
        if schedule["fork_versions"][FORK_NAMES[index]] != expected_version:
            fail("Fork version differs from the pinned generator's network version")
    if schedule["services"] != pins["services"]:
        fail("Service catalog differs from the pinned topology")
    if schedule["services"]["sut_el"]["name"] != schedule["services"]["sut_beacon"]["name"]:
        fail("Execution and embedded consensus must share one service")
    return schedule


def reference_args(schedule):
    network = {
        "network": "kurtosis",
        "network_id": "3151908",
        "preset": "mainnet",
        "seconds_per_slot": 6,
        "slot_duration_ms": 6000,
        "genesis_delay": 300,
        "num_validator_keys_per_node": 128,
        "preregistered_validator_count": 128,
        "max_payload_size": 10485760,
        "genesis_gaslimit": 60000000,
        "gas_limit": 60000000,
        "builder_count": 0,
    }
    for fork in FORK_NAMES[1:]:
        network[fork + "_fork_epoch"] = schedule["fork_epochs"][fork]
    # Preserve one blob schedule across both owners; no hidden default BPO at genesis.
    for number in range(1, 6):
        network["bpo_{0}_epoch".format(number)] = FAR_FUTURE_EPOCH
    return {
        "participants": [{
            "el_type": "reth",
            "el_image": pins["images"]["reference_el"]["image"],
            "cl_type": "lighthouse",
            "cl_image": pins["images"]["beacon_and_validator"]["image"],
            "vc_type": "lighthouse",
            "vc_image": pins["images"]["beacon_and_validator"]["image"],
            "count": 1,
            "validator_count": 128,
            "supernode": True,
            "cl_extra_params": ["--slots-per-restore-point=32"],
        }],
        "network_params": network,
        "ethereum_genesis_generator_params": {"image": pins["images"]["genesis_generator"]["image"]},
        "spamoor_params": {"image": pins["images"]["transaction_generator"]["image"]},
        "additional_services": ["spamoor"],
        "wait_for_finalization": False,
        "parallel_keystore_generation": False,
    }


def embedded_config(schedule):
    activations = []
    for index in range(len(FORK_NAMES)):
        version = schedule["fork_versions"][FORK_NAMES[index]]
        activations.append({
            "fork": FORK_SCHEMAS[index],
            "epoch": schedule["fork_epochs"][FORK_NAMES[index]],
            "version": [int(version[2 + offset:4 + offset], 16) for offset in range(0, 8, 2)],
        })
    # Runtime generation fills original genesis identity; these are not trusted roots.
    return {
        "schema_version": 1,
        "execution_genesis_hash": [],
        "terminal_block_hash": [],
        "terminal_block_hash_activation_epoch": schedule["fork_epochs"]["bellatrix"],
        "consensus": {
            "name": "embedded-" + schedule["profile"],
            "preset": "Mainnet",
            "chain_id": 3151908,
            "genesis_time": 0,
            "genesis_validators_root": [],
            "seconds_per_slot": 6,
            "slots_per_epoch": 32,
            "forks": {"activations": activations},
            "blob_schedule": [],
            "max_payload_size": 10485760,
        },
    }


def run(plan, args={}):
    profile = args.get("profile", "gloas-transition")
    schedule = checked_schedule(profile)
    image = args.get("sut_image", "embedded-beacon:test")
    if not image or len(image) > 512:
        fail("A bounded local embedded Reth image is required")
    if profile == "historical":
        # Pinned primary evidence: reference-compatibility.json. The provider takes the
        # zero pre-Merge execution header directly; TERMINAL_BLOCK_HASH does not repair it.
        fail("Historical live Merge is unsupported by the selected immutable reference: terminal selection is absent from its block-production path. All eight fork observations remain required; choose a verified compatible reference before deployment.")

    # A direct package invocation cannot skip the compiled full-conformance gate.
    # No persistent participant service is created before this actual image check succeeds.
    readiness = plan.run_sh(
        name="check-embedded-readiness",
        image=image,
        run="/usr/local/bin/reth-embedded-beacon node --beacon.status > /tmp/readiness.json",
        store=[StoreSpec(src="/tmp/readiness.json", name="embedded-readiness")],
    )
    scripts = plan.upload_files(src="./runtime", name="embedded-devnet-runtime")
    plan.run_sh(
        name="require-embedded-conformance",
        image=pins["images"]["genesis_generator"]["image"],
        run="sh /runtime/validate-readiness.sh /readiness/readiness.json",
        files={"/runtime": scripts, "/readiness": readiness.files_artifacts[0]},
    )

    # Exactly one real reference pair produces signed blocks, attestations and payloads.
    # The SUT is never included in the reference package's CL participant list.
    reference = reference_package.run(plan, reference_args(schedule))
    if len(reference.all_participants) != 1:
        fail("The reference package must produce exactly one logical participant")
    participant = reference.all_participants[0]
    genesis = "el_cl_genesis_data"
    jwt = "jwt_file"
    plan.get_files_artifact(name=genesis)
    plan.get_files_artifact(name=jwt)
    inputs = plan.render_templates({
        "embedded-template.json": struct(template=json.encode(embedded_config(schedule)), data={}),
        "genesis-time.txt": struct(template="{{.GenesisTime}}", data={"GenesisTime": reference.final_genesis_timestamp}),
        # This signed ENR is read by the reference package from its actual node identity API.
        "bootnodes.txt": struct(template="{{.ENR}}\n", data={"ENR": participant.cl_context.enr}),
    }, name="embedded-network-inputs")
    generated = plan.run_sh(
        name="bind-embedded-network",
        image=pins["images"]["genesis_generator"]["image"],
        run="sh /runtime/build-network.sh",
        files={"/runtime": scripts, "/network-configs": genesis, "/inputs": inputs},
        store=[StoreSpec(src="/embedded-network", name="embedded-network")],
    )
    sut = plan.add_service(
        name=schedule["services"]["sut_el"]["name"],
        config=ServiceConfig(
            image=image,
            entrypoint=["/usr/local/bin/reth-embedded-beacon"],
            cmd=[
                "node", "--chain", "/network-configs/genesis.json",
                "--datadir", "/data/embedded-beacon", "--embedded-beacon",
                "--beacon.devnet-config", "/embedded-network/embedded-beacon.json",
                "--beacon.sync-mode", "genesis", "--beacon.genesis", "/network-configs/genesis.ssz",
                "--beacon.listen-ip", "KURTOSIS_IP_ADDR_PLACEHOLDER", "--beacon.bind-ip", "0.0.0.0",
                "--beacon.port", "9000", "--beacon.quic-port", "9001",
                "--beacon.bootnodes-file", "/embedded-network/bootnodes.txt",
                "--beacon.http", "--beacon.http-addr", "0.0.0.0", "--beacon.http-port", "5052",
                "--http", "--http.addr", "0.0.0.0", "--http.port", "8545", "--http.api", "eth,net,web3",
                "--authrpc.addr", "127.0.0.1", "--authrpc.jwtsecret", "/jwt/jwtsecret",
                "--port", "30303", "--bootnodes", participant.el_context.enode,
            ],
            ports={
                "rpc": PortSpec(number=8545, application_protocol="http", wait="120s"),
                "beacon": PortSpec(number=5052, application_protocol="http", wait="120s"),
                "beacon-tcp": PortSpec(number=9000, transport_protocol="TCP", wait=None),
                "beacon-discovery": PortSpec(number=9000, transport_protocol="UDP", wait=None),
                "beacon-quic": PortSpec(number=9001, transport_protocol="UDP", wait=None),
                "execution-tcp": PortSpec(number=30303, transport_protocol="TCP", wait=None),
                "execution-discovery": PortSpec(number=30303, transport_protocol="UDP", wait=None),
            },
            files={"/network-configs": genesis, "/embedded-network": generated.files_artifacts[0], "/jwt": jwt},
            private_ip_address_placeholder="KURTOSIS_IP_ADDR_PLACEHOLDER",
            env_vars={"RUST_BACKTRACE": "1"},
        ),
    )
    # Planned endpoints are useful output, never authenticated sync/deployment evidence.
    return struct(profile=profile, required_services=schedule["services"], sut=sut.name, acceptance="unverified")
