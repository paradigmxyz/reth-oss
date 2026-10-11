"""Live container identity and endpoint binding for independent embedded-node tests."""

import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit
from contracts import CheckError, array, obj

ENCLAVE_LABEL = "com.kurtosistech.enclave-id"
TYPE_LABEL = "com.kurtosistech.container-type"
ID_LABEL = "com.kurtosistech.id"
NAME_LABEL = "kurtosis_service_name"
ROLES = ("reference_execution", "reference_beacon", "sut")
PORT_ROLES = ("reference_execution", "reference_beacon", "sut_execution", "sut_beacon")


def docker_output(arguments):
    try:
        result = subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise CheckError("live Docker deployment inspection is unavailable") from None
    if result.returncode != 0 or len(result.stdout) > 4 * 1024 * 1024:
        raise CheckError("live Docker deployment inspection failed")
    return result.stdout


def live_containers(enclave):
    identifiers = docker_output(["ps", "-aq", "--no-trunc", "--filter", f"label={ENCLAVE_LABEL}={enclave}", "--filter", f"label={TYPE_LABEL}=user-service"]).splitlines()
    if not identifiers or len(identifiers) > 64 or len(set(identifiers)) != len(identifiers) or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in identifiers):
        raise CheckError("live independent deployment has no valid exact container set")
    try:
        containers = array(json.loads(docker_output(["inspect", *identifiers])), "live container inspection")
    except ValueError:
        raise CheckError("live container inspection returned malformed JSON") from None
    if {obj(entry, "container").get("Id") for entry in containers} != set(identifiers) or len(containers) != len(identifiers):
        raise CheckError("live container inspection differs from enumerated enclave containers")
    return containers


def endpoint_binding(url, container, port_key):
    try:
        parsed = urlsplit(url)
        address = ipaddress.ip_address(parsed.hostname or "")
        port = parsed.port
    except (TypeError, ValueError):
        raise CheckError("independent endpoint must use a literal loopback IP and published port") from None
    if parsed.scheme not in ("http", "https") or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment or parsed.path not in ("", "/") or not address.is_loopback or port is None:
        raise CheckError("independent endpoint must use a literal loopback IP and published port")
    bindings = array(obj(obj(container.get("NetworkSettings"), "container network").get("Ports"), "container ports").get(port_key), "published container port")
    for binding in bindings:
        binding = obj(binding, "host port binding")
        try:
            host = ipaddress.ip_address(binding.get("HostIp", ""))
            host_port = int(binding.get("HostPort", ""))
        except (ValueError, TypeError):
            continue
        if host.version == address.version and (host == address or host.is_unspecified) and host_port == port:
            return {"container_port": port_key, "host_ip": str(address), "host_port": port}
    raise CheckError("independent endpoint is not bound to the declared live participant container")


def deployment_provenance(path, args):
    if path is None:
        raise CheckError("independent verification requires live deployment provenance")
    try:
        expected = obj(json.loads(Path(path).read_text(encoding="utf-8-sig")), "expected deployment")
    except (OSError, ValueError):
        raise CheckError("expected deployment provenance is missing or malformed") from None
    if expected.get("schema_version") != 1 or type(expected.get("schema_version")) is not int:
        raise CheckError("unsupported deployment provenance schema")
    enclave = expected.get("enclave_uuid")
    if not isinstance(enclave, str) or not re.fullmatch(r"[0-9a-f]{32}", enclave):
        raise CheckError("deployment provenance requires the exact enclave UUID")
    roles, ports = obj(expected.get("roles"), "participant roles"), obj(expected.get("ports"), "participant ports")
    allowed = array(expected.get("allowed_services"), "allowed deployment services")
    images = obj(expected.get("images"), "expected participant images")
    if set(images) != set(ROLES) or any(not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value) for value in images.values()):
        raise CheckError("deployment provenance requires the three expected immutable image IDs")
    if set(roles) != set(ROLES) or any(not isinstance(roles[key], str) or not roles[key] for key in ROLES) or len(set(roles.values())) != 3:
        raise CheckError("independent deployment requires exactly one reference pair and one distinct embedded SUT")
    if set(ports) != set(PORT_ROLES) or any(not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,4}/tcp", value) or int(value.split("/")[0]) > 65535 for value in ports.values()):
        raise CheckError("deployment provenance requires exact participant TCP ports")
    if not allowed or len(allowed) > 64 or any(not isinstance(name, str) or not name for name in allowed) or len(set(allowed)) != len(allowed) or not set(roles.values()).issubset(allowed):
        raise CheckError("deployment provenance requires a complete unique service set")
    if any(name.startswith("cl-") and name not in (roles["reference_beacon"], roles["sut"]) for name in allowed) or any(name.startswith("vc-") and not name.startswith("vc-1-") for name in allowed):
        raise CheckError("independent topology forbids an additional SUT consensus or validator service")
    actual = {}
    for value in live_containers(enclave):
        value = obj(value, "live container")
        labels = obj(obj(value.get("Config"), "container configuration").get("Labels"), "container labels")
        name = labels.get(ID_LABEL)
        if labels.get(ENCLAVE_LABEL) != enclave or labels.get(TYPE_LABEL) != "user-service" or name != labels.get(NAME_LABEL) or not isinstance(name, str) or name in actual:
            raise CheckError("live container labels do not uniquely identify the expected enclave services")
        actual[name] = value
    if set(actual) != set(allowed):
        raise CheckError("live deployment service set differs from the exact independent plan")
    evidence = {"source": "live_docker_inspection", "enclave_uuid": enclave, "service_names": sorted(actual), "roles": {}, "endpoint_bindings": {}}
    for role in ROLES:
        container = actual[roles[role]]
        if obj(container.get("State"), "container state").get("Running") is not True:
            raise CheckError("independent participant container is not running")
        image, identifier = container.get("Image"), container.get("Id")
        if not isinstance(image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image) or not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{64}", identifier):
            raise CheckError("live participant lacks an immutable image or container identity")
        if image != images[role]:
            raise CheckError("live participant image differs from the independently planned image")
        config = container["Config"]
        entrypoint, command = config.get("Entrypoint") or [], config.get("Cmd") or []
        if not isinstance(entrypoint, list) or not isinstance(command, list) or any(not isinstance(value, str) for value in entrypoint + command):
            raise CheckError("live participant command is not inspectable")
        argv = entrypoint + command
        if role == "sut" and (argv.count("--embedded-beacon") != 1 or "--embedded-beacon=false" in argv):
            raise CheckError("live SUT was not launched with the embedded beacon flag")
        evidence["roles"][role] = {"service": roles[role], "container_id": identifier, "image_id": image, "argv_digest": hashlib.sha256(json.dumps(argv, separators=(",", ":")).encode()).hexdigest()}
    urls = {"reference_execution": args.reference_el_url, "reference_beacon": args.reference_beacon_url, "sut_execution": args.sut_el_url, "sut_beacon": args.sut_beacon_url}
    for key in PORT_ROLES:
        role = "sut" if key.startswith("sut_") else key
        evidence["endpoint_bindings"][key] = endpoint_binding(urls[key], actual[roles[role]], ports[key])
    if evidence["endpoint_bindings"]["sut_execution"] == evidence["endpoint_bindings"]["sut_beacon"]:
        raise CheckError("embedded consensus and execution APIs require distinct declared ports")
    return evidence
