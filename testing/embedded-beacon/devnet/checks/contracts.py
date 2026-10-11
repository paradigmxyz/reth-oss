"""Sanitized HTTP contracts and deployed consensus configuration validation."""

from dataclasses import dataclass
import json
import re
import time
from urllib import error, parse, request

FORKS = ("phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas")
FAR_FUTURE_EPOCH = (1 << 64) - 1
MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class CheckError(Exception):
    """A sanitized protocol or local-artifact failure."""


class HttpFailure(CheckError):
    """A sanitized HTTP failure retaining status for explicit contract fallbacks."""

    def __init__(self, role, path, code):
        self.code = code
        self.path = path
        super().__init__(f"{role} request failed at {path} (HTTP {code})")

class RpcFailure(CheckError):
    """A sanitized RPC failure retaining its numeric code for readiness handling."""

    def __init__(self, role, method, code):
        self.code = code
        super().__init__(f"{role} RPC {method} failed (code {code})")


class NotReady(CheckError):
    """An unmet condition which can become true before the deadline."""


@dataclass(frozen=True)
class Header:
    slot: int
    root: str


def obj(value, name):
    if not isinstance(value, dict):
        raise CheckError(f"{name} must be an object")
    return value


def array(value, name):
    if not isinstance(value, list):
        raise CheckError(f"{name} must be an array")
    return value


def decimal(value, name, maximum=FAR_FUTURE_EPOCH):
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise CheckError(f"{name} must be a decimal string")
    result = int(value)
    if result > maximum:
        raise CheckError(f"{name} exceeds uint64")
    return result


def quantity(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value):
        raise CheckError(f"{name} must be a canonical hex quantity")
    return int(value, 16)


def hex_bytes(value, size, name):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]*", value) or len(value) != 2 + size * 2:
        raise CheckError(f"{name} has an invalid hex byte encoding")
    return value.lower()


def normalized(value):
    if isinstance(value, str) and value.startswith("0x"):
        return value.lower()
    if isinstance(value, list):
        return [normalized(item) for item in value]
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in sorted(value.items())}
    return value


class HttpTransport:
    def __init__(self, deadline, request_timeout=10):
        self.deadline = deadline
        self.request_timeout = request_timeout

    def exchange(self, url, role, path, payload=None, missing_ok=False):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise NotReady("verification deadline expired")
        encoded = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        req = request.Request(url + path, data=encoded, headers=headers)
        # These paths are fixed API routes or validated public block/state identifiers.
        diagnostic_path = path.split("?", 1)[0].split("#", 1)[0] or "JSON-RPC"
        try:
            with request.urlopen(req, timeout=min(self.request_timeout, remaining)) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except error.HTTPError as exc:
            code = exc.code
            exc.close()
            if missing_ok and code == 404:
                return None
            if code in (429, 502, 503, 504):
                raise NotReady(f"{role} unavailable at {diagnostic_path} (HTTP {code})") from None
            raise HttpFailure(role, diagnostic_path, code) from None
        except (error.URLError, TimeoutError, ConnectionError):
            raise NotReady(f"{role} transport unavailable at {diagnostic_path}") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise CheckError(f"{role} response exceeds byte limit")
        try:
            return json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            raise CheckError(f"{role} returned malformed JSON") from None


class Endpoint:
    def __init__(self, url, role, transport):
        parsed = parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise CheckError(f"{role} requires an HTTP endpoint")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise CheckError(f"{role} endpoint cannot embed credentials or query parameters")
        self.url, self.role, self.transport = url.rstrip("/"), role, transport
        self.request_id = 0

    def beacon(self, path, missing_ok=False):
        return self.transport.exchange(self.url, self.role, path, missing_ok=missing_ok)

    def rpc(self, method, params):
        self.request_id += 1
        payload = {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params}
        response = obj(self.transport.exchange(self.url, self.role, "", payload), "RPC response")
        if response.get("jsonrpc") != "2.0" or type(response.get("id")) is not int or response["id"] != self.request_id:
            raise CheckError(f"{self.role} RPC response identifier or version differs")
        if "error" in response:
            code = obj(response["error"], "RPC error").get("code")
            if type(code) is not int:
                raise CheckError(f"{self.role} malformed RPC error")
            raise RpcFailure(self.role, method, code)
        if "result" not in response:
            raise CheckError(f"{self.role} RPC result is absent")
        return response["result"]


def beacon_data(endpoint, path):
    return obj(endpoint.beacon(path), "beacon response").get("data")


def schedule(endpoint):
    spec = obj(beacon_data(endpoint, "/eth/v1/config/spec"), "consensus spec")
    slots = decimal(spec.get("SLOTS_PER_EPOCH"), "SLOTS_PER_EPOCH")
    duration = decimal(spec["SLOT_DURATION_MS"], "SLOT_DURATION_MS") if "SLOT_DURATION_MS" in spec else 1000 * decimal(spec.get("SECONDS_PER_SLOT"), "SECONDS_PER_SLOT")
    if slots == 0 or duration == 0:
        raise CheckError("consensus clock values must be positive")
    forks = [{"name": "phase0", "epoch": 0, "version": hex_bytes(spec.get("GENESIS_FORK_VERSION"), 4, "genesis fork version")}]
    for name in FORKS[1:]:
        key = name.upper()
        if f"{key}_FORK_EPOCH" in spec or f"{key}_FORK_VERSION" in spec:
            forks.append({"name": name, "epoch": decimal(spec.get(f"{key}_FORK_EPOCH"), f"{key} fork epoch"), "version": hex_bytes(spec.get(f"{key}_FORK_VERSION"), 4, f"{key} fork version")})
    previous, versions = 0, set()
    for fork in forks:
        if fork["epoch"] < previous or fork["version"] in versions:
            raise CheckError("fork epochs regress or versions repeat")
        previous = fork["epoch"]
        versions.add(fork["version"])
    observed = {}
    for entry in array(beacon_data(endpoint, "/eth/v1/config/fork_schedule"), "fork schedule"):
        entry = obj(entry, "fork entry")
        version = hex_bytes(entry.get("current_version"), 4, "scheduled current version")
        hex_bytes(entry.get("previous_version"), 4, "scheduled previous version")
        if version in observed:
            raise CheckError("fork schedule repeats a current version")
        observed[version] = decimal(entry.get("epoch"), "scheduled epoch")
    for fork in forks:
        if (fork["epoch"] != FAR_FUTURE_EPOCH or fork["version"] in observed) and observed.get(fork["version"]) != fork["epoch"]:
            raise CheckError(f"fork schedule API disagrees with spec for {fork['name']}")
    if set(observed) - versions:
        raise CheckError("fork schedule contains an unknown version")
    genesis = obj(beacon_data(endpoint, "/eth/v1/beacon/genesis"), "beacon genesis")
    identity = {"genesis_time": decimal(genesis.get("genesis_time"), "genesis time"), "genesis_validators_root": hex_bytes(genesis.get("genesis_validators_root"), 32, "genesis validators root"), "genesis_fork_version": hex_bytes(genesis.get("genesis_fork_version"), 4, "genesis fork version")}
    if identity["genesis_fork_version"] not in {fork["version"] for fork in forks if fork["epoch"] == 0}:
        raise CheckError("genesis fork version is not active at genesis")
    config = {"slots_per_epoch": slots, "slot_duration_ms": duration, "forks": forks, "genesis": identity}
    if "TERMINAL_BLOCK_HASH" in spec or "TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH" in spec:
        config["terminal_block"] = {
            "hash": hex_bytes(spec.get("TERMINAL_BLOCK_HASH"), 32, "terminal block hash"),
            "activation_epoch": decimal(spec.get("TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH"), "terminal block activation epoch"),
        }
    return config


def active_fork(config, slot):
    epoch, active = slot // config["slots_per_epoch"], None
    for entry in config["forks"]:
        if entry["epoch"] <= epoch:
            active = entry["name"]
    if active is None:
        raise CheckError("no configured fork at requested slot")
    return active


def required_forks(config, names):
    entries = {entry["name"]: entry for entry in config["forks"]}
    for name in names:
        if name not in FORKS or name not in entries or entries[name]["epoch"] == FAR_FUTURE_EPOCH:
            raise CheckError(f"required fork {name} is unsupported or unscheduled")
        if active_fork(config, entries[name]["epoch"] * config["slots_per_epoch"]) != name:
            raise CheckError(f"required fork {name} was superseded at genesis and needs a separate historical case")
    return [entries[name] for name in names]
