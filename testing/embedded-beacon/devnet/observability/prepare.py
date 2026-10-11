#!/usr/bin/env python3
"""Prepare local monitoring config and refuse unrelated Docker resource collisions."""

import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

OWNER = "io.reth.embedded-beacon.observability"


def fail(message):
    raise SystemExit(message)


def docker(*args, absent_ok=False):
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if result.returncode and not absent_ok:
        fail(result.stderr.strip() or "Docker command failed.")
    return result


def inspect(kind, name):
    result = docker(kind, "inspect", name, absent_ok=True)
    if not result.returncode:
        return json.loads(result.stdout)[0]
    # Missing resources are permitted; daemon or permission failures are not.
    if not any(message in result.stderr.lower() for message in ("no such container", "no such volume")):
        fail(result.stderr.strip() or f"Cannot inspect {name}.")
    return None


def port(variable, default):
    raw = os.environ.get(variable, default)
    if not raw.isdecimal() or not 1 <= int(raw) <= 65535:
        fail(f"{variable} must be a port between 1 and 65535.")
    return int(raw)


def check_resources(project, ports, check_ports):
    existing = {}
    for service, number in ports.items():
        name = f"{project}-{service}"
        container = inspect("container", name)
        if container and (container["Config"].get("Labels") or {}).get(OWNER) != project:
            fail(f"Container {name} belongs to another setup; choose a different project name.")
        existing[service] = container
        volume_name = f"{name}-data"
        volume = inspect("volume", volume_name)
        if volume and (volume.get("Labels") or {}).get(OWNER) != project:
            fail(f"Volume {volume_name} belongs to another setup; choose a different project name.")
        if check_ports:
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", number))
                except OSError:
                    labels = (container or {}).get("Config", {}).get("Labels") or {}
                    owned_listener = bool(
                        container
                        and container["State"]["Running"]
                        and container["HostConfig"]["NetworkMode"] == "host"
                        and labels.get(f"{OWNER}.port") == str(number)
                    )
                    if not owned_listener:
                        fail(f"Local port {number} is busy; set a different monitoring port.")
    # Compose commands may affect all containers bearing this project label.
    ids = docker("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}").stdout.split()
    for container_id in ids:
        container = inspect("container", container_id)
        if (container["Config"].get("Labels") or {}).get(OWNER) != project:
            fail(f"Docker Compose project {project} contains an unrelated container.")


def ready(ports, wait):
    deadline = time.monotonic() + (60 if wait else 0)
    while True:
        failures = []
        for service, path in [("prometheus", "/-/ready"), ("grafana", "/api/health")]:
            try:
                with urlopen(f"http://127.0.0.1:{ports[service]}{path}", timeout=2) as response:
                    if response.status != 200:
                        failures.append(service)
            except (OSError, URLError):
                failures.append(service)
        if not failures:
            print("Prometheus and Grafana are ready.")
            return
        if time.monotonic() >= deadline:
            fail("Monitoring is not ready: " + ", ".join(failures) + ". Inspect docker compose logs.")
        time.sleep(1)


def main():
    stack = Path(sys.argv[1]).resolve()
    action = sys.argv[2]
    project = os.environ["BEACON_OBSERVABILITY_PROJECT"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,47}", project):
        fail("BEACON_OBSERVABILITY_PROJECT must contain 1-48 lowercase Docker name characters.")
    ports = {
        "prometheus": port("BEACON_PROMETHEUS_PORT", "9090"),
        "grafana": port("BEACON_GRAFANA_PORT", "3000"),
    }
    if action in {"ready", "status"}:
        ready(ports, action == "ready")
        return
    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint:
        endpoint = docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}").stdout.strip()
    if not endpoint.startswith("unix://"):
        fail("This stack requires a local Linux Docker daemon with host networking.")
    if docker("info", "--format", "{{.OSType}}").stdout.strip() != "linux":
        fail("This stack requires Linux Docker containers.")
    raw_target = os.environ["BEACON_METRICS_ADDR"]
    host, separator, raw_port = raw_target.rpartition(":")
    if host == "localhost":
        host = "127.0.0.1"
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        fail("BEACON_METRICS_ADDR must be a loopback IPv4 address and port.")
    if not separator or address.version != 4 or not address.is_loopback:
        fail("BEACON_METRICS_ADDR must be a loopback IPv4 address and port.")
    if not raw_port.isdecimal() or not 1 <= int(raw_port) <= 65535:
        fail("BEACON_METRICS_ADDR has an invalid port.")
    if len({*ports.values(), int(raw_port)}) != 3:
        fail("Metrics, Prometheus and Grafana ports must be distinct.")
    for variable in ("BEACON_PROMETHEUS_IMAGE", "BEACON_GRAFANA_IMAGE"):
        image = os.environ.get(variable)
        if image and (":" not in image or image.endswith(":latest")):
            fail(f"{variable} must specify a pinned tag or digest.")
    check_resources(project, ports, action == "up")
    work = stack / "work" / project
    if work.is_symlink() or (stack / "work").is_symlink():
        fail("Monitoring work directories must not be symbolic links.")
    work.mkdir(parents=True, exist_ok=True)
    prometheus = (
        "global:\n  scrape_interval: 5s\n  evaluation_interval: 5s\n"
        "scrape_configs:\n  - job_name: embedded-beacon\n"
        "    metrics_path: /\n    scrape_timeout: 4s\n"
        "    static_configs:\n"
        f"      - targets: ['{host}:{int(raw_port)}']\n"
    )
    config = work / "prometheus.yml"
    if config.is_symlink():
        fail("Monitoring configuration must not be a symbolic link.")
    # Avoid changing the inode mounted by an already-running Prometheus.
    if not config.exists() or config.read_text() != prometheus:
        config.write_text(prometheus)
    credentials = work / "admin.env"
    if credentials.is_symlink():
        fail("Monitoring credentials must not be a symbolic link.")
    if not credentials.exists():
        descriptor = os.open(credentials, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write("GF_SECURITY_ADMIN_PASSWORD=" + secrets.token_hex(32) + "\n")
    os.chmod(credentials, 0o600)


if __name__ == "__main__":
    main()
