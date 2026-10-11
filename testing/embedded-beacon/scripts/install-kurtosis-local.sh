#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
version=1.20.0
root="$PWD/testing/embedded-beacon/devnet/work/tools"
mkdir -p "$root"
base="https://github.com/kurtosis-tech/kurtosis-cli-release-artifacts/releases/download/$version"
file="kurtosis-cli_${version}_linux_amd64.tar.gz"
curl -fLsS --connect-timeout 20 --max-time 240 --retry 3 "$base/checksums.txt" -o "$root/checksums.txt"
curl -fLsS --connect-timeout 20 --max-time 240 --retry 3 "$base/$file" -o "$root/$file"
cd "$root"
grep -E "[ *]$file$" checksums.txt > selected-checksum.txt
test "$(wc -l < selected-checksum.txt)" -eq 1
sha256sum --check selected-checksum.txt
printf '3866e805a83a63d3d0b177f54c3fb556066b8c0e12f6442a0579bb7deaa1d5ae  %s\n' "$file" | sha256sum --check
tar -xzf "$file" kurtosis
./kurtosis version
./kurtosis engine start
./kurtosis engine status
