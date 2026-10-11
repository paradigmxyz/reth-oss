#!/usr/bin/env bash
# Resolve HTTPS hostnames without changing system DNS or disabling certificate checks.

beacon_resolve_ipv4() {
    local host="${1:-}" label raw='' address octet native_command
    local -a labels octets addresses=()
    local -A seen=()
    [[ "$#" == 1 && "${#host}" -le 253 && "$host" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || return 2
    IFS='.' read -r -a labels <<< "$host"
    [[ "$host" != *. ]] || return 2
    for label in "${labels[@]}"; do
        [[ "${#label}" -le 63 && "$label" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]] || return 2
    done
    command -v timeout >/dev/null 2>&1 || return 2

    if [[ -r /proc/sys/kernel/osrelease &&
          "$(</proc/sys/kernel/osrelease)" == *[Mm]icrosoft* ]] &&
        command -v powershell.exe >/dev/null 2>&1; then
        native_command="\$ErrorActionPreference = 'Stop'; try { \$lookup = [System.Net.Dns]::GetHostAddressesAsync('$host'); if (-not \$lookup.Wait(5000)) { exit 1 }; \$lookup.Result | Where-Object { \$_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork } | Select-Object -First 16 -ExpandProperty IPAddressToString } catch { exit 1 }"
        raw="$(timeout --kill-after=1s 8s powershell.exe -NoLogo -NoProfile -NonInteractive -Command "$native_command" </dev/null 2>/dev/null)" || raw=''
    fi
    if [[ -z "$raw" ]]; then
        command -v getent >/dev/null 2>&1 || return 1
        raw="$(timeout --kill-after=1s 6s getent ahostsv4 "$host" 2>/dev/null)" || raw=''
    fi
    while read -r address _; do
        address="${address%$'\r'}"
        [[ "$address" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || continue
        IFS='.' read -r -a octets <<< "$address"
        for octet in "${octets[@]}"; do
            [[ "$((10#$octet))" -le 255 ]] || break
        done
        [[ "$((10#$octet))" -le 255 ]] || continue
        address="$((10#${octets[0]})).$((10#${octets[1]})).$((10#${octets[2]})).$((10#${octets[3]}))"
        [[ -z "${seen[$address]+present}" ]] || continue
        seen["$address"]=1
        addresses+=("$address")
        [[ "${#addresses[@]}" -lt 16 ]] || break
    done <<< "$raw"
    [[ "${#addresses[@]}" -gt 0 ]] || return 1
    printf '%s\n' "${addresses[@]}"
}

# Read the two emitted lines into a curl argument array; never evaluate them as shell code.
beacon_https_resolve_args() {
    local url="${1:-}" host addresses address joined=''
    [[ "$#" == 1 && "$url" != *[[:cntrl:]]* &&
       "$url" =~ ^https://([^/:?#]+)(:443)?([/?#].*)?$ ]] || return 2
    host="${BASH_REMATCH[1]}"
    addresses="$(beacon_resolve_ipv4 "$host")" || return
    while IFS= read -r address; do
        joined+="${joined:+,}$address"
    done <<< "$addresses"
    printf '%s\n' --resolve "$host:443:$joined"
}
