#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <hive-dir> <results-dir>" >&2
  exit 2
fi

RETH_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HIVE_DIR="$(cd "$1" && pwd)"
RESULTS="$2"
NORMALIZER="$RETH_REPO/.github/scripts/hive/normalize_results.py"
ANALYZER="$RETH_REPO/.github/scripts/hive/analyze_partial_persistence.py"
EXCLUSIONS="$RETH_REPO/.github/scripts/hive/expected_failures.yaml"
IGNORED="$RETH_REPO/.github/scripts/hive/ignored_tests.yaml"
SEED=424242
PARALLELISM=16
RPC_CI_FILTER='/(eth_blockNumber|eth_call|eth_chainId|eth_createAccessList|eth_estimateGas|eth_feeHistory|eth_getBalance|eth_getBlockBy|eth_getBlockTransactionCountBy|eth_getCode|eth_getProof|eth_getStorage|eth_getTransactionBy|eth_getTransactionCount|eth_getTransactionReceipt|eth_sendRawTransaction|eth_syncing|debug_)'
RPC_FULL_FILTER='/'
PARTIAL_FLAGS='--engine.persistence-threshold 2 --engine.persistence-backpressure-threshold 4 --engine.memory-block-buffer-target 0 --engine.num-state-masking-blocks 1'
CLIENT_IMAGE='ghcr.io/paradigmxyz/reth:latest'

mkdir -p "$RESULTS"
printf '%s\n' "$RPC_CI_FILTER" > "$RESULTS/rpc-ci-filter.txt"
printf '%s\n' "$RPC_FULL_FILTER" > "$RESULTS/rpc-full-filter.txt"
printf '%s\n' "$SEED" > "$RESULTS/random-seed.txt"
printf '%s\n' "$PARALLELISM" > "$RESULTS/parallelism.txt"
python3 "$ANALYZER" rpc-ci-filter \
  "$RETH_REPO/.github/workflows/hive.yml" "$RPC_CI_FILTER" \
  > "$RESULTS/rpc-ci-filter-from-workflow.txt"

cd "$HIVE_DIR"
cp clients/reth/reth.sh clients/reth/reth.sh.baseline
awk -v flags="$PARTIAL_FLAGS" '
  /# Launch the main client\./ {
    print "FLAGS=\"$FLAGS " flags "\""
  }
  { print }
' clients/reth/reth.sh.baseline > clients/reth/reth.sh.partial
chmod +x clients/reth/reth.sh.baseline clients/reth/reth.sh.partial

if [[ "$(grep -cF '# Launch the main client.' clients/reth/reth.sh.baseline)" -ne 1 ]]; then
  echo "Reth launcher does not contain exactly one main-client launch marker" >&2
  exit 1
fi
grep -F -- "$PARTIAL_FLAGS" clients/reth/reth.sh.partial > "$RESULTS/partial-launcher-flags.txt"

select_mode() {
  local mode="$1"
  case "$mode" in
    baseline) cp clients/reth/reth.sh.baseline clients/reth/reth.sh ;;
    partial) cp clients/reth/reth.sh.partial clients/reth/reth.sh ;;
    *) echo "unknown mode: $mode" >&2; return 2 ;;
  esac
  chmod +x clients/reth/reth.sh
}

run_one() {
  local mode="$1"
  local simulator="$2"
  local limit="$3"
  local label="$4"
  local parallelism="$5"
  local out="$RESULTS/$label"
  local status
  local simulator_image="hive/simulators/$simulator:latest"

  mkdir -p "$out"
  select_mode "$mode"
  if [[ -d "$HIVE_DIR/workspace/logs" ]]; then
    rm -rf -- "$HIVE_DIR/workspace/logs"
  fi

  docker image inspect "$CLIENT_IMAGE" --format '{{.Id}}' > "$out/reth-image-id.txt"
  printf '%q ' ./hive --sim "$simulator" --sim.limit "$limit" \
    --sim.limit.exact=false --sim.parallelism "$parallelism" \
    --sim.randomseed "$SEED" --sim.loglevel 4 --client reth \
    --docker.nocache hive/clients/reth > "$out/command.txt"
  printf '\n' >> "$out/command.txt"

  set +e
  ./hive \
    --sim "$simulator" \
    --sim.limit "$limit" \
    --sim.limit.exact=false \
    --sim.parallelism "$parallelism" \
    --sim.randomseed "$SEED" \
    --sim.loglevel 4 \
    --client reth \
    --docker.nocache hive/clients/reth \
    2>&1 | tee "$out/console.log"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$status" > "$out/exit-code.txt"
  docker image inspect "$simulator_image" --format '{{.Id}}' \
    > "$out/simulator-image-id.txt" 2> "$out/simulator-image-inspect-error.txt" || true

  if [[ -d "$HIVE_DIR/workspace/logs" ]]; then
    cp -a "$HIVE_DIR/workspace/logs" "$out/logs"
  else
    mkdir -p "$out/logs"
  fi
}

verify_mode() {
  local label="$1"
  local mode="$2"
  local logs="$RESULTS/$label/logs"
  grep -R -F -- 'Running reth with flags:' "$logs" \
    > "$RESULTS/$label/client-launch-commands.txt" || return 1
  if [[ "$mode" == baseline ]]; then
    if grep -R -F -- '--engine.num-state-masking-blocks' "$logs" > "$RESULTS/$label/unexpected-masking-flag.txt"; then
      echo "$label unexpectedly enabled partial persistence" >&2
      return 1
    fi
  else
    grep -R -F -- '--engine.persistence-threshold 2' "$logs" > "$RESULTS/$label/persistence-threshold.txt" || return 1
    grep -R -F -- '--engine.persistence-backpressure-threshold 4' "$logs" > "$RESULTS/$label/backpressure-threshold.txt" || return 1
    grep -R -F -- '--engine.memory-block-buffer-target 0' "$logs" > "$RESULTS/$label/memory-buffer-target.txt" || return 1
    grep -R -F -- '--engine.num-state-masking-blocks 1' "$logs" > "$RESULTS/$label/masking-blocks.txt" || return 1
  fi
}

normalize_one() {
  local label="$1"
  python3 "$NORMALIZER" "$RESULTS/$label/logs" > "$RESULTS/$label/results.tsv"
}

run_policy_parser() {
  local label="$1"
  local status=0
  local report
  while IFS= read -r -d '' report; do
    python3 "$RETH_REPO/.github/scripts/hive/parse.py" "$report" \
      --exclusion "$EXCLUSIONS" --ignored "$IGNORED" || status=1
  done < <(find "$RESULTS/$label/logs" -type f -name '*.json' ! -name hive.json -print0)
  return "$status"
}

apply_policy() {
  local label="$1"
  local status
  set +e
  run_policy_parser "$label" > "$RESULTS/$label/expected-results.txt" 2>&1
  status=$?
  set -e
  printf '%s\n' "$status" > "$RESULTS/$label/expected-exit-code.txt"
  python3 "$NORMALIZER" "$RESULTS/$label/logs" --policy \
    --exclusion "$EXCLUSIONS" --ignored "$IGNORED" \
    > "$RESULTS/$label/unexpected-results.tsv"
}

compare_policy_pair() {
  local baseline="$1"
  local partial="$2"
  local name="$3"
  local status=0

  apply_policy "$baseline"
  apply_policy "$partial"
  diff -u "$RESULTS/$baseline/unexpected-results.tsv" \
    "$RESULTS/$partial/unexpected-results.tsv" \
    > "$RESULTS/$name-expected.diff" || status=1
  if ! cmp -s "$RESULTS/$baseline/expected-exit-code.txt" \
    "$RESULTS/$partial/expected-exit-code.txt"; then
    echo "Expected-failure parser exit codes differ" >> "$RESULTS/$name-expected.diff"
    status=1
  fi
  return "$status"
}

compare_pair() {
  local baseline="$1"
  local partial="$2"
  local name="$3"
  local status=0

  normalize_one "$baseline"
  normalize_one "$partial"
  wc -l "$RESULTS/$baseline/results.tsv" "$RESULTS/$partial/results.tsv" > "$RESULTS/$name-counts.txt"
  diff -u "$RESULTS/$baseline/results.tsv" "$RESULTS/$partial/results.tsv" > "$RESULTS/$name.diff" || status=1
  if [[ ! -s "$RESULTS/$baseline/results.tsv" || ! -s "$RESULTS/$partial/results.tsv" ]]; then
    echo "One or both modes collected zero test cases" >> "$RESULTS/$name.diff"
    status=1
  fi
  if ! cmp -s "$RESULTS/$baseline/reth-image-id.txt" "$RESULTS/$partial/reth-image-id.txt"; then
    echo "Reth image IDs differ" >> "$RESULTS/$name.diff"
    status=1
  fi
  if [[ ! -s "$RESULTS/$baseline/simulator-image-id.txt" || \
        ! -s "$RESULTS/$partial/simulator-image-id.txt" ]]; then
    echo "Simulator image identity was unavailable in one or both modes" >> "$RESULTS/$name.diff"
    status=1
  elif ! cmp -s "$RESULTS/$baseline/simulator-image-id.txt" \
    "$RESULTS/$partial/simulator-image-id.txt"; then
    echo "Hive simulator image IDs differ" >> "$RESULTS/$name.diff"
    status=1
  fi
  if ! cmp -s "$RESULTS/$baseline/exit-code.txt" "$RESULTS/$partial/exit-code.txt"; then
    echo "Hive exit codes differ" >> "$RESULTS/$name.diff"
    status=1
  fi
  return "$status"
}

record_activation() {
  local label="$1"
  python3 "$ANALYZER" activation "$RESULTS/$label/logs" \
    > "$RESULTS/$label/persistence-activation.tsv"
}

compare_serious_errors() {
  local baseline="$1"
  local partial="$2"
  local name="$3"
  local status=0

  python3 "$ANALYZER" errors "$RESULTS/$baseline" \
    --raw-output "$RESULTS/$baseline/serious-errors-raw.tsv" \
    > "$RESULTS/$baseline/serious-errors.tsv"
  python3 "$ANALYZER" errors "$RESULTS/$partial" \
    --raw-output "$RESULTS/$partial/serious-errors-raw.tsv" \
    > "$RESULTS/$partial/serious-errors.tsv"
  diff -u "$RESULTS/$baseline/serious-errors.tsv" \
    "$RESULTS/$partial/serious-errors.tsv" \
    > "$RESULTS/$name-serious-errors.diff" || status=1
  if [[ "$status" -ne 0 ]]; then
    echo 'manual review required: baseline and partial serious-error signatures differ' \
      > "$RESULTS/$name-serious-errors-review.txt"
  fi
  return "$status"
}

investigate_differences() {
  local simulator="$1"
  local baseline="$2"
  local partial="$3"
  local name="$4"
  local filters="$RESULTS/$name-focused-filters.tsv"
  local differences_status

  set +e
  python3 "$ANALYZER" differences \
    "$RESULTS/$baseline/results.tsv" "$RESULTS/$partial/results.tsv" \
    > "$filters"
  differences_status=$?
  set -e

  if [[ "$differences_status" -eq 0 ]]; then
    echo 'no test-case differences' > "$RESULTS/$name-focused-reruns.txt"
    return 0
  elif [[ "$differences_status" -ne 1 ]]; then
    echo 'failed to derive focused Hive filters' > "$RESULTS/$name-focused-reruns.txt"
    return 1
  fi

  local focused_root="$RESULTS/focused-$name"
  mkdir -p "$focused_root"
  printf 'index\tsuite\ttest\tfilter\toriginal_baseline\toriginal_partial\treproduced_attempts\tclassification\n' \
    > "$focused_root/summary.tsv"

  local index suite_b64 test_b64 filter_b64 original_baseline original_partial
  while IFS=$'\t' read -r index suite_b64 test_b64 filter_b64 original_baseline original_partial; do
    local suite test filter padded case_dir reproduced=0 invalid=0
    suite="$(printf '%s' "$suite_b64" | base64 --decode)"
    test="$(printf '%s' "$test_b64" | base64 --decode)"
    filter="$(printf '%s' "$filter_b64" | base64 --decode)"
    printf -v padded '%03d' "$index"
    case_dir="$focused_root/$padded"
    mkdir -p "$case_dir"
    printf '%s\n' "$suite" > "$case_dir/suite.txt"
    printf '%s\n' "$test" > "$case_dir/test.txt"
    printf '%s\n' "$filter" > "$case_dir/filter.txt"

    local attempt
    for attempt in 1 2 3; do
      local partial_label="focused-$name-$padded-$attempt-partial"
      local baseline_label="focused-$name-$padded-$attempt-baseline"
      local comparison_name="focused-$name-$padded-$attempt"

      # Partial runs first to remove the original baseline-first ordering as a confounder.
      run_one partial "$simulator" "$filter" "$partial_label" 1
      run_one baseline "$simulator" "$filter" "$baseline_label" 1
      if ! verify_mode "$partial_label" partial; then
        invalid=1
      fi
      if ! verify_mode "$baseline_label" baseline; then
        invalid=1
      fi
      normalize_one "$partial_label"
      normalize_one "$baseline_label"

      local partial_cases baseline_cases
      partial_cases="$(wc -l < "$RESULTS/$partial_label/results.tsv")"
      baseline_cases="$(wc -l < "$RESULTS/$baseline_label/results.tsv")"
      if [[ "$partial_cases" -eq 0 || "$baseline_cases" -eq 0 ]]; then
        echo "attempt $attempt selected no test in one or both modes" \
          > "$case_dir/attempt-$attempt-invalid.txt"
        invalid=1
        continue
      fi

      if ! python3 "$ANALYZER" lookup "$RESULTS/$baseline_label/results.tsv" "$suite" "$test" \
        > "$case_dir/attempt-$attempt-baseline-result.txt"; then
        invalid=1
      fi
      if ! python3 "$ANALYZER" lookup "$RESULTS/$partial_label/results.tsv" "$suite" "$test" \
        > "$case_dir/attempt-$attempt-partial-result.txt"; then
        invalid=1
      fi

      local attempt_differences_status
      set +e
      python3 "$ANALYZER" differences \
        "$RESULTS/$baseline_label/results.tsv" "$RESULTS/$partial_label/results.tsv" \
        > "$case_dir/attempt-$attempt-differences.tsv"
      attempt_differences_status=$?
      set -e
      if [[ "$attempt_differences_status" -gt 1 ]]; then
        invalid=1
      elif [[ "$attempt_differences_status" -eq 1 ]] && \
        awk -F '\t' -v suite="$suite_b64" -v test="$test_b64" \
          '$2 == suite && $3 == test { found=1 } END { exit !found }' \
          "$case_dir/attempt-$attempt-differences.tsv"; then
        reproduced=$((reproduced + 1))
      fi
      compare_pair "$baseline_label" "$partial_label" "$comparison_name" || true
      compare_serious_errors "$baseline_label" "$partial_label" "$comparison_name" || true
    done

    local classification
    if [[ "$invalid" -ne 0 ]]; then
      classification='manual-input-required-filter-selected-no-test'
    elif [[ "$reproduced" -eq 3 ]]; then
      classification='consistent-differential'
    else
      classification='not-consistently-reproduced-possible-flake'
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$index" "$suite" "$test" "$filter" "$original_baseline" "$original_partial" \
      "$reproduced" "$classification" >> "$focused_root/summary.tsv"
  done < "$filters"

  # The original result difference remains a failed acceptance condition even
  # when focused reruns identify it as potentially flaky.
  return 1
}

comparison_status=0

run_one baseline ethereum/rpc-compat "$RPC_CI_FILTER" rpc-ci-baseline "$PARALLELISM"
run_one partial ethereum/rpc-compat "$RPC_CI_FILTER" rpc-ci-partial "$PARALLELISM"
# A slash with empty suite and test regexes matches every suite and every test
# in Hive's unmodified ethereum/rpc-compat simulator. This is intentionally
# broader than the method subset selected in Reth's standard hive.yml workflow.
run_one baseline ethereum/rpc-compat "$RPC_FULL_FILTER" rpc-full-baseline "$PARALLELISM"
run_one partial ethereum/rpc-compat "$RPC_FULL_FILTER" rpc-full-partial "$PARALLELISM"

for label in rpc-ci-baseline rpc-full-baseline; do
  verify_mode "$label" baseline || comparison_status=1
done
for label in rpc-ci-partial rpc-full-partial; do
  verify_mode "$label" partial || comparison_status=1
done

compare_pair rpc-ci-baseline rpc-ci-partial rpc-ci || comparison_status=1
compare_pair rpc-full-baseline rpc-full-partial rpc-full || comparison_status=1
compare_serious_errors rpc-ci-baseline rpc-ci-partial rpc-ci || comparison_status=1
compare_serious_errors rpc-full-baseline rpc-full-partial rpc-full || comparison_status=1

investigate_differences ethereum/rpc-compat rpc-ci-baseline rpc-ci-partial rpc-ci || comparison_status=1
investigate_differences ethereum/rpc-compat rpc-full-baseline rpc-full-partial rpc-full || comparison_status=1

compare_policy_pair rpc-ci-baseline rpc-ci-partial rpc-ci || comparison_status=1
# Full-suite absolute failures do not fail the experiment, but applying the same
# policy to both modes makes any expectation difference explicit in the artifact.
compare_policy_pair rpc-full-baseline rpc-full-partial rpc-full || comparison_status=1

activation_found=false
record_activation rpc-ci-partial && activation_found=true
record_activation rpc-full-partial && activation_found=true

if [[ "$activation_found" == false ]]; then
  engine_pairs=(
    'engine-api'
    'cancun'
    'engine-withdrawals'
  )
  for suite in "${engine_pairs[@]}"; do
    run_one baseline ethereum/engine "$suite" "$suite-baseline" "$PARALLELISM"
    run_one partial ethereum/engine "$suite" "$suite-partial" "$PARALLELISM"
    verify_mode "$suite-baseline" baseline || comparison_status=1
    verify_mode "$suite-partial" partial || comparison_status=1
    compare_pair "$suite-baseline" "$suite-partial" "$suite" || comparison_status=1
    compare_policy_pair "$suite-baseline" "$suite-partial" "$suite" || comparison_status=1
    compare_serious_errors "$suite-baseline" "$suite-partial" "$suite" || comparison_status=1
    investigate_differences ethereum/engine "$suite-baseline" "$suite-partial" "$suite" || comparison_status=1
    record_activation "$suite-partial" && activation_found=true
  done
fi

if [[ "$activation_found" == true ]]; then
  echo activated > "$RESULTS/persistence-coverage.txt"
else
  echo 'coverage unresolved: no standard RPC or Engine run logged a persistence cycle' \
    > "$RESULTS/persistence-coverage.txt"
  comparison_status=1
fi

{
  echo '| Suite | Baseline cases | Partial cases | Baseline unexpected failures | Partial unexpected failures | Differential |'
  echo '| --- | ---: | ---: | ---: | ---: | --- |'
  for pair in 'RPC CI subset:rpc-ci' 'Full rpc-compat:rpc-full' \
    'engine-api:engine-api' 'engine-cancun:cancun' 'engine-withdrawals:engine-withdrawals'; do
    title="${pair%%:*}"
    key="${pair#*:}"
    if [[ ! -f "$RESULTS/$key-baseline/results.tsv" ]]; then
      continue
    fi
    baseline_cases="$(wc -l < "$RESULTS/$key-baseline/results.tsv")"
    partial_cases="$(wc -l < "$RESULTS/$key-partial/results.tsv")"
    if [[ -f "$RESULTS/$key-baseline/unexpected-results.tsv" ]]; then
      baseline_unexpected="$(grep -c '^unexpected-failure' "$RESULTS/$key-baseline/unexpected-results.tsv" || true)"
      partial_unexpected="$(grep -c '^unexpected-failure' "$RESULTS/$key-partial/unexpected-results.tsv" || true)"
    else
      baseline_unexpected='n/a'
      partial_unexpected='n/a'
    fi
    if [[ -s "$RESULTS/$key.diff" || -s "$RESULTS/$key-serious-errors.diff" || \
          -s "$RESULTS/$key-expected.diff" ]]; then
      differential=changed
    else
      differential=clean
    fi
    echo "| $title | $baseline_cases | $partial_cases | $baseline_unexpected | $partial_unexpected | $differential |"
  done
} > "$RESULTS/summary.md"

exit "$comparison_status"
