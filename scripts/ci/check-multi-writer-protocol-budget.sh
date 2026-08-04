#!/usr/bin/env bash
set -euo pipefail

actual="${1:-target/multi-writer-protocol.json}"
baseline="${2:-crates/teodb-catalog/benches/baselines/multi_writer_protocol.json}"
timing_mode="${TEODB_PROTOCOL_TIMING_MODE:-auto}"

case "${timing_mode}" in
  auto|required|off) ;;
  *)
    echo "invalid TEODB_PROTOCOL_TIMING_MODE=${timing_mode}; expected auto, required, or off" >&2
    exit 1
    ;;
esac

for file in "${actual}" "${baseline}"; do
  jq -e '.schema_version == 1' "${file}" >/dev/null
done

jq -e '
  (.provenance.source_revision | test("^[0-9a-f]{40}$")) and
  (.provenance.source_branch == "main") and
  (.provenance.run_count >= 5) and
  (.provenance.run_count == (.provenance.runs | length)) and
  (.provenance.waves_per_scenario > 0) and
  (.provenance.capture_command | length > 0) and
  (.provenance.runner.os | length > 0) and
  (.provenance.runner.arch | length > 0) and
  (.provenance.runner.rustc | length > 0) and
  ([.provenance.runs[].structural |
    (.same_table_16_successful_commits > 0 and
     .same_table_16_manifest_files == .same_table_16_successful_commits)] | all)
' "${baseline}" >/dev/null

baseline_waves="$(jq -er '.provenance.waves_per_scenario' "${baseline}")"
actual_waves="$(jq -er '.measurement.waves_per_scenario' "${actual}")"
if [[ "${actual_waves}" -ne "${baseline_waves}" ]]; then
  echo "protocol measurement shape changed: ${actual_waves} waves (baseline ${baseline_waves})" >&2
  exit 1
fi

check_max() {
  local key="$1"
  local actual_value
  local maximum
  actual_value="$(jq -er "${key}" "${actual}")"
  maximum="$(jq -er "${key}" "${baseline}")"
  if ! awk -v value="${actual_value}" -v limit="${maximum}" 'BEGIN { exit !(value <= limit) }'; then
    echo "protocol budget exceeded for ${key}: ${actual_value} > ${maximum}" >&2
    exit 1
  fi
}

check_ratio() {
  local key="$1"
  local actual_value
  local baseline_value
  local tolerance
  actual_value="$(jq -er ".ratios.${key}" "${actual}")"
  baseline_value="$(jq -er ".ratios.${key}" "${baseline}")"
  tolerance="$(jq -er '.ratio_regression_tolerance' "${baseline}")"
  if ! awk \
    -v value="${actual_value}" \
    -v base="${baseline_value}" \
    -v tolerance="${tolerance}" \
    'BEGIN { exit !(value <= base * (1 + tolerance)) }'
  then
    echo "protocol ratio regressed for ${key}: ${actual_value} (baseline ${baseline_value}, tolerance ${tolerance})" >&2
    exit 1
  fi
}

validate_ratio() {
  local key="$1"
  if ! jq -e ".ratios.${key} | type == \"number\" and . > 0" "${actual}" >/dev/null; then
    echo "protocol measurement has an invalid ratio for ${key}" >&2
    exit 1
  fi
}

metadata="$(jq -er '.structural.metadata_bytes_32_writers' "${actual}")"
payload="$(jq -er '.structural.commit_payload_bytes_32_writers' "${actual}")"
rebases="$(jq -er '.structural.same_table_16_rebases' "${actual}")"
conflicts="$(jq -er '.structural.same_table_16_conflicts' "${actual}")"
successes="$(jq -er '.structural.same_table_16_successful_commits' "${actual}")"
manifests="$(jq -er '.structural.same_table_16_manifest_files' "${actual}")"

for pair in \
  "metadata_bytes_32_writers:${metadata}" \
  "commit_payload_bytes_32_writers:${payload}" \
  "same_table_16_rebases:${rebases}" \
  "same_table_16_conflicts:${conflicts}"
do
  name="${pair%%:*}"
  value="${pair#*:}"
  maximum="$(jq -er ".maximums.${name}" "${baseline}")"
  if ! awk -v value="${value}" -v limit="${maximum}" 'BEGIN { exit !(value <= limit) }'; then
    echo "protocol structural cap exceeded for ${name}: ${value} > ${maximum}" >&2
    exit 1
  fi
done

if [[ "${successes}" -le 0 || "${manifests}" -ne "${successes}" ]]; then
  echo "manifest growth must equal successful commits: commits=${successes}, manifests=${manifests}" >&2
  exit 1
fi

validate_ratio same_table_16_to_2_latency
validate_ratio same_to_different_table_16_latency

baseline_os="$(jq -er '.provenance.runner.os' "${baseline}")"
baseline_arch="$(jq -er '.provenance.runner.arch' "${baseline}")"
baseline_rustc="$(jq -er '.provenance.runner.rustc' "${baseline}")"
actual_os="$(uname -s) $(uname -r)"
actual_arch="$(uname -m)"
actual_rustc="$(rustc --version)"
actual_rustc="${actual_rustc#rustc }"

runner_matches_baseline=false
if [[ "${actual_os}" == "${baseline_os}" && \
      "${actual_arch}" == "${baseline_arch}" && \
      "${actual_rustc}" == "${baseline_rustc}" ]]; then
  runner_matches_baseline=true
fi

if [[ "${timing_mode}" == "required" && "${runner_matches_baseline}" != "true" ]]; then
  echo "protocol timing runner does not match the reviewed baseline" >&2
  echo "actual: ${actual_os}; ${actual_arch}; ${actual_rustc}" >&2
  echo "baseline: ${baseline_os}; ${baseline_arch}; ${baseline_rustc}" >&2
  exit 1
fi

if [[ "${timing_mode}" != "off" && "${runner_matches_baseline}" == "true" ]]; then
  check_ratio same_table_16_to_2_latency
  check_ratio same_to_different_table_16_latency
  echo "multi-writer protocol timing ratios are within limits"
else
  echo "multi-writer protocol timing ratios recorded but not compared across runner profiles"
  echo "actual runner: ${actual_os}; ${actual_arch}; ${actual_rustc}"
  echo "baseline runner: ${baseline_os}; ${baseline_arch}; ${baseline_rustc}"
fi

# Provenance is executable evidence, not prose: every checked-in raw run must
# satisfy the same structural and ratio limits enforced for the current run.
jq -e '
  . as $baseline |
  [.provenance.runs[] |
    (.structural.metadata_bytes_32_writers <= $baseline.maximums.metadata_bytes_32_writers) and
    (.structural.commit_payload_bytes_32_writers <= $baseline.maximums.commit_payload_bytes_32_writers) and
    (.structural.same_table_16_rebases <= $baseline.maximums.same_table_16_rebases) and
    (.structural.same_table_16_conflicts <= $baseline.maximums.same_table_16_conflicts) and
    (.structural.same_table_16_successful_commits > 0) and
    (.structural.same_table_16_manifest_files == .structural.same_table_16_successful_commits) and
    (.ratios.same_table_16_to_2_latency <=
      $baseline.ratios.same_table_16_to_2_latency * (1 + $baseline.ratio_regression_tolerance)) and
    (.ratios.same_to_different_table_16_latency <=
      $baseline.ratios.same_to_different_table_16_latency * (1 + $baseline.ratio_regression_tolerance))
  ] | all
' "${baseline}" >/dev/null
echo "multi-writer protocol structural budget is within limits"
