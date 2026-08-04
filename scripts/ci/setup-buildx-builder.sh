#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 || -z "$1" ]]; then
  echo "usage: $0 <buildkit-image>" >&2
  exit 2
fi

buildkit_image="$1"
builder_name="${BUILDX_BUILDER_NAME:-teodb-${GITHUB_RUN_ID:-local}-${GITHUB_JOB:-job}-${GITHUB_RUN_ATTEMPT:-1}}"
max_attempts="${BUILDX_BOOTSTRAP_MAX_ATTEMPTS:-4}"
attempt_timeout_seconds="${BUILDX_BOOTSTRAP_TIMEOUT_SECONDS:-90}"
retry_delay_seconds="${BUILDX_BOOTSTRAP_RETRY_DELAY_SECONDS:-5}"

require_positive_integer() {
  local name="$1"
  local value="$2"

  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer, got: ${value}" >&2
    exit 2
  fi
}

require_positive_integer BUILDX_BOOTSTRAP_MAX_ATTEMPTS "${max_attempts}"
require_positive_integer BUILDX_BOOTSTRAP_TIMEOUT_SECONDS "${attempt_timeout_seconds}"
require_positive_integer BUILDX_BOOTSTRAP_RETRY_DELAY_SECONDS "${retry_delay_seconds}"

if [[ ! "${builder_name}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "invalid Buildx builder name: ${builder_name}" >&2
  exit 2
fi

docker buildx create \
  --name "${builder_name}" \
  --driver docker-container \
  --driver-opt "image=${buildkit_image}" \
  --use >/dev/null

bootstrap_builder() {
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=10s "${attempt_timeout_seconds}s" \
      docker buildx inspect --bootstrap "${builder_name}"
  else
    docker buildx inspect --bootstrap "${builder_name}"
  fi
}

for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  echo "Bootstrapping Buildx builder ${builder_name} (attempt ${attempt}/${max_attempts})"

  if bootstrap_builder; then
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
      printf 'name=%s\n' "${builder_name}" >>"${GITHUB_OUTPUT}"
    fi
    echo "Buildx builder ${builder_name} is ready"
    exit 0
  fi

  if ((attempt == max_attempts)); then
    echo "Failed to bootstrap Buildx builder ${builder_name} after ${max_attempts} attempts" >&2
    exit 1
  fi

  delay=$((retry_delay_seconds * attempt))
  echo "Buildx bootstrap failed; retrying in ${delay}s" >&2
  sleep "${delay}"
done
