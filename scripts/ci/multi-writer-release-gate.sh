#!/usr/bin/env bash
set -euo pipefail

inventory="$(mktemp)"
trap 'rm -f "${inventory}"' EXIT

cargo test --workspace --all-targets --locked -- --list >"${inventory}"
for index in $(seq 1 18); do
  if ! grep -Eq "(^|::)mw_t${index}_" "${inventory}"; then
    echo "missing stable multi-writer test prefix: mw_t${index}_" >&2
    exit 1
  fi
done

cargo test --workspace --all-targets --locked mw_t -- --nocapture
cargo test -p teodb-server --test multi_writer_rest --locked \
  pinned_compose_harness_is_checked_in
cargo test -p teodb-server --test multi_writer_rest --locked -- \
  --ignored --nocapture --test-threads=1
cargo test -p teodb-server --test object_storage_tier --locked \
  drop_purge_reclaims_only_the_dropped_table_prefix -- \
  --ignored --nocapture --test-threads=1
