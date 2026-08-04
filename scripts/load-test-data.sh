#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# load-test-data.sh — Ingest datasets into a running TeoDB for local
# development, manual testing, and UI exploration.
#
# All data is ingested through TeoDB APIs (HTTP JSON ingest + flush).
#
# Usage:
#   ./scripts/load-test-data.sh                  # load all datasets
#   ./scripts/load-test-data.sh --only tpch      # TPC-H (8 tables, ~86K rows)
#   ./scripts/load-test-data.sh --only smoke     # smoke queries
#   ./scripts/load-test-data.sh --only flight    # run Flight SQL benchmarks
#   ./scripts/load-test-data.sh --list           # show available targets
#
# Prerequisites:
#   TeoDB must be running:
#     cargo run -p teodb-server --bin teodb -- --config config/dev.toml
#
# Environment:
#   TEODB_HTTP     override HTTP address   (default: http://127.0.0.1:8080)
#   TEODB_FLIGHT   override Flight address (default: http://127.0.0.1:8815)
# ──────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

HTTP_ADDR="${TEODB_HTTP:-http://127.0.0.1:8080}"
FLIGHT_ADDR="${TEODB_FLIGHT:-http://127.0.0.1:8815}"
PERF=(cargo run -p teodb-perf-suite --)
DS="crates/teodb-perf-suite/datasets"
SC="crates/teodb-perf-suite/scenarios"

# ── Helpers ──────────────────────────────────────────────────────────

log()  { printf "\033[1;34m▸ %s\033[0m\n" "$*"; }
ok()   { printf "\033[1;32m  ✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[1;31m  ✗ %s\033[0m\n" "$*"; }
hr()   { printf "\033[90m%.0s─\033[0m" {1..60}; echo; }

PASS=0; FAIL=0

run() {
    local label="$1"; shift
    log "$label"
    if "$@" 2>&1; then
        ok "$label"; PASS=$((PASS + 1))
    else
        fail "$label"; FAIL=$((FAIL + 1))
    fi
}

validate_query() {
    local table="$1" label="$2"
    local result
    result=$(curl -s -X POST "$HTTP_ADDR/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"SELECT COUNT(*) AS cnt FROM tpch.$table\", \"limit\": 1}" 2>/dev/null) || true
    if [[ -n "$result" ]]; then
        ok "  $label"
    fi
}

check_server() {
    if ! curl -sf "$HTTP_ADDR/live" >/dev/null 2>&1; then
        echo "ERROR: TeoDB not reachable at $HTTP_ADDR" >&2
        echo "" >&2
        echo "Start it first:" >&2
        echo "  cargo run -p teodb-server --bin teodb -- --config config/dev.toml" >&2
        exit 1
    fi
    ok "TeoDB is running at $HTTP_ADDR"
}

show_list() {
    cat <<'EOF'
Available targets:

  tpch      Ingest 8 TPC-H tables via managed pipeline (CREATE → JSON ingest → flush)
  nested    Generate & load 1M nested JSON events with partitioning (perf.events)
  smoke     Run smoke benchmark scenario (basic queries)
  flight    Run Flight SQL benchmarks (TPC-H analytical queries)
  partition Run partition-pruning benchmarks on nested events (REST + Flight)
  taxi      Download real TLC green-taxi parquet, load via normal JSON ingest, query (REST + Flight)

  all       Run all of the above (default)

All data is ingested through TeoDB HTTP or Flight SQL APIs — no external file attach.

Examples:
  ./scripts/load-test-data.sh                          # everything
  ./scripts/load-test-data.sh --only tpch              # just TPC-H
  ./scripts/load-test-data.sh --only nested            # 1M nested JSON + partitioning
  ./scripts/load-test-data.sh --only nested,partition  # load + benchmark partitioning
  ./scripts/load-test-data.sh --only flight            # Flight SQL benchmarks only
  TEODB_HTTP=http://127.0.0.1:8080 TEODB_FLIGHT=http://127.0.0.1:8815 ./scripts/load-test-data.sh --only tpch
EOF
    exit 0
}

# ── Parse args ───────────────────────────────────────────────────────

SELECTED=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --list|-l)     show_list ;;
        --only|-o)     IFS=',' read -ra SELECTED <<< "$2"; shift 2 ;;
        --http)        HTTP_ADDR="$2"; shift 2 ;;
        --flight)      FLIGHT_ADDR="$2"; shift 2 ;;
        -h|--help)     show_list ;;
        *)             echo "Unknown option: $1"; show_list ;;
    esac
done

TARGETS=(tpch nested smoke flight partition taxi)
if [[ ${#SELECTED[@]} -eq 0 ]]; then
    SELECTED=("${TARGETS[@]}")
fi

want() { for t in "${SELECTED[@]}"; do [[ "$t" == "$1" ]] && return 0; done; return 1; }

TPCH_LOADED=false

# ── Preflight ────────────────────────────────────────────────────────

hr
log "TeoDB Local Test Data Loader"
check_server
hr

# ── TPC-H via Managed Ingest ────────────────────────────────────────

if want tpch; then
    log "TPC-H — Ingest 8 tables via managed pipeline (CREATE → JSON ingest → flush)"
    echo ""

    run "Generate TPC-H data" \
        "${PERF[@]}" prepare-data --http "$HTTP_ADDR" --dataset "$DS/tpch.toml"

    run "Ingest TPC-H tables (DDL → JSON ingest → flush)" \
        "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/tpch.toml"

    log "Validating tables..."
    for t in region nation supplier customer part partsupp orders lineitem; do
        validate_query "$t" "$t"
    done
    TPCH_LOADED=true

    echo ""
    printf "\033[1;33m  ➜ Tables ready: region, nation, supplier, customer, part, partsupp, orders, lineitem\033[0m\n"
    printf "\033[1;33m  ➜ Try: SELECT l_returnflag, COUNT(*) FROM tpch.lineitem GROUP BY l_returnflag\033[0m\n"
    hr
fi

# ── Nested JSON Events (1M rows, partitioned by region) ──────────────

NESTED_LOADED=false

if want nested; then
    log "Nested JSON — Generate & load 1M events partitioned by region"
    echo ""

    run "Generate nested JSON events" \
        "${PERF[@]}" prepare-data --http "$HTTP_ADDR" --dataset "$DS/nested_events.toml"

    run "Ingest nested events (DDL → JSON ingest → flush)" \
        "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/nested_events.toml"

    NESTED_LOADED=true

    echo ""
    printf "\033[1;33m  ➜ Table ready: perf.events (1M rows, PARTITIONED BY region)\033[0m\n"
    printf "\033[1;33m  ➜ Try: SELECT region, COUNT(*) FROM perf.events GROUP BY region\033[0m\n"
    hr
fi

# ── Partition Performance Benchmarks ─────────────────────────────────

if want partition; then
    log "Partition — Run partition-pruning benchmarks on nested events"
    echo ""

    if [[ "$NESTED_LOADED" != "true" ]]; then
        run "Ensure nested events are loaded" \
            "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/nested_events.toml"
    fi

    run "Run partition-pruning benchmarks" \
        "${PERF[@]}" run-suite --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --bench "$SC/partition_perf.toml"

    hr
fi

# ── NYC Taxi (real TLC green-taxi parquet, attached server-side) ─────

if want taxi; then
    log "NYC Taxi — Download real TLC green-taxi parquet & load via normal JSON ingest"
    echo ""

    run "Download TLC green-taxi parquet" \
        "${PERF[@]}" prepare-data --http "$HTTP_ADDR" --dataset "$DS/nyc_taxi.toml"

    run "Load taxi data (CREATE → read parquet → JSON ingest → flush)" \
        "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/nyc_taxi.toml"

    run "Run taxi analytics (REST + Flight)" \
        "${PERF[@]}" run-suite --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --bench "$SC/taxi_analytics.toml"

    echo ""
    printf "\033[1;33m  ➜ Table ready: nyc_taxi (real TLC green-taxi trips)\033[0m\n"
    printf "\033[1;33m  ➜ Try: SELECT passenger_count, COUNT(*) FROM default.nyc_taxi GROUP BY passenger_count\033[0m\n"
    hr
fi

# ── Smoke Test (REST queries) ────────────────────────────────────────

if want smoke; then
    log "Smoke — Run baseline REST query benchmarks"
    echo ""

    # Smoke queries need TPC-H data
    if [[ "$TPCH_LOADED" != "true" ]]; then
        run "Ensure TPC-H data is loaded" \
            "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/tpch.toml"
    fi

    run "Run smoke queries" \
        "${PERF[@]}" run-suite --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --bench "$SC/smoke.toml"

    hr
fi

# ── Flight SQL Benchmarks ────────────────────────────────────────────

if want flight; then
    log "Flight SQL — Analytical benchmarks over TPC-H"
    echo ""

    # Flight benchmarks need TPC-H data
    if [[ "$TPCH_LOADED" != "true" ]]; then
        run "Ensure TPC-H data is loaded" \
            "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/tpch.toml"
    fi

    run "Flight SQL TPC-H analytical queries" \
        "${PERF[@]}" run-flight-bench --flight "$FLIGHT_ADDR" --bench "$SC/flight_tpch.toml"

    hr
fi

# ── Summary ──────────────────────────────────────────────────────────

TOTAL=$((PASS + FAIL))
echo ""
if [[ $FAIL -eq 0 ]]; then
    printf "\033[1;32m✓ All %d steps passed.\033[0m\n" "$TOTAL"
    echo ""
    log "TeoDB is loaded — open ${HTTP_ADDR} to explore"
else
    printf "\033[1;31m%d/%d steps failed.\033[0m\n" "$FAIL" "$TOTAL"
    exit 1
fi
