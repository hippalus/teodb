#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# run-perf-suite.sh — Run the full performance benchmark suite.
#
# Optionally starts TeoDB, loads TPC-H data, runs REST + Flight SQL
# benchmarks, and generates reports.
#
# Usage:
#   ./scripts/run-perf-suite.sh                      # full run (start server, load, bench)
#   ./scripts/run-perf-suite.sh --skip-server         # assume TeoDB is already running
#   ./scripts/run-perf-suite.sh --skip-load           # assume data is already loaded
#   ./scripts/run-perf-suite.sh --only rest           # REST benchmarks only
#   ./scripts/run-perf-suite.sh --only flight         # Flight SQL benchmarks only
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
TEODB_BIN="./target/release/teodb"
PERF=(./target/release/teodb-perf-suite)
DS="crates/teodb-perf-suite/datasets"
SC="crates/teodb-perf-suite/scenarios"

SKIP_SERVER=false
SKIP_LOAD=false
DATA_DIR="./data/perf-suite"
RESULTS_DIR="./perf-results"

# ── Helpers ──────────────────────────────────────────────────────────

log()  { printf "\033[1;34m▸ %s\033[0m\n" "$*"; }
ok()   { printf "\033[1;32m  ✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[1;31m  ✗ %s\033[0m\n" "$*"; }
hr()   { printf "\033[90m%.0s─\033[0m" {1..60}; echo; }

toml_string() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf "%s" "$value"
}

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

show_help() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run the TeoDB performance benchmark suite.

Options:
    --skip-server           Don't start TeoDB (assumes it's already running)
    --skip-load             Skip data loading (assumes data is already loaded)
    --only TARGET           Run only specific benchmarks: rest, flight, all (default: all)
    --http URL              TeoDB HTTP address (default: $HTTP_ADDR)
    --flight URL            TeoDB Flight address (default: $FLIGHT_ADDR)
    --data-dir DIR          Data directory for auto-started server (default: $DATA_DIR)
    --results-dir DIR       Output directory for results (default: $RESULTS_DIR)
    -h, --help              Show this help
EOF
    exit 0
}

# ── Parse args ───────────────────────────────────────────────────────

SELECTED=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-server)  SKIP_SERVER=true; shift ;;
        --skip-load)    SKIP_LOAD=true; shift ;;
        --only|-o)      IFS=',' read -ra SELECTED <<< "$2"; shift 2 ;;
        --http)         HTTP_ADDR="$2"; shift 2 ;;
        --flight)       FLIGHT_ADDR="$2"; shift 2 ;;
        --data-dir)     DATA_DIR="$2"; shift 2 ;;
        --results-dir)  RESULTS_DIR="$2"; shift 2 ;;
        -h|--help)      show_help ;;
        *)              echo "Unknown option: $1"; show_help ;;
    esac
done

TARGETS=(rest flight)
if [[ ${#SELECTED[@]} -eq 0 ]]; then
    SELECTED=("${TARGETS[@]}")
fi

want() { for t in "${SELECTED[@]}"; do [[ "$t" == "$1" || "$t" == "all" ]] && return 0; done; return 1; }

mkdir -p "$RESULTS_DIR"
mkdir -p "$DATA_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SERVER_CONFIG="$RESULTS_DIR/teodb-perf.$TIMESTAMP.toml"
CATALOG_URI="${TEODB_CATALOG_URI:-http://localhost:8181}"
WAREHOUSE_URI="${TEODB_WAREHOUSE:-s3://teodb}"

# ── Preflight ────────────────────────────────────────────────────────

hr
log "TeoDB Performance Suite"
hr

# ── Build ────────────────────────────────────────────────────────────

log "Building (release)..."
cargo build --release -p teodb-perf-suite -p teodb-server
ok "Build complete"

# ── Start server (optional) ──────────────────────────────────────────

TEODB_PID=""

cleanup() {
    if [[ -n "$TEODB_PID" ]]; then
        log "Stopping TeoDB (PID $TEODB_PID)..."
        kill "$TEODB_PID" 2>/dev/null || true
        wait "$TEODB_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [[ "$SKIP_SERVER" == "false" ]]; then
    log "Starting TeoDB server..."

    BIND_ADDR="${HTTP_ADDR#http://}"
    BIND_ADDR="${BIND_ADDR#https://}"
    FLIGHT_BIND="${FLIGHT_ADDR#http://}"
    FLIGHT_BIND="${FLIGHT_BIND#https://}"

    cat > "$SERVER_CONFIG" <<EOF
role = "standalone"
data_dir = "$(toml_string "$DATA_DIR")"

[server]
rest_bind = "$(toml_string "$BIND_ADDR")"
flight_bind = "$(toml_string "$FLIGHT_BIND")"

[catalog]
type = "rest"
uri = "$(toml_string "$CATALOG_URI")"
warehouse = "$(toml_string "$WAREHOUSE_URI")"

[storage]
cache_dir = "$(toml_string "$DATA_DIR/cache")"
spill_dir = "$(toml_string "$DATA_DIR/spill")"
s3_allow_http = true

[security]
mode = "plaintext"
EOF

    "$TEODB_BIN" --config "$SERVER_CONFIG" &
    TEODB_PID=$!

    for i in $(seq 1 30); do
        if curl -sf "$HTTP_ADDR/ready" > /dev/null 2>&1; then
            ok "TeoDB is ready (PID $TEODB_PID)"
            break
        fi
        if [[ $i -eq 30 ]]; then
            fail "TeoDB failed to start within 30 seconds"
            exit 1
        fi
        sleep 1
    done
else
    log "Skipping server start (--skip-server)"
    if ! curl -sf "$HTTP_ADDR/live" > /dev/null 2>&1; then
        fail "TeoDB is not reachable at $HTTP_ADDR"
        exit 1
    fi
    ok "TeoDB is reachable at $HTTP_ADDR"
fi

hr

# ── Load data ────────────────────────────────────────────────────────

if [[ "$SKIP_LOAD" == "false" ]]; then
    run "Prepare TPC-H data" \
        "${PERF[@]}" prepare-data --http "$HTTP_ADDR" --dataset "$DS/tpch.toml"

    run "Load TPC-H tables" \
        "${PERF[@]}" load --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --dataset "$DS/tpch.toml"
    hr
else
    log "Skipping data load (--skip-load)"
    hr
fi

# ── TPC-H benchmarks (full sql/tpch suite over REST + Flight) ────────
#
# The tpch scenario declares transport = "both"; --only rest|flight narrows
# it to a single protocol via the --transport override.

TPCH_TRANSPORT="both"
if want rest && ! want flight; then TPCH_TRANSPORT="rest"; fi
if want flight && ! want rest; then TPCH_TRANSPORT="flight"; fi

if want rest || want flight; then
    log "TPC-H query benchmarks (transport: $TPCH_TRANSPORT)"
    echo ""

    run "TPC-H queries" \
        "${PERF[@]}" run-suite --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --transport "$TPCH_TRANSPORT" --bench "$SC/tpch.toml" --output "$RESULTS_DIR/tpch.$TIMESTAMP.json"

    hr
fi

# ── Summary ──────────────────────────────────────────────────────────

TOTAL=$((PASS + FAIL))
echo ""
if [[ $FAIL -eq 0 ]]; then
    printf "\033[1;32m✓ All %d steps passed.\033[0m\n" "$TOTAL"
    echo ""
    log "Results saved to: $RESULTS_DIR"
else
    printf "\033[1;31m%d/%d steps failed.\033[0m\n" "$FAIL" "$TOTAL"
    exit 1
fi
