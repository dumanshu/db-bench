#!/usr/bin/env bash
# calibrate.sh -- Run calibration sweep for a given Aurora instance type.
# Prepares fresh data, runs 60s benchmarks at various thread counts,
# captures IUD rates, then cleans up.
#
# Usage:
#   ./calibrate.sh <instance_label>
#   ./calibrate.sh r6i.16xlarge
#
# Requires: aurora-bench stack provisioned with --seed auroralt-bench01

set -euo pipefail

SEED="auroralt-bench01"
LABEL="${1:?Usage: $0 <instance_label>}"
DURATION=60
TABLES=64
TABLE_SIZE=1000000
PROFILE="sandbox"
RDS_PROFILE="sandbox-storage"
THREAD_COUNTS=(8 16 32 64 128 256 512 1024)
RESULTS_DIR="$(dirname "$0")/calibration"

mkdir -p "$RESULTS_DIR"

echo "============================================"
echo "Calibration sweep: $LABEL"
echo "Threads: ${THREAD_COUNTS[*]}"
echo "Duration: ${DURATION}s per run"
echo "Tables: $TABLES x $TABLE_SIZE rows"
echo "============================================"
echo ""

# First run: prepare data, skip cleanup
echo "[$(date -u +%H:%M:%S)] Running threads=${THREAD_COUNTS[0]} (with prepare)..."
python3 "$(dirname "$0")/benchmark.py" \
    --seed "$SEED" \
    --threads "${THREAD_COUNTS[0]}" \
    --duration "$DURATION" \
    --tables "$TABLES" \
    --table-size "$TABLE_SIZE" \
    --report-interval 10 \
    --aws-profile "$PROFILE" \
    --rds-profile "$RDS_PROFILE" \
    --skip-cleanup \
    2>&1 | tee "$RESULTS_DIR/cal_${LABEL}_t${THREAD_COUNTS[0]}.log"
echo ""

# Middle runs: skip prepare and cleanup
for i in $(seq 1 $((${#THREAD_COUNTS[@]} - 2))); do
    t="${THREAD_COUNTS[$i]}"
    echo "[$(date -u +%H:%M:%S)] Running threads=${t} (skip prepare)..."
    python3 "$(dirname "$0")/benchmark.py" \
        --seed "$SEED" \
        --threads "$t" \
        --duration "$DURATION" \
        --tables "$TABLES" \
        --table-size "$TABLE_SIZE" \
        --report-interval 10 \
        --aws-profile "$PROFILE" \
        --rds-profile "$RDS_PROFILE" \
        --skip-prepare \
        --skip-cleanup \
        2>&1 | tee "$RESULTS_DIR/cal_${LABEL}_t${t}.log"
    echo ""
done

# Last run: skip prepare, do cleanup
LAST_T="${THREAD_COUNTS[$((${#THREAD_COUNTS[@]} - 1))]}"
echo "[$(date -u +%H:%M:%S)] Running threads=${LAST_T} (with cleanup)..."
python3 "$(dirname "$0")/benchmark.py" \
    --seed "$SEED" \
    --threads "$LAST_T" \
    --duration "$DURATION" \
    --tables "$TABLES" \
    --table-size "$TABLE_SIZE" \
    --report-interval 10 \
    --aws-profile "$PROFILE" \
    --rds-profile "$RDS_PROFILE" \
    --skip-prepare \
    2>&1 | tee "$RESULTS_DIR/cal_${LABEL}_t${LAST_T}.log"

echo ""
echo "============================================"
echo "Calibration complete for $LABEL"
echo "Logs in: $RESULTS_DIR/cal_${LABEL}_t*.log"
echo "============================================"
