#!/usr/bin/env bash
# run_final.sh -- Run final benchmarks with windowed metric capture.
#
# For each thread count: prepare fresh data -> start metrics sampler ->
# run sysbench 120s with --report-interval=5 -> stop sampler ->
# parse [30s-90s] window -> query CloudWatch -> save JSON -> cleanup data.
#
# Usage: ./run_final.sh <instance_label> <thread1> [thread2] ...
#   e.g.: ./run_final.sh db.r6i.16xlarge 44 104 176 248

set -euo pipefail

SEED="auroralt-bench01"
DURATION=120
WINDOW_START=30
WINDOW_END=90
SAMPLE_INTERVAL=5
TABLES=64
TABLE_SIZE=1000000
DB="sbtest"
REPORT_INTERVAL=5
PROFILE="sandbox"
RDS_PROFILE="sandbox-storage"
REGION="us-east-1"
PASSWORD='BenchMark2024!'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_FILE="$SCRIPT_DIR/aurora-bench-state.json"
RESULTS_DIR="$SCRIPT_DIR/results"
LOGS_DIR="$SCRIPT_DIR/final_logs"

ENDPOINT=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['endpoint'])")
EC2_IP=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['client_public_ip'])")
INSTANCE_TYPE=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['aurora_instance_type'])")
EC2_TYPE=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['ec2_instance_type'])")
WRITER_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['writer_id'])")
KEY="$SCRIPT_DIR/aurora-bench-key.pem"

LABEL="${1:?Usage: $0 <instance_label> <thread1> [thread2] ...}"
shift
THREAD_COUNTS=("$@")

if [ ${#THREAD_COUNTS[@]} -eq 0 ]; then
    echo "ERROR: No thread counts specified"
    exit 1
fi

mkdir -p "$RESULTS_DIR" "$LOGS_DIR"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
SSH_CMD="ssh $SSH_OPTS -i $KEY ec2-user@$EC2_IP"
SCP_CMD="scp $SSH_OPTS -i $KEY"

ts() { date -u +%H:%M:%S; }

echo "============================================"
echo "Final Benchmark: $LABEL ($INSTANCE_TYPE)"
echo "Threads: ${THREAD_COUNTS[*]}"
echo "Duration: ${DURATION}s (window: ${WINDOW_START}-${WINDOW_END}s @ ${SAMPLE_INTERVAL}s)"
echo "Tables: $TABLES x $TABLE_SIZE rows"
echo "Endpoint: $ENDPOINT"
echo "Client: $EC2_IP ($EC2_TYPE)"
echo "============================================"
echo ""

echo "[$(ts)] Setting up /etc/hosts on EC2..."
$SSH_CMD bash -c "'
    sudo sed -i \"/${ENDPOINT}/d\" /etc/hosts
    IP=\$(dig +short ${ENDPOINT} | tail -1)
    if [ -n \"\$IP\" ]; then
        echo \"\$IP ${ENDPOINT}\" | sudo tee -a /etc/hosts
        echo \"Mapped: \$IP -> ${ENDPOINT}\"
    else
        echo \"WARNING: Could not resolve ${ENDPOINT}\"
    fi
'" 2>&1

echo "[$(ts)] Uploading custom_mixed.lua..."
python3 -c "
import sys, pathlib
sys.path.insert(0, '$(dirname "$SCRIPT_DIR")')
from aurora.benchmark import CUSTOM_MIXED_LUA
pathlib.Path('/tmp/_custom_mixed.lua').write_text(CUSTOM_MIXED_LUA)
"
$SCP_CMD /tmp/_custom_mixed.lua "ec2-user@$EC2_IP:/tmp/custom_mixed.lua"

echo "[$(ts)] Uploading ec2_sampler.py..."
$SCP_CMD "$SCRIPT_DIR/ec2_sampler.py" "ec2-user@$EC2_IP:/tmp/ec2_sampler.py"

COMMON_SB="--mysql-host='${ENDPOINT}' --mysql-port=3306 --mysql-user=admin --mysql-password='${PASSWORD}' --mysql-db=${DB} --tables=${TABLES} --table-size=${TABLE_SIZE}"

TOTAL=${#THREAD_COUNTS[@]}
IDX=0
for THREADS in "${THREAD_COUNTS[@]}"; do
    IDX=$((IDX + 1))
    echo ""
    echo "########## [$IDX/$TOTAL] $LABEL / threads=$THREADS ##########"

    SB_RUN_CMD="sysbench /tmp/custom_mixed.lua ${COMMON_SB} --threads=${THREADS} --time=${DURATION} --report-interval=${REPORT_INTERVAL} run"
    SB_LOG="$LOGS_DIR/final_${LABEL}_t${THREADS}.log"
    METRICS_CSV="$LOGS_DIR/metrics_${LABEL}_t${THREADS}.csv"
    STAMP=$(date -u +%Y%m%d_%H%M%S)
    RESULT_JSON="$RESULTS_DIR/final_${LABEL}_t${THREADS}_${STAMP}.json"

    echo "[$(ts)] Preparing data ($TABLES tables x $TABLE_SIZE rows, 64 threads)..."
    timeout 600 $SSH_CMD "sysbench oltp_read_write ${COMMON_SB} --threads=64 prepare 2>&1" \
        | tail -5
    echo "[$(ts)] Prepare done."

    echo "[$(ts)] Starting metrics sampler (interval=${SAMPLE_INTERVAL}s)..."
    $SSH_CMD "touch /tmp/.metrics_running"
    $SSH_CMD "nohup python3 /tmp/ec2_sampler.py '${ENDPOINT}' '${PASSWORD}' /tmp/metrics.csv ${SAMPLE_INTERVAL} > /tmp/sampler.log 2>&1 &"
    sleep 2

    RUN_START_EPOCH=$($SSH_CMD "date +%s")
    echo "[$(ts)] Running sysbench: threads=$THREADS duration=${DURATION}s (start_epoch=$RUN_START_EPOCH)..."
    timeout $((DURATION + 120)) $SSH_CMD "${SB_RUN_CMD} 2>&1" > "$SB_LOG"
    echo "[$(ts)] Sysbench complete."

    $SSH_CMD "rm -f /tmp/.metrics_running"
    sleep 3
    echo "[$(ts)] Metrics sampler stopped."

    $SCP_CMD "ec2-user@$EC2_IP:/tmp/metrics.csv" "$METRICS_CSV" 2>/dev/null
    METRIC_LINES=$(wc -l < "$METRICS_CSV" | tr -d ' ')
    echo "[$(ts)] Downloaded metrics: $METRIC_LINES lines"

    echo "[$(ts)] Parsing results (window ${WINDOW_START}-${WINDOW_END}s)..."
    python3 "$SCRIPT_DIR/parse_window.py" \
        --sysbench-output "$SB_LOG" \
        --metrics-csv "$METRICS_CSV" \
        --run-start-epoch "$RUN_START_EPOCH" \
        --window-start "$WINDOW_START" \
        --window-end "$WINDOW_END" \
        --instance-type "$INSTANCE_TYPE" \
        --ec2-type "$EC2_TYPE" \
        --threads "$THREADS" \
        --tables "$TABLES" \
        --table-size "$TABLE_SIZE" \
        --duration "$DURATION" \
        --writer-id "$WRITER_ID" \
        --region "$REGION" \
        --rds-profile "$RDS_PROFILE" \
        --sysbench-cmd "$SB_RUN_CMD" \
        --output "$RESULT_JSON"

    echo "[$(ts)] Cleaning up data..."
    timeout 300 $SSH_CMD "sysbench oltp_read_write ${COMMON_SB} --threads=64 cleanup 2>&1" \
        | tail -3
    echo "[$(ts)] Cleanup done."
    echo ""
done

echo "============================================"
echo "All runs complete for $LABEL"
echo "Results: $RESULTS_DIR/final_${LABEL}_t*.json"
echo "Logs:    $LOGS_DIR/final_${LABEL}_t*.log"
echo "============================================"
