#!/usr/bin/env python3
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
aurora/benchmark.py -- Run sysbench benchmarks against Aurora MySQL.

Discovers the Aurora stack (via state file or AWS tags), uploads a custom IUD
workload Lua script to the EC2 client, runs sysbench benchmarks, and reports
results including TPS, latency percentiles, and IUD row rates.

Usage:
    python3 -m aurora.benchmark
    python3 -m aurora.benchmark --workload custom_iud --threads 64 --duration 300
    python3 -m aurora.benchmark --workload oltp_write_only --threads 32
    python3 -m aurora.benchmark --seed auroralt-002 --region us-west-2
"""

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

from common.util import ts, log
from common.ssh import ssh_run_simple, scp_put_simple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "auroralt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
STACK_PREFIX = "aurora-bench"
KEY_NAME = "aurora-bench-key"
STATE_FILE = "aurora-bench-state.json"
DEFAULT_PORT = 3306
DEFAULT_DB = "sbtest"

WORKLOADS = ["oltp_read_write", "oltp_write_only", "custom_iud", "custom_mixed"]
DEFAULT_WORKLOAD = "custom_mixed"
DEFAULT_THREADS = 64
DEFAULT_DURATION = 300
DEFAULT_TABLES = 128
DEFAULT_TABLE_SIZE = None  # auto-computed as 5x instance RAM
DEFAULT_REPORT_INTERVAL = 10
DEFAULT_PREPARE_THREADS = 32
DATA_TO_RAM_RATIO = 5
BYTES_PER_ROW = 250  # id:4 + k:4 + c:120 + pad:60 + InnoDB overhead

# Fill phase defaults (background data to create buffer pool pressure)
DEFAULT_FILL_DB = "sbfill"
DEFAULT_FILL_TABLES = 256
DEFAULT_FILL_SEED_ROWS = 500_000
DEFAULT_FILL_THREADS = 64

# Benchmark table size default (moderate -- fill handles buffer pool pressure)
DEFAULT_BENCH_TABLE_SIZE = 10_000_000  # 10M rows per benchmark table

INSTANCE_MEMORY_GIB = {
    "db.r6i.large": 16,
    "db.r6i.xlarge": 32,
    "db.r6i.2xlarge": 64,
    "db.r6i.4xlarge": 128,
    "db.r6i.8xlarge": 256,
    "db.r6i.12xlarge": 384,
    "db.r6i.16xlarge": 512,
    "db.r6i.24xlarge": 768,
    "db.r6i.32xlarge": 1024,
    "db.r6g.large": 16,
    "db.r6g.xlarge": 32,
    "db.r6g.2xlarge": 64,
    "db.r6g.4xlarge": 128,
    "db.r6g.8xlarge": 256,
    "db.r6g.12xlarge": 384,
    "db.r6g.16xlarge": 512,
    "db.r7g.large": 16,
    "db.r7g.xlarge": 32,
    "db.r7g.2xlarge": 64,
    "db.r7g.4xlarge": 128,
    "db.r7g.8xlarge": 256,
    "db.r7g.12xlarge": 384,
    "db.r7g.16xlarge": 512,
    "db.r7g.24xlarge": 768,
    "db.r7i.large": 16,
    "db.r7i.xlarge": 32,
    "db.r7i.2xlarge": 64,
    "db.r7i.4xlarge": 128,
    "db.r7i.8xlarge": 256,
    "db.r7i.12xlarge": 384,
    "db.r7i.16xlarge": 512,
    "db.r7i.24xlarge": 768,
    "db.r7i.48xlarge": 1536,
    "db.r8g.large": 16,
    "db.r8g.xlarge": 32,
    "db.r8g.2xlarge": 64,
    "db.r8g.4xlarge": 128,
    "db.r8g.8xlarge": 256,
    "db.r8g.12xlarge": 384,
    "db.r8g.16xlarge": 512,
    "db.r8g.24xlarge": 768,
    "db.r8g.48xlarge": 1536,
}

VERBOSE = False

# IUD ratio targets (from production baseline)
IUD_INSERT_PCT = 68
IUD_UPDATE_PCT = 28
IUD_DELETE_PCT = 4

# Production baseline metrics for comparison
PRODUCTION_BASELINE = {
    "instance_type": "db.r6i.16xlarge",
    "engine_version": "8.0.mysql_aurora.3.10.3",
    "storage_type": "aurora-iopt1",
    "insert_rate": 3600,        # rows/sec on writer
    "update_rate": 1500,        # rows/sec on writer
    "delete_rate": 200,         # rows/sec on writer
    "read_rate": 38300,         # innodb_rows_read/sec on writer
    "total_iud_rate": 5300,     # inserts + updates + deletes
    "queries_per_sec": 25900,   # total queries/sec on writer
    "cpu_writer_avg_pct": 18.5,
    "commit_latency_ms": 1.7,
    "dml_latency_ms": 0.3,
    "write_throughput_mbs": 12.4,
}

CUSTOM_IUD_LUA = r"""
-- Self-contained IUD workload (no oltp_common dependency)

sysbench.cmdline.options = {
    tables = {"Number of tables", 8},
    table_size = {"Number of rows per table", 100000},
}

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local num_tables = tonumber(sysbench.opt.tables) or 8
    local table_size = tonumber(sysbench.opt.table_size) or 100000
    local table_name = "sbtest" .. sysbench.rand.uniform(1, num_tables)
    local id = sysbench.rand.uniform(1, table_size)
    local k_val = sysbench.rand.uniform(1, table_size)
    local c_val = sysbench.rand.string(string.rep("@", 120))
    local pad_val = sysbench.rand.string(string.rep("@", 60))
    local r = sysbench.rand.uniform(1, 100)

    con:query("BEGIN")

    if r <= 68 then
        con:query(string.format(
            "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')",
            table_name, k_val, c_val, pad_val))
    elseif r <= 96 then
        con:query(string.format(
            "UPDATE %s SET k = k + 1 WHERE id = %d",
            table_name, id))
    else
        con:query(string.format(
            "DELETE FROM %s WHERE id = %d LIMIT 1",
            table_name, id))
    end

    con:query("COMMIT")
end
""".strip()

# Mixed read+IUD workload matching production ratios:
#   ~88% reads, ~8% inserts, ~3% updates, ~1% deletes
# (derived from: 38.3K reads + 3.6K inserts + 1.5K updates + 0.2K deletes = 43.6K ops/sec)
CUSTOM_MIXED_LUA = r"""
sysbench.cmdline.options = {
    tables = {"Number of tables", 32},
    table_size = {"Number of rows per table", 1000000},
}

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local num_tables = tonumber(sysbench.opt.tables) or 32
    local table_size = tonumber(sysbench.opt.table_size) or 1000000
    local table_name = "sbtest" .. sysbench.rand.uniform(1, num_tables)
    local id = sysbench.rand.uniform(1, table_size)
    local r = sysbench.rand.uniform(1, 1000)

    con:query("BEGIN")

    if r <= 878 then
        -- ~87.8% reads: point lookup by primary key
        con:query(string.format(
            "SELECT c FROM %s WHERE id = %d",
            table_name, id))
    elseif r <= 961 then
        -- ~8.3% inserts
        local k_val = sysbench.rand.uniform(1, table_size)
        local c_val = sysbench.rand.string(string.rep("@", 120))
        local pad_val = sysbench.rand.string(string.rep("@", 60))
        con:query(string.format(
            "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')",
            table_name, k_val, c_val, pad_val))
    elseif r <= 995 then
        -- ~3.4% updates
        con:query(string.format(
            "UPDATE %s SET k = k + 1 WHERE id = %d",
            table_name, id))
    else
        -- ~0.5% deletes
        con:query(string.format(
            "DELETE FROM %s WHERE id = %d LIMIT 1",
            table_name, id))
    end

    con:query("COMMIT")
end
""".strip()

# ---------------------------------------------------------------------------
# Thin wrappers -- adapt original call signatures to common.ssh API
# ---------------------------------------------------------------------------


def _ssh_run(host: str, script: str, key_path: str,
             timeout: int = 300) -> subprocess.CompletedProcess:
    vlog(f"ssh -> {host}: {script[:120]}...")
    return ssh_run_simple(host, key_path, script, timeout=timeout)


def _scp_to(host: str, local_path: str, remote_path: str,
            key_path: str) -> None:
    scp_put_simple(host, key_path, local_path, remote_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def vlog(msg: str) -> None:
    if VERBOSE:
        log(f"  {msg}")


def load_state(script_dir: Path) -> dict:
    path = script_dir / STATE_FILE
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Compute helpers
# ---------------------------------------------------------------------------


def compute_table_size(instance_type: str, tables: int) -> int:
    memory_gib = INSTANCE_MEMORY_GIB.get(instance_type)
    if not memory_gib:
        log(f"WARNING: Unknown instance type '{instance_type}', "
            f"falling back to 1M rows/table")
        return 1_000_000
    target_bytes = memory_gib * (1024 ** 3) * DATA_TO_RAM_RATIO
    rows_total = target_bytes // BYTES_PER_ROW
    table_size = rows_total // tables
    table_size = (table_size // 1_000_000) * 1_000_000
    if table_size < 1_000_000:
        table_size = 1_000_000
    return table_size


def compute_fill_params(instance_type: str, fill_tables: int,
                        seed_rows: int,
                        target_gb: int | None = None) -> tuple[int, int]:
    """Return (doublings, estimated_final_gb) for the fill phase."""
    if target_gb is None:
        memory_gib = INSTANCE_MEMORY_GIB.get(instance_type, 512)
        target_gb = memory_gib * DATA_TO_RAM_RATIO
    target_bytes = target_gb * 1e9
    doublings = 0
    current_rows = seed_rows
    while current_rows * fill_tables * BYTES_PER_ROW < target_bytes:
        doublings += 1
        current_rows *= 2
    final_rows = seed_rows * (2 ** doublings)
    final_gb = round(final_rows * fill_tables * BYTES_PER_ROW / 1e9, 1)
    return doublings, final_gb


# ---------------------------------------------------------------------------
# Stack discovery
# ---------------------------------------------------------------------------


def discover_stack(script_dir: Path, region: str, profile: str,
                   seed: str) -> dict:
    state = load_state(script_dir)
    if state and state.get("endpoint") and state.get("client_public_ip"):
        log(f"Loaded stack from {STATE_FILE}")
        return state

    log("State file not found, discovering via AWS...")
    stack = f"{STACK_PREFIX}-{seed}"

    aws_base = ["aws", "--region", region, "--profile", profile, "--output", "json"]

    r = subprocess.run(
        aws_base + ["rds", "describe-db-clusters",
                    "--db-cluster-identifier", f"{stack}-cluster"],
        capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        log(f"ERROR: Could not find cluster '{stack}-cluster'")
        sys.exit(1)
    cluster = json.loads(r.stdout)["DBClusters"][0]

    r2 = subprocess.run(
        aws_base + ["ec2", "describe-instances",
                    "--filters",
                    f"Name=tag:Project,Values={stack}",
                    "Name=instance-state-name,Values=running"],
        capture_output=True, text=True, timeout=30)
    ec2_data = json.loads(r2.stdout)
    client_ip = ""
    if ec2_data.get("Reservations"):
        client_ip = ec2_data["Reservations"][0]["Instances"][0].get(
            "PublicIpAddress", "")

    return {
        "stack": stack,
        "endpoint": cluster["Endpoint"],
        "cluster_port": cluster.get("Port", DEFAULT_PORT),
        "client_public_ip": client_ip,
    }


# ---------------------------------------------------------------------------
# Lua script upload and database management
# ---------------------------------------------------------------------------


def upload_lua_script(host: str, key_path: str, workload: str) -> str:
    lua_content = CUSTOM_MIXED_LUA if workload == "custom_mixed" else CUSTOM_IUD_LUA
    remote_path = f"/tmp/aurora_{workload}.lua"
    local_tmp = Path(f"/tmp/aurora_{workload}_local.lua")
    local_tmp.write_text(lua_content)
    _scp_to(host, str(local_tmp), remote_path, key_path)
    local_tmp.unlink(missing_ok=True)
    log(f"Uploaded {workload} Lua script to {remote_path}")
    return remote_path


def create_database(host: str, key_path: str, endpoint: str,
                    port: int, password: str, db: str) -> None:
    log(f"Creating database '{db}'...")
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'CREATE DATABASE IF NOT EXISTS {db}' 2>&1"
    )
    r = _ssh_run(host, script, key_path)
    if r.returncode != 0:
        log(f"ERROR creating database: {r.stdout} {r.stderr}")
        sys.exit(1)
    log(f"  Database '{db}' ready")


# ---------------------------------------------------------------------------
# sysbench prepare / cleanup
# ---------------------------------------------------------------------------


def sysbench_prepare(host: str, key_path: str, endpoint: str, port: int,
                     password: str, db: str, tables: int,
                     table_size: int, prepare_threads: int = 1) -> None:
    total_rows = tables * table_size
    est_gb = round(total_rows * 250 / 1e9, 1)
    est_tb = round(est_gb / 1000, 2) if est_gb >= 500 else None
    size_str = f"~{est_tb} TB" if est_tb else f"~{est_gb} GB"
    log(f"Preparing sysbench tables ({tables} x {table_size:,} rows, "
        f"{size_str}, {prepare_threads} threads)...")

    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user=admin --mysql-password='{password}' "
        f"--mysql-db={db} --tables={tables} --table-size={table_size} "
        f"--threads={prepare_threads}"
    )
    remote_cmd = f"sysbench oltp_read_write {common} prepare 2>&1"

    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-o", "LogLevel=ERROR",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=120",
        "-i", key_path,
        f"ec2-user@{host}",
        remote_cmd,
    ]

    start = time.time()
    tables_done = 0
    proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            # sysbench prints "Creating table 'sbtestN'..." for each table
            if "Creating table" in line:
                tables_done += 1
                elapsed_min = (time.time() - start) / 60
                log(f"  [{tables_done}/{tables}] {line}  "
                    f"({elapsed_min:.1f} min elapsed)")
            elif "Inserting" in line or "table_size" in line.lower():
                log(f"  {line}")
            elif VERBOSE:
                vlog(line)
        proc.wait()
    except KeyboardInterrupt:
        log("Interrupted -- killing fill process...")
        proc.kill()
        proc.wait()
        raise

    elapsed_min = (time.time() - start) / 60
    if proc.returncode != 0:
        log(f"ERROR filling tables (exit code {proc.returncode})")
        sys.exit(1)
    log(f"  Fill complete: {tables} tables, {total_rows:,} rows, "
        f"{size_str} in {elapsed_min:.1f} minutes")


def sysbench_cleanup(host: str, key_path: str, endpoint: str, port: int,
                     password: str, db: str, tables: int,
                     table_size: int) -> None:
    log("Cleaning up sysbench tables...")
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user=admin --mysql-password='{password}' "
        f"--mysql-db={db} --tables={tables} --table-size={table_size}"
    )
    script = f"sysbench oltp_read_write {common} cleanup 2>&1"
    _ssh_run(host, script, key_path, timeout=120)
    log("  Cleanup done")


# ---------------------------------------------------------------------------
# Fill phase (INSERT ... SELECT doubling)
# ---------------------------------------------------------------------------


def _build_doubling_script(endpoint: str, port: int, password: str,
                           db: str, tables: int, max_parallel: int) -> str:
    return f"""#!/bin/bash
DB_HOST={endpoint}
DB_PORT={port}
DB_PASS='{password}'
DB_NAME={db}
ERRORS=0

do_insert() {{
    local tbl=$1
    if ! mysql -h "$DB_HOST" -P "$DB_PORT" -u admin -p"$DB_PASS" "$DB_NAME" -e "INSERT INTO sbtest$tbl (k, c, pad) SELECT k, c, pad FROM sbtest$tbl" 2>/tmp/double_err_$tbl.log; then
        echo "FAILED: sbtest$tbl: $(cat /tmp/double_err_$tbl.log)" >&2
        return 1
    fi
    rm -f /tmp/double_err_$tbl.log
}}

running=0
for i in $(seq 1 {tables}); do
    do_insert $i &
    running=$((running + 1))
    if [ $running -ge {max_parallel} ]; then
        wait -n 2>/dev/null || ERRORS=$((ERRORS + 1))
        running=$((running - 1))
    fi
done
wait

FINAL_ROWS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u admin -p"$DB_PASS" "$DB_NAME" -sN -e "SELECT SUM(table_rows) FROM information_schema.tables WHERE table_schema='$DB_NAME'" 2>/dev/null)
echo "DOUBLING_COMPLETE errors=$ERRORS total_rows=$FINAL_ROWS"
"""


def fast_fill(host: str, key_path: str, endpoint: str, port: int,
              password: str, fill_db: str, fill_tables: int,
              seed_rows: int, doublings: int, fill_threads: int) -> None:
    final_rows = seed_rows * (2 ** doublings)
    total_rows = final_rows * fill_tables
    est_gb = round(total_rows * BYTES_PER_ROW / 1e9, 1)

    log(f"=== FILL PHASE ===")
    log(f"Target: {fill_tables} tables x {final_rows:,} rows/table "
        f"(~{est_gb} GB), {doublings} doublings from {seed_rows:,} seed")

    log(f"Dropping existing '{fill_db}' database (if any)...")
    drop_script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'DROP DATABASE IF EXISTS {fill_db}' 2>&1"
    )
    _ssh_run(host, drop_script, key_path)

    create_database(host, key_path, endpoint, port, password, fill_db)
    log(f"Creating {fill_tables} seed tables with {seed_rows:,} rows each...")
    sysbench_prepare(host, key_path, endpoint, port, password, fill_db,
                     fill_tables, seed_rows, fill_threads)

    for d in range(1, doublings + 1):
        rows_before = seed_rows * (2 ** (d - 1))
        rows_after = rows_before * 2
        batch_gb = round(rows_before * fill_tables * BYTES_PER_ROW / 1e9, 1)
        log(f"Doubling {d}/{doublings}: {rows_before:,} -> {rows_after:,} rows/table "
            f"(inserting ~{batch_gb} GB across {fill_tables} tables)...")

        script = _build_doubling_script(endpoint, port, password, fill_db,
                                        fill_tables, fill_threads)
        start = time.time()
        r = _ssh_run(host, script, key_path, timeout=7200)
        elapsed = time.time() - start
        if r.returncode != 0:
            log(f"ERROR during doubling {d}: {r.stdout[:500]} {r.stderr[:500]}")
            sys.exit(1)
        stdout_last = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
        if r.stderr.strip():
            for line in r.stderr.strip().splitlines()[:5]:
                log(f"  WARN: {line}")
        log(f"  Doubling {d} complete in {elapsed:.0f}s -- {stdout_last}")

    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' {fill_db} "
        f"-e \"SELECT COUNT(*) as tables_count, "
        f"SUM(table_rows) as total_rows, "
        f"ROUND(SUM(data_length + index_length)/1024/1024/1024, 2) as total_gb "
        f"FROM information_schema.tables WHERE table_schema='{fill_db}'\" "
        f"--batch 2>/dev/null"
    )
    r = _ssh_run(host, script, key_path)
    log(f"Fill stats:\n{r.stdout.strip()}")
    log(f"=== FILL COMPLETE ===")


# ---------------------------------------------------------------------------
# Sysbench output parsing
# ---------------------------------------------------------------------------


def parse_sysbench_output(output: str) -> dict:
    result = {}
    m = re.search(r"transactions:\s+\d+\s+\(([\d.]+)\s+per sec", output)
    if m:
        result["tps"] = float(m.group(1))
    m = re.search(r"queries:\s+\d+\s+\(([\d.]+)\s+per sec", output)
    if m:
        result["qps"] = float(m.group(1))
    m = re.search(r"read:\s+(\d+)", output)
    if m:
        result["reads"] = int(m.group(1))
    m = re.search(r"write:\s+(\d+)", output)
    if m:
        result["writes"] = int(m.group(1))
    m = re.search(r"other:\s+(\d+)", output)
    if m:
        result["other"] = int(m.group(1))
    m = re.search(r"total:\s+(\d+)", output)
    if m:
        result["total_queries"] = int(m.group(1))
    for pct in ["min", "avg", "max", "95th percentile"]:
        key = pct.replace(" ", "_").replace("percentile", "pct")
        m = re.search(rf"{re.escape(pct)}:\s+([\d.]+)", output)
        if m:
            result[f"latency_{key}_ms"] = float(m.group(1))
    return result


def parse_interval_lines(output: str) -> list[dict]:
    intervals = []
    for line in output.splitlines():
        m = re.match(
            r"\[\s*([\d.]+)s\]\s+thds:\s+(\d+)\s+"
            r"tps:\s+([\d.]+)\s+qps:\s+([\d.]+)\s+.*?"
            r"lat\s+\(ms,\s*95%\):\s+([\d.]+)",
            line)
        if m:
            intervals.append({
                "time_s": float(m.group(1)),
                "threads": int(m.group(2)),
                "tps": float(m.group(3)),
                "qps": float(m.group(4)),
                "p95_ms": float(m.group(5)),
            })
    return intervals


# ---------------------------------------------------------------------------
# CloudWatch and client metrics
# ---------------------------------------------------------------------------


def get_aurora_cpu(writer_id: str, region: str, profile: str,
                   start_time: str, end_time: str) -> float | None:
    try:
        cmd = [
            "aws", "cloudwatch", "get-metric-statistics",
            "--namespace", "AWS/RDS",
            "--metric-name", "CPUUtilization",
            "--dimensions", f"Name=DBInstanceIdentifier,Value={writer_id}",
            "--start-time", start_time,
            "--end-time", end_time,
            "--period", "60",
            "--statistics", "Average",
            "--region", region,
            "--profile", profile,
            "--output", "json",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
        points = data.get("Datapoints", [])
        if not points:
            return None
        avg = sum(p["Average"] for p in points) / len(points)
        return round(avg, 1)
    except Exception:
        return None


def start_mpstat(host: str, key_path: str, duration: int) -> None:
    mpstat_duration = duration + 10
    script = (
        "sudo yum install -y sysstat >/dev/null 2>&1; "
        "rm -f /tmp/mpstat_bench.log; "
        f"nohup mpstat 1 {mpstat_duration} > /tmp/mpstat_bench.log 2>&1 &"
    )
    _ssh_run(host, script, key_path, timeout=30)
    vlog("mpstat started in background on EC2")


def stop_and_parse_mpstat(host: str, key_path: str) -> float | None:
    script = (
        "pkill -f 'mpstat 1' 2>/dev/null; sleep 1; "
        "cat /tmp/mpstat_bench.log 2>/dev/null"
    )
    r = _ssh_run(host, script, key_path, timeout=30)
    if r.returncode != 0 or not r.stdout.strip():
        return None
    return parse_mpstat_output(r.stdout)


def parse_mpstat_output(output: str) -> float | None:
    # mpstat lines look like:
    # HH:MM:SS  CPU  %usr  %nice  %sys  %iowait  %irq  %soft  %steal  %guest  %gnice  %idle
    # We want %usr + %sys columns, averaged across all interval lines.
    user_sys_samples = []
    for line in output.splitlines():
        line = line.strip()
        # Skip headers, blank lines, and the "Average:" summary line
        if not line or "CPU" in line and "%" in line:
            continue
        if line.startswith("Average"):
            continue
        if line.startswith("Linux"):
            continue
        parts = line.split()
        # Need at least: time CPU %usr %nice %sys ... (min 12 columns)
        if len(parts) < 12:
            continue
        # Column 1 should be "all" (we want the aggregate, not per-CPU)
        # mpstat format: time AM/PM CPU %usr %nice %sys ...
        # or: time CPU %usr %nice %sys ... (depending on locale)
        try:
            cpu_col = parts.index("all")
            usr = float(parts[cpu_col + 1])
            # %nice is cpu_col+2, %sys is cpu_col+3
            sys_pct = float(parts[cpu_col + 3])
            user_sys_samples.append(usr + sys_pct)
        except (ValueError, IndexError):
            continue
    if not user_sys_samples:
        return None
    return round(sum(user_sys_samples) / len(user_sys_samples), 1)


def get_aurora_network_metrics(writer_id: str, region: str, profile: str,
                               start_time: str,
                               end_time: str) -> dict:
    metrics = {}
    for metric_name, key in [("NetworkReceiveThroughput", "aurora_net_recv_mbps"),
                              ("NetworkTransmitThroughput", "aurora_net_xmit_mbps")]:
        try:
            cmd = [
                "aws", "cloudwatch", "get-metric-statistics",
                "--namespace", "AWS/RDS",
                "--metric-name", metric_name,
                "--dimensions",
                f"Name=DBInstanceIdentifier,Value={writer_id}",
                "--start-time", start_time,
                "--end-time", end_time,
                "--period", "60",
                "--statistics", "Average",
                "--region", region,
                "--profile", profile,
                "--output", "json",
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                metrics[key] = None
                continue
            data = json.loads(r.stdout)
            points = data.get("Datapoints", [])
            if not points:
                metrics[key] = None
                continue
            avg_bytes = sum(p["Average"] for p in points) / len(points)
            # Convert bytes/sec to Mbps (megabits per second)
            metrics[key] = round(avg_bytes * 8 / 1_000_000, 1)
        except Exception:
            metrics[key] = None
    return metrics


# ---------------------------------------------------------------------------
# Sysbench run (single and parallel)
# ---------------------------------------------------------------------------


def run_sysbench_parallel(host: str, key_path: str, endpoint: str, port: int,
                          password: str, db: str, workload: str, tables: int,
                          table_size: int, threads: int, duration: int,
                          report_interval: int, lua_path: str | None,
                          parallel: int) -> dict:
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user=admin --mysql-password='{password}' "
        f"--mysql-db={db} --tables={tables} --table-size={table_size} "
        f"--threads={threads} --time={duration} "
        f"--report-interval={report_interval}"
    )

    if workload in ("custom_iud", "custom_mixed") and lua_path:
        sb_cmd = f"sysbench {lua_path} {common} run"
    else:
        sb_cmd = f"sysbench {workload} {common} run"

    parts = ["rm -f /tmp/sysbench_run_*.log"]
    for i in range(1, parallel + 1):
        parts.append(f"{sb_cmd} > /tmp/sysbench_run_{i}.log 2>&1 &")
    parts.append("wait")
    for i in range(1, parallel + 1):
        parts.append(f"echo '===SYSBENCH_LOG_{i}_START==='")
        parts.append(f"cat /tmp/sysbench_run_{i}.log")
        parts.append(f"echo '===SYSBENCH_LOG_{i}_END==='")
    script = "\n".join(parts)

    thread_label = f"{parallel}x{threads}"
    log(f"Running sysbench: workload={workload}, {thread_label} threads "
        f"({parallel} processes x {threads}), duration={duration}s...")
    start = time.time()
    r = _ssh_run(host, script, key_path, timeout=duration + 120)
    elapsed = time.time() - start

    if r.returncode != 0:
        log(f"ERROR: parallel sysbench failed:\n{r.stdout}\n{r.stderr}")
        return {"error": r.stdout + r.stderr}

    all_results = []
    for i in range(1, parallel + 1):
        start_marker = f"===SYSBENCH_LOG_{i}_START==="
        end_marker = f"===SYSBENCH_LOG_{i}_END==="
        idx_start = r.stdout.find(start_marker)
        idx_end = r.stdout.find(end_marker)
        if idx_start == -1 or idx_end == -1:
            log(f"WARNING: Could not find output for sysbench process {i}")
            continue
        log_text = r.stdout[idx_start + len(start_marker):idx_end]
        parsed = parse_sysbench_output(log_text)
        if parsed:
            all_results.append(parsed)

    if not all_results:
        log("ERROR: No sysbench outputs could be parsed")
        return {"error": "No parseable sysbench output"}

    combined = {
        "tps": sum(r.get("tps", 0) for r in all_results),
        "qps": sum(r.get("qps", 0) for r in all_results),
        "reads": sum(r.get("reads", 0) for r in all_results),
        "writes": sum(r.get("writes", 0) for r in all_results),
        "other": sum(r.get("other", 0) for r in all_results),
        "total_queries": sum(r.get("total_queries", 0) for r in all_results),
    }
    for lat_key in ["latency_min_ms", "latency_avg_ms", "latency_max_ms"]:
        vals = [r[lat_key] for r in all_results if lat_key in r]
        if vals:
            combined[lat_key] = round(sum(vals) / len(vals), 2)
    p95_vals = [r["latency_95th_pct_ms"] for r in all_results
                if "latency_95th_pct_ms" in r]
    if p95_vals:
        combined["latency_95th_pct_ms"] = max(p95_vals)

    combined["intervals"] = []
    combined["workload"] = workload
    combined["threads"] = threads
    combined["parallel"] = parallel
    combined["thread_label"] = thread_label
    combined["duration_s"] = duration
    combined["elapsed_s"] = round(elapsed, 1)
    combined["raw_output"] = r.stdout
    combined["per_process_results"] = all_results
    return combined


def run_sysbench(host: str, key_path: str, endpoint: str, port: int,
                 password: str, db: str, workload: str, tables: int,
                 table_size: int, threads: int, duration: int,
                 report_interval: int, lua_path: str | None) -> dict:
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user=admin --mysql-password='{password}' "
        f"--mysql-db={db} --tables={tables} --table-size={table_size} "
        f"--threads={threads} --time={duration} "
        f"--report-interval={report_interval}"
    )

    if workload in ("custom_iud", "custom_mixed") and lua_path:
        script = f"sysbench {lua_path} {common} run 2>&1"
    else:
        script = f"sysbench {workload} {common} run 2>&1"

    log(f"Running sysbench: workload={workload}, threads={threads}, "
        f"duration={duration}s...")
    start = time.time()
    r = _ssh_run(host, script, key_path, timeout=duration + 120)
    elapsed = time.time() - start

    if r.returncode != 0:
        log(f"ERROR: sysbench failed:\n{r.stdout}\n{r.stderr}")
        return {"error": r.stdout + r.stderr}

    result = parse_sysbench_output(r.stdout)
    result["intervals"] = parse_interval_lines(r.stdout)
    result["workload"] = workload
    result["threads"] = threads
    result["duration_s"] = duration
    result["elapsed_s"] = round(elapsed, 1)
    result["raw_output"] = r.stdout
    return result


# ---------------------------------------------------------------------------
# InnoDB row counters / IUD measurement
# ---------------------------------------------------------------------------


def get_innodb_rows(host: str, key_path: str, endpoint: str,
                    port: int, password: str) -> dict:
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e \"SHOW GLOBAL STATUS LIKE 'Innodb_rows_%'\" "
        f"--batch --skip-column-names 2>&1"
    )
    r = _ssh_run(host, script, key_path)
    rows = {}
    for line in r.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            rows[parts[0].lower()] = int(parts[1])
    return rows


def measure_iud_rates(host: str, key_path: str, endpoint: str,
                      port: int, password: str, duration: int) -> dict:
    before = get_innodb_rows(host, key_path, endpoint, port, password)
    time.sleep(duration)
    after = get_innodb_rows(host, key_path, endpoint, port, password)

    rates = {}
    for op in ["innodb_rows_inserted", "innodb_rows_updated",
               "innodb_rows_deleted", "innodb_rows_read"]:
        b = before.get(op, 0)
        a = after.get(op, 0)
        rates[op.replace("innodb_rows_", "") + "_per_sec"] = round(
            (a - b) / duration, 1)
    total_iud = (rates.get("inserted_per_sec", 0) +
                 rates.get("updated_per_sec", 0) +
                 rates.get("deleted_per_sec", 0))
    rates["total_iud_per_sec"] = round(total_iud, 1)
    return rates


# ---------------------------------------------------------------------------
# Results display and persistence
# ---------------------------------------------------------------------------


def print_results(result: dict, iud_rates: dict | None = None,
                   cpu_pct: float | None = None,
                   tables: int = 0, table_size: int = 0,
                   client_cpu_pct: float | None = None,
                   net_metrics: dict | None = None) -> None:
    total_rows = tables * table_size
    est_gb = round(total_rows * 250 / 1e9, 1) if total_rows else 0
    thread_label = result.get("thread_label", str(result.get("threads", "?")))
    print()
    print("=" * 70)
    print(f"Benchmark Results: {result.get('workload', 'unknown')}")
    print("=" * 70)
    print(f"  Threads:          {thread_label}")
    print(f"  Duration:         {result.get('duration_s', '?')}s "
          f"(elapsed: {result.get('elapsed_s', '?')}s)")
    if tables and table_size:
        print(f"  Data size:        {tables} tables x {table_size:,} rows "
              f"(~{est_gb} GB)")
    print(f"  TPS:              {result.get('tps', 0):.1f}")
    print(f"  QPS:              {result.get('qps', 0):.1f}")
    print(f"  Reads:            {result.get('reads', 0)}")
    print(f"  Writes:           {result.get('writes', 0)}")
    print(f"  Other:            {result.get('other', 0)}")
    print(f"  Total queries:    {result.get('total_queries', 0)}")
    if cpu_pct is not None:
        print(f"  Aurora CPU avg:   {cpu_pct:.1f}%")
    if client_cpu_pct is not None:
        print(f"  Client CPU avg:   {client_cpu_pct:.1f}%")
    if net_metrics:
        recv = net_metrics.get("aurora_net_recv_mbps")
        xmit = net_metrics.get("aurora_net_xmit_mbps")
        if recv is not None:
            print(f"  Aurora net recv:  {recv:.1f} Mbps")
        if xmit is not None:
            print(f"  Aurora net xmit:  {xmit:.1f} Mbps")
    print()
    print("  Latency:")
    print(f"    min:            {result.get('latency_min_ms', 0):.2f} ms")
    print(f"    avg (commit):   {result.get('latency_avg_ms', 0):.2f} ms")
    print(f"    max:            {result.get('latency_max_ms', 0):.2f} ms")
    print(f"    P95:            {result.get('latency_95th_pct_ms', 0):.2f} ms")

    if iud_rates:
        print()
        print("  IUD Rates (from InnoDB counters):")
        print(f"    Inserts/sec:    {iud_rates.get('inserted_per_sec', 0):.1f}")
        print(f"    Updates/sec:    {iud_rates.get('updated_per_sec', 0):.1f}")
        print(f"    Deletes/sec:    {iud_rates.get('deleted_per_sec', 0):.1f}")
        print(f"    Total IUD/sec:  {iud_rates.get('total_iud_per_sec', 0):.1f}")
        print(f"    Reads/sec:      {iud_rates.get('read_per_sec', 0):.1f}")
    print()

    if iud_rates and iud_rates.get("total_iud_per_sec", 0) > 0:
        baseline = PRODUCTION_BASELINE
        actual_iud = iud_rates["total_iud_per_sec"]
        actual_insert = iud_rates.get("inserted_per_sec", 0)
        actual_update = iud_rates.get("updated_per_sec", 0)
        actual_delete = iud_rates.get("deleted_per_sec", 0)
        actual_read = iud_rates.get("read_per_sec", 0)
        print("  Production Baseline Comparison:")
        print(f"    {'Metric':<24} {'Baseline':>10} {'Actual':>10} {'Ratio':>8}")
        print(f"    {'-'*24} {'-'*10} {'-'*10} {'-'*8}")
        print(f"    {'Inserts/sec':<24} {baseline['insert_rate']:>10} {actual_insert:>10.0f} "
              f"{(actual_insert / baseline['insert_rate'] * 100):>7.1f}%")
        print(f"    {'Updates/sec':<24} {baseline['update_rate']:>10} {actual_update:>10.0f} "
              f"{(actual_update / baseline['update_rate'] * 100):>7.1f}%")
        print(f"    {'Deletes/sec':<24} {baseline['delete_rate']:>10} {actual_delete:>10.0f} "
              f"{(actual_delete / baseline['delete_rate'] * 100):>7.1f}%")
        print(f"    {'Total IUD/sec':<24} {baseline['total_iud_rate']:>10} {actual_iud:>10.0f} "
              f"{(actual_iud / baseline['total_iud_rate'] * 100):>7.1f}%")
        print(f"    {'Reads/sec':<24} {baseline['read_rate']:>10} {actual_read:>10.0f} "
              f"{(actual_read / baseline['read_rate'] * 100):>7.1f}%")
        if result.get("latency_avg_ms"):
            print(f"    {'Commit latency (ms)':<24} {baseline['commit_latency_ms']:>10} "
                  f"{result['latency_avg_ms']:>10.1f} "
                  f"{(result['latency_avg_ms'] / baseline['commit_latency_ms'] * 100):>7.1f}%")
        if cpu_pct is not None:
            print(f"    {'CPU avg %':<24} {baseline['cpu_writer_avg_pct']:>10} "
                  f"{cpu_pct:>10.1f} "
                  f"{(cpu_pct / baseline['cpu_writer_avg_pct'] * 100):>7.1f}%")
        print()


def save_results(result: dict, iud_rates: dict | None,
                 script_dir: Path, workload: str,
                 state: dict | None = None,
                 tables: int = 0, table_size: int = 0,
                 cpu_pct: float | None = None,
                 client_cpu_pct: float | None = None,
                 net_metrics: dict | None = None,
                 parallel: int = 1) -> Path:
    results_dir = script_dir / "results"
    results_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    thread_label = result.get("thread_label", str(result.get("threads", "?")))
    filename = f"{workload}_{thread_label}_{stamp}.json"
    path = results_dir / filename

    total_rows = tables * table_size
    est_gb = round(total_rows * BYTES_PER_ROW / 1e9, 1) if total_rows else 0

    data = {
        "aurora_instance_type": (state or {}).get("aurora_instance_type", ""),
        "ec2_instance_type": (state or {}).get("ec2_instance_type", ""),
        "threads": result.get("threads", 0),
        "parallel": parallel,
        "thread_label": thread_label,
        "duration_s": result.get("duration_s", 0),
        "elapsed_s": result.get("elapsed_s", 0),
        "tables": tables,
        "table_size": table_size,
        "data_size_gb": est_gb,
        "workload": workload,
        "benchmark": result,
        "iud_rates": iud_rates,
        "aurora_cpu_avg_pct": cpu_pct,
        "client_cpu_avg_pct": client_cpu_pct,
        "aurora_net_recv_mbps": (net_metrics or {}).get("aurora_net_recv_mbps"),
        "aurora_net_xmit_mbps": (net_metrics or {}).get("aurora_net_xmit_mbps"),
        "timestamp": ts(),
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    log(f"Results saved to {path}")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run sysbench benchmarks against Aurora MySQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(f"""\
            Workloads:
              oltp_read_write   Standard sysbench mixed read/write
              oltp_write_only   Standard sysbench write-only
              custom_iud        Custom insert/update/delete workload
                                ({IUD_INSERT_PCT}%% insert, {IUD_UPDATE_PCT}%% update, {IUD_DELETE_PCT}%% delete)

            Examples:
              python3 -m aurora.benchmark
              python3 -m aurora.benchmark --workload custom_iud --threads 64 --duration 300
              python3 -m aurora.benchmark --workload oltp_write_only --threads 32
              python3 -m aurora.benchmark --skip-prepare --skip-cleanup
        """),
    )
    p.add_argument("--workload", default=DEFAULT_WORKLOAD, choices=WORKLOADS,
                   help=f"Workload to run (default: {DEFAULT_WORKLOAD})")
    p.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                   help=f"Concurrent threads (default: {DEFAULT_THREADS})")
    p.add_argument("--duration", type=int, default=DEFAULT_DURATION,
                   help=f"Benchmark duration in seconds (default: {DEFAULT_DURATION})")
    p.add_argument("--tables", type=int, default=DEFAULT_TABLES,
                   help=f"Number of sysbench tables (default: {DEFAULT_TABLES})")
    p.add_argument("--table-size", type=int, default=None,
                   help="Rows per table (default: auto-computed as 5x instance RAM)")
    p.add_argument("--report-interval", type=int, default=DEFAULT_REPORT_INTERVAL,
                   help=f"Interval reporting in seconds (default: {DEFAULT_REPORT_INTERVAL})")
    p.add_argument("--db", default=DEFAULT_DB,
                   help=f"Database name (default: {DEFAULT_DB})")
    p.add_argument("--host", default=None,
                   help="EC2 client IP (auto-discovered if omitted)")
    p.add_argument("--endpoint", default=None,
                   help="Aurora endpoint (auto-discovered if omitted)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--region", default=DEFAULT_REGION)
    p.add_argument("--seed", default=DEFAULT_SEED)
    p.add_argument("--aws-profile", default=DEFAULT_PROFILE)
    p.add_argument("--ssh-key", default=None)
    p.add_argument("--password", default=None,
                   help="Aurora master password (default: env AURORA_MASTER_PASSWORD "
                        "or BenchMark2024!)")
    p.add_argument("--prepare-threads", type=int, default=DEFAULT_PREPARE_THREADS,
                   help=f"Threads for parallel table creation (default: {DEFAULT_PREPARE_THREADS})")
    p.add_argument("--rds-profile", default=None,
                   help="AWS profile for CloudWatch RDS metrics (default: same as --aws-profile)")
    p.add_argument("--skip-prepare", action="store_true",
                   help="Skip data preparation (tables already exist from a previous run)")
    p.add_argument("--skip-cleanup", action="store_true",
                   help="Keep tables after benchmark")
    p.add_argument("--skip-iud-measurement", action="store_true",
                   help="Skip InnoDB row counter measurement")
    p.add_argument("--parallel", type=int, default=1,
                   help="Run N sysbench processes in parallel (default: 1)")
    p.add_argument("--fill", action="store_true",
                   help="Run fill phase only: create background data in separate DB, then exit")
    p.add_argument("--fill-target-gb", type=int, default=None,
                   help="Target fill data size in GB (default: 5x instance RAM)")
    p.add_argument("--fill-tables", type=int, default=DEFAULT_FILL_TABLES,
                   help=f"Number of fill tables (default: {DEFAULT_FILL_TABLES})")
    p.add_argument("--fill-seed-rows", type=int, default=DEFAULT_FILL_SEED_ROWS,
                   help=f"Seed rows per fill table before doubling (default: {DEFAULT_FILL_SEED_ROWS:,})")
    p.add_argument("--fill-threads", type=int, default=DEFAULT_FILL_THREADS,
                   help=f"Parallel threads/sessions for fill operations (default: {DEFAULT_FILL_THREADS})")
    p.add_argument("--fill-db", default=DEFAULT_FILL_DB,
                   help=f"Database name for fill data (default: {DEFAULT_FILL_DB})")
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    password = args.password or os.environ.get("AURORA_MASTER_PASSWORD", "BenchMark2024!")
    script_dir = Path(__file__).resolve().parent

    if args.ssh_key:
        key_path = args.ssh_key
    else:
        key_path = str(script_dir / f"{KEY_NAME}.pem")

    if not os.path.isfile(key_path):
        log(f"WARNING: SSH key not found at {key_path}")

    info = discover_stack(script_dir, args.region, args.aws_profile, args.seed)
    host = args.host or info.get("client_public_ip", "")
    endpoint = args.endpoint or info.get("endpoint", "")
    port = args.port or info.get("cluster_port", DEFAULT_PORT)

    if not host or not endpoint:
        log("ERROR: Could not determine host or endpoint. "
            "Run aurora_setup.py first or pass --host/--endpoint.")
        sys.exit(1)

    instance_type = info.get("aurora_instance_type", "")

    # --- Fill mode: create background data and exit ---
    if args.fill:
        log(f"Stack: {info.get('stack', 'unknown')}")
        log(f"Aurora endpoint: {endpoint}")
        log(f"EC2 client: {host}")
        doublings, est_gb = compute_fill_params(
            instance_type, args.fill_tables, args.fill_seed_rows,
            args.fill_target_gb)
        fast_fill(host, key_path, endpoint, port, password, args.fill_db,
                  args.fill_tables, args.fill_seed_rows, doublings,
                  args.fill_threads)
        log("Fill done. Run without --fill to start benchmark.")
        return

    # --- Benchmark mode ---
    if args.table_size is None:
        args.table_size = DEFAULT_BENCH_TABLE_SIZE
        total_gb = round(args.tables * args.table_size * BYTES_PER_ROW / 1e9, 1)
        log(f"Benchmark tables: {args.tables} x {args.table_size:,} rows "
            f"(~{total_gb} GB)")

    thread_label = (f"{args.parallel}x{args.threads}"
                    if args.parallel > 1 else str(args.threads))

    log(f"Stack: {info.get('stack', 'unknown')}")
    log(f"Aurora endpoint: {endpoint}")
    log(f"EC2 client: {host}")
    log(f"Workload: {args.workload}, threads: {thread_label}, "
        f"duration: {args.duration}s")
    print()

    lua_path = None
    if args.workload in ("custom_iud", "custom_mixed"):
        lua_path = upload_lua_script(host, key_path, args.workload)

    if not args.skip_prepare:
        create_database(host, key_path, endpoint, port, password, args.db)
        sysbench_prepare(host, key_path, endpoint, port, password, args.db,
                         args.tables, args.table_size, args.prepare_threads)

    iud_rates = None
    if not args.skip_iud_measurement:
        log("Taking InnoDB row counter snapshot (before)...")
        rows_before = get_innodb_rows(host, key_path, endpoint, port, password)

    log("Starting client CPU monitoring (mpstat)...")
    start_mpstat(host, key_path, args.duration)

    bench_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if args.parallel > 1:
        result = run_sysbench_parallel(
            host, key_path, endpoint, port, password, args.db,
            args.workload, args.tables, args.table_size,
            args.threads, args.duration, args.report_interval,
            lua_path, args.parallel)
    else:
        result = run_sysbench(host, key_path, endpoint, port, password, args.db,
                              args.workload, args.tables, args.table_size,
                              args.threads, args.duration, args.report_interval,
                              lua_path)

    if "error" in result:
        log("Benchmark failed.")
        sys.exit(1)

    bench_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log("Collecting client CPU data...")
    client_cpu_pct = stop_and_parse_mpstat(host, key_path)
    if client_cpu_pct is not None:
        log(f"  Client CPU avg: {client_cpu_pct:.1f}%")
    else:
        log("  Could not retrieve client CPU metrics")
    result["client_cpu_avg_pct"] = client_cpu_pct

    if not args.skip_iud_measurement:
        log("Taking InnoDB row counter snapshot (after)...")
        rows_after = get_innodb_rows(host, key_path, endpoint, port, password)
        elapsed = result.get("elapsed_s", args.duration)
        iud_rates = {}
        for op in ["innodb_rows_inserted", "innodb_rows_updated",
                    "innodb_rows_deleted", "innodb_rows_read"]:
            b = rows_before.get(op, 0)
            a = rows_after.get(op, 0)
            iud_rates[op.replace("innodb_rows_", "") + "_per_sec"] = round(
                (a - b) / elapsed, 1)
        total_iud = (iud_rates.get("inserted_per_sec", 0) +
                     iud_rates.get("updated_per_sec", 0) +
                     iud_rates.get("deleted_per_sec", 0))
        iud_rates["total_iud_per_sec"] = round(total_iud, 1)

    cpu_pct = None
    net_metrics = {}
    writer_id = info.get("writer_id", "")
    rds_profile = args.rds_profile or args.aws_profile
    if writer_id:
        log("Fetching Aurora CPU from CloudWatch...")
        cpu_pct = get_aurora_cpu(writer_id, args.region, rds_profile,
                                 bench_start, bench_end)
        if cpu_pct is not None:
            log(f"  Aurora writer CPU avg: {cpu_pct:.1f}%")
        else:
            log("  Could not retrieve CPU metrics (may need 1-2 min to appear)")

        log("Fetching Aurora network metrics from CloudWatch...")
        net_metrics = get_aurora_network_metrics(
            writer_id, args.region, rds_profile, bench_start, bench_end)
        recv = net_metrics.get("aurora_net_recv_mbps")
        xmit = net_metrics.get("aurora_net_xmit_mbps")
        if recv is not None:
            log(f"  Network recv: {recv:.1f} Mbps")
        if xmit is not None:
            log(f"  Network xmit: {xmit:.1f} Mbps")

    if args.parallel <= 1:
        result["thread_label"] = str(args.threads)
        result["parallel"] = 1

    print_results(result, iud_rates, cpu_pct, args.tables, args.table_size,
                  client_cpu_pct, net_metrics)
    save_results(result, iud_rates, script_dir, args.workload,
                 state=info, tables=args.tables, table_size=args.table_size,
                 cpu_pct=cpu_pct, client_cpu_pct=client_cpu_pct,
                 net_metrics=net_metrics, parallel=args.parallel)

    if not args.skip_cleanup:
        sysbench_cleanup(host, key_path, endpoint, port, password, args.db,
                         args.tables, args.table_size)

    log("Done.")


if __name__ == "__main__":
    main()
