"""Aurora-specific helpers for the unified benchmark runner.

Extracted from aurora/benchmark.py.  Contains stack discovery, CloudWatch
metric retrieval, InnoDB counter measurement, and results display/save.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from common.util import ts, log
from common.ssh import ssh_run_simple
import common.benchmark as cb

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
DEFAULT_TABLE_SIZE = None
DEFAULT_REPORT_INTERVAL = 10
DEFAULT_PREPARE_THREADS = 32

DEFAULT_FILL_DB = "sbfill"
DEFAULT_FILL_TABLES = 256
DEFAULT_FILL_SEED_ROWS = 500_000
DEFAULT_FILL_THREADS = 64
DEFAULT_BENCH_TABLE_SIZE = 10_000_000

PRODUCTION_BASELINE = cb.PRODUCTION_BASELINE

VERBOSE = False

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
# Stack discovery (Aurora-specific)
# ---------------------------------------------------------------------------


def discover_stack(script_dir: Path, region: str, profile: str,
                   seed: str) -> dict:
    state = load_state(script_dir)
    if state and state.get("endpoint"):
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
# CloudWatch metrics (Aurora-specific -- uses --rds-profile)
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
            metrics[key] = round(avg_bytes * 8 / 1_000_000, 1)
        except Exception:
            metrics[key] = None
    return metrics


# ---------------------------------------------------------------------------
# InnoDB row counters (Aurora-specific measurement via SSH)
# ---------------------------------------------------------------------------


def get_innodb_rows(host: str, key_path: str, endpoint: str,
                    port: int, password: str) -> dict:
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e \"SHOW GLOBAL STATUS LIKE 'Innodb_rows_%'\" "
        f"--batch --skip-column-names 2>&1"
    )
    r = ssh_run_simple(host, key_path, script, timeout=30)
    rows = {}
    for line in r.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            rows[parts[0].lower()] = int(parts[1])
    return rows


# ---------------------------------------------------------------------------
# Results display (Aurora-specific -- production baseline comparison)
# ---------------------------------------------------------------------------


def print_results(result: dict, iud_rates: dict | None = None,
                   cpu_pct: float | None = None,
                   tables: int = 0, table_size: int = 0,
                   client_cpu_pct: float | None = None,
                   net_metrics: dict | None = None) -> None:
    total_rows = tables * table_size
    est_gb = round(total_rows * cb.BYTES_PER_ROW / 1e9, 1) if total_rows else 0
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
    p95 = result.get("latency_p95_ms") or result.get("latency_95th_pct_ms", 0)
    print(f"    P95:            {p95:.2f} ms")

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
    est_gb = round(total_rows * cb.BYTES_PER_ROW / 1e9, 1) if total_rows else 0

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
