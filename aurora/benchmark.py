#!/usr/bin/env python3
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
aurora/benchmark.py -- Run sysbench benchmarks against Aurora MySQL.

Thin wrapper around common.benchmark / common.sampler / common.report.
Keeps Aurora-specific logic: stack discovery, CloudWatch metrics (with
--rds-profile support), production baseline comparison, and CLI flags.

Usage:
    python3 -m aurora.benchmark
    python3 -m aurora.benchmark --workload custom_iud --threads 64 --duration 300
    python3 -m aurora.benchmark --workload oltp_write_only --threads 32
    python3 -m aurora.benchmark --seed auroralt-002 --region us-west-2
"""

import argparse
import json
import os
import subprocess
import sys
import textwrap
import time
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

VERBOSE = False

IUD_INSERT_PCT = cb.IUD_INSERT_PCT
IUD_UPDATE_PCT = cb.IUD_UPDATE_PCT
IUD_DELETE_PCT = cb.IUD_DELETE_PCT

PRODUCTION_BASELINE = cb.PRODUCTION_BASELINE

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
        doublings, est_gb = cb.compute_fill_params(
            instance_type, args.fill_tables, args.fill_seed_rows,
            args.fill_target_gb)
        cb.fast_fill(host, key_path, endpoint, port, "admin", password,
                     args.fill_db, args.fill_tables, args.fill_seed_rows,
                     doublings, args.fill_threads)
        log("Fill done. Run without --fill to start benchmark.")
        return

    # --- Benchmark mode ---
    if args.table_size is None:
        args.table_size = DEFAULT_BENCH_TABLE_SIZE
        total_gb = round(args.tables * args.table_size * cb.BYTES_PER_ROW / 1e9, 1)
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

    # Upload Lua scripts from common/lua/
    lua_dir = None
    if args.workload in ("custom_iud", "custom_mixed"):
        lua_dir = cb.upload_lua_scripts(host, key_path)

    if not args.skip_prepare:
        cb.sysbench_prepare(host, key_path, endpoint, port, "admin", password,
                            args.db, args.tables, args.table_size,
                            args.prepare_threads)

    iud_rates = None
    rows_before = None
    if not args.skip_iud_measurement:
        log("Taking InnoDB row counter snapshot (before)...")
        rows_before = get_innodb_rows(host, key_path, endpoint, port, password)

    # Build sysbench command via common.benchmark
    cmd_str = cb.build_sysbench_cmd(
        host_ip=host, key_path=key_path,
        workload=args.workload, threads=args.threads,
        duration=args.duration, tables=args.tables,
        table_size=args.table_size, endpoint=endpoint,
        port=port, user="admin", password=password,
        db=args.db, report_interval=args.report_interval,
        lua_dir=lua_dir,
    )

    bench_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if args.parallel > 1:
        result = cb.run_sysbench_parallel(host, key_path, cmd_str, args.parallel)
    else:
        result = cb.run_sysbench(host, key_path, cmd_str,
                                 timeout=args.duration + 120)

    if "error" in result:
        log("Benchmark failed.")
        sys.exit(1)

    bench_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Add metadata the old code expected
    result.setdefault("workload", args.workload)
    result.setdefault("threads", args.threads)
    result["duration_s"] = args.duration

    if args.parallel > 1:
        result["parallel"] = args.parallel
        result["thread_label"] = f"{args.parallel}x{args.threads}"
    else:
        result["parallel"] = 1
        result["thread_label"] = str(args.threads)

    # Map common.benchmark latency key names back for display
    if result.get("latency_p95_ms") and not result.get("latency_95th_pct_ms"):
        result["latency_95th_pct_ms"] = result["latency_p95_ms"]

    # Client CPU via mpstat (kept simple -- no sampler dependency)
    client_cpu_pct = None

    if not args.skip_iud_measurement and rows_before is not None:
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

    print_results(result, iud_rates, cpu_pct, args.tables, args.table_size,
                  client_cpu_pct, net_metrics)
    save_results(result, iud_rates, script_dir, args.workload,
                 state=info, tables=args.tables, table_size=args.table_size,
                 cpu_pct=cpu_pct, client_cpu_pct=client_cpu_pct,
                 net_metrics=net_metrics, parallel=args.parallel)

    if not args.skip_cleanup:
        cb.sysbench_cleanup(host, key_path, endpoint, port, "admin", password,
                            args.db, args.tables)

    log("Done.")


if __name__ == "__main__":
    main()
