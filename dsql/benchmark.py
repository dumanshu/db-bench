#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
DSQL Benchmark Runner

Runs pgbench workloads against an Amazon Aurora DSQL cluster and collects
client-side metrics (TPS, latency) alongside AWS CloudWatch server-side
metrics (DPU, transactions, conflicts, storage).

Usage:
    python3 -m dsql.benchmark --seed dsqllt-001 --profile standard
    python3 -m dsql.benchmark --seed dsqllt-001 --profile quick --scale-factor 10
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import botocore

# ---------------------------------------------------------------------------
DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_SEED = "dsqllt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_DB_PROFILE = os.environ.get("DB_PROFILE", "sandbox-storage")
DSQL_PORT = 5432

BOTO_CONFIG = botocore.config.Config(retries={"max_attempts": 6, "mode": "adaptive"})

STATE_FILE = Path(__file__).resolve().with_name("dsql-state.json")
SSH_KEY_PATH = Path(__file__).resolve().parent.parent / "tidb" / "tidb-load-test-key.pem"

TOKEN_LIFETIME_SECONDS = 900
TOKEN_REFRESH_MARGIN = 120  # refresh 2min before expiry

# ---------------------------------------------------------------------------
# Workload profiles
# ---------------------------------------------------------------------------

WORKLOAD_PROFILES = {
    "quick": {
        "description": "Smoke test (1min, 4 clients)",
        "scale_factor": 10,
        "clients": 4,
        "threads": 2,
        "duration": 60,
        "report_interval": 5,
    },
    "light": {
        "description": "Light benchmark (5min, 16 clients)",
        "scale_factor": 50,
        "clients": 16,
        "threads": 4,
        "duration": 300,
        "report_interval": 10,
    },
    "standard": {
        "description": "Standard benchmark (15min, 32 clients)",
        "scale_factor": 100,
        "clients": 32,
        "threads": 8,
        "duration": 900,
        "report_interval": 30,
    },
    "heavy": {
        "description": "Heavy benchmark (30min, 64 clients)",
        "scale_factor": 200,
        "clients": 64,
        "threads": 16,
        "duration": 1800,
        "report_interval": 60,
    },
    "stress": {
        "description": "Stress test (60min, 128 clients)",
        "scale_factor": 500,
        "clients": 128,
        "threads": 32,
        "duration": 3600,
        "report_interval": 60,
    },
}


# ---------------------------------------------------------------------------
# Self-contained helpers (matching tidb/benchmark.py pattern)
# ---------------------------------------------------------------------------

def ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg=""):
    print(f"[{ts()}] {msg}", flush=True)


def ec2_client(region=None, profile=None):
    session = boto3.Session(
        region_name=region or DEFAULT_REGION,
        profile_name=profile,
    )
    return session.client("ec2", config=BOTO_CONFIG)


def dsql_client(region=None, profile=None):
    session = boto3.Session(
        region_name=region or DEFAULT_REGION,
        profile_name=profile,
    )
    return session.client("dsql", config=BOTO_CONFIG)


def cw_client(region=None, profile=None):
    session = boto3.Session(
        region_name=region or DEFAULT_REGION,
        profile_name=profile,
    )
    return session.client("cloudwatch", config=BOTO_CONFIG)


def ssh_run(host_ip, script, key_path, strict=True):
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-i", str(key_path),
        f"ec2-user@{host_ip}",
        "bash -s",
    ]
    proc = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=7200)
    if strict and proc.returncode != 0:
        log(f"SSH ERROR (rc={proc.returncode})")
        if proc.stderr:
            log(f"  stderr: {proc.stderr[:500]}")
        raise RuntimeError(f"SSH command failed (rc={proc.returncode})")
    return proc


def ssh_stream(host_ip, key_path):
    """Open an SSH Popen to stream output. Caller writes script to stdin."""
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-i", str(key_path),
        f"ec2-user@{host_ip}",
        "bash -s",
    ]
    return subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )


# ---------------------------------------------------------------------------
# State & discovery
# ---------------------------------------------------------------------------

def load_state():
    if not STATE_FILE.exists():
        raise SystemExit(
            f"ERROR: State file not found: {STATE_FILE}\n"
            "Run setup first: python3 -m dsql.setup"
        )
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError) as e:
        raise SystemExit(f"ERROR: Invalid state file: {e}")


def discover_client_host(state, ec2c):
    instance_id = state.get("client_instance_id")
    if not instance_id:
        raise SystemExit("ERROR: No client_instance_id in state file.")

    resp = ec2c.describe_instances(InstanceIds=[instance_id])
    for res in resp["Reservations"]:
        for inst in res["Instances"]:
            if inst["State"]["Name"] == "running":
                return inst.get("PublicIpAddress") or inst.get("PrivateIpAddress")

    raise SystemExit(f"ERROR: Client instance {instance_id} is not running.")


# ---------------------------------------------------------------------------
# IAM auth token management
# ---------------------------------------------------------------------------

@dataclass
class TokenManager:
    endpoint: str
    region: str
    profile: str = None
    _token: str = ""
    _expires_at: float = 0.0

    def get_token(self):
        now = time.time()
        if self._token and now < (self._expires_at - TOKEN_REFRESH_MARGIN):
            return self._token
        self._refresh()
        return self._token

    def _refresh(self):
        client = dsql_client(self.region, self.profile)
        self._token = client.generate_db_connect_admin_auth_token(
            Hostname=self.endpoint,
            Region=self.region,
            ExpiresIn=TOKEN_LIFETIME_SECONDS,
        )
        self._expires_at = time.time() + TOKEN_LIFETIME_SECONDS
        log(f"Refreshed DSQL auth token (expires in {TOKEN_LIFETIME_SECONDS}s)")


# ---------------------------------------------------------------------------
# pgbench operations
# ---------------------------------------------------------------------------

def pgbench_init(host_ip, key_path, endpoint, token, scale_factor):
    log(f"Initializing pgbench data (scale factor: {scale_factor})...")

    # DSQL rejects fillfactor, TRUNCATE, and caps writes at ~3500 rows/txn
    script = textwrap.dedent(f"""\
        export PGPASSWORD='{token}'
        PSQL="psql -h {endpoint} -p {DSQL_PORT} -U admin -d postgres -v ON_ERROR_STOP=1"
        BATCH=2000
        TOTAL={scale_factor * 100000}

        $PSQL <<'EOSQL'
DROP TABLE IF EXISTS pgbench_history, pgbench_tellers, pgbench_accounts, pgbench_branches;
CREATE TABLE pgbench_branches (bid int NOT NULL PRIMARY KEY, bbalance int, filler char(88));
CREATE TABLE pgbench_tellers  (tid int NOT NULL PRIMARY KEY, bid int, tbalance int, filler char(84));
CREATE TABLE pgbench_accounts (aid int NOT NULL PRIMARY KEY, bid int, abalance int, filler char(84));
CREATE TABLE pgbench_history  (tid int, bid int, aid int, delta int, mtime timestamp, filler char(22));
EOSQL
        if [ $? -ne 0 ]; then echo "PGBENCH_INIT_EXIT:1"; exit 0; fi
        $PSQL -c "INSERT INTO pgbench_branches SELECT g, 0, '' FROM generate_series(1, {scale_factor}) g"
        $PSQL -c "INSERT INTO pgbench_tellers  SELECT g, (g-1)/10+1, 0, '' FROM generate_series(1, {scale_factor * 10}) g"
        if [ $? -ne 0 ]; then echo "PGBENCH_INIT_EXIT:1"; exit 0; fi

        CHUNKS=$(( ($TOTAL + $BATCH - 1) / $BATCH ))
        echo "Loading $TOTAL accounts in $CHUNKS batches of $BATCH..."
        for i in $(seq 0 $(($CHUNKS - 1))); do
            s=$(($i * $BATCH + 1))
            e=$((($i + 1) * $BATCH))
            if [ $e -gt $TOTAL ]; then e=$TOTAL; fi
            echo "INSERT INTO pgbench_accounts SELECT g, (g-1)/100000+1, 0, '' FROM generate_series($s, $e) g;"
        done | $PSQL 2>&1
        RC=${{PIPESTATUS[1]:-$?}}

        if [ $RC -ne 0 ]; then echo "PGBENCH_INIT_EXIT:1"; exit 0; fi
        echo "PGBENCH_INIT_EXIT:0"
    """)
    proc = ssh_run(host_ip, script, key_path, strict=False)
    output = proc.stdout + proc.stderr
    if "PGBENCH_INIT_EXIT:0" in output:
        log("pgbench init complete.")
        return True
    else:
        log(f"pgbench init failed:\n{output[-1000:]}")
        return False


def pgbench_run(host_ip, key_path, endpoint, token, profile_cfg, segment_label=""):
    clients = profile_cfg["clients"]
    threads = profile_cfg["threads"]
    duration = profile_cfg["duration"]
    report_interval = profile_cfg["report_interval"]
    max_tries = profile_cfg.get("max_tries", 3)

    label = f" [{segment_label}]" if segment_label else ""
    log(f"Running pgbench{label}: {clients}C / {threads}T / {duration}s / max_tries={max_tries}")

    script = textwrap.dedent(f"""\
        export PGPASSWORD='{token}'
        pgbench -h {endpoint} -p {DSQL_PORT} -U admin -d postgres \\
            -c {clients} -j {threads} -T {duration} \\
            -P {report_interval} -n -r --max-tries={max_tries} 2>&1
        echo "PGBENCH_RUN_EXIT:$?"
    """)

    proc = ssh_stream(host_ip, key_path)
    proc.stdin.write(script)
    proc.stdin.close()

    progress_lines = []
    full_output = []

    for line in proc.stdout:
        line = line.rstrip()
        full_output.append(line)
        if line.startswith("progress:"):
            progress_lines.append(line)
            log(f"  {line}")

    proc.wait()
    output = "\n".join(full_output)
    return output, progress_lines


def parse_pgbench_output(output):
    result = {}

    tps_match = re.search(r"tps\s*=\s*([\d.]+)\s*\(without initial connection time\)", output)
    if tps_match:
        result["tps"] = float(tps_match.group(1))

    tps_incl = re.search(r"tps\s*=\s*([\d.]+)\s*\(including", output)
    if tps_incl:
        result["tps_including_conn"] = float(tps_incl.group(1))

    lat_avg = re.search(r"latency average\s*=\s*([\d.]+)\s*ms", output)
    if lat_avg:
        result["latency_avg_ms"] = float(lat_avg.group(1))

    lat_std = re.search(r"latency stddev\s*=\s*([\d.]+)\s*ms", output)
    if lat_std:
        result["latency_stddev_ms"] = float(lat_std.group(1))

    init_conn = re.search(r"initial connection time\s*=\s*([\d.]+)\s*ms", output)
    if init_conn:
        result["initial_connection_time_ms"] = float(init_conn.group(1))

    tx_match = re.search(r"number of transactions actually processed:\s*(\d+)", output)
    if tx_match:
        result["transactions_processed"] = int(tx_match.group(1))

    failed = re.search(r"number of failed transactions:\s*(\d+)", output)
    if failed:
        result["transactions_failed"] = int(failed.group(1))

    retried = re.search(r"number of transactions retried:\s*(\d+)", output)
    if retried:
        result["transactions_retried"] = int(retried.group(1))

    retries = re.search(r"total number of retries:\s*(\d+)", output)
    if retries:
        result["total_retries"] = int(retries.group(1))

    return result


def parse_progress_lines(lines):
    data_points = []
    for line in lines:
        match = re.match(
            r"progress:\s+([\d.]+)\s+s,\s+([\d.]+)\s+tps,\s+lat\s+([\d.]+)\s+ms\s+stddev\s+([\d.]+)",
            line,
        )
        if match:
            data_points.append({
                "elapsed_s": float(match.group(1)),
                "tps": float(match.group(2)),
                "lat_ms": float(match.group(3)),
                "stddev_ms": float(match.group(4)),
            })
    return data_points


# ---------------------------------------------------------------------------
# CloudWatch metrics
# ---------------------------------------------------------------------------

DSQL_METRICS = [
    ("TotalTransactions", "Sum"),
    ("ReadOnlyTransactions", "Sum"),
    ("CommitLatency", "Average"),
    ("CommitLatency", "p99", "ExtendedStatistics"),
    ("OccConflicts", "Sum"),
    ("QueryTimeouts", "Sum"),
    ("BytesRead", "Sum"),
    ("BytesWritten", "Sum"),
    ("ComputeTime", "Sum"),
    ("WriteDPU", "Sum"),
    ("ReadDPU", "Sum"),
    ("ComputeDPU", "Sum"),
    ("TotalDPU", "Sum"),
    ("ClusterStorageSize", "Average"),
]


def collect_cloudwatch_metrics(cluster_id, start_time, end_time, region, profile=None):
    cw = cw_client(region, profile)
    duration_seconds = int((end_time - start_time).total_seconds())
    period = max(60, (duration_seconds // 60) * 60)  # at least 60s, aligned to minutes

    results = {}
    for metric_spec in DSQL_METRICS:
        metric_name = metric_spec[0]
        stat = metric_spec[1]
        is_extended = len(metric_spec) > 2 and metric_spec[2] == "ExtendedStatistics"

        try:
            kwargs = {
                "Namespace": "AWS/DSQL",
                "MetricName": metric_name,
                "Dimensions": [{"Name": "ResourceId", "Value": cluster_id}],
                "StartTime": start_time,
                "EndTime": end_time,
                "Period": period,
            }

            if is_extended:
                kwargs["ExtendedStatistics"] = [stat]
            else:
                kwargs["Statistics"] = [stat]

            resp = cw.get_metric_statistics(**kwargs)
            datapoints = resp.get("Datapoints", [])

            if datapoints:
                if is_extended:
                    values = [dp["ExtendedStatistics"].get(stat, 0) for dp in datapoints]
                else:
                    values = [dp[stat] for dp in datapoints]
                key = f"{metric_name}_{stat}"
                results[key] = {
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "count": len(values),
                }
        except Exception as e:
            log(f"  Warning: Could not fetch {metric_name}/{stat}: {e}")

    return results


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

DSQL_PRICING = {
    "write_dpu_per_million": 2.25,
    "read_dpu_per_million": 0.45,
    "storage_gb_month": 0.25,
    "io_per_million": 0.20,
}


def estimate_cost(cw_metrics, duration_hours):
    write_dpu = cw_metrics.get("WriteDPU_Sum", {}).get("sum", 0)
    read_dpu = cw_metrics.get("ReadDPU_Sum", {}).get("sum", 0)
    storage_gb = cw_metrics.get("ClusterStorageSize_Average", {}).get("avg", 0) / (1024**3)

    write_cost = (write_dpu / 1_000_000) * DSQL_PRICING["write_dpu_per_million"]
    read_cost = (read_dpu / 1_000_000) * DSQL_PRICING["read_dpu_per_million"]
    storage_cost = storage_gb * DSQL_PRICING["storage_gb_month"] * (duration_hours / 720)

    total = write_cost + read_cost + storage_cost
    return {
        "write_dpu_total": write_dpu,
        "read_dpu_total": read_dpu,
        "write_cost_usd": round(write_cost, 4),
        "read_cost_usd": round(read_cost, 4),
        "storage_cost_usd": round(storage_cost, 4),
        "total_estimated_usd": round(total, 4),
        "storage_gb": round(storage_gb, 4),
        "duration_hours": round(duration_hours, 4),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    profiles_help = "\n".join(f"  {k:12s} {v['description']}" for k, v in WORKLOAD_PROFILES.items())
    parser = argparse.ArgumentParser(
        description="Run pgbench benchmark against DSQL cluster.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Workload profiles:\n{profiles_help}",
    )
    parser.add_argument("--seed", default=DEFAULT_SEED)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--aws-profile", default=DEFAULT_PROFILE,
                        help="AWS profile for infrastructure (EC2/VPC).")
    parser.add_argument("--db-profile", default=DEFAULT_DB_PROFILE,
                        help="AWS profile for database service APIs (default: sandbox-storage).")
    parser.add_argument(
        "--profile", default="standard", choices=WORKLOAD_PROFILES.keys(),
        help="Workload profile (default: standard).",
    )
    parser.add_argument("--scale-factor", type=int, help="Override scale factor from profile.")
    parser.add_argument("--clients", type=int, help="Override client count from profile.")
    parser.add_argument("--threads", type=int, help="Override thread count from profile.")
    parser.add_argument("--duration", type=int, help="Override duration (seconds) from profile.")
    parser.add_argument("--skip-init", action="store_true", help="Skip pgbench init (reuse data).")
    parser.add_argument("--skip-cloudwatch", action="store_true", help="Skip CloudWatch metric collection.")
    parser.add_argument("--output", help="Output JSON file path (default: dsql/dsql-results-<ts>.json).")
    parser.add_argument(
        "--ssh-key", default=str(SSH_KEY_PATH),
        help=f"SSH private key path (default: {SSH_KEY_PATH}).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    state = load_state()
    cluster_id = state["cluster_id"]
    endpoint = state["endpoint"]
    region = args.region

    log("=" * 70)
    log("DSQL Benchmark")
    log("=" * 70)
    log(f"  Cluster:   {cluster_id}")
    log(f"  Endpoint:  {endpoint}")
    log(f"  Region:    {region}")

    ec2c = ec2_client(region, args.aws_profile)
    client_ip = discover_client_host(state, ec2c)
    log(f"  Client VM: {client_ip}")

    key_path = Path(args.ssh_key).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")

    profile_cfg = dict(WORKLOAD_PROFILES[args.profile])
    if args.scale_factor:
        profile_cfg["scale_factor"] = args.scale_factor
    if args.clients:
        profile_cfg["clients"] = args.clients
    if args.threads:
        profile_cfg["threads"] = args.threads
    if args.duration:
        profile_cfg["duration"] = args.duration

    log(f"  Profile:   {args.profile} -- {profile_cfg['description']}")
    log(f"  Scale:     {profile_cfg['scale_factor']}")
    log(f"  Clients:   {profile_cfg['clients']}")
    log(f"  Threads:   {profile_cfg['threads']}")
    log(f"  Duration:  {profile_cfg['duration']}s")
    log("")

    db_prof = args.db_profile or args.aws_profile
    token_mgr = TokenManager(endpoint=endpoint, region=region, profile=db_prof)

    # ── pgbench init ─────────────────────────────────────────────────────
    if not args.skip_init:
        log("=" * 70)
        log("PHASE 1: pgbench init")
        log("=" * 70)
        token = token_mgr.get_token()
        if not pgbench_init(client_ip, key_path, endpoint, token, profile_cfg["scale_factor"]):
            raise SystemExit("ERROR: pgbench init failed.")
    else:
        log("Skipping pgbench init (--skip-init).")

    # ── pgbench run ──────────────────────────────────────────────────────
    log("")
    log("=" * 70)
    log("PHASE 2: pgbench run")
    log("=" * 70)

    total_duration = profile_cfg["duration"]
    segment_max = TOKEN_LIFETIME_SECONDS - TOKEN_REFRESH_MARGIN
    all_output = []
    all_progress = []
    bench_start = datetime.datetime.now(datetime.timezone.utc)

    if total_duration <= segment_max:
        token = token_mgr.get_token()
        output, progress = pgbench_run(
            client_ip, key_path, endpoint, token, profile_cfg,
        )
        all_output.append(output)
        all_progress.extend(progress)
    else:
        remaining = total_duration
        segment_num = 0
        while remaining > 0:
            segment_num += 1
            seg_duration = min(remaining, segment_max)
            seg_cfg = dict(profile_cfg, duration=seg_duration)
            token = token_mgr.get_token()
            output, progress = pgbench_run(
                client_ip, key_path, endpoint, token, seg_cfg,
                segment_label=f"segment {segment_num}, {seg_duration}s",
            )
            all_output.append(output)
            all_progress.extend(progress)
            remaining -= seg_duration

    bench_end = datetime.datetime.now(datetime.timezone.utc)
    bench_duration = (bench_end - bench_start).total_seconds()

    # ── Parse results ────────────────────────────────────────────────────
    log("")
    log("=" * 70)
    log("PHASE 3: Results")
    log("=" * 70)

    combined_output = "\n".join(all_output)
    client_metrics = parse_pgbench_output(combined_output)
    time_series = parse_progress_lines(all_progress)

    log(f"  TPS:              {client_metrics.get('tps', 'N/A')}")
    log(f"  Latency avg:      {client_metrics.get('latency_avg_ms', 'N/A')} ms")
    log(f"  Latency stddev:   {client_metrics.get('latency_stddev_ms', 'N/A')} ms")
    log(f"  Transactions:     {client_metrics.get('transactions_processed', 'N/A')}")
    log(f"  Failed:           {client_metrics.get('transactions_failed', 'N/A')}")
    log(f"  Retried:          {client_metrics.get('transactions_retried', 'N/A')}")
    log(f"  Total retries:    {client_metrics.get('total_retries', 'N/A')}")

    # ── CloudWatch metrics ───────────────────────────────────────────────
    cw_metrics = {}
    cost_estimate = {}
    if not args.skip_cloudwatch:
        log("")
        log("Collecting CloudWatch metrics...")
        cw_end = bench_end + datetime.timedelta(minutes=2)
        cw_metrics = collect_cloudwatch_metrics(
            cluster_id, bench_start, cw_end, region, db_prof,
        )
        if cw_metrics:
            for k, v in sorted(cw_metrics.items()):
                log(f"  {k}: avg={v['avg']:.2f}, sum={v['sum']:.2f}")

        duration_hours = bench_duration / 3600
        cost_estimate = estimate_cost(cw_metrics, duration_hours)
        log(f"\n  Estimated cost: ${cost_estimate['total_estimated_usd']:.4f}")
        log(f"    Write DPU: {cost_estimate['write_dpu_total']:,.0f} (${cost_estimate['write_cost_usd']:.4f})")
        log(f"    Read DPU:  {cost_estimate['read_dpu_total']:,.0f} (${cost_estimate['read_cost_usd']:.4f})")

    # ── Save results ─────────────────────────────────────────────────────
    result = {
        "benchmark": "dsql-pgbench",
        "timestamp": ts(),
        "cluster_id": cluster_id,
        "endpoint": endpoint,
        "region": region,
        "profile": args.profile,
        "profile_config": profile_cfg,
        "duration_seconds": bench_duration,
        "client_metrics": client_metrics,
        "time_series": time_series,
        "cloudwatch_metrics": cw_metrics,
        "cost_estimate": cost_estimate,
        "raw_output_length": len(combined_output),
    }

    output_path = args.output or str(
        Path(__file__).resolve().with_name(
            f"dsql-results-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        )
    )
    Path(output_path).write_text(json.dumps(result, indent=2, default=str))
    log(f"\nResults saved to: {output_path}")

    log("")
    log("=" * 70)
    log("BENCHMARK COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
