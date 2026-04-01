#!/usr/bin/env python3
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
aurora/validate.py -- Validation / health-check for the Aurora MySQL benchmark stack.

Discovers the Aurora cluster and EC2 client instance via AWS APIs and tags,
then runs a series of checks to confirm the stack is healthy and ready for
benchmarking.  Exits 0 when every check passes, 1 otherwise.

Usage:
    python3 -m aurora.validate
    python3 -m aurora.validate --seed auroralt-002 --region us-west-2
    python3 -m aurora.validate --skip-benchmark -v
"""

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from common.util import ts, log
from common.ssh import ssh_run_simple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "auroralt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_PORT = 3306
from common.aws import KEY_NAME, DEFAULT_SSH_KEY_PATH
STACK_PREFIX = "aurora-bench"

BOTO_CONFIG = {
    "region": DEFAULT_REGION,
    "profile": DEFAULT_PROFILE,
}

VERBOSE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _aws(service: str, action: str, region: str, profile: str,
         extra: list[str] | None = None) -> dict:
    """Run an AWS CLI command, return parsed JSON."""
    cmd = ["aws", service, action, "--region", region, "--profile", profile,
           "--output", "json"]
    if extra:
        cmd.extend(extra)
    if VERBOSE:
        log(f"  aws cmd: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"aws {service} {action} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def discover_aurora_stack(region: str, profile: str, seed: str) -> dict:
    """Discover Aurora cluster, writer instance, and EC2 client from AWS."""
    stack = f"{STACK_PREFIX}-{seed}"
    info: dict = {"seed": seed, "stack": stack}

    # -- EC2 client instance (by Project tag) --------------------------------
    ec2_resp = _aws("ec2", "describe-instances", region, profile, [
        "--filters",
        f"Name=tag:Project,Values={stack}",
        "Name=instance-state-name,Values=running,stopped,pending",
    ])
    reservations = ec2_resp.get("Reservations", [])
    if reservations:
        inst = reservations[0]["Instances"][0]
        info["client_instance_id"] = inst["InstanceId"]
        info["client_public_ip"] = inst.get("PublicIpAddress", "")
        info["client_instance_type"] = inst.get("InstanceType", "")
        info["client_status"] = inst["State"]["Name"]
    else:
        info["client_instance_id"] = ""
        info["client_public_ip"] = ""
        info["client_instance_type"] = ""
        info["client_status"] = "not-found"

    # -- RDS Aurora cluster ---------------------------------------------------
    cluster_id = f"{stack}-cluster"
    try:
        rds_resp = _aws("rds", "describe-db-clusters", region, profile, [
            "--db-cluster-identifier", cluster_id,
        ])
        clusters = rds_resp.get("DBClusters", [])
        if clusters:
            c = clusters[0]
            info["cluster_endpoint"] = c.get("Endpoint", "")
            info["cluster_port"] = c.get("Port", DEFAULT_PORT)
            info["cluster_status"] = c.get("Status", "unknown")
            info["cluster_storage_type"] = c.get("StorageType", "")
            members = c.get("DBClusterMembers", [])
            writer_id = ""
            for m in members:
                if m.get("IsClusterWriter"):
                    writer_id = m.get("DBInstanceIdentifier", "")
                    break
            info["writer_instance_id"] = writer_id
        else:
            info["cluster_endpoint"] = ""
            info["cluster_port"] = DEFAULT_PORT
            info["cluster_status"] = "not-found"
            info["cluster_storage_type"] = ""
            info["writer_instance_id"] = ""
    except RuntimeError:
        info["cluster_endpoint"] = ""
        info["cluster_port"] = DEFAULT_PORT
        info["cluster_status"] = "not-found"
        info["cluster_storage_type"] = ""
        info["writer_instance_id"] = ""

    # -- RDS writer instance --------------------------------------------------
    if info.get("writer_instance_id"):
        try:
            inst_resp = _aws("rds", "describe-db-instances", region, profile, [
                "--db-instance-identifier", info["writer_instance_id"],
            ])
            instances = inst_resp.get("DBInstances", [])
            if instances:
                di = instances[0]
                info["writer_instance_type"] = di.get("DBInstanceClass", "")
                info["writer_status"] = di.get("DBInstanceStatus", "unknown")
            else:
                info["writer_instance_type"] = ""
                info["writer_status"] = "not-found"
        except RuntimeError:
            info["writer_instance_type"] = ""
            info["writer_status"] = "not-found"
    else:
        info["writer_instance_type"] = ""
        info["writer_status"] = "not-found"

    return info


def get_ebs_volumes(region: str, profile: str, instance_id: str) -> list[dict]:
    """Return EBS volume details attached to an EC2 instance."""
    if not instance_id:
        return []
    resp = _aws("ec2", "describe-volumes", region, profile, [
        "--filters", f"Name=attachment.instance-id,Values={instance_id}",
    ])
    volumes = []
    for v in resp.get("Volumes", []):
        volumes.append({
            "volume_id": v.get("VolumeId", ""),
            "size_gb": v.get("Size", 0),
            "volume_type": v.get("VolumeType", ""),
            "iops": v.get("Iops", 0),
            "throughput": v.get("Throughput", 0),
            "state": v.get("State", ""),
        })
    return volumes


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------
def _ssh_capture(host: str, script: str, key_path: str) -> subprocess.CompletedProcess:
    """Run *script* on *host* via SSH; return CompletedProcess."""
    if VERBOSE:
        log(f"  ssh -> {host}: {script[:120]}...")
    return ssh_run_simple(host, key_path, script, timeout=120)


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------
def _result(name: str, ok: bool) -> bool:
    tag = "PASS" if ok else "FAIL"
    log(f"  [{tag}] {name}")
    return ok


def check_ssh_connectivity(host: str, key_path: str) -> bool:
    """Verify SSH access to the EC2 client."""
    try:
        r = _ssh_capture(host, "echo ok", key_path)
        return _result("SSH Connectivity", r.returncode == 0 and "ok" in r.stdout)
    except Exception as exc:
        if VERBOSE:
            log(f"    SSH error: {exc}")
        return _result("SSH Connectivity", False)


def check_aurora_cluster_status(region: str, profile: str, seed: str) -> bool:
    """Aurora cluster status == available."""
    stack = f"{STACK_PREFIX}-{seed}"
    cluster_id = f"{stack}-cluster"
    try:
        resp = _aws("rds", "describe-db-clusters", region, profile, [
            "--db-cluster-identifier", cluster_id,
        ])
        clusters = resp.get("DBClusters", [])
        if not clusters:
            return _result("Aurora Cluster Status", False)
        status = clusters[0].get("Status", "")
        if VERBOSE:
            log(f"    cluster status: {status}")
        return _result("Aurora Cluster Status", status == "available")
    except Exception as exc:
        if VERBOSE:
            log(f"    cluster error: {exc}")
        return _result("Aurora Cluster Status", False)


def check_aurora_writer_status(region: str, profile: str, seed: str) -> bool:
    """Aurora writer instance status == available."""
    stack = f"{STACK_PREFIX}-{seed}"
    cluster_id = f"{stack}-cluster"
    try:
        resp = _aws("rds", "describe-db-clusters", region, profile, [
            "--db-cluster-identifier", cluster_id,
        ])
        clusters = resp.get("DBClusters", [])
        if not clusters:
            return _result("Aurora Writer Status", False)
        writer_id = ""
        for m in clusters[0].get("DBClusterMembers", []):
            if m.get("IsClusterWriter"):
                writer_id = m.get("DBInstanceIdentifier", "")
                break
        if not writer_id:
            return _result("Aurora Writer Status", False)
        inst_resp = _aws("rds", "describe-db-instances", region, profile, [
            "--db-instance-identifier", writer_id,
        ])
        instances = inst_resp.get("DBInstances", [])
        if not instances:
            return _result("Aurora Writer Status", False)
        status = instances[0].get("DBInstanceStatus", "")
        if VERBOSE:
            log(f"    writer status: {status}")
        return _result("Aurora Writer Status", status == "available")
    except Exception as exc:
        if VERBOSE:
            log(f"    writer error: {exc}")
        return _result("Aurora Writer Status", False)


def check_aurora_io_optimized(region: str, profile: str, seed: str) -> bool:
    """Verify Aurora cluster uses IO-Optimized storage (aurora-iopt1)."""
    stack = f"{STACK_PREFIX}-{seed}"
    cluster_id = f"{stack}-cluster"
    try:
        resp = _aws("rds", "describe-db-clusters", region, profile, [
            "--db-cluster-identifier", cluster_id,
        ])
        clusters = resp.get("DBClusters", [])
        if not clusters:
            return _result("IO-Optimized Storage", False)
        storage_type = clusters[0].get("StorageType", "")
        if VERBOSE:
            log(f"    storage type: {storage_type}")
        return _result("IO-Optimized Storage", storage_type == "aurora-iopt1")
    except Exception as exc:
        if VERBOSE:
            log(f"    io-opt error: {exc}")
        return _result("IO-Optimized Storage", False)


def check_mysql_connectivity(host: str, key_path: str, endpoint: str,
                             port: int, password: str) -> bool:
    """Connect to Aurora from the EC2 client via mysql CLI."""
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'SELECT 1 AS ok' --batch --skip-column-names 2>&1"
    )
    try:
        r = _ssh_capture(host, script, key_path)
        ok = r.returncode == 0 and "1" in r.stdout
        if VERBOSE and not ok:
            log(f"    mysql output: {r.stdout.strip()} | {r.stderr.strip()}")
        return _result("MySQL Connectivity", ok)
    except Exception as exc:
        if VERBOSE:
            log(f"    mysql error: {exc}")
        return _result("MySQL Connectivity", False)


def check_aurora_version(host: str, key_path: str, endpoint: str,
                         port: int, password: str) -> bool:
    """Retrieve and log Aurora MySQL version."""
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'SELECT version()' --batch --skip-column-names 2>&1"
    )
    try:
        r = _ssh_capture(host, script, key_path)
        version = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
        if version:
            log(f"    Aurora MySQL version: {version}")
        return _result("Aurora Version", r.returncode == 0 and len(version) > 0)
    except Exception as exc:
        if VERBOSE:
            log(f"    version error: {exc}")
        return _result("Aurora Version", False)


def check_aurora_binlog(host: str, key_path: str, endpoint: str,
                        port: int, password: str) -> bool:
    """Verify binlog is enabled (log_bin = ON)."""
    script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e \"SHOW VARIABLES LIKE 'log_bin'\" --batch --skip-column-names 2>&1"
    )
    try:
        r = _ssh_capture(host, script, key_path)
        output = r.stdout.strip()
        if VERBOSE:
            log(f"    log_bin output: {output}")
        return _result("Binlog Enabled", r.returncode == 0 and "ON" in output.upper())
    except Exception as exc:
        if VERBOSE:
            log(f"    binlog error: {exc}")
        return _result("Binlog Enabled", False)


def check_aurora_parameter_group(host: str, key_path: str, endpoint: str,
                                 port: int, password: str) -> bool:
    """Verify key Aurora parameter group settings."""
    expected = {
        "transaction_isolation": "READ-COMMITTED",
        "binlog_format": "ROW",
    }
    all_ok = True
    for var, want in expected.items():
        script = (
            f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
            f"-e \"SHOW VARIABLES LIKE '{var}'\" --batch --skip-column-names 2>&1"
        )
        try:
            r = _ssh_capture(host, script, key_path)
            value = ""
            for line in r.stdout.strip().splitlines():
                parts = line.split("\t")
                if len(parts) >= 2:
                    value = parts[1].strip()
            if VERBOSE:
                log(f"    {var}: got={value}, want={want}")
            if value.upper() != want.upper():
                all_ok = False
        except Exception as exc:
            if VERBOSE:
                log(f"    param error ({var}): {exc}")
            all_ok = False
    return _result("Parameter Group", all_ok)


def check_sysbench_installed(host: str, key_path: str) -> bool:
    """Check that sysbench is available on the client."""
    try:
        r = _ssh_capture(host, "command -v sysbench", key_path)
        return _result("sysbench Installed", r.returncode == 0 and "sysbench" in r.stdout)
    except Exception as exc:
        if VERBOSE:
            log(f"    sysbench error: {exc}")
        return _result("sysbench Installed", False)


def check_mysql_client_installed(host: str, key_path: str) -> bool:
    """Check that the mysql CLI client is available on the client."""
    try:
        r = _ssh_capture(host, "command -v mysql", key_path)
        return _result("MySQL Client", r.returncode == 0 and "mysql" in r.stdout)
    except Exception as exc:
        if VERBOSE:
            log(f"    mysql-client error: {exc}")
        return _result("MySQL Client", False)


# ---------------------------------------------------------------------------
# Quick validation benchmark
# ---------------------------------------------------------------------------
def run_quick_benchmark(host: str, key_path: str, endpoint: str,
                        port: int, password: str) -> bool:
    """Run a tiny sysbench OLTP benchmark against Aurora to verify end-to-end."""
    db = "sbtest_validate"
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user=admin --mysql-password='{password}' "
        f"--mysql-db={db}"
    )

    # Create database
    create_script = (
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'CREATE DATABASE IF NOT EXISTS {db}' 2>&1"
    )
    try:
        r = _ssh_capture(host, create_script, key_path)
        if r.returncode != 0:
            log(f"    failed to create database: {r.stderr.strip()} {r.stdout.strip()}")
            return _result("Quick Benchmark", False)
    except Exception as exc:
        log(f"    create-db error: {exc}")
        return _result("Quick Benchmark", False)

    # Prepare
    prepare_script = (
        f"sysbench oltp_read_write {common} "
        f"--tables=1 --table-size=10000 prepare 2>&1"
    )
    try:
        r = _ssh_capture(host, prepare_script, key_path)
        if r.returncode != 0:
            log(f"    sysbench prepare failed: {r.stdout.strip()}")
            return _result("Quick Benchmark", False)
    except Exception as exc:
        log(f"    prepare error: {exc}")
        return _result("Quick Benchmark", False)

    # Run
    run_script = (
        f"sysbench oltp_read_write {common} "
        f"--tables=1 --table-size=10000 --threads=8 --time=10 "
        f"--report-interval=0 run 2>&1"
    )
    tps = qps = p95 = 0.0
    try:
        r = _ssh_capture(host, run_script, key_path)
        if r.returncode != 0:
            log(f"    sysbench run failed: {r.stdout.strip()}")
            return _result("Quick Benchmark", False)
        output = r.stdout
        # Parse TPS
        m = re.search(r"transactions:\s+\d+\s+\(([\d.]+)\s+per sec", output)
        if m:
            tps = float(m.group(1))
        # Parse QPS
        m = re.search(r"queries:\s+\d+\s+\(([\d.]+)\s+per sec", output)
        if m:
            qps = float(m.group(1))
        # Parse P95 latency
        m = re.search(r"95th percentile:\s+([\d.]+)", output)
        if m:
            p95 = float(m.group(1))
        log(f"    TPS: {tps:.1f}  QPS: {qps:.1f}  P95 latency: {p95:.2f} ms")
    except Exception as exc:
        log(f"    run error: {exc}")
        return _result("Quick Benchmark", False)

    # Cleanup
    cleanup_script = (
        f"sysbench oltp_read_write {common} "
        f"--tables=1 --table-size=10000 cleanup 2>&1; "
        f"mysql -h '{endpoint}' -P {port} -u admin -p'{password}' "
        f"-e 'DROP DATABASE IF EXISTS {db}' 2>&1"
    )
    try:
        _ssh_capture(host, cleanup_script, key_path)
    except Exception:
        pass

    ok = tps > 0 and qps > 0
    return _result("Quick Benchmark", ok)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate Aurora MySQL benchmark stack health.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python3 -m aurora.validate
              python3 -m aurora.validate --seed auroralt-002 --region us-west-2
              python3 -m aurora.validate --skip-benchmark -v
        """),
    )
    p.add_argument("--host", default=None,
                   help="EC2 client public IP (auto-discovered if omitted)")
    p.add_argument("--endpoint", default=None,
                   help="Aurora cluster endpoint (auto-discovered if omitted)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help=f"Aurora port (default: {DEFAULT_PORT})")
    p.add_argument("--region", default=DEFAULT_REGION,
                   help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--seed", default=DEFAULT_SEED,
                   help=f"Stack seed identifier (default: {DEFAULT_SEED})")
    p.add_argument("--aws-profile", default=DEFAULT_PROFILE,
                   help=f"AWS CLI profile (default: {DEFAULT_PROFILE})")
    p.add_argument("--ssh-key", default=None,
                   help="Path to SSH private key")
    p.add_argument("--skip-benchmark", action="store_true",
                   help="Skip the quick sysbench validation benchmark")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Verbose output")
    p.add_argument("--password", default=None,
                   help="Aurora master password (default: env AURORA_MASTER_PASSWORD or BenchMark2024!)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    password = args.password or os.environ.get("AURORA_MASTER_PASSWORD", "BenchMark2024!")

    # SSH key path
    if args.ssh_key:
        key_path = args.ssh_key
    else:
        key_path = str(DEFAULT_SSH_KEY_PATH)

    if not os.path.isfile(key_path):
        log(f"WARNING: SSH key not found at {key_path}")

    # -- Discover stack ------------------------------------------------------
    log("Discovering Aurora stack...")
    info = discover_aurora_stack(args.region, args.aws_profile, args.seed)

    host = args.host or info.get("client_public_ip", "")
    endpoint = args.endpoint or info.get("cluster_endpoint", "")
    port = args.port or info.get("cluster_port", DEFAULT_PORT)

    print()
    print("=" * 60)
    print("Aurora Benchmark Stack Discovery")
    print("=" * 60)
    print(f"  Seed:             {info['seed']}")
    print(f"  Stack:            {info['stack']}")
    print(f"  Region:           {args.region}")
    print(f"  Cluster endpoint: {endpoint or '(not found)'}")
    print(f"  Cluster port:     {port}")
    print(f"  Cluster status:   {info.get('cluster_status', 'unknown')}")
    print(f"  Storage type:     {info.get('cluster_storage_type', 'unknown')}")
    print(f"  Writer instance:  {info.get('writer_instance_id', '(none)')}")
    print(f"  Writer type:      {info.get('writer_instance_type', 'unknown')}")
    print(f"  Writer status:    {info.get('writer_status', 'unknown')}")
    print(f"  Client instance:  {info.get('client_instance_id', '(none)')}")
    print(f"  Client IP:        {host or '(not found)'}")
    print(f"  Client type:      {info.get('client_instance_type', 'unknown')}")
    print(f"  Client status:    {info.get('client_status', 'unknown')}")
    print(f"  SSH key:          {key_path}")
    print()

    if info.get("client_instance_id"):
        volumes = get_ebs_volumes(args.region, args.aws_profile,
                                  info["client_instance_id"])
        if volumes:
            print("  Client EBS Volumes:")
            for v in volumes:
                print(f"    {v['volume_id']}  {v['size_gb']}GB  "
                      f"{v['volume_type']}  iops={v['iops']}  "
                      f"tput={v['throughput']}  {v['state']}")
            print()

    # -- Pre-flight checks ---------------------------------------------------
    if not host:
        log("ERROR: No client IP found. Pass --host or check EC2 tags.")
        sys.exit(1)
    if not endpoint:
        log("ERROR: No Aurora endpoint found. Pass --endpoint or check cluster.")
        sys.exit(1)

    # -- Run checks ----------------------------------------------------------
    print("=" * 60)
    print("Running Validation Checks")
    print("=" * 60)

    results: list[tuple[str, bool]] = []

    def record(name: str, ok: bool) -> None:
        results.append((name, ok))

    record("SSH Connectivity",
           check_ssh_connectivity(host, key_path))

    record("Aurora Cluster Status",
           check_aurora_cluster_status(args.region, args.aws_profile, args.seed))

    record("Aurora Writer Status",
           check_aurora_writer_status(args.region, args.aws_profile, args.seed))

    record("IO-Optimized Storage",
           check_aurora_io_optimized(args.region, args.aws_profile, args.seed))

    record("MySQL Connectivity",
           check_mysql_connectivity(host, key_path, endpoint, port, password))

    record("Aurora Version",
           check_aurora_version(host, key_path, endpoint, port, password))

    record("Binlog Enabled",
           check_aurora_binlog(host, key_path, endpoint, port, password))

    record("Parameter Group",
           check_aurora_parameter_group(host, key_path, endpoint, port, password))

    record("sysbench Installed",
           check_sysbench_installed(host, key_path))

    record("MySQL Client",
           check_mysql_client_installed(host, key_path))

    if not args.skip_benchmark:
        record("Quick Benchmark",
               run_quick_benchmark(host, key_path, endpoint, port, password))

    # -- Summary -------------------------------------------------------------
    passed = sum(1 for _, ok in results if ok)
    total = len(results)

    print()
    print("=" * 60)
    print("Validation Summary")
    print("=" * 60)
    for name, ok in results:
        tag = "PASS" if ok else "FAIL"
        print(f"  [{tag}] {name}")
    print()
    print(f"Result: {passed}/{total} checks passed")
    print()

    if passed == total:
        print("Stack is healthy and ready for benchmarking!")
        sys.exit(0)
    else:
        failed = [name for name, ok in results if not ok]
        print(f"FAILED checks: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
