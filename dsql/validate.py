#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
DSQL Stack Validator

Verifies that the DSQL load test stack is healthy:
- Client VM reachable via SSH
- psql / pgbench installed
- DSQL cluster reachable (IAM auth)
- Optional quick benchmark to confirm end-to-end

Usage:
    python3 -m dsql.validate --seed dsqllt-001
    python3 -m dsql.validate --seed dsqllt-001 --quick-bench
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import boto3

from common.util import BOTO_CONFIG, ec2_client

# ---------------------------------------------------------------------------
DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_SEED = "dsqllt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_DB_PROFILE = os.environ.get("DB_PROFILE", "sandbox-storage")
DSQL_PORT = 5432

STATE_FILE = Path(__file__).resolve().with_name("dsql-state.json")
SSH_KEY_PATH = Path(__file__).resolve().parent.parent / "tidb" / "tidb-load-test-key.pem"


# ---------------------------------------------------------------------------
# Self-contained helpers (matching tidb/validate.py pattern)
# ---------------------------------------------------------------------------

def ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg=""):
    print(f"[{ts()}] {msg}", flush=True)


def dsql_client(region=None, profile=None):
    session = boto3.Session(
        region_name=region or DEFAULT_REGION,
        profile_name=profile,
    )
    return session.client("dsql", config=BOTO_CONFIG)


def ssh_capture(host_ip, script, key_path, timeout=60):
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-i", str(key_path),
        f"ec2-user@{host_ip}",
        "bash -s",
    ]
    try:
        proc = subprocess.run(
            cmd, input=script, capture_output=True, text=True, timeout=timeout,
        )
        return proc.stdout + proc.stderr, proc.returncode
    except subprocess.TimeoutExpired:
        return "TIMEOUT", -1
    except Exception as e:
        return str(e), -1


# ---------------------------------------------------------------------------
# State & discovery
# ---------------------------------------------------------------------------

def load_state():
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def discover_client_ip(state, ec2c):
    instance_id = state.get("client_instance_id")
    if not instance_id:
        return None
    try:
        resp = ec2c.describe_instances(InstanceIds=[instance_id])
        for res in resp["Reservations"]:
            for inst in res["Instances"]:
                if inst["State"]["Name"] == "running":
                    return inst.get("PublicIpAddress") or inst.get("PrivateIpAddress")
    except Exception:
        pass
    return None


def generate_auth_token(endpoint, region, profile=None):
    client = dsql_client(region, profile)
    return client.generate_db_connect_admin_auth_token(
        Hostname=endpoint,
        Region=region,
        ExpiresIn=900,
    )


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

class Check:
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.detail = ""

    def ok(self, detail=""):
        self.passed = True
        self.detail = detail
        log(f"  PASS  {self.name}: {detail}" if detail else f"  PASS  {self.name}")

    def fail(self, detail=""):
        self.passed = False
        self.detail = detail
        log(f"  FAIL  {self.name}: {detail}" if detail else f"  FAIL  {self.name}")


def check_state_file():
    c = Check("State file")
    state = load_state()
    if state and state.get("cluster_id") and state.get("endpoint"):
        c.ok(f"cluster={state['cluster_id']}")
    else:
        c.fail("Missing or incomplete state file.")
    return c, state


def check_dsql_cluster(state, region, profile):
    c = Check("DSQL cluster status")
    cluster_id = state.get("cluster_id", "")
    if not cluster_id:
        c.fail("No cluster_id in state.")
        return c
    try:
        client = dsql_client(region, profile)
        info = client.get_cluster(identifier=cluster_id)
        status = info.get("status", "unknown")
        if status == "ACTIVE":
            c.ok(f"{cluster_id} ({status})")
        else:
            c.fail(f"{cluster_id} ({status})")
    except Exception as e:
        c.fail(str(e))
    return c


def check_ssh(client_ip, key_path):
    c = Check("SSH connectivity")
    output, rc = ssh_capture(client_ip, "echo SSH_OK", key_path, timeout=15)
    if rc == 0 and "SSH_OK" in output:
        c.ok(client_ip)
    else:
        c.fail(f"rc={rc}, output={output[:200]}")
    return c


def check_pgbench(client_ip, key_path):
    c = Check("pgbench installed")
    output, rc = ssh_capture(client_ip, "pgbench --version", key_path)
    if rc == 0 and "pgbench" in output.lower():
        version = output.strip().split("\n")[0]
        c.ok(version)
    else:
        c.fail(f"rc={rc}")
    return c


def check_psql(client_ip, key_path):
    c = Check("psql installed")
    output, rc = ssh_capture(client_ip, "psql --version", key_path)
    if rc == 0 and "psql" in output.lower():
        version = output.strip().split("\n")[0]
        c.ok(version)
    else:
        c.fail(f"rc={rc}")
    return c


def check_dsql_connectivity(client_ip, key_path, endpoint, region, profile):
    c = Check("DSQL connectivity")
    try:
        token = generate_auth_token(endpoint, region, profile)
    except Exception as e:
        c.fail(f"Auth token generation failed: {e}")
        return c

    script = f"""\
export PGPASSWORD='{token}'
psql -h {endpoint} -p {DSQL_PORT} -U admin -d postgres -c "SELECT 1 AS connectivity_check;" 2>&1
echo "PSQL_EXIT:$?"
"""
    output, rc = ssh_capture(client_ip, script, key_path, timeout=30)
    if "PSQL_EXIT:0" in output and "connectivity_check" in output:
        c.ok("SELECT 1 succeeded")
    else:
        c.fail(f"rc={rc}, output={output[:300]}")
    return c


def check_quick_benchmark(client_ip, key_path, endpoint, region, profile):
    c = Check("Quick benchmark (10s)")
    try:
        token = generate_auth_token(endpoint, region, profile)
    except Exception as e:
        c.fail(f"Auth token generation failed: {e}")
        return c

    # DSQL rejects fillfactor, TRUNCATE, and caps writes at ~3500 rows/txn
    init_script = f"""\
export PGPASSWORD='{token}'
PSQL="psql -h {endpoint} -p {DSQL_PORT} -U admin -d postgres -v ON_ERROR_STOP=1"
$PSQL <<'EOSQL'
DROP TABLE IF EXISTS pgbench_history, pgbench_tellers, pgbench_accounts, pgbench_branches;
CREATE TABLE pgbench_branches (bid int NOT NULL PRIMARY KEY, bbalance int, filler char(88));
CREATE TABLE pgbench_tellers  (tid int NOT NULL PRIMARY KEY, bid int, tbalance int, filler char(84));
CREATE TABLE pgbench_accounts (aid int NOT NULL PRIMARY KEY, bid int, abalance int, filler char(84));
CREATE TABLE pgbench_history  (tid int, bid int, aid int, delta int, mtime timestamp, filler char(22));
INSERT INTO pgbench_branches SELECT g, 0, '' FROM generate_series(1, 1) g;
INSERT INTO pgbench_tellers  SELECT g, (g-1)/10+1, 0, '' FROM generate_series(1, 10) g;
EOSQL
if [ $? -ne 0 ]; then echo "INIT_EXIT:1"; exit 0; fi
BATCH=2000; TOTAL=100000
for i in $(seq 0 $(( (TOTAL + BATCH - 1) / BATCH - 1 )) ); do
    s=$(($i * BATCH + 1)); e=$((($i + 1) * BATCH))
    [ $e -gt $TOTAL ] && e=$TOTAL
    echo "INSERT INTO pgbench_accounts SELECT g, (g-1)/100000+1, 0, '' FROM generate_series($s, $e) g;"
done | $PSQL 2>&1
echo "INIT_EXIT:${{PIPESTATUS[1]:-$?}}"
"""
    init_output, init_rc = ssh_capture(client_ip, init_script, key_path, timeout=180)
    if "INIT_EXIT:0" not in init_output:
        c.fail(f"pgbench init failed: {init_output[-300:]}")
        return c

    script = f"""\
export PGPASSWORD='{token}'
pgbench -h {endpoint} -p {DSQL_PORT} -U admin -d postgres \\
    -c 2 -j 2 -T 10 -n --max-tries=3 2>&1
echo "RUN_EXIT:$?"
"""
    output, rc = ssh_capture(client_ip, script, key_path, timeout=60)
    tps_match = re.search(r"tps\s*=\s*([\d.]+)\s*\(without", output)
    if tps_match:
        c.ok(f"{tps_match.group(1)} TPS")
    elif "RUN_EXIT:0" in output:
        c.ok("completed (TPS not parsed)")
    else:
        c.fail(f"rc={rc}, output={output[-300:]}")
    return c


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Validate DSQL load test stack.")
    parser.add_argument("--seed", default=DEFAULT_SEED)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--aws-profile", default=DEFAULT_PROFILE,
                        help="AWS profile for infrastructure (EC2/VPC).")
    parser.add_argument("--db-profile", default=DEFAULT_DB_PROFILE,
                        help="AWS profile for database service APIs (default: sandbox-storage).")
    parser.add_argument(
        "--ssh-key", default=str(SSH_KEY_PATH),
        help=f"SSH private key path (default: {SSH_KEY_PATH}).",
    )
    parser.add_argument(
        "--quick-bench", action="store_true",
        help="Run a quick 10-second pgbench benchmark as validation.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    region = args.region
    profile = args.aws_profile
    db_prof = args.db_profile or args.aws_profile
    key_path = Path(args.ssh_key).expanduser().resolve()

    log("=" * 70)
    log("DSQL Stack Validation")
    log("=" * 70)

    checks = []

    state_check, state = check_state_file()
    checks.append(state_check)
    if not state:
        log("\nCannot continue without state file.")
        sys.exit(1)

    cluster_id = state.get("cluster_id", "")
    endpoint = state.get("endpoint", "")

    checks.append(check_dsql_cluster(state, region, db_prof))

    ec2c = ec2_client(region=region, profile=profile)
    client_ip = discover_client_ip(state, ec2c)
    if not client_ip:
        c = Check("Client VM discovery")
        c.fail("Could not find running client VM.")
        checks.append(c)
        log("\nCannot continue without client VM.")
    else:
        log(f"  Client VM: {client_ip}")

        checks.append(check_ssh(client_ip, key_path))

        ssh_ok = checks[-1].passed
        if ssh_ok:
            checks.append(check_pgbench(client_ip, key_path))
            checks.append(check_psql(client_ip, key_path))

            if endpoint:
                checks.append(check_dsql_connectivity(client_ip, key_path, endpoint, region, db_prof))

                if args.quick_bench:
                    checks.append(check_quick_benchmark(
                        client_ip, key_path, endpoint, region, db_prof,
                    ))

    log("")
    log("=" * 70)
    log("VALIDATION SUMMARY")
    log("=" * 70)

    passed = sum(1 for c in checks if c.passed)
    total = len(checks)

    for c in checks:
        status = "PASS" if c.passed else "FAIL"
        detail = f" -- {c.detail}" if c.detail else ""
        log(f"  [{status}] {c.name}{detail}")

    log(f"\n  {passed}/{total} checks passed.")

    if passed == total:
        log("  Stack is healthy.")
        sys.exit(0)
    else:
        log("  Stack has issues.")
        sys.exit(1)


if __name__ == "__main__":
    main()
