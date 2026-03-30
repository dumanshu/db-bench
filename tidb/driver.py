"""TiDB-specific helpers for the unified benchmark runner.

Extracted from tidb/benchmark.py.  Contains SSH wrappers (different API from
common.ssh), EC2/cluster discovery, CdcLagTracker, resource monitoring,
disk utilization, bulk-load, sysbench prepare/cleanup, and per-minute
report formatting.
"""

import os
import subprocess
import textwrap
import threading
import time
from pathlib import Path
from typing import Optional

from common.util import log, ec2_client
from common.ssh import ssh_capture_simple
from common.benchmark import (
    seeded_database_name,
    parse_sysbench_output as _common_parse_output,
    parse_interval_line as _common_parse_interval,
    SESSION_VARS_BASE, SESSION_VARS_READ_ONLY,
)
from common.report import CostTracker, print_summary

# ---------------------------------------------------------------------------
# TiDB-specific constants
# ---------------------------------------------------------------------------

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "tidblt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_PORT = 30400
INTERNAL_SERVICE_PORT = 4000
DEFAULT_DATABASE = "sbtest"
DEFAULT_EBS_SIZE_GB = 600

SYSBENCH_ROW_DISK_BYTES = 240

TIDB_MYSQL_IGNORE_ERRORS = "1213,1020,1205,1105,8249"

AWS_COSTS = {
    "ec2": {
        "c7g.xlarge": {"hourly": 0.145, "vcpu": 4, "memory_gb": 8},
        "c7g.2xlarge": {"hourly": 0.29, "vcpu": 8, "memory_gb": 16},
        "c7g.4xlarge": {"hourly": 0.58, "vcpu": 16, "memory_gb": 32},
        "c7g.8xlarge": {"hourly": 1.16, "vcpu": 32, "memory_gb": 64},
        "c8g.xlarge": {"hourly": 0.136, "vcpu": 4, "memory_gb": 8},
        "c8g.2xlarge": {"hourly": 0.272, "vcpu": 8, "memory_gb": 16},
        "c8g.4xlarge": {"hourly": 0.544, "vcpu": 16, "memory_gb": 32},
        "c8g.8xlarge": {"hourly": 1.088, "vcpu": 32, "memory_gb": 64},
        "c8g.24xlarge": {"hourly": 3.264, "vcpu": 96, "memory_gb": 192},
        "m7g.xlarge": {"hourly": 0.163, "vcpu": 4, "memory_gb": 16},
        "m7g.2xlarge": {"hourly": 0.326, "vcpu": 8, "memory_gb": 32},
        "m7g.4xlarge": {"hourly": 0.652, "vcpu": 16, "memory_gb": 64},
        "m8g.xlarge": {"hourly": 0.153, "vcpu": 4, "memory_gb": 16},
        "m8g.2xlarge": {"hourly": 0.306, "vcpu": 8, "memory_gb": 32},
        "m8g.4xlarge": {"hourly": 0.612, "vcpu": 16, "memory_gb": 64},
        "r7g.2xlarge": {"hourly": 0.428, "vcpu": 8, "memory_gb": 64},
        "r7g.4xlarge": {"hourly": 0.856, "vcpu": 16, "memory_gb": 128},
    },
    "ebs": {
        "gp3_per_gb_month": 0.08,
        "gp3_iops_over_3000": 0.005,
        "gp3_throughput_over_125": 0.04,
        "io2_per_gb_month": 0.125,
        "io2_per_iops_month": 0.065,
    },
    "network": {
        "cross_az_per_gb": 0.01,
        "same_az_per_gb": 0.00,
        "internet_out_first_10tb": 0.09,
        "internet_out_next_40tb": 0.085,
        "nat_gateway_per_gb": 0.045,
    },
    "s3": {
        "standard_per_gb_month": 0.023,
        "put_per_1000": 0.005,
        "get_per_1000": 0.0004,
    },
}

# ---------------------------------------------------------------------------
# TiDB-specific SSH helpers (different API from common.ssh)
# ---------------------------------------------------------------------------


def ssh_run(host: str, script: str, key_path: Path, strict: bool = True):
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    cmd = [
        "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=30",
        "-i", str(key_path), f"ec2-user@{host}", "bash", "-s",
    ]
    subprocess.run(cmd, input=full, text=True, check=strict)


def ssh_stream(host: str, script: str, key_path: Path):
    full = "set -euo pipefail\n" + textwrap.dedent(script).lstrip()
    cmd = [
        "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=30",
        "-i", str(key_path), f"ec2-user@{host}", "bash", "-s",
    ]
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    proc.stdin.write(full)
    proc.stdin.close()
    return proc


# ---------------------------------------------------------------------------
# TiDB-specific: EC2 / cluster discovery
# ---------------------------------------------------------------------------


def discover_tidb_host(region: str, profile: Optional[str], seed: str) -> str:
    client = ec2_client(profile=profile, region=region)
    stack = f"tidb-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            role = tags.get("Role", "")
            if role in ("host", "client", "control"):
                ip = inst.get("PublicIpAddress")
                if ip:
                    return ip
    raise SystemExit("ERROR: Unable to discover TiDB host; specify --host explicitly.")


def discover_tidb_endpoint(region: str, profile: Optional[str], seed: str) -> str:
    """Return a k3s node private IP for NodePort access from a decoupled client."""
    client = ec2_client(profile=profile, region=region)
    stack = f"tidb-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            role = tags.get("Role", "")
            if role == "control":
                ip = inst.get("PrivateIpAddress")
                if ip:
                    return ip
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            ip = inst.get("PrivateIpAddress")
            if ip:
                return ip
    raise SystemExit("ERROR: Cannot discover TiDB endpoint for remote client.")


def get_instance_info(region: str, profile: Optional[str], seed: str) -> dict:
    client = ec2_client(profile=profile, region=region)
    stack = f"tidb-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    info = {"instances": [], "az": None}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            info["instances"].append({
                "id": inst.get("InstanceId"),
                "type": inst.get("InstanceType"),
                "az": inst.get("Placement", {}).get("AvailabilityZone"),
                "role": tags.get("Role", "unknown"),
            })
            if not info["az"]:
                info["az"] = inst.get("Placement", {}).get("AvailabilityZone")
    return info


def get_cluster_info(host: str, key_path: Path, port: int = DEFAULT_PORT,
                     db_host: str = "127.0.0.1") -> dict:
    script = f"""
mysql -h {db_host} -P {port} -u root -N -e "
SELECT 'tidb_version', TIDB_VERSION();
SELECT 'tidb_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='tidb';
SELECT 'tikv_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='tikv';
SELECT 'pd_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='pd';
SELECT 'region_count', COUNT(*) FROM information_schema.tikv_region_status;
" 2>/dev/null || echo "cluster_info_error"

kubectl top pods -n tidb-cluster --no-headers 2>/dev/null | head -10 || echo "kubectl_error"
"""
    result = ssh_capture_simple(host, key_path, script)
    info = {
        "tidb_version": "unknown", "tidb_count": 0, "tikv_count": 0,
        "pd_count": 0, "region_count": 0, "pods": [],
    }
    for line in result.stdout.split('\n'):
        parts = line.strip().split('\t')
        if len(parts) == 2:
            key, val = parts
            if key in info:
                try:
                    info[key] = int(val) if key.endswith('_count') else val
                except ValueError:
                    info[key] = val
        elif line and not line.startswith(("kubectl_error", "cluster_info_error")):
            pod_parts = line.split()
            if len(pod_parts) >= 3 and pod_parts[0].startswith("basic-"):
                info["pods"].append({"name": pod_parts[0], "cpu": pod_parts[1], "memory": pod_parts[2]})
    return info


# ---------------------------------------------------------------------------
# TiDB-specific: CDC lag tracker
# ---------------------------------------------------------------------------


class CdcLagTracker:
    MAX_LAG_THRESHOLD = 120.0

    def __init__(self, host: str, key_path: Path,
                 upstream_svc: str = "basic-tidb",
                 upstream_ns: str = "tidb-cluster",
                 downstream_svc: str = "downstream-tidb",
                 downstream_ns: str = "tidb-downstream",
                 downstream_port: int = INTERNAL_SERVICE_PORT,
                 write_interval: float = 0.5,
                 read_interval: float = 0.5):
        self.host = host
        self.key_path = key_path
        self.upstream_svc = upstream_svc
        self.upstream_ns = upstream_ns
        self.downstream_svc = downstream_svc
        self.downstream_ns = downstream_ns
        self.downstream_port = downstream_port
        self.write_interval = write_interval
        self.read_interval = read_interval
        self._samples: list[float] = []
        self._stop = threading.Event()
        self._writer_thread: Optional[threading.Thread] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._seq = 0
        self._last_read_seq = 0

    def _init_table(self):
        script = f"""
UPSTREAM_IP=$(kubectl get svc {self.upstream_svc} -n {self.upstream_ns} \
  -o jsonpath='{{.spec.clusterIP}}')
mysql -h "$UPSTREAM_IP" -P 4000 -u root -e "
  CREATE DATABASE IF NOT EXISTS cdc_test;
  DROP TABLE IF EXISTS cdc_test.lag_tracker;
  CREATE TABLE cdc_test.lag_tracker (
      seq          INT NOT NULL PRIMARY KEY,
      src_write_ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
  );
"
"""
        ssh_run(self.host, script, self.key_path, strict=False)

    def _add_dst_column(self):
        script = f"""
DOWNSTREAM_IP=$(kubectl get svc {self.downstream_svc} -n {self.downstream_ns} \
  -o jsonpath='{{.spec.clusterIP}}')
mysql -h "$DOWNSTREAM_IP" -P {self.downstream_port} -u root -e "
  ALTER TABLE cdc_test.lag_tracker
    ADD COLUMN dst_write_ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
" 2>/dev/null && echo "OK"
"""
        result = ssh_capture_simple(self.host, self.key_path, script)
        return "OK" in result.stdout

    def _write_row(self) -> Optional[int]:
        self._seq += 1
        seq = self._seq
        script = f"""
UPSTREAM_IP=$(kubectl get svc {self.upstream_svc} -n {self.upstream_ns} \
  -o jsonpath='{{.spec.clusterIP}}')
mysql -h "$UPSTREAM_IP" -P 4000 -u root -N -e \
  "INSERT INTO cdc_test.lag_tracker (seq) VALUES ({seq});" 2>/dev/null \
  && echo "OK"
"""
        result = ssh_capture_simple(self.host, self.key_path, script)
        if "OK" in result.stdout:
            return seq
        return None

    def _read_new_rows(self) -> list[tuple[int, float]]:
        script = f"""
DOWNSTREAM_IP=$(kubectl get svc {self.downstream_svc} -n {self.downstream_ns} \
  -o jsonpath='{{.spec.clusterIP}}')
mysql -h "$DOWNSTREAM_IP" -P {self.downstream_port} -u root -N -e \
  "SELECT seq, TIMESTAMPDIFF(MICROSECOND, src_write_ts, dst_write_ts) / 1000000.0 AS lag_s \
   FROM cdc_test.lag_tracker \
   WHERE seq > {self._last_read_seq} ORDER BY seq;" 2>/dev/null
"""
        result = ssh_capture_simple(self.host, self.key_path, script)
        rows: list[tuple[int, float]] = []
        for line in result.stdout.strip().split('\n'):
            parts = line.strip().split()
            if len(parts) == 2:
                try:
                    rows.append((int(parts[0]), float(parts[1])))
                except (ValueError, IndexError):
                    pass
        return rows

    def _writer_loop(self):
        while not self._stop.is_set():
            try:
                self._write_row()
            except Exception:
                pass
            self._stop.wait(self.write_interval)

    def _reader_loop(self):
        while not self._stop.is_set():
            try:
                new_rows = self._read_new_rows()
                if new_rows:
                    for seq, lag in new_rows:
                        if 0 <= lag < self.MAX_LAG_THRESHOLD:
                            self._samples.append(lag)
                    self._last_read_seq = max(seq for seq, _ in new_rows)
            except Exception:
                pass
            self._stop.wait(self.read_interval)

    def _warmup(self):
        log("  Warming up CDC lag tracker (waiting for first row to replicate)...")
        self._init_table()
        time.sleep(2)
        seq = self._write_row()
        if seq is None:
            log("  CDC lag tracker warmup: failed to write seed row")
            return
        max_wait = 60
        start = time.time()
        arrived = False
        while time.time() - start < max_wait:
            time.sleep(2)
            script = f"""
DOWNSTREAM_IP=$(kubectl get svc {self.downstream_svc} -n {self.downstream_ns} \
  -o jsonpath='{{.spec.clusterIP}}')
mysql -h "$DOWNSTREAM_IP" -P {self.downstream_port} -u root -N -e \
  "SELECT seq FROM cdc_test.lag_tracker WHERE seq = {seq};" 2>/dev/null
"""
            result = ssh_capture_simple(self.host, self.key_path, script)
            if str(seq) in result.stdout:
                arrived = True
                break
        if not arrived:
            log("  CDC lag tracker warmup timed out waiting for seed row")
            self._last_read_seq = seq
            return
        if self._add_dst_column():
            log("  Added dst_write_ts column on downstream table")
        else:
            log("  WARNING: failed to add dst_write_ts column on downstream")
        self._last_read_seq = seq
        log(f"  CDC lag tracker warmed up (seed row seq={seq})")

    def start(self):
        log("Starting TiCDC lag tracker...")
        self._warmup()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._writer_thread.start()
        self._reader_thread.start()

    def stop(self):
        self._stop.set()
        if self._writer_thread:
            self._writer_thread.join(timeout=5)
        if self._reader_thread:
            self._reader_thread.join(timeout=5)
        log(f"TiCDC lag tracker stopped ({len(self._samples)} samples)")

    def summary(self) -> dict:
        if not self._samples:
            return {}
        s = sorted(self._samples)
        n = len(s)
        return {
            "samples": n, "min": s[0], "avg": sum(s) / n,
            "p50": s[int(n * 0.50)], "p95": s[int(n * 0.95)],
            "p99": s[min(int(n * 0.99), n - 1)], "max": s[-1],
        }

    def print_summary(self):
        data = self.summary()
        if not data:
            log("TiCDC Lag: no samples collected")
            return
        log("")
        log("=" * 70)
        log("TICDC REPLICATION LAG  (dst_write_ts - src_write_ts)")
        log("=" * 70)
        log(f"Samples: {data['samples']}")
        log(f"  Min:  {data['min']:.3f}s")
        log(f"  Avg:  {data['avg']:.3f}s")
        log(f"  P50:  {data['p50']:.3f}s")
        log(f"  P95:  {data['p95']:.3f}s")
        log(f"  P99:  {data['p99']:.3f}s")
        log(f"  Max:  {data['max']:.3f}s")
        log("=" * 70)


# ---------------------------------------------------------------------------
# TiDB-specific: cluster summary, resource monitoring, disk utilization
# ---------------------------------------------------------------------------


def print_cluster_summary(host: str, key_path: Path, region: str, profile: str, seed: str,
                          port: int = DEFAULT_PORT, db_host: str = "127.0.0.1"):
    log("")
    log("=" * 80)
    log("CLUSTER CONFIGURATION")
    log("=" * 80)
    inst_info = get_instance_info(region, profile, seed)
    cluster_info = get_cluster_info(host, key_path, port, db_host=db_host)
    log(f"Region: {region} | Zone: {inst_info.get('az', 'unknown')}")
    log(f"TiDB Version: {cluster_info.get('tidb_version', 'unknown')}")
    log(f"Regions: {cluster_info.get('region_count', 'N/A')}")
    log("")

    role_types = {}
    for inst in inst_info.get("instances", []):
        role = inst.get("role", "unknown")
        itype = inst.get("type", "unknown")
        base_role = role.rsplit("-", 1)[0] if "-" in role else role
        if base_role not in role_types:
            role_types[base_role] = itype

    default_type = role_types.get("host", role_types.get("client", "c8g.4xlarge"))
    tidb_type = role_types.get("tidb", default_type)
    tikv_type = role_types.get("tikv", default_type)
    pd_type = role_types.get("pd", default_type)
    client_type = role_types.get("client", role_types.get("host", default_type))

    def specs(itype):
        return AWS_COSTS["ec2"].get(itype, {"vcpu": "?", "memory_gb": "?", "hourly": 0.0})

    log("--- Compute Resources ---")
    log(f"{'Component':<12} {'Count':>6} {'Instance':>14} {'vCPU':>6} {'Memory':>8}")
    log("-" * 52)
    ts_ = specs(tidb_type)
    ks_ = specs(tikv_type)
    ps_ = specs(pd_type)
    cs_ = specs(client_type)
    log(f"{'TiDB':<12} {cluster_info.get('tidb_count', 2):>6} {tidb_type:>14} {ts_.get('vcpu', '?'):>6} {ts_.get('memory_gb', '?'):>5}GB")
    log(f"{'TiKV':<12} {cluster_info.get('tikv_count', 3):>6} {tikv_type:>14} {ks_.get('vcpu', '?'):>6} {ks_.get('memory_gb', '?'):>5}GB")
    log(f"{'PD':<12} {cluster_info.get('pd_count', 3):>6} {pd_type:>14} {ps_.get('vcpu', '?'):>6} {ps_.get('memory_gb', '?'):>5}GB")
    log(f"{'Client (EC2)':<12} {'1':>6} {client_type:>14} {cs_.get('vcpu', '?'):>6} {cs_.get('memory_gb', '?'):>5}GB")
    server_count = (cluster_info.get('tidb_count', 2) +
                    cluster_info.get('tikv_count', 3) +
                    cluster_info.get('pd_count', 3))
    log(f"  Total server hosts: {server_count} (each pod on its own dedicated host)")
    log("")

    disk_info = get_disk_utilization(host, key_path, port, DEFAULT_EBS_SIZE_GB,
                                     db_host=db_host)
    ebs_type = "gp3"
    ebs_size_gb = disk_info.get('ebs_total_gb', DEFAULT_EBS_SIZE_GB)
    ebs_iops = 3000
    ebs_throughput = 125

    log("--- Storage Resources ---")
    log(f"{'Volume':<12} {'Type':>8} {'Size':>10} {'IOPS':>8} {'Throughput':>12}")
    log("-" * 56)
    log(f"{'Per-node EBS':<12} {ebs_type:>8} {ebs_size_gb:>8}GB {ebs_iops:>8} {ebs_throughput:>9}MB/s")
    ebs_monthly = ebs_size_gb * cluster_info.get('tikv_count', 3) * AWS_COSTS["ebs"]["gp3_per_gb_month"]
    log(f"{'':>12} EBS Cost: ${ebs_monthly:.2f}/month total ({cluster_info.get('tikv_count', 3)} nodes, ${ebs_monthly/730:.4f}/hr)")
    log("")

    log("--- Network Configuration ---")
    log(f"Cross-AZ transfer: ${AWS_COSTS['network']['cross_az_per_gb']:.3f}/GB (bidirectional)")
    log(f"Same-AZ transfer:  ${AWS_COSTS['network']['same_az_per_gb']:.3f}/GB (free)")
    log("")

    hourly_compute = (
        specs(tidb_type).get("hourly", 0.0) * cluster_info.get('tidb_count', 2) +
        specs(tikv_type).get("hourly", 0.0) * cluster_info.get('tikv_count', 3) +
        specs(pd_type).get("hourly", 0.0) * cluster_info.get('pd_count', 3) +
        specs(client_type).get("hourly", 0.0)
    )
    hourly_storage = ebs_monthly / 730
    total_hourly = hourly_compute + hourly_storage
    log(f"--- Hourly Rate Preview ({server_count} server hosts + 1 client) ---")
    log(f"EC2 Compute: ${hourly_compute:.3f}/hr | EBS Storage: ${hourly_storage:.4f}/hr | Total: ${total_hourly:.3f}/hr")
    log("=" * 80)


def start_resource_monitor(host: str, key_path: Path, interval: int = 60,
                           cost_tracker: Optional[CostTracker] = None,
                           start_time: Optional[float] = None,
                           port: int = DEFAULT_PORT,
                           db_host: str = "127.0.0.1") -> threading.Thread:
    stop_event = threading.Event()
    _start_time = start_time or time.time()

    def monitor_resources():
        while not stop_event.is_set():
            elapsed = time.time() - _start_time
            script = f"TIDB_PORT={port}\nDB_HOST={db_host}\n" + """
echo "--- $(date +%H:%M:%S) Resource Snapshot ---"
CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1 2>/dev/null || echo "N/A")
MEM=$(free -m | awk 'NR==2{printf "%.0f%%", $3*100/$2}' 2>/dev/null || echo "N/A")
CONN=$(mysql -h $DB_HOST -P $TIDB_PORT -u root -N -e "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Threads_connected';" 2>/dev/null || echo "N/A")
echo "Client: CPU=${CPU}% Mem=${MEM} Conn=${CONN}"
echo "Pods:"
kubectl top pods -n tidb-cluster --no-headers 2>/dev/null | awk '{printf "  %-20s CPU:%-6s Mem:%s\\n", $1, $2, $3}' | head -8 || echo "  N/A"
"""
            result = ssh_capture_simple(host, key_path, script)
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    log(line)
            if cost_tracker:
                summary = cost_tracker.get_summary()
                log(f"  Cost so far: ${summary['total_cost']:.4f} ({elapsed/60:.1f} min elapsed)")
            log("")
            stop_event.wait(interval)

    thread = threading.Thread(target=monitor_resources, daemon=True)
    thread.stop_event = stop_event
    thread.start()
    return thread


def stop_resource_monitor(thread: threading.Thread):
    if hasattr(thread, 'stop_event'):
        thread.stop_event.set()


def fetch_resource_snapshot_compact(host: str, key_path: Path, port: int = DEFAULT_PORT,
                                    db_host: str = "127.0.0.1") -> str:
    script = f"TIDB_PORT={port}\nDB_HOST={db_host}\n" + '''
CPU=$(top -bn1 | grep "Cpu(s)" | awk '{printf "%.0f%%", $2+$4}' 2>/dev/null || echo "N/A")
MEM=$(free -m | awk 'NR==2{printf "%.0f%%", $3*100/$2}' 2>/dev/null || echo "N/A")
echo "CLIENT cpu=$CPU mem=$MEM"

mysql -h $DB_HOST -P $TIDB_PORT -u root -N -e "
SELECT
    CONCAT('NODE ', TYPE, '-', SUBSTRING_INDEX(SUBSTRING_INDEX(INSTANCE, '.', 1), '-', -1),
           ' cpu=', ROUND((1 - MAX(CASE WHEN NAME='idle' AND DEVICE_NAME='usage' THEN VALUE ELSE NULL END)) * 100), '%',
           ' mem=', ROUND(MAX(CASE WHEN NAME='used-percent' AND DEVICE_TYPE='memory' THEN VALUE ELSE NULL END) * 100), '%')
FROM information_schema.CLUSTER_LOAD
WHERE (DEVICE_TYPE='cpu' AND DEVICE_NAME='usage') OR (DEVICE_TYPE='memory' AND DEVICE_NAME='virtual')
GROUP BY TYPE, INSTANCE
ORDER BY TYPE, INSTANCE;
" 2>/dev/null || echo "NODE N/A"
'''
    result = ssh_capture_simple(host, key_path, script)
    return result.stdout.strip()


def fetch_final_resource_snapshot(host: str, key_path: Path, port: int = DEFAULT_PORT,
                                  db_host: str = "127.0.0.1"):
    log("")
    log("=" * 70)
    log("FINAL RESOURCE UTILIZATION")
    log("=" * 70)
    script = f"TIDB_PORT={port}\nDB_HOST={db_host}\n" + '''
echo "--- Client VM ---"
echo "CPU: $(top -bn1 | grep "Cpu(s)" | awk '{printf "%.1f%% user, %.1f%% sys, %.1f%% idle", $2, $4, $8}')"
echo "Memory: $(free -h | awk 'NR==2{printf "%s used / %s total (%.1f%%)", $3, $2, $3*100/$2}')"
echo "Load: $(cat /proc/loadavg | awk '{print $1, $2, $3}')"
echo ""
echo "--- TiDB Cluster Nodes ---"
mysql -h $DB_HOST -P $TIDB_PORT -u root -N -e "
SELECT
    CONCAT(UPPER(TYPE), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(INSTANCE, '.', 1), '-', -1),
           '  CPU: ', ROUND((1 - MAX(CASE WHEN NAME='idle' AND DEVICE_NAME='usage' THEN VALUE ELSE NULL END)) * 100), '%',
           '  Mem: ', ROUND(MAX(CASE WHEN NAME='used-percent' AND DEVICE_TYPE='memory' THEN VALUE ELSE NULL END) * 100), '%')
FROM information_schema.CLUSTER_LOAD
WHERE (DEVICE_TYPE='cpu' AND DEVICE_NAME='usage') OR (DEVICE_TYPE='memory' AND DEVICE_NAME='virtual')
GROUP BY TYPE, INSTANCE
ORDER BY TYPE, INSTANCE;
" 2>/dev/null || echo "N/A"
'''
    result = ssh_capture_simple(host, key_path, script)
    if result.stdout.strip():
        for line in result.stdout.strip().split('\n'):
            log(line)
    log("=" * 70)


# ---------------------------------------------------------------------------
# TiDB-specific: disk utilization + bulk load
# ---------------------------------------------------------------------------


def get_disk_utilization(host: str, key_path: Path, port: int = DEFAULT_PORT,
                         ebs_size_gb: int = DEFAULT_EBS_SIZE_GB,
                         database: str = DEFAULT_DATABASE,
                         db_host: str = "127.0.0.1") -> dict:
    script = f'''
DF_LINE=$(df -BG / | tail -1)
EBS_USED=$(echo "$DF_LINE" | awk '{{print $3}}' | tr -d 'G')
EBS_TOTAL=$(echo "$DF_LINE" | awk '{{print $2}}' | tr -d 'G')
EBS_PCT=$(echo "$DF_LINE" | awk '{{print $5}}' | tr -d '%')
echo "EBS_USED=$EBS_USED"
echo "EBS_TOTAL=$EBS_TOTAL"
echo "EBS_PCT=$EBS_PCT"

mysql -h {db_host} -P {port} -u root -N -e "
SELECT CONCAT('TIKV_STORE_BYTES=', IFNULL(SUM(CAPACITY) - SUM(AVAILABLE), 0))
FROM INFORMATION_SCHEMA.TIKV_STORE_STATUS;
" 2>/dev/null || echo "TIKV_STORE_BYTES=0"

mysql -h {db_host} -P {port} -u root -N -e "
SELECT CONCAT('DB_DATA_BYTES=', IFNULL(SUM(data_length + index_length), 0))
FROM information_schema.tables
WHERE table_schema = '{database}';
" 2>/dev/null || echo "DB_DATA_BYTES=0"
'''
    result = ssh_capture_simple(host, key_path, script)
    info = {
        "ebs_total_gb": ebs_size_gb, "ebs_used_gb": 0, "ebs_used_pct": 0,
        "tikv_store_gb": 0, "tikv_store_pct": 0, "db_data_gb": 0,
    }
    for line in result.stdout.strip().split('\n'):
        line = line.strip()
        if line.startswith('EBS_USED='):
            try: info["ebs_used_gb"] = int(line.split('=')[1])
            except ValueError: pass
        elif line.startswith('EBS_TOTAL='):
            try:
                val = int(line.split('=')[1])
                if val > 0: info["ebs_total_gb"] = val
            except ValueError: pass
        elif line.startswith('EBS_PCT='):
            try: info["ebs_used_pct"] = int(line.split('=')[1])
            except ValueError: pass
        elif line.startswith('TIKV_STORE_BYTES='):
            try:
                b = int(line.split('=')[1])
                info["tikv_store_gb"] = b / (1024 ** 3)
                info["tikv_store_pct"] = (b / (1024 ** 3)) / info["ebs_total_gb"] * 100
            except ValueError: pass
        elif line.startswith('DB_DATA_BYTES='):
            try: info["db_data_gb"] = int(line.split('=')[1]) / (1024 ** 3)
            except ValueError: pass
    return info


def calculate_bulk_load_params(target_disk_pct: float, ebs_total_gb: int,
                                current_disk_used_gb: float, num_tables: int) -> dict:
    target_total_gb = ebs_total_gb * (target_disk_pct / 100.0)
    available_for_data_gb = max(0, target_total_gb - current_disk_used_gb)
    total_rows = int(available_for_data_gb * (1024 ** 3) / SYSBENCH_ROW_DISK_BYTES)
    rows_per_table = max(10000, total_rows // num_tables)
    estimated_disk_gb = (rows_per_table * num_tables * SYSBENCH_ROW_DISK_BYTES) / (1024 ** 3)
    estimated_total_gb = current_disk_used_gb + estimated_disk_gb
    return {
        "target_data_gb": available_for_data_gb,
        "rows_per_table": rows_per_table,
        "total_rows": rows_per_table * num_tables,
        "estimated_disk_gb": estimated_disk_gb,
        "estimated_total_gb": estimated_total_gb,
        "estimated_disk_pct": (estimated_total_gb / ebs_total_gb) * 100,
    }


def run_bulk_data_load(host: str, key_path: Path, target_disk_pct: float,
                       num_tables: int, port: int = DEFAULT_PORT,
                       database: str = DEFAULT_DATABASE,
                       ebs_size_gb: int = DEFAULT_EBS_SIZE_GB,
                       db_host: str = "127.0.0.1",
                       bench_host: str = None,
                       bench_key: Path = None) -> dict:
    log("")
    log("=" * 70)
    log("PHASE 1: BULK DATA LOAD")
    log(f"  Target disk utilization: {target_disk_pct}%")
    log("=" * 70)

    log("")
    log("Measuring current disk utilization...")
    disk_before = get_disk_utilization(host, key_path, port, ebs_size_gb, database,
                                       db_host=db_host)
    log(f"  EBS volume: {disk_before['ebs_total_gb']}GB total, "
        f"{disk_before['ebs_used_gb']}GB used ({disk_before['ebs_used_pct']}%)")
    log(f"  TiKV stores: {disk_before['tikv_store_gb']:.1f}GB ({disk_before['tikv_store_pct']:.1f}% of EBS)")
    log(f"  Benchmark DB: {disk_before['db_data_gb']:.2f}GB")

    params = calculate_bulk_load_params(
        target_disk_pct=target_disk_pct,
        ebs_total_gb=disk_before['ebs_total_gb'],
        current_disk_used_gb=disk_before['ebs_used_gb'],
        num_tables=num_tables,
    )
    log("")
    log("Bulk load plan:")
    log(f"  Tables: {num_tables}")
    log(f"  Rows per table: {params['rows_per_table']:,}")
    log(f"  Total rows: {params['total_rows']:,}")
    log(f"  Estimated data on disk (per TiKV node): {params['estimated_disk_gb']:.1f}GB")
    log(f"  Estimated total disk: {params['estimated_total_gb']:.1f}GB ({params['estimated_disk_pct']:.1f}%)")
    log("")

    sb_host = bench_host or host
    sb_key = bench_key or key_path

    load_start = time.time()
    run_sysbench_prepare(sb_host, sb_key, num_tables, params['rows_per_table'], port, database,
                         db_host=db_host)
    load_duration = time.time() - load_start

    log("")
    log("Waiting 30s for TiKV compaction to settle...")
    time.sleep(30)

    disk_after = get_disk_utilization(host, key_path, port, ebs_size_gb, database,
                                      db_host=db_host)
    log("")
    log("Disk utilization after bulk load:")
    log(f"  EBS volume: {disk_after['ebs_total_gb']}GB total, "
        f"{disk_after['ebs_used_gb']}GB used ({disk_after['ebs_used_pct']}%)")
    log(f"  TiKV stores: {disk_after['tikv_store_gb']:.1f}GB ({disk_after['tikv_store_pct']:.1f}% of EBS)")
    log(f"  Benchmark DB: {disk_after['db_data_gb']:.2f}GB")
    log(f"  Load time: {load_duration:.0f}s ({load_duration/60:.1f}min)")
    log(f"  Load rate: {params['total_rows'] / load_duration:,.0f} rows/sec")
    log("=" * 70)

    return {
        "tables": num_tables,
        "rows_per_table": params['rows_per_table'],
        "total_rows": params['total_rows'],
        "disk_before": disk_before,
        "disk_after": disk_after,
        "load_duration_s": load_duration,
        "load_rate_rows_per_sec": params['total_rows'] / load_duration if load_duration > 0 else 0,
    }


# ---------------------------------------------------------------------------
# TiDB-specific: sysbench prepare / cleanup (uses local SSH, not common.ssh)
# ---------------------------------------------------------------------------


def run_sysbench_prepare(host: str, key_path: Path, tables: int, table_size: int,
                          port: int = DEFAULT_PORT, database: str = DEFAULT_DATABASE,
                          db_host: str = "127.0.0.1"):
    log(f"Preparing sysbench tables: {tables} tables x {table_size:,} rows in database '{database}'")
    estimated_row_size_bytes = 120
    total_rows = tables * table_size
    estimated_size_mb = (total_rows * estimated_row_size_bytes) / (1024 * 1024)
    log(f"  Estimated dataset size: {estimated_size_mb:,.1f} MB ({total_rows:,} total rows)")
    ssh_run(host, f"""
mysql -h {db_host} -P {port} -u root -e "CREATE DATABASE IF NOT EXISTS {database};"
EXISTING=$(mysql -h {db_host} -P {port} -u root -D {database} -N -e "
  SELECT table_name FROM information_schema.tables
  WHERE table_schema = '{database}' AND table_name LIKE 'sbtest%';
" 2>/dev/null || true)
for tbl in $EXISTING; do
    mysql -h {db_host} -P {port} -u root -D {database} -e "DROP TABLE IF EXISTS $tbl;" 2>/dev/null || true
done
sysbench oltp_read_write \\
    --mysql-host={db_host} \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    prepare
echo ""
echo "=== Dataset Size Report ==="
mysql -h {db_host} -P {port} -u root -e "
SELECT
    table_schema AS db,
    COUNT(*) AS tables,
    SUM(table_rows) AS total_rows,
    ROUND(SUM(data_length) / 1024 / 1024, 2) AS data_mb,
    ROUND(SUM(index_length) / 1024 / 1024, 2) AS index_mb,
    ROUND((SUM(data_length) + SUM(index_length)) / 1024 / 1024, 2) AS total_mb
FROM information_schema.tables
WHERE table_schema = '{database}'
GROUP BY table_schema;
"
""", key_path)


def run_sysbench_cleanup(host: str, key_path: Path, tables: int,
                          port: int = DEFAULT_PORT, database: str = DEFAULT_DATABASE,
                          db_host: str = "127.0.0.1"):
    log("Cleaning up sysbench tables")
    ssh_run(host, f"""
sysbench oltp_read_write \\
    --mysql-host={db_host} \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    cleanup
""", key_path, strict=False)


# ---------------------------------------------------------------------------
# TiDB-specific: per-minute report formatting
# ---------------------------------------------------------------------------


def format_minute_report(minute: int, intervals: list, resource_text: str) -> str:
    lines = []
    if intervals:
        avg_tps = sum(i['tps'] for i in intervals) / len(intervals)
        avg_qps = sum(i['qps'] for i in intervals) / len(intervals)
        max_p99 = max(i['latency_pct_ms'] for i in intervals)
        avg_p99 = sum(i['latency_pct_ms'] for i in intervals) / len(intervals)
        total_errs = sum(i['err_per_sec'] for i in intervals)
        total_txns_est = sum(i['tps'] * 10 for i in intervals)
        avail = 100.0
        if total_txns_est > 0 and total_errs > 0:
            err_count_est = total_errs * 10 * len(intervals) / len(intervals)
            avail = max(0, (1 - err_count_est / (total_txns_est + err_count_est)) * 100)
        lines.append(f"--- Minute {minute} Report ---")
        lines.append(f"  Perf:  TPS={avg_tps:,.1f}  QPS={avg_qps:,.1f}  P99={avg_p99:.1f}ms (max {max_p99:.1f}ms)  err/s={total_errs/len(intervals):.2f}  avail={avail:.2f}%")
    else:
        lines.append(f"--- Minute {minute} Report ---")
        lines.append(f"  Perf:  (no interval data)")

    client_line = ""
    node_lines = []
    if resource_text:
        for rline in resource_text.split('\n'):
            rline = rline.strip()
            if rline.startswith('CLIENT '):
                client_line = ' '.join(rline.split()[1:])
            elif rline.startswith('NODE '):
                node_lines.append(rline[5:].strip())

    res_str = f"  Rsrc:  VM=[{client_line}]"
    if node_lines:
        res_str += f"  Nodes=[{', '.join(node_lines)}]"
    lines.append(res_str)
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# TiDB-specific: session variables
# ---------------------------------------------------------------------------


def set_session_variables(host: str, key_path: Path, port: int, workload: str):
    session_vars = SESSION_VARS_BASE
    if workload in ("oltp_read_only", "oltp_point_select"):
        session_vars += SESSION_VARS_READ_ONLY
    log(f"Setting session variables for {workload}...")
    log(f"  Recommended: {session_vars.replace(';', '; ')}")
