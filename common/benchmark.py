"""Unified benchmark orchestration for db-bench.

Provides shared sysbench functions for TiDB and Aurora, and routes Valkey
benchmarks to the remote runner (memtier_benchmark).  The CLI entry point
(``python3 -m common``) supports all three server types via ``--server-type``.
"""

import hashlib
import os
import re
import subprocess
import textwrap
import time
from pathlib import Path
from typing import Optional, Callable

from common.ssh import ssh_run_simple, ssh_capture_simple, scp_put_simple
from common.util import log

# ---------------------------------------------------------------------------
# Bench-client state discovery
# ---------------------------------------------------------------------------

def _load_bench_client(seed):
    """Try to load bench-client state from common/client-{seed}-state.json.

    Returns dict with ``public_ip`` and ``key_path`` keys, or empty dict.
    """
    from common.client import load_state
    state = load_state(seed)
    if state and state.get("public_ip") and state.get("key_path"):
        return state
    return {}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MYSQL_IGNORE_ERRORS = "1062,8002,8028,8249"
BYTES_PER_ROW = 250
DATA_TO_RAM_RATIO = 5

WORKLOADS = [
    "oltp_read_write",
    "oltp_read_only",
    "oltp_write_only",
    "oltp_point_select",
    "oltp_insert",
    "oltp_delete",
    "oltp_update_index",
    "oltp_update_non_index",
    "custom_iud",
    "custom_mixed",
]

# TiDB session variables for benchmarking (safe for all workloads)
SESSION_VARS_BASE = (
    "SET SESSION tikv_client_read_timeout=5;"
    "SET SESSION max_execution_time=10000;"
)

# Additional session vars for read-only workloads (TiDB specific)
SESSION_VARS_READ_ONLY = (
    "SET SESSION tidb_replica_read='closest-replicas';"
)

# IUD ratio targets (from production baseline)
IUD_INSERT_PCT = 68
IUD_UPDATE_PCT = 28
IUD_DELETE_PCT = 4

# Production baseline metrics for comparison (Aurora db.r6i.16xlarge)
PRODUCTION_BASELINE = {
    "instance_type": "db.r6i.16xlarge",
    "engine_version": "8.0.mysql_aurora.3.10.3",
    "storage_type": "aurora-iopt1",
    "insert_rate": 3600,
    "update_rate": 1500,
    "delete_rate": 200,
    "read_rate": 38300,
    "total_iud_rate": 5300,
    "queries_per_sec": 25900,
    "cpu_writer_avg_pct": 18.5,
    "commit_latency_ms": 1.7,
    "dml_latency_ms": 0.3,
    "write_throughput_mbs": 12.4,
}

# Instance memory lookup for auto-sizing (Aurora RDS + other managed DB instances)
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

# ---------------------------------------------------------------------------
# Workload profiles
# ---------------------------------------------------------------------------

WORKLOAD_PROFILES = {
    "quick": {
        "tables": 4, "table_size": 10000, "threads": 16,
        "duration": 30, "disk_fill_pct": 30,
    },
    "light": {
        "tables": 8, "table_size": 50000, "threads": 32,
        "duration": 60, "disk_fill_pct": 30,
    },
    "medium": {
        "tables": 16, "table_size": 100000, "threads": 64,
        "duration": 120, "disk_fill_pct": 30,
    },
    "heavy": {
        "tables": 32, "table_size": 500000, "threads": 128,
        "duration": 300, "disk_fill_pct": 30,
    },
    "standard": {
        "tables": 16, "table_size": 100000, "threads": 64,
        "duration": 300, "disk_fill_pct": 30,
    },
    "stress": {
        "tables": 16, "table_size": 100000, "threads": 64,
        "duration": 120, "multi_phase": "stress", "disk_fill_pct": 30,
    },
    "scaling": {
        "tables": 16, "table_size": 100000, "threads": 64,
        "duration": 120, "multi_phase": "scaling", "disk_fill_pct": 30,
    },
}

MULTI_PHASE_PROFILES = {
    "stress": {
        "description": "CPU stress test: ramp-up, push to breaking point, then recover",
        "phases": [
            {"name": "warmup", "threads": 32, "duration": 30},
            {"name": "ramp_up", "threads": 64, "duration": 30},
            {
                "name": "overload", "threads": 128, "duration": 30,
                "adaptive": True, "thread_step": 32, "max_threads": 512,
                "decay_threshold": 0.50, "latency_multiplier": 10,
            },
            {"name": "recovery", "threads": 64, "duration": 30},
            {"name": "sustained", "threads": 48, "duration": 45},
        ],
        "total_duration": "adaptive",
    },
    "scaling": {
        "description": "Traffic scaling test: gradual ramp-up then ramp-down",
        "phases": [
            {"name": "baseline", "threads": 16, "duration": 30},
            {"name": "scale_up_1", "threads": 32, "duration": 30},
            {"name": "scale_up_2", "threads": 64, "duration": 30},
            {"name": "peak", "threads": 96, "duration": 30},
            {"name": "scale_down_1", "threads": 64, "duration": 30},
            {"name": "scale_down_2", "threads": 32, "duration": 30},
        ],
        "total_duration": 180,
    },
}

# ---------------------------------------------------------------------------
# Internal paths and regex
# ---------------------------------------------------------------------------

_MODULE_DIR = Path(__file__).resolve().parent
_LUA_DIR = _MODULE_DIR / "lua"

# Interval report line regex -- handles both tidb and aurora sysbench output:
#   [ 10s ] thds: 16 tps: 355.96 qps: 7136.79 (r/w/o: 4998/1425/713) lat (ms,99%): 61.08 err/s: 0.00 reconn/s: 0.00
_INTERVAL_RE = re.compile(
    r'\[\s*([\d.]+)s\s*\]\s+thds:\s+(\d+)\s+'
    r'tps:\s+([\d.]+)\s+qps:\s+([\d.]+)\s+'
    r'.*?'
    r'lat\s+\(ms,\s*(\d+)%\):\s+([\d.]+)\s+'
    r'err/s:\s+([\d.]+)\s+'
    r'reconn/s:\s+([\d.]+)'
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _empty_result() -> dict:
    """Return a result dict with all unified keys set to None."""
    return {
        "tps": None,
        "qps": None,
        "latency_min_ms": None,
        "latency_avg_ms": None,
        "latency_p95_ms": None,
        "latency_p99_ms": None,
        "latency_max_ms": None,
        "reads": None,
        "writes": None,
        "other": None,
        "total_queries": None,
        "total_transactions": None,
        "ignored_errors": None,
        "reconnects": None,
        "availability_pct": None,
        "errors_per_sec": None,
    }


def _ssh_popen(host_ip: str, key_path: str, script: str,
               user: str = "ec2-user") -> subprocess.Popen:
    """Open a streaming SSH process that reads stdout line-by-line."""
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-o", "LogLevel=ERROR",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=120",
        "-i", str(key_path),
        f"{user}@{host_ip}",
        "bash", "-s",
    ]
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    full_script = textwrap.dedent(script).lstrip()
    proc.stdin.write(full_script)
    proc.stdin.close()
    return proc


def _combine_parallel_results(results: list[dict]) -> dict:
    """Combine results from multiple parallel sysbench processes.

    Throughput metrics (tps, qps, counts) are summed.
    Latency min/avg are averaged; max and percentiles take the max.
    """
    combined = _empty_result()

    for key in ("tps", "qps", "reads", "writes", "other",
                "total_queries", "total_transactions",
                "ignored_errors", "reconnects"):
        vals = [r.get(key) for r in results if r.get(key) is not None]
        if vals:
            combined[key] = sum(vals)

    vals = [r.get("errors_per_sec") for r in results
            if r.get("errors_per_sec") is not None]
    if vals:
        combined["errors_per_sec"] = round(sum(vals), 4)

    for key in ("latency_min_ms", "latency_avg_ms"):
        vals = [r.get(key) for r in results if r.get(key) is not None]
        if vals:
            combined[key] = round(sum(vals) / len(vals), 2)

    for key in ("latency_max_ms", "latency_p95_ms", "latency_p99_ms"):
        vals = [r.get(key) for r in results if r.get(key) is not None]
        if vals:
            combined[key] = max(vals)

    txns = combined.get("total_transactions") or 0
    errs = combined.get("ignored_errors") or 0
    if txns + errs > 0:
        combined["availability_pct"] = round(txns / (txns + errs) * 100, 4)
    elif txns > 0:
        combined["availability_pct"] = 100.0

    return combined


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def seeded_database_name(seed: str) -> str:
    """Derive a deterministic database name from a seed.

    Format: sb_<first-8-hex-of-sha256>.  Always the same for a given seed,
    making cleanup reliable across runs.
    """
    h = hashlib.sha256(seed.encode()).hexdigest()[:8]
    return f"sb_{h}"


def seeded_table_name(seed: str, table_index: int) -> str:
    """Derive a deterministic table name from seed and index.

    Uses the standard sysbench naming (sbtestN) so that built-in lua
    scripts work unmodified.
    """
    return f"sbtest{table_index}"


# ---------------------------------------------------------------------------
# Sizing helpers
# ---------------------------------------------------------------------------


def compute_table_size(instance_type: str, tables: int) -> int:
    """Compute rows per table so total data is DATA_TO_RAM_RATIO x instance RAM.

    Returns at least 1,000,000 rows, rounded down to the nearest million.
    """
    memory_gib = INSTANCE_MEMORY_GIB.get(instance_type)
    if not memory_gib:
        log(f"WARNING: Unknown instance type '{instance_type}', "
            f"falling back to 1M rows/table")
        return 1_000_000
    target_bytes = memory_gib * (1024 ** 3) * DATA_TO_RAM_RATIO
    rows_total = target_bytes // BYTES_PER_ROW
    table_size = rows_total // tables
    table_size = (table_size // 1_000_000) * 1_000_000
    return max(table_size, 1_000_000)


def compute_fill_params(instance_type: str, fill_tables: int,
                        seed_rows: int,
                        target_gb: Optional[int] = None) -> tuple[int, int]:
    """Return (doublings, estimated_final_gb) for the INSERT...SELECT fill phase.

    If *target_gb* is None the target is DATA_TO_RAM_RATIO x instance RAM.
    """
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
# Parsing
# ---------------------------------------------------------------------------


def parse_sysbench_output(text: str) -> dict:
    """Parse sysbench summary output into a unified result dict.

    Handles both tidb (--percentile=99, --histogram) and aurora (default
    percentile=95) output format variations.  All keys are always present;
    values are None when the corresponding metric was not found in *text*.
    """
    result = _empty_result()

    # -- Transactions: "transactions:  2000  (66.32 per sec.)" ----------------
    m = re.search(r'transactions:\s+(\d+)\s+\(([\d.]+)\s+per sec', text)
    if m:
        result["total_transactions"] = int(m.group(1))
        result["tps"] = float(m.group(2))

    # -- Queries summary: "queries:  40000  (1326.53 per sec.)" ---------------
    m = re.search(r'queries:\s+(\d+)\s+\(([\d.]+)\s+per sec', text)
    if m:
        result["total_queries"] = int(m.group(1))
        result["qps"] = float(m.group(2))

    # -- Query breakdown (in the "queries performed:" subsection) -------------
    m = re.search(r'^\s+read:\s+(\d+)', text, re.MULTILINE)
    if m:
        result["reads"] = int(m.group(1))
    m = re.search(r'^\s+write:\s+(\d+)', text, re.MULTILINE)
    if m:
        result["writes"] = int(m.group(1))
    m = re.search(r'^\s+other:\s+(\d+)', text, re.MULTILINE)
    if m:
        result["other"] = int(m.group(1))

    # Fallback for total_queries from the breakdown "total:" line
    if result["total_queries"] is None:
        m = re.search(r'^\s+total:\s+(\d+)\s*$', text, re.MULTILINE)
        if m:
            result["total_queries"] = int(m.group(1))

    # -- Ignored errors: "ignored errors:  0  (0.00 per sec.)" ---------------
    m = re.search(r'ignored errors:\s+(\d+)\s+\(([\d.]+)\s+per sec', text)
    if m:
        result["ignored_errors"] = int(m.group(1))
        result["errors_per_sec"] = float(m.group(2))

    # -- Reconnects: "reconnects:  0  (0.00 per sec.)" -----------------------
    m = re.search(r'reconnects:\s+(\d+)', text)
    if m:
        result["reconnects"] = int(m.group(1))

    # -- Latency block --------------------------------------------------------
    # Parse within the "Latency (ms):" section to avoid false matches from
    # other parts of the output.
    lat_block_match = re.search(
        r'Latency \(ms\):(.*?)(?:\n\s*\n|\Z)', text, re.DOTALL)
    if lat_block_match:
        block = lat_block_match.group(1)
        m = re.search(r'min:\s+([\d.]+)', block)
        if m:
            result["latency_min_ms"] = float(m.group(1))
        m = re.search(r'avg:\s+([\d.]+)', block)
        if m:
            result["latency_avg_ms"] = float(m.group(1))
        m = re.search(r'max:\s+([\d.]+)', block)
        if m:
            result["latency_max_ms"] = float(m.group(1))
        m = re.search(r'95th percentile:\s+([\d.]+)', block)
        if m:
            result["latency_p95_ms"] = float(m.group(1))
        m = re.search(r'99th percentile:\s+([\d.]+)', block)
        if m:
            result["latency_p99_ms"] = float(m.group(1))
        # Some sysbench versions output just "percentile:" when using
        # --percentile=N.  Assign to p99 if no specific percentile was found
        # (our default build uses --percentile=99).
        if result["latency_p95_ms"] is None and result["latency_p99_ms"] is None:
            m = re.search(r'(?<!\d\dth )percentile:\s+([\d.]+)', block)
            if m:
                result["latency_p99_ms"] = float(m.group(1))

    # -- Availability ---------------------------------------------------------
    txns = result["total_transactions"] or 0
    errs = result["ignored_errors"] or 0
    if txns + errs > 0:
        result["availability_pct"] = round(txns / (txns + errs) * 100, 4)
    elif txns > 0:
        result["availability_pct"] = 100.0

    return result


def parse_interval_line(line: str) -> Optional[dict]:
    """Parse a single sysbench interval report line.

    Returns None if the line is not an interval line.  Example input::

        [ 10s ] thds: 16 tps: 355.96 qps: 7136.79 (r/w/o: ...) lat (ms,99%): 61.08 err/s: 0.00 reconn/s: 0.00
    """
    m = _INTERVAL_RE.search(line)
    if not m:
        return None
    return {
        "elapsed_s": float(m.group(1)),
        "threads": int(m.group(2)),
        "tps": float(m.group(3)),
        "qps": float(m.group(4)),
        "latency_pct": int(m.group(5)),
        "latency_pct_ms": float(m.group(6)),
        "err_per_sec": float(m.group(7)),
        "reconn_per_sec": float(m.group(8)),
    }


def parse_interval_lines(text: str) -> list[dict]:
    """Parse all interval report lines from full sysbench output."""
    intervals = []
    for line in text.splitlines():
        iv = parse_interval_line(line)
        if iv:
            intervals.append(iv)
    return intervals


# ---------------------------------------------------------------------------
# Command building
# ---------------------------------------------------------------------------


def build_sysbench_cmd(
    host_ip: str,
    key_path: str,
    workload: str,
    threads: int,
    duration: int,
    tables: int,
    table_size: int,
    endpoint: str,
    port: int,
    user: str,
    password: str,
    db: str,
    report_interval: int,
    extra_args: Optional[list[str]] = None,
    lua_dir: Optional[str] = None,
) -> str:
    """Build a sysbench command string suitable for remote SSH execution.

    For custom workloads (``custom_iud``, ``custom_mixed``), the *lua_dir*
    parameter specifies where the lua scripts live on the remote host
    (default ``/tmp/lua``).  Call :func:`upload_lua_scripts` first to put
    them there.

    *host_ip* and *key_path* are accepted for API consistency but are not
    used by this function.
    """
    # Resolve workload argument
    if workload in ("custom_iud", "custom_mixed"):
        remote_lua_dir = lua_dir or "/tmp/lua"
        workload_arg = f"{remote_lua_dir}/{workload}.lua"
    else:
        workload_arg = workload

    parts = [f"sysbench {workload_arg}"]

    # Connection parameters
    parts.append(f"--mysql-host='{endpoint}'")
    parts.append(f"--mysql-port={port}")
    parts.append(f"--mysql-user={user}")
    if password:
        parts.append(f"--mysql-password='{password}'")
    parts.append(f"--mysql-db={db}")

    # Table / concurrency parameters
    parts.append(f"--tables={tables}")
    parts.append(f"--table-size={table_size}")
    parts.append(f"--threads={threads}")
    parts.append(f"--time={duration}")
    parts.append(f"--report-interval={report_interval}")

    # Defaults: 99th-percentile reporting, ignore known transient errors
    parts.append("--percentile=99")
    parts.append(f"--mysql-ignore-errors={MYSQL_IGNORE_ERRORS}")

    if extra_args:
        parts.extend(extra_args)

    parts.append("run")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Lua script upload
# ---------------------------------------------------------------------------


def upload_lua_scripts(host_ip: str, key_path: str) -> str:
    """SCP all ``common/lua/*.lua`` files to ``/tmp/lua/`` on the remote host.

    Returns the remote directory path (``/tmp/lua``).
    """
    remote_dir = "/tmp/lua"
    ssh_run_simple(host_ip, key_path, f"mkdir -p {remote_dir}", timeout=30)

    lua_files = sorted(_LUA_DIR.glob("*.lua"))
    if not lua_files:
        log(f"WARNING: No .lua files found in {_LUA_DIR}")
        return remote_dir

    for lua_file in lua_files:
        remote_path = f"{remote_dir}/{lua_file.name}"
        scp_put_simple(host_ip, key_path, str(lua_file), remote_path)
        log(f"Uploaded {lua_file.name} -> {remote_path}")

    return remote_dir


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def run_sysbench(host_ip: str, key_path: str, cmd_str: str,
                 timeout: int = 600) -> dict:
    """Run a sysbench command on the remote host, parse and return results.

    Args:
        host_ip: SSH target IP.
        key_path: Path to SSH private key.
        cmd_str: The sysbench command (from :func:`build_sysbench_cmd`).
        timeout: SSH command timeout in seconds.

    Returns:
        Unified result dict.  On failure an ``"error"`` key is present.
    """
    script = f"{cmd_str} 2>&1"
    start = time.time()
    r = ssh_run_simple(host_ip, key_path, script, timeout=timeout)
    elapsed = time.time() - start

    if r.returncode != 0:
        log(f"ERROR: sysbench failed (exit {r.returncode})")
        if r.stdout:
            for line in r.stdout.strip().splitlines()[-5:]:
                log(f"  {line}")
        return {"error": f"{r.stdout}\n{r.stderr}".strip()}

    result = parse_sysbench_output(r.stdout)
    result["intervals"] = parse_interval_lines(r.stdout)
    result["elapsed_s"] = round(elapsed, 1)
    result["raw_output"] = r.stdout
    return result


def run_sysbench_streaming(host_ip: str, key_path: str, cmd_str: str,
                           callback: Optional[Callable[[dict], None]] = None,
                           ) -> dict:
    """Run sysbench with real-time streaming output.

    Reads stdout line-by-line.  For each interval report line the parsed
    data is appended to the returned ``intervals`` list and, if *callback*
    is not None, ``callback(interval_data)`` is called.

    Non-interval lines are passed through to :func:`log`.

    Returns:
        Unified result dict (includes ``"intervals"`` and ``"raw_output"``).
    """
    proc = _ssh_popen(host_ip, key_path, f"{cmd_str} 2>&1")

    full_output: list[str] = []
    all_intervals: list[dict] = []
    start = time.time()

    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            full_output.append(line)

            iv = parse_interval_line(line)
            if iv:
                all_intervals.append(iv)
                if callback:
                    callback(iv)
            elif line.strip():
                log(line)
    except KeyboardInterrupt:
        proc.kill()
        raise
    finally:
        proc.wait()

    elapsed = time.time() - start
    full_text = "\n".join(full_output)
    result = parse_sysbench_output(full_text)
    result["intervals"] = all_intervals
    result["elapsed_s"] = round(elapsed, 1)
    result["raw_output"] = full_text
    return result


def run_sysbench_parallel(host_ip: str, key_path: str, base_cmd_str: str,
                          num_processes: int) -> dict:
    """Run *num_processes* parallel sysbench processes and combine results.

    Throughput metrics are summed; latency percentiles take the max across
    processes (conservative estimate).

    Returns:
        Combined unified result dict.  On failure ``"error"`` key is present.
    """
    parts = ["rm -f /tmp/sysbench_run_*.log"]
    for i in range(1, num_processes + 1):
        parts.append(f"{base_cmd_str} > /tmp/sysbench_run_{i}.log 2>&1 &")
    parts.append("wait")
    for i in range(1, num_processes + 1):
        parts.append(f"echo '===SYSBENCH_LOG_{i}_START==='")
        parts.append(f"cat /tmp/sysbench_run_{i}.log")
        parts.append(f"echo '===SYSBENCH_LOG_{i}_END==='")
    script = "\n".join(parts)

    # Derive timeout from --time= flag in the command + generous overhead
    duration_match = re.search(r'--time=(\d+)', base_cmd_str)
    duration = int(duration_match.group(1)) if duration_match else 600
    timeout = duration + 180

    log(f"Running {num_processes} parallel sysbench processes "
        f"(timeout={timeout}s)...")
    start = time.time()
    r = ssh_run_simple(host_ip, key_path, script, timeout=timeout)
    elapsed = time.time() - start

    if r.returncode != 0:
        log(f"ERROR: parallel sysbench failed (exit {r.returncode})")
        return {"error": f"{r.stdout}\n{r.stderr}".strip()}

    all_results: list[dict] = []
    for i in range(1, num_processes + 1):
        start_marker = f"===SYSBENCH_LOG_{i}_START==="
        end_marker = f"===SYSBENCH_LOG_{i}_END==="
        idx_s = r.stdout.find(start_marker)
        idx_e = r.stdout.find(end_marker)
        if idx_s == -1 or idx_e == -1:
            log(f"WARNING: missing output for sysbench process {i}")
            continue
        log_text = r.stdout[idx_s + len(start_marker):idx_e]
        parsed = parse_sysbench_output(log_text)
        if parsed.get("tps") is not None:
            all_results.append(parsed)

    if not all_results:
        return {"error": "No parseable sysbench output from any process"}

    combined = _combine_parallel_results(all_results)
    combined["elapsed_s"] = round(elapsed, 1)
    combined["num_processes"] = num_processes
    combined["per_process_results"] = all_results
    combined["raw_output"] = r.stdout
    return combined


# ---------------------------------------------------------------------------
# Data lifecycle -- prepare / cleanup / fast-fill
# ---------------------------------------------------------------------------


def sysbench_prepare(host_ip: str, key_path: str, endpoint: str, port: int,
                     user: str, password: str, db: str, tables: int,
                     table_size: int, threads: int = 1) -> None:
    """Prepare sysbench tables (CREATE DATABASE + ``sysbench ... prepare``).

    Creates the database if it does not exist, then runs the sysbench
    prepare phase which creates tables and loads initial data.
    """
    total_rows = tables * table_size
    est_gb = round(total_rows * BYTES_PER_ROW / 1e9, 1)
    log(f"Preparing sysbench: {tables} tables x {table_size:,} rows "
        f"(~{est_gb} GB), {threads} threads")

    # Build connection options
    pw_opt = f"-p'{password}'" if password else ""

    # Create database
    create_db_script = (
        f"mysql -h '{endpoint}' -P {port} -u {user} {pw_opt} "
        f"-e 'CREATE DATABASE IF NOT EXISTS {db}' 2>&1"
    )
    r = ssh_run_simple(host_ip, key_path, create_db_script, timeout=60)
    if r.returncode != 0:
        log(f"WARNING: CREATE DATABASE returned {r.returncode}: "
            f"{r.stdout.strip()} {r.stderr.strip()}")

    # Build sysbench prepare command
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user={user} "
    )
    if password:
        common += f"--mysql-password='{password}' "
    common += (
        f"--mysql-db={db} --tables={tables} --table-size={table_size} "
        f"--threads={threads}"
    )
    prepare_cmd = f"sysbench oltp_read_write {common} prepare 2>&1"

    # Use streaming Popen for progress reporting on large data loads
    proc = _ssh_popen(host_ip, key_path, prepare_cmd)
    tables_done = 0
    start = time.time()
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            if "Creating table" in line:
                tables_done += 1
                elapsed_min = (time.time() - start) / 60
                log(f"  [{tables_done}/{tables}] {line}  "
                    f"({elapsed_min:.1f} min)")
            elif "Inserting" in line or "FATAL" in line or "ERROR" in line:
                log(f"  {line}")
        proc.wait()
    except KeyboardInterrupt:
        proc.kill()
        proc.wait()
        raise

    elapsed_min = (time.time() - start) / 60
    if proc.returncode != 0:
        log(f"ERROR: sysbench prepare failed (exit {proc.returncode})")
        return
    log(f"  Prepare complete: {tables} tables, {total_rows:,} rows, "
        f"~{est_gb} GB in {elapsed_min:.1f} min")


def sysbench_cleanup(host_ip: str, key_path: str, endpoint: str, port: int,
                     user: str, password: str, db: str,
                     tables: int) -> None:
    """Drop sysbench tables via ``sysbench ... cleanup``."""
    log(f"Cleaning up sysbench tables in {db}...")
    common = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user={user} "
    )
    if password:
        common += f"--mysql-password='{password}' "
    common += f"--mysql-db={db} --tables={tables}"
    script = f"sysbench oltp_read_write {common} cleanup 2>&1"
    ssh_run_simple(host_ip, key_path, script, timeout=120)
    log("  Cleanup done")


def fast_fill(host_ip: str, key_path: str, endpoint: str, port: int,
              user: str, password: str, fill_db: str, fill_tables: int,
              seed_rows: int, doublings: int,
              fill_threads: int = 64) -> None:
    """Populate a database using INSERT...SELECT doubling.

    Creates *fill_tables* tables with *seed_rows* rows each via
    ``sysbench ... prepare``, then doubles the row count *doublings*
    times using ``INSERT INTO t (k,c,pad) SELECT k,c,pad FROM t``.
    """
    final_rows = seed_rows * (2 ** doublings)
    total_rows = final_rows * fill_tables
    est_gb = round(total_rows * BYTES_PER_ROW / 1e9, 1)
    pw_opt = f"-p'{password}'" if password else ""

    log("=== FILL PHASE ===")
    log(f"Target: {fill_tables} tables x {final_rows:,} rows/table "
        f"(~{est_gb} GB), {doublings} doublings from {seed_rows:,} seed")

    # Drop existing fill database
    log(f"Dropping existing '{fill_db}' (if any)...")
    drop_script = (
        f"mysql -h '{endpoint}' -P {port} -u {user} {pw_opt} "
        f"-e 'DROP DATABASE IF EXISTS {fill_db}' 2>&1"
    )
    ssh_run_simple(host_ip, key_path, drop_script, timeout=120)

    # Create fill database and seed tables
    create_script = (
        f"mysql -h '{endpoint}' -P {port} -u {user} {pw_opt} "
        f"-e 'CREATE DATABASE IF NOT EXISTS {fill_db}' 2>&1"
    )
    r = ssh_run_simple(host_ip, key_path, create_script, timeout=60)
    if r.returncode != 0:
        log(f"ERROR creating fill database: {r.stdout} {r.stderr}")
        return

    log(f"Creating {fill_tables} seed tables with {seed_rows:,} rows each...")
    sysbench_prepare(host_ip, key_path, endpoint, port, user, password,
                     fill_db, fill_tables, seed_rows, fill_threads)

    # Doubling loop
    for d in range(1, doublings + 1):
        rows_before = seed_rows * (2 ** (d - 1))
        rows_after = rows_before * 2
        batch_gb = round(rows_before * fill_tables * BYTES_PER_ROW / 1e9, 1)
        log(f"Doubling {d}/{doublings}: {rows_before:,} -> {rows_after:,} "
            f"rows/table (inserting ~{batch_gb} GB across {fill_tables} tables)...")

        script = _build_doubling_script(
            endpoint, port, user, password, fill_db,
            fill_tables, fill_threads,
        )
        start = time.time()
        r = ssh_run_simple(host_ip, key_path, script, timeout=7200)
        elapsed = time.time() - start
        if r.returncode != 0:
            log(f"ERROR during doubling {d}: "
                f"{r.stdout[:500]} {r.stderr[:500]}")
            return
        stdout_last = (r.stdout.strip().splitlines()[-1]
                       if r.stdout.strip() else "")
        if r.stderr.strip():
            for line in r.stderr.strip().splitlines()[:5]:
                log(f"  WARN: {line}")
        log(f"  Doubling {d} complete in {elapsed:.0f}s -- {stdout_last}")

    # Report final stats
    stats_script = (
        f"mysql -h '{endpoint}' -P {port} -u {user} {pw_opt} {fill_db} "
        f"-e \"SELECT COUNT(*) as tables_count, "
        f"SUM(table_rows) as total_rows, "
        f"ROUND(SUM(data_length + index_length)/1024/1024/1024, 2) as total_gb "
        f"FROM information_schema.tables "
        f"WHERE table_schema='{fill_db}'\" "
        f"--batch 2>/dev/null"
    )
    r = ssh_run_simple(host_ip, key_path, stats_script, timeout=60)
    log(f"Fill stats:\n{r.stdout.strip()}")
    log("=== FILL COMPLETE ===")


def _build_doubling_script(endpoint: str, port: int, user: str,
                           password: str, db: str, tables: int,
                           max_parallel: int) -> str:
    """Build a bash script that doubles every table via INSERT...SELECT."""
    pw_flag = f'-p"{password}"' if password else ""
    return f"""#!/bin/bash
DB_HOST='{endpoint}'
DB_PORT={port}
DB_USER='{user}'
DB_NAME='{db}'
ERRORS=0

do_insert() {{
    local tbl=$1
    if ! mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" {pw_flag} "$DB_NAME" \
         -e "INSERT INTO sbtest$tbl (k, c, pad) SELECT k, c, pad FROM sbtest$tbl" \
         2>/tmp/double_err_$tbl.log; then
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

FINAL_ROWS=$(mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" {pw_flag} "$DB_NAME" \
    -sN -e "SELECT SUM(table_rows) FROM information_schema.tables \
    WHERE table_schema='$DB_NAME'" 2>/dev/null)
echo "DOUBLING_COMPLETE errors=$ERRORS total_rows=$FINAL_ROWS"
"""


# ---------------------------------------------------------------------------
# Generalized benchmark runners (used by unified CLI)
# ---------------------------------------------------------------------------


def run_benchmark_streaming(
    host: str,
    key_path,
    cmd_str: str,
    stream_fn: Callable,
    parse_interval_fn: Callable = None,
    parse_output_fn: Callable = None,
    resource_fn: Optional[Callable[[], str]] = None,
    format_report_fn: Optional[Callable[[int, list, str], str]] = None,
) -> dict:
    """Run sysbench with real-time streaming and optional per-minute resource reports.

    Generalised from tidb/benchmark.py ``run_sysbench_benchmark()``.

    Args:
        host: SSH target (unused if stream_fn handles it).
        key_path: SSH key path.
        cmd_str: Full sysbench command string.
        stream_fn: ``stream_fn(host, cmd_str + " 2>&1", key_path)`` ->
            Popen-like object with ``.stdout`` iterable and ``.wait()``.
        parse_interval_fn: Parse an interval line (default:
            :func:`parse_interval_line`).
        parse_output_fn: Parse full sysbench text (default:
            :func:`parse_sysbench_output`).
        resource_fn: Optional callable returning a resource snapshot string
            at each minute boundary.
        format_report_fn: Optional ``(minute_num, intervals, resource_text)``
            -> formatted report string.

    Returns:
        Unified result dict with ``"workload"`` placeholder.
    """
    if parse_interval_fn is None:
        parse_interval_fn = parse_interval_line
    if parse_output_fn is None:
        parse_output_fn = parse_sysbench_output

    proc = stream_fn(host, cmd_str + " 2>&1", key_path)
    full_output: list[str] = []
    current_minute = 0
    minute_intervals: list[dict] = []
    all_intervals: list[dict] = []

    for raw_line in proc.stdout:
        line = raw_line.rstrip("\n")
        full_output.append(line)
        iv = parse_interval_fn(line)
        if iv:
            all_intervals.append(iv)
            elapsed_s = iv["elapsed_s"]
            minute_of = (int(elapsed_s) - 1) // 60 + 1
            if minute_of > current_minute:
                if current_minute > 0 and minute_intervals:
                    res_text = resource_fn() if resource_fn else ""
                    if format_report_fn:
                        report = format_report_fn(current_minute, minute_intervals, res_text)
                        log(report)
                        log("")
                current_minute = minute_of
                minute_intervals = []
            minute_intervals.append(iv)
        elif line.strip():
            log(line)

    proc.wait()

    # Flush last partial minute
    if minute_intervals:
        res_text = resource_fn() if resource_fn else ""
        if format_report_fn:
            report = format_report_fn(current_minute, minute_intervals, res_text)
            log(report)
            log("")

    full_text = "\n".join(full_output)
    result = parse_output_fn(full_text)
    result["intervals"] = all_intervals
    return result


def run_adaptive_phase(
    phase: dict,
    workload: str,
    build_cmd_fn: Callable[..., str],
    run_capture_fn: Callable[..., object],
    parse_output_fn: Callable = None,
) -> list:
    """Run an adaptive (overload-finding) phase with ramping thread counts.

    Generalised from tidb/benchmark.py ``run_adaptive_phase()``.

    Args:
        phase: Phase dict with keys ``threads``, ``thread_step``,
            ``max_threads``, ``decay_threshold``, ``latency_multiplier``,
            ``duration``.
        workload: Sysbench workload name.
        build_cmd_fn: ``(threads, duration) -> cmd_str``.  The caller is
            responsible for baking in tables/table_size/port/db/etc.
        run_capture_fn: ``(cmd_str) -> result`` where *result* has
            ``.stdout`` text attribute.
        parse_output_fn: Parse full sysbench text (default:
            :func:`parse_sysbench_output`).

    Returns:
        List of per-iteration result dicts.
    """
    if parse_output_fn is None:
        parse_output_fn = parse_sysbench_output

    results = []
    start_threads = phase["threads"]
    thread_step = phase.get("thread_step", 32)
    max_threads = phase.get("max_threads", 512)
    decay_threshold = phase.get("decay_threshold", 0.50)
    latency_multiplier = phase.get("latency_multiplier", 10)
    phase_duration = phase.get("duration", 30)

    current_threads = start_threads
    iteration = 0
    peak_qps = 0
    peak_threads = start_threads
    baseline_p95 = None

    log(f"    Adaptive mode: starting at {start_threads} threads, "
        f"step={thread_step}, max={max_threads}")
    log(f"    Breaking conditions: QPS decay >= {decay_threshold*100:.0f}% "
        f"OR latency >= {latency_multiplier}x baseline")

    while current_threads <= max_threads:
        iteration += 1
        log(f"")
        log(f"    >> Iteration {iteration}: {current_threads} threads, "
            f"{phase_duration}s")

        cmd = build_cmd_fn(current_threads, phase_duration)
        result = run_capture_fn(cmd)
        output = result.stdout
        print(output)

        metrics = parse_output_fn(output)
        metrics["phase"] = f"overload_iter{iteration}"
        metrics["threads"] = current_threads
        metrics["duration"] = phase_duration
        metrics["workload"] = workload
        results.append(metrics)

        current_qps = metrics.get("qps", 0) or 0
        current_p95 = metrics.get("latency_p95_ms", 0) or 0
        current_p99 = metrics.get("latency_p99_ms", 0) or 0
        avail = metrics.get("availability_pct", 100.0) or 100.0
        errors = metrics.get("ignored_errors", 0) or 0

        if baseline_p95 is None and current_p95 > 0:
            baseline_p95 = current_p95
            log(f"       Baseline P95: {baseline_p95:.1f}ms")

        if current_qps > peak_qps:
            peak_qps = current_qps
            peak_threads = current_threads

        decay_from_peak = ((peak_qps - current_qps) / peak_qps
                           if peak_qps > 0 else 0)
        latency_ratio = (current_p95 / baseline_p95
                         if baseline_p95 and baseline_p95 > 0 else 1)

        log(f"       QPS: {current_qps:,.1f} | Peak: {peak_qps:,.1f} "
            f"@ {peak_threads} thr | Decay: {decay_from_peak*100:.1f}%")
        log(f"       P95: {current_p95:.1f}ms P99: {current_p99:.1f}ms | "
            f"Ratio: {latency_ratio:.1f}x | Avail: {avail:.2f}% (err: {errors})")

        broken = False
        break_reason = ""
        if decay_from_peak >= decay_threshold:
            broken = True
            break_reason = (
                f"QPS decayed {decay_from_peak*100:.1f}% from peak "
                f"(threshold: {decay_threshold*100:.0f}%)")
        elif latency_ratio >= latency_multiplier:
            broken = True
            break_reason = (
                f"Latency {latency_ratio:.1f}x baseline "
                f"(threshold: {latency_multiplier}x)")

        if broken:
            log(f"")
            log(f"    !! BREAKING POINT REACHED: {break_reason}")
            log(f"    !! Peak QPS: {peak_qps:,.1f} at {peak_threads} threads")
            log(f"    !! Final: {current_qps:,.1f} QPS, "
                f"P95={current_p95:.1f}ms, P99={current_p99:.1f}ms "
                f"@ {current_threads} threads")
            break

        current_threads += thread_step
        if current_threads <= max_threads:
            log(f"       Ramping up to {current_threads} threads...")
            time.sleep(3)

    if current_threads > max_threads:
        log(f"    !! Reached max threads ({max_threads}) without breaking")
        log(f"    !! Peak QPS: {peak_qps:,.1f} at {peak_threads} threads")

    return results


def run_multi_phase_benchmark(
    profile_name: str,
    workload: str,
    build_cmd_fn: Callable[..., str],
    run_capture_fn: Callable[..., object],
    run_streaming_fn: Callable[..., dict],
    error_codes: str = "",
    parse_output_fn: Callable = None,
) -> list:
    """Run a multi-phase benchmark profile (warmup -> steady -> overload).

    Generalised from tidb/benchmark.py ``run_multi_phase_benchmark()``.

    Args:
        profile_name: Key in :data:`MULTI_PHASE_PROFILES`.
        workload: Sysbench workload name.
        build_cmd_fn: ``(threads, duration) -> cmd_str``.
        run_capture_fn: ``(cmd_str) -> result`` with ``.stdout``.
        run_streaming_fn: ``(threads, duration) -> result_dict``.
            Called for non-adaptive phases.
        error_codes: Ignored-error string for display.
        parse_output_fn: Parse full sysbench text (default:
            :func:`parse_sysbench_output`).

    Returns:
        List of per-phase result dicts.
    """
    if parse_output_fn is None:
        parse_output_fn = parse_sysbench_output

    profile = MULTI_PHASE_PROFILES[profile_name]
    log("")
    log("=" * 70)
    log(f"MULTI-PHASE BENCHMARK: {profile_name.upper()}")
    log(f"  {profile['description']}")

    total_phases = len(profile["phases"])
    if profile["total_duration"] == "adaptive":
        estimated = sum(p.get("duration", 30) for p in profile["phases"])
        log(f"  Phases: {total_phases} (adaptive duration, min ~{estimated}s)")
    else:
        log(f"  Total duration: {profile['total_duration']}s "
            f"({total_phases} phases)")
    if error_codes:
        log(f"  Error handling: ignoring {error_codes}")
    log("=" * 70)

    all_results = []
    for i, phase in enumerate(profile["phases"]):
        phase_name = phase["name"]
        threads = phase["threads"]
        duration = phase["duration"]
        is_adaptive = phase.get("adaptive", False)

        log("")
        log(f">>> PHASE {i+1}/{len(profile['phases'])}: {phase_name.upper()}")
        if is_adaptive:
            log(f"    Mode: ADAPTIVE (start={threads}, "
                f"step={phase.get('thread_step', 32)}, "
                f"max={phase.get('max_threads', 512)})")
        else:
            log(f"    Threads: {threads}, Duration: {duration}s")
        log("-" * 50)

        if is_adaptive:
            phase_results = run_adaptive_phase(
                phase, workload, build_cmd_fn, run_capture_fn,
                parse_output_fn,
            )
            all_results.extend(phase_results)
        else:
            metrics = run_streaming_fn(threads, duration)
            metrics["phase"] = phase_name
            metrics["threads"] = threads
            metrics["duration"] = duration
            all_results.append(metrics)

            tps = metrics.get("tps", 0) or 0
            qps = metrics.get("qps", 0) or 0
            p95 = metrics.get("latency_p95_ms", "N/A")
            p99 = metrics.get("latency_p99_ms", "N/A")
            avail = metrics.get("availability_pct", 100.0) or 100.0
            log(f"    Result: TPS={tps:,.1f} QPS={qps:,.1f} "
                f"P95={p95}ms P99={p99}ms Avail={avail:.2f}%")

    return all_results


# ---------------------------------------------------------------------------
# Unified CLI (parse_args + main)
# ---------------------------------------------------------------------------


def parse_args():
    """Unified argument parser for ``python3 -m common.benchmark``.

    Supports ``--server-type {aurora,tidb}`` with shared and
    server-specific arguments.
    """
    import argparse

    p = argparse.ArgumentParser(
        description="Run sysbench benchmarks against Aurora MySQL or TiDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python3 -m common.benchmark --server-type aurora
              python3 -m common.benchmark --server-type tidb --profile heavy
              python3 -m aurora.benchmark   (equivalent to --server-type aurora)
              python3 -m tidb.benchmark     (equivalent to --server-type tidb)
        """),
    )

    # Required: server type
    p.add_argument("--server-type", required=True,
                   choices=["aurora", "tidb", "valkey"],
                   help="Database server type to benchmark")

    p.add_argument("--action", choices=["run", "deploy", "status", "fetch"],
                   default="deploy",
                   help="Action: deploy (remote, default), run (interactive),"
                        " status (check), fetch (download)")

    # --- Shared arguments ---
    p.add_argument("--host", default=None,
                   help="Client/host IP (auto-discovered if omitted)")
    p.add_argument("--port", type=int, default=None,
                   help="Database port (default: server-specific)")
    p.add_argument("--region", default=None,
                   help="AWS region (default: us-east-1)")
    p.add_argument("--seed", default=None,
                   help="Stack seed (default: server-specific)")
    p.add_argument("--aws-profile", default=None,
                   help="AWS profile (default: env AWS_PROFILE or sandbox)")
    p.add_argument("--ssh-key", default=None,
                   help="Path to SSH private key")

    p.add_argument("--workload", default=None,
                   help="Sysbench workload type (default: server-specific)")
    p.add_argument("--tables", type=int, default=None,
                   help="Number of sysbench tables")
    p.add_argument("--table-size", type=int, default=None,
                   help="Rows per table")
    p.add_argument("--threads", type=int, default=None,
                   help="Number of concurrent threads")
    p.add_argument("--duration", type=int, default=None,
                   help="Benchmark duration in seconds")
    p.add_argument("--report-interval", type=int, default=None,
                   help="Report interval in seconds")
    p.add_argument("--skip-prepare", action="store_true",
                   help="Skip table preparation")
    p.add_argument("--cleanup", action="store_true",
                   help="Clean up tables after benchmark")
    p.add_argument("--verbose", "-v", action="store_true")

    p.add_argument("--endpoint", default=None,
                   help="Server endpoint (auto-discovered if omitted)")

    # --- Aurora-specific arguments ---
    aurora_g = p.add_argument_group("Aurora-specific options")
    aurora_g.add_argument("--password", default=None,
                          help="Aurora master password")
    aurora_g.add_argument("--db", default=None,
                          help="Database name (Aurora default: sbtest)")
    aurora_g.add_argument("--rds-profile", default=None,
                          help="AWS profile for CloudWatch RDS metrics")
    aurora_g.add_argument("--skip-iud-measurement", action="store_true",
                          help="Skip InnoDB row counter measurement")
    aurora_g.add_argument("--skip-cleanup", action="store_true",
                          help="Keep tables after benchmark (Aurora)")
    aurora_g.add_argument("--parallel", type=int, default=1,
                          help="Run N sysbench processes in parallel")
    aurora_g.add_argument("--prepare-threads", type=int, default=None,
                          help="Threads for parallel table creation (Aurora)")
    aurora_g.add_argument("--fill", action="store_true",
                          help="Run fill phase only (Aurora)")
    aurora_g.add_argument("--fill-target-gb", type=int, default=None,
                          help="Target fill data size in GB")
    aurora_g.add_argument("--fill-tables", type=int, default=None,
                          help="Number of fill tables")
    aurora_g.add_argument("--fill-seed-rows", type=int, default=None,
                          help="Seed rows per fill table")
    aurora_g.add_argument("--fill-threads", type=int, default=None,
                          help="Parallel threads for fill operations")
    aurora_g.add_argument("--fill-db", default=None,
                          help="Database name for fill data")

    # --- TiDB-specific arguments ---
    tidb_g = p.add_argument_group("TiDB-specific options")
    tidb_g.add_argument("--profile", choices=list(WORKLOAD_PROFILES.keys()),
                        default=None,
                        help="Workload profile (TiDB: quick/light/medium/heavy/stress/scaling)")
    tidb_g.add_argument("--prepare-only", action="store_true",
                        help="Only prepare tables (TiDB)")
    tidb_g.add_argument("--cleanup-only", action="store_true",
                        help="Only clean up tables (TiDB)")
    tidb_g.add_argument("--no-resource-monitor", action="store_true",
                        help="Disable per-minute resource monitoring (TiDB)")
    tidb_g.add_argument("--no-disk-fill", action="store_true",
                        help="Skip Phase 1 bulk data load (TiDB)")
    tidb_g.add_argument("--disk-fill-pct", type=int, default=None,
                        help="Override disk fill target percentage (TiDB)")
    tidb_g.add_argument("--output", "-o", default=None,
                        help="Output log file path (TiDB)")
    tidb_g.add_argument("--ticdc", action="store_true",
                        help="Enable TiCDC replication lag tracking")
    tidb_g.add_argument("--downstream-host", default=None,
                        help="Downstream TiDB cluster IP")
    tidb_g.add_argument("--downstream-port", type=int, default=None,
                        help="Downstream TiDB internal service port")

    return p.parse_args()


def main():
    """Unified entry point for ``python3 -m common.benchmark``."""
    args = parse_args()

    if args.action == "deploy":
        _action_deploy(args)
    elif args.action == "status":
        _action_status(args)
    elif args.action == "fetch":
        _action_fetch(args)
    elif args.server_type == "aurora":
        _main_aurora(args)
    elif args.server_type == "tidb":
        _main_tidb(args)
    elif args.server_type == "valkey":
        _main_valkey(args)
    else:
        raise SystemExit(f"Unknown server type: {args.server_type}")


# ---------------------------------------------------------------------------
# Remote runner actions (deploy / status / fetch)
# ---------------------------------------------------------------------------


def _resolve_host_and_key(args):
    """Resolve host IP and SSH key path from args and client state."""
    import sys

    seed = args.seed
    server_type = args.server_type

    client_state = _load_bench_client(seed) if seed else {}

    if args.host:
        host = args.host
    elif client_state.get("public_ip"):
        host = client_state["public_ip"]
    else:
        host = None

    if args.ssh_key:
        key_path = args.ssh_key
    elif client_state.get("key_path"):
        key_path = client_state["key_path"]
    else:
        base_dir = Path(__file__).resolve().parent.parent
        if server_type == "aurora":
            key_path = str(base_dir / "aurora" / "aurora-bench-key.pem")
        elif server_type == "valkey":
            key_path = str(base_dir / "valkey" / "valkey-load-test-key.pem")
        else:
            key_path = str(base_dir / "tidb" / "tidb-load-test-key.pem")

    if not host:
        log("ERROR: Could not determine host IP. "
            "Use --host or ensure client state exists (--seed).")
        sys.exit(1)

    return host, key_path


def _build_deploy_params(args):
    """Build the params dict for remote_runner.deploy() from CLI args."""
    server_type = args.server_type
    profile_name = getattr(args, "profile", None)

    if server_type == "tidb":
        seed = args.seed or "default"
        db = seeded_database_name(seed)
        port = args.port if args.port is not None else 4000
        user = "root"
        password = ""
        workload = args.workload or "oltp_read_write"

        client_state = _load_bench_client(seed)
        if client_state.get("public_ip"):
            from tidb.driver import discover_tidb_endpoint, DEFAULT_REGION, DEFAULT_PROFILE
            region = args.region or DEFAULT_REGION
            aws_profile = args.aws_profile or DEFAULT_PROFILE
            tidb_endpoint = discover_tidb_endpoint(region, aws_profile, seed)
        else:
            tidb_endpoint = "127.0.0.1"

        if profile_name:
            profile = WORKLOAD_PROFILES[profile_name]
            tables = args.tables if args.tables is not None else profile["tables"]
            table_size = (args.table_size if args.table_size is not None
                          else profile["table_size"])
            threads = (args.threads if args.threads is not None
                       else profile["threads"])
            duration = (args.duration if args.duration is not None
                        else profile["duration"])
            multi_phase_name = profile.get("multi_phase")
            multi_phase = (MULTI_PHASE_PROFILES[multi_phase_name]["phases"]
                           if multi_phase_name else None)
        else:
            tables = args.tables if args.tables is not None else 16
            table_size = args.table_size if args.table_size is not None else 100000
            threads = args.threads if args.threads is not None else 64
            duration = args.duration if args.duration is not None else 120
            multi_phase = None

        extra_args = [f"--mysql-ignore-errors={MYSQL_IGNORE_ERRORS}"]
        skip_trx = workload in ("oltp_read_only", "oltp_point_select")
        if skip_trx:
            extra_args.append("--skip_trx=true")

        return {
            "server_type": server_type,
            "endpoint": tidb_endpoint,
            "port": port,
            "user": user,
            "password": password,
            "db": db,
            "workload": workload,
            "tables": tables,
            "table_size": table_size,
            "threads": threads,
            "duration": duration,
            "report_interval": (args.report_interval
                                if args.report_interval is not None else 10),
            "extra_args": extra_args,
            "lua_dir": "/tmp/lua",
            "skip_prepare": args.skip_prepare,
            "skip_cleanup": not args.cleanup,
            "multi_phase": multi_phase,
        }

    elif server_type == "valkey":
        endpoint = getattr(args, "endpoint", None) or ""
        if not endpoint:
            seed = args.seed
            if seed:
                try:
                    endpoint = _discover_valkey_endpoint(
                        args.region or "us-east-1",
                        args.aws_profile or "sandbox",
                        seed)
                except Exception as exc:
                    log(f"WARNING: Could not discover Valkey endpoint: {exc}")

        if not endpoint:
            raise SystemExit(
                "ERROR: Valkey endpoint required. Use --endpoint or ensure "
                "stack is discoverable via --seed.")

        port = args.port if args.port is not None else 6379
        threads = args.threads if args.threads is not None else 4
        duration = args.duration if args.duration is not None else 120

        return {
            "server_type": "valkey",
            "endpoint": endpoint,
            "port": port,
            "threads": threads,
            "clients": 50,
            "data_size": 256,
            "key_pattern": "R:R",
            "ratio": "1:1",
            "pipeline": 10,
            "duration": duration,
        }

    else:  # aurora
        password = (args.password
                    or os.environ.get("AURORA_MASTER_PASSWORD", "BenchMark2024!"))
        port = args.port if args.port is not None else 3306
        db = args.db or "sbtest"
        workload = args.workload or "oltp_read_write"
        tables = args.tables if args.tables is not None else 16
        table_size = args.table_size if args.table_size is not None else 100000
        threads = args.threads if args.threads is not None else 64
        duration = args.duration if args.duration is not None else 300
        endpoint = getattr(args, "endpoint", None) or ""

        if not endpoint:
            seed = args.seed
            if seed:
                try:
                    from aurora.driver import discover_stack, DEFAULT_REGION, DEFAULT_PROFILE
                    region = args.region or DEFAULT_REGION
                    aws_profile = args.aws_profile or DEFAULT_PROFILE
                    script_dir = Path(__file__).resolve().parent.parent / "aurora"
                    info = discover_stack(script_dir, region, aws_profile, seed)
                    endpoint = info.get("endpoint", "")
                except Exception as exc:
                    log(f"WARNING: Could not discover Aurora endpoint: {exc}")

        if not endpoint:
            raise SystemExit(
                "ERROR: Aurora endpoint required. Use --endpoint or ensure "
                "stack is discoverable via --seed.")

        return {
            "server_type": server_type,
            "endpoint": endpoint,
            "port": port,
            "user": "admin",
            "password": password,
            "db": db,
            "workload": workload,
            "tables": tables,
            "table_size": table_size,
            "threads": threads,
            "duration": duration,
            "report_interval": (args.report_interval
                                if args.report_interval is not None else 10),
            "extra_args": [],
            "lua_dir": "/tmp/lua",
            "skip_prepare": args.skip_prepare,
            "skip_cleanup": getattr(args, "skip_cleanup", False),
        }


def _discover_valkey_endpoint(region, aws_profile, seed):
    """Discover Valkey NLB endpoint by seed-based naming convention."""
    import boto3
    session = boto3.Session(region_name=region, profile_name=aws_profile)
    elbv2 = session.client("elbv2")
    name_pattern = f"valkey-loadtest-{seed}-nlb"
    resp = elbv2.describe_load_balancers()
    for lb in resp.get("LoadBalancers", []):
        if lb["LoadBalancerName"] == name_pattern:
            return lb["DNSName"]
    raise RuntimeError(f"No NLB found matching '{name_pattern}'")


def _main_valkey(args):
    """Run Valkey benchmark interactively (action=run)."""
    raise SystemExit(
        "Valkey interactive run is not supported via the unified CLI.\n"
        "Use --action deploy to run autonomously on the client VM, or\n"
        "run directly: python3 -m valkey.benchmark --seed <seed>")


def _action_deploy(args):
    """Deploy a benchmark to run autonomously on the remote host."""
    from common.remote_runner import deploy

    host, key_path = _resolve_host_and_key(args)
    params = _build_deploy_params(args)

    server_type = params["server_type"]
    needs_lua = params.get("workload", "") in ("custom_iud", "custom_mixed")

    log(f"Deploying {server_type} benchmark to {host}...")
    if server_type == "valkey":
        log(f"  Endpoint : {params['endpoint']}:{params['port']}")
        log(f"  Threads  : {params['threads']}, Clients: {params['clients']}")
        log(f"  Duration : {params['duration']}s")
    else:
        log(f"  Workload : {params['workload']}")
        log(f"  Tables   : {params['tables']} x {params['table_size']:,} rows")
        log(f"  Threads  : {params['threads']}, Duration: {params['duration']}s")

    result = deploy(host, key_path, params, lua_files=needs_lua)
    log("")
    log(f"Benchmark is running in tmux session '{result['tmux_session']}'")
    log(f"Check progress:  python3 -m common.benchmark "
        f"--server-type {args.server_type} --action status"
        + (f" --host {host}" if args.host else "")
        + (f" --seed {args.seed}" if args.seed else ""))


def _action_status(args):
    """Check the status of a remote benchmark."""
    from common.remote_runner import status as remote_status

    host, key_path = _resolve_host_and_key(args)

    log(f"Checking benchmark status on {host}...")
    st = remote_status(host, key_path)

    running_str = "RUNNING" if st["running"] else "STOPPED"
    log(f"  Status     : {running_str}")
    log(f"  Phase      : {st['phase']}")
    log(f"  Started at : {st['started_at'] or 'N/A'}")
    if st.get("progress"):
        log(f"  Progress   : {st['progress']}")
    if st.get("completed_at"):
        log(f"  Completed  : {st['completed_at']}")
    if st.get("error"):
        log(f"  Error      : {st['error']}")
    log(f"  Results dir: {st['results_dir'] or 'N/A'}")


def _action_fetch(args):
    """Fetch benchmark results from the remote host."""
    from common.remote_runner import fetch

    host, key_path = _resolve_host_and_key(args)

    local_dir = None
    local_path = fetch(host, key_path, local_dir)
    log(f"Results downloaded to: {local_path}")


# ---------------------------------------------------------------------------
# Aurora main
# ---------------------------------------------------------------------------


def _main_aurora(args):
    """Run Aurora benchmark flow (fill or benchmark)."""
    import sys
    from datetime import datetime, timezone

    from aurora.driver import (
        DEFAULT_REGION, DEFAULT_SEED, DEFAULT_PROFILE, KEY_NAME,
        DEFAULT_PORT, DEFAULT_DB, DEFAULT_WORKLOAD, WORKLOADS as AURORA_WORKLOADS,
        DEFAULT_THREADS, DEFAULT_DURATION, DEFAULT_TABLES,
        DEFAULT_REPORT_INTERVAL, DEFAULT_PREPARE_THREADS,
        DEFAULT_FILL_DB, DEFAULT_FILL_TABLES, DEFAULT_FILL_SEED_ROWS,
        DEFAULT_FILL_THREADS, DEFAULT_BENCH_TABLE_SIZE,
        VERBOSE,
        discover_stack, get_aurora_cpu, get_aurora_network_metrics,
        get_innodb_rows, print_results, save_results,
    )
    import aurora.driver as adrv
    from common.sampler import (start_sampler, stop_sampler, parse_metrics_csv,
                                derive_interval_metrics, safe_stats, render_history_chart)
    from common.report import _print_client_extended_metrics, _print_resource_history

    # Apply defaults for unset args
    region = args.region or DEFAULT_REGION
    seed = args.seed or DEFAULT_SEED
    aws_profile = args.aws_profile or DEFAULT_PROFILE
    port = args.port or DEFAULT_PORT
    db = args.db or DEFAULT_DB
    workload = args.workload or DEFAULT_WORKLOAD
    threads = args.threads if args.threads is not None else DEFAULT_THREADS
    duration = args.duration if args.duration is not None else DEFAULT_DURATION
    tables = args.tables if args.tables is not None else DEFAULT_TABLES
    table_size = args.table_size  # None means auto-compute
    report_interval = (args.report_interval if args.report_interval is not None
                       else DEFAULT_REPORT_INTERVAL)
    prepare_threads = (args.prepare_threads if args.prepare_threads is not None
                       else DEFAULT_PREPARE_THREADS)
    fill_tables = (args.fill_tables if args.fill_tables is not None
                   else DEFAULT_FILL_TABLES)
    fill_seed_rows = (args.fill_seed_rows if args.fill_seed_rows is not None
                      else DEFAULT_FILL_SEED_ROWS)
    fill_threads = (args.fill_threads if args.fill_threads is not None
                    else DEFAULT_FILL_THREADS)
    fill_db = args.fill_db or DEFAULT_FILL_DB

    if workload not in AURORA_WORKLOADS:
        raise SystemExit(
            f"Invalid workload '{workload}' for Aurora. "
            f"Choose from: {', '.join(AURORA_WORKLOADS)}")

    adrv.VERBOSE = args.verbose
    password = (args.password
                or os.environ.get("AURORA_MASTER_PASSWORD", "BenchMark2024!"))
    script_dir = Path(__file__).resolve().parent.parent / "aurora"

    client_state = _load_bench_client(seed)

    if args.ssh_key:
        key_path = args.ssh_key
    elif client_state.get("key_path"):
        key_path = client_state["key_path"]
    else:
        key_path = str(script_dir / f"{KEY_NAME}.pem")

    if not os.path.isfile(key_path):
        log(f"WARNING: SSH key not found at {key_path}")

    info = discover_stack(script_dir, region, aws_profile, seed)
    host = args.host or client_state.get("public_ip") or info.get("client_public_ip", "")
    endpoint = args.endpoint or info.get("endpoint", "")
    port = args.port or info.get("cluster_port", DEFAULT_PORT)

    if not host or not endpoint:
        log("ERROR: Could not determine host or endpoint. "
            "Run aurora_setup.py first or pass --host/--endpoint.")
        sys.exit(1)

    instance_type = info.get("aurora_instance_type", "")

    # --- Fill mode ---
    if args.fill:
        log(f"Stack: {info.get('stack', 'unknown')}")
        log(f"Aurora endpoint: {endpoint}")
        log(f"EC2 client: {host}")
        doublings, est_gb = compute_fill_params(
            instance_type, fill_tables, fill_seed_rows,
            args.fill_target_gb)
        fast_fill(host, key_path, endpoint, port, "admin", password,
                  fill_db, fill_tables, fill_seed_rows,
                  doublings, fill_threads)
        log("Fill done. Run without --fill to start benchmark.")
        return

    # --- Benchmark mode ---
    if table_size is None:
        table_size = DEFAULT_BENCH_TABLE_SIZE
        total_gb = round(tables * table_size * BYTES_PER_ROW / 1e9, 1)
        log(f"Benchmark tables: {tables} x {table_size:,} rows "
            f"(~{total_gb} GB)")

    thread_label = (f"{args.parallel}x{threads}"
                    if args.parallel > 1 else str(threads))

    log(f"Stack: {info.get('stack', 'unknown')}")
    log(f"Aurora endpoint: {endpoint}")
    log(f"EC2 client: {host}")
    log(f"Workload: {workload}, threads: {thread_label}, "
        f"duration: {duration}s")
    print()

    # Upload Lua scripts
    lua_dir = None
    if workload in ("custom_iud", "custom_mixed"):
        lua_dir = upload_lua_scripts(host, key_path)

    if not args.skip_prepare:
        sysbench_prepare(host, key_path, endpoint, port, "admin", password,
                         db, tables, table_size, prepare_threads)

    iud_rates = None
    rows_before = None
    if not args.skip_iud_measurement:
        log("Taking InnoDB row counter snapshot (before)...")
        rows_before = get_innodb_rows(host, key_path, endpoint, port, password)

    cmd_str = build_sysbench_cmd(
        host_ip=host, key_path=key_path,
        workload=workload, threads=threads,
        duration=duration, tables=tables,
        table_size=table_size, endpoint=endpoint,
        port=port, user="admin", password=password,
        db=db, report_interval=report_interval,
        lua_dir=lua_dir,
    )

    # Start client resource sampler
    sampler_csv_path = f"/tmp/aurora_sampler_{int(time.time())}.csv"
    sampler_started = False
    try:
        start_sampler(host, key_path, 'aurora', interval=1, user='ec2-user',
                      mysql_host=endpoint, mysql_user='admin',
                      mysql_password=password, mysql_port=str(port))
        sampler_started = True
    except Exception as e:
        log(f"Warning: could not start sampler: {e}")

    bench_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if args.parallel > 1:
        result = run_sysbench_parallel(host, key_path, cmd_str, args.parallel)
    else:
        result = run_sysbench(host, key_path, cmd_str,
                              timeout=duration + 120)

    if "error" in result:
        log("Benchmark failed.")
        sys.exit(1)

    bench_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Stop sampler and collect metrics
    cpu_samples = []
    interval_data = []
    sampler_ws = {}
    client_cpu_pct = None
    if sampler_started:
        try:
            stop_sampler(host, key_path, sampler_csv_path, user='ec2-user')
            metrics_rows = parse_metrics_csv(sampler_csv_path)
            if metrics_rows:
                bench_start_epoch = metrics_rows[0].get('epoch', 0)
                bench_end_epoch = metrics_rows[-1].get('epoch', 0)
                cpu_samples, _ = derive_interval_metrics(
                    metrics_rows, bench_start_epoch, bench_end_epoch, 'aurora')
                cpu_vals = [s['cpu_pct'] for s in cpu_samples if s.get('cpu_pct') is not None]
                if cpu_vals:
                    client_cpu_pct = round(sum(cpu_vals) / len(cpu_vals), 1)
                interval_data = cpu_samples
                sampler_ws = {
                    'client_thermal_mc': safe_stats([s.get('client_thermal_mc') for s in cpu_samples]),
                    'client_loadavg_1m': safe_stats([s.get('client_loadavg_1m') for s in cpu_samples]),
                    'client_loadavg_5m': safe_stats([s.get('client_loadavg_5m') for s in cpu_samples]),
                    'client_loadavg_15m': safe_stats([s.get('client_loadavg_15m') for s in cpu_samples]),
                    'client_cpufreq_avg_mhz': safe_stats([s.get('client_cpufreq_avg_mhz') for s in cpu_samples]),
                }
        except Exception as e:
            log(f"Warning: could not collect sampler metrics: {e}")

    result.setdefault("workload", workload)
    result.setdefault("threads", threads)
    result["duration_s"] = duration

    if args.parallel > 1:
        result["parallel"] = args.parallel
        result["thread_label"] = f"{args.parallel}x{threads}"
    else:
        result["parallel"] = 1
        result["thread_label"] = str(threads)

    if result.get("latency_p95_ms") and not result.get("latency_95th_pct_ms"):
        result["latency_95th_pct_ms"] = result["latency_p95_ms"]

    if not args.skip_iud_measurement and rows_before is not None:
        log("Taking InnoDB row counter snapshot (after)...")
        rows_after = get_innodb_rows(host, key_path, endpoint, port, password)
        elapsed = result.get("elapsed_s", duration)
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
    rds_profile = args.rds_profile or aws_profile
    if writer_id:
        log("Fetching Aurora CPU from CloudWatch...")
        cpu_pct = get_aurora_cpu(writer_id, region, rds_profile,
                                 bench_start, bench_end)
        if cpu_pct is not None:
            log(f"  Aurora writer CPU avg: {cpu_pct:.1f}%")
        else:
            log("  Could not retrieve CPU metrics "
                "(may need 1-2 min to appear)")

        log("Fetching Aurora network metrics from CloudWatch...")
        net_metrics = get_aurora_network_metrics(
            writer_id, region, rds_profile, bench_start, bench_end)
        recv = net_metrics.get("aurora_net_recv_mbps")
        xmit = net_metrics.get("aurora_net_xmit_mbps")
        if recv is not None:
            log(f"  Network recv: {recv:.1f} Mbps")
        if xmit is not None:
            log(f"  Network xmit: {xmit:.1f} Mbps")

    print_results(result, iud_rates, cpu_pct, tables, table_size,
                  client_cpu_pct, net_metrics)
    save_results(result, iud_rates, script_dir, workload,
                 state=info, tables=tables, table_size=table_size,
                 cpu_pct=cpu_pct, client_cpu_pct=client_cpu_pct,
                 net_metrics=net_metrics, parallel=args.parallel)

    if sampler_ws or interval_data:
        fake_result = {
            'window_stats': sampler_ws,
            'interval_data': interval_data,
            'ec2_instance_type': info.get('ec2_instance_type', ''),
        }
        _print_client_extended_metrics([fake_result])
        _print_resource_history([fake_result])

    if not args.skip_cleanup:
        sysbench_cleanup(host, key_path, endpoint, port, "admin", password,
                         db, tables)

    log("Done.")


# ---------------------------------------------------------------------------
# TiDB main
# ---------------------------------------------------------------------------


def _main_tidb(args):
    """Run TiDB benchmark flow (prepare/cleanup/benchmark)."""
    from datetime import datetime

    import common.util as _cu
    from tidb.driver import (
        DEFAULT_REGION, DEFAULT_SEED, DEFAULT_PROFILE, DEFAULT_PORT,
        INTERNAL_SERVICE_PORT, DEFAULT_DATABASE, DEFAULT_EBS_SIZE_GB,
        TIDB_MYSQL_IGNORE_ERRORS, AWS_COSTS,
        ssh_run, ssh_capture, ssh_stream,
        discover_tidb_host, get_instance_info, get_cluster_info,
        print_cluster_summary, fetch_resource_snapshot_compact,
        fetch_final_resource_snapshot, get_disk_utilization,
        calculate_bulk_load_params, run_bulk_data_load,
        run_sysbench_prepare, run_sysbench_cleanup,
        format_minute_report, set_session_variables,
        CdcLagTracker,
    )
    from common.report import CostTracker, print_summary

    # Apply defaults for unset args
    region = args.region or DEFAULT_REGION
    seed = args.seed or DEFAULT_SEED
    aws_profile = args.aws_profile or DEFAULT_PROFILE
    port = args.port if args.port is not None else DEFAULT_PORT
    database = DEFAULT_DATABASE

    workload = args.workload or "oltp_read_write"
    tables_arg = args.tables if args.tables is not None else 16
    table_size_arg = args.table_size if args.table_size is not None else 100000
    threads_arg = args.threads if args.threads is not None else 64
    duration_arg = args.duration if args.duration is not None else 120
    report_interval = (args.report_interval if args.report_interval is not None
                       else 10)
    profile_name = args.profile or "heavy"
    disk_fill_pct = args.disk_fill_pct
    downstream_port = (args.downstream_port if args.downstream_port is not None
                       else INTERNAL_SERVICE_PORT)

    client_state = _load_bench_client(seed)

    if not args.host and client_state.get("public_ip"):
        args.host = client_state["public_ip"]
        log(f"Using bench-client from state file: {args.host}")

    if args.ssh_key:
        ssh_key_resolved = args.ssh_key
    elif client_state.get("key_path"):
        ssh_key_resolved = client_state["key_path"]
    else:
        ssh_key_resolved = str(
            Path(__file__).resolve().parent.parent / "tidb" /
            "tidb-load-test-key.pem")
    key_path = Path(ssh_key_resolved).expanduser().resolve()

    # Log file setup
    log_path = None
    if args.output and args.output.lower() != "none":
        log_path = Path(args.output).expanduser().resolve()
    elif args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = Path(
            f"tidb_benchmark_{profile_name}_{timestamp}.log").resolve()

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        _cu.LOG_TO_FILE = log_path
        print(f"Logging output to: {log_path}")

    try:
        _run_tidb_benchmark(
            args, key_path, region, seed, aws_profile, port, database,
            workload, tables_arg, table_size_arg, threads_arg, duration_arg,
            report_interval, profile_name, disk_fill_pct, downstream_port,
        )
    finally:
        if log_path:
            print(f"\nLog saved to: {log_path}")


def _run_tidb_benchmark(
    args, key_path, region, seed, aws_profile, port, database,
    workload, tables, table_size, threads, duration,
    report_interval, profile_name, disk_fill_pct, downstream_port,
):
    """Inner TiDB benchmark logic (matches tidb/benchmark.py _run_benchmark)."""
    from tidb.driver import (
        DEFAULT_EBS_SIZE_GB, TIDB_MYSQL_IGNORE_ERRORS, AWS_COSTS,
        ssh_run, ssh_capture, ssh_stream,
        discover_tidb_host, discover_tidb_endpoint,
        get_instance_info, get_cluster_info,
        print_cluster_summary, fetch_resource_snapshot_compact,
        fetch_final_resource_snapshot, get_disk_utilization,
        run_bulk_data_load, run_sysbench_prepare, run_sysbench_cleanup,
        format_minute_report, CdcLagTracker,
    )
    from common.report import CostTracker, print_summary
    from common.sampler import (
        start_sampler, stop_sampler, parse_metrics_csv,
        derive_interval_metrics, safe_stats, render_history_chart,
    )
    from common.report import _print_client_extended_metrics, _print_resource_history

    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")

    client_state = _load_bench_client(seed)

    if args.host:
        host = args.host
    else:
        log("Discovering TiDB host from EC2 tags...")
        host = discover_tidb_host(region, aws_profile, seed)
    log(f"Using TiDB host: {host}")

    if client_state.get("public_ip"):
        db_host = discover_tidb_endpoint(region, aws_profile, seed)
        log(f"Using TiDB endpoint: {db_host}:{port} (from control node private IP)")
    else:
        db_host = "127.0.0.1"

    # Determine benchmark execution host (sysbench runs on client VM when
    # a decoupled benchmark client exists; cluster management stays on
    # the control node via host/key_path).
    if client_state and client_state.get("public_ip"):
        bench_host = client_state["public_ip"]
        bench_key = Path(client_state["key_path"])
        log(f"Using decoupled client for benchmark: {bench_host}")
    else:
        bench_host = host
        bench_key = key_path

    database = seeded_database_name(seed)
    log(f"Using seeded database name: {database} (seed={seed})")

    if args.cleanup_only:
        run_sysbench_cleanup(bench_host, bench_key, tables, port, database,
                             db_host=db_host)
        log("Cleanup complete.")
        return

    multi_phase = None
    if profile_name:
        profile = WORKLOAD_PROFILES[profile_name]
        tables = profile["tables"]
        table_size = profile["table_size"]
        threads = profile["threads"]
        duration = profile["duration"]
        multi_phase = profile.get("multi_phase")
        if disk_fill_pct is None:
            disk_fill_pct = profile.get("disk_fill_pct", 30)
        log(f"Using profile '{profile_name}': {tables} tables, "
            f"{threads} threads, {duration}s")
        if multi_phase:
            log(f"  Multi-phase mode: {multi_phase}")
    else:
        if disk_fill_pct is None:
            disk_fill_pct = 30

    try:
        print_cluster_summary(host, key_path, region, aws_profile, seed, port,
                              db_host=db_host)
    except Exception as exc:
        log(f"WARNING: Could not fetch cluster summary "
            f"(AWS creds may be expired): {exc}")
        log("Continuing with benchmark...")

    log("Disabling TiDB resource control (avoids error 8249)...")
    ssh_run(host, f"""
mysql -h {db_host} -P {port} -u root -e \
"SET GLOBAL tidb_enable_resource_control = OFF;" 2>/dev/null || true
""", key_path, strict=False)

    # Initialize cost tracker
    try:
        inst_info = get_instance_info(region, aws_profile, seed)
    except Exception:
        inst_info = {}

    role_types = {}
    for inst in inst_info.get("instances", []):
        role = inst.get("role", "unknown")
        itype = inst.get("type", "")
        base_role = role.rsplit("-", 1)[0] if "-" in role else role
        if base_role not in role_types and itype:
            role_types[base_role] = itype
    default_type = role_types.get(
        "host", role_types.get("client", "c8g.4xlarge"))
    server_type = role_types.get("tikv", default_type)

    cluster_info = get_cluster_info(host, key_path, port, db_host=db_host)
    server_count = (cluster_info.get("tidb_count", 2) +
                    cluster_info.get("tikv_count", 3) +
                    cluster_info.get("pd_count", 3))
    if args.ticdc:
        server_count += 8

    _disk_probe = get_disk_utilization(
        host, key_path, port, DEFAULT_EBS_SIZE_GB, database,
        db_host=db_host)
    detected_ebs_gb = _disk_probe.get("ebs_total_gb", DEFAULT_EBS_SIZE_GB)
    log(f"Detected EBS volume size: {detected_ebs_gb}GB")

    cost_tracker = CostTracker("tidb", server_type, region)
    cost_tracker.set_infrastructure(
        server_count=server_count, ebs_gb=detected_ebs_gb)
    cost_tracker.start()

    benchmark_start_time = time.time()

    # Start client resource sampler
    sampler_csv_path = f"/tmp/tidb_sampler_{int(time.time())}.csv"
    sampler_started = False
    try:
        start_sampler(bench_host, bench_key, 'generic', interval=1,
                      user='ec2-user')
        sampler_started = True
    except Exception as e:
        log(f"Warning: could not start sampler: {e}")

    cdc_tracker = None
    if args.ticdc:
        cdc_tracker = CdcLagTracker(
            host=host, key_path=key_path,
            downstream_port=downstream_port)
        cdc_tracker.start()

    # Phase 1: Bulk data load
    load_stats = None
    if not args.no_disk_fill and not args.skip_prepare:
        load_stats = run_bulk_data_load(
            host, key_path, target_disk_pct=disk_fill_pct,
            num_tables=tables, port=port, database=database,
            ebs_size_gb=detected_ebs_gb, db_host=db_host,
            bench_host=bench_host, bench_key=bench_key,
        )
        table_size = load_stats["rows_per_table"]
        log(f"Phase 1 complete: {load_stats['total_rows']:,} rows loaded")
        log(f"  Actual disk: {load_stats['disk_after']['ebs_used_pct']}% "
            f"of {load_stats['disk_after']['ebs_total_gb']}GB EBS")
    elif not args.skip_prepare:
        run_sysbench_prepare(bench_host, bench_key, tables, table_size, port,
                             database, db_host=db_host)

    if args.prepare_only:
        log("Tables prepared. Skipping benchmark (--prepare-only).")
        return

    # Phase 2: Benchmark
    log("")
    log("=" * 70)
    log("PHASE 2: BENCHMARK (updates + reads on loaded data)")
    log("=" * 70)

    server_specs = AWS_COSTS["ec2"].get(server_type, {})
    server_hourly = server_specs.get("hourly", 0.29)
    tikv_count = cluster_info.get("tikv_count", 3)
    ebs_monthly = (detected_ebs_gb * tikv_count *
                   AWS_COSTS["ebs"]["gp3_per_gb_month"])
    server_compute_monthly = server_hourly * server_count * 730
    server_total_monthly = server_compute_monthly + ebs_monthly
    log("")
    log(f"Server Monthly Cost ({server_count} hosts): "
        f"EC2=${server_compute_monthly:.0f} + EBS=${ebs_monthly:.0f} "
        f"= ${server_total_monthly:.0f}/mo")
    log("")

    # Build closures for the generalized runners
    skip_trx = workload in ("oltp_read_only", "oltp_point_select")

    def _build_cmd(w_threads, w_duration):
        extra = [f"--mysql-ignore-errors={TIDB_MYSQL_IGNORE_ERRORS}"]
        if skip_trx:
            extra.append("--skip_trx=true")
        return build_sysbench_cmd(
            host_ip="", key_path="", workload=workload,
            threads=w_threads, duration=w_duration, tables=tables,
            table_size=table_size, endpoint=db_host, port=port,
            user="root", password="", db=database,
            report_interval=report_interval, extra_args=extra,
        )

    def _run_capture(cmd_str):
        return ssh_capture(bench_host, cmd_str + " 2>&1", bench_key)

    def _resource_fn():
        return fetch_resource_snapshot_compact(host, key_path, port,
                                               db_host=db_host)

    def _run_streaming(w_threads, w_duration):
        cmd = _build_cmd(w_threads, w_duration)
        log(f"Running sysbench {workload} benchmark")
        log(f"  Tables: {tables}, Table size: {table_size:,}")
        log(f"  Threads: {w_threads}, Duration: {w_duration}s")
        log(f"  Error handling: ignoring errors "
            f"{TIDB_MYSQL_IGNORE_ERRORS}")
        if skip_trx:
            log(f"  Read-only mode: --skip_trx=true")
        result = run_benchmark_streaming(
            bench_host, bench_key, cmd, ssh_stream,
            resource_fn=_resource_fn,
            format_report_fn=format_minute_report,
        )
        result["workload"] = workload
        return result

    total_queries = 0
    total_transactions = 0
    avg_qps = 0
    avg_tps = 0

    if multi_phase:
        phase_results = run_multi_phase_benchmark(
            multi_phase, workload,
            _build_cmd, _run_capture, _run_streaming,
            error_codes=TIDB_MYSQL_IGNORE_ERRORS,
        )
        print_summary(phase_results, "tidb")
        total_duration = 0
        weighted_qps = 0
        weighted_tps = 0
        for r in phase_results:
            total_queries += r.get("total_queries", 0) or 0
            total_transactions += r.get("total_transactions", 0) or 0
            phase_dur = r.get("duration", 0) or 0
            total_duration += phase_dur
            weighted_qps += (r.get("qps", 0) or 0) * phase_dur
            weighted_tps += (r.get("tps", 0) or 0) * phase_dur
        if total_duration > 0:
            avg_qps = weighted_qps / total_duration
            avg_tps = weighted_tps / total_duration
    else:
        benchmark_metrics = _run_streaming(threads, duration)
        print_summary(benchmark_metrics, "tidb")
        total_queries = benchmark_metrics.get("total_queries", 0) or 0
        total_transactions = (benchmark_metrics.get("total_transactions", 0)
                              or 0)
        avg_qps = benchmark_metrics.get("qps", 0) or 0
        avg_tps = benchmark_metrics.get("tps", 0) or 0

    cost_tracker.stop()
    cost_tracker.add_queries(
        query_count=total_queries,
        transaction_count=total_transactions,
        avg_qps=avg_qps, avg_tps=avg_tps,
        avg_row_size_bytes=200,
    )

    fetch_final_resource_snapshot(host, key_path, port, db_host=db_host)
    if load_stats:
        disk_final = get_disk_utilization(
            host, key_path, port, detected_ebs_gb, database,
            db_host=db_host)
        log("")
        log("--- Final Disk Utilization ---")
        log(f"  EBS: {disk_final['ebs_used_gb']}GB / "
            f"{disk_final['ebs_total_gb']}GB "
            f"({disk_final['ebs_used_pct']}%)")
        log(f"  TiKV stores: {disk_final['tikv_store_gb']:.1f}GB "
            f"({disk_final['tikv_store_pct']:.1f}%)")
        log(f"  Benchmark DB: {disk_final['db_data_gb']:.2f}GB")

    cost_tracker.print_cost_summary()

    interval_data = []
    sampler_ws = {}
    client_cpu_pct = None
    if sampler_started:
        try:
            stop_sampler(bench_host, bench_key, sampler_csv_path,
                         user='ec2-user')
            metrics_rows = parse_metrics_csv(sampler_csv_path)
            if metrics_rows:
                bench_start_epoch = metrics_rows[0].get('epoch', 0)
                bench_end_epoch = metrics_rows[-1].get('epoch', 0)
                cpu_samples, _ = derive_interval_metrics(
                    metrics_rows, bench_start_epoch, bench_end_epoch, 'generic')
                cpu_vals = [s['cpu_pct'] for s in cpu_samples
                            if s.get('cpu_pct') is not None]
                if cpu_vals:
                    client_cpu_pct = round(sum(cpu_vals) / len(cpu_vals), 1)
                interval_data = cpu_samples
                sampler_ws = {
                    'client_thermal_mc': safe_stats(
                        [s.get('client_thermal_mc') for s in cpu_samples]),
                    'client_loadavg_1m': safe_stats(
                        [s.get('client_loadavg_1m') for s in cpu_samples]),
                    'client_loadavg_5m': safe_stats(
                        [s.get('client_loadavg_5m') for s in cpu_samples]),
                    'client_loadavg_15m': safe_stats(
                        [s.get('client_loadavg_15m') for s in cpu_samples]),
                    'client_cpufreq_avg_mhz': safe_stats(
                        [s.get('client_cpufreq_avg_mhz') for s in cpu_samples]),
                }
        except Exception as e:
            log(f"Warning: could not collect sampler metrics: {e}")

    if sampler_ws or interval_data:
        fake_result = {
            'window_stats': sampler_ws,
            'interval_data': interval_data,
            'ec2_instance_type': cluster_info.get('tidb_instance', ''),
        }
        _print_client_extended_metrics([fake_result])
        _print_resource_history([fake_result])

    if cdc_tracker:
        cdc_tracker.stop()
        cdc_tracker.print_summary()

    if args.cleanup:
        run_sysbench_cleanup(bench_host, bench_key, tables, port, database,
                             db_host=db_host)

    log("Benchmark complete.")
