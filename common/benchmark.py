"""Unified sysbench benchmark orchestration for db-bench.

Provides shared sysbench functions usable by both tidb and aurora modules.
Handles command building, execution, output parsing, and data lifecycle
(prepare, cleanup, fast-fill).

Valkey uses memtier (not sysbench) and does not use this module.
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
