"""Remote benchmark runner for db-bench.

Generates self-contained shell scripts that run autonomously on a client VM
(inside tmux), eliminating the need for continuous SSH sessions.  The local
machine only needs connectivity for deploy / status / fetch operations.

Public API
----------
- generate_sysbench_script(params)  -- bash script for Aurora/TiDB
- generate_memtier_script(params)   -- bash script for Valkey
- deploy(host_ip, key_path, params, lua_files)
- status(host_ip, key_path)
- fetch(host_ip, key_path, local_dir)
"""

import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

from common.ssh import (
    ssh_run_simple,
    ssh_capture_simple,
    scp_put_simple,
    scp_get_simple,
)
from common.util import log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_RESULTS_BASE = "/home/ec2-user/bench-results"
_RUNNER_PATH = "/home/ec2-user/bench-runner.sh"
_TMUX_SESSION = "bench"
_MODULE_DIR = Path(__file__).resolve().parent
_LUA_DIR = _MODULE_DIR / "lua"


# ---------------------------------------------------------------------------
# Script generators
# ---------------------------------------------------------------------------


def generate_sysbench_script(params: dict) -> tuple[str, str]:
    """Return a self-contained bash script that runs a sysbench benchmark.

    The script is designed to run unattended inside tmux on an EC2 instance.
    It writes ``status.json`` atomically at each phase transition and captures
    all output to ``benchmark.log``.

    Parameters
    ----------
    params : dict
        Required keys: server_type, endpoint, port, user, password, db,
        workload, tables, table_size, threads, duration, report_interval.
        Optional keys: extra_args (list[str]), lua_dir (str),
        skip_prepare (bool), skip_cleanup (bool), multi_phase (list[dict]).
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results_dir = f"{_RESULTS_BASE}/{ts}"

    server_type = params["server_type"]
    endpoint = params["endpoint"]
    port = params["port"]
    user = params["user"]
    password = params.get("password", "")
    db = params["db"]
    workload = params["workload"]
    tables = params["tables"]
    table_size = params["table_size"]
    threads = params["threads"]
    duration = params["duration"]
    report_interval = params.get("report_interval", 10)
    extra_args = params.get("extra_args", [])
    lua_dir = params.get("lua_dir", "/tmp/lua")
    skip_prepare = params.get("skip_prepare", False)
    skip_cleanup = params.get("skip_cleanup", False)
    multi_phase = params.get("multi_phase")

    if workload in ("custom_iud", "custom_mixed", "custom_kv_sql"):
        workload_arg = f"{lua_dir}/{workload}.lua"
    else:
        workload_arg = workload

    pw_flag = f"--mysql-password='{password}'" if password else ""
    extra_str = " ".join(extra_args) if extra_args else ""

    sysbench_opts = (
        f"--mysql-host='{endpoint}' --mysql-port={port} "
        f"--mysql-user={user} {pw_flag} --mysql-db={db} "
        f"--tables={tables} --table-size={table_size} "
        f"--report-interval={report_interval} --percentile=99"
    )
    if extra_str:
        sysbench_opts += f" {extra_str}"

    if multi_phase:
        run_block = _build_multi_phase_run_block(
            multi_phase, workload_arg, sysbench_opts)
    else:
        run_block = dedent(f"""\
            update_status "run" "" ""
            echo ">>> Phase: run (threads={threads}, duration={duration}s)"
            sysbench {workload_arg} {sysbench_opts} --threads={threads} --time={duration} run 2>&1
            echo ">>> Phase run completed with exit code $?"
        """)

    prepare_block = ""
    if not skip_prepare:
        pw_mysql = f"-p'{password}'" if password else ""
        prepare_block = dedent(f"""\
            update_status "prepare" "" ""
            echo ">>> Creating database {db} (if not exists)"
            mysql -h '{endpoint}' -P {port} -u {user} {pw_mysql} \\
                -e 'CREATE DATABASE IF NOT EXISTS {db}' 2>&1 || true
            echo ">>> Phase: prepare"
            sysbench {workload_arg} {sysbench_opts} --threads={threads} prepare 2>&1
            echo ">>> Phase prepare completed with exit code $?"
        """)

    cleanup_block = ""
    if not skip_cleanup:
        cleanup_block = dedent(f"""\
            update_status "cleanup" "" ""
            echo ">>> Phase: cleanup"
            sysbench {workload_arg} {sysbench_opts} --threads=1 cleanup 2>&1
            echo ">>> Phase cleanup completed with exit code $?"
        """)

    script = f"""#!/usr/bin/env bash
# Auto-generated benchmark runner -- do not edit
# Server type : {server_type}
# Generated   : {ts}
set -euo pipefail

RESULTS_DIR="{results_dir}"
mkdir -p "$RESULTS_DIR"

# --- status.json helper -------------------------------------------
update_status() {{
    local phase="$1"
    local progress="$2"
    local error="$3"
    local now
    now=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    if [ -n "$error" ]; then
        cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "$phase", "started_at": "$BENCH_START", "progress": "$progress", "error": "$error", "completed_at": "$now"}}
STATUSEOF
    else
        cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "$phase", "started_at": "$BENCH_START", "progress": "$progress", "completed_at": null}}
STATUSEOF
    fi
    mv "$RESULTS_DIR/status.json.tmp" "$RESULTS_DIR/status.json"
}}

# --- error trap ----------------------------------------------------
on_error() {{
    local lineno="$1"
    echo "ERROR at line $lineno" >&2
    update_status "error" "" "Script failed at line $lineno"
    exit 1
}}
trap 'on_error $LINENO' ERR

BENCH_START=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# --- banner --------------------------------------------------------
echo "============================================================"
echo "Benchmark Runner"
echo "============================================================"
echo "Server type    : {server_type}"
echo "Endpoint       : {endpoint}:{port}"
echo "Database       : {db}"
echo "Workload       : {workload}"
echo "Tables         : {tables}"
echo "Table size     : {table_size}"
echo "Threads        : {threads}"
echo "Duration       : {duration}s"
echo "Report interval: {report_interval}s"
echo "Extra args     : {extra_str}"
echo "Results dir    : $RESULTS_DIR"
echo "Started at     : $BENCH_START"
echo "============================================================"
echo ""

# --- prepare -------------------------------------------------------
{prepare_block}
# --- run -----------------------------------------------------------
{run_block}
# --- cleanup -------------------------------------------------------
{cleanup_block}
# --- done ----------------------------------------------------------
BENCH_END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "done", "started_at": "$BENCH_START", "completed_at": "$BENCH_END"}}
STATUSEOF
mv "$RESULTS_DIR/status.json.tmp" "$RESULTS_DIR/status.json"

echo ""
echo "============================================================"
echo "Benchmark complete at $BENCH_END"
echo "Results in $RESULTS_DIR"
echo "============================================================"
"""
    return script, results_dir


def _build_multi_phase_run_block(phases, workload_arg, sysbench_opts):
    """Build bash for sequential multi-phase sysbench runs."""
    lines = []
    for i, phase in enumerate(phases):
        name = phase.get("name", f"phase_{i+1}")
        thr = phase["threads"]
        dur = phase["duration"]
        lines.append(f'update_status "run/{name}" "phase {i+1}/{len(phases)}" ""')
        lines.append(
            f'echo ">>> Phase: {name} (threads={thr}, duration={dur}s)"')
        lines.append(
            f"sysbench {workload_arg} {sysbench_opts} "
            f"--threads={thr} --time={dur} run 2>&1")
        lines.append(
            f'echo ">>> Phase {name} completed with exit code $?"')
        lines.append("")
    return "\n".join(lines)


def generate_memtier_script(params: dict) -> tuple[str, str]:
    """Return a self-contained bash script that runs a memtier benchmark.

    Parameters
    ----------
    params : dict
        Required keys: target_host, target_port.
        Optional keys: tests (int), operations (int), clients (int),
        threads (int), data_size (int), random_range (int).
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results_dir = f"{_RESULTS_BASE}/{ts}"

    target_host = params.get("target_host") or params["endpoint"]
    target_port = params.get("target_port") or params.get("port", 6379)
    tests = params.get("tests", 3)
    operations = params.get("operations", 100000)
    clients = params.get("clients", 50)
    threads = params.get("threads", 4)
    data_size = params.get("data_size", 256)
    random_range = params.get("random_range", 1000000)

    script = f"""#!/usr/bin/env bash
# Auto-generated memtier benchmark runner -- do not edit
# Generated: {ts}
set -euo pipefail

RESULTS_DIR="{results_dir}"
mkdir -p "$RESULTS_DIR"

update_status() {{
    local phase="$1"
    local progress="$2"
    local error="$3"
    local now
    now=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    if [ -n "$error" ]; then
        cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "$phase", "started_at": "$BENCH_START", "progress": "$progress", "error": "$error", "completed_at": "$now"}}
STATUSEOF
    else
        cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "$phase", "started_at": "$BENCH_START", "progress": "$progress", "completed_at": null}}
STATUSEOF
    fi
    mv "$RESULTS_DIR/status.json.tmp" "$RESULTS_DIR/status.json"
}}

on_error() {{
    local lineno="$1"
    echo "ERROR at line $lineno" >&2
    update_status "error" "" "Script failed at line $lineno"
    exit 1
}}
trap 'on_error $LINENO' ERR

BENCH_START=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "============================================================"
echo "Memtier Benchmark Runner"
echo "============================================================"
echo "Target         : {target_host}:{target_port}"
echo "Tests          : {tests}"
echo "Operations     : {operations}"
echo "Clients        : {clients}"
echo "Threads        : {threads}"
echo "Data size      : {data_size}"
echo "Random range   : {random_range}"
echo "Results dir    : $RESULTS_DIR"
echo "Started at     : $BENCH_START"
echo "============================================================"
echo ""

update_status "run" "" ""

for i in $(seq 1 {tests}); do
    echo ">>> Test run $i/{tests}"
    update_status "run" "test $i/{tests}" ""
    memtier_benchmark \\
        -s {target_host} \\
        -p {target_port} \\
        -n {operations} \\
        -c {clients} \\
        -t {threads} \\
        -d {data_size} \\
        --key-maximum={random_range} \\
        --ratio=1:10 \\
        --key-pattern=G:G \\
        --hide-histogram \\
        --json-out-file="$RESULTS_DIR/memtier_run_$i.json" \\
        2>&1
    echo ">>> Test run $i completed with exit code $?"
    echo ""
done

BENCH_END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cat > "$RESULTS_DIR/status.json.tmp" <<STATUSEOF
{{"phase": "done", "started_at": "$BENCH_START", "completed_at": "$BENCH_END"}}
STATUSEOF
mv "$RESULTS_DIR/status.json.tmp" "$RESULTS_DIR/status.json"

echo "============================================================"
echo "Benchmark complete at $BENCH_END"
echo "Results in $RESULTS_DIR"
echo "============================================================"
"""
    return script, results_dir


# ---------------------------------------------------------------------------
# deploy / status / fetch
# ---------------------------------------------------------------------------


def deploy(host_ip: str, key_path: str, params: dict,
           lua_files: bool = False) -> dict:
    """Deploy and start a benchmark script on a remote host.

    Parameters
    ----------
    host_ip : str
        Public IP of the bench-client EC2 instance.
    key_path : str
        Path to the SSH private key.
    params : dict
        Benchmark parameters (passed to the appropriate generator).
    lua_files : bool
        If True, upload ``common/lua/*.lua`` to ``/tmp/lua/`` on the host.

    Returns
    -------
    dict
        ``{"results_dir": str, "tmux_session": str}``
    """
    server_type = params.get("server_type", "")

    if lua_files:
        remote_lua_dir = "/tmp/lua"
        ssh_run_simple(host_ip, key_path,
                       f"mkdir -p {remote_lua_dir}", timeout=30)
        for lua_file in sorted(_LUA_DIR.glob("*.lua")):
            remote_path = f"{remote_lua_dir}/{lua_file.name}"
            scp_put_simple(host_ip, key_path, str(lua_file), remote_path)
            log(f"  Uploaded {lua_file.name} -> {remote_path}")

    if server_type == "valkey":
        script_content, results_dir = generate_memtier_script(params)
    else:
        script_content, results_dir = generate_sysbench_script(params)

    local_tmp = Path("/tmp/bench-runner.sh")
    local_tmp.write_text(script_content, encoding="utf-8")
    try:
        scp_put_simple(host_ip, key_path, str(local_tmp), _RUNNER_PATH,
                       timeout=30)
    finally:
        local_tmp.unlink(missing_ok=True)

    ssh_run_simple(host_ip, key_path,
                   f"tmux kill-session -t {_TMUX_SESSION} 2>/dev/null || true",
                   timeout=15)
    time.sleep(1)

    ssh_run_simple(
        host_ip, key_path,
        f"chmod +x {_RUNNER_PATH} && "
        f"mkdir -p {results_dir} && "
        f"tmux new-session -d -s {_TMUX_SESSION} "
        f"'bash {_RUNNER_PATH} > {results_dir}/benchmark.log 2>&1'",
        timeout=15,
    )

    log(f"Benchmark deployed on {host_ip}")
    log(f"  Results dir : {results_dir}")
    log(f"  tmux session: {_TMUX_SESSION}")
    log(f"  Monitor with: ssh -i {key_path} ec2-user@{host_ip} "
        f"'tail -f {results_dir}/benchmark.log'")

    return {"results_dir": results_dir, "tmux_session": _TMUX_SESSION}


def status(host_ip: str, key_path: str) -> dict:
    """Check the status of a running benchmark on a remote host.

    Returns
    -------
    dict
        ``{"running": bool, "phase": str, "started_at": str,
           "progress": str, "results_dir": str, "error": str|None}``
    """
    r = ssh_capture_simple(
        host_ip, key_path,
        f"ls -1dt {_RESULTS_BASE}/*/ 2>/dev/null | head -1",
        timeout=15,
    )
    results_dir = r.stdout.strip().rstrip("/")
    if not results_dir:
        return {
            "running": False,
            "phase": "none",
            "started_at": "",
            "progress": "",
            "results_dir": "",
            "error": "No benchmark results found",
        }

    r = ssh_capture_simple(
        host_ip, key_path,
        f"cat {results_dir}/status.json 2>/dev/null || echo '{{}}'",
        timeout=15,
    )
    try:
        status_data = json.loads(r.stdout.strip())
    except (json.JSONDecodeError, ValueError):
        status_data = {}

    r = ssh_capture_simple(
        host_ip, key_path,
        f"tmux has-session -t {_TMUX_SESSION} 2>/dev/null && echo yes || echo no",
        timeout=15,
    )
    tmux_running = r.stdout.strip() == "yes"

    return {
        "running": tmux_running,
        "phase": status_data.get("phase", "unknown"),
        "started_at": status_data.get("started_at", ""),
        "progress": status_data.get("progress", ""),
        "results_dir": results_dir,
        "completed_at": status_data.get("completed_at"),
        "error": status_data.get("error"),
    }


def fetch(host_ip: str, key_path: str, local_dir: str | None = None) -> str:
    """Fetch benchmark results from a remote host.

    Uses ``scp -r`` to download the latest results directory.

    Parameters
    ----------
    host_ip : str
        Public IP of the bench-client.
    key_path : str
        SSH private key path.
    local_dir : str, optional
        Local directory to store results.  Defaults to
        ``./results/{timestamp}/``.

    Returns
    -------
    str
        Local path where results were saved.
    """
    r = ssh_capture_simple(
        host_ip, key_path,
        f"ls -1dt {_RESULTS_BASE}/*/ 2>/dev/null | head -1",
        timeout=15,
    )
    results_dir = r.stdout.strip().rstrip("/")
    if not results_dir:
        raise RuntimeError("No benchmark results found on remote host")

    remote_basename = results_dir.rsplit("/", 1)[-1]

    if local_dir is None:
        local_dir = str(Path("results") / remote_basename)

    local_path = Path(local_dir)
    local_path.mkdir(parents=True, exist_ok=True)

    log(f"Fetching results from {host_ip}:{results_dir}/ ...")

    cmd = [
        "scp", "-r",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-i", str(key_path),
        f"ec2-user@{host_ip}:{results_dir}/*",
        str(local_path),
    ]
    subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                   check=True)

    log(f"Results saved to {local_path}")
    return str(local_path)
