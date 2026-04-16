"""Unified metrics sampler and post-processing for db-bench.

Merges aurora/ec2_sampler.py (runs on EC2) and aurora/parse_window.py
(local post-processing) into a single module parameterized by server type.

Server types: aurora, tidb, valkey

Sampler (EC2-side):
  Collects /proc/stat CPU jiffies, /proc/meminfo, plus DB-specific counters.
  Writes CSV until sentinel file is removed.

Post-processing (local-side):
  Parses sysbench output, metrics CSV, and CloudWatch data into JSON results.

Usage as library:
  from common.sampler import (
      generate_sampler_script, start_sampler, stop_sampler,
      parse_sysbench_summary, parse_interval_lines, build_interval_data,
      query_cloudwatch_metrics,
  )
"""

import csv
import json
import os
import re
import statistics
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from common.ssh import ssh_run_simple, scp_put_simple, scp_get_simple
from common.util import log, aws_session

SENTINEL_PATH = "/tmp/.metrics_running"
REMOTE_SAMPLER_PATH = "/tmp/sampler.py"
REMOTE_CSV_PATH = "/tmp/metrics.csv"


# ---------------------------------------------------------------------------
# Sampler script generation (deployed to EC2)
# ---------------------------------------------------------------------------

_SAMPLER_HEADER = '''\
#!/usr/bin/env python3
"""Auto-generated metrics sampler. Do not edit."""
import csv
import os
import subprocess
import sys
import time

SENTINEL = "{sentinel}"
OUTPUT = "{output_csv}"
INTERVAL = {interval}
SERVER_TYPE = "{server_type}"
'''

_SAMPLER_CPU_FN = '''
def read_cpu_jiffies():
    with open("/proc/stat") as f:
        parts = f.readline().split()
    return {
        "user": int(parts[1]),
        "nice": int(parts[2]),
        "system": int(parts[3]),
        "idle": int(parts[4]),
        "iowait": int(parts[5]),
        "irq": int(parts[6]),
        "softirq": int(parts[7]),
        "steal": int(parts[8]) if len(parts) > 8 else 0,
    }
'''

_SAMPLER_MEM_FN = '''
def read_memory():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split()
            key = parts[0].rstrip(":")
            if key in ("MemTotal", "MemAvailable", "Buffers", "Cached"):
                info[key] = int(parts[1])
    return {
        "mem_total_mb": info.get("MemTotal", 0) // 1024,
        "mem_avail_mb": info.get("MemAvailable", 0) // 1024,
        "mem_buffers_mb": info.get("Buffers", 0) // 1024,
        "mem_cached_mb": info.get("Cached", 0) // 1024,
    }
'''

_SAMPLER_NET_FN = '''
def read_net_bytes():
    """Read total rx/tx bytes across all non-lo interfaces from /proc/net/dev."""
    rx_total = 0
    tx_total = 0
    with open("/proc/net/dev") as f:
        for line in f:
            if ":" not in line:
                continue
            iface, data = line.split(":", 1)
            iface = iface.strip()
            if iface == "lo":
                continue
            cols = data.split()
            rx_total += int(cols[0])   # bytes received
            tx_total += int(cols[8])   # bytes transmitted
    return {"net_rx_bytes": rx_total, "net_tx_bytes": tx_total}
'''

_SAMPLER_THERMAL_FN = '''
def read_thermal():
    import glob
    temps = []
    for zone in sorted(glob.glob("/sys/class/thermal/thermal_zone*/temp")):
        try:
            with open(zone) as f:
                temps.append(int(f.read().strip()))
        except (IOError, ValueError):
            pass
    return {"thermal_zone0_mc": temps[0] if temps else 0}
'''

_SAMPLER_CPUFREQ_FN = '''
def read_cpufreq():
    import glob
    freqs = []
    for path in sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq")):
        try:
            with open(path) as f:
                freqs.append(int(f.read().strip()))
        except (IOError, ValueError):
            pass
    if not freqs:
        return {"cpufreq_min_mhz": 0, "cpufreq_max_mhz": 0, "cpufreq_avg_mhz": 0}
    return {
        "cpufreq_min_mhz": round(min(freqs) / 1000),
        "cpufreq_max_mhz": round(max(freqs) / 1000),
        "cpufreq_avg_mhz": round(sum(freqs) / len(freqs) / 1000),
    }
'''

_SAMPLER_DISKSTATS_FN = '''
def read_diskstats():
    """Read aggregate disk I/O counters from /proc/diskstats.

    Sums across all real block devices (skips ram*, loop*, dm-*).
    Returns cumulative counters -- caller computes deltas.
    """
    reads = writes = sectors_read = sectors_written = io_ticks = 0
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            name = parts[2]
            # Skip virtual / loopback / device-mapper partitions
            if name.startswith(("ram", "loop", "dm-")):
                continue
            # Skip partitions (e.g. sda1) -- keep whole-disk only
            if name[-1].isdigit() and not name.startswith("nvme"):
                continue
            # For nvme, skip partition entries like nvme0n1p1
            if "p" in name and name.startswith("nvme"):
                idx = name.rfind("p")
                if idx > 0 and name[idx + 1:].isdigit():
                    continue
            reads += int(parts[3])           # reads completed
            sectors_read += int(parts[5])    # sectors read
            writes += int(parts[7])          # writes completed
            sectors_written += int(parts[9]) # sectors written
            io_ticks += int(parts[12])       # ms spent doing I/O
    return {
        "disk_reads_completed": reads,
        "disk_writes_completed": writes,
        "disk_sectors_read": sectors_read,
        "disk_sectors_written": sectors_written,
        "disk_io_ticks_ms": io_ticks,
    }
'''

_SAMPLER_CTXSWITCH_FN = '''
def read_context_switches():
    """Read context switches and process forks from /proc/stat."""
    ctxt = 0
    procs = 0
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("ctxt "):
                ctxt = int(line.split()[1])
            elif line.startswith("processes "):
                procs = int(line.split()[1])
    return {"ctx_switches": ctxt, "processes_created": procs}
'''

_SAMPLER_LOADAVG_FN = '''
def read_loadavg():
    with open("/proc/loadavg") as f:
        parts = f.read().split()
    return {
        "loadavg_1m": float(parts[0]),
        "loadavg_5m": float(parts[1]),
        "loadavg_15m": float(parts[2]),
    }
'''

_SAMPLER_PER_CORE_CPU_FN = '''
import json as _json

_prev_per_core = {}

def read_per_core_cpu_pct():
    global _prev_per_core
    cores = {}
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu") and not line.startswith("cpu "):
                parts = line.split()
                name = parts[0]
                vals = [int(x) for x in parts[1:8]]
                cores[name] = vals
    result = {}
    for name, vals in sorted(cores.items()):
        total = sum(vals)
        idle = vals[3]
        if name in _prev_per_core:
            prev_total, prev_idle = _prev_per_core[name]
            dt = total - prev_total
            di = idle - prev_idle
            if dt > 0:
                result[name] = round((1.0 - di / dt) * 100, 1)
            else:
                result[name] = 0.0
        _prev_per_core[name] = (total, idle)
    return _json.dumps(result) if result else ""
'''

_SAMPLER_MYSQL_FN = '''
MYSQL_HOST = "{mysql_host}"
MYSQL_USER = "{mysql_user}"
MYSQL_PASSWORD = "{mysql_password}"
MYSQL_PORT = "{mysql_port}"

def read_innodb_counters():
    try:
        r = subprocess.run(
            [
                "mysql",
                "-h" + MYSQL_HOST,
                "-P" + MYSQL_PORT,
                "-u" + MYSQL_USER,
                "-p" + MYSQL_PASSWORD,
                "--batch", "--skip-column-names",
                "-e",
                "SHOW GLOBAL STATUS WHERE Variable_name IN "
                "('Innodb_rows_inserted','Innodb_rows_updated',"
                "'Innodb_rows_deleted','Innodb_rows_read',"
                "'Com_commit','Com_rollback')",
            ],
            capture_output=True, text=True, timeout=5,
        )
        counters = {{}}
        for line in r.stdout.strip().splitlines():
            parts = line.split("\\t")
            if len(parts) >= 2:
                counters[parts[0]] = int(parts[1])
        return counters
    except Exception as e:
        print(f"WARNING: InnoDB query failed: {{{{e}}}}", file=sys.stderr)
        return {{}}
'''

_SAMPLER_VALKEY_FN = '''
VALKEY_HOST = "{valkey_host}"
VALKEY_PORT = "{valkey_port}"

def read_valkey_stats():
    try:
        r = subprocess.run(
            ["valkey-cli", "-h", VALKEY_HOST, "-p", VALKEY_PORT, "INFO", "stats"],
            capture_output=True, text=True, timeout=5,
        )
        stats = {{}}
        for line in r.stdout.strip().splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                k = k.strip()
                v = v.strip()
                if k in (
                    "total_commands_processed", "instantaneous_ops_per_sec",
                    "total_net_input_bytes", "total_net_output_bytes",
                    "keyspace_hits", "keyspace_misses",
                    "total_connections_received", "evicted_keys",
                    "expired_keys",
                ):
                    try:
                        stats[k] = int(v)
                    except ValueError:
                        try:
                            stats[k] = float(v)
                        except ValueError:
                            pass
        return stats
    except Exception as e:
        print(f"WARNING: Valkey query failed: {{{{e}}}}", file=sys.stderr)
        return {{}}
'''

_SAMPLER_MAIN_COMMON_FIELDS = [
    "epoch",
    "cpu_user", "cpu_nice", "cpu_system", "cpu_idle",
    "cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_steal",
    "mem_total_mb", "mem_avail_mb", "mem_buffers_mb", "mem_cached_mb",
    "net_rx_bytes", "net_tx_bytes",
    "thermal_zone0_mc",
    "cpufreq_min_mhz",
    "cpufreq_max_mhz",
    "cpufreq_avg_mhz",
    "loadavg_1m",
    "loadavg_5m",
    "loadavg_15m",
    "per_core_cpu_pct_json",
    "disk_reads_completed",
    "disk_writes_completed",
    "disk_sectors_read",
    "disk_sectors_written",
    "disk_io_ticks_ms",
    "ctx_switches",
    "processes_created",
]

_SAMPLER_MYSQL_FIELDS = [
    "innodb_inserted", "innodb_updated", "innodb_deleted", "innodb_read",
    "com_commit", "com_rollback",
]

_SAMPLER_VALKEY_FIELDS = [
    "total_commands_processed", "instantaneous_ops_per_sec",
    "total_net_input_bytes", "total_net_output_bytes",
    "keyspace_hits", "keyspace_misses",
    "total_connections_received", "evicted_keys", "expired_keys",
]


def _sampler_fieldnames(server_type):
    fields = list(_SAMPLER_MAIN_COMMON_FIELDS)
    if server_type in ("aurora", "tidb"):
        fields.extend(_SAMPLER_MYSQL_FIELDS)
    elif server_type == "valkey":
        fields.extend(_SAMPLER_VALKEY_FIELDS)
    return fields


def _sampler_main_loop(server_type):
    fields = _sampler_fieldnames(server_type)
    fields_str = json.dumps(fields)

    if server_type in ("aurora", "tidb"):
        collect_extra = '''
            innodb = read_innodb_counters()
            row["innodb_inserted"] = innodb.get("Innodb_rows_inserted", "")
            row["innodb_updated"] = innodb.get("Innodb_rows_updated", "")
            row["innodb_deleted"] = innodb.get("Innodb_rows_deleted", "")
            row["innodb_read"] = innodb.get("Innodb_rows_read", "")
            row["com_commit"] = innodb.get("Com_commit", "")
            row["com_rollback"] = innodb.get("Com_rollback", "")'''
    elif server_type == "valkey":
        collect_extra = '''
            vs = read_valkey_stats()
            row["total_commands_processed"] = vs.get("total_commands_processed", "")
            row["instantaneous_ops_per_sec"] = vs.get("instantaneous_ops_per_sec", "")
            row["total_net_input_bytes"] = vs.get("total_net_input_bytes", "")
            row["total_net_output_bytes"] = vs.get("total_net_output_bytes", "")
            row["keyspace_hits"] = vs.get("keyspace_hits", "")
            row["keyspace_misses"] = vs.get("keyspace_misses", "")
            row["total_connections_received"] = vs.get("total_connections_received", "")
            row["evicted_keys"] = vs.get("evicted_keys", "")
            row["expired_keys"] = vs.get("expired_keys", "")'''
    else:
        collect_extra = ""

    return f'''
def main():
    fieldnames = {fields_str}
    with open(OUTPUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        next_sample = time.time()
        sample_count = 0
        while os.path.exists(SENTINEL):
            epoch = int(time.time())
            cpu = read_cpu_jiffies()
            mem = read_memory()
            net = read_net_bytes()
            thermal = read_thermal()
            cpufreq = read_cpufreq()
            loadavg = read_loadavg()
            per_core_json = read_per_core_cpu_pct()
            disk = read_diskstats()
            ctxsw = read_context_switches()
            row = {{
                "epoch": epoch,
                "cpu_user": cpu["user"],
                "cpu_nice": cpu["nice"],
                "cpu_system": cpu["system"],
                "cpu_idle": cpu["idle"],
                "cpu_iowait": cpu["iowait"],
                "cpu_irq": cpu["irq"],
                "cpu_softirq": cpu["softirq"],
                "cpu_steal": cpu["steal"],
                "mem_total_mb": mem["mem_total_mb"],
                "mem_avail_mb": mem["mem_avail_mb"],
                "mem_buffers_mb": mem["mem_buffers_mb"],
                "mem_cached_mb": mem["mem_cached_mb"],
                "net_rx_bytes": net["net_rx_bytes"],
                "net_tx_bytes": net["net_tx_bytes"],
                "thermal_zone0_mc": thermal["thermal_zone0_mc"],
                "cpufreq_min_mhz": cpufreq["cpufreq_min_mhz"],
                "cpufreq_max_mhz": cpufreq["cpufreq_max_mhz"],
                "cpufreq_avg_mhz": cpufreq["cpufreq_avg_mhz"],
                "loadavg_1m": loadavg["loadavg_1m"],
                "loadavg_5m": loadavg["loadavg_5m"],
                "loadavg_15m": loadavg["loadavg_15m"],
                "per_core_cpu_pct_json": per_core_json,
                "disk_reads_completed": disk["disk_reads_completed"],
                "disk_writes_completed": disk["disk_writes_completed"],
                "disk_sectors_read": disk["disk_sectors_read"],
                "disk_sectors_written": disk["disk_sectors_written"],
                "disk_io_ticks_ms": disk["disk_io_ticks_ms"],
                "ctx_switches": ctxsw["ctx_switches"],
                "processes_created": ctxsw["processes_created"],
            }}{collect_extra}
            w.writerow(row)
            f.flush()
            sample_count += 1
            next_sample += INTERVAL
            sleep_time = next_sample - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
    print(f"Sampler stopped after {{sample_count}} samples. Output: {{OUTPUT}}")

if __name__ == "__main__":
    main()
'''


def generate_sampler_script(server_type, interval=5,
                            mysql_host="", mysql_user="admin",
                            mysql_password="", mysql_port="3306",
                            valkey_host="127.0.0.1", valkey_port="6379"):
    parts = [
        _SAMPLER_HEADER.format(
            sentinel=SENTINEL_PATH,
            output_csv=REMOTE_CSV_PATH,
            interval=interval,
            server_type=server_type,
        ),
        _SAMPLER_CPU_FN,
        _SAMPLER_MEM_FN,
        _SAMPLER_NET_FN,
    ]
    parts.append(_SAMPLER_THERMAL_FN)
    parts.append(_SAMPLER_CPUFREQ_FN)
    parts.append(_SAMPLER_LOADAVG_FN)
    parts.append(_SAMPLER_PER_CORE_CPU_FN)
    parts.append(_SAMPLER_DISKSTATS_FN)
    parts.append(_SAMPLER_CTXSWITCH_FN)

    if server_type in ("aurora", "tidb"):
        parts.append(_SAMPLER_MYSQL_FN.format(
            mysql_host=mysql_host,
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            mysql_port=mysql_port,
        ))
    elif server_type == "valkey":
        parts.append(_SAMPLER_VALKEY_FN.format(
            valkey_host=valkey_host,
            valkey_port=valkey_port,
        ))

    parts.append(_sampler_main_loop(server_type))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Start / stop API (runs locally, manages remote EC2 sampler)
# ---------------------------------------------------------------------------

def start_sampler(host_ip, key_path, server_type, interval=5, user="ec2-user",
                  mysql_host="", mysql_user="admin", mysql_password="",
                  mysql_port="3306", valkey_host="127.0.0.1", valkey_port="6379"):
    script = generate_sampler_script(
        server_type,
        interval=interval,
        mysql_host=mysql_host,
        mysql_user=mysql_user,
        mysql_password=mysql_password,
        mysql_port=mysql_port,
        valkey_host=valkey_host,
        valkey_port=valkey_port,
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
        tmp.write(script)
        local_script = tmp.name

    try:
        scp_put_simple(host_ip, key_path, local_script, REMOTE_SAMPLER_PATH,
                       user=user)
    finally:
        Path(local_script).unlink(missing_ok=True)

    ssh_run_simple(host_ip, key_path, f"touch {SENTINEL_PATH}", user=user)
    ssh_run_simple(
        host_ip, key_path,
        f"nohup python3 {REMOTE_SAMPLER_PATH} > /tmp/sampler.log 2>&1 &",
        user=user,
    )
    log(f"Sampler started on {host_ip} (type={server_type}, interval={interval}s)")


def stop_sampler(host_ip, key_path, local_csv_path, user="ec2-user",
                 timeout=15):
    ssh_run_simple(host_ip, key_path, f"rm -f {SENTINEL_PATH}", user=user)

    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = ssh_run_simple(
            host_ip, key_path,
            f"pgrep -f '{REMOTE_SAMPLER_PATH}' || echo stopped",
            user=user,
        )
        if "stopped" in r.stdout:
            break
        time.sleep(1)

    scp_get_simple(host_ip, key_path, REMOTE_CSV_PATH, local_csv_path,
                   user=user)
    log(f"Sampler stopped on {host_ip}, CSV saved to {local_csv_path}")
    return local_csv_path


# ---------------------------------------------------------------------------
# Multi-host sampler management
# ---------------------------------------------------------------------------

@dataclass
class SamplerSession:
    """Tracks a running sampler on one EC2 host."""
    label: str
    host_ip: str
    key_path: str
    csv_path: str
    user: str = "ec2-user"
    instance_type: str = ""
    started: bool = False


def start_samplers(hosts, server_type='generic', interval=1, user='ec2-user',
                   **sampler_kwargs):
    """Start sampler on multiple hosts. Returns list of SamplerSession.

    ``hosts`` is a list of dicts with keys: label, host_ip, key_path,
    and optionally instance_type, user.
    """
    sessions = []
    for h in hosts:
        label = h["label"]
        host_ip = h["host_ip"]
        kp = h["key_path"]
        u = h.get("user", user)
        it = h.get("instance_type", "")
        csv_path = f"/tmp/sampler_{label.lower()}_{int(time.time())}.csv"
        sess = SamplerSession(label=label, host_ip=host_ip, key_path=kp,
                              csv_path=csv_path, user=u, instance_type=it)
        try:
            start_sampler(host_ip, kp, server_type, interval=interval, user=u,
                          **sampler_kwargs)
            sess.started = True
            log(f"  Sampler started on {label} ({host_ip})")
        except Exception as e:
            log(f"Warning: could not start sampler on {label} ({host_ip}): {e}")
        sessions.append(sess)
    return sessions


def stop_samplers(sessions):
    """Stop sampler on all hosts and collect CSV files.

    Returns list of dicts with keys: label, csv_path, instance_type, rows.
    Only includes sessions that were started and collected successfully.
    """
    collected = []
    for sess in sessions:
        if not sess.started:
            continue
        try:
            stop_sampler(sess.host_ip, sess.key_path, sess.csv_path,
                         user=sess.user)
            rows = parse_metrics_csv(sess.csv_path)
            collected.append({
                "label": sess.label,
                "csv_path": sess.csv_path,
                "instance_type": sess.instance_type,
                "rows": rows,
            })
        except Exception as e:
            log(f"Warning: could not collect sampler from {sess.label}: {e}")
    return collected


# ---------------------------------------------------------------------------
# Flamegraph capture
# ---------------------------------------------------------------------------

def capture_flamegraph(host_ip, key_path, duration=30, user="ec2-user",
                       output_dir="/tmp"):
    """Run perf record on remote host for ``duration`` seconds and generate
    a flamegraph SVG.  Returns local path to the SVG, or None on failure.

    Requires ``perf`` and FlameGraph tools on the remote host (best-effort).
    """
    tag = f"fg_{int(time.time())}"
    remote_perf = f"/tmp/{tag}.perf.data"
    remote_folded = f"/tmp/{tag}.folded"
    remote_svg = f"/tmp/{tag}.svg"

    cmds = [
        f"sudo perf record -F 99 -a -g -o {remote_perf} -- sleep {duration}",
        f"sudo perf script -i {remote_perf} | stackcollapse-perf.pl > {remote_folded}",
        f"flamegraph.pl {remote_folded} > {remote_svg}",
    ]
    try:
        for cmd in cmds:
            ssh_run_simple(host_ip, key_path, cmd, user=user)
        local_svg = os.path.join(output_dir, f"{tag}.svg")
        scp_get_simple(host_ip, key_path, remote_svg, local_svg, user=user)
        log(f"Flamegraph captured: {local_svg}")
        return local_svg
    except Exception as e:
        log(f"Warning: flamegraph capture failed on {host_ip}: {e}")
        return None


def diff_flamegraphs(baseline_folded, current_folded, output_path):
    """Generate a differential flamegraph from two folded stack files.

    Uses difffolded.pl locally.  Returns output_path on success, None on
    failure.
    """
    try:
        cmd = f"difffolded.pl {baseline_folded} {current_folded} | flamegraph.pl > {output_path}"
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
        log(f"Differential flamegraph: {output_path}")
        return output_path
    except Exception as e:
        log(f"Warning: diff flamegraph failed: {e}")
        return None


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def parse_metrics_csv(csv_path):
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                if v is None or v == "":
                    parsed[k] = None
                    continue
                try:
                    parsed[k] = int(v)
                except ValueError:
                    try:
                        parsed[k] = float(v)
                    except ValueError:
                        parsed[k] = v
            rows.append(parsed)
    return rows


# ---------------------------------------------------------------------------
# Sysbench output parsing
# ---------------------------------------------------------------------------

def parse_interval_lines(output):
    """Parse sysbench interval report lines.

    The regex captures whichever percentile sysbench was configured to
    report (``--percentile=99`` produces ``lat (ms,99%)``).
    """
    intervals = []
    for line in output.splitlines():
        m = re.match(
            r"\[\s*([\d.]+)s\s*\]\s+thds:\s+(\d+)\s+"
            r"tps:\s+([\d.]+)\s+qps:\s+([\d.]+)\s+"
            r"\(r/w/o:\s+([\d.]+)/([\d.]+)/([\d.]+)\)\s+"
            r"lat\s+\(ms,\s*(\d+)%\):\s+([\d.]+)\s+"
            r"err/s:\s+([\d.]+)",
            line,
        )
        if m:
            pct = int(m.group(8))
            val = float(m.group(9))
            intervals.append({
                "time_s": float(m.group(1)),
                "threads": int(m.group(2)),
                "tps": float(m.group(3)),
                "qps": float(m.group(4)),
                "read_qps": float(m.group(5)),
                "write_qps": float(m.group(6)),
                "other_qps": float(m.group(7)),
                "latency_pct": pct,
                "latency_pct_ms": val,
                "err_s": float(m.group(10)),
            })
    return intervals


def parse_sysbench_summary(output):
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
    for label in ["min", "avg", "max"]:
        m = re.search(rf"{re.escape(label)}:\s+([\d.]+)", output)
        if m:
            result[f"latency_{label}_ms"] = float(m.group(1))
    # Capture whichever percentile sysbench was configured to report
    m = re.search(r"(\d+)th percentile:\s+([\d.]+)", output)
    if m:
        pct_num = int(m.group(1))
        result[f"latency_{pct_num}th_pct_ms"] = float(m.group(2))
    return result


# ---------------------------------------------------------------------------
# CPU / counter delta computations
# ---------------------------------------------------------------------------

_CPU_FIELDS = [
    "cpu_user", "cpu_nice", "cpu_system", "cpu_idle",
    "cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_steal",
]


def compute_cpu_pct(prev, curr):
    try:
        total_prev = sum(prev[f] for f in _CPU_FIELDS if prev.get(f) is not None)
        total_curr = sum(curr[f] for f in _CPU_FIELDS if curr.get(f) is not None)
        idle_prev = (prev.get("cpu_idle") or 0) + (prev.get("cpu_iowait") or 0)
        idle_curr = (curr.get("cpu_idle") or 0) + (curr.get("cpu_iowait") or 0)
        total_d = total_curr - total_prev
        idle_d = idle_curr - idle_prev
        if total_d > 0:
            return round((total_d - idle_d) / total_d * 100, 1)
    except (KeyError, TypeError):
        pass
    return None


_MYSQL_COUNTER_MAP = [
    ("innodb_inserted", "insert_per_sec"),
    ("innodb_updated", "update_per_sec"),
    ("innodb_deleted", "delete_per_sec"),
    ("innodb_read", "read_per_sec"),
    ("com_commit", "commits_per_sec"),
]

_VALKEY_COUNTER_MAP = [
    ("total_commands_processed", "commands_per_sec"),
    ("keyspace_hits", "hits_per_sec"),
    ("keyspace_misses", "misses_per_sec"),
    ("total_net_input_bytes", "net_input_bytes_per_sec"),
    ("total_net_output_bytes", "net_output_bytes_per_sec"),
    ("evicted_keys", "evictions_per_sec"),
    ("expired_keys", "expirations_per_sec"),
]


def compute_iud_rates(prev, curr):
    dt = (curr.get("epoch") or 0) - (prev.get("epoch") or 0)
    if dt <= 0:
        dt = 5
    rates = {}
    for counter, rate_key in _MYSQL_COUNTER_MAP:
        p = prev.get(counter)
        c = curr.get(counter)
        if p is not None and c is not None:
            rates[rate_key] = round((c - p) / dt, 1)
    iud = (
        rates.get("insert_per_sec", 0)
        + rates.get("update_per_sec", 0)
        + rates.get("delete_per_sec", 0)
    )
    rates["total_iud_per_sec"] = round(iud, 1)
    return rates


def compute_valkey_rates(prev, curr):
    dt = (curr.get("epoch") or 0) - (prev.get("epoch") or 0)
    if dt <= 0:
        dt = 5
    rates = {}
    for counter, rate_key in _VALKEY_COUNTER_MAP:
        p = prev.get(counter)
        c = curr.get(counter)
        if p is not None and c is not None:
            rates[rate_key] = round((c - p) / dt, 1)
    rates["instantaneous_ops_per_sec"] = curr.get("instantaneous_ops_per_sec")
    hit = rates.get("hits_per_sec", 0)
    miss = rates.get("misses_per_sec", 0)
    total = hit + miss
    rates["hit_rate_pct"] = round(hit / total * 100, 1) if total > 0 else None
    return rates


_DISK_COUNTER_MAP = [
    ("disk_reads_completed", "disk_read_iops"),
    ("disk_writes_completed", "disk_write_iops"),
    ("disk_sectors_read", "disk_read_sectors_per_sec"),
    ("disk_sectors_written", "disk_write_sectors_per_sec"),
]


def compute_disk_rates(prev, curr):
    dt = (curr.get("epoch") or 0) - (prev.get("epoch") or 0)
    if dt <= 0:
        dt = 5
    rates = {}
    for counter, rate_key in _DISK_COUNTER_MAP:
        p = prev.get(counter)
        c = curr.get(counter)
        if p is not None and c is not None:
            rates[rate_key] = round((c - p) / dt, 1)
    # Throughput in MB/s (512 bytes per sector)
    rs = rates.get("disk_read_sectors_per_sec", 0)
    ws = rates.get("disk_write_sectors_per_sec", 0)
    rates["disk_read_mbps"] = round(rs * 512 / 1_000_000, 2)
    rates["disk_write_mbps"] = round(ws * 512 / 1_000_000, 2)
    # Utilisation % from io_ticks (ms of wall-clock I/O)
    ticks_prev = prev.get("disk_io_ticks_ms")
    ticks_curr = curr.get("disk_io_ticks_ms")
    if ticks_prev is not None and ticks_curr is not None:
        rates["disk_util_pct"] = round(
            (ticks_curr - ticks_prev) / (dt * 1000) * 100, 1
        )
    return rates


def compute_ctx_rates(prev, curr):
    dt = (curr.get("epoch") or 0) - (prev.get("epoch") or 0)
    if dt <= 0:
        dt = 5
    rates = {}
    for key, rate_key in [("ctx_switches", "ctx_per_sec"),
                          ("processes_created", "forks_per_sec")]:
        p = prev.get(key)
        c = curr.get(key)
        if p is not None and c is not None:
            rates[rate_key] = round((c - p) / dt, 1)
    return rates


# ---------------------------------------------------------------------------
# Statistics helper
# ---------------------------------------------------------------------------

def safe_stats(values):
    clean = [v for v in values if v is not None and isinstance(v, (int, float))]
    if not clean:
        return {}
    result = {
        "min": round(min(clean), 2),
        "avg": round(statistics.mean(clean), 2),
        "max": round(max(clean), 2),
        "count": len(clean),
    }
    if len(clean) >= 2:
        result["median"] = round(statistics.median(clean), 2)
        result["stdev"] = round(statistics.stdev(clean), 2)
    if len(clean) >= 5:
        s = sorted(clean)
        p50_idx = min(int(len(s) * 0.50), len(s) - 1)
        p95_idx = min(int(len(s) * 0.95), len(s) - 1)
        p99_idx = min(int(len(s) * 0.99), len(s) - 1)
        result["p50"] = round(s[p50_idx], 2)
        result["p95"] = round(s[p95_idx], 2)
        result["p99"] = round(s[p99_idx], 2)
    return result


# ---------------------------------------------------------------------------
# Unicode sparkline history charts
# ---------------------------------------------------------------------------

SPARK_CHARS = '\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588'


def render_history_chart(values, width=40, label=""):
    """Render a sparkline for percentage values (0-100 scale).

    Returns a string like ``"CPU%: ▁▂▃▅▇█▇▅▃▂ (avg 45%, peak 89%)"``
    or ``"CPU%: (no data)"`` when *values* is empty / all-None.
    """
    clean = [v for v in (values or []) if v is not None and isinstance(v, (int, float))]
    if not clean:
        return f"{label}: (no data)" if label else "(no data)"

    # Subsample to *width* evenly spaced points when needed
    if len(clean) > width:
        step = len(clean) / width
        clean = [clean[int(i * step)] for i in range(width)]

    avg = sum(clean) / len(clean)
    peak = max(clean)

    chars = []
    for v in clean:
        clamped = max(0.0, min(100.0, v))
        idx = int(clamped / 100.0 * 7)
        idx = max(0, min(7, idx))
        chars.append(SPARK_CHARS[idx])

    sparkline = "".join(chars)
    if label:
        return f"{label}: {sparkline} (avg {avg:.0f}%, peak {peak:.0f}%)"
    return f"{sparkline} (avg {avg:.0f}%, peak {peak:.0f}%)"


def render_history_chart_raw(values, width=40, label=""):
    """Render a sparkline for arbitrary numeric values (scaled to min/max).

    Unlike :func:`render_history_chart` this does **not** assume a 0-100
    percentage scale -- it normalises to the actual data range.

    Returns e.g. ``"Load 1m: ▁▂▃▅▇█▇▅▃▂ (avg 3.2, peak 7.1)"``.
    """
    clean = [v for v in (values or []) if v is not None and isinstance(v, (int, float))]
    if not clean:
        return f"{label}: (no data)" if label else "(no data)"

    # Subsample to *width* evenly spaced points when needed
    if len(clean) > width:
        step = len(clean) / width
        clean = [clean[int(i * step)] for i in range(width)]

    avg = sum(clean) / len(clean)
    peak = max(clean)
    lo = min(clean)
    span = peak - lo

    chars = []
    for v in clean:
        if span == 0:
            idx = 4  # mid-level when all values are identical
        else:
            idx = int((v - lo) / span * 7)
            idx = max(0, min(7, idx))
        chars.append(SPARK_CHARS[idx])

    sparkline = "".join(chars)
    if label:
        return f"{label}: {sparkline} (avg {avg:.1f}, peak {peak:.1f})"
    return f"{sparkline} (avg {avg:.1f}, peak {peak:.1f})"


# ---------------------------------------------------------------------------
# Windowed interval derivation
# ---------------------------------------------------------------------------

def _find_closest(samples, target_epoch, max_delta=6):
    if not samples:
        return None
    best = min(samples, key=lambda r: abs(r["epoch"] - target_epoch))
    if abs(best["epoch"] - target_epoch) <= max_delta:
        return best
    return None


def derive_interval_metrics(metrics_rows, window_epoch_start, window_epoch_end,
                            server_type="aurora"):
    cpu_samples = []
    db_samples = []
    for i in range(1, len(metrics_rows)):
        prev = metrics_rows[i - 1]
        curr = metrics_rows[i]
        epoch = curr.get("epoch")
        if epoch is None:
            continue
        if epoch < window_epoch_start or epoch > window_epoch_end + 3:
            continue

        cpu_pct = compute_cpu_pct(prev, curr)
        mem_used = None
        total = curr.get("mem_total_mb")
        avail = curr.get("mem_avail_mb")
        if total is not None and avail is not None:
            mem_used = total - avail

        # Network throughput (bytes/sec) from cumulative counters
        dt = (epoch or 0) - (prev.get("epoch") or 0)
        if dt <= 0:
            dt = 5
        net_rx_bps = None
        net_tx_bps = None
        rx_curr = curr.get("net_rx_bytes")
        rx_prev = prev.get("net_rx_bytes")
        tx_curr = curr.get("net_tx_bytes")
        tx_prev = prev.get("net_tx_bytes")
        if rx_curr is not None and rx_prev is not None:
            net_rx_bps = (rx_curr - rx_prev) / dt
        if tx_curr is not None and tx_prev is not None:
            net_tx_bps = (tx_curr - tx_prev) / dt

        sample = {
            "epoch": epoch,
            "client_cpu_pct": cpu_pct,
            "client_mem_used_mb": mem_used,
            "client_mem_total_mb": total,
            "client_net_rx_mbps": round(net_rx_bps / 1_000_000, 2) if net_rx_bps is not None else None,
            "client_net_tx_mbps": round(net_tx_bps / 1_000_000, 2) if net_tx_bps is not None else None,
            "client_thermal_mc": curr.get("thermal_zone0_mc"),
            "client_cpufreq_min_mhz": curr.get("cpufreq_min_mhz"),
            "client_cpufreq_max_mhz": curr.get("cpufreq_max_mhz"),
            "client_cpufreq_avg_mhz": curr.get("cpufreq_avg_mhz"),
            "client_loadavg_1m": curr.get("loadavg_1m"),
            "client_loadavg_5m": curr.get("loadavg_5m"),
            "client_loadavg_15m": curr.get("loadavg_15m"),
            "client_per_core_cpu_pct": curr.get("per_core_cpu_pct_json"),
        }

        for dk, dv in compute_disk_rates(prev, curr).items():
            sample[f"client_{dk}"] = dv
        for ck, cv in compute_ctx_rates(prev, curr).items():
            sample[f"client_{ck}"] = cv

        cpu_samples.append(sample)

        if server_type in ("aurora", "tidb"):
            rates = compute_iud_rates(prev, curr)
        elif server_type == "valkey":
            rates = compute_valkey_rates(prev, curr)
        else:
            rates = {}
        rates["epoch"] = epoch
        db_samples.append(rates)

    return cpu_samples, db_samples


def build_interval_data(window_intervals, cpu_samples, db_samples,
                        run_start_epoch, server_type="aurora"):
    rows = []
    for iv in window_intervals:
        iv_epoch = run_start_epoch + iv["time_s"]
        pct = iv.get("latency_pct", 95)
        entry = {
            "time_s": iv["time_s"],
            "tps": iv["tps"],
            "qps": iv["qps"],
            "client_latency_pct": pct,
            "client_latency_pct_ms": iv["latency_pct_ms"],
            "err_s": iv["err_s"],
        }

        closest_db = _find_closest(db_samples, iv_epoch)
        if closest_db:
            for k, v in closest_db.items():
                if k != "epoch":
                    entry[k] = v

        closest_cpu = _find_closest(cpu_samples, iv_epoch)
        if closest_cpu:
            entry["client_cpu_pct"] = closest_cpu.get("client_cpu_pct")
            entry["client_mem_used_mb"] = closest_cpu.get("client_mem_used_mb")
            entry["client_net_rx_mbps"] = closest_cpu.get("client_net_rx_mbps")
            entry["client_net_tx_mbps"] = closest_cpu.get("client_net_tx_mbps")
            entry["client_thermal_mc"] = closest_cpu.get("client_thermal_mc")
            entry["client_cpufreq_avg_mhz"] = closest_cpu.get("client_cpufreq_avg_mhz")
            entry["client_loadavg_1m"] = closest_cpu.get("client_loadavg_1m")
            entry["client_per_core_cpu_pct"] = closest_cpu.get("client_per_core_cpu_pct")
            for key in ("client_disk_read_iops", "client_disk_write_iops",
                        "client_disk_read_mbps", "client_disk_write_mbps",
                        "client_disk_util_pct",
                        "client_ctx_per_sec", "client_forks_per_sec"):
                if key in closest_cpu:
                    entry[key] = closest_cpu[key]

        rows.append(entry)
    return rows


# ---------------------------------------------------------------------------
# CloudWatch queries
# ---------------------------------------------------------------------------

_CW_AURORA_QUERIES = [
    ("aurora_cpu_avg_pct", "AWS/RDS", "CPUUtilization", "Average"),
    ("aurora_cpu_max_pct", "AWS/RDS", "CPUUtilization", "Maximum"),
    ("aurora_freeable_memory_mb", "AWS/RDS", "FreeableMemory", "Average"),
    ("aurora_db_connections", "AWS/RDS", "DatabaseConnections", "Average"),
    ("aurora_write_iops", "AWS/RDS", "WriteIOPS", "Average"),
    ("aurora_read_iops", "AWS/RDS", "ReadIOPS", "Average"),
    ("aurora_write_throughput", "AWS/RDS", "WriteThroughput", "Average"),
    ("aurora_read_throughput", "AWS/RDS", "ReadThroughput", "Average"),
    ("aurora_write_latency_ms", "AWS/RDS", "WriteLatency", "Average"),
    ("aurora_commit_latency_ms", "AWS/RDS", "CommitLatency", "Average"),
    ("aurora_dml_latency_ms", "AWS/RDS", "DMLLatency", "Average"),
    ("aurora_insert_latency_ms", "AWS/RDS", "InsertLatency", "Average"),
    ("aurora_update_latency_ms", "AWS/RDS", "UpdateLatency", "Average"),
    ("aurora_delete_latency_ms", "AWS/RDS", "DeleteLatency", "Average"),
    ("aurora_select_latency_ms", "AWS/RDS", "SelectLatency", "Average"),
    ("aurora_ddl_latency_ms", "AWS/RDS", "DDLLatency", "Average"),
    # Server-side p99 latencies via CloudWatch ExtendedStatistics
    ("aurora_write_latency_p99_ms", "AWS/RDS", "WriteLatency", "p99"),
    ("aurora_commit_latency_p99_ms", "AWS/RDS", "CommitLatency", "p99"),
    ("aurora_dml_latency_p99_ms", "AWS/RDS", "DMLLatency", "p99"),
    ("aurora_insert_latency_p99_ms", "AWS/RDS", "InsertLatency", "p99"),
    ("aurora_update_latency_p99_ms", "AWS/RDS", "UpdateLatency", "p99"),
    ("aurora_delete_latency_p99_ms", "AWS/RDS", "DeleteLatency", "p99"),
    ("aurora_select_latency_p99_ms", "AWS/RDS", "SelectLatency", "p99"),
    ("aurora_ddl_latency_p99_ms", "AWS/RDS", "DDLLatency", "p99"),
    ("aurora_queries", "AWS/RDS", "Queries", "Average"),
    ("aurora_active_transactions", "AWS/RDS", "ActiveTransactions", "Average"),
    ("aurora_engine_uptime", "AWS/RDS", "EngineUptime", "Average"),
    ("aurora_network_receive_tp", "AWS/RDS", "NetworkReceiveThroughput", "Average"),
    ("aurora_network_transmit_tp", "AWS/RDS", "NetworkTransmitThroughput", "Average"),
]

_CW_EC2_QUERIES = [
    ("ec2_cpu_avg_pct", "AWS/EC2", "CPUUtilization", "Average"),
    ("ec2_network_in", "AWS/EC2", "NetworkIn", "Average"),
    ("ec2_network_out", "AWS/EC2", "NetworkOut", "Average"),
]

_CW_ELASTICACHE_QUERIES = [
    ("cache_cpu_avg_pct", "AWS/ElastiCache", "CPUUtilization", "Average"),
    ("cache_curr_connections", "AWS/ElastiCache", "CurrConnections", "Average"),
    ("cache_bytes_used", "AWS/ElastiCache", "BytesUsedForCache", "Average"),
    ("cache_evictions", "AWS/ElastiCache", "Evictions", "Sum"),
    ("cache_cache_hits", "AWS/ElastiCache", "CacheHits", "Sum"),
    ("cache_cache_misses", "AWS/ElastiCache", "CacheMisses", "Sum"),
    ("cache_curr_items", "AWS/ElastiCache", "CurrItems", "Average"),
    ("cache_network_bytes_in", "AWS/ElastiCache", "NetworkBytesIn", "Sum"),
    ("cache_network_bytes_out", "AWS/ElastiCache", "NetworkBytesOut", "Sum"),
    ("cache_replication_lag", "AWS/ElastiCache", "ReplicationLag", "Average"),
]


def _cw_queries_for_server_type(server_type):
    if server_type == "aurora":
        return _CW_AURORA_QUERIES
    elif server_type == "tidb":
        return _CW_EC2_QUERIES
    elif server_type == "valkey":
        return _CW_ELASTICACHE_QUERIES
    return []


def _cw_dimension_for_server_type(server_type, resource_id):
    if server_type == "aurora":
        return [{"Name": "DBInstanceIdentifier", "Value": resource_id}]
    elif server_type == "tidb":
        return [{"Name": "InstanceId", "Value": resource_id}]
    elif server_type == "valkey":
        return [{"Name": "CacheClusterId", "Value": resource_id}]
    return []


def _cw_transform_value(key, value):
    if "memory" in key or "bytes_used" in key:
        return round(value / (1024 * 1024), 0)
    if "latency" in key:
        return round(value * 1000, 4)
    if "iops" in key:
        return round(value, 0)
    return round(value, 2)


def _is_extended_stat(stat):
    return stat.startswith("p") and stat[1:].replace(".", "").isdigit()


def query_cloudwatch_metrics(server_type, resource_id, start_epoch, end_epoch,
                             region=None, profile=None):
    queries = _cw_queries_for_server_type(server_type)
    if not queries:
        return {}

    dimensions = _cw_dimension_for_server_type(server_type, resource_id)
    start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc)

    session = aws_session()
    cw = session.client("cloudwatch", region_name=region or session.region_name)

    metrics = {}
    for key, namespace, metric_name, stat in queries:
        try:
            extended = _is_extended_stat(stat)
            kwargs = dict(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_dt,
                EndTime=end_dt,
                Period=60,
            )
            if extended:
                kwargs["ExtendedStatistics"] = [stat]
            else:
                kwargs["Statistics"] = [stat]

            resp = cw.get_metric_statistics(**kwargs)
            points = resp.get("Datapoints", [])
            if points:
                if extended:
                    vals = [p["ExtendedStatistics"][stat] for p in points
                            if stat in p.get("ExtendedStatistics", {})]
                    val = sum(vals) / len(vals) if vals else None
                else:
                    val = sum(p[stat] for p in points) / len(points)
                metrics[key] = _cw_transform_value(key, val) if val is not None else None
            else:
                metrics[key] = None
        except Exception:
            metrics[key] = None
    return metrics


# ---------------------------------------------------------------------------
# Full windowed analysis (combines everything)
# ---------------------------------------------------------------------------

def analyze_window(sysbench_text, csv_path, run_start_epoch,
                   window_start=30, window_end=90, server_type="aurora",
                   resource_id=None, region=None, profile=None):
    all_intervals = parse_interval_lines(sysbench_text)
    summary = parse_sysbench_summary(sysbench_text)

    window_intervals = [
        iv for iv in all_intervals
        if window_start <= iv["time_s"] <= window_end
    ]

    metrics_rows = parse_metrics_csv(csv_path)
    window_epoch_start = run_start_epoch + window_start
    window_epoch_end = run_start_epoch + window_end
    cpu_samples, db_samples = derive_interval_metrics(
        metrics_rows, window_epoch_start, window_epoch_end,
        server_type=server_type,
    )

    interval_data = build_interval_data(
        window_intervals, cpu_samples, db_samples, run_start_epoch,
        server_type=server_type,
    )

    latency_vals = [iv["latency_pct_ms"] for iv in window_intervals]
    latency_pct = window_intervals[0]["latency_pct"] if window_intervals else 99

    ws = {
        "tps": safe_stats([iv["tps"] for iv in window_intervals]),
        "qps": safe_stats([iv["qps"] for iv in window_intervals]),
        "client_latency_pct": latency_pct,
        "client_latency_pct_ms": safe_stats(latency_vals),
        "client_cpu_pct": safe_stats(
            [r.get("client_cpu_pct") for r in cpu_samples]
        ),
        "client_mem_used_mb": safe_stats(
            [r.get("client_mem_used_mb") for r in cpu_samples]
        ),
        "client_net_rx_mbps": safe_stats(
            [r.get("client_net_rx_mbps") for r in cpu_samples]
        ),
        "client_net_tx_mbps": safe_stats(
            [r.get("client_net_tx_mbps") for r in cpu_samples]
        ),
        "client_thermal_mc": safe_stats(
            [r.get("client_thermal_mc") for r in cpu_samples]
        ),
        "client_cpufreq_avg_mhz": safe_stats(
            [r.get("client_cpufreq_avg_mhz") for r in cpu_samples]
        ),
        "client_loadavg_1m": safe_stats(
            [r.get("client_loadavg_1m") for r in cpu_samples]
        ),
        "client_loadavg_5m": safe_stats(
            [r.get("client_loadavg_5m") for r in cpu_samples]
        ),
        "client_loadavg_15m": safe_stats(
            [r.get("client_loadavg_15m") for r in cpu_samples]
        ),
        "client_disk_read_iops": safe_stats(
            [r.get("client_disk_read_iops") for r in cpu_samples]
        ),
        "client_disk_write_iops": safe_stats(
            [r.get("client_disk_write_iops") for r in cpu_samples]
        ),
        "client_disk_read_mbps": safe_stats(
            [r.get("client_disk_read_mbps") for r in cpu_samples]
        ),
        "client_disk_write_mbps": safe_stats(
            [r.get("client_disk_write_mbps") for r in cpu_samples]
        ),
        "client_disk_util_pct": safe_stats(
            [r.get("client_disk_util_pct") for r in cpu_samples]
        ),
        "client_ctx_per_sec": safe_stats(
            [r.get("client_ctx_per_sec") for r in cpu_samples]
        ),
        "client_forks_per_sec": safe_stats(
            [r.get("client_forks_per_sec") for r in cpu_samples]
        ),
    }

    if server_type in ("aurora", "tidb"):
        for rate_key in ("total_iud_per_sec", "insert_per_sec", "update_per_sec",
                         "delete_per_sec", "read_per_sec", "commits_per_sec"):
            ws[rate_key] = safe_stats([r.get(rate_key) for r in db_samples])
    elif server_type == "valkey":
        for rate_key in ("commands_per_sec", "hits_per_sec", "misses_per_sec",
                         "hit_rate_pct", "instantaneous_ops_per_sec"):
            ws[rate_key] = safe_stats([r.get(rate_key) for r in db_samples])

    cloudwatch = {}
    if resource_id:
        cw_start = run_start_epoch + window_start - 60
        cw_end = run_start_epoch + window_end + 60
        cloudwatch = query_cloudwatch_metrics(
            server_type, resource_id, cw_start, cw_end,
            region=region, profile=profile,
        )

    server_latency = {}
    server_resources = {}
    for cw_key, cw_val in cloudwatch.items():
        if cw_val is None:
            continue
        if "latency" in cw_key:
            label = cw_key.replace("aurora_", "server_").replace("cache_", "server_")
            server_latency[label] = cw_val
        elif "cpu" in cw_key or "memory" in cw_key or "freeable" in cw_key:
            server_resources[cw_key] = cw_val
        elif "network" in cw_key:
            server_resources[cw_key] = cw_val
    if server_latency:
        ws["server_latency"] = server_latency
    if server_resources:
        ws["server_resources"] = server_resources

    return {
        "summary": summary,
        "window_stats": ws,
        "cloudwatch": cloudwatch,
        "interval_data": interval_data,
        "capture_window": {
            "start_s": window_start,
            "end_s": window_end,
            "duration_s": window_end - window_start,
        },
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
