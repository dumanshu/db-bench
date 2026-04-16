"""Hardware performance counter collection via perf stat for bare-metal EC2 instances.

Runs ``perf stat`` on remote hosts over SSH to collect CPU hardware counters
(cycles, instructions, cache misses, TLB misses, branch mispredictions, etc.).

Only meaningful on ``.metal`` instance types where the hypervisor does not
virtualise the PMU.  All functions degrade gracefully when perf is unavailable
-- they return empty dicts / None rather than raising.

Usage as library::

    from common.perf_stat import collect_hw_counters, is_metal_instance

    if is_metal_instance(instance_type):
        results = collect_hw_counters(host_ip, key_path, duration=60,
                                      output_dir="/tmp/results")
"""

import json
import os
import re
import time

from common.ssh import ssh_run_simple
from common.util import log

# Events collected by perf stat.  Order matters for the command line.
_PERF_EVENTS = [
    "cycles",
    "instructions",
    "cache-references",
    "cache-misses",
    "LLC-loads",
    "LLC-load-misses",
    "dTLB-loads",
    "dTLB-load-misses",
    "branch-instructions",
    "branch-misses",
    "context-switches",
    "cpu-migrations",
]

_PERF_EVENT_LIST = ",".join(_PERF_EVENTS)

# Remote paths used for intermediate files.
_REMOTE_PERF_OUTPUT = "/tmp/perf_stat_output.txt"


# ---------------------------------------------------------------------------
# Instance type gate
# ---------------------------------------------------------------------------

def is_metal_instance(instance_type: str) -> bool:
    """Check if *instance_type* supports hardware perf counters.

    Returns ``True`` for bare-metal instance types (those containing
    ``.metal``), which expose the real PMU to the OS.
    """
    return ".metal" in instance_type.lower()


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

def check_perf_available(host_ip: str, key_path: str,
                         user: str = "ec2-user") -> bool:
    """Return ``True`` if ``perf`` is installed and functional on *host_ip*.

    Runs a quick smoke-test (``perf stat -e cycles -a -- sleep 0.1``) to
    confirm the kernel allows hardware counter access.
    """
    try:
        r = ssh_run_simple(
            host_ip, key_path,
            "which perf && sudo perf stat -e cycles -a -- sleep 0.1 2>&1",
            timeout=30, user=user,
        )
        return r.returncode == 0
    except Exception as exc:
        log(f"perf availability check failed on {host_ip}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Start / stop collection
# ---------------------------------------------------------------------------

def start_perf_stat(host_ip: str, key_path: str, duration: int = 60,
                    user: str = "ec2-user", pid=None):
    """Start ``perf stat`` collection on *host_ip*.

    Collects the standard set of hardware events for *duration* seconds,
    writing results to a temporary file on the remote host.  The ``perf``
    process runs in the background via ``nohup``.

    Parameters
    ----------
    pid : int, optional
        If given, attach ``perf stat`` to that specific PID instead of
        system-wide (``-a``).

    Returns
    -------
    str or None
        The remote PID of the ``perf stat`` process, or ``None`` if perf
        is unavailable or failed to start.
    """
    if not check_perf_available(host_ip, key_path, user=user):
        log(f"perf not available on {host_ip} -- skipping hw counter collection")
        return None

    target_flag = f"-p {pid}" if pid else "-a"
    script = (
        f"rm -f {_REMOTE_PERF_OUTPUT}; "
        f"nohup sudo perf stat -e {_PERF_EVENT_LIST} "
        f"{target_flag} -o {_REMOTE_PERF_OUTPUT} "
        f"-- sleep {duration} > /dev/null 2>&1 & "
        f"echo $!"
    )

    try:
        r = ssh_run_simple(host_ip, key_path, script, timeout=30, user=user)
        perf_pid = r.stdout.strip()
        if perf_pid and perf_pid.isdigit():
            log(f"perf stat started on {host_ip} (pid={perf_pid}, "
                f"duration={duration}s)")
            return perf_pid
        log(f"perf stat start on {host_ip} returned unexpected output: "
            f"{r.stdout!r}")
        return None
    except Exception as exc:
        log(f"Failed to start perf stat on {host_ip}: {exc}")
        return None


def stop_perf_stat(host_ip: str, key_path: str, perf_pid: str,
                   user: str = "ec2-user"):
    """Stop a running ``perf stat`` process and collect results.

    Sends SIGINT to *perf_pid* (which causes perf to flush its output),
    waits briefly, then reads and parses the output file.

    Returns
    -------
    dict
        Parsed counters with derived ratios (IPC, miss rates, etc.),
        or an empty dict on failure.
    """
    if not perf_pid:
        return {}

    try:
        ssh_run_simple(
            host_ip, key_path,
            f"sudo kill -INT {perf_pid} 2>/dev/null; "
            f"for i in $(seq 1 10); do "
            f"  kill -0 {perf_pid} 2>/dev/null || break; "
            f"  sleep 0.5; "
            f"done",
            timeout=30, user=user,
        )
    except Exception as exc:
        log(f"Warning: failed to stop perf pid {perf_pid} on {host_ip}: {exc}")

    try:
        r = ssh_run_simple(
            host_ip, key_path,
            f"cat {_REMOTE_PERF_OUTPUT} 2>/dev/null",
            timeout=30, user=user,
        )
        if r.returncode != 0 or not r.stdout.strip():
            log(f"No perf stat output on {host_ip}")
            return {}
        return parse_perf_stat_output(r.stdout)
    except Exception as exc:
        log(f"Failed to read perf stat output from {host_ip}: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Interval collection (time-series)
# ---------------------------------------------------------------------------

def run_perf_stat_interval(host_ip: str, key_path: str, duration: int = 60,
                           interval_ms: int = 1000,
                           user: str = "ec2-user"):
    """Run ``perf stat`` with ``-I`` for per-interval time-series data.

    Blocks for *duration* seconds while perf collects, then parses the
    interval output.

    Returns
    -------
    list[dict]
        One dict per interval with the same fields as
        :func:`stop_perf_stat`.  Empty list on failure.
    """
    if not check_perf_available(host_ip, key_path, user=user):
        log(f"perf not available on {host_ip} -- skipping interval collection")
        return []

    script = (
        f"sudo perf stat -e {_PERF_EVENT_LIST} "
        f"-a -I {interval_ms} "
        f"-- sleep {duration} 2>&1"
    )

    try:
        r = ssh_run_simple(
            host_ip, key_path, script,
            timeout=duration + 60, user=user,
        )
        if r.returncode != 0 and not r.stdout.strip():
            log(f"perf stat interval failed on {host_ip}: {r.stderr}")
            return []
        # perf stat -I writes to stderr; some SSH wrappers merge streams.
        output = r.stderr if r.stderr.strip() else r.stdout
        return _parse_interval_output(output)
    except Exception as exc:
        log(f"perf stat interval collection failed on {host_ip}: {exc}")
        return []


# ---------------------------------------------------------------------------
# Output parsing
# ---------------------------------------------------------------------------

# Matches lines like:
#   1,234,567      cycles                    #    3.456 GHz
#   2,345,678      instructions              #    1.90  insn per cycle
# Also handles:
#   <not supported>  LLC-loads
#   <not counted>    cache-references
_RE_PERF_LINE = re.compile(
    r"^\s*"
    r"(?P<value>[\d,]+|<not supported>|<not counted>)"
    r"\s+"
    r"(?P<event>[\w-]+)"
)

# Interval mode prepends a timestamp:
#   1.000123456      1,234,567      cycles   ...
_RE_INTERVAL_LINE = re.compile(
    r"^\s*"
    r"(?P<time>[\d.]+)"
    r"\s+"
    r"(?P<value>[\d,]+|<not supported>|<not counted>)"
    r"\s+"
    r"(?P<event>[\w-]+)"
)


def parse_perf_stat_output(output: str):
    """Parse ``perf stat`` text output into a structured dict.

    Handles ``<not supported>`` and ``<not counted>`` by recording them
    as ``None`` in the raw values.

    Returns
    -------
    dict
        Keys: ``ipc``, ``cache_miss_rate``, ``llc_miss_rate``,
        ``tlb_miss_rate``, ``branch_miss_rate``, ``ctx_switches``,
        ``cpu_migrations``, ``raw`` (dict of all counter values).
    """
    raw = _extract_raw_counters(output, _RE_PERF_LINE)
    return _derive_ratios(raw)


def _extract_raw_counters(output, pattern):
    """Pull counter name -> int mappings from *output* using *pattern*."""
    raw = {}
    for line in output.splitlines():
        m = pattern.search(line)
        if not m:
            continue
        val_str = m.group("value")
        event = m.group("event")
        if val_str.startswith("<"):
            raw[event] = None
        else:
            raw[event] = int(val_str.replace(",", ""))
    return raw


def _safe_div(numerator, denominator):
    """Return numerator/denominator rounded to 6 dp, or None."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator, 6)


def _derive_ratios(raw):
    """Compute derived metrics from raw counter values."""
    cycles = raw.get("cycles")
    instructions = raw.get("instructions")
    cache_refs = raw.get("cache-references")
    cache_misses = raw.get("cache-misses")
    llc_loads = raw.get("LLC-loads")
    llc_misses = raw.get("LLC-load-misses")
    dtlb_loads = raw.get("dTLB-loads")
    dtlb_misses = raw.get("dTLB-load-misses")
    branch_insns = raw.get("branch-instructions")
    branch_misses = raw.get("branch-misses")

    return {
        "ipc": _safe_div(instructions, cycles),
        "cache_miss_rate": _safe_div(cache_misses, cache_refs),
        "llc_miss_rate": _safe_div(llc_misses, llc_loads),
        "tlb_miss_rate": _safe_div(dtlb_misses, dtlb_loads),
        "branch_miss_rate": _safe_div(branch_misses, branch_insns),
        "ctx_switches": raw.get("context-switches"),
        "cpu_migrations": raw.get("cpu-migrations"),
        "raw": raw,
    }


def _parse_interval_output(output):
    """Parse ``perf stat -I`` output into a list of per-interval dicts."""
    buckets = {}  # timestamp_str -> {event: value}
    for line in output.splitlines():
        m = _RE_INTERVAL_LINE.search(line)
        if not m:
            continue
        ts = m.group("time")
        val_str = m.group("value")
        event = m.group("event")
        if ts not in buckets:
            buckets[ts] = {}
        if val_str.startswith("<"):
            buckets[ts][event] = None
        else:
            buckets[ts][event] = int(val_str.replace(",", ""))

    results = []
    for ts in sorted(buckets, key=float):
        entry = _derive_ratios(buckets[ts])
        entry["time_s"] = float(ts)
        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# High-level integration helper
# ---------------------------------------------------------------------------

def collect_hw_counters(host_ip: str, key_path: str, duration: int,
                        user: str = "ec2-user",
                        output_dir=None):
    """Check availability, collect counters, parse, and optionally save.

    This is the recommended entry point for callers who just want a dict
    of hardware counter results.

    Returns
    -------
    dict
        Parsed results including ``available: True`` and all derived
        ratios, or ``{"available": False}`` when perf is not usable.
    """
    if not check_perf_available(host_ip, key_path, user=user):
        log(f"perf not available on {host_ip} -- returning empty hw counters")
        return {"available": False}

    perf_pid = start_perf_stat(host_ip, key_path, duration=duration, user=user)
    if not perf_pid:
        return {"available": False}

    log(f"Waiting {duration}s for perf stat collection on {host_ip}...")
    time.sleep(duration + 2)

    results = stop_perf_stat(host_ip, key_path, perf_pid, user=user)
    results["available"] = bool(results.get("raw"))

    if output_dir:
        try:
            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, "perf_stat.json")
            with open(out_path, "w") as f:
                json.dump(results, f, indent=2)
            log(f"perf stat results saved to {out_path}")
        except OSError as exc:
            log(f"Warning: could not save perf stat results: {exc}")

    return results
