"""Benchmark run comparison with regression detection.

Compares two benchmark result directories and produces Markdown + JSON
output highlighting QPS changes, latency deltas, efficiency metric shifts,
and detected regressions.

Usage::

    python -m common.compare --baseline results/run1 --current results/run2
"""

import json
import math
import statistics
from datetime import datetime
from pathlib import Path

THRESHOLD_RQPS_WARNING = 0.95    # rQPS below this = >5% QPS drop
THRESHOLD_RQPS_CRITICAL = 0.90   # rQPS below this = >10% QPS drop

THRESHOLD_EFFICIENCY_WARNING = 10.0   # efficiency degrades >10%
THRESHOLD_EFFICIENCY_CRITICAL = 25.0  # efficiency degrades >25%

THRESHOLD_P99_WARNING = 20.0   # p99 latency increases >20%
THRESHOLD_P99_CRITICAL = 50.0  # p99 latency increases >50%

SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"

EFFICIENCY_METRICS = (
    "cpu_us_per_q",
    "cs_per_q",
    "rkb_per_q",
    "wkb_per_q",
    "iops_per_q",
)

EFFICIENCY_LABELS = {
    "cpu_us_per_q": "CPU us/q",
    "cs_per_q": "CS/q",
    "rkb_per_q": "rKB/q",
    "wkb_per_q": "wKB/q",
    "iops_per_q": "IOPS/q",
}


def _load_results(results_dir):
    results_dir = Path(results_dir)
    if not results_dir.is_dir():
        return []
    runs = []
    for f in sorted(results_dir.glob("*.json")):
        try:
            with f.open() as fh:
                data = json.load(fh)
                data["_source_file"] = f.name
                runs.append(data)
        except (json.JSONDecodeError, OSError):
            pass
    return runs


def _run_key(run):
    return (run.get("workload", "unknown"), run.get("threads", 0))


def _get_qps(run):
    tp = run.get("throughput", {})
    return tp.get("qps", run.get("qps"))


def _get_tps(run):
    tp = run.get("throughput", {})
    return tp.get("tps", run.get("tps"))


def _get_latency(run):
    lat = run.get("latency", {})
    return {
        "avg_ms": lat.get("avg_ms", lat.get("avg", run.get("latency_avg_ms"))),
        "p95_ms": lat.get("p95_ms", lat.get("p95", run.get("latency_p95_ms"))),
        "p99_ms": lat.get("p99_ms", lat.get("p99", run.get("latency_p99_ms"))),
    }


def _compute_efficiency(run):
    """Compute per-query efficiency from window_stats + QPS.

    Mirrors report._section_efficiency_metrics: cpu_us/q, cs/q, rkb/q,
    wkb/q, iops/q.
    """
    ws = run.get("window_stats", {})
    qps = _get_qps(run)
    if not qps or qps <= 0:
        return {}

    eff = {}

    cpu_pct = ws.get("client_cpu_pct", {}).get("avg")
    if cpu_pct is not None:
        eff["cpu_us_per_q"] = (cpu_pct / 100.0) * 1_000_000 / qps

    ctx_per_sec = ws.get("client_ctx_per_sec", {}).get("avg")
    if ctx_per_sec is not None:
        eff["cs_per_q"] = ctx_per_sec / qps

    disk_read_mbps = ws.get("client_disk_read_mbps", {}).get("avg")
    if disk_read_mbps is not None:
        eff["rkb_per_q"] = disk_read_mbps * 1024 / qps

    disk_write_mbps = ws.get("client_disk_write_mbps", {}).get("avg")
    if disk_write_mbps is not None:
        eff["wkb_per_q"] = disk_write_mbps * 1024 / qps

    disk_read_iops = ws.get("client_disk_read_iops", {}).get("avg")
    disk_write_iops = ws.get("client_disk_write_iops", {}).get("avg")
    if disk_read_iops is not None and disk_write_iops is not None:
        eff["iops_per_q"] = (disk_read_iops + disk_write_iops) / qps

    return eff


def _pct_change(current, baseline):
    if baseline is None or baseline == 0 or current is None:
        return None
    return ((current - baseline) / abs(baseline)) * 100.0


def _safe_ratio(current, baseline):
    if baseline is None or baseline == 0 or current is None:
        return None
    return current / baseline


def _avg_of(values):
    return statistics.mean(values) if values else None


def _compare_workload(key, baseline_runs, current_runs):
    b_qps = _avg_of([v for v in (_get_qps(r) for r in baseline_runs)
                      if v is not None])
    c_qps = _avg_of([v for v in (_get_qps(r) for r in current_runs)
                      if v is not None])
    b_tps = _avg_of([v for v in (_get_tps(r) for r in baseline_runs)
                      if v is not None])
    c_tps = _avg_of([v for v in (_get_tps(r) for r in current_runs)
                      if v is not None])

    rqps = _safe_ratio(c_qps, b_qps)

    b_lats = [_get_latency(r) for r in baseline_runs]
    c_lats = [_get_latency(r) for r in current_runs]

    lat_comparison = {}
    for metric in ("avg_ms", "p95_ms", "p99_ms"):
        b_val = _avg_of([la[metric] for la in b_lats
                         if la.get(metric) is not None])
        c_val = _avg_of([la[metric] for la in c_lats
                         if la.get(metric) is not None])
        lat_comparison[f"baseline_{metric}"] = b_val
        lat_comparison[f"current_{metric}"] = c_val
        lat_comparison[f"{metric}_pct_change"] = _pct_change(c_val, b_val)

    b_effs = [_compute_efficiency(r) for r in baseline_runs]
    c_effs = [_compute_efficiency(r) for r in current_runs]

    eff_comparison = {}
    for metric in EFFICIENCY_METRICS:
        b_vals = [e[metric] for e in b_effs if metric in e]
        c_vals = [e[metric] for e in c_effs if metric in e]
        b_avg = _avg_of(b_vals)
        c_avg = _avg_of(c_vals)
        eff_comparison[metric] = {
            "baseline": b_avg,
            "current": c_avg,
            "pct_change": _pct_change(c_avg, b_avg),
        }

    cost_comparison = None
    b_inst = baseline_runs[0].get("instance_type") if baseline_runs else None
    c_inst = current_runs[0].get("instance_type") if current_runs else None
    if b_inst and c_inst and b_inst != c_inst:
        cost_comparison = {
            "baseline_instance": b_inst,
            "current_instance": c_inst,
        }

    return {
        "key": list(key),
        "workload": key[0],
        "threads": key[1],
        "baseline_qps": b_qps,
        "current_qps": c_qps,
        "rqps": rqps,
        "qps_pct_change": _pct_change(c_qps, b_qps),
        "baseline_tps": b_tps,
        "current_tps": c_tps,
        "latency": lat_comparison,
        "efficiency": eff_comparison,
        "cost_comparison": cost_comparison,
        "baseline_count": len(baseline_runs),
        "current_count": len(current_runs),
    }


def _detect_regressions(workload_comparisons):
    regressions = []

    for wc in workload_comparisons:
        label = f"{wc['workload']} (threads={wc['threads']})"

        rqps = wc.get("rqps")
        if rqps is not None and rqps < THRESHOLD_RQPS_WARNING:
            severity = (SEVERITY_CRITICAL if rqps < THRESHOLD_RQPS_CRITICAL
                        else SEVERITY_WARNING)
            regressions.append({
                "workload": label,
                "metric": "QPS",
                "baseline_value": wc["baseline_qps"],
                "current_value": wc["current_qps"],
                "pct_change": wc.get("qps_pct_change"),
                "severity": severity,
            })

        p99_pct = wc["latency"].get("p99_ms_pct_change")
        if p99_pct is not None and p99_pct > THRESHOLD_P99_WARNING:
            severity = (SEVERITY_CRITICAL if p99_pct > THRESHOLD_P99_CRITICAL
                        else SEVERITY_WARNING)
            regressions.append({
                "workload": label,
                "metric": "p99 latency",
                "baseline_value": wc["latency"].get("baseline_p99_ms"),
                "current_value": wc["latency"].get("current_p99_ms"),
                "pct_change": p99_pct,
                "severity": severity,
            })

        for metric_key in EFFICIENCY_METRICS:
            eff = wc.get("efficiency", {}).get(metric_key, {})
            pct = eff.get("pct_change")
            if pct is not None and pct > THRESHOLD_EFFICIENCY_WARNING:
                severity = (SEVERITY_CRITICAL
                            if pct > THRESHOLD_EFFICIENCY_CRITICAL
                            else SEVERITY_WARNING)
                regressions.append({
                    "workload": label,
                    "metric": EFFICIENCY_LABELS.get(metric_key, metric_key),
                    "baseline_value": eff.get("baseline"),
                    "current_value": eff.get("current"),
                    "pct_change": pct,
                    "severity": severity,
                })

    return regressions


def compare_runs(baseline_dir, current_dir):
    """Compare two benchmark result directories.

    Returns dict with keys:
    - summary: avg/min/max rQPS, regression counts
    - workloads: per-(workload, threads) comparison with rQPS, latency
      deltas, efficiency deltas, cost comparison
    - regressions: list of detected regressions with severity
    """
    baseline_dir = str(baseline_dir)
    current_dir = str(current_dir)

    baseline_runs = _load_results(baseline_dir)
    current_runs = _load_results(current_dir)

    if not baseline_runs:
        raise ValueError(
            f"No JSON results found in baseline directory: {baseline_dir}")
    if not current_runs:
        raise ValueError(
            f"No JSON results found in current directory: {current_dir}")

    baseline_by_key = {}
    for r in baseline_runs:
        baseline_by_key.setdefault(_run_key(r), []).append(r)

    current_by_key = {}
    for r in current_runs:
        current_by_key.setdefault(_run_key(r), []).append(r)

    all_keys = sorted(set(baseline_by_key) | set(current_by_key))

    workload_comparisons = [
        _compare_workload(
            key,
            baseline_by_key.get(key, []),
            current_by_key.get(key, []),
        )
        for key in all_keys
    ]

    regressions = _detect_regressions(workload_comparisons)

    rqps_vals: list[float] = []
    for wc in workload_comparisons:
        _rq = wc["rqps"]
        if isinstance(_rq, (int, float)):
            rqps_vals.append(_rq)

    summary = {
        "baseline_dir": baseline_dir,
        "current_dir": current_dir,
        "baseline_run_count": len(baseline_runs),
        "current_run_count": len(current_runs),
        "workloads_compared": len(workload_comparisons),
        "workloads_baseline_only": sum(
            1 for wc in workload_comparisons
            if wc["baseline_qps"] is not None and wc["current_qps"] is None
        ),
        "workloads_current_only": sum(
            1 for wc in workload_comparisons
            if wc["baseline_qps"] is None and wc["current_qps"] is not None
        ),
        "avg_rqps": _avg_of(rqps_vals),
        "min_rqps": min(rqps_vals) if rqps_vals else None,
        "max_rqps": max(rqps_vals) if rqps_vals else None,
        "regression_count": len(regressions),
        "critical_count": sum(1 for r in regressions
                              if r["severity"] == SEVERITY_CRITICAL),
        "warning_count": sum(1 for r in regressions
                             if r["severity"] == SEVERITY_WARNING),
        "timestamp": datetime.now().isoformat(),
    }

    return {
        "summary": summary,
        "workloads": workload_comparisons,
        "regressions": regressions,
    }


def _fmt(v, decimals=2):
    if v is None:
        return "N/A"
    if isinstance(v, str):
        return v
    if abs(v) >= 1000:
        return f"{v:,.{decimals}f}"
    return f"{v:.{decimals}f}"


def _fmt_pct(v):
    if v is None:
        return "N/A"
    return f"{v:+.1f}%"


def _status_label(rqps):
    if rqps is None:
        return "N/A"
    if rqps >= 1.05:
        return "IMPROVED"
    if rqps >= THRESHOLD_RQPS_WARNING:
        return "OK"
    if rqps >= THRESHOLD_RQPS_CRITICAL:
        return "REGRESSED"
    return "CRITICAL"


def generate_comparison_report(comparison):
    """Generate a Markdown comparison report from a comparison dict."""
    lines = []
    summary = comparison["summary"]
    workloads = comparison["workloads"]
    regressions = comparison["regressions"]

    lines.append("# Benchmark Comparison Report")
    lines.append("")
    lines.append(
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| Baseline | {summary['baseline_dir']} |")
    lines.append(f"| Current | {summary['current_dir']} |")
    lines.append(
        f"| Workloads Compared | {summary['workloads_compared']} |")
    lines.append(f"| Average rQPS | {_fmt(summary['avg_rqps'], 3)} |")
    lines.append(f"| Min rQPS | {_fmt(summary['min_rqps'], 3)} |")
    lines.append(f"| Max rQPS | {_fmt(summary['max_rqps'], 3)} |")
    lines.append(
        f"| Regressions | {summary['regression_count']} "
        f"({summary['critical_count']} critical, "
        f"{summary['warning_count']} warning) |")
    lines.append("")

    baseline_only = summary.get("workloads_baseline_only", 0)
    current_only = summary.get("workloads_current_only", 0)
    if baseline_only > 0:
        lines.append(
            f"> **Note:** {baseline_only} workload(s) present only in "
            f"baseline (removed or renamed)")
        lines.append("")
    if current_only > 0:
        lines.append(
            f"> **Note:** {current_only} workload(s) present only in "
            f"current (new)")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## QPS Comparison")
    lines.append("")
    lines.append(
        "| Workload | Threads | Baseline QPS | Current QPS "
        "| rQPS | Change | Status |")
    lines.append(
        "|----------|--------:|-------------:|------------:"
        "|-----:|-------:|--------|")

    for wc in workloads:
        lines.append(
            f"| {wc['workload']} | {wc['threads']} "
            f"| {_fmt(wc['baseline_qps'])} | {_fmt(wc['current_qps'])} "
            f"| {_fmt(wc['rqps'], 3)} | {_fmt_pct(wc['qps_pct_change'])} "
            f"| {_status_label(wc['rqps'])} |")
    lines.append("")

    _append_latency_section(lines, workloads)
    _append_efficiency_section(lines, workloads)
    _append_regression_section(lines, regressions)

    return "\n".join(lines) + "\n"


def _append_latency_section(lines, workloads):
    has_data = any(
        wc["latency"].get("baseline_p99_ms") is not None
        or wc["latency"].get("current_p99_ms") is not None
        for wc in workloads
    )
    if not has_data:
        return

    lines.append("---")
    lines.append("")
    lines.append("## Latency Comparison")
    lines.append("")
    lines.append(
        "| Workload | Threads "
        "| Base Avg (ms) | Cur Avg (ms) | Avg Change "
        "| Base P95 (ms) | Cur P95 (ms) | P95 Change "
        "| Base P99 (ms) | Cur P99 (ms) | P99 Change |")
    lines.append(
        "|----------|--------:"
        "|--------------:|-------------:|-----------:"
        "|--------------:|-------------:|-----------:"
        "|--------------:|-------------:|-----------:|")

    for wc in workloads:
        lat = wc["latency"]
        lines.append(
            f"| {wc['workload']} | {wc['threads']} "
            f"| {_fmt(lat.get('baseline_avg_ms'))} "
            f"| {_fmt(lat.get('current_avg_ms'))} "
            f"| {_fmt_pct(lat.get('avg_ms_pct_change'))} "
            f"| {_fmt(lat.get('baseline_p95_ms'))} "
            f"| {_fmt(lat.get('current_p95_ms'))} "
            f"| {_fmt_pct(lat.get('p95_ms_pct_change'))} "
            f"| {_fmt(lat.get('baseline_p99_ms'))} "
            f"| {_fmt(lat.get('current_p99_ms'))} "
            f"| {_fmt_pct(lat.get('p99_ms_pct_change'))} |")
    lines.append("")


def _append_efficiency_section(lines, workloads):
    has_data = any(
        any(wc["efficiency"].get(m, {}).get("baseline") is not None
            for m in EFFICIENCY_METRICS)
        for wc in workloads
    )
    if not has_data:
        return

    lines.append("---")
    lines.append("")
    lines.append("## Efficiency Comparison (per query)")
    lines.append("")
    lines.append(
        "*Lower is better. Positive change means regression "
        "(more resource per query).*")
    lines.append("")
    lines.append(
        "| Workload | Threads "
        "| CPU us/q base | CPU us/q cur | Change "
        "| CS/q base | CS/q cur | Change "
        "| IOPS/q base | IOPS/q cur | Change |")
    lines.append(
        "|----------|--------:"
        "|--------------:|-------------:|-------:"
        "|----------:|---------:|-------:"
        "|------------:|-----------:|-------:|")

    for wc in workloads:
        eff = wc["efficiency"]
        cpu = eff.get("cpu_us_per_q", {})
        cs = eff.get("cs_per_q", {})
        iops = eff.get("iops_per_q", {})
        lines.append(
            f"| {wc['workload']} | {wc['threads']} "
            f"| {_fmt(cpu.get('baseline'))} "
            f"| {_fmt(cpu.get('current'))} "
            f"| {_fmt_pct(cpu.get('pct_change'))} "
            f"| {_fmt(cs.get('baseline'), 3)} "
            f"| {_fmt(cs.get('current'), 3)} "
            f"| {_fmt_pct(cs.get('pct_change'))} "
            f"| {_fmt(iops.get('baseline'), 3)} "
            f"| {_fmt(iops.get('current'), 3)} "
            f"| {_fmt_pct(iops.get('pct_change'))} |")
    lines.append("")


def _append_regression_section(lines, regressions):
    lines.append("---")
    lines.append("")

    if not regressions:
        lines.append("## Regressions")
        lines.append("")
        lines.append("No regressions detected.")
        lines.append("")
        return

    lines.append("## Regressions Detected")
    lines.append("")

    critical = [r for r in regressions if r["severity"] == SEVERITY_CRITICAL]
    warnings = [r for r in regressions if r["severity"] == SEVERITY_WARNING]

    for group_label, group in (("Critical", critical), ("Warnings", warnings)):
        if not group:
            continue
        lines.append(f"### {group_label}")
        lines.append("")
        lines.append("| Workload | Metric | Baseline | Current | Change |")
        lines.append("|----------|--------|----------|---------|--------|")
        for r in group:
            lines.append(
                f"| {r['workload']} | {r['metric']} "
                f"| {_fmt(r['baseline_value'])} "
                f"| {_fmt(r['current_value'])} "
                f"| {_fmt_pct(r['pct_change'])} |")
        lines.append("")


def _make_serializable(obj):
    """Deep-convert for JSON: tuple -> list, NaN/Inf -> None."""
    if isinstance(obj, dict):
        return {str(k): _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(item) for item in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def save_comparison(comparison, output_path):
    """Save comparison dict as a JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    serializable = _make_serializable(comparison)
    with output_path.open("w") as fh:
        json.dump(serializable, fh, indent=2, default=str)


def main():
    """CLI: python -m common.compare --baseline dir1 --current dir2"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Compare two benchmark result directories")
    parser.add_argument(
        "--baseline", required=True,
        help="Baseline results directory")
    parser.add_argument(
        "--current", required=True,
        help="Current results directory")
    parser.add_argument(
        "--output",
        help="Markdown output file (default: stdout)")
    parser.add_argument(
        "--json", dest="json_output",
        help="JSON output file")
    args = parser.parse_args()

    try:
        comparison = compare_runs(args.baseline, args.current)
    except ValueError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)

    md = generate_comparison_report(comparison)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(md)
    else:
        sys.stdout.write(md)

    if args.json_output:
        save_comparison(comparison, args.json_output)


if __name__ == "__main__":
    main()
