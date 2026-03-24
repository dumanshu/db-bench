#!/usr/bin/env python3
"""Parse benchmark results with windowed metrics.

Filters sysbench intervals + metrics CSV to a [window_start, window_end] window,
queries CloudWatch for Aurora metrics (CPU, memory, commit/DML latency, IOPS),
and produces a comprehensive JSON result.
"""

import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
from datetime import datetime, timezone


def parse_interval_lines(output):
    intervals = []
    for line in output.splitlines():
        m = re.match(
            r"\[\s*([\d.]+)s\s*\]\s+thds:\s+(\d+)\s+"
            r"tps:\s+([\d.]+)\s+qps:\s+([\d.]+)\s+"
            r"\(r/w/o:\s+([\d.]+)/([\d.]+)/([\d.]+)\)\s+"
            r"lat\s+\(ms,\s*95%\):\s+([\d.]+)\s+"
            r"err/s:\s+([\d.]+)",
            line,
        )
        if m:
            intervals.append(
                {
                    "time_s": float(m.group(1)),
                    "threads": int(m.group(2)),
                    "tps": float(m.group(3)),
                    "qps": float(m.group(4)),
                    "read_qps": float(m.group(5)),
                    "write_qps": float(m.group(6)),
                    "other_qps": float(m.group(7)),
                    "p95_ms": float(m.group(8)),
                    "err_s": float(m.group(9)),
                }
            )
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
    for pct in ["min", "avg", "max", "95th percentile"]:
        key = pct.replace(" ", "_").replace("percentile", "pct")
        m = re.search(rf"{re.escape(pct)}:\s+([\d.]+)", output)
        if m:
            result[f"latency_{key}_ms"] = float(m.group(1))
    return result


def parse_metrics_csv(csv_path):
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                if v == "":
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


def compute_cpu_pct(prev, curr):
    fields = [
        "cpu_user", "cpu_nice", "cpu_system", "cpu_idle",
        "cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_steal",
    ]
    try:
        total_prev = sum(prev[f] for f in fields if prev.get(f) is not None)
        total_curr = sum(curr[f] for f in fields if curr.get(f) is not None)
        idle_prev = (prev.get("cpu_idle") or 0) + (prev.get("cpu_iowait") or 0)
        idle_curr = (curr.get("cpu_idle") or 0) + (curr.get("cpu_iowait") or 0)
        total_d = total_curr - total_prev
        idle_d = idle_curr - idle_prev
        if total_d > 0:
            return round((total_d - idle_d) / total_d * 100, 1)
    except (KeyError, TypeError):
        pass
    return None


def compute_iud_rates(prev, curr):
    dt = (curr.get("epoch") or 0) - (prev.get("epoch") or 0)
    if dt <= 0:
        dt = 5
    rates = {}
    for counter, rate_key in [
        ("innodb_inserted", "insert_per_sec"),
        ("innodb_updated", "update_per_sec"),
        ("innodb_deleted", "delete_per_sec"),
        ("innodb_read", "read_per_sec"),
        ("com_commit", "commits_per_sec"),
    ]:
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


def derive_interval_metrics(metrics_rows, window_epoch_start, window_epoch_end):
    cpu_samples = []
    iud_samples = []
    for i in range(1, len(metrics_rows)):
        prev = metrics_rows[i - 1]
        curr = metrics_rows[i]
        epoch = curr.get("epoch")
        if epoch is None:
            continue
        if epoch < window_epoch_start or epoch > window_epoch_end + 3:
            continue

        cpu_pct = compute_cpu_pct(prev, curr)
        cpu_samples.append({
            "epoch": epoch,
            "client_cpu_pct": cpu_pct,
            "client_mem_used_mb": curr.get("mem_used_mb"),
            "client_mem_total_mb": curr.get("mem_total_mb"),
        })

        rates = compute_iud_rates(prev, curr)
        rates["epoch"] = epoch
        iud_samples.append(rates)

    return cpu_samples, iud_samples


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
        result["p99"] = round(s[min(int(len(s) * 0.99), len(s) - 1)], 2)
    return result


CW_QUERIES = [
    ("aurora_cpu_avg_pct", "CPUUtilization", "Average"),
    ("aurora_cpu_max_pct", "CPUUtilization", "Maximum"),
    ("aurora_freeable_memory_mb", "FreeableMemory", "Average"),
    ("aurora_db_connections", "DatabaseConnections", "Average"),
    ("aurora_write_iops", "WriteIOPS", "Average"),
    ("aurora_read_iops", "ReadIOPS", "Average"),
    ("aurora_write_latency_ms", "WriteLatency", "Average"),
    ("aurora_commit_latency_ms", "CommitLatency", "Average"),
    ("aurora_dml_latency_ms", "DMLLatency", "Average"),
    ("aurora_insert_latency_ms", "InsertLatency", "Average"),
    ("aurora_update_latency_ms", "UpdateLatency", "Average"),
    ("aurora_delete_latency_ms", "DeleteLatency", "Average"),
    ("aurora_select_latency_ms", "SelectLatency", "Average"),
    ("aurora_ddl_latency_ms", "DDLLatency", "Average"),
]


def query_cloudwatch(writer_id, region, profile, start_epoch, end_epoch):
    start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    metrics = {}
    for key, metric_name, stat in CW_QUERIES:
        try:
            cmd = [
                "aws", "cloudwatch", "get-metric-statistics",
                "--namespace", "AWS/RDS",
                "--metric-name", metric_name,
                "--dimensions", f"Name=DBInstanceIdentifier,Value={writer_id}",
                "--start-time", start_dt,
                "--end-time", end_dt,
                "--period", "60",
                "--statistics", stat,
                "--profile", profile,
                "--region", region,
                "--output", "json",
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if r.returncode == 0:
                data = json.loads(r.stdout)
                points = data.get("Datapoints", [])
                if points:
                    val = sum(p[stat] for p in points) / len(points)
                    if "memory" in key:
                        val = round(val / (1024 * 1024), 0)
                    elif "latency" in key:
                        val = round(val * 1000, 4)
                    elif "iops" in key:
                        val = round(val, 0)
                    else:
                        val = round(val, 2)
                    metrics[key] = val
                else:
                    metrics[key] = None
            else:
                metrics[key] = None
        except Exception:
            metrics[key] = None
    return metrics


def find_closest(samples, target_epoch, max_delta=6):
    if not samples:
        return None
    best = min(samples, key=lambda r: abs(r["epoch"] - target_epoch))
    if abs(best["epoch"] - target_epoch) <= max_delta:
        return best
    return None


def build_interval_data(window_intervals, cpu_samples, iud_samples, run_start_epoch):
    rows = []
    for iv in window_intervals:
        iv_epoch = run_start_epoch + iv["time_s"]
        entry = {
            "time_s": iv["time_s"],
            "tps": iv["tps"],
            "qps": iv["qps"],
            "p95_latency_ms": iv["p95_ms"],
            "err_s": iv["err_s"],
        }
        closest_iud = find_closest(iud_samples, iv_epoch)
        if closest_iud:
            for k in (
                "insert_per_sec", "update_per_sec", "delete_per_sec",
                "read_per_sec", "total_iud_per_sec", "commits_per_sec",
            ):
                if k in closest_iud:
                    entry[k] = closest_iud[k]

        closest_cpu = find_closest(cpu_samples, iv_epoch)
        if closest_cpu:
            entry["client_cpu_pct"] = closest_cpu.get("client_cpu_pct")
            entry["client_mem_used_mb"] = closest_cpu.get("client_mem_used_mb")

        rows.append(entry)
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sysbench-output", required=True)
    p.add_argument("--metrics-csv", required=True)
    p.add_argument("--run-start-epoch", type=int, required=True)
    p.add_argument("--window-start", type=int, default=30)
    p.add_argument("--window-end", type=int, default=90)
    p.add_argument("--instance-type", required=True)
    p.add_argument("--ec2-type", default="r8g.48xlarge")
    p.add_argument("--threads", type=int, required=True)
    p.add_argument("--tables", type=int, default=64)
    p.add_argument("--table-size", type=int, default=1000000)
    p.add_argument("--duration", type=int, default=120)
    p.add_argument("--writer-id", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--rds-profile", default="sandbox-storage")
    p.add_argument("--sysbench-cmd", default="")
    p.add_argument("--output", required=True)
    args = p.parse_args()

    with open(args.sysbench_output) as f:
        sb_output = f.read()
    all_intervals = parse_interval_lines(sb_output)
    summary = parse_sysbench_summary(sb_output)

    window_intervals = [
        iv
        for iv in all_intervals
        if args.window_start <= iv["time_s"] <= args.window_end
    ]

    metrics_rows = parse_metrics_csv(args.metrics_csv)
    window_epoch_start = args.run_start_epoch + args.window_start
    window_epoch_end = args.run_start_epoch + args.window_end
    cpu_samples, iud_samples = derive_interval_metrics(
        metrics_rows, window_epoch_start, window_epoch_end
    )

    interval_data = build_interval_data(
        window_intervals, cpu_samples, iud_samples, args.run_start_epoch
    )

    ws = {
        "tps": safe_stats([iv["tps"] for iv in window_intervals]),
        "qps": safe_stats([iv["qps"] for iv in window_intervals]),
        "p95_latency_ms": safe_stats([iv["p95_ms"] for iv in window_intervals]),
        "total_iud_per_sec": safe_stats(
            [r.get("total_iud_per_sec") for r in iud_samples]
        ),
        "insert_per_sec": safe_stats(
            [r.get("insert_per_sec") for r in iud_samples]
        ),
        "update_per_sec": safe_stats(
            [r.get("update_per_sec") for r in iud_samples]
        ),
        "delete_per_sec": safe_stats(
            [r.get("delete_per_sec") for r in iud_samples]
        ),
        "read_per_sec": safe_stats(
            [r.get("read_per_sec") for r in iud_samples]
        ),
        "commits_per_sec": safe_stats(
            [r.get("commits_per_sec") for r in iud_samples]
        ),
        "client_cpu_pct": safe_stats(
            [r.get("client_cpu_pct") for r in cpu_samples]
        ),
        "client_mem_used_mb": safe_stats(
            [r.get("client_mem_used_mb") for r in cpu_samples]
        ),
    }

    cw_start = args.run_start_epoch + args.window_start - 60
    cw_end = args.run_start_epoch + args.window_end + 60
    cloudwatch = query_cloudwatch(
        args.writer_id, args.region, args.rds_profile, cw_start, cw_end
    )

    result = {
        "instance_type": args.instance_type,
        "ec2_instance_type": args.ec2_type,
        "threads": args.threads,
        "duration_s": args.duration,
        "capture_window": {
            "start_s": args.window_start,
            "end_s": args.window_end,
            "duration_s": args.window_end - args.window_start,
        },
        "tables": args.tables,
        "table_size": args.table_size,
        "workload": "custom_mixed",
        "commit_latency": {
            "full_run": {
                "min_ms": summary.get("latency_min_ms"),
                "avg_ms": summary.get("latency_avg_ms"),
                "p95_ms": summary.get("latency_95th_pct_ms"),
                "max_ms": summary.get("latency_max_ms"),
            },
            "window_p95_stats": ws.get("p95_latency_ms", {}),
            "aurora_commit_latency_ms": cloudwatch.get("aurora_commit_latency_ms"),
            "aurora_dml_latency_ms": cloudwatch.get("aurora_dml_latency_ms"),
            "aurora_insert_latency_ms": cloudwatch.get("aurora_insert_latency_ms"),
            "aurora_update_latency_ms": cloudwatch.get("aurora_update_latency_ms"),
            "aurora_delete_latency_ms": cloudwatch.get("aurora_delete_latency_ms"),
            "aurora_select_latency_ms": cloudwatch.get("aurora_select_latency_ms"),
            "aurora_write_latency_ms": cloudwatch.get("aurora_write_latency_ms"),
        },
        "iud_rates": {
            "window_avg": {
                "total_iud_per_sec": ws.get("total_iud_per_sec", {}).get("avg"),
                "insert_per_sec": ws.get("insert_per_sec", {}).get("avg"),
                "update_per_sec": ws.get("update_per_sec", {}).get("avg"),
                "delete_per_sec": ws.get("delete_per_sec", {}).get("avg"),
                "read_per_sec": ws.get("read_per_sec", {}).get("avg"),
                "commits_per_sec": ws.get("commits_per_sec", {}).get("avg"),
            },
            "window_stats": {
                "total_iud": ws.get("total_iud_per_sec", {}),
                "insert": ws.get("insert_per_sec", {}),
                "update": ws.get("update_per_sec", {}),
                "delete": ws.get("delete_per_sec", {}),
                "read": ws.get("read_per_sec", {}),
                "commits": ws.get("commits_per_sec", {}),
            },
        },
        "throughput": {
            "window_avg_tps": ws.get("tps", {}).get("avg"),
            "window_avg_qps": ws.get("qps", {}).get("avg"),
            "window_tps_stats": ws.get("tps", {}),
            "full_run_tps": summary.get("tps"),
            "full_run_qps": summary.get("qps"),
        },
        "resource_utilization": {
            "client_cpu": ws.get("client_cpu_pct", {}),
            "client_mem_used_mb": ws.get("client_mem_used_mb", {}),
            "aurora_cpu_avg_pct": cloudwatch.get("aurora_cpu_avg_pct"),
            "aurora_cpu_max_pct": cloudwatch.get("aurora_cpu_max_pct"),
            "aurora_freeable_memory_mb": cloudwatch.get("aurora_freeable_memory_mb"),
            "aurora_db_connections": cloudwatch.get("aurora_db_connections"),
            "aurora_write_iops": cloudwatch.get("aurora_write_iops"),
            "aurora_read_iops": cloudwatch.get("aurora_read_iops"),
        },
        "interval_data": interval_data,
        "sysbench_command": args.sysbench_cmd,
        "sysbench_full_summary": summary,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, default=str)

    iud_avg = ws.get("total_iud_per_sec", {}).get("avg", "N/A")
    tps_avg = ws.get("tps", {}).get("avg", "N/A")
    p95_avg = ws.get("p95_latency_ms", {}).get("avg", "N/A")
    p95_min = ws.get("p95_latency_ms", {}).get("min", "N/A")
    p95_max = ws.get("p95_latency_ms", {}).get("max", "N/A")
    ccpu = ws.get("client_cpu_pct", {}).get("avg", "N/A")
    acpu = cloudwatch.get("aurora_cpu_avg_pct", "N/A")
    commit_lat = cloudwatch.get("aurora_commit_latency_ms", "N/A")
    dml_lat = cloudwatch.get("aurora_dml_latency_ms", "N/A")

    print(f"\n{'='*60}")
    print(
        f"[{args.window_start}s-{args.window_end}s] "
        f"{args.instance_type} / {args.threads} threads"
    )
    print(f"{'='*60}")
    print(f"  TPS:              {tps_avg}")
    print(f"  P95 latency:      {p95_avg} ms (avg)  [{p95_min} - {p95_max}]")
    print(
        f"  Commit latency:   min={summary.get('latency_min_ms')} "
        f"avg={summary.get('latency_avg_ms')} "
        f"p95={summary.get('latency_95th_pct_ms')} "
        f"max={summary.get('latency_max_ms')}"
    )
    print(f"  Aurora commit:    {commit_lat} ms   DML: {dml_lat} ms")
    print(f"  Total IUD/s:      {iud_avg}")
    ins = ws.get("insert_per_sec", {}).get("avg", "N/A")
    upd = ws.get("update_per_sec", {}).get("avg", "N/A")
    dlt = ws.get("delete_per_sec", {}).get("avg", "N/A")
    rd = ws.get("read_per_sec", {}).get("avg", "N/A")
    print(f"    I/U/D/R:        {ins} / {upd} / {dlt} / {rd}")
    print(f"  Client CPU:       {ccpu}%")
    print(f"  Aurora CPU:       {acpu}%  (max: {cloudwatch.get('aurora_cpu_max_pct', 'N/A')}%)")
    print(f"  Aurora Memory:    {cloudwatch.get('aurora_freeable_memory_mb', 'N/A')} MB free")
    print(f"  Write IOPS:       {cloudwatch.get('aurora_write_iops', 'N/A')}")
    print(f"  Saved: {args.output}")
    print()


if __name__ == "__main__":
    main()
