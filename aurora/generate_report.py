#!/usr/bin/env python3
"""Generate comprehensive Aurora benchmark report from JSON results."""
import json, glob, os, sys
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
OUTPUT = os.path.join(SCRIPT_DIR, "BENCHMARK_REPORT.md")

# Pricing data (us-east-1, On-Demand, Aurora MySQL)
PRICING = {
    "db.r6i.16xlarge": {"od": 9.280, "ri1y": 6.080, "vcpu": 64, "mem_gb": 512, "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.24xlarge": {"od": 13.920, "ri1y": 9.120, "vcpu": 96, "mem_gb": 768, "arch": "Intel Ice Lake (x86_64)"},
    "db.r8g.16xlarge": {"od": 8.832, "ri1y": 5.917, "vcpu": 64, "mem_gb": 512, "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.24xlarge": {"od": 13.248, "ri1y": 8.876, "vcpu": 96, "mem_gb": 768, "arch": "AWS Graviton4 (ARM)"},
}

# Instance display order: pair by size (16xl intel, 16xl graviton, 24xl intel, 24xl graviton)
INSTANCE_ORDER = ["db.r6i.16xlarge", "db.r8g.16xlarge", "db.r6i.24xlarge", "db.r8g.24xlarge"]

# Thread counts per instance size
THREAD_COUNTS = {
    "16xlarge": [128, 256, 512],
    "24xlarge": [256, 512, 1024],
}

def target_label(inst, threads):
    return str(threads)

def load_results():
    runs = []
    for f in glob.glob(os.path.join(RESULTS_DIR, "final_*.json")):
        with open(f) as fh:
            runs.append(json.load(fh))
    order_map = {v: i for i, v in enumerate(INSTANCE_ORDER)}
    runs.sort(key=lambda r: (order_map.get(r["instance_type"], 99), r["threads"]))
    return runs

def fmt(v, decimals=2):
    if v is None: return "N/A"
    if isinstance(v, str): return v
    if abs(v) >= 1000: return f"{v:,.{decimals}f}"
    return f"{v:.{decimals}f}"

def fmti(v):
    if v is None: return "N/A"
    return f"{int(round(v)):,}"

def pct_diff(new, old):
    if old is None or old == 0 or new is None: return "N/A"
    return f"{((new - old) / old) * 100:+.1f}%"

runs = load_results()
lines = []
def w(s=""): lines.append(s)


w("# Aurora MySQL Benchmark Report")
w()
w("**Aurora Engine:** 3.10.3 (MySQL 8.0.42), IO-Optimized  ")
w("**Region:** us-east-1  ")
w("**Workload:** `custom_mixed` (87.8% read, 8.3% insert, 3.4% update, 0.5% delete per sysbench transaction)  ")
w()

best_per_instance = {}
for r in runs:
    inst = r["instance_type"]
    iud = r["iud_rates"]["window_avg"]["total_iud_per_sec"]
    if inst not in best_per_instance or iud > best_per_instance[inst]["iud_rates"]["window_avg"]["total_iud_per_sec"]:
        best_per_instance[inst] = r

w("---")
w()
w("## Executive Summary")
w()
w("### Peak IUD Rate + Commit Latency per Instance")
w()
w("| Instance | Architecture | vCPU | Peak IUD/s | Commits/s | Tx Lat avg | Tx Lat P95 | Aurora CPU % | Threads |")
w("|----------|-------------|------|-----------|-----------|------------|------------|--------------|---------|")
for inst in INSTANCE_ORDER:
    if inst not in best_per_instance:
        continue
    r = best_per_instance[inst]
    p = PRICING[inst]
    iud = r["iud_rates"]["window_avg"]
    cl = r["commit_latency"]["full_run"]
    cpu = r["resource_utilization"].get("aurora_cpu_avg_pct")
    cpu_str = f"{fmt(cpu, 1)}%" if cpu else "N/A"
    w(f"| {inst} | {p['arch']} | {p['vcpu']} | {fmti(iud['total_iud_per_sec'])} | {fmti(iud['commits_per_sec'])} | {fmt(cl['avg_ms'])} ms | {fmt(cl['p95_ms'])} ms | {cpu_str} | {r['threads']} |")
w()

w("---")
w()
w("## All Runs: IUD Rate, Commit Latency, and Resource Utilization")
w()
w("| Instance | Thr | IUD/s | Commits/s | Tx Lat avg (ms) | Tx Lat P95 (ms) | Win P95 avg (ms) | Aurora CPU % | Client CPU % |")
w("|----------|-----|-------|-----------|-----------------|-----------------|------------------|--------------|--------------|")
for r in runs:
    inst = r["instance_type"]
    iud = r["iud_rates"]["window_avg"]
    cl = r["commit_latency"]["full_run"]
    wp = r["commit_latency"]["window_p95_stats"]
    ru = r["resource_utilization"]
    cpu_a = ru.get("aurora_cpu_avg_pct")
    cpu_c = ru.get("client_cpu", {}).get("avg")
    w(f"| {inst} | {r['threads']} | {fmti(iud['total_iud_per_sec'])} | {fmti(iud['commits_per_sec'])} | {fmt(cl['avg_ms'])} | {fmt(cl['p95_ms'])} | {fmt(wp['avg'])} | {fmt(cpu_a,1) if cpu_a else 'N/A'}{'%' if cpu_a else ''} | {fmt(cpu_c,2) if cpu_c else 'N/A'}{'%' if cpu_c else ''} |")
w()

w("---")
w()
w("## Detailed IUD Rate Breakdown")
w()
w("| Instance | Thr | Insert/s | Update/s | Delete/s | Read/s | Total IUD/s | Commits/s |")
w("|----------|-----|----------|----------|----------|--------|-------------|-----------|")
for r in runs:
    inst = r["instance_type"]
    iud = r["iud_rates"]["window_avg"]
    w(f"| {inst} | {r['threads']} | {fmti(iud['insert_per_sec'])} | {fmti(iud['update_per_sec'])} | {fmti(iud['delete_per_sec'])} | {fmti(iud['read_per_sec'])} | {fmti(iud['total_iud_per_sec'])} | {fmti(iud['commits_per_sec'])} |")
w()

w("---")
w()
w("## Instance Specifications and Pricing")
w()
w("| Instance | Architecture | vCPU | Memory | On-Demand $/hr | 1yr RI $/hr | RI Savings |")
w("|----------|-------------|------|--------|---------------|-------------|------------|")
for inst in INSTANCE_ORDER:
    p = PRICING[inst]
    sav = ((p["od"] - p["ri1y"]) / p["od"]) * 100
    w(f"| {inst} | {p['arch']} | {p['vcpu']} | {p['mem_gb']} GB | ${p['od']:.3f} | ${p['ri1y']:.3f} | {sav:.0f}% |")
w()

run_count = len(runs)
inst_count = len(set(r["instance_type"] for r in runs))
w(f"*Report generated by `generate_report.py`. Data from {run_count} benchmark runs across {inst_count} Aurora MySQL instance types.*")

with open(OUTPUT, "w") as f:
    f.write("\n".join(lines) + "\n")

print(f"Report written to {OUTPUT}")
print(f"Total lines: {len(lines)}")
