"""Unified benchmark report generator for Aurora and TiDB."""
import json
import time
from datetime import datetime
from pathlib import Path

from common.util import log

INSTANCE_PRICING = {
    # Aurora MySQL (us-east-1, On-Demand)
    "db.r6i.large":     {"hourly": 0.580,  "vcpu": 2,   "mem_gb": 16,   "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.xlarge":    {"hourly": 1.160,  "vcpu": 4,   "mem_gb": 32,   "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.2xlarge":   {"hourly": 2.320,  "vcpu": 8,   "mem_gb": 64,   "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.4xlarge":   {"hourly": 4.640,  "vcpu": 16,  "mem_gb": 128,  "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.8xlarge":   {"hourly": 6.960,  "vcpu": 32,  "mem_gb": 256,  "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.16xlarge":  {"hourly": 9.280,  "vcpu": 64,  "mem_gb": 512,  "arch": "Intel Ice Lake (x86_64)"},
    "db.r6i.24xlarge":  {"hourly": 13.920, "vcpu": 96,  "mem_gb": 768,  "arch": "Intel Ice Lake (x86_64)"},
    "db.r6g.large":     {"hourly": 0.522,  "vcpu": 2,   "mem_gb": 16,   "arch": "AWS Graviton2 (ARM)"},
    "db.r6g.xlarge":    {"hourly": 1.044,  "vcpu": 4,   "mem_gb": 32,   "arch": "AWS Graviton2 (ARM)"},
    "db.r6g.2xlarge":   {"hourly": 2.088,  "vcpu": 8,   "mem_gb": 64,   "arch": "AWS Graviton2 (ARM)"},
    "db.r6g.4xlarge":   {"hourly": 4.176,  "vcpu": 16,  "mem_gb": 128,  "arch": "AWS Graviton2 (ARM)"},
    "db.r6g.8xlarge":   {"hourly": 6.264,  "vcpu": 32,  "mem_gb": 256,  "arch": "AWS Graviton2 (ARM)"},
    "db.r6g.16xlarge":  {"hourly": 8.352,  "vcpu": 64,  "mem_gb": 512,  "arch": "AWS Graviton2 (ARM)"},
    "db.r7g.large":     {"hourly": 0.549,  "vcpu": 2,   "mem_gb": 16,   "arch": "AWS Graviton3 (ARM)"},
    "db.r7g.xlarge":    {"hourly": 1.098,  "vcpu": 4,   "mem_gb": 32,   "arch": "AWS Graviton3 (ARM)"},
    "db.r7g.2xlarge":   {"hourly": 2.196,  "vcpu": 8,   "mem_gb": 64,   "arch": "AWS Graviton3 (ARM)"},
    "db.r7g.4xlarge":   {"hourly": 4.392,  "vcpu": 16,  "mem_gb": 128,  "arch": "AWS Graviton3 (ARM)"},
    "db.r7g.8xlarge":   {"hourly": 6.588,  "vcpu": 32,  "mem_gb": 256,  "arch": "AWS Graviton3 (ARM)"},
    "db.r7g.16xlarge":  {"hourly": 8.784,  "vcpu": 64,  "mem_gb": 512,  "arch": "AWS Graviton3 (ARM)"},
    "db.r7i.large":     {"hourly": 0.610,  "vcpu": 2,   "mem_gb": 16,   "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.xlarge":    {"hourly": 1.220,  "vcpu": 4,   "mem_gb": 32,   "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.2xlarge":   {"hourly": 2.440,  "vcpu": 8,   "mem_gb": 64,   "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.4xlarge":   {"hourly": 4.880,  "vcpu": 16,  "mem_gb": 128,  "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.8xlarge":   {"hourly": 7.320,  "vcpu": 32,  "mem_gb": 256,  "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.16xlarge":  {"hourly": 9.760,  "vcpu": 64,  "mem_gb": 512,  "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r7i.24xlarge":  {"hourly": 14.640, "vcpu": 96,  "mem_gb": 768,  "arch": "Intel Sapphire Rapids (x86_64)"},
    "db.r8g.large":     {"hourly": 0.552,  "vcpu": 2,   "mem_gb": 16,   "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.xlarge":    {"hourly": 1.104,  "vcpu": 4,   "mem_gb": 32,   "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.2xlarge":   {"hourly": 2.208,  "vcpu": 8,   "mem_gb": 64,   "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.4xlarge":   {"hourly": 4.416,  "vcpu": 16,  "mem_gb": 128,  "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.8xlarge":   {"hourly": 6.624,  "vcpu": 32,  "mem_gb": 256,  "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.16xlarge":  {"hourly": 8.832,  "vcpu": 64,  "mem_gb": 512,  "arch": "AWS Graviton4 (ARM)"},
    "db.r8g.24xlarge":  {"hourly": 13.248, "vcpu": 96,  "mem_gb": 768,  "arch": "AWS Graviton4 (ARM)"},
    # TiDB EC2 (us-east-1, On-Demand)
    "c7g.xlarge":   {"hourly": 0.145,  "vcpu": 4,   "mem_gb": 8,    "arch": "AWS Graviton3 (ARM)"},
    "c7g.2xlarge":  {"hourly": 0.290,  "vcpu": 8,   "mem_gb": 16,   "arch": "AWS Graviton3 (ARM)"},
    "c7g.4xlarge":  {"hourly": 0.580,  "vcpu": 16,  "mem_gb": 32,   "arch": "AWS Graviton3 (ARM)"},
    "c7g.8xlarge":  {"hourly": 1.160,  "vcpu": 32,  "mem_gb": 64,   "arch": "AWS Graviton3 (ARM)"},
    "c7g.16xlarge": {"hourly": 2.320,  "vcpu": 64,  "mem_gb": 128,  "arch": "AWS Graviton3 (ARM)"},
    "c8g.xlarge":   {"hourly": 0.153,  "vcpu": 4,   "mem_gb": 8,    "arch": "AWS Graviton4 (ARM)"},
    "c8g.2xlarge":  {"hourly": 0.306,  "vcpu": 8,   "mem_gb": 16,   "arch": "AWS Graviton4 (ARM)"},
    "c8g.4xlarge":  {"hourly": 0.612,  "vcpu": 16,  "mem_gb": 32,   "arch": "AWS Graviton4 (ARM)"},
    "c8g.8xlarge":  {"hourly": 1.224,  "vcpu": 32,  "mem_gb": 64,   "arch": "AWS Graviton4 (ARM)"},
    "c8g.16xlarge": {"hourly": 2.448,  "vcpu": 64,  "mem_gb": 128,  "arch": "AWS Graviton4 (ARM)"},
}

EBS_COSTS = {
    "gp3_per_gb_month": 0.08,
    "gp3_iops_over_3000": 0.005,
    "gp3_throughput_over_125": 0.04,
}

NETWORK_COSTS = {
    "cross_az_per_gb": 0.01,
    "same_az_per_gb": 0.00,
}


def _fmt(v, decimals=2):
    if v is None:
        return "N/A"
    if isinstance(v, str):
        return v
    if abs(v) >= 1000:
        return f"{v:,.{decimals}f}"
    return f"{v:.{decimals}f}"


def _fmti(v):
    if v is None:
        return "N/A"
    return f"{int(round(v)):,}"


def _pct_diff(new, old):
    if old is None or old == 0 or new is None:
        return "N/A"
    return f"{((new - old) / old) * 100:+.1f}%"


def _load_results(results_dir: Path) -> list:
    results_dir = Path(results_dir)
    runs = []
    for f in sorted(results_dir.glob("*.json")):
        with f.open() as fh:
            try:
                runs.append(json.load(fh))
            except json.JSONDecodeError:
                log(f"WARNING: skipping malformed JSON: {f}")
    return runs


def _section_executive_summary(lines, runs, server_type):
    if not runs:
        return
    first = runs[0]
    instance_type = first.get("instance_type", "unknown")
    timestamp = first.get("timestamp", datetime.now().isoformat())
    test_date = timestamp[:10] if isinstance(timestamp, str) else datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

    lines.append(f"## Executive Summary")
    lines.append("")
    lines.append(f"| Property | Value |")
    lines.append(f"|----------|-------|")
    lines.append(f"| Server Type | {server_type} |")
    lines.append(f"| Instance Type | {instance_type} |")
    lines.append(f"| Test Date | {test_date} |")
    lines.append(f"| Total Runs | {len(runs)} |")

    instance_types = sorted(set(r.get("instance_type", "unknown") for r in runs))
    if len(instance_types) > 1:
        lines.append(f"| Instance Types Tested | {', '.join(instance_types)} |")

    lines.append("")


def _section_all_runs(lines, runs):
    if not runs:
        return

    lines.append("## All Runs")
    lines.append("")
    lines.append("| Workload | Instance | Threads | Duration (s) | TPS | QPS | P95 (ms) | P99 (ms) |")
    lines.append("|----------|----------|--------:|-------------:|----:|----:|---------:|---------:|")

    for r in runs:
        workload = r.get("workload", "unknown")
        instance = r.get("instance_type", "")
        threads = r.get("threads", "")
        duration = r.get("duration", "")

        tp = r.get("throughput", {})
        tps = tp.get("tps", r.get("tps", None))
        qps = tp.get("qps", r.get("qps", None))

        lat = r.get("latency", {})
        p95 = lat.get("p95_ms", r.get("latency_p95_ms", None))
        p99 = lat.get("p99_ms", r.get("latency_p99_ms", None))

        lines.append(
            f"| {workload} | {instance} | {threads} | {duration} "
            f"| {_fmt(tps)} | {_fmt(qps)} | {_fmt(p95)} | {_fmt(p99)} |"
        )
    lines.append("")


def _section_iud_breakdown(lines, runs):
    iud_runs = [r for r in runs if r.get("iud_rates")]
    if not iud_runs:
        return

    lines.append("## IUD Rate Breakdown")
    lines.append("")
    lines.append("| Instance | Threads | Insert/s | Update/s | Delete/s | Read/s | Total IUD/s | Commits/s |")
    lines.append("|----------|--------:|---------:|---------:|---------:|-------:|------------:|----------:|")

    for r in iud_runs:
        instance = r.get("instance_type", "")
        threads = r.get("threads", "")
        iud = r.get("iud_rates", {}).get("window_avg", r.get("iud_rates", {}))
        lines.append(
            f"| {instance} | {threads} "
            f"| {_fmti(iud.get('insert_per_sec'))} "
            f"| {_fmti(iud.get('update_per_sec'))} "
            f"| {_fmti(iud.get('delete_per_sec'))} "
            f"| {_fmti(iud.get('read_per_sec'))} "
            f"| {_fmti(iud.get('total_iud_per_sec'))} "
            f"| {_fmti(iud.get('commits_per_sec'))} |"
        )
    lines.append("")


def _section_instance_specs(lines, runs):
    instance_types = sorted(set(r.get("instance_type", "") for r in runs))
    known = [it for it in instance_types if it in INSTANCE_PRICING]
    if not known:
        return

    lines.append("## Instance Specifications and Pricing")
    lines.append("")
    lines.append("| Instance | Architecture | vCPU | Memory | On-Demand $/hr |")
    lines.append("|----------|-------------|-----:|-------:|---------------:|")

    for inst in known:
        p = INSTANCE_PRICING[inst]
        lines.append(
            f"| {inst} | {p['arch']} | {p['vcpu']} | {p['mem_gb']} GB | ${p['hourly']:.3f} |"
        )
    lines.append("")


def _section_cost_estimate(lines, runs, server_type):
    if not runs:
        return
    total_duration = sum(r.get("duration", 0) for r in runs)
    if total_duration <= 0:
        return

    instance_types = set(r.get("instance_type", "") for r in runs)
    known = [it for it in instance_types if it in INSTANCE_PRICING]
    if not known:
        return

    primary_type = known[0]
    hourly = INSTANCE_PRICING[primary_type]["hourly"]
    hours = total_duration / 3600
    bench_cost = hourly * hours
    monthly_730 = hourly * 730

    lines.append("## Cost Estimate")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Instance | {primary_type} |")
    lines.append(f"| Hourly Rate | ${hourly:.3f} |")
    lines.append(f"| Benchmark Duration | {total_duration:,.0f}s ({hours:.3f} hrs) |")
    lines.append(f"| Benchmark Cost | ${bench_cost:.4f} |")
    lines.append(f"| Projected Monthly (730 hrs) | ${monthly_730:,.2f} |")
    lines.append("")


def generate_report(results_dir, server_type, output_path=None) -> str:
    results_dir = Path(results_dir)
    runs = _load_results(results_dir)

    if not runs:
        log(f"No JSON results found in {results_dir}")
        return ""

    order_map = {it: i for i, it in enumerate(sorted(set(
        r.get("instance_type", "") for r in runs
    )))}
    runs.sort(key=lambda r: (
        order_map.get(r.get("instance_type", ""), 99),
        r.get("threads", 0),
    ))

    lines = []
    title = f"Aurora MySQL" if server_type == "aurora" else f"TiDB"
    lines.append(f"# {title} Benchmark Report")
    lines.append("")
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")
    lines.append("---")
    lines.append("")

    _section_executive_summary(lines, runs, server_type)
    lines.append("---")
    lines.append("")
    _section_all_runs(lines, runs)
    lines.append("---")
    lines.append("")
    _section_iud_breakdown(lines, runs)
    _section_instance_specs(lines, runs)
    lines.append("---")
    lines.append("")
    _section_cost_estimate(lines, runs, server_type)

    run_count = len(runs)
    inst_count = len(set(r.get("instance_type", "") for r in runs))
    lines.append(
        f"*Report from {run_count} benchmark run(s) "
        f"across {inst_count} instance type(s).*"
    )

    md = "\n".join(lines) + "\n"

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(md)
        log(f"Report written to {output_path}")

    return md


class CostTracker:
    """Track AWS costs during benchmark runs."""

    def __init__(self, server_type, instance_type, region="us-east-1"):
        self.server_type = server_type
        self.instance_type = instance_type
        self.region = region
        self._start = None
        self._stop = None
        self.server_count = 1
        self.ebs_gb = 0
        self.metrics = {
            "total_queries": 0,
            "total_transactions": 0,
            "total_bytes_transferred": 0,
            "avg_qps": 0.0,
            "avg_tps": 0.0,
        }

    def start(self):
        self._start = time.time()

    def stop(self):
        self._stop = time.time()

    def elapsed_hours(self) -> float:
        end = self._stop or time.time()
        start = self._start or end
        return max(0.0, (end - start) / 3600)

    def estimated_cost(self) -> float:
        pricing = INSTANCE_PRICING.get(self.instance_type, {})
        hourly = pricing.get("hourly", 0.0)
        hours = self.elapsed_hours()
        compute = hourly * hours * self.server_count
        ebs = 0.0
        if self.ebs_gb > 0:
            monthly_ebs = self.ebs_gb * self.server_count * EBS_COSTS["gp3_per_gb_month"]
            ebs = monthly_ebs * (hours / 720)
        return compute + ebs

    def set_infrastructure(self, server_count=1, ebs_gb=0):
        self.server_count = server_count
        self.ebs_gb = ebs_gb

    def add_queries(self, query_count=0, transaction_count=0,
                    avg_qps=0.0, avg_tps=0.0, avg_row_size_bytes=200):
        self.metrics["total_queries"] += query_count
        self.metrics["total_transactions"] += transaction_count
        if avg_qps > 0:
            self.metrics["avg_qps"] = avg_qps
        if avg_tps > 0:
            self.metrics["avg_tps"] = avg_tps
        self.metrics["total_bytes_transferred"] += query_count * avg_row_size_bytes

    def get_summary(self) -> dict:
        hours = self.elapsed_hours()
        seconds = hours * 3600
        pricing = INSTANCE_PRICING.get(self.instance_type, {})
        hourly = pricing.get("hourly", 0.0)

        compute = hourly * hours * self.server_count
        ebs = 0.0
        if self.ebs_gb > 0:
            monthly_ebs = self.ebs_gb * self.server_count * EBS_COSTS["gp3_per_gb_month"]
            ebs = monthly_ebs * (hours / 720)

        bytes_gb = self.metrics["total_bytes_transferred"] / (1024 ** 3)
        network = bytes_gb * 0.5 * NETWORK_COSTS["cross_az_per_gb"] * 2

        total = compute + ebs + network
        hourly_rate = total / hours if hours > 0 else 0.0
        monthly_730 = hourly_rate * 730

        return {
            "duration_seconds": seconds,
            "duration_hours": hours,
            "server_type": self.server_type,
            "instance_type": self.instance_type,
            "server_count": self.server_count,
            "compute_cost": compute,
            "storage_cost": ebs,
            "network_cost": network,
            "total_cost": total,
            "hourly_rate": hourly_rate,
            "monthly_projected": monthly_730,
            "metrics": dict(self.metrics),
        }

    def print_cost_summary(self):
        s = self.get_summary()
        log("")
        log("=" * 70)
        log("COST SUMMARY")
        log("=" * 70)
        log(f"Duration: {s['duration_seconds']:.0f}s ({s['duration_hours']:.3f} hours)")
        log(f"Instance: {s['instance_type']} x{s['server_count']}")
        log("")
        log(f"  Compute:  ${s['compute_cost']:.4f}")
        log(f"  Storage:  ${s['storage_cost']:.4f}")
        log(f"  Network:  ${s['network_cost']:.4f}")
        log(f"  TOTAL:    ${s['total_cost']:.4f}")
        log("")
        log(f"Effective hourly rate: ${s['hourly_rate']:.2f}/hr")
        log(f"Projected monthly (730 hrs): ${s['monthly_projected']:,.2f}")

        avg_qps = self.metrics.get("avg_qps", 0)
        avg_tps = self.metrics.get("avg_tps", 0)
        if avg_qps > 0 or avg_tps > 0:
            log("")
            log("--- Price-Performance ---")
            if avg_qps > 0:
                cost_per_m = (s["monthly_projected"] / avg_qps / 3600 / 720) * 1_000_000
                log(f"  QPS: {avg_qps:,.1f}  |  $/M queries: ${cost_per_m:.4f}  |  Monthly $/QPS: ${s['monthly_projected'] / avg_qps:.4f}")
            if avg_tps > 0:
                cost_per_m = (s["monthly_projected"] / avg_tps / 3600 / 720) * 1_000_000
                log(f"  TPS: {avg_tps:,.1f}  |  $/M txns: ${cost_per_m:.4f}  |  Monthly $/TPS: ${s['monthly_projected'] / avg_tps:.4f}")
        log("=" * 70)


def print_summary(results, server_type) -> None:
    if isinstance(results, dict):
        results = [results]
    if not results:
        return

    log("")
    log("=" * 100)
    log(f"BENCHMARK SUMMARY ({server_type.upper()})")
    log("=" * 100)
    log(f"{'Workload':<22} {'Inst':<22} {'Thr':>5} {'Dur':>5} {'TPS':>10} {'QPS':>10} {'P95':>8} {'P99':>8}")
    log("-" * 100)

    for r in results:
        workload = r.get("workload", r.get("phase", "unknown"))
        instance = r.get("instance_type", "")
        threads = r.get("threads", 0)
        duration = r.get("duration", 0)

        tp = r.get("throughput", {})
        tps = tp.get("tps", r.get("tps", 0))
        qps = tp.get("qps", r.get("qps", 0))

        lat = r.get("latency", {})
        p95 = lat.get("p95_ms", r.get("latency_p95_ms", 0))
        p99 = lat.get("p99_ms", r.get("latency_p99_ms", 0))

        p95_s = f"{p95:.1f}" if isinstance(p95, (int, float)) and p95 > 0 else "N/A"
        p99_s = f"{p99:.1f}" if isinstance(p99, (int, float)) and p99 > 0 else "N/A"

        log(f"{workload:<22} {instance:<22} {threads:>5} {duration:>5} {tps:>10,.1f} {qps:>10,.1f} {p95_s:>8} {p99_s:>8}")

    log("-" * 100)

    total_dur = sum(r.get("duration", 0) for r in results)
    weighted_tps = sum(
        r.get("throughput", {}).get("tps", r.get("tps", 0)) * r.get("duration", 0)
        for r in results
    )
    avg_tps = weighted_tps / total_dur if total_dur > 0 else 0
    peak_tps = max(
        (r.get("throughput", {}).get("tps", r.get("tps", 0)) for r in results),
        default=0,
    )
    log(f"Duration: {total_dur}s | Avg TPS: {avg_tps:,.1f} | Peak TPS: {peak_tps:,.1f}")
    log("=" * 100)


def save_results(results, output_dir, label) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{label}.json"
    with path.open("w") as fh:
        json.dump(results, fh, indent=2, default=str)
    log(f"Results saved to {path}")
    return path
