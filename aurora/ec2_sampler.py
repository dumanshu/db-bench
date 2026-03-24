#!/usr/bin/env python3
"""Metrics sampler for EC2 client during Aurora benchmarks.

Runs on the EC2 client instance. Samples every INTERVAL seconds:
- Client CPU (cumulative jiffies from /proc/stat -- compute deltas in post-processing)
- Client memory (from /proc/meminfo)
- InnoDB row counters (cumulative -- compute deltas in post-processing)

Writes CSV to the specified output file. Runs until /tmp/.metrics_running is removed.

Usage: python3 ec2_sampler.py <mysql_endpoint> <password> <output_csv> [interval_s]
"""

import csv
import os
import subprocess
import sys
import time


def read_cpu_jiffies():
    """Read cumulative CPU jiffies from /proc/stat (all CPUs aggregate)."""
    with open("/proc/stat") as f:
        parts = f.readline().split()
    # Fields: cpu user nice system idle iowait irq softirq steal [guest guest_nice]
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


def read_memory_mb():
    """Read memory from /proc/meminfo. Returns (used_mb, total_mb)."""
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split()
            key = parts[0].rstrip(":")
            if key in ("MemTotal", "MemAvailable"):
                info[key] = int(parts[1])  # kB
    total = info.get("MemTotal", 0) // 1024
    avail = info.get("MemAvailable", 0) // 1024
    return total - avail, total


def read_innodb_counters(endpoint, password):
    """Query cumulative InnoDB row counters via MySQL CLI."""
    try:
        r = subprocess.run(
            [
                "mysql",
                f"-h{endpoint}",
                "-P3306",
                "-uadmin",
                f"-p{password}",
                "--batch",
                "--skip-column-names",
                "-e",
                "SHOW GLOBAL STATUS WHERE Variable_name IN "
                "('Innodb_rows_inserted','Innodb_rows_updated',"
                "'Innodb_rows_deleted','Innodb_rows_read',"
                "'Com_commit','Com_rollback')",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        counters = {}
        for line in r.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 2:
                counters[parts[0]] = int(parts[1])
        return counters
    except Exception as e:
        print(f"WARNING: InnoDB query failed: {e}", file=sys.stderr)
        return {}


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <endpoint> <password> <output_csv> [interval_s]")
        sys.exit(1)

    endpoint = sys.argv[1]
    password = sys.argv[2]
    outfile = sys.argv[3]
    interval = int(sys.argv[4]) if len(sys.argv) > 4 else 5

    fieldnames = [
        "epoch",
        "cpu_user",
        "cpu_nice",
        "cpu_system",
        "cpu_idle",
        "cpu_iowait",
        "cpu_irq",
        "cpu_softirq",
        "cpu_steal",
        "mem_used_mb",
        "mem_total_mb",
        "innodb_inserted",
        "innodb_updated",
        "innodb_deleted",
        "innodb_read",
        "com_commit",
        "com_rollback",
    ]

    sentinel = "/tmp/.metrics_running"
    with open(outfile, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        next_sample = time.time()
        sample_count = 0
        while os.path.exists(sentinel):
            epoch = int(time.time())
            cpu = read_cpu_jiffies()
            mem_used, mem_total = read_memory_mb()
            innodb = read_innodb_counters(endpoint, password)

            w.writerow(
                {
                    "epoch": epoch,
                    "cpu_user": cpu["user"],
                    "cpu_nice": cpu["nice"],
                    "cpu_system": cpu["system"],
                    "cpu_idle": cpu["idle"],
                    "cpu_iowait": cpu["iowait"],
                    "cpu_irq": cpu["irq"],
                    "cpu_softirq": cpu["softirq"],
                    "cpu_steal": cpu["steal"],
                    "mem_used_mb": mem_used,
                    "mem_total_mb": mem_total,
                    "innodb_inserted": innodb.get("Innodb_rows_inserted", ""),
                    "innodb_updated": innodb.get("Innodb_rows_updated", ""),
                    "innodb_deleted": innodb.get("Innodb_rows_deleted", ""),
                    "innodb_read": innodb.get("Innodb_rows_read", ""),
                    "com_commit": innodb.get("Com_commit", ""),
                    "com_rollback": innodb.get("Com_rollback", ""),
                }
            )
            f.flush()
            sample_count += 1

            next_sample += interval
            sleep_time = next_sample - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    print(f"Sampler stopped after {sample_count} samples. Output: {outfile}")


if __name__ == "__main__":
    main()
