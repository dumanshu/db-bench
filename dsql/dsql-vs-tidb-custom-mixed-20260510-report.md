# Aurora DSQL vs TiDB - 32 x 500K `custom_mixed` Sysbench Comparison

## TL;DR

Both runs used the same repository workload file, `common/lua/custom_mixed.lua`, with full prepare, 32 tables x 500,000 rows, 128 sysbench threads, 600 measured seconds, 10-second sysbench reporting, explicit warmup, and client resource sampling. TiDB was faster on this run, sustaining **44,156.53 TPS/QPS** versus DSQL's **32,005.38 TPS/QPS** (**+37.96%**), with lower sysbench p99 latency (**7.43 ms** vs **12.30 ms**).

Price-performance depends on billing model. Using CloudWatch DSQL DPU at `$8.00 / 1M DPU`, DSQL's measured active-use DPU cost was **$1.94** (**$0.1009 / 1M queries**). Using the TiDB runner's provisioned EC2/EBS/network run-window total, TiDB cost was **$1.16** (**$0.0439 / 1M queries**); its own framework-reported query price was **$0.0332 / 1M queries**. This is not a pure serverless-vs-serverless billing comparison: DSQL is consumption-priced DPU, while this TiDB cluster is fixed EC2/EBS infrastructure.

| Metric | Aurora DSQL | TiDB | Better |
|---|---:|---:|---|
| TPS / QPS | 32,005.38 | 44,156.53 | TiDB |
| Total queries | 19,203,916 | 26,494,622 | TiDB processed more |
| Sysbench avg latency | 4.00 ms | 2.90 ms | TiDB |
| Sysbench p99 latency | 12.30 ms | 7.43 ms | TiDB |
| Max latency | 420.71 ms | 221.06 ms | TiDB |
| Client CPU avg | 15.43% | 1.70% | TiDB client lower |
| Client RX/TX avg | 10.13 / 6.01 Mbps | 7.66 / 4.20 Mbps | TiDB lower client net |
| Run-window cost | $1.94 DPU-only | $1.16 provisioned infra | TiDB |
| Cost / 1M queries | $0.1009 | $0.0439 | TiDB |

## Run identity

| Field | Aurora DSQL | TiDB |
|---|---|---|
| Result JSON | `dsql/dsql-sysbench-20260508-110556.json` | `tidb/tidb-sysbench-20260510-101100.json` |
| Run log | `logs/dsql-heavy500k-autocommit-fresh-full-20260508-103027.log` | `logs/tidb-heavy500k-custom-mixed-full-20260510-093932.log` |
| Measured phase | 2026-05-08 10:54:49 to 11:04:51 PDT | 2026-05-10 09:58:32 to 10:08:32 PDT |
| Region | `us-east-1` | `us-east-1` |
| Endpoint | `tntx4vlsltctecltgkgbysrpfm.dsql.us-east-1.on.aws` | `10.43.1.199:30400` |
| Database | `postgres` | `sb_5311b95d` |
| Client | `i-013a9803fc14f0adb`, `c7g.2xlarge`, `54.82.85.101` | `i-00063216bed6b8b0b`, `c8g.4xlarge`, `3.81.200.159` |
| Client OS/tools | Amazon Linux 2023, aarch64, sysbench `1.0.20-ebf1c90` | sysbench `1.0.20-ebf1c90` |
| Server shape | Aurora DSQL serverless cluster | 2 TiDB `m8g.4xlarge`, 3 TiKV `m8g.4xlarge`, 3 PD `c8g.2xlarge` |
| Dataset | 32 x 500,000 rows = 16,000,000 rows | 32 x 500,000 rows = 16,000,000 rows |
| Prepare | Full prepare, 21.6 min | Full prepare, 15.6 min |
| Warmup | 30s read-only warmup, 60s cooldown | 30s read-only warmup, 60s cooldown |
| Cleanup | DSQL stack deleted after snapshot | TiDB cluster left running for this comparison |

## Benchmark parity check

Shared and intentionally matched:

- Same workload source: `common/lua/custom_mixed.lua`.
- Same table count and table size: `--tables=32 --table-size=500000`.
- Same measured concurrency and duration: `--threads=128 --time=600`.
- Same sysbench version: `1.0.20-ebf1c90`.
- Same explicit warmup pattern: 30-second read-only pass plus 60-second cooldown before measurement.
- Same autocommit shape: one SQL statement per sysbench event, no explicit `BEGIN` or `COMMIT` in per-template stats.
- Same client-side per-query and per-minute instrumentation: `query_latency_ms`, `op_latency_ms`, `minute_stats`, `interval_data`, and `client_resource_totals`.

Known differences:

- DSQL uses PostgreSQL wire protocol and delete template `DELETE FROM %s WHERE id = %d`; TiDB uses MySQL protocol and delete template `DELETE FROM %s WHERE id = %d LIMIT 1`.
- DSQL cost is CloudWatch DPU consumption. TiDB cost here is provisioned EC2/EBS/network for the fixed test cluster.
- DSQL client was `c7g.2xlarge`; TiDB client was `c8g.4xlarge`. Both were lightly loaded; TiDB client CPU was much lower.
- One TiDB PD node sampler timed out, but the client sampler plus TiDB/TiKV/control and two PD samplers completed. Client-side comparison data is complete.

## Exact commands

Aurora DSQL:

```bash
python3 -m dsql.benchmark \
  --action run \
  --seed dsqllt-001 \
  --host 54.82.85.101 \
  --ssh-key common/dbbench-key.pem \
  --dsql-cluster-id tntx4vlsltctecltgkgbysrpfm \
  --dsql-cluster-endpoint tntx4vlsltctecltgkgbysrpfm.dsql.us-east-1.on.aws \
  --dsql-region us-east-1 \
  --dsql-db-profile sandbox-storage \
  --aws-profile sandbox \
  --workload custom_mixed \
  --tables 32 \
  --table-size 500000 \
  --threads 128 \
  --duration 600 \
  --report-interval 10
```

TiDB:

```bash
python3 -m tidb.benchmark \
  --action run \
  --seed tidblt-001 \
  --aws-profile sandbox \
  --workload custom_mixed \
  --profile heavy \
  --no-disk-fill \
  --report-interval 10 \
  --output none
```

The TiDB path was patched before this run so `--profile heavy --no-disk-fill` used the same custom workload prepare path as DSQL instead of stock `oltp_read_write` prepare or disk-fill row resizing.

## Throughput and latency

| Metric | Aurora DSQL | TiDB |
|---|---:|---:|
| TPS | 32,005.38 | 44,156.53 |
| QPS | 32,005.38 | 44,156.53 |
| Total transactions/events | 19,203,916 | 26,494,622 |
| Total SQL statements | 19,203,916 | 26,494,622 |
| Reads | 87.80% [16,861,690] | 87.80% [23,260,667] |
| Writes | 12.18% [2,339,989] | 12.19% [3,229,187] |
| Other | 0.01% [2,237] | 0.02% [4,768] |
| Sysbench avg latency | 4.00 ms | 2.90 ms |
| Sysbench p99 latency | 12.30 ms | 7.43 ms |
| Sysbench max latency | 420.71 ms | 221.06 ms |
| Errors / ignored / reconnects | 0 / 0 / 0 | 0 / 0 / 0 |

## Per-query latency by template

| Query key | Aurora DSQL count / avg / p99 | TiDB count / avg / p99 | SQL template |
|---|---:|---:|---|
| `select_by_id` | 16,861,690 / 3.013 ms / 8 ms | 23,260,667 / 2.464 ms / 4 ms | `SELECT c FROM %s WHERE id = %d` |
| `insert_row` | 1,593,430 / 11.257 ms / 16 ms | 2,199,733 / 5.782 ms / 16 ms | `INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')` |
| `update_by_id` | 652,960 / 10.622 ms / 16 ms | 901,381 / 6.497 ms / 16 ms | `UPDATE %s SET k = k + 1 WHERE id = %d` |
| `delete_by_id` | 95,836 / 10.047 ms / 16 ms | 0 / n/a / n/a | `DELETE FROM %s WHERE id = %d` |
| `delete_by_id_limit` | 0 / n/a / n/a | 132,841 / 6.460 ms / 16 ms | `DELETE FROM %s WHERE id = %d LIMIT 1` |

## Client VM resource utilization during run

| Metric | Aurora DSQL client | TiDB client |
|---|---:|---:|
| Client instance type | `c7g.2xlarge` | `c8g.4xlarge` |
| Sampler samples / duration | 663 / 662s | 804 / 803s |
| CPU avg / p95 / p99 | 15.43% / 19.70% / 20.40% | 1.70% / 2.90% / 3.30% |
| Memory used avg | 736.27 MB | 965.69 MB |
| Network RX avg / p95 | 10.13 / 11.37 Mbps | 7.66 / 10.35 Mbps |
| Network TX avg / p95 | 6.01 / 6.73 Mbps | 4.20 / 5.66 Mbps |
| Disk write avg / p99 | 0.10 / 4.31 MB/s | 0.11 / 3.17 MB/s |
| Context switches avg / p95 | 105,515.90 / 118,315.00 per sec | 66,625.67 / 89,767.00 per sec |
| Network received / transmitted | 6,708.49 / 3,977.17 MB | 6,152.74 / 3,369.54 MB |

## Price-performance

| Cost basis | Aurora DSQL | TiDB |
|---|---:|---:|
| Billing model used here | CloudWatch `TotalDPU` x `$8.00 / 1M DPU` | Provisioned EC2/EBS/network from runner `CostTracker` |
| Run-window cost | $1.94 | $1.16 |
| Cost / 1M queries | $0.1009 | $0.0439 |
| Framework-reported $/M queries | n/a for DSQL DPU | $0.0332 |
| Monthly cost basis | DPU scales with usage; storage continues after idle | Projected fixed cluster `$3,800.37/mo` |
| Monthly $/QPS | not meaningful for serverless DPU | $0.0861 |

Interpretation: TiDB was both faster and cheaper in this specific fixed-cluster benchmark using provisioned infrastructure cost. DSQL remains consumption-priced and scales to zero for compute when idle, so the comparison is about this heavy 600-second load shape, not idle economics.

## DSQL CloudWatch DPU detail

Collected from `AWS/AuroraDSQL` with dimension `ClusterId=tntx4vlsltctecltgkgbysrpfm` for `2026-05-08T17:54:48+00:00` to `2026-05-08T18:05:50+00:00`.

| Metric | Value | Share |
|---|---:|---:|
| TotalDPU | 242,151.21 | 100.00% |
| WriteDPU | 117,017.27 | 48.32% |
| ReadDPU | 73,143.42 | 30.21% |
| ComputeDPU | 51,990.53 | 21.47% |

## Code links

GitHub repo: https://github.com/dumanshu/db-bench

Branch file links (no PR links):

- Benchmark orchestration: https://github.com/dumanshu/db-bench/blob/w_dgo-dsql-stats-enhancements/common/benchmark.py
- Shared `custom_mixed` workload: https://github.com/dumanshu/db-bench/blob/w_dgo-dsql-stats-enhancements/common/lua/custom_mixed.lua
- DSQL entrypoint: https://github.com/dumanshu/db-bench/blob/w_dgo-dsql-stats-enhancements/dsql/benchmark.py
- TiDB entrypoint: https://github.com/dumanshu/db-bench/blob/w_dgo-dsql-stats-enhancements/tidb/benchmark.py
- TiDB helpers: https://github.com/dumanshu/db-bench/blob/w_dgo-dsql-stats-enhancements/tidb/driver.py

The branch links become representative of the executed code after the local changes are committed and pushed to `w_dgo-dsql-stats-enhancements`.

## Artifacts

- Combined report: `dsql/dsql-vs-tidb-custom-mixed-20260510-report.md`
- Aurora DSQL JSON: `dsql/dsql-sysbench-20260508-110556.json`
- Aurora DSQL log: `logs/dsql-heavy500k-autocommit-fresh-full-20260508-103027.log`
- TiDB JSON: `tidb/tidb-sysbench-20260510-101100.json`
- TiDB log: `logs/tidb-heavy500k-custom-mixed-full-20260510-093932.log`

Raw JSON/log artifacts are local and gitignored. The Markdown report is the shareable artifact.

## Cleanup status

Aurora DSQL resources were cleaned up after capturing metadata and metrics:

- `aws dsql get-cluster` for `tntx4vlsltctecltgkgbysrpfm` returns `ResourceNotFoundException`.
- No active EC2 instances remain with `Project=dsql-loadtest-dsqllt-001`.
- No VPC remains with `Project=dsql-loadtest-dsqllt-001`.
- `dsql/dsql-state.json` was removed by cleanup.

TiDB resources were left running because they are the active comparison cluster.

## Caveats

- DSQL and TiDB use different wire protocols and delete syntax, even though the same workload source controls the mix and templates.
- DSQL client and TiDB client instance types differ. Client CPU was low in both runs, so the client was not saturated.
- One TiDB PD sampler failed to start. Client sampler and core TiDB/TiKV/control samplers completed; client-side comparison is complete.
- TiDB cost here is provisioned EC2/EBS/network, not TiDB Cloud RU billing. DSQL cost here is DPU-only active-use, not a full monthly bill.
- Per-query p50/p95/p99 values are fixed bucket upper bounds from the Lua instrumentation.
