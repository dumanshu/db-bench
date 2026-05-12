# Aurora DSQL vs TiDB - 32 x 500K `custom_mixed` Sysbench Comparison

## TL;DR

This run compares Aurora DSQL and TiDB with the same committed sysbench workload, same client VM, same dataset size, same measured duration, and same explicit warmup pattern. Both runs used `common/lua/custom_mixed.lua` from commit `650a97e20f43c53c588255377ead3393664bffba`, 32 tables x 500,000 rows, 128 sysbench threads, 600 measured seconds, 10-second sysbench reporting, a 30-second read-only warmup, and a 60-second cooldown before measurement.

TiDB sustained **39,282.12 TPS/QPS** over the measured window, versus Aurora DSQL at **27,730.50 TPS/QPS**. TiDB also had lower sysbench latency: **3.26 ms average / 9.91 ms p99**, versus DSQL at **4.62 ms average / 14.46 ms p99**. Both runs used the same `c8g.4xlarge` benchmark client (`34.207.176.175`, 16 vCPU / 32 GiB), and neither run had ignored errors or reconnects.

| Summary metric | Aurora DSQL | TiDB |
|---|---:|---:|
| Measured window (UTC) | 2026-05-11 22:54:46 to 23:05:56 | 2026-05-12 00:25:42 to 00:35:50 |
| TPS / QPS | 27,730.50 | 39,282.12 |
| Total SQL statements | 16,638,851 | 23,569,915 |
| Sysbench avg latency | 4.62 ms | 3.26 ms |
| Sysbench p99 latency | 14.46 ms | 9.91 ms |
| Max latency | 961.02 ms | 225.51 ms |
| Client CPU avg / p95 | 3.51% / 4.80% | 2.44% / 3.60% |
| Client RX/TX total | 5,670.34 / 3,297.60 MB | 5,473.60 / 2,997.58 MB |

## Test specification

| Field | Aurora DSQL | TiDB |
|---|---|---|
| Region | `us-east-1` | `us-east-1` |
| Endpoint | `qftymetrvifxmplzfay5kxf3bu.dsql.us-east-1.on.aws` | `10.43.1.47:30400` |
| Database | `postgres` | `sb_5311b95d` |
| Server shape | Aurora DSQL serverless cluster | 2 TiDB `m8g.4xlarge`, 3 TiKV `m8g.4xlarge`, 3 PD `c8g.2xlarge` |
| Client VM | `c8g.4xlarge`, 16 vCPU, 32 GiB, AL2023 ARM64 | Same client VM: `c8g.4xlarge`, 16 vCPU, 32 GiB, AL2023 ARM64 |
| Client IP | `34.207.176.175` | `34.207.176.175` |
| sysbench | `1.0.20-ebf1c90` | `1.0.20-ebf1c90` |
| Workload | `custom_mixed` | `custom_mixed` |
| Dataset | 32 x 500,000 rows = 16,000,000 rows | 32 x 500,000 rows = 16,000,000 rows |
| Prepare | Full prepare, then row-count verified before measured rerun | Full prepare, then row-count verified before measured rerun |
| Warmup | 30s read-only pass + 60s cooldown | 30s read-only pass + 60s cooldown |
| Measurement | 128 threads, 600s, 10s report interval | 128 threads, 600s, 10s report interval |

## Code links

Executed source commit: `650a97e20f43c53c588255377ead3393664bffba`

- Shared workload: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/common/lua/custom_mixed.lua
- Benchmark orchestration: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/common/benchmark.py
- Shared client provisioning: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/common/client.py
- DSQL entrypoint: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/dsql/benchmark.py
- TiDB entrypoint: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/tidb/benchmark.py
- TiDB setup/helper code: https://github.com/dumanshu/db-bench/blob/650a97e20f43c53c588255377ead3393664bffba/tidb/driver.py

## Benchmark parity

The workload source, table count, table size, measured duration, thread count, report interval, warmup/cooldown shape, and client VM are intentionally matched. The per-event workload is one autocommit SQL statement selected by the same Lua logic: point select, insert, update-by-id, or delete-by-id.

Protocol-specific differences are limited to the database driver and delete syntax:

- DSQL uses PostgreSQL wire protocol and `DELETE FROM %s WHERE id = %d`.
- TiDB uses MySQL protocol and `DELETE FROM %s WHERE id = %d LIMIT 1`.

Both result sets include per-template latency data and contain no `BEGIN` or `COMMIT` query keys.

## Throughput and latency

| Metric | Aurora DSQL (2026-05-11 22:54:46 to 23:05:56 UTC) | TiDB (2026-05-12 00:25:42 to 00:35:50 UTC) |
|---|---:|---:|
| TPS | 27,730.50 | 39,282.12 |
| QPS | 27,730.50 | 39,282.12 |
| Total transactions/events | 16,638,851 | 23,569,915 |
| Total SQL statements | 16,638,851 | 23,569,915 |
| Reads | 87.81% [14,610,317] | 87.81% [20,695,694] |
| Writes | 12.18% [2,026,885] | 12.18% [2,869,941] |
| Other | 0.01% [1,649] | 0.02% [4,280] |
| Sysbench avg latency | 4.62 ms | 3.26 ms |
| Sysbench p99 latency | 14.46 ms | 9.91 ms |
| Sysbench max latency | 961.02 ms | 225.51 ms |
| Ignored errors | 0 | 0 |
| Reconnects | 0 | 0 |

## Per-query latency by template

Per-query p99 values are bucket upper bounds from the shared Lua client-side latency buckets.

| Query key | Aurora DSQL count / avg / p99 | TiDB count / avg / p99 | SQL template |
|---|---:|---:|---|
| `select_by_id` | 14,610,317 / 3.402 ms / 8 ms | 20,695,694 / 2.610 ms / 4 ms | `SELECT c FROM %s WHERE id = %d` |
| `insert_row` | 1,380,140 / 13.550 ms / 32 ms | 1,954,905 / 7.663 ms / 16 ms | `INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')` |
| `update_by_id` | 565,081 / 12.949 ms / 16 ms | 801,539 / 8.449 ms / 16 ms | `UPDATE %s SET k = k + 1 WHERE id = %d` |
| `delete_by_id` | 83,313 / 12.267 ms / 16 ms | 0 / n/a / n/a | `DELETE FROM %s WHERE id = %d` |
| `delete_by_id_limit` | 0 / n/a / n/a | 117,777 / 8.366 ms / 16 ms | `DELETE FROM %s WHERE id = %d LIMIT 1` |

## Per-minute performance

| Minute | DSQL QPS | DSQL p99 ms | DSQL CPU % | TiDB QPS | TiDB p99 ms | TiDB CPU % |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 26,785.42 | 22.65 | 3.65 | 39,304.71 | 9.73 | 2.80 |
| 2 | 27,925.34 | 14.55 | 4.07 | 39,294.87 | 9.88 | 2.90 |
| 3 | 27,809.62 | 14.34 | 3.92 | 39,267.28 | 9.85 | 2.85 |
| 4 | 28,069.53 | 14.21 | 3.99 | 38,883.73 | 10.20 | 2.81 |
| 5 | 27,947.46 | 14.29 | 3.83 | 39,329.78 | 9.85 | 2.81 |
| 6 | 27,883.58 | 14.25 | 3.90 | 39,313.32 | 9.79 | 2.81 |
| 7 | 27,685.99 | 14.46 | 3.81 | 39,435.89 | 9.73 | 2.97 |
| 8 | 27,713.86 | 14.38 | 3.92 | 39,457.17 | 9.73 | 2.89 |
| 9 | 27,723.49 | 14.38 | 3.88 | 39,428.71 | 9.76 | 2.78 |
| 10 | 27,767.44 | 14.34 | 3.97 | 39,113.63 | 10.22 | 2.81 |

## Client VM resource utilization

| Metric | Aurora DSQL | TiDB |
|---|---:|---:|
| Client instance type | `c8g.4xlarge` | `c8g.4xlarge` |
| Sampler samples / duration | 671 / 670s | 710 / 709s |
| CPU avg / p95 / p99 | 3.51% / 4.80% / 5.20% | 2.44% / 3.60% / 3.80% |
| Memory used avg | 884.20 MB | 863.71 MB |
| Network RX avg | 8.46 Mbps | 7.72 Mbps |
| Network TX avg | 4.92 Mbps | 4.23 Mbps |
| Network received / transmitted | 5,670.34 / 3,297.60 MB | 5,473.60 / 2,997.58 MB |
| Disk written | 50.31 MB | 65.91 MB |
| Context switches | 51,158,682 | 47,604,073 |
| Processes created | 1,349 | 1,247 |

## Price-performance

| Cost basis | Aurora DSQL | TiDB |
|---|---:|---:|
| Billing model used | CloudWatch `TotalDPU` x `$8.00 / 1M DPU` | Provisioned EC2/EBS/network run-window cost from benchmark runner |
| Measured window | 2026-05-11 22:54:46 to 23:05:56 UTC | 2026-05-12 00:25:42 to 00:35:50 UTC |
| Run-window cost | $1.70 | $1.03 |
| Total queries | 16,638,851 | 23,569,915 |
| Cost / 1M queries | $0.1023 | $0.0435 |
| Monthly cost basis | DPU scales with usage; storage continues after idle | Fixed provisioned cluster projection: `$3,801.64/mo` |
| Monthly $/QPS | Not meaningful for serverless DPU | `$0.0968` |

For this measured 600-second load shape, TiDB delivered higher throughput and lower query cost. This does not compare idle economics: DSQL compute scales to zero when idle, while this TiDB result uses fixed provisioned infrastructure.

## DSQL DPU detail

DSQL CloudWatch collection window: 2026-05-11 22:54:46 to 23:07:56 UTC.

| Metric | Value | Share |
|---|---:|---:|
| TotalDPU | 212,808.00 | 100.00% |
| WriteDPU | 98,563.44 | 46.32% |
| ReadDPU | 62,594.42 | 29.41% |
| ComputeDPU | 51,650.13 | 24.27% |

## Notes on interpretation

- The comparison uses the same client VM and workload shape, but DSQL and TiDB are different systems with different wire protocols and billing surfaces.
- TiDB server-side sampler discovery was skipped during the final measured run because local AWS credentials expired, but client-side sampler data and all sysbench metrics were collected successfully.
- The DSQL result is from a full prepared dataset verified at 32 x 500,000 rows, then measured with `--skip-prepare` after credentials were refreshed; no write workload ran before the accepted measurement.
