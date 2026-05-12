# db-bench

Monorepo for database benchmarking tools on AWS. Each subdirectory contains scripts to provision, validate, and benchmark a specific database system on bare EC2.

## Structure

```
common/          Shared AWS, SSH, client, sampler, reporting, and utility modules
tidb/            TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
valkey/          Valkey with Envoy proxy (standalone and cluster modes)
dsql/            Amazon Aurora DSQL (serverless, PostgreSQL-compatible)
aurora/          Aurora MySQL provisioning, validation, and benchmarking
aurora-pg/       Aurora PostgreSQL benchmark wrapper
```

## Common Library

`common/` provides shared infrastructure used by all database modules:

- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution (context-based and lightweight), SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session/client helpers, AMI resolution, CLI argument helpers
- **types.py** -- Shared dataclasses (`InstanceInfo` with role, instance_id, public/private IPs, instance_type)

## Prerequisites

- Python 3.10+ with `boto3` and `botocore`
- AWS CLI v2 configured with a profile pointing at the target account (default profile: `sandbox`)
- `ssh` and `scp` in PATH

### SSH Key Setup

Most module setup scripts create or reuse a shared EC2 key pair (`dbbench-key`) stored at `common/dbbench-key.pem`. TiDB expects that key file to exist before provisioning, so the safest bootstrap is to generate and import it explicitly once:

```bash
ssh-keygen -t ed25519 -f common/dbbench-key.pem -N "" -C "dbbench"
aws ec2 import-key-pair \
  --key-name dbbench-key \
  --public-key-material fileb://common/dbbench-key.pem.pub \
  --profile sandbox --region us-east-1
```

The `.pem` file is gitignored.

## Running

All modules are designed to run from the repo root using `python3 -m`:

```bash
python3 -m tidb.setup --help
python3 -m tidb.benchmark --help
python3 -m tidb.validate --help

python3 -m valkey.setup --help
python3 -m valkey.benchmark --help
python3 -m valkey.validate --help

python3 -m dsql.setup --help
python3 -m dsql.benchmark --help
python3 -m dsql.validate --help

python3 -m aurora.setup --help
python3 -m aurora.benchmark --help
python3 -m aurora.validate --help

python3 -m aurora-pg.benchmark --help
python3 -m common.client --help
python3 -m common.benchmark --help
```

## TiDB

Provisions a multi-AZ TiDB cluster on EC2 via k3s and TiDB Operator, with optional TiCDC replication to a downstream cluster.

### Features

- **Multi-AZ**: 3 availability zones by default (1 leader + 2 replicas per TiKV raft group)
- **Single-AZ mode**: `--single-az` sets all replicas to 1 for low-cost testing
- **Instance tiers**: `--production` (default, PingCAP-recommended) or `--benchmark-mode` (cost-optimized)
- **Dedicated VMs**: Each TiKV pod consumes the entire EC2 instance
- **TiCDC replication**: Deploys upstream + downstream clusters with changefeed lag measurement
- **Benchmark profiles**: quick, light, standard, heavy, stress, scaling
- **Workloads**: oltp_read_write, oltp_read_only, oltp_write_only, oltp_point_select, oltp_insert, oltp_delete, oltp_update_index, oltp_update_non_index

### Quick Start

```bash
# Provision (default: 3 PD, 3 TiKV, 2 TiDB across 3 AZs, production instance types)
AWS_PROFILE=sandbox python3 -m tidb.setup

# Cost-optimized single-AZ (1 PD, 1 TiKV, 1 TiDB, smaller instances)
AWS_PROFILE=sandbox python3 -m tidb.setup --single-az --benchmark-mode

# With TiCDC replication
AWS_PROFILE=sandbox python3 -m tidb.setup --ticdc

# Validate cluster health
AWS_PROFILE=sandbox python3 -m tidb.validate

# Benchmark (quick profile, oltp_read_write)
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile quick

# Benchmark specific workload
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile quick --workload oltp_point_select

# Benchmark with TiCDC lag measurement
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile quick --ticdc

# Cleanup all resources
AWS_PROFILE=sandbox python3 -m tidb.setup --cleanup
```

### TiCDC Lag Measurement

When `--ticdc` is passed to the benchmark, replication lag is measured using injected timestamps:

1. Writer thread INSERTs sequenced rows into `cdc_test.lag_tracker` on upstream
2. Reader thread polls downstream for newly replicated rows
3. Lag = time row appeared on downstream minus time written to upstream (both client-side, no clock skew)
4. Reports min/avg/p50/p95/p99/max lag alongside benchmark TPS and latency

## Valkey

Provisions Valkey instances with an Envoy sidecar proxy on EC2, supporting standalone and cluster modes.

### Features

- **Standalone and cluster modes** (3+ Valkey nodes enables cluster mode)
- **Envoy proxy**: Load balancing with configurable topology
- **NLB integration**: Internal Network Load Balancer for client -> Envoy traffic
- **FlameGraph capture**: CPU profiling of Envoy during benchmarks
- **Benchmarks**: valkey-benchmark and memtier_benchmark support

### Quick Start

```bash
# Provision standalone (1 Valkey node, 1 Envoy, NLB)
AWS_PROFILE=sandbox python3 -m valkey.setup

# Provision clustered (3 Valkey nodes, 1 Envoy, NLB)
AWS_PROFILE=sandbox python3 -m valkey.setup --valkey-nodes 3

# Validate cluster health
AWS_PROFILE=sandbox python3 -m valkey.validate

# Benchmark (requires client public IP from setup output)
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_PUBLIC_IP> \
  --mode proxy

# Benchmark directly against Valkey (bypassing Envoy)
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_PUBLIC_IP> \
  --target-host <VALKEY_PRIVATE_IP> \
  --mode valkey

# Cleanup all resources (S3 bucket retained)
AWS_PROFILE=sandbox python3 -m valkey.setup --cleanup
```

## DSQL

Benchmarks Amazon Aurora DSQL, a serverless PostgreSQL-compatible database, using sysbench (PostgreSQL driver) with IAM authentication.

### Features

- **Serverless**: No server EC2 to provision -- only a client VM and a DSQL cluster via AWS API
- **sysbench (unified)**: Same sysbench pipeline as TiDB/Aurora MySQL/Aurora PG (`--db-driver=pgsql`), with custom Lua workloads ported to PostgreSQL-compatible engines for cross-engine comparison
- **IAM auth tokens**: Automatic token generation and refresh for runs exceeding 15 minutes
- **CloudWatch metrics**: Captures DSQL-specific server-side metrics (DPU, OCC conflicts, commit latency, storage)
- **Cost estimation**: Estimates DSQL costs from DPU consumption during the benchmark
- **Benchmark profiles**: quick, light, standard, heavy, stress

### Quick Start

```bash
# Provision (client VM + DSQL cluster)
AWS_PROFILE=sandbox python3 -m dsql.setup --seed dsqllt-001

# Validate
AWS_PROFILE=sandbox python3 -m dsql.validate --seed dsqllt-001

# Benchmark (quick profile)
AWS_PROFILE=sandbox DB_PROFILE=sandbox-storage python3 -m dsql.benchmark \
  --action run \
  --seed dsqllt-001 \
  --host <CLIENT_PUBLIC_IP> \
  --dsql-cluster-id <DSQL_CLUSTER_ID> \
  --dsql-cluster-endpoint <DSQL_ENDPOINT> \
  --dsql-region us-east-1 \
  --dsql-db-profile sandbox-storage \
  --aws-profile sandbox \
  --profile quick

# Benchmark (quick smoke test, 1 minute)
AWS_PROFILE=sandbox DB_PROFILE=sandbox-storage python3 -m dsql.benchmark \
  --action run \
  --seed dsqllt-001 \
  --host <CLIENT_PUBLIC_IP> \
  --dsql-cluster-id <DSQL_CLUSTER_ID> \
  --dsql-cluster-endpoint <DSQL_ENDPOINT> \
  --dsql-region us-east-1 \
  --dsql-db-profile sandbox-storage \
  --aws-profile sandbox \
  --profile quick

# Cleanup
python3 -m dsql.setup --seed dsqllt-001 --cleanup --aws-profile sandbox
```

### DSQL Limitations

- Only `postgres` database available (no custom databases)
- No VACUUM support (DSQL rejects PostgreSQL `VACUUM`)
- Optimistic concurrency conflicts can happen under write-heavy loads; capture `OccConflicts` from CloudWatch and sysbench ignored-error counters in reports
- 3000-row transaction limit
- Auth tokens expire after 15 minutes (auto-refreshed for long runs)

### DSQL sysbench result fields

The DSQL sysbench JSON includes the normal sysbench transaction metrics (`tps`, `qps`, `latency_avg_ms`, `latency_p99_ms`, query counts, and interval samples). For `custom_mixed`, it also includes:

- `op_latency_ms`: client-side latency by operation category (`select`, `insert`, `update`, `delete` for the single-statement `custom_mixed` workload)
- `query_latency_ms`: client-side latency by stable query template key, with `type`, `category`, and the raw Lua SQL template string
- `read_qps`, `write_qps`, and `other_qps` on interval samples when sysbench emits `(r/w/o: ...)`

The per-operation and per-template `p50_ms` / `p95_ms` / `p99_ms` values are derived from fixed latency buckets and represent bucket upper bounds. The top-level sysbench percentile is still the configured sysbench percentile; the default benchmark command uses `--percentile=99`, so `latency_p95_ms` can be `null` while `latency_p99_ms` is populated.

DSQL does not support `pg_database_size`, so DB-size-derived storage numbers can be zero in local result JSON. Local result JSON/CSV/log artifacts are intentionally gitignored and should not be committed.

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
- [Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/)
- [sysbench](https://github.com/akopytov/sysbench)
