# db-bench

Monorepo for database benchmarking tools on AWS. Each subdirectory contains scripts to provision, validate, and benchmark a specific database system on bare EC2.

## Structure

```
common/          Shared AWS, SSH, and utility modules
tidb/            TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
valkey/          Valkey with Envoy proxy (standalone and cluster modes)
dsql/            Amazon Aurora DSQL (serverless, PostgreSQL-compatible)
aurora/          Aurora benchmarking (planned)
```

## Common Library

`common/` provides shared infrastructure used by all database modules:

- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution, SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session helpers, AMI resolution
- **types.py** -- Shared dataclasses (`InstanceInfo`, `BootstrapContext`)

## TiDB

Provisions a multi-AZ TiDB cluster on EC2 via k3s and TiDB Operator, with optional TiCDC replication to a downstream cluster.

### Features

- **Multi-AZ**: 3 availability zones by default (1 leader + 2 replicas per TiKV raft group)
- **Dedicated VMs**: Each TiKV pod consumes the entire EC2 instance
- **TiCDC replication**: Deploys upstream + downstream clusters with changefeed lag measurement
- **Benchmark profiles**: quick, light, medium, heavy, standard, stress, scaling
- **Workloads**: oltp_read_write, oltp_read_only, oltp_write_only, oltp_point_select, oltp_insert, and more

### Quick Start

```bash
# Provision (default: 3 PD, 3 TiKV, 2 TiDB across 3 AZs)
AWS_PROFILE=sandbox python3 -m tidb.setup

# With TiCDC replication
python3 -m tidb.setup --aws-profile sandbox --ticdc

# Validate
AWS_PROFILE=sandbox python3 -m tidb.validate

# Benchmark (standard profile, 5 minutes)
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile standard

# Benchmark with TiCDC lag measurement
python3 -m tidb.benchmark --aws-profile sandbox --profile standard --ticdc

# Cleanup
python3 -m tidb.setup --cleanup --aws-profile sandbox
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
- **NLB integration**: Network Load Balancer for external access
- **FlameGraph capture**: CPU profiling of Envoy during benchmarks
- **Benchmarks**: valkey-benchmark and memtier_benchmark support

### Quick Start

```bash
# Provision
AWS_PROFILE=sandbox python3 -m valkey.setup \
  --region us-east-1 \
  --seed vlklt-001 \
  --ssh-private-key-path ./valkey-load-test-key.pem

# Validate
AWS_PROFILE=sandbox python3 -m valkey.validate \
  --ssh-private-key-path ./valkey-load-test-key.pem

# Benchmark with FlameGraph
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_IP> \
  --ssh-user ec2-user \
  --ssh-key ./valkey-load-test-key.pem \
  --mode proxy

# Cleanup
python3 -m valkey.setup --cleanup \
  --seed vlklt-001 \
  --ssh-private-key-path ./valkey-load-test-key.pem
```

## DSQL

Benchmarks Amazon Aurora DSQL, a serverless PostgreSQL-compatible database, using pgbench with IAM authentication.

### Features

- **Serverless**: No server EC2 to provision -- only a client VM and a DSQL cluster via AWS API
- **pgbench**: Standard PostgreSQL benchmarking tool with `--max-tries` for OCC retry handling
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

# Validate with quick benchmark
AWS_PROFILE=sandbox python3 -m dsql.validate --seed dsqllt-001 --quick-bench

# Benchmark (standard profile, 15 minutes)
AWS_PROFILE=sandbox python3 -m dsql.benchmark --profile standard

# Benchmark (quick smoke test, 1 minute)
AWS_PROFILE=sandbox python3 -m dsql.benchmark --profile quick

# Cleanup
python3 -m dsql.setup --seed dsqllt-001 --cleanup --aws-profile sandbox
```

### DSQL Limitations

- Only `postgres` database available (no custom databases)
- No VACUUM support (pgbench init uses `-I dtGp` to skip vacuum)
- OCC serialization: conflicts are retried via `--max-tries`
- 3000-row transaction limit
- Auth tokens expire after 15 minutes (auto-refreshed for long runs)

## Prerequisites

- Python 3.9+ with `boto3`
- AWS CLI v2 configured with a profile pointing at the target account
- SSH key pair uploaded to EC2 (see individual module READMEs in the original repos for key generation steps)

## Running

All modules are designed to run from the repo root using `python3 -m`:

```bash
# From db-bench/
python3 -m tidb.setup --help
python3 -m valkey.setup --help
python3 -m dsql.setup --help
```

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
- [Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/)
- [pgbench Documentation](https://www.postgresql.org/docs/current/pgbench.html)
