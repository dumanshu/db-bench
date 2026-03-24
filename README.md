# db-bench

Monorepo for database benchmarking tools on AWS. Each subdirectory contains scripts to provision, validate, and benchmark a specific database system on bare EC2.

## Structure

```
common/          Shared AWS, SSH, and utility modules
tidb/            TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
valkey/          Valkey with Envoy proxy (standalone and cluster modes)
aurora/          Aurora MySQL benchmarking (sysbench, IO-Optimized storage)
```

## Common Library

`common/` provides shared infrastructure used by all database modules:

- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution, SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session helpers, AMI resolution
- **types.py** -- Shared dataclasses (`InstanceInfo`, `BootstrapContext`)

## Aurora MySQL

Provisions an Aurora MySQL cluster with IO-Optimized storage (aurora-iopt1) and an EC2 client instance pre-loaded with sysbench. Includes custom IUD workloads matching production read/write ratios, fill-phase data generation via INSERT...SELECT doubling, snapshot/restore workflow, and production baseline comparison reporting.

### Features

- **IO-Optimized storage**: aurora-iopt1 for consistent throughput
- **Custom workloads**: IUD (68% insert, 28% update, 4% delete) and mixed (88% read, 8% insert, 3% update, 1% delete) Lua scripts matching production ratios
- **Fill phase**: INSERT...SELECT doubling to fill buffer pool to N x instance RAM
- **Snapshot/restore**: Create cluster snapshots after fill, restore to skip re-filling
- **Parallel sysbench**: Run multiple sysbench processes concurrently
- **InnoDB row counter measurement**: Tracks actual insert/update/delete/read rates
- **CloudWatch integration**: Aurora CPU, network throughput metrics
- **Client CPU monitoring**: mpstat-based EC2 client utilization tracking
- **Production baseline comparison**: Automatic comparison against known production metrics
- **Dual AWS profile support**: Separate profiles for EC2/VPC and RDS operations

### Quick Start

```bash
# Generate SSH key (if not already done)
ssh-keygen -t ed25519 -f aurora/aurora-bench-key.pem -N ""

# Provision (default: db.r8g.xlarge Aurora, c8g.24xlarge EC2 client)
python3 -m aurora.setup --seed auroralt-001 --aws-profile sandbox

# Provision with larger instance
python3 -m aurora.setup --instance-type db.r8g.16xlarge --aws-profile sandbox

# Validate (checks cluster, connectivity, sysbench, runs quick benchmark)
python3 -m aurora.validate --seed auroralt-001 --aws-profile sandbox

# Fill phase (create background data to pressure buffer pool)
python3 -m aurora.benchmark --fill --seed auroralt-001 --aws-profile sandbox

# Snapshot after fill (avoids re-filling on future runs)
python3 -m aurora.setup --snapshot --seed auroralt-001

# Benchmark (custom mixed workload, 64 threads, 5 minutes)
python3 -m aurora.benchmark --seed auroralt-001 --aws-profile sandbox

# Benchmark with parallel sysbench processes
python3 -m aurora.benchmark --parallel 4 --threads 64 --seed auroralt-001

# Restore from snapshot (skip fill on next provision)
python3 -m aurora.setup --restore-snapshot <snapshot-id> --seed auroralt-002

# Cleanup
python3 -m aurora.setup --cleanup --seed auroralt-001 --aws-profile sandbox
```

### Additional Tools

- **aurora/ec2_sampler.py** -- EC2 instance type sampling and selection
- **aurora/parse_window.py** -- CloudWatch metric window parsing and analysis
- **aurora/generate_report.py** -- Benchmark result report generation
- **aurora/calibrate.sh** -- Instance calibration shell script
- **aurora/run_final.sh** -- Full benchmark run orchestration
- **aurora/modify_instance.sh** -- Live Aurora instance type modification

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
python3 -m aurora.setup --help
```

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
- [Aurora MySQL Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMySQL.html)
- [sysbench](https://github.com/akopytov/sysbench)
