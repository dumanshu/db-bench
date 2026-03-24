# db-bench

Monorepo for database benchmarking tools on AWS. Each subdirectory contains scripts to provision, validate, and benchmark a specific database system on bare EC2.

## Structure

```
common/          Shared AWS, SSH, and utility modules
tidb/            TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
valkey/          Valkey with Envoy proxy (standalone and cluster modes)
```

## Common Library

`common/` provides shared infrastructure used by all database modules:

- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution (context-based and lightweight), SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session/client helpers, AMI resolution, CLI argument helpers
- **types.py** -- Shared dataclasses (`InstanceInfo` with role, instance_id, public/private IPs, instance_type)

## Prerequisites

- Python 3.9+ with `boto3`
- AWS CLI v2 configured with a profile pointing at the target account (default profile: `sandbox`)
- `ssh` and `scp` in PATH

### SSH Key Setup

Each module requires an EC2 key pair for SSH access to provisioned instances. Generate and import them before first use:

```bash
# TiDB key pair
ssh-keygen -t ed25519 -f tidb/tidb-load-test-key.pem -N "" -C "tidb-load-test"
aws ec2 import-key-pair \
  --key-name tidb-load-test-key \
  --public-key-material fileb://tidb/tidb-load-test-key.pem.pub \
  --profile sandbox --region us-east-1

# Valkey key pair
ssh-keygen -t ed25519 -f valkey/valkey-load-test-key.pem -N "" -C "valkey-load-test"
aws ec2 import-key-pair \
  --key-name valkey-load-test-key \
  --public-key-material fileb://valkey/valkey-load-test-key.pem.pub \
  --profile sandbox --region us-east-1
```

The `.pem` files are gitignored. Each setup script defaults to looking for its key at `<module>/<module>-load-test-key.pem` (e.g. `tidb/tidb-load-test-key.pem`).

## Running

All modules are designed to run from the repo root using `python3 -m`:

```bash
python3 -m tidb.setup --help
python3 -m tidb.benchmark --help
python3 -m tidb.validate --help

python3 -m valkey.setup --help
python3 -m valkey.benchmark --help
python3 -m valkey.validate --help
```

## TiDB

Provisions a multi-AZ TiDB cluster on EC2 via k3s and TiDB Operator, with optional TiCDC replication to a downstream cluster.

### Features

- **Multi-AZ**: 3 availability zones by default (1 leader + 2 replicas per TiKV raft group)
- **Single-AZ mode**: `--single-az` sets all replicas to 1 for low-cost testing
- **Instance tiers**: `--production` (default, PingCAP-recommended) or `--benchmark-mode` (cost-optimized)
- **Dedicated VMs**: Each TiKV pod consumes the entire EC2 instance
- **TiCDC replication**: Deploys upstream + downstream clusters with changefeed lag measurement
- **Benchmark profiles**: quick, light, medium, heavy, standard, stress, scaling
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

# Benchmark (standard profile, oltp_read_write)
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile standard

# Benchmark specific workload
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile quick --workload oltp_point_select

# Benchmark with TiCDC lag measurement
AWS_PROFILE=sandbox python3 -m tidb.benchmark --profile standard --ticdc

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

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
