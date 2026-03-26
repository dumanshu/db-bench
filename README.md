# db-bench

Monorepo for database benchmarking tools on AWS. Supports TiDB, Aurora MySQL, and Valkey with a unified client provisioner, shared benchmark framework, and autonomous remote execution.

## Structure

```
common/              Shared AWS, SSH, benchmarking, metrics, and reporting modules
  aws.py             VPC, subnets, security groups, EC2 instance lifecycle, cleanup
  ssh.py             SSH command execution, SCP file transfer, wait-for-ready
  util.py            Timestamps, logging, AWS session helpers, AMI resolution
  types.py           Shared dataclasses (InstanceInfo, BootstrapContext)
  client.py          Unified client VM provisioning (all tools for any server type)
  benchmark.py       Unified benchmark CLI entry point (--server-type aurora|tidb|valkey)
  remote_runner.py   Autonomous benchmark execution on client VM (deploy/status/fetch)
  sampler.py         EC2 metrics sampling + CloudWatch queries + post-processing
  report.py          Markdown report generation + cost tracking
  lua/               Custom sysbench Lua workloads (IUD, mixed read/write)
  __main__.py        Allows `python3 -m common` to run the unified benchmark CLI
tidb/                TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
  setup.py           Multi-node k3s cluster provisioning + TiDB Operator bootstrap
  driver.py          TiDB-specific helpers (SSH, cluster discovery, CDC lag, disk, bulk load)
  benchmark.py       Thin redirect to common.benchmark --server-type tidb
  validate.py        Cluster health validation
aurora/              Aurora MySQL benchmarking (sysbench, IO-Optimized storage)
  setup.py           RDS Aurora cluster provisioning (IO-Optimized, snapshots)
  driver.py          Aurora-specific helpers (stack discovery, CloudWatch, InnoDB counters)
  benchmark.py       Thin redirect to common.benchmark --server-type aurora
  validate.py        Cluster health validation
  modify_instance.sh Live instance type modification
valkey/              Valkey with Envoy proxy (standalone and cluster modes)
  setup.py           Valkey + Envoy + NLB provisioning and bootstrap
  benchmark.py       Valkey benchmark CLI (valkey-benchmark + memtier + FlameGraph)
  validate.py        Stack health validation
  capture_cpu_split.py     CPU profiling capture
  envoy_latency_report.py  Envoy latency analysis
```

## Workflow

All benchmarks follow a 3-step decoupled workflow:

1. **Provision server** -- deploy the database cluster and its VPC
2. **Provision client** -- deploy an independent benchmark VM in the server's VPC (one client works with any server type)
3. **Run benchmark** -- interactive (continuous SSH) or remote (fire-and-forget on client VM)

```
                     +--> tidb.setup   --> TiDB k3s cluster
                     |
  common/client.py --+--> aurora.setup --> Aurora RDS cluster
  (shared client VM) |
                     +--> valkey.setup --> Valkey + Envoy + NLB
```

The client VM is provisioned with sysbench, mysql client, memtier_benchmark, docker, and valkey-cli -- everything needed to benchmark any of the three database systems.

## Prerequisites

- Python 3.9+ with `boto3`
- AWS CLI v2 configured with a profile pointing at the target account
- SSH key pair uploaded to EC2 (one per database type -- see Quick Start sections)
- For Aurora: separate AWS profile with RDS permissions (`--rds-profile`)

## Quick Reference

```bash
# --- TiDB ---
# 1. Provision cluster (single-AZ for benchmarking)
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --benchmark-mode --single-az

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed my-001 --server-type tidb --size small

# 3. Run benchmark (interactive)
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --profile quick

# 3b. Or fire-and-forget (remote runner) -- deploy is the default action
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --profile heavy
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --action status
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --action fetch

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed my-001 --server-type tidb
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --cleanup

# --- Aurora ---
# 1. Provision cluster (dual-profile: sandbox for EC2, sandbox-storage for RDS)
AWS_PROFILE=sandbox python3 -m aurora.setup --seed my-001 --rds-profile sandbox-storage

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed my-001 --server-type aurora --size small

# 3. Run benchmark (deploy is the default -- fire-and-forget on client VM)
AWS_PROFILE=sandbox python3 -m common --server-type aurora --seed my-001 \
  --rds-profile sandbox-storage --tables 4 --table-size 10000 --threads 16 --duration 30

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed my-001 --server-type aurora
AWS_PROFILE=sandbox python3 -m aurora.setup --seed my-001 --cleanup --rds-profile sandbox-storage

# --- Valkey ---
# 1. Provision cluster (bastion + envoy + valkey nodes + NLB)
AWS_PROFILE=sandbox python3 -m valkey.setup --seed my-001

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed my-001 --server-type valkey --size small

# 3. Run benchmark (via unified CLI -- deploys memtier on client VM)
AWS_PROFILE=sandbox python3 -m common --server-type valkey --seed vlklt-001 --endpoint <NLB_DNS>

# Or use the standalone Valkey benchmark CLI for more control:
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_IP> --ssh-key valkey/valkey-load-test-key.pem --seed vlklt-001

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed my-001 --server-type valkey
AWS_PROFILE=sandbox python3 -m valkey.setup --seed my-001 --cleanup
```

## Common Library

`common/` provides shared infrastructure and benchmark tooling used by all database modules.

### Infrastructure

- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution, SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session helpers, AMI resolution
- **types.py** -- Shared dataclasses (`InstanceInfo`, `BootstrapContext`)

### Client Provisioning

**`python3 -m common.client`** -- Standalone benchmark client provisioner:

- Discovers the server VPC by `--seed` and `--server-type`, provisions an EC2 instance with all benchmark tools (sysbench, mysql client, memtier_benchmark 2.3.0, docker, valkey-cli)
- Supports `--size small` (c8g.4xlarge) or `--size heavy` (c8g.24xlarge)
- Saves state to `common/client-{seed}-state.json` (auto-discovered by benchmark CLI)
- `--cleanup` tears down the client instance and security group

```bash
python3 -m common.client --seed foo --server-type aurora --size small
python3 -m common.client --seed foo --server-type tidb --size heavy
python3 -m common.client --seed foo --server-type valkey --size small
python3 -m common.client --cleanup --seed foo --server-type aurora
```

### Unified Benchmark CLI

**`python3 -m common`** (or `python3 -m common.benchmark`) -- Single entry point for all three database types:

```bash
# Aurora (sysbench)
python3 -m common --server-type aurora --seed my-001 --threads 64 --duration 300

# TiDB (sysbench)
python3 -m common --server-type tidb --seed my-001 --profile heavy

# Valkey (memtier_benchmark via remote runner)
python3 -m common --server-type valkey --seed my-001 --endpoint <NLB_DNS>

# Legacy module entry points still work (thin redirects):
python3 -m aurora.benchmark --seed my-001    # injects --server-type aurora
python3 -m tidb.benchmark --seed my-001      # injects --server-type tidb
```

The default `--action` is `deploy` (fire-and-forget remote execution). Use `--action run` for interactive (continuous SSH) mode. Valkey always uses deploy mode via the remote runner.

Valkey also has its own standalone benchmark CLI for direct memtier/valkey-benchmark control: `python3 -m valkey.benchmark`.

**Key features:**
- Auto-discovers server endpoints and client VM from seed + state files
- Built-in profiles: `quick`, `light`, `medium`, `heavy`, `standard`, `stress`, `scaling`
- Workloads: `oltp_read_write`, `oltp_read_only`, `oltp_write_only`, `oltp_point_select`, `oltp_insert`, `oltp_delete`, `oltp_update_index`, `oltp_update_non_index`, `custom_iud`, `custom_mixed`
- Custom Lua workloads in `common/lua/`:
  - `custom_iud.lua` -- 68% insert, 28% update, 4% delete (production ratio)
  - `custom_mixed.lua` -- 88% read, 8% insert, 3% update, 1% delete (production ratio)
- Parallel sysbench processes (`--parallel N`)
- Fill phase for buffer pool pressure (`--fill`)
- TiCDC replication lag measurement (`--ticdc`)
- Dual-host routing: cluster management commands go to control node, benchmark execution goes to client VM

### Remote Runner (Autonomous Benchmarks)

**`python3 -m common`** -- By default (`--action deploy`), deploys a self-contained bash script to the client VM and runs it inside a tmux session. Expired AWS credentials won't interrupt a running benchmark.

```bash
# Deploy (default action -- generates script, uploads, starts in tmux on client VM)
python3 -m common --server-type tidb --seed my-001 --profile quick

# Check progress (reads status.json from client)
python3 -m common --server-type tidb --seed my-001 --action status

# Fetch results when done
python3 -m common --server-type tidb --seed my-001 --action fetch

# Manual monitoring (SSH to client, attach tmux)
ssh -i <key> ec2-user@<client-ip> 'tmux attach -t bench'
# Or tail the log:
ssh -i <key> ec2-user@<client-ip> 'tail -f /home/ec2-user/bench-results/*/benchmark.log'
```

The remote runner generates scripts for:
- **sysbench** (TiDB, Aurora): prepare/run/cleanup phases with atomic `status.json` writes
- **memtier** (Valkey): multi-run test loops with JSON output per run

### Metrics and Reporting

- **sampler.py** -- EC2 metrics sampling, parameterized by `--server-type`:
  - Always: CPU jiffies from `/proc/stat`, memory from `/proc/meminfo`
  - tidb/aurora: InnoDB row counters via mysql CLI
  - valkey: `valkey-cli INFO stats`
  - CloudWatch queries per server type (Aurora RDS, EC2, ElastiCache)
  - Post-processing: windowed analysis, safe_stats, sysbench output parsing
- **report.py** -- Benchmark report generation:
  - `generate_report()` -- Markdown report from JSON results
  - `CostTracker` -- AWS cost estimation during benchmark runs
  - Instance pricing for Aurora db.r8g families, TiDB/Valkey c8g/m8g EC2 families (Graviton4)

## Aurora MySQL

Provisions an Aurora MySQL 3.12.0 cluster with IO-Optimized storage (aurora-iopt1) and 1 read replica by default. Uses dual AWS profiles -- one for EC2/VPC operations and a separate one for RDS operations (required when RDS permissions are in a different IAM role).

### Features

- **IO-Optimized storage**: aurora-iopt1 for consistent throughput
- **Read replicas**: 1 replica by default (`--replicas N`), spread across AZs
- **Dual AWS profile**: `--aws-profile` for EC2/VPC, `--rds-profile` for RDS
- **Snapshot/restore**: Create cluster snapshots after fill, restore to skip re-filling
- **Custom workloads**: IUD and mixed Lua scripts matching production ratios
- **Fill phase**: INSERT...SELECT doubling to fill buffer pool to N x instance RAM
- **Parallel sysbench**: Run multiple sysbench processes concurrently
- **InnoDB counters**: Tracks actual insert/update/delete/read rates
- **CloudWatch integration**: CPU, network throughput, IOPS, latency metrics

### Quick Start

```bash
# 1. Provision (default: db.r8g.xlarge, IO-Optimized, 1 read replica)
AWS_PROFILE=sandbox python3 -m aurora.setup --seed auroralt-001 --rds-profile sandbox-storage

# No replicas
AWS_PROFILE=sandbox python3 -m aurora.setup --seed auroralt-001 --rds-profile sandbox-storage --replicas 0

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed auroralt-001 --server-type aurora --size small

# 3. Benchmark
AWS_PROFILE=sandbox python3 -m common --server-type aurora --seed auroralt-001 \
  --rds-profile sandbox-storage --threads 64 --duration 300

# Parallel sysbench
python3 -m common --server-type aurora --seed auroralt-001 --parallel 4 --threads 64

# Fill phase (buffer pool pressure)
python3 -m common --server-type aurora --seed auroralt-001 --fill --rds-profile sandbox-storage

# Snapshot after fill
python3 -m aurora.setup --snapshot --seed auroralt-001 --rds-profile sandbox-storage

# Restore from snapshot
python3 -m aurora.setup --restore-snapshot <snapshot-id> --seed auroralt-002 --rds-profile sandbox-storage

# Live instance type change
bash aurora/modify_instance.sh

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed auroralt-001 --server-type aurora
AWS_PROFILE=sandbox python3 -m aurora.setup --cleanup --seed auroralt-001 --rds-profile sandbox-storage
```

## TiDB

Provisions a multi-AZ TiDB cluster on EC2 via k3s and TiDB Operator (Graviton4 c8g/m8g instances, sized per PingCAP production requirements), with optional TiCDC replication to a downstream cluster.

### Features

- **Multi-AZ**: 3 availability zones by default (configurable, `--single-az` for benchmarks)
- **Dedicated VMs**: Each TiKV pod consumes the entire EC2 instance
- **TiCDC replication**: Upstream + downstream clusters with changefeed lag measurement
- **Benchmark profiles**: quick, light, medium, heavy, standard, stress, scaling
- **Dual-host SSH routing**: Cluster management on control node, benchmark execution on client VM
- **Bulk data load**: Disk fill phase for realistic storage pressure

### Quick Start

```bash
# 1. Provision (single-AZ for benchmarking)
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --benchmark-mode --single-az

# Multi-AZ production-like
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --multi-az

# With TiCDC replication
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --ticdc

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed my-001 --server-type tidb --size small

# 3. Validate
AWS_PROFILE=sandbox python3 -m tidb.validate

# 4. Benchmark
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --profile quick

# With TiCDC lag measurement
AWS_PROFILE=sandbox python3 -m common --server-type tidb --seed my-001 --profile standard --ticdc

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed my-001 --server-type tidb
AWS_PROFILE=sandbox python3 -m tidb.setup --seed my-001 --cleanup
```

### TiCDC Lag Measurement

When `--ticdc` is passed to the benchmark:

1. Writer thread INSERTs sequenced rows into `cdc_test.lag_tracker` on upstream
2. Reader thread polls downstream for newly replicated rows
3. Lag = time row appeared on downstream minus time written to upstream (client-side, no clock skew)
4. Reports min/avg/p50/p95/p99/max lag alongside benchmark TPS and latency

## Valkey

Provisions Valkey 9.0.3 instances with an Envoy v1.37.1 sidecar proxy on EC2, supporting standalone and cluster modes. Uses Docker containers for both Valkey and Envoy.

### Features

- **Standalone and cluster modes** (3+ Valkey nodes enables cluster mode)
- **Envoy proxy**: Load balancing with configurable topology
- **NLB integration**: Network Load Balancer for external access via Envoy
- **FlameGraph capture**: CPU profiling of Envoy during benchmarks
- **Benchmarks**: valkey-benchmark and memtier_benchmark support
- **Bastion host**: SSH jump box for accessing private Valkey and Envoy nodes

### Quick Start

```bash
# 1. Provision (bastion + envoy + valkey + NLB)
AWS_PROFILE=sandbox python3 -m valkey.setup --seed vlklt-001

# Cluster mode (3+ nodes)
AWS_PROFILE=sandbox python3 -m valkey.setup --seed vlklt-001 --valkey-nodes 3

# 2. Provision client
AWS_PROFILE=sandbox python3 -m common.client --seed vlklt-001 --server-type valkey --size small

# 3. Validate
AWS_PROFILE=sandbox python3 -m valkey.validate --ssh-private-key-path valkey/valkey-load-test-key.pem

# 4. Benchmark (from client VM, targets NLB endpoint)
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_IP> \
  --ssh-key valkey/valkey-load-test-key.pem \
  --seed vlklt-001

# With FlameGraph capture
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_IP> \
  --ssh-key valkey/valkey-load-test-key.pem \
  --seed vlklt-001 \
  --flamegraph-host <ENVOY_PRIVATE_IP> \
  --flamegraph-jump-host <BASTION_IP>

# Cleanup
AWS_PROFILE=sandbox python3 -m common.client --cleanup --seed vlklt-001 --server-type valkey
AWS_PROFILE=sandbox python3 -m valkey.setup --seed vlklt-001 --cleanup
```

### SSH Access to Private Nodes

Valkey and Envoy nodes are in private subnets. Access via bastion:

```bash
# Direct SSH through bastion
ssh -o ProxyCommand="ssh -i valkey/valkey-load-test-key.pem ec2-user@<BASTION_IP> -W %h:%p" \
  -i valkey/valkey-load-test-key.pem ec2-user@<PRIVATE_IP>
```

## SSH Keys

Each database type uses its own SSH key pair. Keys must be uploaded to EC2 before provisioning:

| Database | Key File | AWS Key Name |
|----------|----------|--------------|
| TiDB | `tidb/tidb-load-test-key.pem` | `tidb-load-test-key` |
| Aurora | `aurora/aurora-bench-key.pem` | `aurora-bench-key` |
| Valkey | `valkey/valkey-load-test-key.pem` | `valkey-load-test-key` |
| Client | `common/db-bench-client-{seed}.pem` | `db-bench-client-{seed}` |

The client key is auto-generated during `common.client` provisioning.

## AWS Profiles

Different database types may require different IAM permissions:

| Operation | Profile | Notes |
|-----------|---------|-------|
| EC2, VPC, SG | `sandbox` | Default for all server types |
| RDS (Aurora) | `sandbox-storage` | Pass via `--rds-profile` |
| CloudWatch | Same as RDS profile | Aurora metrics require RDS-level access |

## Running

All modules are invoked from the repo root using `python3 -m`:

```bash
# Unified benchmark CLI (Aurora + TiDB + Valkey)
python3 -m common --help
python3 -m common.benchmark --help

# Client provisioning
python3 -m common.client --help

# Server setup
python3 -m tidb.setup --help
python3 -m aurora.setup --help
python3 -m valkey.setup --help

# Valkey benchmark (separate CLI)
python3 -m valkey.benchmark --help
```

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Aurora MySQL Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMySQL.html)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
- [sysbench](https://github.com/akopytov/sysbench)
- [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
