# db-bench

Monorepo for database benchmarking tools on AWS. Each subdirectory contains scripts to provision, validate, and benchmark a specific database system on bare EC2.

## Structure

```
common/          Shared AWS, SSH, benchmarking, metrics, and reporting modules
  aws.py         VPC, subnets, security groups, EC2 instance lifecycle, cleanup
  ssh.py         SSH command execution, SCP file transfer, wait-for-ready
  util.py        Timestamps, logging, AWS session helpers, AMI resolution
  types.py       Shared dataclasses (InstanceInfo, BootstrapContext)
  client.py      Unified client VM provisioning (all tools for any server type)
  benchmark.py   Unified sysbench orchestration + CLI entry point (--server-type aurora|tidb)
  sampler.py     EC2 metrics sampling + CloudWatch queries + post-processing
  report.py      Markdown report generation + cost tracking
  lua/           Custom sysbench Lua workloads (IUD, mixed read/write)
  __main__.py    Allows `python3 -m common` to run the unified benchmark CLI
tidb/            TiDB on k3s + TiDB Operator (multi-AZ, TiCDC replication)
  driver.py      TiDB-specific helpers (SSH, cluster discovery, CDC lag, disk, bulk load)
  benchmark.py   Thin redirect to common.benchmark --server-type tidb
valkey/          Valkey with Envoy proxy (standalone and cluster modes)
aurora/          Aurora MySQL benchmarking (sysbench, IO-Optimized storage)
  driver.py      Aurora-specific helpers (stack discovery, CloudWatch, InnoDB counters)
  benchmark.py   Thin redirect to common.benchmark --server-type aurora
```

## Common Library

`common/` provides shared infrastructure and benchmark tooling used by all database modules:

### Infrastructure
- **aws.py** -- VPC, subnets, security groups, EC2 instance lifecycle, cleanup
- **ssh.py** -- SSH command execution, SCP file transfer, wait-for-ready
- **util.py** -- Timestamps, logging, AWS session helpers, AMI resolution
- **types.py** -- Shared dataclasses (`InstanceInfo`, `BootstrapContext`)

### Client Provisioning
- **client.py** -- Standalone benchmark client provisioner (`python3 -m common.client`):
  - Discovers server VPC by `--seed` and `--server-type`, provisions an EC2 instance with all benchmark tools (sysbench, mysql client, memtier, docker)
  - Supports `--size small` (c7g.4xlarge) or `--size heavy` (c8g.24xlarge)
  - Saves state to `common/client-{seed}-state.json` (auto-discovered by benchmark CLI)
  - `--cleanup` tears down the client instance and security group

### Benchmarking (sysbench)
- **benchmark.py** -- Unified sysbench orchestration AND CLI entry point for tidb and aurora:
  - **Unified CLI**: `python3 -m common.benchmark --server-type {aurora,tidb}` (single entry point for all sysbench-based benchmarks)
  - `build_sysbench_cmd()` -- parameterized by endpoint, port, user, password, workload
  - `run_sysbench()` / `run_sysbench_streaming()` / `run_sysbench_parallel()` -- execution modes
  - `run_benchmark_streaming()` / `run_adaptive_phase()` / `run_multi_phase_benchmark()` -- generalized orchestration with callable callbacks
  - `sysbench_prepare()` / `sysbench_cleanup()` -- table lifecycle
  - `fast_fill()` -- INSERT...SELECT doubling for buffer pool pressure
  - `upload_lua_scripts()` -- deploy custom Lua workloads to EC2
  - `parse_sysbench_output()` -- unified result parser
  - Built-in profiles: quick, light, medium, heavy, stress, scaling
  - Workloads: oltp_read_write, oltp_read_only, oltp_write_only, oltp_point_select, oltp_insert, oltp_delete, oltp_update_index, oltp_update_non_index, custom_iud, custom_mixed
- **aurora/driver.py** -- Aurora-specific: stack discovery, CloudWatch metrics, InnoDB counters, result display
- **tidb/driver.py** -- TiDB-specific: SSH helpers, EC2/cluster discovery, CdcLagTracker, resource monitoring, bulk load
- **lua/** -- Custom sysbench Lua scripts:
  - `custom_iud.lua` -- 68% insert, 28% update, 4% delete (production ratio)
  - `custom_mixed.lua` -- 88% read, 8% insert, 3% update, 1% delete (production ratio)

### Metrics and Reporting
- **sampler.py** -- EC2 metrics sampling, parameterized by `--server-type`:
  - Always: CPU jiffies from /proc/stat, memory from /proc/meminfo
  - tidb/aurora: InnoDB row counters via mysql CLI
  - valkey: valkey-cli INFO stats
  - Post-processing: windowed analysis, safe_stats, sysbench output parsing
  - CloudWatch queries per server type (Aurora RDS metrics, EC2 metrics, ElastiCache metrics)
- **report.py** -- Benchmark report generation:
  - `generate_report()` -- Markdown report from JSON results
  - `CostTracker` -- AWS cost estimation during benchmark runs
  - `save_results()` / `print_summary()` -- result persistence and display
  - Instance pricing for Aurora db.r* families and TiDB c7g/c8g EC2 families

## Workflow

All benchmarks follow a 3-step workflow:

1. **Provision server** -- `python3 -m {aurora,tidb,valkey}.setup` creates the database cluster and VPC
2. **Provision client** -- `python3 -m common.client --seed <seed> --server-type <type> --size small|heavy` creates a benchmark EC2 in the server's VPC with all tools pre-installed
3. **Run benchmark** -- `python3 -m common.benchmark --server-type <type>` (the client IP and SSH key are auto-discovered from the state file)

The client is independent of the server type -- one client VM has sysbench, mysql, memtier, docker, and works against any database.

## Unified Benchmark CLI

Both Aurora and TiDB benchmarks are invoked through a single entry point:

```bash
# Aurora benchmark
python3 -m common.benchmark --server-type aurora [options]

# TiDB benchmark
python3 -m common.benchmark --server-type tidb [options]

# Equivalently, the legacy module entry points still work (thin redirects):
python3 -m aurora.benchmark [options]   # injects --server-type aurora
python3 -m tidb.benchmark [options]     # injects --server-type tidb
```

Run `python3 -m common.benchmark --help` to see all shared and server-specific options.

Valkey uses a different benchmark tool (valkey-benchmark / memtier) and has its own entry point: `python3 -m valkey.benchmark`.

## Aurora MySQL

Provisions an Aurora MySQL cluster with IO-Optimized storage (aurora-iopt1). The benchmark client is provisioned separately via `common/client.py`.

### Features

- **IO-Optimized storage**: aurora-iopt1 for consistent throughput
- **Custom workloads**: IUD and mixed Lua scripts from common/lua/ matching production ratios
- **Fill phase**: INSERT...SELECT doubling to fill buffer pool to N x instance RAM
- **Snapshot/restore**: Create cluster snapshots after fill, restore to skip re-filling
- **Parallel sysbench**: Run multiple sysbench processes concurrently
- **InnoDB row counter measurement**: Tracks actual insert/update/delete/read rates
- **CloudWatch integration**: Aurora CPU, network throughput, IOPS, latency metrics
- **Production baseline comparison**: Automatic comparison against known production metrics
- **Dual AWS profile support**: Separate profiles for EC2/VPC and RDS operations

### Quick Start

```bash
# 1. Provision Aurora cluster (default: db.r8g.xlarge, IO-Optimized)
python3 -m aurora.setup --seed auroralt-001 --aws-profile sandbox

# 2. Provision benchmark client in the Aurora VPC
python3 -m common.client --seed auroralt-001 --server-type aurora --size small

# 3. Benchmark (custom mixed workload, 64 threads, 5 minutes)
python3 -m common.benchmark --server-type aurora --seed auroralt-001 --aws-profile sandbox

# Benchmark with parallel sysbench processes
python3 -m common.benchmark --server-type aurora --parallel 4 --threads 64 --seed auroralt-001

# Fill phase (create background data to pressure buffer pool)
python3 -m common.benchmark --server-type aurora --fill --seed auroralt-001 --aws-profile sandbox

# Legacy entry point still works
python3 -m aurora.benchmark --seed auroralt-001 --aws-profile sandbox

# Snapshot after fill
python3 -m aurora.setup --snapshot --seed auroralt-001

# Restore from snapshot
python3 -m aurora.setup --restore-snapshot <snapshot-id> --seed auroralt-002

# Cleanup (server and client separately)
python3 -m common.client --cleanup --seed auroralt-001 --server-type aurora
python3 -m aurora.setup --cleanup --seed auroralt-001 --aws-profile sandbox
```

### Additional Tools

- **aurora/modify_instance.sh** -- Live Aurora instance type modification

## TiDB

Provisions a multi-AZ TiDB cluster on EC2 via k3s and TiDB Operator, with optional TiCDC replication to a downstream cluster.

### Features

- **Multi-AZ**: 3 availability zones by default (1 leader + 2 replicas per TiKV raft group)
- **Dedicated VMs**: Each TiKV pod consumes the entire EC2 instance
- **TiCDC replication**: Deploys upstream + downstream clusters with changefeed lag measurement
- **Benchmark profiles**: quick, light, medium, heavy, standard, stress, scaling
- **Workloads**: All standard sysbench workloads plus custom IUD and mixed from common/lua/

### Quick Start

```bash
# 1. Provision TiDB cluster (default: 3 PD, 3 TiKV, 2 TiDB across 3 AZs)
AWS_PROFILE=sandbox python3 -m tidb.setup

# With TiCDC replication
python3 -m tidb.setup --aws-profile sandbox --ticdc

# 2. Provision benchmark client in the TiDB VPC
python3 -m common.client --seed <seed> --server-type tidb --size small

# Validate
AWS_PROFILE=sandbox python3 -m tidb.validate

# 3. Benchmark (standard profile, 5 minutes) -- unified CLI
python3 -m common.benchmark --server-type tidb --profile standard --aws-profile sandbox

# Benchmark with TiCDC lag measurement
python3 -m common.benchmark --server-type tidb --profile standard --ticdc --aws-profile sandbox

# Legacy entry point still works
python3 -m tidb.benchmark --profile standard --aws-profile sandbox

# Cleanup (server and client separately)
python3 -m common.client --cleanup --seed <seed> --server-type tidb
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
# 1. Provision Valkey cluster + bastion
AWS_PROFILE=sandbox python3 -m valkey.setup \
  --region us-east-1 \
  --seed vlklt-001 \
  --ssh-private-key-path ./valkey-load-test-key.pem

# 2. Provision benchmark client in the Valkey VPC
python3 -m common.client --seed vlklt-001 --server-type valkey --size small

# Validate
AWS_PROFILE=sandbox python3 -m valkey.validate \
  --ssh-private-key-path ./valkey-load-test-key.pem

# 3. Benchmark with FlameGraph
AWS_PROFILE=sandbox python3 -m valkey.benchmark \
  --ssh-host <CLIENT_IP> \
  --ssh-user ec2-user \
  --ssh-key ./valkey-load-test-key.pem \
  --mode proxy

# Cleanup (server and client separately)
python3 -m common.client --cleanup --seed vlklt-001 --server-type valkey
python3 -m valkey.setup --cleanup \
  --seed vlklt-001 \
  --ssh-private-key-path ./valkey-load-test-key.pem
```

## Prerequisites

- Python 3.9+ with `boto3`
- AWS CLI v2 configured with a profile pointing at the target account
- SSH key pair uploaded to EC2 (see individual module quick starts)

## Running

All modules are designed to run from the repo root using `python3 -m`:

```bash
# Unified benchmark CLI (Aurora + TiDB)
python3 -m common.benchmark --help

# Setup commands
python3 -m tidb.setup --help
python3 -m valkey.setup --help
python3 -m aurora.setup --help

# Valkey benchmark (separate tool)
python3 -m valkey.benchmark --help
```

## References

- [TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [TiCDC Architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [Valkey Documentation](https://valkey.io/docs/)
- [Envoy Proxy](https://www.envoyproxy.io/docs/)
- [Aurora MySQL Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMySQL.html)
- [sysbench](https://github.com/akopytov/sysbench)
