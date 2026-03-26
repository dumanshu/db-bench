#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
TiDB Load Test Stack Provisioner

Provisions AWS infrastructure and bootstraps a TiDB cluster using TiDB Operator
on a multi-node k3s cluster across dedicated EC2 instances.

Architecture:
- 1 control EC2 (k3s server / control plane)
- 3 PD EC2 instances (k3s agents, labeled for PD pods)
- 3 TiKV EC2 instances (k3s agents, labeled for TiKV pods)
- 2 TiDB EC2 instances (k3s agents, labeled for TiDB pods)
- All nodes in a single VPC/subnet with self-referencing SG for intra-cluster traffic
"""

import argparse
import base64
import botocore
import os
import subprocess
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import common.util as _cu
import common.aws as _caws
from common.util import (
    ts, log, need_cmd, my_public_cidr, aws_session, ec2, ssm, tags_common,
    configure_from_args as _common_configure_from_args,
)
from common.types import InstanceInfo
from common.aws import (
    describe_instance, get_vpcs, get_igw_for_vpc, ensure_igw,
    find_subnet, ensure_subnet, find_rtb_by_name, ensure_public_rtb,
    ensure_sg, ensure_ingress_tcp_cidr, refresh_ssh_rule,
    find_instance_id_by_name, find_all_stack_instances, wait_running,
    ensure_instance as _common_ensure_instance,
    instance_info_from_id,
    terminate_stack_instances, delete_stack_security_groups,
    delete_route_table_by_name, delete_stack_subnets,
    delete_stack_igw_and_vpc, cleanup_stack,
    ensure_vpc as _common_ensure_vpc,
)

SEED = "tidblt-001"

# Default instance type for control VM (k3s server / control plane)
# c8g.4xlarge provides 16 vCPU, 32GB RAM (Graviton4) - enough for control plane
CONTROL_INSTANCE_TYPE = "c8g.4xlarge"

# PingCAP recommended production instance types (AWS Graviton4)
# https://docs.pingcap.com/tidb/stable/hardware-and-software-requirements
# PD: 8+ vCPU, 16+ GB | TiDB: 16+ vCPU, 48+ GB | TiKV: 16+ vCPU, 64+ GB
# TiCDC: 16+ vCPU, 64+ GB
PRODUCTION_INSTANCE_TYPES = {
    "pd": "c8g.2xlarge",      # 8 vCPU, 16GB - meets PingCAP 8 core+/16GB+ req
    "tidb": "m8g.4xlarge",    # 16 vCPU, 64GB - meets PingCAP 16 core+/48GB+ req
    "tikv": "m8g.4xlarge",    # 16 vCPU, 64GB - meets PingCAP 16 core+/64GB+ req
    "ticdc": "m8g.4xlarge",   # 16 vCPU, 64GB - meets PingCAP 16 core+/64GB+ req
    "control": "c8g.4xlarge", # 16 vCPU, 32GB - control plane
    "downstream-pd": "c8g.2xlarge",
    "downstream-tidb": "m8g.4xlarge",
    "downstream-tikv": "m8g.4xlarge",
}

# Cost-optimized instance types for testing/benchmarking (Graviton4)
BENCHMARK_INSTANCE_TYPES = {
    "pd": "c8g.large",       # 2 vCPU, 4GB
    "tidb": "c8g.2xlarge",   # 8 vCPU, 16GB
    "tikv": "m8g.2xlarge",   # 8 vCPU, 32GB
    "ticdc": "c8g.2xlarge",  # 8 vCPU, 16GB
    "control": "c8g.2xlarge", # 8 vCPU, 16GB
    "downstream-pd": "c8g.large",
    "downstream-tidb": "c8g.2xlarge",
    "downstream-tikv": "m8g.2xlarge",
}

# EBS gp3 storage recommendations from PingCAP
# https://docs.pingcap.com/tidb-in-kubernetes/stable/configure-storage-class
EBS_CONFIG = {
    "tikv": {
        "type": "gp3",
        "iops": 4000,
        "throughput": 400,  # MiB/s
        "size_gb": 500,
    },
    "pd": {
        "type": "gp3",
        "iops": 3000,
        "throughput": 125,  # MiB/s
        "size_gb": 50,
    },
    "ticdc": {
        "type": "gp3",
        "iops": 3000,
        "throughput": 250,  # MiB/s - TiCDC sort-dir I/O
        "size_gb": 500,
    },
}

# Root volume sizes per role (GB)
ROOT_VOLUME_SIZES = {
    "control": 100,
    "pd": 50,
    "tikv": 500,
    "tidb": 50,
    "ticdc": 500,
    "downstream-pd": 50,
    "downstream-tikv": 500,
    "downstream-tidb": 50,
}

# Multi-AZ configuration
AVAILABILITY_ZONES = ["us-east-1a", "us-east-1b", "us-east-1c"]

# Per-AZ subnet CIDRs (one public subnet per AZ for true multi-AZ)
SUBNET_CIDRS = {
    "us-east-1a": "10.43.1.0/24",
    "us-east-1b": "10.43.2.0/24",
    "us-east-1c": "10.43.3.0/24",
}

AMI_OVERRIDE = os.environ.get("TIDB_AMI_ID")
AMI_SSM_PARAM = os.environ.get(
    "TIDB_AMI_PARAM",
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
)
_RESOLVED_AMI_ID = None

KEY_NAME = "tidb-load-test-key"
DEFAULT_SSH_KEY_PATH = Path(__file__).resolve().with_name("tidb-load-test-key.pem")

VPC_CIDR = "10.43.0.0/16"
PUB_CIDR = "10.43.1.0/24"

TIDB_PORT = 4000

# k3s ports for inter-node communication
K3S_API_PORT = 6443
K3S_FLANNEL_PORT = 8472  # UDP VXLAN
KUBELET_PORT = 10250

TIDB_OPERATOR_VERSION = os.environ.get("TIDB_OPERATOR_VERSION", "v1.6.5")
TIDB_VERSION = os.environ.get("TIDB_VERSION", "v8.5.5")
HELM_VERSION = os.environ.get("HELM_VERSION", "v3.20.1")
KUBECTL_VERSION = os.environ.get("KUBECTL_VERSION", "v1.35.3")

# All roles that get their own EC2 instances
COMPONENT_ROLES = ["pd", "tikv", "tidb"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Provision TiDB load test stack on multi-node k3s cluster."
    )
    parser.add_argument("--region", default=_cu.REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Unique seed used in stack name.")
    parser.add_argument("--owner", default=os.environ.get("OWNER", ""), help="Owner tag value.")
    parser.add_argument(
        "--ssh-private-key-path",
        dest="ssh_key_path",
        default=str(DEFAULT_SSH_KEY_PATH),
        help=f"Path to SSH private key (.pem) (default: {DEFAULT_SSH_KEY_PATH}).",
    )
    parser.add_argument("--ssh-cidr", help="CIDR allowed for SSH (default: detected public IP /32).")
    parser.add_argument("--aws-profile", help="AWS named profile (default: sandbox or $AWS_PROFILE).")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Provision infrastructure only.")
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources.")
    parser.add_argument(
        "--tidb-version",
        default=TIDB_VERSION,
        help=f"TiDB version to deploy (default: {TIDB_VERSION}).",
    )
    parser.add_argument(
        "--pd-replicas",
        type=int,
        default=3,
        help="Number of PD replicas (default: 3 for multi-AZ).",
    )
    parser.add_argument(
        "--tikv-replicas",
        type=int,
        default=3,
        help="Number of TiKV replicas (default: 3 for multi-AZ).",
    )
    parser.add_argument(
        "--tidb-replicas",
        type=int,
        default=2,
        help="Number of TiDB replicas (default: 2 for HA).",
    )
    parser.add_argument(
        "--multi-az",
        action="store_true",
        default=True,
        help="Deploy across 3 AZs for HA (1 node per AZ minimum). Default: enabled.",
    )
    parser.add_argument(
        "--single-az",
        action="store_true",
        help="Disable multi-AZ deployment (single zone, sets replicas to 1).",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        default=True,
        help="Use PingCAP recommended production instance types. Default: enabled.",
    )
    parser.add_argument(
        "--benchmark-mode",
        action="store_true",
        help="Use cost-optimized instance types for benchmarking.",
    )
    parser.add_argument(
        "--client-zone",
        default="us-east-1a",
        help="AZ for control VM; leaders prefer this zone (default: us-east-1a).",
    )
    parser.add_argument(
        "--ticdc",
        action="store_true",
        help="Enable TiCDC replication mode: deploy downstream cluster + changefeed.",
    )
    parser.add_argument(
        "--ticdc-replicas",
        type=int,
        default=1,
        help="Number of TiCDC replicas (default: 1).",
    )
    parser.add_argument(
        "--downstream-pd-replicas",
        type=int,
        default=3,
        help="PD replicas for downstream cluster (default: 3).",
    )
    parser.add_argument(
        "--downstream-tikv-replicas",
        type=int,
        default=3,
        help="TiKV replicas for downstream cluster (default: 3).",
    )
    parser.add_argument(
        "--downstream-tidb-replicas",
        type=int,
        default=1,
        help="TiDB replicas for downstream cluster (default: 1).",
    )
    return parser


def configure_from_args(args):
    """Configure runtime from CLI args, then fix common.aws stale bindings."""
    _common_configure_from_args(args, "tidb-loadtest")
    # common.aws imported STACK/REGION by value; patch after configure updates them
    _caws.STACK = _cu.STACK
    _caws.REGION = _cu.REGION


def resolved_ami_id():
    global _RESOLVED_AMI_ID
    if AMI_OVERRIDE:
        return AMI_OVERRIDE
    if _RESOLVED_AMI_ID:
        return _RESOLVED_AMI_ID
    try:
        param = ssm().get_parameter(Name=AMI_SSM_PARAM)
        value = param["Parameter"]["Value"]
        _RESOLVED_AMI_ID = value
        log(f"Using Amazon Linux 2023 AMI via {AMI_SSM_PARAM}: {value}")
        return value
    except botocore.exceptions.ClientError as exc:
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        raise SystemExit(
            f"ERROR: Unable to resolve AMI from SSM ({AMI_SSM_PARAM}): {msg}. Set TIDB_AMI_ID to override."
        )


def ensure_keypair_accessible():
    try:
        ec2().describe_key_pairs(KeyNames=[KEY_NAME])
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidKeyPair.NotFound":
            raise SystemExit(
                f"ERROR: KeyPair '{KEY_NAME}' not found in EC2. "
                "Create it with: aws ec2 import-key-pair --key-name tidb-load-test-key "
                "--public-key-material fileb://tidb-load-test-key.pub --profile sandbox"
            )
        raise


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    jump_host: InstanceInfo
    pd_nodes: List[InstanceInfo] = field(default_factory=list)
    tikv_nodes: List[InstanceInfo] = field(default_factory=list)
    tidb_nodes: List[InstanceInfo] = field(default_factory=list)
    ticdc_nodes: List[InstanceInfo] = field(default_factory=list)
    downstream_pd_nodes: List[InstanceInfo] = field(default_factory=list)
    downstream_tikv_nodes: List[InstanceInfo] = field(default_factory=list)
    downstream_tidb_nodes: List[InstanceInfo] = field(default_factory=list)
    tidb_version: str = TIDB_VERSION
    pd_replicas: int = 3
    tikv_replicas: int = 3
    tidb_replicas: int = 2
    ticdc_replicas: int = 0
    enable_ticdc: bool = False
    downstream_pd_replicas: int = 3
    downstream_tikv_replicas: int = 3
    downstream_tidb_replicas: int = 1
    multi_az: bool = False
    production: bool = False
    client_zone: str = "us-east-1a"

    @property
    def host(self) -> InstanceInfo:
        return self.jump_host

    @property
    def all_agent_nodes(self) -> List[InstanceInfo]:
        nodes = self.pd_nodes + self.tikv_nodes + self.tidb_nodes + self.ticdc_nodes
        nodes += self.downstream_pd_nodes + self.downstream_tikv_nodes + self.downstream_tidb_nodes
        return nodes

    @property
    def all_nodes(self) -> List[InstanceInfo]:
        return [self.jump_host] + self.all_agent_nodes


# ---------------------------------------------------------------------------
# EC2 / Instance helpers
# ---------------------------------------------------------------------------

def ssh_base_cmd(host: InstanceInfo, ctx: BootstrapContext):
    """Build SSH command for a node.
    
    For the control node: direct SSH via public IP.
    For agent nodes: SSH via ProxyJump through control (private IP), since
    some AWS public IPs may not be routable from corporate VPNs.
    """
    is_agent = host.role != "control"
    if is_agent and ctx.jump_host and ctx.jump_host.public_ip:
        # Use ProxyJump through control to reach agent via private IP
        proxy = f"ec2-user@{ctx.jump_host.public_ip}"
        target_ip = host.private_ip
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "IdentitiesOnly=yes",
            "-o", "ConnectTimeout=30",
            "-i", str(ctx.ssh_key_path),
            "-o", f"ProxyCommand=ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -i {ctx.ssh_key_path} -W %h:%p {proxy}",
            f"ec2-user@{target_ip}",
            "bash", "-s"
        ]
    else:
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "IdentitiesOnly=yes",
            "-o", "ConnectTimeout=30",
            "-i", str(ctx.ssh_key_path),
            f"ec2-user@{host.public_ip}",
            "bash", "-s"
        ]
    return cmd


def ssh_run(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    subprocess.run(cmd, input=full_script, text=True, check=True)


def ssh_capture(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    result = subprocess.run(cmd, input=full_script, text=True, capture_output=True)
    if strict:
        result.check_returncode()
    return result


def ensure_vpc():
    """Wrapper: pass tidb-specific VPC CIDR to common ensure_vpc."""
    return _common_ensure_vpc(VPC_CIDR)


# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

def ensure_intra_cluster_rules(sg_id):
    """Allow all traffic between instances in the same security group.

    This is needed for k3s API (6443), kubelet (10250), flannel VXLAN (8472/UDP),
    TiDB/TiKV/PD inter-node communication, and CoreDNS.
    """
    try:
        ec2().authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "-1",  # All traffic
                "UserIdGroupPairs": [{"GroupId": sg_id}]
            }]
        )
        log(f"CREATED intra-cluster SG rule (self-referencing): {sg_id}")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            log(f"REUSED  intra-cluster SG rule: {sg_id}")
            return False
        raise


# ---------------------------------------------------------------------------
# EC2 Instances
# ---------------------------------------------------------------------------

def ensure_instance(name, role, itype, subnet_id, sg_id, root_volume_size=100):
    """Wrapper: delegate to common ensure_instance with tidb-specific AMI/key."""
    return _common_ensure_instance(
        name, role, itype, subnet_id, sg_id,
        resolved_ami_id, KEY_NAME, ensure_keypair_accessible,
        root_volume_size,
    )


def provision_role_instances(role, count, instance_types, subnet_ids, sg_id, production):
    """Provision `count` EC2 instances for a given role, round-robin across subnet_ids."""
    if isinstance(subnet_ids, str):
        subnet_ids = [subnet_ids]
    types = PRODUCTION_INSTANCE_TYPES if production else BENCHMARK_INSTANCE_TYPES
    itype = types.get(role, instance_types.get(role, CONTROL_INSTANCE_TYPE))
    vol_size = ROOT_VOLUME_SIZES.get(role, 100)
    ids = []
    for i in range(1, count + 1):
        subnet = subnet_ids[(i - 1) % len(subnet_ids)]
        name = f"{_cu.STACK}-{role}-{i}"
        iid = ensure_instance(name, role, itype, subnet, sg_id, vol_size)
        ids.append(iid)
    return ids


# ---------------------------------------------------------------------------
# Software Installation (shared across nodes)
# ---------------------------------------------------------------------------

def install_base_packages(host: InstanceInfo, ctx: BootstrapContext):
    """Install base OS packages and tune sysctl on any node."""
    log(f"Installing base packages on {host.role} ({host.public_ip})")
    ssh_run(host, """
sudo dnf -y update || true
sudo dnf -y install jq htop sysstat mtr || true

# mysql client needed on control node for create_sysbench_database()
if ! command -v mysql &>/dev/null; then
    sudo dnf -y install mariadb105 2>/dev/null || \
    sudo dnf -y install mariadb 2>/dev/null || \
    echo "WARNING: mysql client not installed"
fi

sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null <<'EOF'
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 512
EOF

sudo tee /etc/sysctl.d/99-tidb-bench.conf >/dev/null <<'EOF'
# File descriptor limits (nr_open must be >= k3s LimitNOFILE=1048576)
fs.file-max = 1048576
fs.nr_open = 1048576

# Network tuning for high-throughput benchmarks
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 1024 65535

# Perf profiling (for flamegraphs)
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF

sudo tee /etc/security/limits.d/99-tidb-bench.conf >/dev/null <<'EOF'
* soft nofile 1000000
* hard nofile 1000000
* soft nproc 65535
* hard nproc 65535
root soft nofile 1000000
root hard nofile 1000000
EOF

sudo sysctl --system || true
""", ctx)


# ---------------------------------------------------------------------------
# k3s Installation (replaces kind + Docker)
# ---------------------------------------------------------------------------

def install_k3s_server(control: InstanceInfo, ctx: BootstrapContext):
    """Install k3s in server (control-plane) mode on the control node.

    Disables traefik, servicelb, and local-storage (not needed for TiDB).
    Taints the control-plane node so TiDB pods don't schedule on it.
    """
    log(f"Installing k3s server on control ({control.public_ip})")
    ssh_run(control, f"""
# Check if k3s is already running
if sudo systemctl is-active k3s >/dev/null 2>&1; then
    echo "k3s server already running"
    sudo k3s kubectl get nodes || true
    exit 0
fi

curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --disable=traefik,servicelb,local-storage \
  --tls-san={control.private_ip} \
  --tls-san={control.public_ip} \
  --node-taint=node-role.kubernetes.io/control-plane:NoSchedule \
  --write-kubeconfig-mode=644" sh -

# Wait for k3s to be ready
for i in $(seq 1 30); do
    if sudo k3s kubectl get nodes >/dev/null 2>&1; then
        echo "k3s server ready"
        break
    fi
    echo "Waiting for k3s server... (attempt $i/30)"
    sleep 5
done

# Set up kubeconfig for ec2-user
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
chmod 600 ~/.kube/config

# Verify
kubectl get nodes
echo "k3s server installed successfully"
""", ctx)


def get_k3s_token(control: InstanceInfo, ctx: BootstrapContext) -> str:
    """Retrieve the k3s node-token from the server node."""
    result = ssh_capture(control, """
sudo cat /var/lib/rancher/k3s/server/node-token
""", ctx)
    return result.stdout.strip()


def install_k3s_agent(agent: InstanceInfo, server_ip: str, token: str, ctx: BootstrapContext):
    """Install k3s agent on a worker node and join it to the cluster."""
    log(f"Joining k3s agent {agent.role} ({agent.public_ip}) to server {server_ip}")
    ssh_run(agent, f"""
# Check if k3s-agent is already running
if sudo systemctl is-active k3s-agent >/dev/null 2>&1; then
    echo "k3s agent already running on {agent.role}"
    exit 0
fi

curl -sfL https://get.k3s.io | K3S_URL="https://{server_ip}:6443" \
  K3S_TOKEN="{token}" sh -

# Wait for agent to register
for i in $(seq 1 20); do
    if sudo systemctl is-active k3s-agent >/dev/null 2>&1; then
        echo "k3s agent started on {agent.role}"
        break
    fi
    echo "Waiting for k3s agent... (attempt $i/20)"
    sleep 5
done
""", ctx)


def label_and_taint_nodes(ctx: BootstrapContext):
    """Label and taint all agent nodes from the k3s server.

    This ensures TiDB Operator schedules each component to the correct node.
    """
    log("Labeling and tainting k3s agent nodes for TiDB scheduling")

    # Wait for all agents to register
    total_agents = len(ctx.all_agent_nodes)
    ssh_run(ctx.jump_host, f"""
echo "Waiting for {total_agents} agent nodes to register..."
for i in $(seq 1 60); do
    COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)
    # Total = agents + 1 server
    if [ "$COUNT" -ge "{total_agents + 1}" ]; then
        echo "All {total_agents + 1} nodes registered"
        break
    fi
    echo "Nodes registered: $COUNT/{total_agents + 1} (attempt $i/60)"
    sleep 10
done
kubectl get nodes -o wide
""", ctx)

    # Label and taint each node by matching on private IP (k3s uses the IP as node name)
    for node in ctx.pd_nodes:
        _label_taint_node(ctx, node.private_ip, "pd")
    for node in ctx.tikv_nodes:
        _label_taint_node(ctx, node.private_ip, "tikv")
    for node in ctx.tidb_nodes:
        _label_taint_node(ctx, node.private_ip, "tidb")
    for node in ctx.ticdc_nodes:
        _label_taint_node(ctx, node.private_ip, "ticdc")
    for node in ctx.downstream_pd_nodes:
        _label_taint_node(ctx, node.private_ip, "downstream-pd")
    for node in ctx.downstream_tikv_nodes:
        _label_taint_node(ctx, node.private_ip, "downstream-tikv")
    for node in ctx.downstream_tidb_nodes:
        _label_taint_node(ctx, node.private_ip, "downstream-tidb")


def _label_taint_node(ctx: BootstrapContext, node_ip: str, component: str):
    """Label and taint a single k3s node by its IP address."""
    ssh_run(ctx.jump_host, f"""
# Find the node name by IP
NODE_NAME=$(kubectl get nodes -o jsonpath='{{.items[?(@.status.addresses[?(@.type=="InternalIP")].address=="{node_ip}")].metadata.name}}')
if [ -z "$NODE_NAME" ]; then
    echo "WARNING: Could not find node with IP {node_ip}, trying hostname-based lookup..."
    NODE_NAME=$(kubectl get nodes -o wide --no-headers | grep "{node_ip}" | awk '{{print $1}}')
fi
if [ -z "$NODE_NAME" ]; then
    echo "ERROR: Could not find node with IP {node_ip}"
    exit 1
fi

echo "Labeling node $NODE_NAME as {component} (IP: {node_ip})"
kubectl label node "$NODE_NAME" tidb.pingcap.com/{component}="" --overwrite
kubectl taint node "$NODE_NAME" tidb.pingcap.com/{component}:NoSchedule --overwrite 2>/dev/null || \
    kubectl taint node "$NODE_NAME" tidb.pingcap.com/{component}=:NoSchedule 2>/dev/null || true

# Also add a zone label based on the node's AZ for topology-aware scheduling
kubectl label node "$NODE_NAME" topology.kubernetes.io/zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone 2>/dev/null || echo "unknown") --overwrite 2>/dev/null || true
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Local-path storage provisioner (k3s local-storage is disabled)
# ---------------------------------------------------------------------------

LOCAL_PATH_PROVISIONER_URL = (
    "https://raw.githubusercontent.com/rancher/local-path-provisioner"
    "/v0.0.30/deploy/local-path-storage.yaml"
)


def install_local_path_provisioner(host: InstanceInfo, ctx: BootstrapContext):
    log("Installing local-path-provisioner for PV storage")
    ssh_run(host, f"""
if kubectl get sc local-path >/dev/null 2>&1; then
    echo "local-path StorageClass already exists"
    exit 0
fi

kubectl apply -f {LOCAL_PATH_PROVISIONER_URL}

kubectl patch storageclass local-path -p \
    '{{"metadata":{{"annotations":{{"storageclass.kubernetes.io/is-default-class":"true"}}}}}}'

kubectl patch deployment local-path-provisioner -n local-path-storage --type='json' \
    -p='[{{"op":"add","path":"/spec/template/spec/tolerations","value":[{{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}}]}}]'

for i in $(seq 1 30); do
    READY=$(kubectl get pods -n local-path-storage \
        -l app.kubernetes.io/name=local-path-provisioner \
        -o jsonpath='{{.items[0].status.containerStatuses[0].ready}}' 2>/dev/null || echo "false")
    if [ "$READY" = "true" ]; then
        echo "local-path-provisioner is ready"
        break
    fi
    echo "Waiting for local-path-provisioner... (attempt $i/30)"
    sleep 5
done

kubectl get sc
""", ctx)


def label_control_plane_zone(host: InstanceInfo, ctx: BootstrapContext):
    log("Labeling control-plane node with availability zone")
    ssh_run(host, f"""
CP_NODE=$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{{.items[0].metadata.name}}')
ZONE=$(kubectl get node $CP_NODE -o jsonpath='{{.metadata.labels.topology\\.kubernetes\\.io/zone}}' 2>/dev/null || echo "")
if [ -n "$ZONE" ]; then
    echo "Control-plane node already has zone label: $ZONE"
    exit 0
fi
kubectl label node $CP_NODE topology.kubernetes.io/zone={AVAILABILITY_ZONES[0]}
echo "Labeled $CP_NODE with zone {AVAILABILITY_ZONES[0]}"
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Helm + TiDB Operator
# ---------------------------------------------------------------------------

def install_helm(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing Helm on {host.role}")
    ssh_run(host, f"""
if command -v helm >/dev/null 2>&1; then
    echo "Helm already installed"
    exit 0
fi

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
elif [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
fi

curl -fsSL https://get.helm.sh/helm-{HELM_VERSION}-linux-$ARCH.tar.gz | tar xz
sudo mv linux-$ARCH/helm /usr/local/bin/
rm -rf linux-$ARCH
helm version || true
""", ctx)


def install_tidb_operator(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing TiDB Operator {TIDB_OPERATOR_VERSION} on {host.role}")
    ssh_run(host, f"""
kubectl create -f https://raw.githubusercontent.com/pingcap/tidb-operator/{TIDB_OPERATOR_VERSION}/manifests/crd.yaml || true

helm repo add pingcap https://charts.pingcap.com/ || true
helm repo update

kubectl create namespace tidb-admin || true

if helm status tidb-operator -n tidb-admin >/dev/null 2>&1; then
    echo "TiDB Operator already installed"
else
    helm install --namespace tidb-admin tidb-operator pingcap/tidb-operator --version {TIDB_OPERATOR_VERSION} \
        --set controllerManager.tolerations[0].key=node-role.kubernetes.io/control-plane \
        --set controllerManager.tolerations[0].effect=NoSchedule \
        --set controllerManager.tolerations[0].operator=Exists \
        --set scheduler.tolerations[0].key=node-role.kubernetes.io/control-plane \
        --set scheduler.tolerations[0].effect=NoSchedule \
        --set scheduler.tolerations[0].operator=Exists \
        --timeout 10m --wait
fi

kubectl get pods --namespace tidb-admin -l app.kubernetes.io/instance=tidb-operator
""", ctx)


# ---------------------------------------------------------------------------
# TiDB Cluster Deployment (with nodeSelector + tolerations for multi-node)
# ---------------------------------------------------------------------------

def deploy_tidb_cluster(host: InstanceInfo, ctx: BootstrapContext):
    """Deploy TiDB cluster with nodeSelector/tolerations for dedicated hosts."""
    log(f"Deploying TiDB cluster {ctx.tidb_version} with multi-node scheduling")

    pd_config_block = ""
    tikv_extra_config = ""

    if ctx.multi_az:
        # PD config is TOML format -- use proper TOML syntax with quoted strings
        pd_config_block = (
            '      [replication]\n'
            '      location-labels = ["topology.kubernetes.io/zone", "kubernetes.io/hostname"]\n'
            '      max-replicas = 3\n'
            '      enable-placement-rules = true'
        )

    # Build the YAML as a plain Python string (no f-string for the YAML body)
    # to avoid brace-escaping issues with YAML/JSON syntax
    yaml_lines = []
    yaml_lines.append('apiVersion: pingcap.com/v1alpha1')
    yaml_lines.append('kind: TidbCluster')
    yaml_lines.append('metadata:')
    yaml_lines.append('  name: basic')
    yaml_lines.append('  namespace: tidb-cluster')
    yaml_lines.append('spec:')
    yaml_lines.append(f'  version: {ctx.tidb_version}')
    yaml_lines.append('  timezone: UTC')
    yaml_lines.append('  pvReclaimPolicy: Retain')
    yaml_lines.append('  enableDynamicConfiguration: true')
    yaml_lines.append('  configUpdateStrategy: RollingUpdate')
    yaml_lines.append('  discovery:')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: node-role.kubernetes.io/control-plane')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('  helper:')
    yaml_lines.append('    image: alpine:3.16.0')
    if ctx.multi_az:
        yaml_lines.append('  topologySpreadConstraints:')
        yaml_lines.append('    - topologyKey: topology.kubernetes.io/zone')
        yaml_lines.append('      maxSkew: 1')
    yaml_lines.append('  pd:')
    yaml_lines.append('    baseImage: pingcap/pd')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.pd_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "10Gi"')
    if pd_config_block:
        yaml_lines.append('    config: |')
        yaml_lines.append(pd_config_block)
    else:
        yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/pd: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/pd')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        requiredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - labelSelector:')
    yaml_lines.append('              matchExpressions:')
    yaml_lines.append('                - key: app.kubernetes.io/component')
    yaml_lines.append('                  operator: In')
    yaml_lines.append('                  values:')
    yaml_lines.append('                    - pd')
    yaml_lines.append('            topologyKey: kubernetes.io/hostname')
    # TiKV
    yaml_lines.append('  tikv:')
    yaml_lines.append('    baseImage: pingcap/tikv')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append('    evictLeaderTimeout: 1m')
    yaml_lines.append(f'    replicas: {ctx.tikv_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "50Gi"')
    yaml_lines.append('    config: |')
    yaml_lines.append('      [storage]')
    yaml_lines.append('      reserve-space = "0MB"')
    yaml_lines.append('      [rocksdb]')
    yaml_lines.append('      max-open-files = 256')
    yaml_lines.append('      [raftdb]')
    yaml_lines.append('      max-open-files = 256')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/tikv: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/tikv')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        requiredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - labelSelector:')
    yaml_lines.append('              matchExpressions:')
    yaml_lines.append('                - key: app.kubernetes.io/component')
    yaml_lines.append('                  operator: In')
    yaml_lines.append('                  values:')
    yaml_lines.append('                    - tikv')
    yaml_lines.append('            topologyKey: kubernetes.io/hostname')
    # TiDB
    yaml_lines.append('  tidb:')
    yaml_lines.append('    baseImage: pingcap/tidb')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.tidb_replicas}')
    yaml_lines.append('    service:')
    yaml_lines.append('      type: NodePort')
    yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/tidb: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/tidb')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        preferredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - weight: 100')
    yaml_lines.append('            podAffinityTerm:')
    yaml_lines.append('              labelSelector:')
    yaml_lines.append('                matchExpressions:')
    yaml_lines.append('                  - key: app.kubernetes.io/component')
    yaml_lines.append('                    operator: In')
    yaml_lines.append('                    values:')
    yaml_lines.append('                      - tidb')
    yaml_lines.append('              topologyKey: kubernetes.io/hostname')
    if ctx.enable_ticdc:
        yaml_lines.append('  ticdc:')
        yaml_lines.append('    baseImage: pingcap/ticdc')
        yaml_lines.append(f'    replicas: {ctx.ticdc_replicas}')
        yaml_lines.append('    config: |')
        yaml_lines.append('      newarch = true')
        yaml_lines.append('      gc-ttl = 86400')
        yaml_lines.append('    requests:')
        yaml_lines.append('      storage: "500Gi"')
        yaml_lines.append('    nodeSelector:')
        yaml_lines.append('      tidb.pingcap.com/ticdc: ""')
        yaml_lines.append('    tolerations:')
        yaml_lines.append('      - key: tidb.pingcap.com/ticdc')
        yaml_lines.append('        operator: Exists')
        yaml_lines.append('        effect: NoSchedule')
        yaml_lines.append('    affinity:')
        yaml_lines.append('      podAntiAffinity:')
        yaml_lines.append('        preferredDuringSchedulingIgnoredDuringExecution:')
        yaml_lines.append('          - weight: 100')
        yaml_lines.append('            podAffinityTerm:')
        yaml_lines.append('              labelSelector:')
        yaml_lines.append('                matchExpressions:')
        yaml_lines.append('                  - key: app.kubernetes.io/component')
        yaml_lines.append('                    operator: In')
        yaml_lines.append('                    values:')
        yaml_lines.append('                      - ticdc')
        yaml_lines.append('              topologyKey: kubernetes.io/hostname')

    yaml_str = '\n'.join(yaml_lines)

    # Write YAML via base64 to avoid shell quoting issues
    yaml_b64 = base64.b64encode(yaml_str.encode()).decode()

    ssh_run(host, f"""
kubectl create namespace tidb-cluster || true

echo '{yaml_b64}' | base64 -d > /tmp/tidb-cluster.yaml
kubectl apply -f /tmp/tidb-cluster.yaml -n tidb-cluster

echo "Waiting for TiDB service to be created..."
for i in $(seq 1 30); do
    if kubectl get svc basic-tidb -n tidb-cluster >/dev/null 2>&1; then
        kubectl patch svc basic-tidb -n tidb-cluster --type='json' \\
            -p='[{{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}}]' || true
        break
    fi
    sleep 2
done
""", ctx)


def wait_for_tidb_ready(host: InstanceInfo, ctx: BootstrapContext):
    log("Waiting for TiDB cluster to be ready...")
    ssh_run(host, """
echo "Waiting for TiDB pods to be ready (this may take 5-10 minutes)..."

for i in $(seq 1 60); do
    READY=$(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=tidb \
        -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
    if [ "$READY" = "true" ]; then
        echo "TiDB is ready!"
        break
    fi
    echo "Waiting for TiDB... (attempt $i/60)"
    kubectl get pods -n tidb-cluster 2>/dev/null || true
    sleep 10
done

kubectl get pods -n tidb-cluster
kubectl get svc -n tidb-cluster

# Ensure NodePort is set correctly (operator may have reverted it)
kubectl patch svc basic-tidb -n tidb-cluster --type='json' \
    -p='[{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}]' 2>/dev/null || true
echo "TiDB NodePort set to 30400"
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Leader zone affinity
# ---------------------------------------------------------------------------

def configure_leader_zone_affinity(host: InstanceInfo, ctx: BootstrapContext):
    """Configure placement rules to prefer leaders in the client zone."""
    if not ctx.multi_az:
        log("Skipping leader zone affinity (single-AZ deployment)")
        return

    log(f"Configuring leader zone affinity for zone: {ctx.client_zone}")
    zone_short = ctx.client_zone.split("-")[-1]

    # Find TiDB node IP to connect through NodePort
    tidb_ip = ctx.tidb_nodes[0].private_ip if ctx.tidb_nodes else "127.0.0.1"

    ssh_run(host, f"""
# Get the TiDB NodePort service cluster IP or use node IP
TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}' 2>/dev/null || echo "")
TIDB_PORT=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.ports[0].port}}' 2>/dev/null || echo "4000")

if [ -z "$TIDB_HOST" ]; then
    TIDB_HOST="{tidb_ip}"
    TIDB_PORT="30400"
fi

for i in $(seq 1 30); do
    if mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "
-- Create placement policy preferring leaders in client zone
CREATE PLACEMENT POLICY IF NOT EXISTS local_leader
  LEADER_CONSTRAINTS='[+zone={zone_short}]'
  FOLLOWER_CONSTRAINTS='{{\\"+zone={zone_short}\\": 1}}';

-- Show placement policies
SHOW PLACEMENT;
" 2>/dev/null || echo "Note: Placement rules require TiDB 5.2+ and enabled placement-rules"
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# TiCDC downstream cluster + changefeed
# ---------------------------------------------------------------------------

DOWNSTREAM_NAMESPACE = "tidb-downstream"
DOWNSTREAM_CLUSTER_NAME = "downstream"
DOWNSTREAM_NODEPORT = 30401


def deploy_downstream_cluster(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Deploying downstream TiDB cluster in namespace {DOWNSTREAM_NAMESPACE}")

    yaml_lines = []
    yaml_lines.append('apiVersion: pingcap.com/v1alpha1')
    yaml_lines.append('kind: TidbCluster')
    yaml_lines.append('metadata:')
    yaml_lines.append(f'  name: {DOWNSTREAM_CLUSTER_NAME}')
    yaml_lines.append(f'  namespace: {DOWNSTREAM_NAMESPACE}')
    yaml_lines.append('spec:')
    yaml_lines.append(f'  version: {ctx.tidb_version}')
    yaml_lines.append('  timezone: UTC')
    yaml_lines.append('  pvReclaimPolicy: Retain')
    yaml_lines.append('  enableDynamicConfiguration: true')
    yaml_lines.append('  configUpdateStrategy: RollingUpdate')
    yaml_lines.append('  discovery:')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: node-role.kubernetes.io/control-plane')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('  helper:')
    yaml_lines.append('    image: alpine:3.16.0')
    yaml_lines.append('  pd:')
    yaml_lines.append('    baseImage: pingcap/pd')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.downstream_pd_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "10Gi"')
    yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/downstream-pd: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/downstream-pd')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('  tikv:')
    yaml_lines.append('    baseImage: pingcap/tikv')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append('    evictLeaderTimeout: 1m')
    yaml_lines.append(f'    replicas: {ctx.downstream_tikv_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "50Gi"')
    yaml_lines.append('    config: |')
    yaml_lines.append('      [storage]')
    yaml_lines.append('      reserve-space = "0MB"')
    yaml_lines.append('      [rocksdb]')
    yaml_lines.append('      max-open-files = 256')
    yaml_lines.append('      [raftdb]')
    yaml_lines.append('      max-open-files = 256')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/downstream-tikv: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/downstream-tikv')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('  tidb:')
    yaml_lines.append('    baseImage: pingcap/tidb')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.downstream_tidb_replicas}')
    yaml_lines.append('    service:')
    yaml_lines.append('      type: NodePort')
    yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/downstream-tidb: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/downstream-tidb')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')

    yaml_str = '\n'.join(yaml_lines)
    yaml_b64 = base64.b64encode(yaml_str.encode()).decode()

    ssh_run(host, f"""
kubectl create namespace {DOWNSTREAM_NAMESPACE} || true

echo '{yaml_b64}' | base64 -d > /tmp/tidb-downstream.yaml
kubectl apply -f /tmp/tidb-downstream.yaml -n {DOWNSTREAM_NAMESPACE}

echo "Waiting for downstream TiDB service..."
for i in $(seq 1 30); do
    if kubectl get svc {DOWNSTREAM_CLUSTER_NAME}-tidb -n {DOWNSTREAM_NAMESPACE} >/dev/null 2>&1; then
        kubectl patch svc {DOWNSTREAM_CLUSTER_NAME}-tidb -n {DOWNSTREAM_NAMESPACE} --type='json' \
            -p='[{{"op": "replace", "path": "/spec/ports/0/nodePort", "value": {DOWNSTREAM_NODEPORT}}}]' || true
        break
    fi
    sleep 2
done
""", ctx)


def wait_for_downstream_ready(host: InstanceInfo, ctx: BootstrapContext):
    log("Waiting for downstream TiDB cluster to be ready...")
    ssh_run(host, f"""
for i in $(seq 1 60); do
    READY=$(kubectl get pods -n {DOWNSTREAM_NAMESPACE} -l app.kubernetes.io/component=tidb \
        -o jsonpath='{{.items[0].status.containerStatuses[0].ready}}' 2>/dev/null || echo "false")
    if [ "$READY" = "true" ]; then
        echo "Downstream TiDB is ready!"
        break
    fi
    echo "Waiting for downstream TiDB... (attempt $i/60)"
    kubectl get pods -n {DOWNSTREAM_NAMESPACE} 2>/dev/null || true
    sleep 10
done

kubectl get pods -n {DOWNSTREAM_NAMESPACE}
kubectl get svc -n {DOWNSTREAM_NAMESPACE}

kubectl patch svc {DOWNSTREAM_CLUSTER_NAME}-tidb -n {DOWNSTREAM_NAMESPACE} --type='json' \
    -p='[{{"op": "replace", "path": "/spec/ports/0/nodePort", "value": {DOWNSTREAM_NODEPORT}}}]' 2>/dev/null || true
echo "Downstream TiDB NodePort set to {DOWNSTREAM_NODEPORT}"
""", ctx, strict=False)


def create_ticdc_changefeed(host: InstanceInfo, ctx: BootstrapContext):
    log("Creating TiCDC changefeed to downstream cluster")
    downstream_svc = f"{DOWNSTREAM_CLUSTER_NAME}-tidb.{DOWNSTREAM_NAMESPACE}.svc"

    changefeed_toml = (
        '[mounter]\n'
        'worker-num = 32\n'
        '\n'
        '[scheduler]\n'
        'enable-table-across-nodes = true\n'
    )
    toml_b64 = base64.b64encode(changefeed_toml.encode()).decode()

    ssh_run(host, f"""
TICDC_POD=$(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=ticdc -o jsonpath='{{.items[0].metadata.name}}' 2>/dev/null)
if [ -z "$TICDC_POD" ]; then
    echo "ERROR: No TiCDC pod found"
    exit 1
fi

echo "TiCDC pod: $TICDC_POD"

echo '{toml_b64}' | base64 -d > /tmp/changefeed.toml
kubectl cp /tmp/changefeed.toml tidb-cluster/$TICDC_POD:/tmp/changefeed.toml

for i in $(seq 1 30); do
    STATUS=$(kubectl exec -n tidb-cluster $TICDC_POD -- /cdc cli capture list \
        --server=http://127.0.0.1:8301 2>/dev/null | head -1)
    if echo "$STATUS" | grep -q '"id"'; then
        echo "TiCDC captures are registered"
        break
    fi
    echo "Waiting for TiCDC captures... (attempt $i/30)"
    sleep 10
done

kubectl exec -n tidb-cluster $TICDC_POD -- /cdc cli changefeed create \
    --server=http://127.0.0.1:8301 \
    --sink-uri="mysql://root@{downstream_svc}:4000/?worker-count=32&max-txn-row=512&batch-dml-enable=true" \
    --changefeed-id="benchmark-replication" \
    --config=/tmp/changefeed.toml

echo "Changefeed created successfully"

kubectl exec -n tidb-cluster $TICDC_POD -- /cdc cli changefeed list \
    --server=http://127.0.0.1:8301
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Sysbench Database Setup
# ---------------------------------------------------------------------------


def create_sysbench_database(host: InstanceInfo, ctx: BootstrapContext):
    """Create the sysbench database via the TiDB service in k8s."""
    log("Creating sysbench database...")

    # Get the TiDB NodePort IP (use first TiDB node IP with NodePort 30400)
    tidb_ip = ctx.tidb_nodes[0].private_ip if ctx.tidb_nodes else "127.0.0.1"

    ssh_run(host, f"""
# Determine TiDB connection endpoint
TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}' 2>/dev/null || echo "")
TIDB_PORT=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.ports[0].port}}' 2>/dev/null || echo "4000")

if [ -z "$TIDB_HOST" ]; then
    TIDB_HOST="{tidb_ip}"
    TIDB_PORT="30400"
fi

for i in $(seq 1 30); do
    if mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS sbtest;"
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SHOW DATABASES;"

# Disable resource control to avoid error 8249 (Unknown resource group 'default')
# Resource control is not needed for benchmarking and causes sysbench FATAL errors
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SET GLOBAL tidb_enable_resource_control = OFF;" 2>/dev/null || true
echo "Resource control disabled"
""", ctx)


# ---------------------------------------------------------------------------
# Wait for SSH on multiple nodes
# ---------------------------------------------------------------------------

def wait_for_ssh(node: InstanceInfo, ctx: BootstrapContext, max_attempts=30):
    """Wait for SSH to become available on a node."""
    for attempt in range(max_attempts):
        result = ssh_capture(node, "echo ready", ctx, strict=False)
        if result.returncode == 0:
            return True
        if attempt < max_attempts - 1:
            log(f"  {node.role} ({node.private_ip}) not ready yet (attempt {attempt + 1}/{max_attempts})...")
            time.sleep(10)
    log(f"  WARNING: {node.role} ({node.private_ip}) did not become reachable after {max_attempts} attempts")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = parse_args()
    args = parser.parse_args()
    configure_from_args(args)

    if args.cleanup:
        cleanup_stack()
        return

    key_path = Path(args.ssh_key_path).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")
    need_cmd("ssh")

    self_cidr = args.ssh_cidr or my_public_cidr()
    log(f"Using SSH CIDR: {self_cidr}")
    log(f"Using TiDB version: {args.tidb_version}")

    multi_az = args.multi_az and not args.single_az
    production = args.production and not args.benchmark_mode

    pd_replicas = args.pd_replicas
    tikv_replicas = args.tikv_replicas
    tidb_replicas = args.tidb_replicas

    if args.single_az:
        pd_replicas = 1
        tikv_replicas = 1
        tidb_replicas = 1
        log("Single-AZ mode: using 1 replica per component")
    else:
        log(f"Multi-AZ mode: PD={pd_replicas}, TiKV={tikv_replicas}, TiDB={tidb_replicas}")

    types = PRODUCTION_INSTANCE_TYPES if production else BENCHMARK_INSTANCE_TYPES
    log(f"Instance types: PD={types['pd']}, TiKV={types['tikv']}, TiDB={types['tidb']}, Control={types['control']}")

    # -----------------------------------------------------------------------
    # Network
    # -----------------------------------------------------------------------
    log("=== Provisioning Network ===")
    vpc_id = ensure_vpc()
    igw_id = ensure_igw(vpc_id)
    if multi_az:
        az_subnets = {}
        for az, cidr in SUBNET_CIDRS.items():
            sid = ensure_subnet(vpc_id, f"{_cu.STACK}-public-{az}", cidr, az=az, public=True)
            ensure_public_rtb(vpc_id, igw_id, sid)
            az_subnets[az] = sid
        subnet_ids = [az_subnets[az] for az in AVAILABILITY_ZONES]
        public_subnet = az_subnets[args.client_zone]
    else:
        public_subnet = ensure_subnet(vpc_id, f"{_cu.STACK}-public", PUB_CIDR, az=args.client_zone, public=True)
        ensure_public_rtb(vpc_id, igw_id, public_subnet)
        subnet_ids = [public_subnet]

    # -----------------------------------------------------------------------
    # Security Group
    # -----------------------------------------------------------------------
    log("=== Provisioning Security Group ===")
    sg = ensure_sg(vpc_id, f"{_cu.STACK}-cluster", "TiDB load test cluster (multi-node)")
    refresh_ssh_rule(sg, self_cidr)
    ensure_ingress_tcp_cidr(sg, TIDB_PORT, self_cidr)
    ensure_intra_cluster_rules(sg)  # All traffic within the cluster

    # -----------------------------------------------------------------------
    # EC2 Instances
    # -----------------------------------------------------------------------
    log("=== Provisioning EC2 Instances ===")
    control_type = types["control"]
    control_vol = ROOT_VOLUME_SIZES["control"]
    control_id = ensure_instance(f"{_cu.STACK}-control", "control", control_type, public_subnet, sg, control_vol)

    pd_ids = provision_role_instances("pd", pd_replicas, types, subnet_ids, sg, production)
    tikv_ids = provision_role_instances("tikv", tikv_replicas, types, subnet_ids, sg, production)
    tidb_ids = provision_role_instances("tidb", tidb_replicas, types, subnet_ids, sg, production)

    enable_ticdc = args.ticdc
    ticdc_replicas = args.ticdc_replicas if enable_ticdc else 0
    ticdc_ids = []
    ds_pd_ids = []
    ds_tikv_ids = []
    ds_tidb_ids = []

    if enable_ticdc:
        log(f"TiCDC mode: provisioning {ticdc_replicas} TiCDC + downstream cluster instances")
        ticdc_ids = provision_role_instances("ticdc", ticdc_replicas, types, subnet_ids, sg, production)
        ds_pd_ids = provision_role_instances("downstream-pd", args.downstream_pd_replicas, types, subnet_ids, sg, production)
        ds_tikv_ids = provision_role_instances("downstream-tikv", args.downstream_tikv_replicas, types, subnet_ids, sg, production)
        ds_tidb_ids = provision_role_instances("downstream-tidb", args.downstream_tidb_replicas, types, subnet_ids, sg, production)

    # -----------------------------------------------------------------------
    # Gather Instance Information
    # -----------------------------------------------------------------------
    log("=== Gathering Instance Information ===")
    time.sleep(10)

    control = instance_info_from_id(control_id, "control")
    pd_nodes = [instance_info_from_id(iid, f"pd-{i+1}") for i, iid in enumerate(pd_ids)]
    tikv_nodes = [instance_info_from_id(iid, f"tikv-{i+1}") for i, iid in enumerate(tikv_ids)]
    tidb_nodes = [instance_info_from_id(iid, f"tidb-{i+1}") for i, iid in enumerate(tidb_ids)]
    ticdc_nodes = [instance_info_from_id(iid, f"ticdc-{i+1}") for i, iid in enumerate(ticdc_ids)]
    ds_pd_nodes = [instance_info_from_id(iid, f"downstream-pd-{i+1}") for i, iid in enumerate(ds_pd_ids)]
    ds_tikv_nodes = [instance_info_from_id(iid, f"downstream-tikv-{i+1}") for i, iid in enumerate(ds_tikv_ids)]
    ds_tidb_nodes = [instance_info_from_id(iid, f"downstream-tidb-{i+1}") for i, iid in enumerate(ds_tidb_ids)]

    log(f"  control: public={control.public_ip} private={control.private_ip}")
    for n in pd_nodes + tikv_nodes + tidb_nodes + ticdc_nodes:
        log(f"  {n.role}: public={n.public_ip} private={n.private_ip}")
    for n in ds_pd_nodes + ds_tikv_nodes + ds_tidb_nodes:
        log(f"  {n.role}: public={n.public_ip} private={n.private_ip}")

    all_nodes = [pd_nodes, tikv_nodes, tidb_nodes, ticdc_nodes, ds_pd_nodes, ds_tikv_nodes, ds_tidb_nodes]
    total_instances = 1 + sum(len(ns) for ns in all_nodes)
    log(f"  Total instances: {total_instances}")

    if args.skip_bootstrap:
        log("Skipping bootstrap (--skip-bootstrap). Infrastructure ready.")
        log(f"SSH to control: ssh -i {key_path} ec2-user@{control.public_ip}")
        return

    # -----------------------------------------------------------------------
    # Build context
    # -----------------------------------------------------------------------
    ctx = BootstrapContext(
        ssh_key_path=key_path,
        self_cidr=self_cidr,
        jump_host=control,
        pd_nodes=pd_nodes,
        tikv_nodes=tikv_nodes,
        tidb_nodes=tidb_nodes,
        ticdc_nodes=ticdc_nodes,
        downstream_pd_nodes=ds_pd_nodes,
        downstream_tikv_nodes=ds_tikv_nodes,
        downstream_tidb_nodes=ds_tidb_nodes,
        tidb_version=args.tidb_version,
        pd_replicas=pd_replicas,
        tikv_replicas=tikv_replicas,
        tidb_replicas=tidb_replicas,
        ticdc_replicas=ticdc_replicas,
        enable_ticdc=enable_ticdc,
        downstream_pd_replicas=args.downstream_pd_replicas if enable_ticdc else 0,
        downstream_tikv_replicas=args.downstream_tikv_replicas if enable_ticdc else 0,
        downstream_tidb_replicas=args.downstream_tidb_replicas if enable_ticdc else 0,
        multi_az=multi_az,
        production=production,
        client_zone=args.client_zone,
    )

    # -----------------------------------------------------------------------
    # Wait for SSH on all nodes
    # -----------------------------------------------------------------------
    log("=== Waiting for SSH on all nodes ===")
    for node in ctx.all_nodes:
        wait_for_ssh(node, ctx)

    # -----------------------------------------------------------------------
    # Install base packages on all nodes
    # -----------------------------------------------------------------------
    log("=== Installing Base Packages ===")
    for node in ctx.all_nodes:
        install_base_packages(node, ctx)

    # -----------------------------------------------------------------------
    # k3s Cluster Setup
    # -----------------------------------------------------------------------
    log("=== Setting up k3s Cluster ===")
    install_k3s_server(control, ctx)
    k3s_token = get_k3s_token(control, ctx)
    log(f"k3s token retrieved (length={len(k3s_token)})")

    log("=== Joining k3s Agent Nodes ===")
    for node in ctx.all_agent_nodes:
        install_k3s_agent(node, control.private_ip, k3s_token, ctx)

    log("=== Labeling and Tainting Nodes ===")
    label_and_taint_nodes(ctx)

    log("=== Installing Storage Provisioner ===")
    install_local_path_provisioner(control, ctx)

    if ctx.multi_az:
        label_control_plane_zone(control, ctx)

    # -----------------------------------------------------------------------
    # TiDB Operator + Cluster
    # -----------------------------------------------------------------------
    log("=== Installing Helm ===")
    install_helm(control, ctx)

    log("=== Installing TiDB Operator ===")
    install_tidb_operator(control, ctx)

    log("=== Deploying TiDB Cluster ===")
    deploy_tidb_cluster(control, ctx)
    wait_for_tidb_ready(control, ctx)

    # -----------------------------------------------------------------------
    # Sysbench Database
    # -----------------------------------------------------------------------
    create_sysbench_database(control, ctx)

    if ctx.multi_az:
        log("=== Configuring Leader Zone Affinity ===")
        configure_leader_zone_affinity(control, ctx)

    if ctx.enable_ticdc:
        log("=== Deploying Downstream TiDB Cluster ===")
        deploy_downstream_cluster(control, ctx)
        wait_for_downstream_ready(control, ctx)

        log("=== Creating TiCDC Changefeed ===")
        create_ticdc_changefeed(control, ctx)

    # -----------------------------------------------------------------------
    # Done
    # -----------------------------------------------------------------------
    log("=== Deployment Complete ===")
    log("")
    log(f"SSH to control: ssh -i {key_path} ec2-user@{control.public_ip}")
    log("")

    tidb_node_ip = tidb_nodes[0].public_ip if tidb_nodes else control.public_ip
    log(f"TiDB accessible via NodePort: {tidb_node_ip}:30400")
    log("")
    log("From the control host, connect to TiDB:")
    log(f"  TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}')")
    log(f"  mysql -h $TIDB_HOST -P 4000 -u root")
    log("")
    log("To check cluster status:")
    log("  kubectl get nodes -o wide")
    log("  kubectl get pods -n tidb-cluster -o wide")
    log("  kubectl get tc -n tidb-cluster")
    log("")
    log("Next: provision a benchmark client in this VPC:")
    log(f"  python3 -m common.client --seed {args.seed} --server-type tidb --size small")

    if ctx.enable_ticdc:
        ds_tidb_ip = ds_tidb_nodes[0].public_ip if ds_tidb_nodes else control.public_ip
        log("")
        log(f"Downstream TiDB via NodePort: {ds_tidb_ip}:{DOWNSTREAM_NODEPORT}")
        log(f"  kubectl get pods -n {DOWNSTREAM_NAMESPACE} -o wide")
        log(f"  kubectl get tc -n {DOWNSTREAM_NAMESPACE}")
        log("")
        log("TiCDC changefeed status:")
        log("  kubectl exec -n tidb-cluster $(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=ticdc -o jsonpath='{.items[0].metadata.name}') -- /cdc cli changefeed list --server=http://127.0.0.1:8301")


if __name__ == "__main__":
    main()
