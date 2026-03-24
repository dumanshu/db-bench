#!/usr/bin/env python3
"""
Unified client VM provisioning for db-bench.

Provides a single EC2 client instance with all benchmark tools installed:
  - Base OS packages + sysctl tuning (all server types)
  - sysbench (tidb, aurora)
  - mysql-client / mariadb (tidb, aurora)
  - memtier_benchmark (valkey)
  - valkey-cli (valkey)
  - docker (valkey)

Standalone CLI:
    python3 -m common.client --seed foo --server-type aurora --size small
    python3 -m common.client --seed foo --server-type tidb --size heavy
    python3 -m common.client --cleanup --seed foo --server-type aurora

Library usage from module setup.py files:
    from common.client import install_client_tools
    install_client_tools(host_ip, key_path, server_type="tidb")
"""

import argparse
import json
import os
import sys
from pathlib import Path

from common.util import log, ts
from common.ssh import ssh_run_simple, wait_for_ssh_simple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVER_TYPES = ("tidb", "aurora", "valkey")

# Stack name prefixes used by each server type's setup.py
STACK_PREFIXES = {
    "aurora": "aurora-bench",
    "tidb": "tidb-loadtest",
    "valkey": "valkey-loadtest",
}

# Size presets for the client EC2 instance
SIZE_PRESETS = {
    "small": "c7g.4xlarge",    # 16 vCPU, 32 GB
    "heavy": "c8g.24xlarge",   # 96 vCPU, 192 GB
}

# AL2023 ARM64 AMI SSM parameter (all client sizes are Graviton)
AL2023_SSM_ARM64 = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"

KEY_NAME_PREFIX = "db-bench-client"

MEMTIER_VERSION = "2.1.1.0"
MEMTIER_SRC_URL = (
    f"https://github.com/RedisLabs/memtier_benchmark/archive/"
    f"refs/tags/{MEMTIER_VERSION}.tar.gz"
)


# ---------------------------------------------------------------------------
# State file helpers
# ---------------------------------------------------------------------------

def _state_path(seed):
    """Return path to client state file, relative to repo root."""
    return Path(__file__).resolve().parent / f"client-{seed}-state.json"


def save_state(state, seed):
    path = _state_path(seed)
    with open(path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    log(f"Client state saved to {path}")


def load_state(seed):
    path = _state_path(seed)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# VPC / infrastructure discovery
# ---------------------------------------------------------------------------

def _compute_server_stack(server_type, seed):
    """Compute the stack name used by the server's setup.py."""
    prefix = STACK_PREFIXES.get(server_type)
    if not prefix:
        raise ValueError(f"Unknown server_type={server_type!r}; expected one of {SERVER_TYPES}")
    return f"{prefix}-{seed}"


def discover_server_vpc(ec2_client, server_type, seed):
    """Find the VPC created by the server's setup.py via Project tag."""
    server_stack = _compute_server_stack(server_type, seed)
    resp = ec2_client.describe_vpcs(
        Filters=[{"Name": "tag:Project", "Values": [server_stack]}]
    )
    vpcs = resp.get("Vpcs", [])
    if not vpcs:
        raise SystemExit(
            f"ERROR: No VPC found with tag Project={server_stack}. "
            f"Run the {server_type} setup first."
        )
    vpc = vpcs[0]
    vpc_id = vpc["VpcId"]
    vpc_cidr = vpc.get("CidrBlock", "")
    log(f"Discovered server VPC: {vpc_id} (CIDR: {vpc_cidr}, stack: {server_stack})")
    return vpc_id, vpc_cidr, server_stack


def _find_public_subnet(ec2_client, vpc_id, server_stack):
    """Find a public subnet in the server VPC (one with MapPublicIpOnLaunch or IGW route)."""
    resp = ec2_client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    subnets = resp.get("Subnets", [])

    # Prefer subnets tagged with the server stack and marked public
    for sn in subnets:
        tags = {t["Key"]: t["Value"] for t in sn.get("Tags", [])}
        if tags.get("Project") == server_stack and sn.get("MapPublicIpOnLaunch"):
            log(f"  Using public subnet: {sn['SubnetId']} (AZ: {sn['AvailabilityZone']})")
            return sn["SubnetId"], sn["AvailabilityZone"]

    # Fallback: any subnet with public IP mapping
    for sn in subnets:
        if sn.get("MapPublicIpOnLaunch"):
            log(f"  Using public subnet (fallback): {sn['SubnetId']}")
            return sn["SubnetId"], sn["AvailabilityZone"]

    # Last resort: first subnet
    if subnets:
        sn = subnets[0]
        log(f"  WARNING: No public subnet found; using {sn['SubnetId']} (may lack public IP)")
        return sn["SubnetId"], sn["AvailabilityZone"]

    raise SystemExit(f"ERROR: No subnets found in VPC {vpc_id}")


# ---------------------------------------------------------------------------
# EC2 provisioning
# ---------------------------------------------------------------------------

def _resolve_ami(ssm_client):
    """Resolve AL2023 ARM64 AMI from SSM."""
    resp = ssm_client.get_parameter(Name=AL2023_SSM_ARM64)
    ami_id = resp["Parameter"]["Value"]
    log(f"AL2023 AMI (arm64): {ami_id}")
    return ami_id


def _ensure_key_pair(ec2_client, key_name, key_dir):
    """Create or reuse an EC2 key pair; save PEM locally."""
    key_file = key_dir / f"{key_name}.pem"

    if key_file.exists():
        try:
            ec2_client.describe_key_pairs(KeyNames=[key_name])
            log(f"Key pair '{key_name}' exists (local + AWS)")
            return str(key_file)
        except ec2_client.exceptions.ClientError:
            pass  # local file exists but AWS key missing -- recreate

    # Remove stale AWS key
    try:
        ec2_client.delete_key_pair(KeyName=key_name)
    except Exception:
        pass

    log(f"Creating key pair '{key_name}'...")
    tags = [{"Key": "Name", "Value": key_name}, {"Key": "ManagedBy", "Value": "db-bench-client"}]
    kp = ec2_client.create_key_pair(
        KeyName=key_name,
        KeyType="rsa",
        TagSpecifications=[{"ResourceType": "key-pair", "Tags": tags}],
    )
    if key_file.exists():
        os.chmod(key_file, 0o600)
        key_file.unlink()
    key_file.write_text(kp["KeyMaterial"])
    os.chmod(key_file, 0o400)
    log(f"  Saved to {key_file}")
    return str(key_file)


def _ensure_client_sg(ec2_client, vpc_id, server_stack, vpc_cidr, ssh_cidr):
    """Create or reuse client SG allowing SSH from user + all outbound."""
    sg_name = f"{server_stack}-bench-client"
    resp = ec2_client.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [sg_name]},
                 {"Name": "vpc-id", "Values": [vpc_id]}]
    )
    sgs = resp.get("SecurityGroups", [])
    if sgs:
        sg_id = sgs[0]["GroupId"]
        log(f"REUSED  client SG: {sg_id}")
    else:
        resp = ec2_client.create_security_group(
            GroupName=sg_name,
            Description=f"db-bench client ({server_stack})",
            VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "security-group",
                "Tags": [
                    {"Key": "Project", "Value": server_stack},
                    {"Key": "Name", "Value": sg_name},
                    {"Key": "ManagedBy", "Value": "db-bench-client"},
                ],
            }],
        )
        sg_id = resp["GroupId"]
        log(f"CREATED client SG: {sg_id}")

    # SSH from user's IP
    if ssh_cidr:
        try:
            ec2_client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                    "IpRanges": [{"CidrIp": ssh_cidr}],
                }],
            )
        except ec2_client.exceptions.ClientError as e:
            if "InvalidPermission.Duplicate" not in str(e):
                raise

    return sg_id


def _find_instance(ec2_client, name, server_stack):
    """Find a running/pending client instance by Name + Project tags."""
    resp = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [name]},
            {"Name": "tag:Project", "Values": [server_stack]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
        ]
    )
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst["InstanceId"]
    return None


def provision_client(ec2_client, ssm_client, server_stack, vpc_id, vpc_cidr,
                     subnet_id, sg_id, instance_type, key_name, key_path):
    """Launch the client EC2 instance (idempotent)."""
    instance_name = f"{server_stack}-bench-client"

    existing = _find_instance(ec2_client, instance_name, server_stack)
    if existing:
        log(f"REUSED  client instance: {existing}")
        ec2_client.get_waiter("instance_running").wait(InstanceIds=[existing])
        inst = _describe_instance(ec2_client, existing)
        return existing, inst.get("PublicIpAddress", ""), inst.get("PrivateIpAddress", "")

    ami_id = _resolve_ami(ssm_client)
    log(f"Launching client instance ({instance_type})...")
    resp = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "SubnetId": subnet_id,
            "AssociatePublicIpAddress": True,
            "Groups": [sg_id],
        }],
        BlockDeviceMappings=[{
            "DeviceName": "/dev/xvda",
            "Ebs": {"VolumeSize": 100, "VolumeType": "gp3", "DeleteOnTermination": True},
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Project", "Value": server_stack},
                {"Key": "Name", "Value": instance_name},
                {"Key": "Role", "Value": "bench-client"},
                {"Key": "ManagedBy", "Value": "db-bench-client"},
            ],
        }],
        MinCount=1, MaxCount=1,
    )
    iid = resp["Instances"][0]["InstanceId"]
    log(f"CREATED client instance: {iid}")

    ec2_client.get_waiter("instance_running").wait(InstanceIds=[iid])
    inst = _describe_instance(ec2_client, iid)
    pub_ip = inst.get("PublicIpAddress", "")
    priv_ip = inst.get("PrivateIpAddress", "")
    log(f"  Client running: public={pub_ip} private={priv_ip}")
    return iid, pub_ip, priv_ip


def _describe_instance(ec2_client, iid):
    resp = ec2_client.describe_instances(InstanceIds=[iid])
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst
    raise RuntimeError(f"Instance {iid} not found")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_client(ec2_client, server_stack, seed):
    """Terminate client instance and delete its SG."""
    instance_name = f"{server_stack}-bench-client"
    iid = _find_instance(ec2_client, instance_name, server_stack)
    if iid:
        log(f"TERMINATING client instance: {iid}")
        ec2_client.terminate_instances(InstanceIds=[iid])
        try:
            ec2_client.get_waiter("instance_terminated").wait(InstanceIds=[iid])
        except Exception:
            log("  Warning: timeout waiting for termination; continuing")
    else:
        log("No client instance found to terminate.")

    # Delete client SG
    sg_name = f"{server_stack}-bench-client"
    resp = ec2_client.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [sg_name]},
                 {"Name": "tag:Project", "Values": [server_stack]}]
    )
    for sg in resp.get("SecurityGroups", []):
        sg_id = sg["GroupId"]
        log(f"DELETING client SG: {sg_id}")
        try:
            ec2_client.delete_security_group(GroupId=sg_id)
        except Exception as e:
            log(f"  Warning: could not delete SG {sg_id}: {e}")

    # Remove state file
    state_file = _state_path(seed)
    if state_file.exists():
        state_file.unlink()
        log(f"Removed state file: {state_file}")

    log("Client cleanup complete.")


# ---------------------------------------------------------------------------
# Tool installation (library API -- unchanged from original)
# ---------------------------------------------------------------------------

def install_client_tools(host_ip, key_path, server_type, timeout=600):
    """Install all benchmark tools on the client VM.

    Installs the SUPERSET of all tools regardless of server_type, so the
    client can be reused across different database benchmarks.
    """
    if server_type not in SERVER_TYPES:
        raise ValueError(f"server_type must be one of {SERVER_TYPES}, got {server_type!r}")

    log(f"Installing client tools on {host_ip} (superset for all server types)")
    wait_for_ssh_simple(host_ip, key_path)

    _install_base_packages(host_ip, key_path, timeout=timeout)
    _tune_sysctl(host_ip, key_path)

    # Always install everything so client works with any DB
    _install_mysql_client(host_ip, key_path, timeout=timeout)
    _install_sysbench(host_ip, key_path, timeout=timeout)
    _install_memtier(host_ip, key_path, timeout=timeout)
    _install_docker(host_ip, key_path)

    log(f"All client tools installed on {host_ip}")


def _install_base_packages(host_ip, key_path, timeout=300):
    log(f"  Installing base packages on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        if command -v dnf >/dev/null 2>&1; then
            PKG=dnf
        else
            PKG=yum
        fi
        sudo $PKG -y update || true

        if command -v amazon-linux-extras >/dev/null 2>&1; then
            sudo amazon-linux-extras enable epel || true
        fi
        sudo $PKG -y install epel-release || true

        sudo $PKG -y install \\
            gcc make automake libtool git jq htop sysstat mtr \\
            openssl-devel pkg-config binutils iproute \\
            tar xz perf ethtool iperf3 \\
            jemalloc-devel || true
    """, timeout=timeout)


def _tune_sysctl(host_ip, key_path):
    log(f"  Tuning sysctl on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        sudo tee /etc/sysctl.d/99-db-bench.conf >/dev/null <<'SYSCTL'
        fs.file-max = 1048576
        fs.nr_open = 1048576

        net.core.somaxconn = 65535
        net.core.netdev_max_backlog = 65535
        net.core.rmem_max = 16777216
        net.core.wmem_max = 16777216
        net.ipv4.ip_forward = 1
        net.ipv4.tcp_rmem = 4096 87380 16777216
        net.ipv4.tcp_wmem = 4096 65536 16777216
        net.ipv4.tcp_max_syn_backlog = 65535
        net.ipv4.tcp_fin_timeout = 15
        net.ipv4.tcp_tw_reuse = 1
        net.ipv4.ip_local_port_range = 1024 65535

        kernel.perf_event_paranoid = -1
        kernel.kptr_restrict = 0
SYSCTL

        sudo tee /etc/security/limits.d/99-db-bench.conf >/dev/null <<'LIMITS'
        * soft nofile 1000000
        * hard nofile 1000000
        * soft nproc 65535
        * hard nproc 65535
        root soft nofile 1000000
        root hard nofile 1000000
LIMITS

        sudo sysctl --system || true
    """)


def _install_mysql_client(host_ip, key_path, timeout=300):
    log(f"  Installing mysql client on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        if command -v mysql >/dev/null 2>&1; then
            echo "mysql client already installed"
            exit 0
        fi
        if command -v dnf >/dev/null 2>&1; then
            sudo dnf -y install mariadb105 mariadb105-devel 2>/dev/null || \
            sudo dnf -y install mariadb mariadb-devel 2>/dev/null || \
            sudo dnf -y install mysql mysql-devel 2>/dev/null || true
        else
            sudo yum -y install mariadb mariadb-devel 2>/dev/null || \
            sudo yum -y install mysql mysql-devel 2>/dev/null || true
        fi
        mysql --version || echo "WARNING: mysql client not found after install"
    """, timeout=timeout)


def _install_sysbench(host_ip, key_path, timeout=600):
    log(f"  Installing sysbench on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        if command -v sysbench >/dev/null 2>&1; then
            echo "sysbench already installed"
            sysbench --version
            exit 0
        fi
        set -euo pipefail
        cd /tmp
        if [ ! -d sysbench ]; then
            git clone https://github.com/akopytov/sysbench.git
        fi
        cd sysbench
        git checkout 1.0.20
        ./autogen.sh
        ./configure --with-mysql
        make -j$(nproc)
        sudo make install
        sudo ldconfig
        sysbench --version
    """, timeout=timeout)


def _install_memtier(host_ip, key_path, timeout=600):
    log(f"  Installing memtier_benchmark on {host_ip}")
    ssh_run_simple(host_ip, key_path, f"""
        if command -v memtier_benchmark >/dev/null 2>&1; then
            echo "memtier_benchmark already installed"
            exit 0
        fi
        if command -v dnf >/dev/null 2>&1; then
            PKG=dnf
        else
            PKG=yum
        fi
        if sudo $PKG -y install memtier-benchmark >/dev/null 2>&1; then
            exit 0
        fi
        echo "Building memtier_benchmark from source"
        set -euo pipefail
        sudo $PKG -y install libevent-devel pkgconfig autoconf automake libtool gcc gcc-c++ || true
        WORK=$(mktemp -d /tmp/memtier.XXXX)
        trap 'rm -rf "$WORK"' EXIT
        cd "$WORK"
        curl -L -o memtier-src.tgz \\
            'https://github.com/RedisLabs/memtier_benchmark/archive/refs/tags/{MEMTIER_VERSION}.tar.gz'
        tar -xzf memtier-src.tgz
        cd memtier_benchmark-{MEMTIER_VERSION}
        if [ -x ./build.sh ]; then
            ./build.sh
        else
            autoreconf -ivf
            ./configure
            make -j $(nproc)
        fi
        sudo make install
    """, timeout=timeout)


def _install_docker(host_ip, key_path):
    log(f"  Installing docker on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        if command -v docker >/dev/null 2>&1; then
            echo "docker already installed"
            exit 0
        fi
        if command -v amazon-linux-extras >/dev/null 2>&1; then
            sudo amazon-linux-extras enable docker || true
        fi
        sudo dnf -y install docker || sudo yum -y install docker || true
        sudo systemctl enable docker || true
        sudo systemctl start docker || true
        sudo usermod -aG docker ec2-user || true
    """)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Provision a standalone benchmark client VM in an existing server VPC.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python3 -m common.client --seed foo --server-type aurora --size small
  python3 -m common.client --seed foo --server-type tidb --size heavy
  python3 -m common.client --cleanup --seed foo --server-type aurora
""",
    )
    p.add_argument("--seed", required=True, help="Same seed used when provisioning the server.")
    p.add_argument("--server-type", required=True, choices=SERVER_TYPES,
                   help="Type of database server (determines VPC discovery).")
    p.add_argument("--size", default="small", choices=list(SIZE_PRESETS.keys()),
                   help="Client instance size preset (default: small).")
    p.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1).")
    p.add_argument("--aws-profile", default=os.environ.get("AWS_PROFILE", "sandbox"),
                   help="AWS CLI profile (default: sandbox).")
    p.add_argument("--cleanup", action="store_true", help="Tear down client resources.")
    return p.parse_args()


def main():
    import boto3
    from botocore.config import Config as BotoConfig

    args = parse_args()
    seed = args.seed
    server_type = args.server_type
    region = args.region
    profile = args.aws_profile

    boto_config = BotoConfig(
        retries={"max_attempts": 10, "mode": "adaptive"},
        connect_timeout=15, read_timeout=60,
    )

    session = boto3.Session(profile_name=profile, region_name=region)
    ec2_client = session.client("ec2", config=boto_config)
    ssm_client = session.client("ssm", config=boto_config)

    server_stack = _compute_server_stack(server_type, seed)

    if args.cleanup:
        log(f"Cleaning up client for {server_stack}...")
        cleanup_client(ec2_client, server_stack, seed)
        return

    # Discover server VPC
    vpc_id, vpc_cidr, server_stack = discover_server_vpc(ec2_client, server_type, seed)

    # Find public subnet in server VPC
    subnet_id, az = _find_public_subnet(ec2_client, vpc_id, server_stack)

    # SSH CIDR for the user
    from common.util import my_public_cidr
    try:
        ssh_cidr = my_public_cidr()
    except Exception:
        ssh_cidr = "0.0.0.0/0"

    # Security group
    sg_id = _ensure_client_sg(ec2_client, vpc_id, server_stack, vpc_cidr, ssh_cidr)

    # Key pair
    key_name = f"{KEY_NAME_PREFIX}-{seed}"
    key_dir = Path(__file__).resolve().parent
    key_path = _ensure_key_pair(ec2_client, key_name, key_dir)

    # Instance type from size preset
    instance_type = SIZE_PRESETS[args.size]

    # Provision client
    iid, pub_ip, priv_ip = provision_client(
        ec2_client, ssm_client, server_stack, vpc_id, vpc_cidr,
        subnet_id, sg_id, instance_type, key_name, key_path,
    )

    # Install tools (superset)
    install_client_tools(pub_ip, key_path, server_type)

    # Save state
    state = {
        "seed": seed,
        "server_type": server_type,
        "server_stack": server_stack,
        "region": region,
        "vpc_id": vpc_id,
        "subnet_id": subnet_id,
        "sg_id": sg_id,
        "instance_id": iid,
        "instance_type": instance_type,
        "public_ip": pub_ip,
        "private_ip": priv_ip,
        "key_name": key_name,
        "key_path": key_path,
        "created_at": ts(),
    }
    save_state(state, seed)

    # Summary
    print()
    print("=" * 60)
    print("Benchmark Client Ready")
    print("=" * 60)
    print(f"  Server type:     {server_type}")
    print(f"  Server stack:    {server_stack}")
    print(f"  Instance type:   {instance_type} ({args.size})")
    print(f"  Public IP:       {pub_ip}")
    print(f"  Private IP:      {priv_ip}")
    print(f"  SSH key:         {key_path}")
    print()
    print(f"SSH to client:")
    print(f"  ssh -i {key_path} ec2-user@{pub_ip}")
    print()
    print(f"Run benchmark:")
    print(f"  python3 -m common.benchmark --server-type {server_type} --seed {seed} ...")
    print()
    print(f"Cleanup:")
    print(f"  python3 -m common.client --cleanup --seed {seed} --server-type {server_type}")
    print()


if __name__ == "__main__":
    main()
