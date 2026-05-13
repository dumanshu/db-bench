#!/usr/bin/env python3
"""
Unified client VM provisioning for db-bench.

Provides a single EC2 client instance with all benchmark tools installed:
  - Base OS packages + sysctl tuning (all server types)
  - sysbench with MySQL and PostgreSQL drivers (tidb, aurora, aurora-pg, dsql)
  - mysql-client / mariadb (tidb, aurora)
  - PostgreSQL client/development libraries (aurora-pg, dsql)
  - memtier_benchmark (valkey)
  - valkey-cli (valkey)
  - docker (valkey)

Standalone CLI:
    python3 -m common.client --seed foo --server-type aurora --size small
    python3 -m common.client --seed foo --server-type tidb --size heavy
    python3 -m common.client --cleanup --seed foo

Library usage from module setup.py files:
    from common.client import install_client_tools
    install_client_tools(host_ip, key_path, server_type="tidb")
"""

import argparse
import os
import sys
from pathlib import Path

from common.util import log, ts
from common.ssh import ssh_run_simple, wait_for_ssh_simple

# ---------------------------------------------------------------------------
# System tuning (sysctl + file limits) -- shared across all benchmarks
# ---------------------------------------------------------------------------

def system_tuning_script(extra_sysctl="", conf_name="db-bench"):
    """Return a shell script that applies sysctl and file-limit tuning.

    Parameters
    ----------
    extra_sysctl : str
        Additional sysctl key=value lines to append (e.g. k8s bridge-nf
        settings, perf_event_paranoid).  Blank lines / comments are fine.
    conf_name : str
        Base name used for the config files written under
        ``/etc/sysctl.d/99-{conf_name}.conf`` and
        ``/etc/security/limits.d/99-{conf_name}.conf``.
    """
    extra = extra_sysctl.rstrip()
    extra_block = f"\n{extra}\n" if extra else ""
    return f"""\
# --- db-bench system tuning ({conf_name}) ---
sudo tee /etc/sysctl.d/99-{conf_name}.conf >/dev/null <<'SYSEOF'
# File descriptor limits
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
net.ipv4.ip_local_port_range = 1024 65535{extra_block}SYSEOF
sudo sysctl --system >/dev/null 2>&1 || true

# Raise open file / process limits
sudo tee /etc/security/limits.d/99-{conf_name}.conf >/dev/null <<'LIMEOF'
* soft nofile 1000000
* hard nofile 1000000
* soft nproc  65535
* hard nproc  65535
root soft nofile 1000000
root hard nofile 1000000
LIMEOF
"""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVER_TYPES = ("tidb", "aurora", "valkey", "dsql", "aurora-pg")

# Stack name prefixes used by each server type's setup.py
STACK_PREFIXES = {
    "aurora": "aurora-bench",
    "tidb": "tidb-loadtest",
    "valkey": "valkey-loadtest",
    "dsql": "dsql-loadtest",
    "aurora-pg": "aurora-pg-bench",
}

# Size presets for the client EC2 instance (Graviton4)
SIZE_PRESETS = {
    "small": "c8g.4xlarge",    # 16 vCPU, 32 GB
    "heavy": "c8g.24xlarge",   # 96 vCPU, 192 GB
}

CLIENT_DB_PORTS = {
    "aurora": [3306],
    "aurora-pg": [5432],
    "dsql": [5432],
    "tidb": [30400],
    "valkey": [6379],
}

# AL2023 ARM64 AMI SSM parameter (all client sizes are Graviton)
AL2023_SSM_ARM64 = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"

KEY_NAME_PREFIX = "db-bench-client"

MEMTIER_VERSION = "2.3.0"
MEMTIER_SRC_URL = (
    f"https://github.com/RedisLabs/memtier_benchmark/archive/"
    f"refs/tags/{MEMTIER_VERSION}.tar.gz"
)

VALKEY_VERSION = os.environ.get("VALKEY_VERSION", "9.0.3")
VALKEY_BIN_URL = os.environ.get(
    "VALKEY_BIN_URL",
    f"https://github.com/valkey-io/valkey/releases/download/"
    f"valkey-{VALKEY_VERSION}/valkey-{VALKEY_VERSION}-linux-arm64.tar.gz",
)


# ---------------------------------------------------------------------------
# Client discovery helpers
# ---------------------------------------------------------------------------

def client_key_path(client_seed):
    """Return the deterministic local SSH key path for a client seed."""
    return Path(__file__).resolve().parent / f"{KEY_NAME_PREFIX}-{client_seed}.pem"


def discover_client(ec2_client, client_seed, server_stack=None,
                    states=("pending", "running")):
    """Discover a benchmark client from AWS tags.

    Local JSON state is intentionally not part of this contract.  Client
    identity is the ``ClientSeed`` tag; ``server_stack`` is accepted only for
    legacy clients created before that tag existed.
    """
    if not client_seed:
        return None

    filters = [
        {"Name": "tag:ManagedBy", "Values": ["db-bench-client"]},
        {"Name": "tag:Role", "Values": ["bench-client"]},
        {"Name": "tag:ClientSeed", "Values": [client_seed]},
        {"Name": "instance-state-name", "Values": list(states)},
    ]
    matches = _discover_client_instances(ec2_client, filters, client_seed)

    if not matches and server_stack:
        legacy_filters = [
            {"Name": "tag:Project", "Values": [server_stack]},
            {"Name": "tag:Role", "Values": ["bench-client"]},
            {"Name": "tag:ManagedBy", "Values": ["db-bench-client"]},
            {"Name": "instance-state-name", "Values": list(states)},
        ]
        matches = _discover_client_instances(ec2_client, legacy_filters, client_seed)

    if not matches:
        return None
    matches.sort(key=lambda item: (
        item["state"] != "running",
        str(item.get("launch_time") or ""),
    ))
    if len(matches) > 1:
        log(f"WARNING: multiple bench clients found for seed {client_seed}; using {matches[0]['instance_id']}")
    return matches[0]


def _discover_client_instances(ec2_client, filters, client_seed):
    resp = ec2_client.describe_instances(Filters=filters)
    matches = []
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            sg_ids = [sg.get("GroupId", "") for sg in inst.get("SecurityGroups", [])]
            matches.append({
                "instance_id": inst["InstanceId"],
                "public_ip": inst.get("PublicIpAddress", ""),
                "private_ip": inst.get("PrivateIpAddress", ""),
                "vpc_id": inst.get("VpcId", ""),
                "subnet_id": inst.get("SubnetId", ""),
                "sg_ids": [sg_id for sg_id in sg_ids if sg_id],
                "key_path": str(client_key_path(client_seed)),
                "state": inst.get("State", {}).get("Name", "unknown"),
                "launch_time": inst.get("LaunchTime"),
                "tags": tags,
            })
    return matches


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


def _ensure_key_pair(ec2_client, key_name, key_dir, client_seed=None):
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
    tags = [
        {"Key": "Name", "Value": key_name},
        {"Key": "ManagedBy", "Value": "db-bench-client"},
    ]
    if client_seed:
        tags.append({"Key": "ClientSeed", "Value": client_seed})
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


def _ensure_client_sg(ec2_client, vpc_id, server_stack, vpc_cidr, ssh_cidr,
                      server_type=None, client_seed=None):
    """Create or reuse client SG allowing SSH from user + all outbound."""
    sg_name = f"db-bench-client-{client_seed}" if client_seed else f"{server_stack}-bench-client"
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
                    {"Key": "ServerType", "Value": server_type or ""},
                    {"Key": "ClientSeed", "Value": client_seed or ""},
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


def _authorize_client_db_access(ec2_client, server_stack, client_sg_id,
                                server_type):
    """Allow the benchmark client SG to reach server DB ports."""
    ports = CLIENT_DB_PORTS.get(server_type, [])
    if not ports:
        return
    resp = ec2_client.describe_security_groups(
        Filters=[{"Name": "tag:Project", "Values": [server_stack]}]
    )
    for sg in resp.get("SecurityGroups", []):
        sg_id = sg["GroupId"]
        if sg_id == client_sg_id or sg.get("GroupName") == "default":
            continue
        for port in ports:
            try:
                ec2_client.authorize_security_group_ingress(
                    GroupId=sg_id,
                    IpPermissions=[{
                        "IpProtocol": "tcp",
                        "FromPort": port,
                        "ToPort": port,
                        "UserIdGroupPairs": [{
                            "GroupId": client_sg_id,
                            "Description": "db-bench-client",
                        }],
                    }],
                )
                log(f"  Allowed client SG {client_sg_id} -> {sg_id}:{port}")
            except ec2_client.exceptions.ClientError as e:
                if "InvalidPermission.Duplicate" not in str(e):
                    raise


def _find_instance(ec2_client, name, server_stack, client_seed=None):
    """Find a running/pending client instance by Name + Project tags."""
    if client_seed:
        client = discover_client(
            ec2_client, client_seed, server_stack,
            states=("pending", "running", "stopping", "stopped"),
        )
        if client:
            return client["instance_id"]

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
                     subnet_id, sg_id, instance_type, key_name, key_path,
                     server_type=None, client_seed=None):
    """Launch the client EC2 instance (idempotent)."""
    instance_name = f"db-bench-client-{client_seed}" if client_seed else f"{server_stack}-bench-client"

    existing = _find_instance(ec2_client, instance_name, server_stack, client_seed)
    if existing:
        log(f"REUSED  client instance: {existing}")
        ec2_client.get_waiter("instance_running").wait(InstanceIds=[existing])
        inst = _describe_instance(ec2_client, existing)
        existing_vpc = inst.get("VpcId", "")
        if (existing_vpc and existing_vpc != vpc_id and
                server_type in ("aurora", "aurora-pg", "tidb", "valkey")):
            raise SystemExit(
                f"ERROR: Client seed {client_seed} already exists in VPC "
                f"{existing_vpc}, but {server_type} requires a client in "
                f"server VPC {vpc_id}. Use a client seed tied to this server "
                "stack or clean/recreate the client."
            )
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
                {"Key": "ServerType", "Value": server_type or ""},
                {"Key": "ClientSeed", "Value": client_seed or ""},
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

def cleanup_client(ec2_client, client_seed, server_stack=None):
    """Terminate benchmark client resources by client seed."""
    client = discover_client(
        ec2_client, client_seed, server_stack,
        states=("pending", "running", "stopping", "stopped"),
    )
    if client:
        iid = client["instance_id"]
        log(f"TERMINATING client instance: {iid}")
        ec2_client.terminate_instances(InstanceIds=[iid])
        try:
            ec2_client.get_waiter("instance_terminated").wait(InstanceIds=[iid])
        except Exception:
            log("  Warning: timeout waiting for termination; continuing")
    else:
        log(f"No client instance found for client seed {client_seed}.")

    sg_filters = [
        {"Name": "tag:ManagedBy", "Values": ["db-bench-client"]},
        {"Name": "tag:ClientSeed", "Values": [client_seed]},
    ]
    resp = ec2_client.describe_security_groups(Filters=sg_filters)
    legacy_groups = []
    if server_stack:
        legacy_resp = ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [f"{server_stack}-bench-client"]},
                     {"Name": "tag:Project", "Values": [server_stack]}]
        )
        legacy_groups = legacy_resp.get("SecurityGroups", [])

    seen = set()
    groups = list(resp.get("SecurityGroups", [])) + legacy_groups
    group_ids = [sg["GroupId"] for sg in groups]
    _remove_group_references(ec2_client, group_ids)
    for sg in groups:
        sg_id = sg["GroupId"]
        if sg_id in seen:
            continue
        seen.add(sg_id)
        log(f"DELETING client SG: {sg_id}")
        try:
            ec2_client.delete_security_group(GroupId=sg_id)
        except Exception as e:
            log(f"  Warning: could not delete SG {sg_id}: {e}")

    key_name = f"{KEY_NAME_PREFIX}-{client_seed}"
    try:
        ec2_client.delete_key_pair(KeyName=key_name)
        log(f"Deleted key pair: {key_name}")
    except Exception:
        pass
    pem_path = client_key_path(client_seed)
    if pem_path.exists():
        os.chmod(pem_path, 0o600)
        pem_path.unlink()
        log(f"Removed SSH key: {pem_path}")

    log("Client cleanup complete.")


def _remove_group_references(ec2_client, group_ids):
    if not group_ids:
        return
    resp = ec2_client.describe_security_groups()
    for sg in resp.get("SecurityGroups", []):
        sg_id = sg["GroupId"]
        ingress_revoke = []
        for perm in sg.get("IpPermissions", []):
            matching = [
                pair for pair in perm.get("UserIdGroupPairs", [])
                if pair.get("GroupId") in group_ids
            ]
            if matching:
                entry = {"IpProtocol": perm.get("IpProtocol", "-1"),
                         "UserIdGroupPairs": matching}
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                ingress_revoke.append(entry)
        if ingress_revoke:
            try:
                ec2_client.revoke_security_group_ingress(
                    GroupId=sg_id, IpPermissions=ingress_revoke)
            except Exception as e:
                log(f"  Warning: could not revoke ingress refs from {sg_id}: {e}")

        egress_revoke = []
        for perm in sg.get("IpPermissionsEgress", []):
            matching = [
                pair for pair in perm.get("UserIdGroupPairs", [])
                if pair.get("GroupId") in group_ids
            ]
            if matching:
                entry = {"IpProtocol": perm.get("IpProtocol", "-1"),
                         "UserIdGroupPairs": matching}
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                egress_revoke.append(entry)
        if egress_revoke:
            try:
                ec2_client.revoke_security_group_egress(
                    GroupId=sg_id, IpPermissions=egress_revoke)
            except Exception as e:
                log(f"  Warning: could not revoke egress refs from {sg_id}: {e}")


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
    _install_psql_client(host_ip, key_path, timeout=timeout)
    _install_sysbench(host_ip, key_path, timeout=timeout)
    _install_valkey_tools(host_ip, key_path, timeout=timeout)
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
            gcc gcc-c++ make automake autoconf libtool git jq htop sysstat mtr \\
            openssl-devel pkg-config binutils iproute \\
            tar xz ethtool iperf3 tmux jemalloc-devel
        sudo $PKG -y install perf || true
    """, timeout=timeout, strict=True)


def _tune_sysctl(host_ip, key_path):
    log(f"  Tuning sysctl on {host_ip}")
    extra = """\
net.ipv4.ip_forward = 1

kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0"""
    ssh_run_simple(host_ip, key_path, system_tuning_script(
        extra_sysctl=extra, conf_name="db-bench",
    ), strict=True)


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
        mysql --version
    """, timeout=timeout, strict=True)


def _install_psql_client(host_ip, key_path, timeout=300):
    log(f"  Installing psql client on {host_ip}")
    ssh_run_simple(host_ip, key_path, """
        if command -v psql >/dev/null 2>&1; then
            psql --version
            exit 0
        fi
        if command -v dnf >/dev/null 2>&1; then
            sudo dnf -y install postgresql16 postgresql16-contrib 2>/dev/null || \
            sudo dnf -y install postgresql15 postgresql15-contrib 2>/dev/null || \
            sudo dnf -y install postgresql postgresql-contrib 2>/dev/null || true
        else
            sudo yum -y install postgresql postgresql-contrib 2>/dev/null || true
        fi
        psql --version
    """, timeout=timeout, strict=True)


def _install_sysbench(host_ip, key_path, timeout=600):
    log(f"  Installing sysbench on {host_ip}")
    # Build with BOTH MySQL and PostgreSQL drivers so the same binary
    # benchmarks Aurora MySQL/TiDB (mysql) and Aurora PG/DSQL (pgsql).
    ssh_run_simple(host_ip, key_path, """
        # Idempotent: skip only if existing binary already has both drivers.
        if command -v sysbench >/dev/null 2>&1; then
            BIN=$(command -v sysbench)
            HAS_PG=$(ldd "$BIN" 2>/dev/null | grep -ci 'libpq\\.' || true)
            HAS_MY=$(ldd "$BIN" 2>/dev/null | grep -ciE 'libmysqlclient\\.|libmariadb\\.' || true)
            if [ "$HAS_PG" -gt 0 ] && [ "$HAS_MY" -gt 0 ]; then
                echo "sysbench already installed (mysql + pgsql)"
                sysbench --version
                exit 0
            fi
            echo "sysbench present but missing driver(s); rebuilding..."
        fi
        set -euo pipefail
        # libpq for pgsql driver; mariadb-devel/mysql-devel for mysql driver.
        if command -v dnf >/dev/null 2>&1; then
            sudo dnf -y install postgresql-devel mariadb105-devel \
                || sudo dnf -y install postgresql-devel mysql-devel \
                || sudo dnf -y install postgresql-devel
        else
            sudo yum -y install postgresql-devel mysql-devel || true
        fi
        cd /tmp
        if [ ! -d sysbench ]; then
            git clone https://github.com/akopytov/sysbench.git
        fi
        cd sysbench
        git checkout 1.0.20
        make distclean >/dev/null 2>&1 || true
        ./autogen.sh
        ./configure --with-mysql --with-pgsql
        make -j$(nproc)
        sudo make install
        sudo ldconfig
        sysbench --version
        ldd "$(command -v sysbench)" | grep -Ei 'libpq|mysql|mariadb' || true
    """, timeout=timeout, strict=True)


def _install_valkey_tools(host_ip, key_path, timeout=600):
    log(f"  Installing valkey tools on {host_ip}")
    ssh_run_simple(host_ip, key_path, f"""
        if command -v valkey-cli >/dev/null 2>&1 && \
           command -v valkey-benchmark >/dev/null 2>&1; then
            echo "valkey tools already installed"
            exit 0
        fi
        if command -v dnf >/dev/null 2>&1; then
            sudo dnf -y install valkey 2>/dev/null || true
        else
            sudo yum -y install valkey 2>/dev/null || true
        fi
        if command -v valkey-cli >/dev/null 2>&1 && \
           command -v valkey-benchmark >/dev/null 2>&1; then
            exit 0
        fi
        set -euo pipefail
        WORK=$(mktemp -d /tmp/valkey-tools.XXXX)
        trap 'rm -rf "$WORK"' EXIT
        cd "$WORK"
        curl -fL -o valkey-bin.tgz '{VALKEY_BIN_URL}'
        tar -xzf valkey-bin.tgz
        CLI=$(find . -type f -name valkey-cli | head -n 1)
        BENCH=$(find . -type f -name valkey-benchmark | head -n 1)
        if [ -z "$CLI" ] || [ -z "$BENCH" ]; then
            echo "ERROR: valkey archive missing valkey-cli/valkey-benchmark" >&2
            exit 1
        fi
        sudo install -m 0755 "$CLI" /usr/local/bin/valkey-cli
        sudo install -m 0755 "$BENCH" /usr/local/bin/valkey-benchmark
        valkey-cli --version || true
        valkey-benchmark --help >/dev/null
    """, timeout=timeout, strict=True)


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
    """, timeout=timeout, strict=True)


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
  python3 -m common.client --cleanup --seed foo
""",
    )
    p.add_argument("--seed", required=True,
                   help="Server seed for provisioning; client seed for cleanup unless --bench-client-seed is set.")
    p.add_argument("--bench-client-seed", default=None,
                   help="Reusable client seed (default: --seed).")
    p.add_argument("--server-type", choices=SERVER_TYPES,
                   help="Type of database server (required unless --cleanup).")
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
    server_seed = args.seed
    client_seed = args.bench_client_seed or server_seed
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

    if args.cleanup:
        server_stack = (_compute_server_stack(server_type, server_seed)
                        if server_type else None)
        log(f"Cleaning up client seed {client_seed}...")
        cleanup_client(ec2_client, client_seed, server_stack)
        return

    if not server_type:
        raise SystemExit("ERROR: --server-type is required when provisioning a client.")

    server_stack = _compute_server_stack(server_type, server_seed)

    # Discover server VPC
    vpc_id, vpc_cidr, server_stack = discover_server_vpc(ec2_client, server_type, server_seed)

    # Find public subnet in server VPC
    subnet_id, az = _find_public_subnet(ec2_client, vpc_id, server_stack)

    # SSH CIDR for the user
    from common.util import my_public_cidr
    try:
        ssh_cidr = my_public_cidr()
    except Exception as exc:
        raise SystemExit(
            "ERROR: Could not detect your public IP for the client SSH "
            "security-group rule. Pass a working network environment and retry; "
            "refusing to open SSH to 0.0.0.0/0."
        ) from exc

    # Security group
    sg_id = _ensure_client_sg(
        ec2_client, vpc_id, server_stack, vpc_cidr, ssh_cidr,
        server_type, client_seed)
    _authorize_client_db_access(ec2_client, server_stack, sg_id, server_type)

    # Key pair
    key_name = f"{KEY_NAME_PREFIX}-{client_seed}"
    key_dir = Path(__file__).resolve().parent
    key_path = _ensure_key_pair(ec2_client, key_name, key_dir, client_seed)

    # Instance type from size preset
    instance_type = SIZE_PRESETS[args.size]

    # Provision client
    iid, pub_ip, priv_ip = provision_client(
        ec2_client, ssm_client, server_stack, vpc_id, vpc_cidr,
        subnet_id, sg_id, instance_type, key_name, key_path,
        server_type, client_seed,
    )

    # Install tools (superset)
    install_client_tools(pub_ip, key_path, server_type)

    # Summary
    print()
    print("=" * 60)
    print("Benchmark Client Ready")
    print("=" * 60)
    print(f"  Server type:     {server_type}")
    print(f"  Server seed:     {server_seed}")
    print(f"  Client seed:     {client_seed}")
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
    print(f"  python3 -m common.benchmark --server-type {server_type} --seed {server_seed} --bench-client-seed {client_seed} ...")
    print()
    print(f"Cleanup:")
    print(f"  python3 -m common.client --cleanup --seed {server_seed} --bench-client-seed {client_seed}")
    print()


if __name__ == "__main__":
    main()
