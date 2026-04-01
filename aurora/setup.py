#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
aurora/setup.py -- Provision Aurora MySQL + EC2 benchmark stack in AWS sandbox.

Creates:
  - VPC with 2 subnets (different AZs), IGW, route table
  - Security groups for Aurora (3306 from EC2) and EC2 (SSH + outbound)
  - SSH key pair (saved to <script_dir>/aurora-bench-key.pem)
  - Aurora cluster parameter group (binlog=ROW, transaction_isolation=READ-COMMITTED)
  - Aurora DB subnet group
  - Aurora MySQL cluster (IO-Optimized, aurora-iopt1)
  - Aurora writer DB instance
  - EC2 client instance (c8g.24xlarge default) with sysbench + mysql-client installed

Snapshot workflow (avoids re-running fill phase):
  1. Provision + fill data as usual
  2. python3 -m aurora.setup --snapshot --seed auroralt-005    # snapshot the cluster
  3. python3 -m aurora.setup --cleanup --seed auroralt-005     # tear down (snapshot persists)
  4. python3 -m aurora.setup --restore-snapshot <snapshot-id>  # restore with all data

Usage:
    python3 -m aurora.setup
    python3 -m aurora.setup --instance-type db.r8g.16xlarge
    python3 -m aurora.setup --cleanup
    python3 -m aurora.setup --cleanup --seed auroralt-002
    python3 -m aurora.setup --snapshot --seed auroralt-005
    python3 -m aurora.setup --restore-snapshot aurora-bench-auroralt-005-snap-20260320
    python3 -m aurora.setup --list-snapshots
    python3 -m aurora.setup --delete-snapshots
"""

import argparse
import json
import os
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, WaiterError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip3 install boto3")
    sys.exit(1)

import common.util as _cu
import common.aws as _caws
from common.util import ts, log, configure_runtime, my_public_cidr, tags_common, ensure_tags

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "auroralt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
STACK_PREFIX = "aurora-bench"
KEY_NAME = "aurora-bench-key"

VPC_CIDR = "10.44.0.0/16"
SUBNET_A_CIDR = "10.44.1.0/24"
SUBNET_B_CIDR = "10.44.2.0/24"

DEFAULT_AURORA_INSTANCE = "db.r8g.xlarge"

AURORA_ENGINE = "aurora-mysql"
AURORA_ENGINE_VERSION = "8.0.mysql_aurora.3.12.0"
AURORA_MASTER_USER = "admin"
AURORA_PORT = 3306

AL2023_SSM_X86 = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
AL2023_SSM_ARM64 = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"

# Instance families that use ARM64 (Graviton)
ARM64_PREFIXES = ("c6g", "c7g", "c8g", "m6g", "m7g", "m8g", "r6g", "r7g", "r8g", "t4g")

STATE_FILE = "aurora-bench-state.json"
VERBOSE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def vlog(msg: str) -> None:
    if VERBOSE:
        log(f"  {msg}")


def get_session(region: str, profile: str) -> boto3.Session:
    return boto3.Session(region_name=region, profile_name=profile)


def _tag_spec(resource_type, name_suffix=""):
    """Build TagSpecifications using common tags plus aurora-bench ManagedBy."""
    name = f"{_cu.STACK}-{name_suffix}" if name_suffix else _cu.STACK
    return [{
        "ResourceType": resource_type,
        "Tags": tags_common() + [
            {"Key": "Name", "Value": name},
            {"Key": "ManagedBy", "Value": "aurora-bench"},
        ],
    }]


def rds_tags(name_suffix: str = "") -> list[dict]:
    name = f"{_cu.STACK}-{name_suffix}" if name_suffix else _cu.STACK
    return tags_common() + [
        {"Key": "Name", "Value": name},
        {"Key": "ManagedBy", "Value": "aurora-bench"},
    ]


def save_state(state: dict, script_dir: Path) -> None:
    path = script_dir / STATE_FILE
    with open(path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    log(f"State saved to {path}")


def load_state(script_dir: Path) -> dict:
    path = script_dir / STATE_FILE
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Configuration -- patch common.aws module-level bindings after configure
# ---------------------------------------------------------------------------
def configure_from_args(args):
    _cu.configure_runtime(
        region=args.region,
        seed=args.seed,
        aws_profile=args.aws_profile,
        stack_prefix="aurora-bench",
    )
    _caws.STACK = _cu.STACK
    _caws.REGION = _cu.REGION


# ---------------------------------------------------------------------------
# AMI lookup
# ---------------------------------------------------------------------------
def is_arm64_instance(instance_type: str) -> bool:
    family = instance_type.split(".")[0]
    return any(family.startswith(p) for p in ARM64_PREFIXES)


def get_al2023_ami(session: boto3.Session, ec2_instance_type: str = "") -> str:
    ssm = session.client("ssm")
    if is_arm64_instance(ec2_instance_type):
        param = AL2023_SSM_ARM64
        arch = "arm64"
    else:
        param = AL2023_SSM_X86
        arch = "x86_64"
    resp = ssm.get_parameter(Name=param)
    ami_id = resp["Parameter"]["Value"]
    log(f"AL2023 AMI ({arch}): {ami_id}")
    return ami_id


# ---------------------------------------------------------------------------
# Networking: VPC, subnets, IGW, route table  (uses common.aws)
# ---------------------------------------------------------------------------
def create_networking(ec2_client) -> dict:
    """Provision VPC, two subnets in different AZs, IGW, and route table."""
    log("Creating VPC...")
    vpc_id = _caws.ensure_vpc(VPC_CIDR)

    # Two AZs required for Aurora subnet group
    azs = ec2_client.describe_availability_zones(
        Filters=[{"Name": "state", "Values": ["available"]}]
    )["AvailabilityZones"]
    az_a = azs[0]["ZoneName"]
    az_b = azs[1]["ZoneName"]

    log("Creating subnets...")
    subnet_a_id = _caws.ensure_subnet(
        vpc_id, f"{_cu.STACK}-subnet-a", SUBNET_A_CIDR, az=az_a, public=True,
    )
    subnet_b_id = _caws.ensure_subnet(
        vpc_id, f"{_cu.STACK}-subnet-b", SUBNET_B_CIDR, az=az_b, public=True,
    )
    log(f"  Subnets: {subnet_a_id} ({az_a}), {subnet_b_id} ({az_b})")

    log("Creating internet gateway...")
    igw_id = _caws.ensure_igw(vpc_id)

    log("Creating route table...")
    rtb_id = _caws.ensure_public_rtb(vpc_id, igw_id, [subnet_a_id, subnet_b_id])

    return {
        "vpc_id": vpc_id,
        "subnet_a_id": subnet_a_id,
        "subnet_b_id": subnet_b_id,
        "az_a": az_a,
        "az_b": az_b,
        "igw_id": igw_id,
        "rtb_id": rtb_id,
    }


# ---------------------------------------------------------------------------
# Security groups  (uses common.aws + aurora-specific UserIdGroupPairs)
# ---------------------------------------------------------------------------
def create_security_groups(ec2_client, vpc_id: str) -> dict:
    log("Creating security groups...")

    rds_sg_id = _caws.ensure_sg(
        vpc_id,
        f"{_cu.STACK}-rds-sg",
        f"Aurora bench RDS ({_cu.STACK})",
    )
    _caws.ensure_ingress_tcp_cidr(rds_sg_id, AURORA_PORT, VPC_CIDR)
    log(f"  RDS SG: {rds_sg_id} (MySQL from VPC CIDR)")

    return {"rds_sg_id": rds_sg_id}


# ---------------------------------------------------------------------------
# SSH key pair
# ---------------------------------------------------------------------------
def create_key_pair(ec2_client, script_dir: Path) -> str:
    key_file = script_dir / f"{KEY_NAME}.pem"

    if key_file.exists():
        log(f"Key file already exists: {key_file}")
        try:
            ec2_client.describe_key_pairs(KeyNames=[KEY_NAME])
            log(f"  Key pair '{KEY_NAME}' exists in AWS")
            return str(key_file)
        except ClientError:
            pass  # key exists locally but not in AWS, recreate

    # Remove stale AWS key if present
    try:
        ec2_client.delete_key_pair(KeyName=KEY_NAME)
    except ClientError:
        pass

    log(f"Creating key pair '{KEY_NAME}'...")
    kp = ec2_client.create_key_pair(
        KeyName=KEY_NAME,
        KeyType="rsa",
        TagSpecifications=_tag_spec("key-pair", "key"),
    )
    if key_file.exists():
        os.chmod(key_file, 0o600)
        key_file.unlink()
    key_file.write_text(kp["KeyMaterial"])
    os.chmod(key_file, 0o400)
    log(f"  Saved to {key_file}")
    return str(key_file)


# ---------------------------------------------------------------------------
# Aurora cluster parameter group
# ---------------------------------------------------------------------------
def create_parameter_group(rds) -> str:
    pg_name = f"{_cu.STACK}-cluster-pg"
    log(f"Creating cluster parameter group '{pg_name}'...")

    try:
        rds.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=pg_name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description=f"Aurora bench cluster params ({_cu.STACK})",
            Tags=rds_tags("cluster-pg"),
        )
    except ClientError as e:
        if "already exists" in str(e):
            log(f"  Parameter group '{pg_name}' already exists")
        else:
            raise

    params = [
        {"ParameterName": "binlog_format", "ParameterValue": "ROW",
         "ApplyMethod": "pending-reboot"},
        {"ParameterName": "transaction_isolation", "ParameterValue": "READ-COMMITTED",
         "ApplyMethod": "pending-reboot"},
        {"ParameterName": "collation_server", "ParameterValue": "utf8mb4_unicode_ci",
         "ApplyMethod": "pending-reboot"},
        {"ParameterName": "sql_mode",
         "ParameterValue": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
         "ApplyMethod": "pending-reboot"},
    ]
    rds.modify_db_cluster_parameter_group(
        DBClusterParameterGroupName=pg_name,
        Parameters=params,
    )
    log("  Parameters set: binlog_format=ROW, transaction_isolation=READ-COMMITTED, "
        "collation=utf8mb4_unicode_ci, sql_mode=STRICT")
    return pg_name


# ---------------------------------------------------------------------------
# Aurora DB subnet group
# ---------------------------------------------------------------------------
def create_subnet_group(rds, subnet_ids: list[str]) -> str:
    sg_name = f"{_cu.STACK}-subnet-group"
    log(f"Creating DB subnet group '{sg_name}'...")

    try:
        rds.create_db_subnet_group(
            DBSubnetGroupName=sg_name,
            DBSubnetGroupDescription=f"Aurora bench subnets ({_cu.STACK})",
            SubnetIds=subnet_ids,
            Tags=rds_tags("subnet-group"),
        )
    except ClientError as e:
        if "already exists" in str(e):
            log(f"  Subnet group '{sg_name}' already exists")
        else:
            raise

    return sg_name


# ---------------------------------------------------------------------------
# Aurora cluster
# ---------------------------------------------------------------------------
def create_aurora_cluster(rds, subnet_group: str, pg_name: str,
                          rds_sg_id: str, password: str, azs: list[str]) -> dict:
    cluster_id = f"{_cu.STACK}-cluster"
    log(f"Creating Aurora cluster '{cluster_id}'...")

    try:
        rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine=AURORA_ENGINE,
            EngineVersion=AURORA_ENGINE_VERSION,
            MasterUsername=AURORA_MASTER_USER,
            MasterUserPassword=password,
            DBSubnetGroupName=subnet_group,
            VpcSecurityGroupIds=[rds_sg_id],
            DBClusterParameterGroupName=pg_name,
            Port=AURORA_PORT,
            StorageType="aurora-iopt1",
            AvailabilityZones=azs,
            BackupRetentionPeriod=1,
            DeletionProtection=False,
            Tags=rds_tags("cluster"),
        )
    except ClientError as e:
        if "already exists" in str(e):
            log(f"  Cluster '{cluster_id}' already exists")
        else:
            raise

    log("  Waiting for cluster to become available (5-10 min)...")
    waiter = rds.get_waiter("db_cluster_available")
    waiter.wait(
        DBClusterIdentifier=cluster_id,
        WaiterConfig={"Delay": 30, "MaxAttempts": 60},
    )

    resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    endpoint = resp["DBClusters"][0]["Endpoint"]
    log(f"  Cluster available. Endpoint: {endpoint}")

    return {"cluster_id": cluster_id, "endpoint": endpoint}


# ---------------------------------------------------------------------------
# Aurora writer instance
# ---------------------------------------------------------------------------
def create_aurora_instance(rds, cluster_id: str,
                           instance_type: str, az: str) -> str:
    instance_id = f"{_cu.STACK}-writer"
    log(f"Creating Aurora instance '{instance_id}' ({instance_type})...")

    try:
        rds.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass=instance_type,
            Engine=AURORA_ENGINE,
            DBClusterIdentifier=cluster_id,
            AvailabilityZone=az,
            PubliclyAccessible=False,
            Tags=rds_tags("writer"),
        )
    except ClientError as e:
        if "already exists" in str(e):
            log(f"  Instance '{instance_id}' already exists")
        else:
            raise

    log("  Waiting for writer instance (5-15 min)...")
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(
        DBInstanceIdentifier=instance_id,
        WaiterConfig={"Delay": 30, "MaxAttempts": 60},
    )
    log(f"  Writer instance '{instance_id}' available")
    return instance_id


def create_aurora_readers(rds, cluster_id: str, instance_type: str,
                          azs: list[str], count: int) -> list[str]:
    """Create Aurora read replicas spread across AZs."""
    reader_ids = []
    for i in range(count):
        reader_id = f"{_cu.STACK}-reader-{i + 1}"
        az = azs[i % len(azs)]
        log(f"Creating Aurora reader '{reader_id}' ({instance_type}) in {az}...")
        try:
            rds.create_db_instance(
                DBInstanceIdentifier=reader_id,
                DBInstanceClass=instance_type,
                Engine=AURORA_ENGINE,
                DBClusterIdentifier=cluster_id,
                AvailabilityZone=az,
                PubliclyAccessible=False,
                Tags=rds_tags(f"reader-{i + 1}"),
            )
        except ClientError as e:
            if "already exists" in str(e):
                log(f"  Reader '{reader_id}' already exists")
            else:
                raise
        reader_ids.append(reader_id)

    for rid in reader_ids:
        log(f"  Waiting for reader '{rid}'...")
        waiter = rds.get_waiter("db_instance_available")
        waiter.wait(
            DBInstanceIdentifier=rid,
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},
        )
        log(f"  Reader '{rid}' available")

    return reader_ids


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------
def create_snapshot(rds, cluster_id: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snap_id = f"{_cu.STACK}-snap-{stamp}"
    log(f"Creating cluster snapshot '{snap_id}'...")
    rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=snap_id,
        DBClusterIdentifier=cluster_id,
        Tags=rds_tags("snapshot"),
    )
    log("  Waiting for snapshot to complete (may take 10-30 min for large datasets)...")
    waiter = rds.get_waiter("db_cluster_snapshot_available")
    waiter.wait(
        DBClusterSnapshotIdentifier=snap_id,
        WaiterConfig={"Delay": 30, "MaxAttempts": 120},
    )
    resp = rds.describe_db_cluster_snapshots(
        DBClusterSnapshotIdentifier=snap_id)
    snap = resp["DBClusterSnapshots"][0]
    size_gb = snap.get("AllocatedStorage", 0)
    log(f"  Snapshot ready: {snap_id} ({size_gb} GB)")
    return snap_id


def list_snapshots(rds) -> list[dict]:
    snapshots = []
    paginator = rds.get_paginator("describe_db_cluster_snapshots")
    for page in paginator.paginate(SnapshotType="manual"):
        for snap in page.get("DBClusterSnapshots", []):
            if snap["DBClusterSnapshotIdentifier"].startswith(STACK_PREFIX):
                snapshots.append(snap)
    return snapshots


def print_snapshots(rds) -> None:
    snaps = list_snapshots(rds)
    if not snaps:
        log("No aurora-bench snapshots found.")
        return
    log(f"Found {len(snaps)} aurora-bench snapshot(s):")
    print()
    print(f"  {'Snapshot ID':<55} {'Status':<12} {'Size GB':>8} {'Created'}")
    print(f"  {'-'*55} {'-'*12} {'-'*8} {'-'*20}")
    for s in sorted(snaps, key=lambda x: x.get("SnapshotCreateTime", "")):
        sid = s["DBClusterSnapshotIdentifier"]
        status = s.get("Status", "?")
        size = s.get("AllocatedStorage", 0)
        created = str(s.get("SnapshotCreateTime", "?"))[:19]
        print(f"  {sid:<55} {status:<12} {size:>8} {created}")
    print()


def delete_all_snapshots(rds) -> None:
    snaps = list_snapshots(rds)
    if not snaps:
        log("No aurora-bench snapshots to delete.")
        return
    for s in snaps:
        sid = s["DBClusterSnapshotIdentifier"]
        log(f"Deleting snapshot '{sid}'...")
        try:
            rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=sid)
            log(f"  Deleted")
        except ClientError as e:
            log(f"  Error: {e}")
    log(f"Deleted {len(snaps)} snapshot(s).")


def restore_cluster_from_snapshot(rds, snapshot_id: str,
                                  subnet_group: str, rds_sg_id: str,
                                  pg_name: str) -> dict:
    cluster_id = f"{_cu.STACK}-cluster"
    log(f"Restoring cluster '{cluster_id}' from snapshot '{snapshot_id}'...")
    try:
        rds.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=cluster_id,
            SnapshotIdentifier=snapshot_id,
            Engine=AURORA_ENGINE,
            EngineVersion=AURORA_ENGINE_VERSION,
            DBSubnetGroupName=subnet_group,
            VpcSecurityGroupIds=[rds_sg_id],
            DBClusterParameterGroupName=pg_name,
            Port=AURORA_PORT,
            StorageType="aurora-iopt1",
            DeletionProtection=False,
            Tags=rds_tags("cluster"),
        )
    except ClientError as e:
        if "already exists" in str(e):
            log(f"  Cluster '{cluster_id}' already exists")
        else:
            raise

    log("  Waiting for restored cluster to become available (5-15 min)...")
    waiter = rds.get_waiter("db_cluster_available")
    waiter.wait(
        DBClusterIdentifier=cluster_id,
        WaiterConfig={"Delay": 30, "MaxAttempts": 60},
    )
    resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    endpoint = resp["DBClusters"][0]["Endpoint"]
    log(f"  Cluster restored. Endpoint: {endpoint}")
    return {"cluster_id": cluster_id, "endpoint": endpoint}


# ---------------------------------------------------------------------------
# Cleanup (tear down entire stack) -- aurora-specific, manages RDS resources
# ---------------------------------------------------------------------------
def cleanup_stack(session: boto3.Session, region: str, stack: str,
                  rds_session: boto3.Session | None = None,
                  delete_snapshots: bool = False) -> None:
    ec2_client = session.client("ec2")
    rds = (rds_session or session).client("rds")

    # Fail-fast: verify RDS credentials before proceeding.
    # If the RDS profile lacks permissions, we must not proceed to
    # EC2/VPC cleanup and state file deletion -- that would leave
    # orphaned RDS resources with no way to re-run cleanup.
    try:
        rds.describe_db_clusters(MaxRecords=20)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDenied", "AccessDeniedException",
                    "InvalidClientTokenId", "ExpiredTokenException",
                    "UnrecognizedClientException"):
            log(f"FATAL: RDS credentials check failed ({code}). "
                f"Aborting cleanup to prevent orphaned RDS resources. "
                f"Fix --rds-profile and retry.")
            raise SystemExit(1)
        # Other errors (throttling, etc.) are not auth failures -- proceed

    cluster_id = f"{stack}-cluster"
    pg_name = f"{stack}-cluster-pg"
    sg_name = f"{stack}-subnet-group"

    # 1. Delete all Aurora DB instances in the cluster (writer + readers)
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        members = resp["DBClusters"][0].get("DBClusterMembers", [])
        instance_ids_to_delete = [m["DBInstanceIdentifier"] for m in members]
    except ClientError:
        instance_ids_to_delete = [f"{stack}-writer"]

    for inst_id in instance_ids_to_delete:
        log(f"Deleting Aurora instance '{inst_id}'...")
        try:
            rds.delete_db_instance(
                DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)
            log("  Waiting for instance deletion...")
            waiter = rds.get_waiter("db_instance_deleted")
            waiter.wait(DBInstanceIdentifier=inst_id,
                        WaiterConfig={"Delay": 30, "MaxAttempts": 60})
            log(f"  Instance '{inst_id}' deleted")
        except ClientError as e:
            if "not found" in str(e).lower() or "DBInstanceNotFound" in str(e):
                log(f"  Instance not found, skipping")
            else:
                log(f"  Error: {e}")

    # 2. Delete Aurora cluster
    log(f"Deleting Aurora cluster '{cluster_id}'...")
    try:
        rds.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
        log("  Waiting for cluster deletion...")
        waiter = rds.get_waiter("db_cluster_deleted")
        waiter.wait(DBClusterIdentifier=cluster_id,
                    WaiterConfig={"Delay": 30, "MaxAttempts": 60})
        log("  Cluster deleted")
    except ClientError as e:
        if "not found" in str(e).lower() or "DBClusterNotFound" in str(e):
            log(f"  Cluster not found, skipping")
        else:
            log(f"  Error: {e}")

    # 3. Delete parameter group
    log(f"Deleting parameter group '{pg_name}'...")
    try:
        rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName=pg_name)
        log("  Deleted")
    except ClientError as e:
        log(f"  Skipped: {e}")

    # 4. Delete subnet group
    log(f"Deleting subnet group '{sg_name}'...")
    try:
        rds.delete_db_subnet_group(DBSubnetGroupName=sg_name)
        log("  Deleted")
    except ClientError as e:
        log(f"  Skipped: {e}")

    # 5. Terminate EC2 instances
    log("Terminating EC2 instances...")
    ec2_resp = ec2_client.describe_instances(Filters=[
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["running", "stopped", "pending"]},
    ])
    instance_ids = []
    for r in ec2_resp.get("Reservations", []):
        for i in r["Instances"]:
            instance_ids.append(i["InstanceId"])
    if instance_ids:
        ec2_client.terminate_instances(InstanceIds=instance_ids)
        log(f"  Terminating: {instance_ids}")
        waiter = ec2_client.get_waiter("instance_terminated")
        waiter.wait(InstanceIds=instance_ids)
        log("  Terminated")
    else:
        log("  No EC2 instances found")

    # 6. Delete security groups (retry -- ENIs take a moment to detach)
    log("Deleting security groups...")
    for attempt in range(5):
        sg_resp = ec2_client.describe_security_groups(Filters=[
            {"Name": "tag:Project", "Values": [stack]},
        ])
        sgs = [sg for sg in sg_resp.get("SecurityGroups", [])
               if sg["GroupName"] != "default"]
        if not sgs:
            break
        for sg in sgs:
            try:
                ec2_client.delete_security_group(GroupId=sg["GroupId"])
                log(f"  Deleted SG: {sg['GroupId']}")
            except ClientError:
                if attempt < 4:
                    time.sleep(10)

    # 7. Delete internet gateway
    log("Deleting internet gateway...")
    igw_resp = ec2_client.describe_internet_gateways(Filters=[
        {"Name": "tag:Project", "Values": [stack]},
    ])
    for igw in igw_resp.get("InternetGateways", []):
        igw_id = igw["InternetGatewayId"]
        for att in igw.get("Attachments", []):
            ec2_client.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=att["VpcId"])
        ec2_client.delete_internet_gateway(InternetGatewayId=igw_id)
        log(f"  Deleted IGW: {igw_id}")

    # 8. Delete subnets
    log("Deleting subnets...")
    sub_resp = ec2_client.describe_subnets(Filters=[
        {"Name": "tag:Project", "Values": [stack]},
    ])
    for sub in sub_resp.get("Subnets", []):
        ec2_client.delete_subnet(SubnetId=sub["SubnetId"])
        log(f"  Deleted subnet: {sub['SubnetId']}")

    # 9. Delete route tables (non-main only)
    log("Deleting route tables...")
    rtb_resp = ec2_client.describe_route_tables(Filters=[
        {"Name": "tag:Project", "Values": [stack]},
    ])
    for rtb in rtb_resp.get("RouteTables", []):
        for assoc in rtb.get("Associations", []):
            if not assoc.get("Main", False):
                try:
                    ec2_client.disassociate_route_table(
                        AssociationId=assoc["RouteTableAssociationId"])
                except ClientError:
                    pass
        try:
            ec2_client.delete_route_table(RouteTableId=rtb["RouteTableId"])
            log(f"  Deleted RTB: {rtb['RouteTableId']}")
        except ClientError as e:
            vlog(f"  RTB skip: {e}")

    # 10. Delete VPC
    log("Deleting VPC...")
    vpc_resp = ec2_client.describe_vpcs(Filters=[
        {"Name": "tag:Project", "Values": [stack]},
    ])
    for vpc in vpc_resp.get("Vpcs", []):
        ec2_client.delete_vpc(VpcId=vpc["VpcId"])
        log(f"  Deleted VPC: {vpc['VpcId']}")

    # 11. Delete key pair from AWS (keep local .pem)
    log(f"Deleting key pair '{KEY_NAME}' from AWS...")
    try:
        ec2_client.delete_key_pair(KeyName=KEY_NAME)
        log("  Deleted")
    except ClientError:
        pass

    # 12. Delete snapshots (if requested)
    if delete_snapshots:
        log("Deleting aurora-bench snapshots...")
        delete_all_snapshots(rds)
    else:
        snaps = list_snapshots(rds)
        if snaps:
            log(f"  Keeping {len(snaps)} snapshot(s). "
                f"Use --delete-snapshots to remove them.")

    # 13. Remove state file
    script_dir = Path(__file__).resolve().parent
    state_path = script_dir / STATE_FILE
    if state_path.exists():
        state_path.unlink()
        log(f"  Removed {state_path}")

    log("Cleanup complete!")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Provision Aurora MySQL + EC2 benchmark stack.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python3 -m aurora.setup                             # Default (db.r8g.xlarge)
              python3 -m aurora.setup --instance-type db.r8g.16xlarge
              python3 -m aurora.setup --cleanup                   # Tear down everything
        """),
    )
    p.add_argument("--cleanup", action="store_true",
                   help="Tear down the entire stack and exit")
    p.add_argument("--delete-snapshots", action="store_true",
                   help="Also delete snapshots during cleanup (default: keep them)")
    p.add_argument("--snapshot", action="store_true",
                   help="Create a manual cluster snapshot and exit")
    p.add_argument("--restore-snapshot", default=None, metavar="SNAPSHOT_ID",
                   help="Restore cluster from snapshot instead of creating fresh")
    p.add_argument("--list-snapshots", action="store_true",
                   help="List available aurora-bench snapshots and exit")
    p.add_argument("--seed", default=DEFAULT_SEED,
                   help=f"Stack seed identifier (default: {DEFAULT_SEED})")
    p.add_argument("--region", default=DEFAULT_REGION,
                   help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--aws-profile", default=DEFAULT_PROFILE,
                   help=f"AWS CLI profile for EC2/VPC (default: {DEFAULT_PROFILE})")
    p.add_argument("--rds-profile", default="sandbox-storage",
                   help="AWS CLI profile for RDS operations (default: sandbox-storage)")
    p.add_argument("--instance-type", default=DEFAULT_AURORA_INSTANCE,
                   help=f"Aurora instance type (default: {DEFAULT_AURORA_INSTANCE})")
    p.add_argument("--replicas", type=int, default=1,
                   help="Number of Aurora read replicas (default: 1)")
    p.add_argument("--password", default=None,
                   help="Aurora master password (default: env AURORA_MASTER_PASSWORD "
                        "or BenchMark2024!)")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    # Configure common.util and common.aws module-level globals
    configure_from_args(args)

    stack = _cu.STACK
    password = args.password or os.environ.get("AURORA_MASTER_PASSWORD", "BenchMark2024!")
    script_dir = Path(__file__).resolve().parent
    rds_profile = args.rds_profile or args.aws_profile

    # Aurora manages its own sessions (dual profile support)
    session = get_session(args.region, args.aws_profile)
    rds_session = get_session(args.region, rds_profile) if rds_profile != args.aws_profile else session

    if args.list_snapshots:
        rds = rds_session.client("rds")
        print_snapshots(rds)
        return

    if args.delete_snapshots and not args.cleanup:
        rds = rds_session.client("rds")
        delete_all_snapshots(rds)
        return

    if args.cleanup:
        log(f"Cleaning up stack '{stack}' in {args.region}...")
        cleanup_stack(session, args.region, stack, rds_session,
                      delete_snapshots=args.delete_snapshots)
        return

    if args.snapshot:
        rds = rds_session.client("rds")
        cluster_id = f"{stack}-cluster"
        try:
            rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        except ClientError:
            log(f"ERROR: Cluster '{cluster_id}' not found. Nothing to snapshot.")
            sys.exit(1)
        snap_id = create_snapshot(rds, cluster_id)
        existing = load_state(script_dir)
        if existing:
            existing["last_snapshot_id"] = snap_id
            save_state(existing, script_dir)
        print()
        print(f"Snapshot created: {snap_id}")
        print(f"Restore with: python3 -m aurora.setup --restore-snapshot {snap_id} "
              f"--seed <new-seed>")
        print()
        return

    log(f"Provisioning stack '{stack}' in {args.region}...")
    log(f"  Aurora instance type: {args.instance_type}")
    log(f"  EC2/VPC profile:      {args.aws_profile}")
    log(f"  RDS profile:          {rds_profile}")
    if args.restore_snapshot:
        log(f"  Restoring from:       {args.restore_snapshot}")
    print()

    state = {"stack": stack, "region": args.region, "seed": args.seed}
    ec2_client = session.client("ec2")
    rds = rds_session.client("rds")

    net = create_networking(ec2_client)
    state.update(net)

    sgs = create_security_groups(ec2_client, net["vpc_id"])
    state.update(sgs)

    key_path = create_key_pair(ec2_client, script_dir)
    state["key_path"] = key_path

    pg_name = create_parameter_group(rds)
    state["parameter_group"] = pg_name

    subnet_group = create_subnet_group(rds, [net["subnet_a_id"], net["subnet_b_id"]])
    state["subnet_group"] = subnet_group

    if args.restore_snapshot:
        cluster = restore_cluster_from_snapshot(
            rds, args.restore_snapshot, subnet_group,
            sgs["rds_sg_id"], pg_name)
    else:
        cluster = create_aurora_cluster(rds, subnet_group, pg_name,
                                        sgs["rds_sg_id"], password,
                                        [net["az_a"], net["az_b"]])
    state.update(cluster)

    writer_id = create_aurora_instance(rds, cluster["cluster_id"],
                                       args.instance_type, net["az_a"])
    state["writer_id"] = writer_id
    state["aurora_instance_type"] = args.instance_type

    if args.replicas > 0:
        reader_ids = create_aurora_readers(
            rds, cluster["cluster_id"], args.instance_type,
            [net["az_a"], net["az_b"]], args.replicas)
        state["reader_ids"] = reader_ids
    state["replicas"] = args.replicas

    if args.restore_snapshot:
        state["restored_from_snapshot"] = args.restore_snapshot

    state["created_at"] = ts()
    save_state(state, script_dir)

    print()
    print("=" * 60)
    print("Aurora Server Stack Ready")
    print("=" * 60)
    print(f"  Stack:            {stack}")
    print(f"  Aurora endpoint:  {state['endpoint']}")
    print(f"  Aurora instance:  {args.instance_type}")
    print(f"  Replicas:         {args.replicas}")
    print(f"  Aurora storage:   aurora-iopt1 (IO-Optimized)")
    if args.restore_snapshot:
        print(f"  Restored from:    {args.restore_snapshot}")
    print()
    print("Next: provision a benchmark client in this VPC:")
    print(f"  python3 -m common.client --seed {args.seed} --server-type aurora --size small")
    print()
    if args.restore_snapshot:
        print("Data restored from snapshot. Run benchmark directly (skip fill + prepare):")
        print(f"  python3 -m aurora.benchmark --seed {args.seed} --skip-prepare")
    else:
        print("Validate:")
        print(f"  python3 -m aurora.validate --seed {args.seed}")
        print()
        print("Benchmark:")
        print(f"  python3 -m aurora.benchmark --seed {args.seed}")
    print()
    print("Snapshot (after fill):")
    print(f"  python3 -m aurora.setup --snapshot --seed {args.seed}")
    print()
    print("Cleanup (server only -- client cleaned separately):")
    print(f"  python3 -m aurora.setup --cleanup --seed {args.seed}")
    print(f"  python3 -m common.client --cleanup --seed {args.seed} --server-type aurora")
    print()


if __name__ == "__main__":
    main()
