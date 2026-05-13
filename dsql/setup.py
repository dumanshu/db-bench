#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
DSQL Load Test Stack Provisioner

Provisions AWS infrastructure for benchmarking Amazon Aurora DSQL:
- 1 DSQL cluster (managed serverless, created via AWS API)
- VPC/subnet/SG where an optional common benchmark client can be provisioned

DSQL is a fully managed serverless database -- there are no server EC2 instances
to provision.  Benchmark clients are provisioned through ``common.client`` and
discovered by AWS tags, not local state files.
"""

import argparse
import botocore
import os

import common.util as _cu
import common.aws as _caws
from common.util import (
    log, my_public_cidr, db_session, ec2, tags_common,
    configure_from_args as _common_configure_from_args,
    BOTO_CONFIG,
)
from common.aws import (
    ensure_vpc as _common_ensure_vpc,
    ensure_igw, ensure_subnet, ensure_public_rtb,
    ensure_sg, refresh_ssh_rule,
    cleanup_stack as _common_cleanup_stack,
)

SEED = "dsqllt-001"

# Network
VPC_CIDR = "10.44.0.0/16"
PUB_CIDR = "10.44.1.0/24"

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Provision DSQL load test stack (DSQL cluster plus optional client VM)."
    )
    parser.add_argument("--region", default=_cu.REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Unique seed used in stack name.")
    parser.add_argument("--owner", default=os.environ.get("OWNER", ""), help="Owner tag value.")
    parser.add_argument("--ssh-cidr", help="CIDR allowed for SSH (default: detected public IP /32).")
    parser.add_argument("--aws-profile", help="AWS profile for infrastructure (EC2/VPC).")
    parser.add_argument("--db-profile", help="AWS profile for database service APIs (default: sandbox-storage).")
    parser.add_argument(
        "--bench-client-seed",
        default=None,
        help="Benchmark client seed to clean up (default: --seed).",
    )
    parser.add_argument(
        "--keep-client",
        action="store_true",
        help="Do not clean the benchmark client during --cleanup.",
    )
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources.")
    parser.add_argument(
        "--deletion-protection",
        action="store_true",
        help="Enable DSQL cluster deletion protection.",
    )
    return parser


# ---------------------------------------------------------------------------
# Configure
# ---------------------------------------------------------------------------

def configure_from_args(args):
    """Configure runtime from CLI args, then fix common.aws stale bindings."""
    _common_configure_from_args(args, "dsql-loadtest")
    _caws.STACK = _cu.STACK
    _caws.REGION = _cu.REGION


def ensure_vpc():
    return _common_ensure_vpc(VPC_CIDR)


# ---------------------------------------------------------------------------
# DSQL cluster lifecycle
# ---------------------------------------------------------------------------

def dsql_client():
    return db_session().client("dsql", region_name=_cu.REGION, config=BOTO_CONFIG)


def create_dsql_cluster(deletion_protection=False):
    """Create a DSQL cluster and wait for it to become ACTIVE."""
    client = dsql_client()
    cluster_tags = {t["Key"]: t["Value"] for t in tags_common()}
    cluster_tags["Name"] = f"{_cu.STACK}-dsql"

    log("Creating DSQL cluster...")
    resp = client.create_cluster(
        deletionProtectionEnabled=deletion_protection,
        tags=cluster_tags,
    )
    cluster_id = resp["identifier"]
    endpoint = resp.get("endpoint", "")
    log(f"CREATED DSQL cluster: {cluster_id}")
    if endpoint:
        log(f"  endpoint: {endpoint}")

    # Wait for cluster to become ACTIVE (waiter: delay=15s, max ~20min)
    log("Waiting for DSQL cluster to become ACTIVE...")
    waiter = client.get_waiter("cluster_active")
    waiter.wait(identifier=cluster_id, WaiterConfig={"Delay": 15, "MaxAttempts": 80})

    # Re-fetch to get final endpoint
    info = client.get_cluster(identifier=cluster_id)
    endpoint = info.get("endpoint", endpoint)
    status = info.get("status", "unknown")
    log(f"DSQL cluster ACTIVE: {cluster_id} (endpoint: {endpoint}, status: {status})")
    return cluster_id, endpoint


def delete_dsql_cluster(cluster_id):
    """Delete a DSQL cluster and wait for it to disappear."""
    client = dsql_client()
    try:
        # Disable deletion protection first (in case it was enabled)
        try:
            client.update_cluster(
                identifier=cluster_id,
                deletionProtectionEnabled=False,
            )
        except Exception:
            pass

        log(f"Deleting DSQL cluster: {cluster_id}")
        client.delete_cluster(identifier=cluster_id)

        log("Waiting for DSQL cluster to be deleted...")
        waiter = client.get_waiter("cluster_not_exists")
        waiter.wait(identifier=cluster_id, WaiterConfig={"Delay": 15, "MaxAttempts": 80})
        log(f"DELETED DSQL cluster: {cluster_id}")
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("ResourceNotFoundException",):
            log(f"DSQL cluster {cluster_id} already deleted.")
        else:
            raise


def find_dsql_cluster():
    """Try to find an existing DSQL cluster by AWS tags."""
    client = dsql_client()
    token = None
    while True:
        kwargs = {"maxResults": 100}
        if token:
            kwargs["nextToken"] = token
        resp = client.list_clusters(**kwargs)
        for cluster in resp.get("clusters", []):
            cid = cluster.get("identifier", "")
            if not cid:
                continue
            info = client.get_cluster(identifier=cid)
            tags = info.get("tags", {}) or {}
            status = info.get("status", "")
            if (tags.get("Project") == _cu.STACK and
                    status in ("ACTIVE", "CREATING", "UPDATING")):
                endpoint = info.get("endpoint", "")
                log(f"REUSED  DSQL cluster by tags: {cid} (status: {status})")
                return cid, endpoint
        token = resp.get("nextToken")
        if not token:
            break
    return None, None


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(args):
    """Full cleanup: DSQL cluster + EC2 infrastructure."""
    log(f"Cleanup requested for stack: {_cu.STACK} in {_cu.REGION}")

    if not args.keep_client:
        from common.client import cleanup_client
        client_seed = args.bench_client_seed or args.seed
        cleanup_client(ec2(), client_seed, _cu.STACK)

    cluster_id, _endpoint = find_dsql_cluster()
    if cluster_id:
        delete_dsql_cluster(cluster_id)
    else:
        log("No DSQL cluster found by tags; skipping cluster deletion.")

    # Clean up EC2 infrastructure (VPC, subnet, SG, instances) via common
    _common_cleanup_stack()

    log("Cleanup complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = parse_args()
    args = parser.parse_args()
    configure_from_args(args)

    if args.cleanup:
        cleanup(args)
        return

    self_cidr = args.ssh_cidr or my_public_cidr()
    log(f"Stack: {_cu.STACK} | Region: {_cu.REGION} | SSH CIDR: {self_cidr}")

    # ── PHASE 1: EC2 Infrastructure ──────────────────────────────────────
    log("")
    log("=" * 70)
    log("PHASE 1: EC2 Infrastructure")
    log("=" * 70)

    vpc_id = ensure_vpc()
    igw_id = ensure_igw(vpc_id)
    subnet_id = ensure_subnet(
        vpc_id, f"{_cu.STACK}-pub", PUB_CIDR,
        az=f"{_cu.REGION}a", public=True,
    )
    ensure_public_rtb(vpc_id, igw_id, subnet_id)

    sg_id = ensure_sg(vpc_id, f"{_cu.STACK}-sg", "DSQL bench security group")
    refresh_ssh_rule(sg_id, self_cidr)

    # ── PHASE 2: DSQL Cluster ────────────────────────────────────────────
    log("")
    log("=" * 70)
    log("PHASE 2: DSQL Cluster")
    log("=" * 70)

    cluster_id, endpoint = find_dsql_cluster()
    if not cluster_id:
        cluster_id, endpoint = create_dsql_cluster(
            deletion_protection=args.deletion_protection,
        )

    # ── Summary ──────────────────────────────────────────────────────────
    log("")
    log("=" * 70)
    log("SETUP COMPLETE")
    log("=" * 70)
    log(f"  Stack:           {_cu.STACK}")
    log(f"  Region:          {_cu.REGION}")
    log("  Client VM:       provision separately with common.client")
    log(f"  DSQL Cluster:    {cluster_id}")
    log(f"  DSQL Endpoint:   {endpoint}")
    log("")
    log("Next steps:")
    client_seed = args.bench_client_seed or args.seed
    log(
        "  Client:    python3 -m common.client "
        f"--seed {args.seed} --bench-client-seed {client_seed} "
        f"--server-type dsql --size small --region {_cu.REGION} "
        f"--aws-profile {_cu.AWS_PROFILE}"
    )
    log(
        "  Benchmark: python3 -m dsql.benchmark --action run "
        f"--seed {args.seed} --bench-client-seed {client_seed} "
        f"--dsql-region {_cu.REGION} --dsql-db-profile {_cu.DB_PROFILE} "
        f"--aws-profile {_cu.AWS_PROFILE}"
    )
    log(f"  Validate:  python3 -m dsql.validate --seed {args.seed}")
    log(f"  Cleanup:   python3 -m dsql.setup --seed {args.seed} --cleanup")


if __name__ == "__main__":
    main()
