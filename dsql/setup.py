#!/usr/bin/env python3
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
"""
DSQL Load Test Stack Provisioner

Provisions AWS infrastructure for benchmarking Amazon Aurora DSQL:
- 1 client EC2 (pgbench runner, Amazon Linux 2023 ARM64)
- 1 DSQL cluster (managed serverless, created via AWS API)
- VPC/subnet/SG for the client VM
- Security group rules for SSH access and DSQL connectivity (port 5432)

DSQL is a fully managed serverless database -- there are no server EC2 instances
to provision.  The client VM runs pgbench against the DSQL endpoint using IAM
authentication tokens.
"""

import argparse
import botocore
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import common.util as _cu
import common.aws as _caws
from common.util import (
    ts, log, need_cmd, my_public_cidr, aws_session, db_session, ec2, ssm,
    tags_common,
    configure_from_args as _common_configure_from_args,
    BOTO_CONFIG,
)
from common.types import InstanceInfo
from common.aws import (
    ensure_vpc as _common_ensure_vpc,
    ensure_igw, ensure_subnet, ensure_public_rtb,
    ensure_sg, ensure_ingress_tcp_cidr, refresh_ssh_rule,
    ensure_instance as _common_ensure_instance,
    ensure_keypair as _common_ensure_keypair,
    instance_info_from_id,
    cleanup_stack as _common_cleanup_stack,
)
from common.ssh import ssh_run, ssh_capture, wait_for_ssh

SEED = "dsqllt-001"

# Client instance types
CLIENT_INSTANCE_TYPE = "c7g.2xlarge"        # 8 vCPU, 16GB -- pgbench client
PRODUCTION_CLIENT_TYPE = "c7g.4xlarge"      # 16 vCPU, 32GB

# Network
VPC_CIDR = "10.44.0.0/16"
PUB_CIDR = "10.44.1.0/24"
DSQL_PORT = 5432

# AMI
AMI_OVERRIDE = os.environ.get("DSQL_AMI_ID")
AMI_SSM_PARAM = os.environ.get(
    "DSQL_AMI_PARAM",
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
)
_RESOLVED_AMI_ID = None

from common.aws import KEY_NAME, DEFAULT_SSH_KEY_PATH

# State file -- persists cluster info between setup / benchmark / validate
STATE_FILE = Path(__file__).resolve().with_name("dsql-state.json")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Provision DSQL load test stack (client VM + DSQL cluster)."
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
    parser.add_argument("--aws-profile", help="AWS profile for infrastructure (EC2/VPC).")
    parser.add_argument("--db-profile", help="AWS profile for database service APIs (default: sandbox-storage).")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Provision infrastructure only.")
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources.")
    parser.add_argument(
        "--production",
        action="store_true",
        help="Use larger client instance type for production benchmarks.",
    )
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


# ---------------------------------------------------------------------------
# AMI / Key pair
# ---------------------------------------------------------------------------

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
            f"ERROR: Unable to resolve AMI from SSM ({AMI_SSM_PARAM}): {msg}. "
            "Set DSQL_AMI_ID to override."
        )


def ensure_keypair_accessible():
    _common_ensure_keypair(KEY_NAME, DEFAULT_SSH_KEY_PATH)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    client: InstanceInfo
    jump_host: InstanceInfo = None  # No jump host for DSQL; set to client
    dsql_cluster_id: str = ""
    dsql_endpoint: str = ""

    def __post_init__(self):
        if self.jump_host is None:
            self.jump_host = self.client


# ---------------------------------------------------------------------------
# EC2
# ---------------------------------------------------------------------------

def ensure_vpc():
    return _common_ensure_vpc(VPC_CIDR)


def ensure_instance(name, role, itype, subnet_id, sg_id, root_volume_size=100):
    """Wrapper: delegate to common ensure_instance with dsql-specific AMI/key."""
    return _common_ensure_instance(
        name, role, itype, subnet_id, sg_id,
        resolved_ami_id, KEY_NAME, ensure_keypair_accessible,
        root_volume_size,
    )


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
    """Try to find an existing DSQL cluster from state file or by listing."""
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            cid = state.get("cluster_id", "")
            ep = state.get("endpoint", "")
            if cid:
                # Verify it still exists
                try:
                    info = dsql_client().get_cluster(identifier=cid)
                    status = info.get("status", "")
                    ep = info.get("endpoint", ep)
                    if status in ("ACTIVE", "CREATING", "UPDATING"):
                        log(f"REUSED  DSQL cluster from state: {cid} (status: {status})")
                        return cid, ep
                except botocore.exceptions.ClientError:
                    pass
        except (json.JSONDecodeError, OSError):
            pass
    return None, None


def generate_auth_token(endpoint):
    """Generate an IAM auth token for DSQL admin access."""
    client = dsql_client()
    token = client.generate_db_connect_admin_auth_token(
        Hostname=endpoint,
        Region=_cu.REGION,
        ExpiresIn=900,
    )
    return token


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def save_state(cluster_id, endpoint, client_instance_id, client_ip):
    state = {
        "stack": _cu.STACK,
        "region": _cu.REGION,
        "cluster_id": cluster_id,
        "endpoint": endpoint,
        "client_instance_id": client_instance_id,
        "client_ip": client_ip,
        "created_at": ts(),
    }
    STATE_FILE.write_text(json.dumps(state, indent=2))
    log(f"State saved to {STATE_FILE}")


def load_state():
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Client VM bootstrap
# ---------------------------------------------------------------------------

def bootstrap_client(ctx: BootstrapContext):
    log("Bootstrapping client VM...")

    ssh_run(ctx.client, """
sudo dnf -y update || true
sudo dnf -y install postgresql16 postgresql16-contrib jq htop sysstat || true
pgbench --version
psql --version
""", ctx)

    from common.client import system_tuning_script
    ssh_run(ctx.client, system_tuning_script(conf_name="dsql-bench"), ctx)

    # Verify connectivity to DSQL endpoint
    if ctx.dsql_endpoint:
        log("Verifying DSQL endpoint connectivity from client VM...")
        result = ssh_capture(ctx.client, f"""
timeout 10 bash -c 'echo | openssl s_client -connect {ctx.dsql_endpoint}:{DSQL_PORT} -servername {ctx.dsql_endpoint} 2>/dev/null | head -5'
echo "EXIT:$?"
""", ctx, strict=False)
        if "EXIT:0" in result.stdout or "CONNECTED" in result.stdout:
            log("  DSQL endpoint reachable from client VM.")
        else:
            log("  WARNING: Could not verify DSQL endpoint connectivity. "
                "This may resolve once the cluster is fully active.")

    log("Client VM bootstrap complete.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(args):
    """Full cleanup: DSQL cluster + EC2 infrastructure."""
    log(f"Cleanup requested for stack: {_cu.STACK} in {_cu.REGION}")

    # Delete DSQL cluster first
    state = load_state()
    cluster_id = None
    if state:
        cluster_id = state.get("cluster_id")

    if cluster_id:
        delete_dsql_cluster(cluster_id)
    else:
        log("No DSQL cluster found in state file; skipping cluster deletion.")

    # Clean up EC2 infrastructure (VPC, subnet, SG, instances) via common
    _common_cleanup_stack()

    # Remove state file
    if STATE_FILE.exists():
        STATE_FILE.unlink()
        log("Removed state file.")

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

    ssh_key_path = Path(args.ssh_key_path).expanduser().resolve()
    ensure_keypair_accessible()
    if not ssh_key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {ssh_key_path}")

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
    # Allow outbound to DSQL (port 5432) -- default SG egress allows all,
    # but explicitly document the requirement
    ensure_ingress_tcp_cidr(sg_id, DSQL_PORT, "0.0.0.0/0")

    client_type = PRODUCTION_CLIENT_TYPE if args.production else CLIENT_INSTANCE_TYPE
    client_iid = ensure_instance(
        f"{_cu.STACK}-client", "client", client_type, subnet_id, sg_id,
    )
    client_info = instance_info_from_id(client_iid, "client")
    log(f"Client: {client_info.public_ip} ({client_type})")

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

    # Save state immediately so benchmark/validate can find the cluster
    save_state(cluster_id, endpoint, client_iid, client_info.public_ip)

    # ── PHASE 3: Bootstrap ───────────────────────────────────────────────
    if not args.skip_bootstrap:
        log("")
        log("=" * 70)
        log("PHASE 3: Client VM Bootstrap")
        log("=" * 70)

        ctx = BootstrapContext(
            ssh_key_path=ssh_key_path,
            self_cidr=self_cidr,
            client=client_info,
            dsql_cluster_id=cluster_id,
            dsql_endpoint=endpoint,
        )

        log("Waiting for SSH connectivity...")
        if not wait_for_ssh(client_info, ctx):
            raise SystemExit("ERROR: Client VM not reachable via SSH.")

        bootstrap_client(ctx)

    # ── Summary ──────────────────────────────────────────────────────────
    log("")
    log("=" * 70)
    log("SETUP COMPLETE")
    log("=" * 70)
    log(f"  Stack:           {_cu.STACK}")
    log(f"  Region:          {_cu.REGION}")
    log(f"  Client VM:       {client_info.public_ip} ({client_type})")
    log(f"  DSQL Cluster:    {cluster_id}")
    log(f"  DSQL Endpoint:   {endpoint}")
    log(f"  State file:      {STATE_FILE}")
    log("")
    log("Next steps:")
    log(f"  Benchmark: python3 -m dsql.benchmark --seed {args.seed}")
    log(f"  Validate:  python3 -m dsql.validate --seed {args.seed}")
    log(f"  Cleanup:   python3 -m dsql.setup --seed {args.seed} --cleanup")


if __name__ == "__main__":
    main()
