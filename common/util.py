"""Shared globals, logging, AWS session helpers, and argument parsing for db-bench."""

import argparse
import ipaddress
import os
import shutil
import urllib.request

import boto3
import botocore
from botocore.config import Config
from datetime import datetime
from pathlib import Path


REGION = "us-east-1"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DB_PROFILE = os.environ.get("DB_PROFILE", "sandbox-storage")
SEED = ""
STACK = ""
OWNER = os.environ.get("OWNER", "")
CLEANUP_LOG_PATH = Path("cleanup.log")
LOG_TO_FILE = None
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)

SSH_PORT = 22


def configure_runtime(*, region=None, seed=None, owner=None, aws_profile=None,
                       db_profile=None, stack_prefix="loadtest"):
    global REGION, SEED, STACK, AWS_PROFILE, DB_PROFILE, OWNER
    if region:
        REGION = region
    if seed:
        SEED = seed
    if owner is not None:
        OWNER = owner
    if aws_profile:
        AWS_PROFILE = aws_profile
    if db_profile:
        DB_PROFILE = db_profile
    STACK = f"{stack_prefix}-{SEED}"


def configure_from_args(args, stack_prefix="loadtest"):
    configure_runtime(
        region=args.region,
        seed=args.seed,
        owner=args.owner if args.owner != "" else OWNER,
        aws_profile=args.aws_profile,
        db_profile=getattr(args, "db_profile", None),
        stack_prefix=stack_prefix,
    )


def add_common_args(parser):
    parser.add_argument("--region", default=REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Unique seed used in stack name.")
    parser.add_argument("--owner", default=os.environ.get("OWNER", ""), help="Owner tag value.")
    parser.add_argument("--ssh-cidr", help="CIDR allowed for SSH (default: detected public IP /32).")
    parser.add_argument("--aws-profile", help="AWS named profile for infrastructure (EC2/VPC).")
    parser.add_argument(
        "--db-profile",
        help="AWS named profile for database service APIs (default: same as --aws-profile).",
    )
    parser.add_argument("--skip-bootstrap", action="store_true", help="Provision infrastructure only.")
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources.")


def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S%z]")


def log(msg):
    global LOG_TO_FILE
    line = f"{ts()} {msg}"
    print(line, flush=True)
    if LOG_TO_FILE:
        try:
            LOG_TO_FILE.parent.mkdir(parents=True, exist_ok=True)
            with LOG_TO_FILE.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


def need_cmd(cmd):
    if shutil.which(cmd):
        return
    raise SystemExit(f"ERROR: Required command '{cmd}' not found in PATH.")


def my_public_cidr():
    ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    ipaddress.ip_address(ip)
    return f"{ip}/32"


def aws_session():
    try:
        return boto3.session.Session(profile_name=AWS_PROFILE, region_name=REGION)
    except botocore.exceptions.ProfileNotFound:
        raise SystemExit(
            f"ERROR: AWS profile '{AWS_PROFILE}' not found. "
            "Configure it with aws configure or export AWS_PROFILE."
        )


def db_session():
    profile = DB_PROFILE or AWS_PROFILE
    try:
        return boto3.session.Session(profile_name=profile, region_name=REGION)
    except botocore.exceptions.ProfileNotFound:
        raise SystemExit(
            f"ERROR: AWS profile '{profile}' not found. "
            "Configure it with aws configure or export AWS_PROFILE."
        )


def ec2():
    return aws_session().client("ec2", region_name=REGION, config=BOTO_CONFIG)


def ssm():
    return aws_session().client("ssm", region_name=REGION, config=BOTO_CONFIG)


def elbv2():
    return aws_session().client("elbv2", region_name=REGION, config=BOTO_CONFIG)


def tags_common():
    tags = [{"Key": "Project", "Value": STACK}]
    if OWNER:
        tags.append({"Key": "Owner", "Value": OWNER})
    return tags


def ensure_tags(resource_ids, extra):
    if not resource_ids:
        return
    ec2().create_tags(Resources=resource_ids, Tags=tags_common() + extra)
