"""Shared SSH/SCP helpers for remote execution on EC2 instances."""

import subprocess
import textwrap
import time
from pathlib import Path

from common.types import InstanceInfo
from common.util import log


def host_target_and_jump(host, ctx):
    target = host.public_ip or host.private_ip
    if not target:
        raise RuntimeError(f"Instance {host.role} has no reachable IP.")
    jump = None
    if host.role != ctx.client.role and not host.public_ip:
        if not ctx.client.public_ip:
            raise RuntimeError("Client instance missing public IP for ProxyJump.")
        jump = ctx.client.public_ip
    return target, jump


def ssh_base_cmd(host, ctx):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=30",
        "-i", str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"ec2-user@{target}", "bash", "-s"]
    return cmd


def ssh_run(host, script, ctx, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    subprocess.run(cmd, input=full_script, text=True, check=True)


def ssh_capture(host, script, ctx, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    result = subprocess.run(cmd, input=full_script, text=True, capture_output=True)
    if strict:
        result.check_returncode()
    return result


def scp_put(host, local_path, remote_path, ctx):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "scp",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-i", str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [str(local_path), f"ec2-user@{target}:{remote_path}"]
    subprocess.run(cmd, check=True)


def scp_get(host, remote_path, local_path, ctx):
    target, jump = host_target_and_jump(host, ctx)
    cmd = [
        "scp",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-i", str(ctx.ssh_key_path),
    ]
    if jump:
        proxy = (
            "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"-o IdentitiesOnly=yes -i {ctx.ssh_key_path} ec2-user@{jump} -W %h:%p"
        )
        cmd += ["-o", f"ProxyCommand={proxy}"]
    cmd += [f"ec2-user@{target}:{remote_path}", str(local_path)]
    subprocess.run(cmd, check=True)


def _simple_ssh_cmd(host_ip: str, key_path):
    """Build an SSH command for direct (no ProxyJump) access to *host_ip*."""
    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=30",
        "-i", str(key_path),
        f"ec2-user@{host_ip}",
        "bash", "-s",
    ]


def ssh_run_simple(host_ip: str, key_path, script: str, strict: bool = True):
    """Run *script* on a directly-reachable host (no ProxyJump)."""
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    subprocess.run(_simple_ssh_cmd(host_ip, key_path), input=full, text=True, check=strict)


def ssh_capture_simple(host_ip: str, key_path, script: str, strict: bool = True):
    """Run *script* on a directly-reachable host and capture stdout/stderr."""
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    result = subprocess.run(
        _simple_ssh_cmd(host_ip, key_path), input=full, text=True, capture_output=True,
    )
    if strict:
        result.check_returncode()
    return result


def ssh_stream_simple(host_ip: str, key_path, script: str):
    """Run *script* on a directly-reachable host, returning a streaming Popen."""
    full = "set -euo pipefail\n" + textwrap.dedent(script).lstrip()
    proc = subprocess.Popen(
        _simple_ssh_cmd(host_ip, key_path),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    proc.stdin.write(full)
    proc.stdin.close()
    return proc


def wait_for_ssh(node, ctx, max_attempts=30):
    for attempt in range(max_attempts):
        result = ssh_capture(node, "echo ready", ctx, strict=False)
        if result.returncode == 0:
            return True
        if attempt < max_attempts - 1:
            log(f"  {node.role} ({node.private_ip}) not ready yet "
                f"(attempt {attempt + 1}/{max_attempts})...")
            time.sleep(10)
    log(f"  WARNING: {node.role} ({node.private_ip}) did not become reachable "
        f"after {max_attempts} attempts")
    return False
