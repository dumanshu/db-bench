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

Usage from module setup.py files:
    from common.client import install_client_tools
    install_client_tools(host_ip, key_path, server_type="tidb")
"""

from common.util import log
from common.ssh import ssh_run_simple, wait_for_ssh_simple

SERVER_TYPES = ("tidb", "aurora", "valkey")

MEMTIER_VERSION = "2.1.1.0"
MEMTIER_SRC_URL = (
    f"https://github.com/RedisLabs/memtier_benchmark/archive/"
    f"refs/tags/{MEMTIER_VERSION}.tar.gz"
)


def install_client_tools(host_ip, key_path, server_type, timeout=600):
    if server_type not in SERVER_TYPES:
        raise ValueError(f"server_type must be one of {SERVER_TYPES}, got {server_type!r}")

    log(f"Installing client tools on {host_ip} for server_type={server_type}")
    wait_for_ssh_simple(host_ip, key_path)

    _install_base_packages(host_ip, key_path, timeout=timeout)
    _tune_sysctl(host_ip, key_path)

    if server_type in ("tidb", "aurora"):
        _install_mysql_client(host_ip, key_path, timeout=timeout)
        _install_sysbench(host_ip, key_path, timeout=timeout)

    if server_type == "valkey":
        _install_memtier(host_ip, key_path, timeout=timeout)
        _install_docker(host_ip, key_path)

    log(f"Client tools installed on {host_ip} for {server_type}")


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
