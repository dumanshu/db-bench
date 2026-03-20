"""Shared data types for db-bench infrastructure."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class InstanceInfo:
    """Describes a provisioned EC2 instance."""
    role: str
    instance_id: str
    public_ip: str
    private_ip: str
    availability_zone: Optional[str] = None
