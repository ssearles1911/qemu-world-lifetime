"""Capability registry.

Capabilities are the unit of authorization above project scoping. They
are *fixed in code* — adding a capability is a code change. Admins map
Keystone role names to existing capabilities via the SQLite store.

Local administrators implicitly hold every capability; their access is
not subject to the role mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List


class Capability(str, Enum):
    """Authoritative capability identifiers. The string value is what
    gets persisted in `role_capabilities.capability` and what callers
    pass to `has_capability()` / `requires_capability()`.
    """

    VIEW_ALL_PROJECTS = "view_all_projects"
    MANAGE_CONFIG = "manage_config"
    MANAGE_USERS = "manage_users"
    VIEW_AUDIT_LOG = "view_audit_log"


@dataclass(frozen=True)
class CapabilityInfo:
    name: str
    label: str
    description: str


CAPABILITY_REGISTRY: List[CapabilityInfo] = [
    CapabilityInfo(
        name=Capability.VIEW_ALL_PROJECTS.value,
        label="View all projects",
        description=(
            "Bypass per-user project filtering. Reports that support "
            "scoping return data across every project, not just the ones "
            "the user has Keystone roles on."
        ),
    ),
    CapabilityInfo(
        name=Capability.MANAGE_CONFIG.value,
        label="Manage configuration",
        description=(
            "Edit regions, schema names, Keystone settings, and the "
            "role-to-capability mapping. Equivalent to the local-admin "
            "configuration surface."
        ),
    ),
    CapabilityInfo(
        name=Capability.MANAGE_USERS.value,
        label="Manage local administrators",
        description=(
            "Create, reset, and remove local administrator accounts."
        ),
    ),
    CapabilityInfo(
        name=Capability.VIEW_AUDIT_LOG.value,
        label="View audit log",
        description=(
            "Read access to the configuration audit log. Useful for "
            "security/compliance reviewers without granting edit rights."
        ),
    ),
]


ALL_CAPABILITIES = frozenset(c.value for c in Capability)


def is_known_capability(name: str) -> bool:
    return name in ALL_CAPABILITIES
