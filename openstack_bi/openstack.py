"""Shared OpenStack queries used by multiple reports.

Report-specific queries belong in the report module; only queries that
more than one report consumes (Keystone domain/project lookups, Nova cell
discovery) live here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .config import Region, keystone_db, keystone_region, nova_api_db
from .db import query


def list_domains() -> List[Dict[str, Any]]:
    """Enabled Keystone domains with their project counts.

    Keystone is shared; we hit the region configured via `KEYSTONE_REGION`.
    """
    sql = """
        SELECT d.id, d.name,
               (SELECT COUNT(*) FROM project p
                WHERE p.domain_id = d.id AND p.is_domain = 0 AND p.enabled = 1
               ) AS project_count
        FROM project d
        WHERE d.is_domain = 1 AND d.enabled = 1
        ORDER BY d.name
    """
    return query(keystone_region(), keystone_db(), sql)


def find_domain(needle: str) -> Optional[Dict[str, Any]]:
    """Resolve a domain by id or name."""
    sql = """
        SELECT id, name
        FROM project
        WHERE is_domain = 1 AND enabled = 1 AND (id = %s OR name = %s)
        LIMIT 1
    """
    rows = query(keystone_region(), keystone_db(), sql, (needle, needle))
    return rows[0] if rows else None


def list_projects(domain_id: str) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, name
        FROM project
        WHERE domain_id = %s AND is_domain = 0 AND enabled = 1
        ORDER BY name
    """
    return query(keystone_region(), keystone_db(), sql, (domain_id,))


def list_cells(region: Region) -> List[str]:
    """Discover cell DB names for one region from its `nova_api.cell_mappings`."""
    rows = query(
        region,
        nova_api_db(),
        "SELECT name, database_connection FROM cell_mappings ORDER BY id",
    )
    cells: List[str] = []
    for r in rows:
        conn = r.get("database_connection") or ""
        # SQLAlchemy URL: dialect+driver://user:pass@host/dbname
        parsed = urlparse(conn.replace("mysql+pymysql://", "mysql://", 1))
        dbname = (parsed.path or "").lstrip("/")
        if dbname:
            cells.append(dbname)
    return cells
