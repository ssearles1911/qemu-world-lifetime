"""Shared OpenStack queries used by multiple reports.

Report-specific queries belong in the report module; only queries that
more than one report consumes (Keystone domain/project lookups, Nova cell
discovery, host aggregate discovery) live here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from .config import Region, keystone_db, keystone_region, nova_api_db, parse_regions
from .db import query
from .util import safe_for_each_region


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


def list_all_projects() -> List[Dict[str, Any]]:
    """Every enabled non-domain project across every Keystone domain.

    Returns rows with `id`, `name`, and `domain_id`. Used for reports
    that aggregate over the whole deployment (domain-level leaderboard,
    cross-domain QEMU lifetime, etc).
    """
    sql = """
        SELECT id, name, domain_id
        FROM project
        WHERE is_domain = 0 AND enabled = 1
        ORDER BY domain_id, name
    """
    return query(keystone_region(), keystone_db(), sql)


def list_aggregates() -> List[Dict[str, Any]]:
    """Names of every Nova host aggregate, across every configured region.

    Returns one row per `(region, aggregate)` pair: `{"region", "name"}`.
    Aggregate names can repeat across regions; the form's multiselect
    deduplicates by name when rendering choices, but we surface the
    region in the row so callers can correlate where each aggregate
    lives if needed.

    Per-region failures are silently dropped — a dead replica should not
    prevent the SPLA form from rendering.
    """
    schema = nova_api_db()

    def _collect(region: Region) -> List[Dict[str, Any]]:
        rows = query(
            region, schema,
            "SELECT name FROM aggregates WHERE deleted = 0 ORDER BY name",
        )
        return [{"region": region.name, "name": r["name"]} for r in rows]

    results, _errors = safe_for_each_region(parse_regions(), _collect)
    out: List[Dict[str, Any]] = []
    for _, region_rows in results:
        out.extend(region_rows)
    return out


def aggregate_hosts(region: Region, aggregate_names: Sequence[str]) -> List[str]:
    """Compute hosts that belong to any of the named aggregates in `region`.

    Returns a flat list of hostnames. Empty `aggregate_names` short-circuits
    to an empty list to avoid the awkward `WHERE name IN ()` SQL.
    """
    if not aggregate_names:
        return []
    placeholders = ",".join(["%s"] * len(aggregate_names))
    rows = query(
        region, nova_api_db(),
        f"""
        SELECT DISTINCT ah.host
        FROM aggregate_hosts ah
        JOIN aggregates a ON a.id = ah.aggregate_id
        WHERE a.name IN ({placeholders})
          AND ah.deleted = 0
          AND a.deleted = 0
        """,
        list(aggregate_names),
    )
    return [r["host"] for r in rows if r.get("host")]


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
