"""Shared DB + query logic for the QEMU lifetime report.

Used by both the CLI (`qemu_lifetime_report.py`) and the web UI (`web.py`).
All OpenStack DBs (keystone, nova_api, nova_cell*) are expected to live on
one MariaDB replica reachable with a single set of credentials.

Config (env vars; auto-loaded from `.env` in the CWD if present):
    OS_DB_HOST       (default 127.0.0.1)
    OS_DB_PORT       (default 3306)
    OS_DB_USER       (default nova)
    OS_DB_PASSWORD   (default empty)
    KEYSTONE_DB      (default keystone)
    NOVA_API_DB      (default nova_api)
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import pymysql
import pymysql.cursors

# Load .env from CWD (or any parent) before any os.environ.get() runs.
# Real env vars take precedence over .env entries.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# Nova action names that count as a QEMU lifecycle event for this report.
# Reboot, migrate, resize, rebuild, and create are intentionally excluded.
LIFECYCLE_ACTIONS: Tuple[str, ...] = (
    "start",
    "stop",
    "shelve",
    "unshelve",
    "shelveOffload",
    "live-migration",
)


def db_params(database: Optional[str] = None) -> Dict[str, Any]:
    return {
        "host": os.environ.get("OS_DB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("OS_DB_PORT", "3306")),
        "user": os.environ.get("OS_DB_USER", "nova"),
        "password": os.environ.get("OS_DB_PASSWORD", ""),
        "database": database,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


def keystone_db() -> str:
    return os.environ.get("KEYSTONE_DB", "keystone")


def nova_api_db() -> str:
    return os.environ.get("NOVA_API_DB", "nova_api")


def query(database: str, sql: str, args: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    conn = pymysql.connect(**db_params(database))
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return list(cur.fetchall())
    finally:
        conn.close()


def list_domains() -> List[Dict[str, Any]]:
    """Domains in Keystone are rows in `project` with is_domain=1."""
    sql = """
        SELECT d.id, d.name,
               (SELECT COUNT(*) FROM project p
                WHERE p.domain_id = d.id AND p.is_domain = 0 AND p.enabled = 1
               ) AS project_count
        FROM project d
        WHERE d.is_domain = 1 AND d.enabled = 1
        ORDER BY d.name
    """
    return query(keystone_db(), sql)


def find_domain(needle: str) -> Optional[Dict[str, Any]]:
    """Resolve a domain by id or name."""
    sql = """
        SELECT id, name
        FROM project
        WHERE is_domain = 1 AND enabled = 1 AND (id = %s OR name = %s)
        LIMIT 1
    """
    rows = query(keystone_db(), sql, (needle, needle))
    return rows[0] if rows else None


def list_projects(domain_id: str) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, name
        FROM project
        WHERE domain_id = %s AND is_domain = 0 AND enabled = 1
        ORDER BY name
    """
    return query(keystone_db(), sql, (domain_id,))


def list_cell_dbs() -> List[str]:
    """Discover cell DB names from `nova_api.cell_mappings`."""
    rows = query(
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


def fetch_instances(
    cell_db: str,
    project_ids: Sequence[str],
    days: Optional[int],
) -> List[Dict[str, Any]]:
    """Active instances in the given projects with their most-recent
    lifecycle action, scoped to one cell DB. Cross-DB join into keystone
    for project name (all DBs live on the same MariaDB server).
    """
    if not project_ids:
        return []

    proj_ph = ",".join(["%s"] * len(project_ids))
    act_ph = ",".join(["%s"] * len(LIFECYCLE_ACTIONS))
    ks = keystone_db()

    sql = f"""
        WITH project_instances AS (
            SELECT uuid, project_id
            FROM instances
            WHERE deleted = 0
              AND project_id IN ({proj_ph})
        ),
        ranked AS (
            SELECT ia.instance_uuid, ia.action, ia.start_time, ia.user_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ia.instance_uuid
                       ORDER BY ia.start_time DESC
                   ) AS rn
            FROM instance_actions ia
            JOIN project_instances pi ON pi.uuid = ia.instance_uuid
            WHERE ia.deleted = 0
              AND ia.action IN ({act_ph})
        )
        SELECT
            i.uuid                                  AS uuid,
            i.display_name                          AS name,
            i.host                                  AS compute_host,
            i.vm_state                              AS vm_state,
            i.power_state                           AS power_state,
            i.created_at                            AS created_at,
            i.project_id                            AS project_id,
            p.name                                  AS project_name,
            r.action                                AS last_action,
            r.start_time                            AS last_action_time,
            r.user_id                               AS last_action_user,
            COALESCE(r.start_time, i.created_at)    AS effective_time
        FROM instances i
        LEFT JOIN ranked r ON r.instance_uuid = i.uuid AND r.rn = 1
        LEFT JOIN {ks}.project p ON p.id = i.project_id
        WHERE i.deleted = 0
          AND i.project_id IN ({proj_ph})
    """

    args: List[Any] = list(project_ids) + list(LIFECYCLE_ACTIONS) + list(project_ids)

    if days is not None:
        sql += " AND COALESCE(r.start_time, i.created_at) < (UTC_TIMESTAMP() - INTERVAL %s DAY)"
        args.append(days)

    sql += " ORDER BY p.name, effective_time"
    return query(cell_db, sql, args)


def humanize(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def annotate_ages(rows: Iterable[Dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in rows:
        eff = r.get("effective_time")
        if eff is None:
            r["age_seconds"] = None
            r["age"] = "-"
        else:
            r["age_seconds"] = (now - eff).total_seconds()
            r["age"] = humanize(r["age_seconds"])
        if r.get("last_action") is None:
            r["last_action"] = "(none recorded)"


def collect_report(domain_selector: str, days: Optional[int]) -> Dict[str, Any]:
    """Resolve a domain by name/id, then fetch + annotate every instance
    in its projects across all cells. Returns a dict with keys:
        domain    — dict or None if not found
        projects  — list of project dicts (sorted by name)
        rows      — list of instance dicts (annotated with age)
    """
    domain = find_domain(domain_selector)
    if domain is None:
        return {"domain": None, "projects": [], "rows": []}
    projects = list_projects(domain["id"])
    project_ids = [p["id"] for p in projects]
    rows: List[Dict[str, Any]] = []
    for cell in list_cell_dbs():
        rows.extend(fetch_instances(cell, project_ids, days))
    annotate_ages(rows)
    return {"domain": domain, "projects": projects, "rows": rows}
