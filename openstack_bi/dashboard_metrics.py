"""Dashboard metrics — single source of truth for the daily snapshot.

Defines every metric the cloud-health dashboard tracks, the SQL that
produces it, and which regional database it lives in. Used by both:

* `opsbi snapshot-metrics` — the daily collector that writes a row
  per `(snapshot_date, region, metric)` into the SQLite
  `dashboard_metric_history` table. Designed to be wired into cron.
* The `/dashboard` view — reads the same metrics live for the
  current-state tiles, and reads the SQLite history for sparklines
  and trend charts.

Adding a new metric is one entry in `METRIC_DEFS` — no migrations,
no new columns.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from . import openstack
from .config import (
    Region,
    cinder_db,
    keystone_db,
    keystone_region,
    neutron_db,
    parse_regions,
)
from .db import query

# Scope strings — how a metric is computed.
#   cells     — fan out across the per-cell Nova DBs in each region; the
#               per-region value is the sum of the cell results.
#   neutron   — run once against `neutron_db()` in each region.
#   cinder    — run once against `cinder_db()`  in each region.
#   keystone  — run once on the keystone region only; it's a global
#               service so per-region rows would be misleading.
SCOPE_CELLS = "cells"
SCOPE_NEUTRON = "neutron"
SCOPE_CINDER = "cinder"
SCOPE_KEYSTONE = "keystone"

# Sentinel `region` value for the cloud-wide rollup row.
COMBINED = "_combined"


@dataclass(frozen=True)
class MetricDef:
    name: str
    sql: str
    scope: str
    combinable: bool = True   # whether the _combined row is a sum across regions
    needs_date: bool = False  # whether SQL has one %s for the snapshot date


# Canonical metric set. Mirrors the bash health-check script the
# dashboard is replacing — same SELECTs, same filters. Adding a new
# metric is *just* a new MetricDef entry; the history table is long
# format so it never needs a schema change.
METRIC_DEFS: List[MetricDef] = [
    # Compute (per-cell within each region).
    MetricDef(
        "instances_total",
        "SELECT COUNT(*) FROM instances WHERE deleted=0",
        SCOPE_CELLS,
    ),
    MetricDef(
        "instances_error",
        "SELECT COUNT(*) FROM instances WHERE deleted=0 AND vm_state='error'",
        SCOPE_CELLS,
    ),

    # Network (neutron schema per region).
    # Note: LIKE literals use `%%` so pymysql, which mogrifies on every
    # `execute(sql, args)`, treats a bare `%` as itself rather than a
    # format spec.
    MetricDef(
        "ports_total",
        "SELECT COUNT(*) FROM ports",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "ports_build",
        "SELECT COUNT(*) FROM ports WHERE status LIKE '%%build%%'",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "ports_error",
        "SELECT COUNT(*) FROM ports WHERE status LIKE '%%error%%'",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "routers_total",
        "SELECT COUNT(*) FROM routers",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "routers_error",
        "SELECT COUNT(*) FROM routers WHERE status LIKE '%%error%%'",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "floating_ips_total",
        "SELECT COUNT(*) FROM floatingips",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "vpn_connections_total",
        "SELECT COUNT(*) FROM ipsec_site_connections",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "vpn_connections_active",
        "SELECT COUNT(*) FROM ipsec_site_connections WHERE status='active'",
        SCOPE_NEUTRON,
    ),
    MetricDef(
        "vpn_connections_down",
        "SELECT COUNT(*) FROM ipsec_site_connections WHERE status='down'",
        SCOPE_NEUTRON,
    ),

    # Block storage (cinder schema per region).
    MetricDef(
        "volumes_total",
        "SELECT COUNT(*) FROM volumes WHERE deleted=0",
        SCOPE_CINDER,
    ),
    MetricDef(
        "volumes_error",
        "SELECT COUNT(*) FROM volumes WHERE deleted=0 AND status='error'",
        SCOPE_CINDER,
    ),
    MetricDef(
        "snapshots_total",
        "SELECT COUNT(*) FROM snapshots WHERE deleted=0",
        SCOPE_CINDER,
    ),
    MetricDef(
        "snapshots_error",
        "SELECT COUNT(*) FROM snapshots WHERE deleted=0 AND status LIKE '%%error%%'",
        SCOPE_CINDER,
    ),
    MetricDef(
        # Cinder's `created_at` is a DATETIME; `LIKE 'YYYY-MM-DD%%'`
        # matches any timestamp on that day. Same pattern the bash
        # script uses for the daily autobackup CSV. The date itself
        # binds as a parameter (the only `%s` here).
        "snapshots_autobackup_today",
        "SELECT COUNT(*) FROM snapshots "
        "WHERE created_at LIKE %s "
        "AND display_description LIKE '%%autobackup%%'",
        SCOPE_CINDER,
        needs_date=True,
    ),

    # Keystone (global — single query, not summed; no per-region rows).
    MetricDef(
        "keystone_domains",
        "SELECT COUNT(*) FROM project WHERE is_domain=1 AND enabled=1",
        SCOPE_KEYSTONE,
        combinable=False,
    ),
    MetricDef(
        "keystone_projects",
        "SELECT COUNT(*) FROM project WHERE is_domain=0 AND enabled=1",
        SCOPE_KEYSTONE,
        combinable=False,
    ),
]

METRIC_DEFS_BY_NAME: Dict[str, MetricDef] = {m.name: m for m in METRIC_DEFS}


def _today() -> str:
    """Today's UTC date as `YYYY-MM-DD`.

    Matches SQLite's `date('now')` so historic queries line up.
    """
    return datetime.now(timezone.utc).date().isoformat()


def _scalar(rows: List[Any]) -> int:
    """Single integer out of a count(*) result, regardless of row shape."""
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(next(iter(first.values())) or 0)
    return int(first[0] or 0)


def _row(snapshot_date: str, snapshot_at: str, region: str,
        metric: str, value: int) -> Dict[str, Any]:
    return {
        "snapshot_date": snapshot_date,
        "snapshot_at": snapshot_at,
        "region": region,
        "metric": metric,
        "value": int(value),
    }


def collect_snapshot(
    snapshot_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Run every metric and return the rows ready to insert.

    Per-region rows are written for `cells`, `neutron`, and `cinder`
    metrics; the `_combined` row sums them when `combinable=True`.
    Keystone metrics are global and only emit a `_combined` row — the
    dashboard treats them as cloud-wide and never tries to attribute
    them to a region.
    """
    snapshot_date = snapshot_date or _today()
    snapshot_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    regions = parse_regions()
    cells_by_region = {r.name: openstack.list_cells(r) for r in regions}
    keystone_r = keystone_region(regions)

    rows: List[Dict[str, Any]] = []
    per_region: Dict[str, Dict[str, int]] = {r.name: {} for r in regions}

    for metric in METRIC_DEFS:
        args: Tuple[Any, ...] = (
            (f"%{snapshot_date}%",) if metric.needs_date else ()
        )

        if metric.scope == SCOPE_KEYSTONE:
            value = _scalar(query(keystone_r, keystone_db(), metric.sql, args))
            rows.append(_row(snapshot_date, snapshot_at,
                             COMBINED, metric.name, value))
            continue

        for region in regions:
            if metric.scope == SCOPE_CELLS:
                value = sum(
                    _scalar(query(region, cell, metric.sql, args))
                    for cell in cells_by_region[region.name]
                )
            elif metric.scope == SCOPE_NEUTRON:
                value = _scalar(query(region, neutron_db(), metric.sql, args))
            elif metric.scope == SCOPE_CINDER:
                value = _scalar(query(region, cinder_db(), metric.sql, args))
            else:
                raise ValueError(f"unknown metric scope: {metric.scope!r}")
            per_region[region.name][metric.name] = int(value)
            rows.append(_row(snapshot_date, snapshot_at,
                             region.name, metric.name, value))

        if metric.combinable:
            combined = sum(
                per_region[r.name].get(metric.name, 0) for r in regions
            )
            rows.append(_row(snapshot_date, snapshot_at,
                             COMBINED, metric.name, combined))

    return rows


def write_snapshot(rows: List[Dict[str, Any]]) -> None:
    """`INSERT OR REPLACE` rows into `dashboard_metric_history`.

    Idempotent on `(snapshot_date, region, metric)` — re-running the
    collector for the same day overwrites that day's values, so cron
    retries don't accumulate duplicates.
    """
    from . import config_db
    with config_db.cursor() as cur:
        cur.executemany(
            "INSERT OR REPLACE INTO dashboard_metric_history "
            "(snapshot_date, snapshot_at, region, metric, value) "
            "VALUES (:snapshot_date, :snapshot_at, :region, :metric, :value)",
            rows,
        )


def current_snapshot(
    snapshot_date: Optional[str] = None,
) -> Dict[Tuple[str, str], int]:
    """Live current-state values keyed by `(region, metric)`.

    Same query path as `collect_snapshot` but in-memory only — no
    history table write. The dashboard uses this for the right-now
    tile values; the history table is the trend layer.
    """
    return {
        (r["region"], r["metric"]): int(r["value"])
        for r in collect_snapshot(snapshot_date)
    }


def history(metric: str, region: str, days: int) -> List[Tuple[str, int]]:
    """Time series for one `(metric, region)` over the last `days` days.

    Returns `[(YYYY-MM-DD, value), ...]` sorted oldest-first. Empty
    when the collector has not yet run.
    """
    from . import config_db
    days = max(1, int(days))
    with config_db.cursor() as cur:
        cur.execute(
            "SELECT snapshot_date, value "
            "FROM dashboard_metric_history "
            "WHERE metric = ? AND region = ? "
            "AND snapshot_date >= date('now', ?) "
            "ORDER BY snapshot_date",
            (metric, region, f"-{days} days"),
        )
        return [(r[0], int(r[1])) for r in cur.fetchall()]


def today_autobackups_csv(region: Region, snapshot_date: str) -> str:
    """CSV string of autobackup snapshot rows created on `snapshot_date`
    in `region`. Mirrors the bash script's CSV attachment — same
    selection (`display_description LIKE '%autobackup%'`), full row.
    Empty string when no rows match.
    """
    rows = query(
        region, cinder_db(),
        "SELECT * FROM snapshots "
        "WHERE created_at LIKE %s "
        "AND display_description LIKE '%%autobackup%%' "
        "ORDER BY created_at",
        (f"%{snapshot_date}%",),
    )
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if v is None else v) for k, v in r.items()})
    return buf.getvalue()
