"""Snapshot leaderboard: projects ranked by snapshot footprint.

Counts Cinder volume snapshots (storage-expensive) and Glance snapshot
images (usually small but long-lived) per project, across the selected
regions. Reports total count, total GB (Cinder), and oldest-snapshot age.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import (
    cinder_db,
    glance_db,
    keystone_db,
    keystone_region,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, humanize, safe_for_each_region

from .base import ChartSpec, Param, Report, ReportResult


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class SnapshotLeaderboardReport(Report):
    id = "snapshot_leaderboard"
    name = "Snapshot leaderboard"
    description = (
        "Projects ranked by snapshot count and storage footprint. Separately "
        "counts Cinder volume snapshots (with GB total) and Glance snapshot "
        "images. Oldest-snapshot age is surfaced so retention gaps stand out."
    )
    params = [
        Param(name="domain", label="Domain", kind="select",
              choices=_domain_choices, default="",
              help="Keystone domain to scope the report. Empty = all domains."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
        Param(name="top", label="Top N (chart)", kind="int", default=20,
              placeholder="20",
              help="Projects in the top-N chart. Table always shows everything."),
    ]

    def run(
        self,
        domain: Optional[str] = None,
        regions: Optional[List[str]] = None,
        top: Optional[int] = 20,
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        domain_obj: Optional[Dict[str, Any]] = None
        project_filter: Optional[List[str]] = None
        name_by_id: Dict[str, str] = {}
        if domain:
            domain_obj = openstack.find_domain(domain)
            if domain_obj is None:
                return ReportResult(
                    columns=[],
                    rows=[],
                    metadata={"error": f"Domain not found: {domain!r}"},
                    filename_stem=f"snapshot-leaderboard-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            project_filter = [p["id"] for p in projects]
            name_by_id = {p["id"]: p["name"] for p in projects}

        totals: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "cinder_snapshots": 0, "cinder_gb": 0,
                "glance_snapshots": 0,
                "oldest_created_at": None,
            }
        )

        # --- Cinder snapshots -----------------------------------------------
        if project_filter is not None:
            ph = ",".join(["%s"] * len(project_filter))
            cinder_sql = f"""
                SELECT project_id,
                       COUNT(*) AS n,
                       COALESCE(SUM(volume_size), 0) AS gb,
                       MIN(created_at) AS oldest
                FROM snapshots
                WHERE deleted = 0
                  AND project_id IN ({ph})
                GROUP BY project_id
            """
            cinder_args: List[Any] = list(project_filter)
        else:
            cinder_sql = """
                SELECT project_id,
                       COUNT(*) AS n,
                       COALESCE(SUM(volume_size), 0) AS gb,
                       MIN(created_at) AS oldest
                FROM snapshots
                WHERE deleted = 0
                GROUP BY project_id
            """
            cinder_args = []

        def _cinder_rows(region):
            return query(region, cinder_db(), cinder_sql, cinder_args)

        cinder_results, cinder_errors = safe_for_each_region(selected_regions, _cinder_rows)
        for _, rs in cinder_results:
            for r in rs:
                pid = r["project_id"]
                bucket = totals[pid]
                bucket["cinder_snapshots"] += int(r["n"] or 0)
                bucket["cinder_gb"] += int(r["gb"] or 0)
                _absorb_oldest(bucket, r.get("oldest"))

        # --- Glance snapshot images ----------------------------------------
        # Glance marks snapshot-origin images via an image_properties row
        # (name='image_type', value='snapshot'). Join once per region so
        # we can avoid pulling the full properties table.
        if project_filter is not None:
            ph = ",".join(["%s"] * len(project_filter))
            glance_sql = f"""
                SELECT i.owner AS project_id,
                       COUNT(*) AS n,
                       MIN(i.created_at) AS oldest
                FROM images i
                JOIN image_properties ip ON ip.image_id = i.id
                WHERE i.status = 'active'
                  AND i.owner IN ({ph})
                  AND ip.deleted = 0
                  AND ip.name = 'image_type'
                  AND ip.value = 'snapshot'
                GROUP BY i.owner
            """
            glance_args: List[Any] = list(project_filter)
        else:
            glance_sql = """
                SELECT i.owner AS project_id,
                       COUNT(*) AS n,
                       MIN(i.created_at) AS oldest
                FROM images i
                JOIN image_properties ip ON ip.image_id = i.id
                WHERE i.status = 'active'
                  AND ip.deleted = 0
                  AND ip.name = 'image_type'
                  AND ip.value = 'snapshot'
                GROUP BY i.owner
            """
            glance_args = []

        def _glance_rows(region):
            return query(region, glance_db(), glance_sql, glance_args)

        glance_results, glance_errors = safe_for_each_region(selected_regions, _glance_rows)
        for _, rs in glance_results:
            for r in rs:
                pid = r["project_id"]
                bucket = totals[pid]
                bucket["glance_snapshots"] += int(r["n"] or 0)
                _absorb_oldest(bucket, r.get("oldest"))

        # --- Resolve project names if we didn't already have them ---------
        if project_filter is None and totals:
            pid_list = list(totals.keys())
            ph = ",".join(["%s"] * len(pid_list))
            rows = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name, domain_id FROM project WHERE id IN ({ph})",
                pid_list,
            )
            name_by_id = {r["id"]: r["name"] for r in rows}

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows_out: List[Dict[str, Any]] = []
        for pid, b in totals.items():
            total_count = b["cinder_snapshots"] + b["glance_snapshots"]
            oldest = b["oldest_created_at"]
            oldest_age = humanize((now - oldest).total_seconds()) if oldest else "-"
            rows_out.append({
                "project_id": pid,
                "project_name": name_by_id.get(pid, "(unknown)"),
                "cinder_snapshots": b["cinder_snapshots"],
                "cinder_gb": b["cinder_gb"],
                "glance_snapshots": b["glance_snapshots"],
                "total": total_count,
                "oldest_created_at": oldest,
                "oldest_age": oldest_age,
            })

        rows_out.sort(key=lambda r: (-r["total"], -r["cinder_gb"], r["project_name"] or ""))

        top_n = max(1, int(top or 20))
        top_rows = rows_out[:top_n]
        chart = ChartSpec(
            kind="bar",
            title=f"Top {len(top_rows)} projects by snapshot count",
            x_label="Project",
            y_label="Snapshots",
            x_categories=[r["project_name"] for r in top_rows],
            series=[
                {"label": "Cinder", "data": [r["cinder_snapshots"] for r in top_rows]},
                {"label": "Glance", "data": [r["glance_snapshots"] for r in top_rows]},
            ],
        )

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "projects_with_snapshots": len(rows_out),
            "total_cinder_snapshots": sum(r["cinder_snapshots"] for r in rows_out),
            "total_cinder_gb": sum(r["cinder_gb"] for r in rows_out),
            "total_glance_snapshots": sum(r["glance_snapshots"] for r in rows_out),
            "region_errors": format_region_errors(cinder_errors + glance_errors),
        }

        stem_bits = ["snapshot-leaderboard"]
        stem_bits.append(domain_obj["name"] if domain_obj else "all-domains")
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("project_name", "Project"),
                ("project_id", "Project ID"),
                ("cinder_snapshots", "Cinder snaps"),
                ("cinder_gb", "Cinder GB"),
                ("glance_snapshots", "Glance snaps"),
                ("total", "Total"),
                ("oldest_age", "Oldest"),
                ("oldest_created_at", "Oldest created_at"),
            ],
            rows=rows_out,
            charts=[chart] if top_rows else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


def _absorb_oldest(bucket: Dict[str, Any], candidate) -> None:
    if candidate is None:
        return
    current = bucket["oldest_created_at"]
    if current is None or candidate < current:
        bucket["oldest_created_at"] = candidate


REPORT = SnapshotLeaderboardReport()
